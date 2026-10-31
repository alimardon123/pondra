//! The host of extensions' WebAssembly parts (ADR-075 §2, E2a in designs/extensions-before-1-0.md): Wasmtime with the
//! component model, calling the world in `wit/extension.wit`. A part is compiled once per Wasmtime release and kept on disk
//! by its SHA-256 (`compiled`), and each call runs in a store of its own, so no call sees another's memory and a part holds
//! nothing between batches. A part imports nothing yet: one that imports what this Pondra doesn't offer is refused by the
//! import's name when it is loaded (ADR-075 §3). Arguments and answers cross as Arrow IPC.
#![cfg_attr(not(test), allow(dead_code))] // (E2b registers its functions in every session; until then its tests call it)
use crate::codes::coded;
use anyhow::{bail, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::{reader::StreamReader, writer::StreamWriter};
use datafusion::arrow::record_batch::{RecordBatch, RecordBatchOptions};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};
use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};

wasmtime::component::bindgen!({ path: "wit", world: "extension" });

/// One scalar function an extension has, as its `describe()` says.
#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub name: String,
    pub arguments: Vec<DataType>,
    pub returns: DataType,
    pub volatile: bool,
}

/// An extension's WebAssembly part, compiled and ready to call: its name and version, and its functions.
pub struct Loaded {
    pub name: String,
    pub version: String,
    pub functions: Vec<Function>,
    pre: ExtensionPre<()>,
}

impl std::fmt::Debug for Loaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{} {}", self.name, self.version) }
}

/// The process's Wasmtime: compiled code is the engine's, so one is shared by every node in a process.
fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config.wasm_component_model(true);
        Engine::new(&config).expect("Wasmtime's engine")
    })
}

/// Where compiled parts are kept: `PONDRA_CACHE_DIR`, else the user's cache folder; in a folder per Wasmtime release, whose
/// compiled code only that release reads. None: compiled at each load.
fn cache() -> Option<PathBuf> {
    let base = crate::env::get("PONDRA_CACHE_DIR").map(PathBuf::from).or_else(|| {
        std::env::var_os("XDG_CACHE_HOME").map(PathBuf::from).or_else(|| std::env::var_os("HOME").map(|h| PathBuf::from(h).join(".cache"))).map(|c| c.join("pondra"))
    })?;
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    engine().precompile_compatibility_hash().hash(&mut h);
    Some(base.join("extensions").join(format!("{:016x}", h.finish())))
}

/// `bytes` compiled: read back from the cache when this Wasmtime compiled them before, else compiled and kept there. A file
/// the cache can't read (another build's, cut short) is compiled again and replaced.
fn compiled(bytes: &[u8], sha256: &str) -> Result<Component> {
    let file = cache().map(|dir| dir.join(format!("{sha256}.cwasm")));
    if let Some(file) = file.as_ref().filter(|f| f.exists()) {
        // SAFETY: the file is this process's own compiled output, written whole and renamed into place below; Wasmtime checks
        // that it was compiled by this release with these settings before it runs any of it.
        if let Ok(c) = unsafe { Component::deserialize_file(engine(), file) } {
            return Ok(c);
        }
    }
    let component = Component::new(engine(), bytes).map_err(|e| coded("22023", format!("not a WebAssembly component Pondra can load: {e:#}")))?;
    if let (Some(file), Ok(code)) = (file, component.serialize()) {
        let kept = file.parent().map(std::fs::create_dir_all).transpose().and_then(|_| {
            let part = file.with_extension(format!("{}.part", uuid::Uuid::new_v4().simple()));
            std::fs::write(&part, code).and_then(|()| std::fs::rename(&part, &file))
        });
        if let Err(e) = kept {
            eprintln!("an extension's compiled code wasn't kept ({}): {e}", file.display()); // (it is compiled again next time)
        }
    }
    Ok(component)
}

/// Loads a part from its bytes: compiled (or read from the cache), linked against what this Pondra offers, and asked what it
/// has. A part that imports anything this Pondra lacks is refused by the import's name; one whose `describe()` can't be read
/// is refused saying why.
pub fn load(bytes: &[u8], sha256: &str) -> Result<Loaded> {
    let component = compiled(bytes, sha256)?;
    let linker: Linker<()> = Linker::new(engine()); // (no imports yet: `log@1` and `clock@1` come in E5)
    let ty = component.component_type();
    if let Some((import, _)) = ty.imports(engine()).next() {
        return Err(coded("0A000", format!("the extension needs {import}, which this Pondra doesn't offer")));
    }
    let pre = ExtensionPre::new(linker.instantiate_pre(&component)?)?;
    let mut store = Store::new(engine(), ());
    let said = pre.instantiate(&mut store)?.call_describe(&mut store)?;
    let (name, version, functions) = described(&said)?;
    Ok(Loaded { name, version, functions, pre })
}

/// `describe()`'s JSON: the name, version and functions, each type Arrow's as written (`Int64`, `Utf8`). Fields a host
/// doesn't know are passed over, as the world says.
fn described(json: &str) -> Result<(String, String, Vec<Function>)> {
    let v: serde_json::Value = serde_json::from_str(json).context("describe() isn't JSON")?;
    let text = |v: &serde_json::Value, k: &str| v[k].as_str().map(str::to_string).with_context(|| format!("describe() has no \"{k}\""));
    let ty = |t: &serde_json::Value| -> Result<DataType> {
        let t = t.as_str().context("a type is a string")?;
        t.parse::<DataType>().map_err(|_| coded("22023", format!("describe() names a type Pondra doesn't know: {t}")))
    };
    let mut functions = vec![];
    for f in v["functions"].as_array().map(Vec::as_slice).unwrap_or_default() {
        let arguments = f["arguments"].as_array().map(Vec::as_slice).unwrap_or_default().iter().map(ty).collect::<Result<_>>()?;
        functions.push(Function { name: text(f, "name")?, arguments, returns: ty(&f["returns"])?, volatile: f["volatile"].as_bool().unwrap_or(false) });
    }
    Ok((text(&v, "name")?, text(&v, "version")?, functions))
}

impl Loaded {
    /// Calls `function` over `columns`, one per argument, cast to the types `describe()` gave: its one column of answers, as
    /// many as the rows. The function's own error, a trap or an answer of the wrong type or length fails the call with its
    /// words.
    pub fn call(&self, function: &str, columns: &[ArrayRef], rows: usize) -> Result<ArrayRef> {
        let f = self.functions.iter().find(|f| f.name == function).with_context(|| format!("{} has no function {function}", self.name))?;
        if columns.len() != f.arguments.len() {
            bail!("{function} takes {} arguments, not {}", f.arguments.len(), columns.len());
        }
        let cast = columns.iter().zip(&f.arguments).map(|(c, t)| datafusion::arrow::compute::cast(c, t)).collect::<Result<Vec<_>, _>>()?;
        let fields: Vec<Field> = f.arguments.iter().enumerate().map(|(i, t)| Field::new(format!("a{i}"), t.clone(), true)).collect();
        let batch = RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), cast, &RecordBatchOptions::new().with_row_count(Some(rows)))?;
        let mut store = Store::new(engine(), ());
        let instance = self.pre.instantiate(&mut store)?;
        let out = instance.call_call(&mut store, function, &ipc(&batch)?).map_err(|e| anyhow::anyhow!("{}'s {function} stopped: {e:#}", self.name))?;
        let out = out.map_err(|said| coded("38000", format!("{}'s {function}: {said}", self.name)))?;
        let column = StreamReader::try_new(out.as_slice(), None)?.next().context("an answer with no batch")??.columns().first().cloned().context("an answer with no column")?;
        if column.len() != rows || column.data_type() != &f.returns {
            bail!("{}'s {function} answered {} rows of {}, not {rows} of {}", self.name, column.len(), column.data_type(), f.returns);
        }
        Ok(column)
    }
}

/// A batch as Arrow IPC stream bytes, the world's own form.
fn ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut out = vec![];
    let mut w = StreamWriter::try_new(&mut out, &batch.schema())?;
    w.write(batch)?;
    w.finish()?;
    drop(w);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};

    /// `tests/extensions/words`, built by tools/extension_fixture.sh: word_count, shout and boom (and spin and hog, for limits).
    const WORDS: &[u8] = include_bytes!("../tests/fixtures/extensions/words-0.1.wasm");

    fn words() -> Loaded { load(WORDS, "words-test").unwrap() }

    #[test]
    fn a_part_says_what_it_has() {
        let w = words();
        assert_eq!((w.name.as_str(), w.version.as_str()), ("words", "0.1"));
        assert_eq!(w.functions[0], Function { name: "word_count".into(), arguments: vec![DataType::Utf8], returns: DataType::Int64, volatile: false });
        assert_eq!(w.functions.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), ["word_count", "shout", "boom", "spin", "hog"]);
    }

    #[test]
    fn a_scalar_answers_each_row_and_its_nulls() {
        let w = words();
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("a quick test"), None, Some("")]));
        let counted = w.call("word_count", &[text.clone()], 3).unwrap();
        assert_eq!(counted.as_any().downcast_ref::<Int64Array>().unwrap(), &Int64Array::from(vec![Some(3), None, Some(0)]));
        let shouted = w.call("shout", &[text], 3).unwrap();
        assert_eq!(shouted.as_any().downcast_ref::<StringArray>().unwrap(), &StringArray::from(vec![Some("A QUICK TEST"), None, Some("")]));
        // (an argument of another type is cast to the one describe() gave: a view of strings to Utf8)
        let view: ArrayRef = Arc::new(datafusion::arrow::array::StringViewArray::from(vec!["one two"]));
        assert_eq!(w.call("word_count", &[view], 1).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0), 2);
    }

    #[test]
    fn a_functions_error_is_its_own_words_and_the_part_goes_on() {
        let w = words();
        let n: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let e = w.call("boom", &[n], 1).unwrap_err();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("38000", "words's boom: boom always fails".to_string()));
        assert!(w.call("nope", &[], 0).unwrap_err().to_string().contains("words has no function nope"));
        let text: ArrayRef = Arc::new(StringArray::from(vec!["still here"]));
        assert_eq!(w.call("word_count", &[text], 1).unwrap().len(), 1, "a failed call leaves nothing behind");
    }

    #[test]
    fn what_isnt_a_component_is_refused() {
        let e = load(b"\0asm\x01\0\0\0", "not-a-component").unwrap_err();
        assert_eq!(crate::codes::of(&e), "22023", "{e:#}");
    }
}
