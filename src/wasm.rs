//! The host of extensions' WebAssembly parts (ADR-075 §2, E2a in designs/extensions-before-1-0.md): Wasmtime with the
//! component model, calling the world in `wit/extension.wit`. A part is compiled once per Wasmtime release and kept on disk
//! by its SHA-256 (`compiled`), and each call runs in a store of its own, so no call sees another's memory and a part holds
//! nothing between batches. A part imports only the capabilities this Pondra offers (`OFFERED`: the node's log and the
//! clock, E5); one that imports any other is refused by the import's name when it is loaded (ADR-075 §3). Arguments and
//! answers cross as Arrow IPC.
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

/// The world of an extension with table functions too (`open`, `next`, `close`): asked for only when `describe()` names one.
mod tables {
    wasmtime::component::bindgen!({ path: "wit", world: "extension-with-tables", with: {
        "pondra:log/log": super::pondra::log::log,
        "pondra:clock/clock": super::pondra::clock::clock,
    } });
}

/// The capabilities this Pondra offers an extension's part, each a package versioned on its own (`wit/deps`). A part that
/// imports any other is refused by its name (`load`); an import's `@2`, if one comes, is offered beside its `@1`.
pub const OFFERED: [&str; 2] = ["pondra:log/log@1.0.0", "pondra:clock/clock@1.0.0"];

/// The clock: every function of a part that imports it is volatile, whatever its `describe()` says (ADR-075 §2), so a
/// spread query never takes two nodes' times as one answer.
const CLOCK: &str = "pondra:clock/clock@1.0.0";

/// The lines a call may write to the node's log (`pondra:log`); past them it writes one line saying how many more it had.
const LINES: u32 = 100;

/// One function an extension has, as its `describe()` says: a scalar's answer is `returns`; a table function's rows have
/// `columns` (and its `returns` is Null).
#[derive(Debug, Clone, PartialEq)]
pub struct Function {
    pub name: String,
    pub arguments: Vec<DataType>,
    pub returns: DataType,
    pub volatile: bool,
    pub columns: Vec<(String, DataType)>,
}

impl Function {
    pub fn is_table(&self) -> bool { !self.columns.is_empty() }

    /// A table function's rows' schema.
    pub fn schema(&self) -> Arc<Schema> { Arc::new(Schema::new(self.columns.iter().map(|(n, t)| Field::new(n, t.clone(), true)).collect::<Vec<_>>())) }
}

/// An extension's WebAssembly part, compiled and ready to call: its name and version, and its functions.
pub struct Loaded {
    pub name: String,
    pub version: String,
    pub functions: Vec<Function>,
    pre: ExtensionPre<Held>,
    tables: Option<tables::ExtensionWithTablesPre<Held>>,
}

impl std::fmt::Debug for Loaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "{} {}", self.name, self.version) }
}

/// How often the engine's epoch ticks: a call past its time is stopped within a tick of it, at its next loop or call.
const TICK: std::time::Duration = std::time::Duration::from_millis(10);

/// The process's Wasmtime: compiled code is the engine's, so one is shared by every node in a process. Its epoch ticks on a
/// thread of its own (`TICK`), which is how a call that runs past its time is stopped (epoch interruption, E2c).
fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| {
        let mut config = Config::new();
        config.wasm_component_model(true);
        config.epoch_interruption(true);
        let engine = Engine::new(&config).expect("Wasmtime's engine");
        let ticking = engine.weak();
        std::thread::Builder::new()
            .name("pondra-wasm-epoch".into())
            .spawn(move || {
                while let Some(engine) = ticking.upgrade() {
                    engine.increment_epoch();
                    drop(engine);
                    std::thread::sleep(TICK);
                }
            })
            .expect("the extensions' clock thread");
        engine
    })
}

/// A call's limits: its part's memory (`PONDRA_EXTENSION_MB`, 256), and its time for one batch (`PONDRA_EXTENSION_SECS`, 30),
/// each the node's admin's, read when the call starts (`now`).
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub mb: usize,
    pub secs: u64,
}

impl Limits {
    pub fn now() -> Self { Limits { mb: crate::env::or("PONDRA_EXTENSION_MB", 256usize), secs: crate::env::or("PONDRA_EXTENSION_SECS", 30u64).max(1) } }
}

/// What a call's store holds: its memory's limit and whether the call reached it, and for its log lines the extension's name
/// and how many it wrote.
struct Held {
    max_bytes: usize,
    /// The bytes of every linear memory the call's part has grown, and of its tables' elements, together.
    memory: usize,
    full: bool,
    who: String,
    lines: u32,
}

impl pondra::log::log::Host for Held {
    fn write(&mut self, level: pondra::log::log::Level, message: String) {
        use pondra::log::log::Level;
        self.lines += 1;
        if self.lines > LINES {
            return; // (counted: said once, when the call ends)
        }
        let level = match level {
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        };
        let cut: String = message.chars().take(1000).collect::<String>().replace(['\n', '\r'], " ");
        said(format!("extension {}: {level}: {cut}", self.who));
    }
}

impl pondra::clock::clock::Host for Held {
    fn now(&mut self) -> i64 { crate::log::now_us() as i64 } // (the node's clock, in microseconds since 1970)
}

impl Drop for Held {
    fn drop(&mut self) {
        if self.lines > LINES {
            said(format!("extension {}: {} more lines not written (a call writes at most {LINES})", self.who, self.lines - LINES));
        }
    }
}

/// A line an extension wrote, to the node's log (in a test, kept for it too).
fn said(line: String) {
    #[cfg(test)]
    SAID.with(|s| s.borrow_mut().push(line.clone()));
    eprintln!("{line}");
}

#[cfg(test)]
thread_local! {
    /// The lines extensions wrote on this thread, for the tests to read.
    static SAID: std::cell::RefCell<Vec<String>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// A call's limits over every memory and instance its part makes: the memories' bytes summed (a component may have several
/// linear memories, each of which was allowed the whole limit: audit 11-01 M2), and few instances, memories and tables.
impl wasmtime::ResourceLimiter for Held {
    fn memory_growing(&mut self, current: usize, desired: usize, _: Option<usize>) -> wasmtime::Result<bool> {
        let total = self.memory.saturating_add(desired.saturating_sub(current));
        if total > self.max_bytes {
            self.full = true; // (said by `stopped`: the call fails, the node goes on)
            wasmtime::bail!("an extension's memory past {} MB", self.max_bytes >> 20);
        }
        self.memory = total;
        Ok(true)
    }
    /// A table grows to 2^20 elements at most, and its elements count in the call's bytes, a pointer each (audit 11-02 L1:
    /// 64 tables of 2^20 held 512 MB past the limit).
    fn table_growing(&mut self, current: usize, desired: usize, _: Option<usize>) -> wasmtime::Result<bool> {
        if desired > 1 << 20 {
            return Ok(false);
        }
        let total = self.memory.saturating_add(desired.saturating_sub(current).saturating_mul(size_of::<usize>()));
        if total > self.max_bytes {
            self.full = true;
            wasmtime::bail!("an extension's tables past {} MB", self.max_bytes >> 20);
        }
        self.memory = total;
        Ok(true)
    }
    fn instances(&self) -> usize { 64 }
    fn memories(&self) -> usize { 16 }
    fn tables(&self) -> usize { 64 }
}

/// A store for one call of `who`'s part: nothing kept from another, its limits set.
fn store(limits: Limits, who: &str) -> Store<Held> {
    let mut store = Store::new(engine(), Held { max_bytes: limits.mb << 20, memory: 0, full: false, who: who.to_string(), lines: 0 });
    store.limiter(|held| held);
    store.set_epoch_deadline(limits.secs * 1000 / TICK.as_millis() as u64);
    store
}

/// A call that stopped, said by why: past its time (57014, as a cancelled statement is), past its memory (53200, out of
/// memory), or a trap of its own (38000, as its own error is).
fn stopped(store: &Store<Held>, limits: Limits, name: &str, function: &str, e: wasmtime::Error) -> anyhow::Error {
    if matches!(e.downcast_ref::<wasmtime::Trap>(), Some(wasmtime::Trap::Interrupt)) {
        return coded("57014", format!("{name}'s {function} ran past {} s, its time for one batch (PONDRA_EXTENSION_SECS)", limits.secs));
    }
    if store.data().full {
        return coded("53200", format!("{name}'s {function} passed {} MB, an extension's memory (PONDRA_EXTENSION_MB)", limits.mb));
    }
    coded("38000", format!("{name}'s {function} stopped: {e:#}"))
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
    // (a hash names the cache's file, and only a hash: a record's `sha256` is read from a bucket, and a path in it would load
    // a file elsewhere on this disk as native code. `extension::part` refuses such a record before this; anything else
    // that isn't a hash is compiled, and not cached)
    let file = cache().filter(|_| crate::extension::sha_ok(sha256)).map(|dir| dir.join(format!("{sha256}.cwasm")));
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

/// Loads a part from its bytes: compiled (or read from the cache), linked against what this Pondra offers (`OFFERED`), and
/// asked what it has. A part that imports anything else is refused by the import's name (`needs pondra:http/http@1.0.0,
/// which this Pondra doesn't offer …`, 0A000: the caller names the extension); one whose `describe()` can't be read is
/// refused saying why. Every function of a part that imports the clock is volatile.
pub fn load(bytes: &[u8], sha256: &str) -> Result<Loaded> {
    let component = compiled(bytes, sha256)?;
    let imports: Vec<String> = component.component_type().imports(engine()).map(|(import, _)| import.to_string()).collect();
    if let Some(missing) = imports.iter().find(|i| !OFFERED.contains(&i.as_str())) {
        return Err(coded("0A000", format!("needs {missing}, which this Pondra doesn't offer (it offers {})", OFFERED.join(" and "))));
    }
    let mut linker: Linker<Held> = Linker::new(engine());
    Extension::add_to_linker::<Held, wasmtime::component::HasSelf<Held>>(&mut linker, |held| held)?;
    let instance_pre = linker.instantiate_pre(&component)?;
    let pre = ExtensionPre::new(instance_pre.clone())?;
    let limits = Limits::now();
    let mut store = store(limits, "(loading)");
    let said = match pre.instantiate(&mut store).and_then(|i| i.call_describe(&mut store)) {
        Ok(said) => said,
        Err(e) => return Err(stopped(&store, limits, "the extension", "describe()", e)),
    };
    let (name, version, mut functions) = described(&said)?;
    if imports.iter().any(|i| i == CLOCK) {
        functions.iter_mut().for_each(|f| f.volatile = true);
    }
    let tables = match functions.iter().filter(|f| f.is_table()).map(|f| f.name.as_str()).collect::<Vec<_>>() {
        none if none.is_empty() => None,
        named => Some(tables::ExtensionWithTablesPre::new(instance_pre).map_err(|e| {
            coded("22023", format!("{name}'s describe() names table functions ({}), but it doesn't export open, next and close: {e:#}", named.join(", ")))
        })?),
    };
    Ok(Loaded { name, version, functions, pre, tables })
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
        let mut columns = vec![];
        for c in f["columns"].as_array().map(Vec::as_slice).unwrap_or_default() {
            columns.push((text(c, "name")?, ty(&c["type"])?));
        }
        let returns = if columns.is_empty() { ty(&f["returns"])? } else { DataType::Null };
        functions.push(Function { name: text(f, "name")?, arguments, returns, volatile: f["volatile"].as_bool().unwrap_or(false), columns });
    }
    Ok((text(&v, "name")?, text(&v, "version")?, functions))
}

impl Loaded {
    /// Calls `function` over `columns`, one per argument, cast to the types `describe()` gave: its one column of answers, as
    /// many as the rows. The function's own error, a trap or an answer of the wrong type or length fails the call with its
    /// words.
    pub fn call(&self, function: &str, columns: &[ArrayRef], rows: usize) -> Result<ArrayRef> { self.call_with(function, columns, rows, Limits::now()) }

    /// `call`, within `limits`.
    pub fn call_with(&self, function: &str, columns: &[ArrayRef], rows: usize, limits: Limits) -> Result<ArrayRef> {
        let f = self.functions.iter().find(|f| f.name == function).with_context(|| format!("{} has no function {function}", self.name))?;
        let batch = arguments(f, columns, rows)?;
        let mut store = store(limits, &self.name);
        let out = match self.pre.instantiate(&mut store).and_then(|i| i.call_call(&mut store, function, &batch)) {
            Ok(out) => out,
            Err(e) => return Err(stopped(&store, limits, &self.name, function, e)),
        };
        let out = out.map_err(|said| coded("38000", format!("{}'s {function}: {said}", self.name)))?;
        let column = StreamReader::try_new(out.as_slice(), None)?.next().context("an answer with no batch")??.columns().first().cloned().context("an answer with no column")?;
        if column.len() != rows || column.data_type() != &f.returns {
            bail!("{}'s {function} answered {} rows of {}, not {rows} of {}", self.name, column.len(), column.data_type(), f.returns);
        }
        Ok(column)
    }
}

impl Loaded {
    /// Starts a call of the table function `function` with its arguments, one value each, cast to the types `describe()`
    /// gave: its rows, read a batch at a time (`Rows::next`). The call keeps its store until it is dropped, within `limits`:
    /// its memory for the whole call, its time for each batch.
    pub fn rows(&self, function: &str, values: &[ArrayRef], limits: Limits) -> Result<Rows> {
        let f = self.functions.iter().find(|f| f.name == function && f.is_table()).with_context(|| format!("{} has no table function {function}", self.name))?;
        let pre = self.tables.as_ref().with_context(|| format!("{} has no table function {function}", self.name))?;
        let (bytes, mut store) = (arguments(f, values, 1)?, store(limits, &self.name));
        let opened = pre.instantiate(&mut store).and_then(|i| Ok((i.call_open(&mut store, function, &bytes)?, i)));
        let (handle, instance) = match opened {
            Ok((handle, instance)) => (handle.map_err(|said| coded("38000", format!("{}'s {function}: {said}", self.name)))?, instance),
            Err(e) => return Err(stopped(&store, limits, &self.name, function, e)),
        };
        Ok(Rows { store, instance, handle, name: self.name.clone(), function: function.to_string(), schema: f.schema(), limits, open: true })
    }
}

/// A call's arguments as the part reads them: `rows` rows of one column each, cast to the types `describe()` gave, in Arrow IPC.
fn arguments(f: &Function, columns: &[ArrayRef], rows: usize) -> Result<Vec<u8>> {
    if columns.len() != f.arguments.len() {
        bail!("{} takes {} arguments, not {}", f.name, f.arguments.len(), columns.len());
    }
    let cast = columns.iter().zip(&f.arguments).map(|(c, t)| datafusion::arrow::compute::cast(c, t)).collect::<Result<Vec<_>, _>>()?;
    let fields: Vec<Field> = f.arguments.iter().enumerate().map(|(i, t)| Field::new(format!("a{i}"), t.clone(), true)).collect();
    ipc(&RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), cast, &RecordBatchOptions::new().with_row_count(Some(rows)))?)
}

/// A table function's call in progress: its store, held until its rows are read or it is dropped.
pub struct Rows {
    store: Store<Held>,
    instance: tables::ExtensionWithTables,
    handle: u32,
    name: String,
    function: String,
    schema: Arc<Schema>,
    limits: Limits,
    open: bool,
}

impl Rows {
    /// The next batch, in the columns `describe()` gave, or None once the call has given them all. A batch of other columns
    /// fails the call, as a trap or the function's own error does.
    pub fn next(&mut self) -> Result<Option<RecordBatch>> {
        if !self.open {
            return Ok(None);
        }
        self.store.set_epoch_deadline(self.limits.secs * 1000 / TICK.as_millis() as u64); // (its time is each batch's)
        let out = match self.instance.call_next(&mut self.store, self.handle) {
            Ok(out) => out,
            Err(e) => {
                self.open = false; // (a trapped instance is called no more)
                return Err(stopped(&self.store, self.limits, &self.name, &self.function, e));
            }
        };
        let Some(bytes) = out.map_err(|said| coded("38000", format!("{}'s {}: {said}", self.name, self.function)))? else {
            self.close();
            return Ok(None);
        };
        let got = StreamReader::try_new(bytes.as_slice(), None)?.next().context("a batch with no rows")??;
        let types = |s: &Schema| s.fields().iter().map(|f| f.data_type().clone()).collect::<Vec<_>>();
        if types(&got.schema()) != types(&self.schema) {
            bail!("{}'s {} gave columns of {:?}, not {:?}", self.name, self.function, types(&got.schema()), types(&self.schema));
        }
        Ok(Some(RecordBatch::try_new(self.schema.clone(), got.columns().to_vec())?)) // (its names as describe() gave them)
    }

    fn close(&mut self) {
        if std::mem::take(&mut self.open) {
            self.store.set_epoch_deadline(1000 / TICK.as_millis() as u64); // (a second to let go)
            let _ = self.instance.call_close(&mut self.store, self.handle);
        }
    }
}

impl Drop for Rows {
    fn drop(&mut self) { self.close() } // (a query that stops reading lets the call go)
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

    /// `tests/extensions/words`, built by tools/extension_fixture.sh: word_count, shout and boom (spin and hog, for limits;
    /// words_of and series, tables).
    const WORDS: &[u8] = include_bytes!("../tests/fixtures/extensions/words-0.1.wasm");

    fn words() -> Loaded { load(WORDS, "words-test").unwrap() }

    #[test]
    fn a_part_says_what_it_has() {
        let w = words();
        assert_eq!((w.name.as_str(), w.version.as_str()), ("words", "0.1"));
        assert_eq!(w.functions[0], Function { name: "word_count".into(), arguments: vec![DataType::Utf8], returns: DataType::Int64, volatile: false, columns: vec![] });
        assert_eq!(w.functions[5].columns, [("word".to_string(), DataType::Utf8), ("place".to_string(), DataType::Int64)], "a table function's columns");
        assert_eq!(w.functions.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), ["word_count", "shout", "boom", "spin", "hog", "words_of", "series"]);
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

    /// A runaway call is stopped at its time, and one that takes too much memory at its limit, each failing alone with its
    /// code; the part answers the next call.
    #[test]
    fn a_runaway_or_a_hog_fails_alone() {
        let w = words();
        let n: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let started = std::time::Instant::now();
        let e = w.call_with("spin", &[n.clone()], 1, Limits { mb: 256, secs: 1 }).unwrap_err();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("57014", "words's spin ran past 1 s, its time for one batch (PONDRA_EXTENSION_SECS)".to_string()));
        assert!(started.elapsed() < std::time::Duration::from_secs(5), "stopped at its time: {:?}", started.elapsed());
        let big: ArrayRef = Arc::new(Int64Array::from(vec![64]));
        let e = w.call_with("hog", &[big.clone()], 1, Limits { mb: 32, secs: 30 }).unwrap_err();
        assert_eq!((crate::codes::of(&e), format!("{e:#}")), ("53200", "words's hog passed 32 MB, an extension's memory (PONDRA_EXTENSION_MB)".to_string()));
        assert_eq!(w.call("hog", &[big], 1).unwrap().as_any().downcast_ref::<Int64Array>().unwrap().value(0), 64 << 20, "under the default 256 MB it answers");
        let text: ArrayRef = Arc::new(StringArray::from(vec!["still here"]));
        assert_eq!(w.call("word_count", &[text], 1).unwrap().len(), 1, "the part goes on");
    }

    #[test]
    fn tables_grow_in_the_calls_bytes() {
        use wasmtime::ResourceLimiter;
        let mut held = Held { max_bytes: 4 << 20, memory: 0, full: false, who: "t".into(), lines: 0 };
        assert!(held.memory_growing(0, 1 << 20, None).unwrap());
        assert!(held.table_growing(0, 1 << 18, None).unwrap(), "2 MB of elements beside 1 MB of memory");
        assert_eq!(held.memory, (1 << 20) + (1 << 18) * size_of::<usize>());
        assert!(!held.table_growing(0, (1 << 20) + 1, None).unwrap(), "past 2^20 elements a table doesn't grow");
        assert!(held.table_growing(0, 1 << 18, None).is_err() && held.full, "another 2 MB passes the 4 MB");
    }

    /// A table function's rows come a batch at a time, in its columns, until there are none; a call let go before its end
    /// leaves the part as it was.
    #[test]
    fn a_table_function_gives_its_rows_a_batch_at_a_time() {
        use datafusion::arrow::array::AsArray;
        let w = words();
        let n: ArrayRef = Arc::new(Int64Array::from(vec![2500]));
        let mut rows = w.rows("series", &[n.clone()], Limits::now()).unwrap();
        let (mut batches, mut sum) = (0, 0i64);
        while let Some(b) = rows.next().unwrap() {
            assert_eq!(b.schema().field(0).name(), "i");
            batches += 1;
            sum += b.column(0).as_primitive::<datafusion::arrow::datatypes::Int64Type>().iter().flatten().sum::<i64>();
        }
        assert_eq!((batches, sum), (3, 2500 * 2501 / 2), "1,000 a batch");
        assert!(rows.next().unwrap().is_none(), "and none after its end");
        let text: ArrayRef = Arc::new(StringArray::from(vec!["to be or not"]));
        let got = w.rows("words_of", &[text], Limits::now()).unwrap().next().unwrap().unwrap();
        assert_eq!((got.num_rows(), got.column(0).as_string::<i32>().value(3)), (4, "not"));
        drop(w.rows("series", &[n], Limits::now()).unwrap()); // (let go unread)
        assert!(w.rows("word_count", &[], Limits::now()).err().unwrap().to_string().contains("no table function word_count"));
    }

    /// `tests/extensions/clocked`, built by tools/extension_fixture.sh: the capabilities 1.0 offers, the clock and the log.
    const CLOCKED: &[u8] = include_bytes!("../tests/fixtures/extensions/clocked-0.1.wasm");

    /// `tests/extensions/needs-http.wat`: a part that needs a capability 1.0 doesn't offer, as one built for a later Pondra may.
    const NEEDS_HTTP: &[u8] = include_bytes!("../tests/fixtures/extensions/needs-http.wasm");

    fn said() -> Vec<String> { SAID.with(|s| std::mem::take(&mut *s.borrow_mut())) }

    /// A part may import the clock and the node's log (E5): its functions answer with the time and write lines, at most 100 a
    /// call and then one counting the rest; every function of a part that imports the clock is volatile, whatever its
    /// `describe()` says. One that needs a capability this Pondra doesn't offer is refused by the import's name, and one
    /// built before either came (words) loads as it did.
    #[test]
    fn a_part_imports_the_clock_and_the_log_and_nothing_else() {
        let c = load(CLOCKED, "clocked-test").unwrap();
        assert!(c.functions.iter().all(|f| f.volatile), "the clock makes each volatile: {:?}", c.functions);
        let micros = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros() as i64;
        let before = micros();
        let now = c.call("now_us", &[Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef], 2).unwrap();
        let now = now.as_any().downcast_ref::<Int64Array>().unwrap().value(1);
        assert!((before..=micros()).contains(&now), "{before} <= {now}");
        said();
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some("hello\nthere"), None, Some("again")]));
        assert_eq!(c.call("say", &[text], 3).unwrap().len(), 3);
        assert_eq!(said(), ["extension clocked: info: hello there", "extension clocked: info: again"]);
        c.call("chatty", &[Arc::new(Int64Array::from(vec![150])) as ArrayRef], 1).unwrap();
        let lines = said();
        assert_eq!((lines.len(), lines[99].as_str()), (101, "extension clocked: debug: line 99"));
        assert_eq!(lines[100], "extension clocked: 50 more lines not written (a call writes at most 100)");
        assert!(!words().functions[0].volatile, "a part without the clock keeps describe()'s word");
        let e = load(NEEDS_HTTP, "needs-http-test").unwrap_err();
        assert_eq!((crate::codes::of(&e), e.to_string().as_str()), ("0A000", "needs pondra:http/http@1.0.0, which this Pondra doesn't offer (it offers pondra:log/log@1.0.0 and pondra:clock/clock@1.0.0)"));
    }

    /// `extensions/hello`, written with the kit (`extensions/kit`, E4): its functions over plain Rust values.
    const HELLO: &[u8] = include_bytes!("../tests/fixtures/extensions/hello-0.1.wasm");

    /// The kit takes each function's types from its Rust signature; a NULL skips a function whose argument is a plain type
    /// and reaches one whose argument is an Option; an Err fails the call in its words; a table function's tuples come a
    /// thousand rows a batch.
    #[test]
    fn the_kits_functions_take_their_types_from_their_signatures() {
        use datafusion::arrow::array::AsArray;
        let h = load(HELLO, "hello-test").unwrap();
        let shape = |n: &str| h.functions.iter().find(|f| f.name == n).map(|f| (f.arguments.clone(), f.returns.clone(), f.columns.clone(), f.volatile)).unwrap();
        assert_eq!(shape("greet"), (vec![DataType::Utf8], DataType::Utf8, vec![], false));
        assert_eq!(shape("add"), (vec![DataType::Int64, DataType::Int64], DataType::Int64, vec![], false));
        let int = DataType::Int64;
        assert_eq!(shape("count_to"), (vec![int.clone()], DataType::Null, vec![("i".to_string(), int.clone()), ("square".to_string(), int)], false));
        let names: ArrayRef = Arc::new(StringArray::from(vec![Some("Ada Lovelace"), None]));
        let strings = |a: ArrayRef| a.as_string::<i32>().iter().map(|v| v.map(str::to_string)).collect::<Vec<_>>();
        assert_eq!(strings(h.call("greet", &[names.clone()], 2).unwrap()), [Some("hello, Ada Lovelace".to_string()), None]);
        assert_eq!(strings(h.call("initials", &[names], 2).unwrap()), [Some("AL".to_string()), None]);
        let (seven, zero): (ArrayRef, ArrayRef) = (Arc::new(Int64Array::from(vec![7])), Arc::new(Int64Array::from(vec![0])));
        let e = h.call("checked_div", &[seven, zero], 1).unwrap_err();
        assert_eq!((crate::codes::of(&e), e.to_string().as_str()), ("38000", "hello's checked_div: checked_div: division by zero"));
        let mut rows = h.rows("count_to", &[Arc::new(Int64Array::from(vec![2500])) as ArrayRef], Limits::now()).unwrap();
        let (mut batches, mut squares) = (0, 0i64);
        while let Some(b) = rows.next().unwrap() {
            batches += 1;
            squares += b.column(1).as_primitive::<datafusion::arrow::datatypes::Int64Type>().iter().flatten().sum::<i64>();
        }
        assert_eq!((batches, squares), (3, 2500 * 2501 * 5001 / 6));
    }

    #[test]
    fn what_isnt_a_component_is_refused() {
        let e = load(b"\0asm\x01\0\0\0", "not-a-component").unwrap_err();
        assert_eq!(crate::codes::of(&e), "22023", "{e:#}");
    }
}
