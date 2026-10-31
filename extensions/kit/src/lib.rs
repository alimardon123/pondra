//! Write a Pondra extension in Rust (ADR-075, E4). Functions take and give plain Rust values; this crate makes them the
//! WebAssembly component every Pondra 1.x loads: it says what the extension has (`describe()`), reads each batch's Arrow
//! columns into your arguments, writes your answers back, and gives a table function's rows a batch at a time.
//!
//! ```ignore
//! use pondra_extension::Extension;
//!
//! fn hello() -> Extension {
//!     Extension::new("hello", "0.1")
//!         .scalar("greet", |name: String| format!("hello, {name}"))
//!         .scalar("add", |a: i64, b: i64| a + b)
//!         .table("count_to", ["i"], |n: i64| (1..=n).map(|i| (i,)))
//! }
//!
//! pondra_extension::export!(hello);
//! ```
//!
//! Built for `wasm32-unknown-unknown` and made a component with `wasm-tools component new`, it imports nothing but what
//! it uses of Pondra's capabilities (`log`, `clock`): no files, no network, no WASI.
//!
//! - **Types come from the signature:** an argument or an answer is `i64`, `i32`, `f64`, `bool` or `String` (Arrow's
//!   `Int64`, `Int32`, `Float64`, `Boolean`, `Utf8`), or an `Option` of one. A NULL argument gives NULL without calling a
//!   function whose argument isn't an `Option`, as SQL's functions do; `None` answers NULL.
//! - **An error fails the query, in your words:** answer `Result<T, String>`.
//! - **A table function** gives an iterator of tuples, one value per column, named by `table`'s list.
//! - **Deterministic unless you say so:** `volatile()` marks the function before it; one that reads the clock is
//!   volatile whatever it says.
extern crate self as pondra_extension;

use arrow_array::{cast::AsArray, types, Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Field, Schema};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

#[doc(hidden)]
pub use wit_bindgen as __wit_bindgen;

#[doc(hidden)]
pub mod bindings {
    wit_bindgen::generate!({
        path: "../../wit",
        world: "extension-with-tables",
        generate_all,
        pub_export_macro: true,
        export_macro_name: "__export_world",
        default_bindings_module: "pondra_extension::bindings",
        runtime_path: "pondra_extension::__wit_bindgen::rt",
    });
}

/// Writes a line to the node's log, with your extension's name (at most 100 a call).
pub fn log(message: &str) { bindings::pondra::log::log::write(bindings::pondra::log::log::Level::Info, message) }

/// The time: microseconds since 1970-01-01 UTC. A function that calls it is volatile.
pub fn now_micros() -> i64 { bindings::pondra::clock::clock::now() }

/// A value an argument or an answer can be: its Arrow type, read from a column's row and written to a column.
pub trait Value: Sized + 'static {
    /// Its Arrow type, as `describe()` names it.
    const TYPE: &'static str;
    /// The row's value, or None for NULL.
    fn read(column: &dyn Array, row: usize) -> Option<Self>;
    /// A column of these values.
    fn column(values: Vec<Option<Self>>) -> ArrayRef;
}

macro_rules! primitive {
    ($t:ty, $name:literal, $arrow:ty, $array:ty) => {
        impl Value for $t {
            const TYPE: &'static str = $name;
            fn read(column: &dyn Array, row: usize) -> Option<Self> {
                let c = column.as_primitive::<$arrow>();
                c.is_valid(row).then(|| c.value(row))
            }
            fn column(values: Vec<Option<Self>>) -> ArrayRef { Arc::new(<$array>::from(values)) }
        }
    };
}
primitive!(i64, "Int64", types::Int64Type, Int64Array);
primitive!(i32, "Int32", types::Int32Type, Int32Array);
primitive!(f64, "Float64", types::Float64Type, Float64Array);

impl Value for bool {
    const TYPE: &'static str = "Boolean";
    fn read(column: &dyn Array, row: usize) -> Option<Self> {
        let c = column.as_boolean();
        c.is_valid(row).then(|| c.value(row))
    }
    fn column(values: Vec<Option<Self>>) -> ArrayRef { Arc::new(BooleanArray::from(values)) }
}

impl Value for String {
    const TYPE: &'static str = "Utf8";
    fn read(column: &dyn Array, row: usize) -> Option<Self> {
        let c = column.as_string::<i32>();
        c.is_valid(row).then(|| c.value(row).to_string())
    }
    fn column(values: Vec<Option<Self>>) -> ArrayRef { Arc::new(StringArray::from(values)) }
}

/// An argument: a value (a NULL skips the call: the answer is NULL), or an `Option` of one (the call sees the NULL).
pub trait Arg: Sized + 'static {
    const TYPE: &'static str;
    /// Some(the argument), or None when the row's NULL means the function isn't called.
    fn arg(column: &dyn Array, row: usize) -> Option<Self>;
}

impl<T: Value> Arg for T {
    const TYPE: &'static str = T::TYPE;
    fn arg(column: &dyn Array, row: usize) -> Option<Self> { T::read(column, row) }
}

impl<T: Value> Arg for Option<T> {
    const TYPE: &'static str = T::TYPE;
    fn arg(column: &dyn Array, row: usize) -> Option<Self> { Some(T::read(column, row)) }
}

/// An answer: a value, an `Option` of one (None is NULL), or a `Result` whose error fails the query in its words.
pub trait Answer: Sized + 'static {
    type Of: Value;
    fn answer(self) -> Result<Option<Self::Of>, String>;
}

impl<T: Value> Answer for T {
    type Of = T;
    fn answer(self) -> Result<Option<T>, String> { Ok(Some(self)) }
}

impl<T: Value> Answer for Option<T> {
    type Of = T;
    fn answer(self) -> Result<Option<T>, String> { Ok(self) }
}

impl<T: Value> Answer for Result<T, String> {
    type Of = T;
    fn answer(self) -> Result<Option<T>, String> { self.map(Some) }
}

impl<T: Value> Answer for Result<Option<T>, String> {
    type Of = T;
    fn answer(self) -> Result<Option<T>, String> { self }
}

/// A cell of a table function's row: a value, or an `Option` of one (None is NULL).
pub trait Cell: 'static {
    type Of: Value;
    fn cell(self) -> Option<Self::Of>;
}

impl<T: Value> Cell for T {
    type Of = T;
    fn cell(self) -> Option<T> { Some(self) }
}

impl<T: Value> Cell for Option<T> {
    type Of = T;
    fn cell(self) -> Option<T> { self }
}

/// A row of a table function: a tuple, one cell per column.
pub trait Row: Sized + 'static {
    /// Its columns' Arrow types.
    fn types() -> Vec<&'static str>;
    /// Rows as columns.
    fn columns(rows: Vec<Self>) -> Vec<ArrayRef>;
}

/// A function's body, made from a Rust closure: its arguments' types, and how it answers a batch.
type Run = Box<dyn Fn(&[ArrayRef], usize) -> Result<ArrayRef, String>>;
/// A table function's body: its arguments' types, and the batches of its rows for one call.
type Open = Box<dyn Fn(&[ArrayRef]) -> Result<Box<dyn Iterator<Item = Result<RecordBatch, String>>>, String>>;

/// A closure that can be a scalar function: its arguments' types from its signature (`M` tells the arities apart).
pub trait Scalar<M>: 'static {
    fn arguments() -> Vec<&'static str>;
    fn returns() -> &'static str;
    fn run(self) -> Run;
}

/// A closure that can be a table function, with rows of type `R`.
pub trait Table<M>: 'static {
    fn arguments() -> Vec<&'static str>;
    fn types() -> Vec<&'static str>;
    fn open(self, names: Vec<String>) -> Open;
}

/// The rows a table function gives at once, at most.
const BATCH: usize = 1000;

macro_rules! arities {
    ($($a:ident $i:tt),*) => {
        impl<F, R, $($a),*> Scalar<(R, $($a,)*)> for F
        where F: Fn($($a),*) -> R + 'static, R: Answer, $($a: Arg),*
        {
            fn arguments() -> Vec<&'static str> { vec![$($a::TYPE),*] }
            fn returns() -> &'static str { <R::Of as Value>::TYPE }
            #[allow(non_snake_case)]
            fn run(self) -> Run {
                Box::new(move |columns: &[ArrayRef], rows: usize| {
                    let _ = columns; // (none, for a function of no arguments)
                    let mut out = Vec::with_capacity(rows);
                    #[allow(unused_variables)]
                    for row in 0..rows {
                        $(let Some($a) = $a::arg(columns[$i].as_ref(), row) else { out.push(None); continue };)*
                        out.push((self)($($a),*).answer()?);
                    }
                    Ok(<R::Of as Value>::column(out))
                })
            }
        }

        impl<F, I, T, $($a),*> Table<(T, $($a,)*)> for F
        where F: Fn($($a),*) -> I + 'static, I: IntoIterator<Item = T> + 'static, T: Row, $($a: Arg),*
        {
            fn arguments() -> Vec<&'static str> { vec![$($a::TYPE),*] }
            fn types() -> Vec<&'static str> { T::types() }
            #[allow(non_snake_case)]
            fn open(self, names: Vec<String>) -> Open {
                let schema = Arc::new(Schema::new(names.iter().zip(T::types()).map(|(n, t)| Field::new(n, data_type(t), true)).collect::<Vec<_>>()));
                Box::new(move |columns: &[ArrayRef]| {
                    let _ = columns;
                    #[allow(unused_variables)]
                    let rows: Box<dyn Iterator<Item = T>> = match ($($a::arg(columns[$i].as_ref(), 0),)*) {
                        ($(Some($a),)*) => Box::new((self)($($a),*).into_iter()),
                        #[allow(unreachable_patterns)]
                        _ => Box::new(std::iter::empty()), // (a NULL argument: no rows)
                    };
                    let schema = schema.clone();
                    let mut rows = rows.peekable();
                    Ok(Box::new(std::iter::from_fn(move || {
                        rows.peek()?;
                        let batch: Vec<T> = rows.by_ref().take(BATCH).collect();
                        Some(RecordBatch::try_new(schema.clone(), T::columns(batch)).map_err(|e| e.to_string()))
                    })))
                })
            }
        }
    };
}
arities!();
arities!(A 0);
arities!(A 0, B 1);
arities!(A 0, B 1, C 2);
arities!(A 0, B 1, C 2, D 3);

macro_rules! rows {
    ($($t:ident $i:tt),+) => {
        impl<$($t: Cell),+> Row for ($($t,)+) {
            fn types() -> Vec<&'static str> { vec![$(<$t::Of as Value>::TYPE),+] }
            #[allow(non_snake_case)]
            fn columns(rows: Vec<Self>) -> Vec<ArrayRef> {
                let ($(mut $t,)+) = ($(Vec::<Option<$t::Of>>::with_capacity(rows.len()),)+);
                for row in rows {
                    $($t.push(row.$i.cell());)+
                }
                vec![$(<$t::Of as Value>::column($t)),+]
            }
        }
    };
}

rows!(A 0);
rows!(A 0, B 1);
rows!(A 0, B 1, C 2);
rows!(A 0, B 1, C 2, D 3);
rows!(A 0, B 1, C 2, D 3, E 4);
rows!(A 0, B 1, C 2, D 3, E 4, F 5);

fn data_type(name: &str) -> arrow_schema::DataType {
    use arrow_schema::DataType::*;
    match name {
        "Int64" => Int64,
        "Int32" => Int32,
        "Float64" => Float64,
        "Boolean" => Boolean,
        _ => Utf8,
    }
}

/// One function of an extension.
struct Function {
    name: String,
    arguments: Vec<&'static str>,
    returns: Option<&'static str>,
    columns: Vec<(String, &'static str)>,
    volatile: bool,
    run: Option<Run>,
    open: Option<Open>,
}

/// What an extension has: its name, its version, and its functions.
pub struct Extension {
    name: String,
    version: String,
    functions: Vec<Function>,
}

impl Extension {
    /// An extension of this name and version, with no functions yet.
    pub fn new(name: &str, version: &str) -> Self { Extension { name: name.into(), version: version.into(), functions: vec![] } }

    /// A scalar function: SQL calls `name(…)` with one argument per closure parameter, each row once.
    pub fn scalar<M>(mut self, name: &str, f: impl Scalar<M>) -> Self {
        fn types<M, F: Scalar<M>>(_: &F) -> (Vec<&'static str>, &'static str) { (F::arguments(), F::returns()) }
        let (arguments, returns) = types(&f);
        self.functions.push(Function { name: name.into(), arguments, returns: Some(returns), columns: vec![], volatile: false, run: Some(f.run()), open: None });
        self
    }

    /// A table function: `SELECT * FROM name(…)` gives the closure's rows, its tuples' values in the columns named here.
    pub fn table<M, const N: usize>(mut self, name: &str, columns: [&str; N], f: impl Table<M>) -> Self {
        fn types<M, F: Table<M>>(_: &F) -> (Vec<&'static str>, Vec<&'static str>) { (F::arguments(), F::types()) }
        let (arguments, kinds) = types(&f);
        assert_eq!(kinds.len(), N, "{name}: {N} column names for rows of {} values", kinds.len());
        let names: Vec<String> = columns.iter().map(|c| c.to_string()).collect();
        let columns = names.iter().cloned().zip(kinds).collect();
        self.functions.push(Function { name: name.into(), arguments, returns: None, columns, volatile: false, run: None, open: Some(f.open(names)) });
        self
    }

    /// Marks the function added last volatile: called again for each batch, never taken as a constant.
    pub fn volatile(mut self) -> Self {
        if let Some(f) = self.functions.last_mut() {
            f.volatile = true;
        }
        self
    }

    fn describe(&self) -> String {
        let text = |s: &str| format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""));
        let functions: Vec<String> = self.functions.iter().map(|f| {
            let arguments = f.arguments.iter().map(|a| text(a)).collect::<Vec<_>>().join(", ");
            let shape = match f.returns {
                Some(r) => format!("\"returns\": {}", text(r)),
                None => format!("\"columns\": [{}]", f.columns.iter().map(|(n, t)| format!("{{\"name\": {}, \"type\": {}}}", text(n), text(t))).collect::<Vec<_>>().join(", ")),
            };
            format!("{{\"name\": {}, \"arguments\": [{arguments}], {shape}, \"volatile\": {}}}", text(&f.name), f.volatile)
        }).collect();
        format!("{{\"name\": {}, \"version\": {}, \"functions\": [{}]}}", text(&self.name), text(&self.version), functions.join(", "))
    }
}

thread_local! {
    static CALLS: RefCell<(u32, HashMap<u32, Box<dyn Iterator<Item = Result<RecordBatch, String>>>>)> = RefCell::new((0, HashMap::new()));
}

fn read(bytes: &[u8]) -> Result<RecordBatch, String> {
    let mut reader = StreamReader::try_new(bytes, None).map_err(|e| e.to_string())?;
    reader.next().ok_or("an empty batch")?.map_err(|e| e.to_string())
}

fn write(batch: &RecordBatch) -> Result<Vec<u8>, String> {
    let mut out = vec![];
    let mut writer = StreamWriter::try_new(&mut out, &batch.schema()).map_err(|e| e.to_string())?;
    writer.write(batch).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    drop(writer);
    Ok(out)
}

/// What `export!` calls: the world's exports, answered from your extension.
#[doc(hidden)]
pub mod run {
    use super::*;

    pub fn describe(e: &Extension) -> String { e.describe() }

    pub fn call(e: &Extension, name: &str, batch: &[u8]) -> Result<Vec<u8>, String> {
        let f = e.functions.iter().find(|f| f.name == name).and_then(|f| f.run.as_ref()).ok_or_else(|| format!("{} has no function {name}", e.name))?;
        let batch = read(batch)?;
        let column = f(batch.columns(), batch.num_rows())?;
        let schema = Arc::new(Schema::new(vec![Field::new("out", column.data_type().clone(), true)]));
        write(&RecordBatch::try_new(schema, vec![column]).map_err(|e| e.to_string())?)
    }

    pub fn open(e: &Extension, name: &str, arguments: &[u8]) -> Result<u32, String> {
        let f = e.functions.iter().find(|f| f.name == name).and_then(|f| f.open.as_ref()).ok_or_else(|| format!("{} has no table function {name}", e.name))?;
        let rows = f(read(arguments)?.columns())?;
        Ok(CALLS.with(|c| {
            let (next, calls) = &mut *c.borrow_mut();
            *next += 1;
            calls.insert(*next, rows);
            *next
        }))
    }

    pub fn next(handle: u32) -> Result<Option<Vec<u8>>, String> {
        let batch = CALLS.with(|c| c.borrow_mut().1.get_mut(&handle).map(|rows| rows.next()));
        match batch {
            None => Err(format!("no call {handle}")),
            Some(None) => Ok(None),
            Some(Some(batch)) => write(&batch?).map(Some),
        }
    }

    pub fn close(handle: u32) { CALLS.with(|c| c.borrow_mut().1.remove(&handle)); }
}

/// Makes the function `$make` (`fn() -> Extension`) your extension: the component's exports, answered from it.
#[macro_export]
macro_rules! export {
    ($make:path) => {
        #[doc(hidden)]
        struct __PondraExtension;

        impl __PondraExtension {
            fn with<T>(f: impl FnOnce(&$crate::Extension) -> T) -> T {
                thread_local! { static MADE: $crate::Extension = $make(); }
                MADE.with(f)
            }
        }

        impl $crate::bindings::Guest for __PondraExtension {
            fn describe() -> String { Self::with($crate::run::describe) }
            fn call(name: String, batch: Vec<u8>) -> Result<Vec<u8>, String> { Self::with(|e| $crate::run::call(e, &name, &batch)) }
            fn open(name: String, arguments: Vec<u8>) -> Result<u32, String> { Self::with(|e| $crate::run::open(e, &name, &arguments)) }
            fn next(handle: u32) -> Result<Option<Vec<u8>>, String> { $crate::run::next(handle) }
            fn close(handle: u32) { $crate::run::close(handle) }
        }

        $crate::bindings::__export_world!(__PondraExtension);
    };
}
