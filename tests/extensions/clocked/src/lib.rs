//! `clocked`: the extension the host's tests load for the capabilities 1.0 offers (E5). `now_us(n)` is the clock's time for
//! each row (`pondra:clock`), `say(text)` writes each text to the node's log at info and gives it back, and `chatty(n)` writes
//! n lines (`pondra:log`). Its `describe()` calls `now_us` deterministic: the host makes it volatile, since the part imports
//! the clock.
use arrow_array::{cast::AsArray, types::Int64Type, Array, ArrayRef, Int64Array, RecordBatch};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

wit_bindgen::generate!({ path: "../../../wit", world: "extension", generate_all });

use pondra::clock::clock;
use pondra::log::log::{self, Level};

struct Clocked;

const DESCRIBE: &str = r#"{"name": "clocked", "version": "0.1", "functions": [
  {"name": "now_us", "arguments": ["Int64"], "returns": "Int64", "volatile": false},
  {"name": "say", "arguments": ["Utf8"], "returns": "Utf8", "volatile": false},
  {"name": "chatty", "arguments": ["Int64"], "returns": "Int64", "volatile": false}
]}"#;

impl Guest for Clocked {
    fn describe() -> String {
        DESCRIBE.to_string()
    }

    fn call(name: String, batch: Vec<u8>) -> Result<Vec<u8>, String> {
        let batch = read(&batch)?;
        let column = batch.columns().first().ok_or("a call has one column per argument")?.clone();
        let out: ArrayRef = match name.as_str() {
            "now_us" => Arc::new(Int64Array::from(vec![clock::now(); column.len()])),
            "say" => {
                for text in column.as_string::<i32>().iter().flatten() {
                    log::write(Level::Info, text);
                }
                column
            }
            "chatty" => {
                let n = column.as_primitive::<Int64Type>().iter().flatten().max().unwrap_or(0);
                for i in 0..n {
                    log::write(Level::Debug, &format!("line {i}"));
                }
                Arc::new(Int64Array::from(vec![n; column.len()]))
            }
            _ => return Err(format!("clocked has no function {name}")),
        };
        write(out)
    }
}

fn read(bytes: &[u8]) -> Result<RecordBatch, String> {
    let mut reader = StreamReader::try_new(bytes, None).map_err(|e| e.to_string())?;
    reader.next().ok_or("an empty batch")?.map_err(|e| e.to_string())
}

fn write(column: ArrayRef) -> Result<Vec<u8>, String> {
    let schema = Arc::new(Schema::new(vec![Field::new("out", column.data_type().clone(), true)]));
    let batch = RecordBatch::try_new(schema.clone(), vec![column]).map_err(|e| e.to_string())?;
    let mut out = vec![];
    let mut writer = StreamWriter::try_new(&mut out, &schema).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    drop(writer);
    Ok(out)
}

export!(Clocked);
