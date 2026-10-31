//! `words`: the extension the host's tests load. `word_count(text)` counts a text's words, `shout(text)` writes it in capitals,
//! and `boom(n)` always fails, saying so. Two misbehave, for the host's limits: `spin(n)` never ends, and `hog(n)` takes n MB. Built for `wasm32-unknown-unknown` and made a component (tools/extension_fixture.sh),
//! so it imports nothing.
use arrow_array::{cast::AsArray, Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

wit_bindgen::generate!({ path: "../../../wit", world: "extension" });

struct Words;

const DESCRIBE: &str = r#"{"name": "words", "version": "0.1", "functions": [
  {"name": "word_count", "arguments": ["Utf8"], "returns": "Int64", "volatile": false},
  {"name": "shout", "arguments": ["Utf8"], "returns": "Utf8", "volatile": false},
  {"name": "boom", "arguments": ["Int64"], "returns": "Int64", "volatile": false},
  {"name": "spin", "arguments": ["Int64"], "returns": "Int64", "volatile": true},
  {"name": "hog", "arguments": ["Int64"], "returns": "Int64", "volatile": true}
]}"#;

impl Guest for Words {
    fn describe() -> String {
        DESCRIBE.to_string()
    }

    fn call(name: String, batch: Vec<u8>) -> Result<Vec<u8>, String> {
        let batch = read(&batch)?;
        let column = batch.columns().first().ok_or("a call has one column per argument")?;
        let out: ArrayRef = match name.as_str() {
            "word_count" => Arc::new(column.as_string::<i32>().iter().map(|v| v.map(|v| v.split_whitespace().count() as i64)).collect::<Int64Array>()),
            "shout" => Arc::new(column.as_string::<i32>().iter().map(|v| v.map(str::to_uppercase)).collect::<StringArray>()),
            "boom" => return Err("boom always fails".to_string()),
            "spin" => loop {
                std::hint::black_box(column.len()); // (never ends: the host's time limit stops it)
            },
            "hog" => {
                let mb = column.as_primitive::<arrow_array::types::Int64Type>().iter().flatten().max().unwrap_or(0) as usize;
                let held = std::hint::black_box(vec![1u8; mb << 20]); // (the host's memory limit stops it first)
                Arc::new(Int64Array::from(vec![held.len() as i64; column.len()]))
            }
            _ => return Err(format!("words has no function {name}")),
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

export!(Words);
