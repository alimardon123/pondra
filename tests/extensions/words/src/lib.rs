//! `words`: the extension the host's tests load. `word_count(text)` counts a text's words, `shout(text)` writes it in capitals,
//! and `boom(n)` always fails, saying so. Two misbehave, for the host's limits: `spin(n)` never ends, and `hog(n)` takes n MB.
//! Two are tables: `words_of(text)` is a row per word with its place, and `series(n)` is 1 to n in batches of 1,000. Built for `wasm32-unknown-unknown` and made a component (tools/extension_fixture.sh),
//! so it imports nothing.
use arrow_array::{cast::AsArray, Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::StreamReader, writer::StreamWriter};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

wit_bindgen::generate!({ path: "../../../wit", world: "extension-with-tables" });

struct Words;

const DESCRIBE: &str = r#"{"name": "words", "version": "0.1", "functions": [
  {"name": "word_count", "arguments": ["Utf8"], "returns": "Int64", "volatile": false},
  {"name": "shout", "arguments": ["Utf8"], "returns": "Utf8", "volatile": false},
  {"name": "boom", "arguments": ["Int64"], "returns": "Int64", "volatile": false},
  {"name": "spin", "arguments": ["Int64"], "returns": "Int64", "volatile": true},
  {"name": "hog", "arguments": ["Int64"], "returns": "Int64", "volatile": true},
  {"name": "words_of", "arguments": ["Utf8"], "columns": [{"name": "word", "type": "Utf8"}, {"name": "place", "type": "Int64"}]},
  {"name": "series", "arguments": ["Int64"], "columns": [{"name": "i", "type": "Int64"}]}
]}"#;

/// A table function's call in progress: the rows still to give, as batches.
enum Rows {
    Words(Vec<String>),
    Series { next: i64, last: i64 },
}

thread_local! {
    static CALLS: std::cell::RefCell<(u32, std::collections::HashMap<u32, Rows>)> = Default::default();
}

impl Guest for Words {
    fn open(name: String, arguments: Vec<u8>) -> Result<u32, String> {
        let a = first(&arguments)?;
        let rows = match name.as_str() {
            "words_of" => Rows::Words(a.as_string::<i32>().iter().flatten().flat_map(|t| t.split_whitespace().map(str::to_string).collect::<Vec<_>>()).collect()),
            "series" => Rows::Series { next: 1, last: a.as_primitive::<arrow_array::types::Int64Type>().iter().flatten().next().unwrap_or(0) },
            _ => return Err(format!("words has no table function {name}")),
        };
        CALLS.with_borrow_mut(|(n, calls)| {
            *n += 1;
            calls.insert(*n, rows);
            Ok(*n)
        })
    }

    fn next(handle: u32) -> Result<Option<Vec<u8>>, String> {
        CALLS.with_borrow_mut(|(_, calls)| {
            let rows = calls.get_mut(&handle).ok_or("no such call")?;
            match rows {
                Rows::Words(words) if words.is_empty() => Ok(None),
                Rows::Words(words) => {
                    let all = std::mem::take(words);
                    let places: Int64Array = (1..=all.len() as i64).collect();
                    batch(vec![("word", Arc::new(StringArray::from(all)) as ArrayRef), ("place", Arc::new(places))]).map(Some)
                }
                Rows::Series { next, last } if *next > *last => Ok(None),
                Rows::Series { next, last } => {
                    let to = (*next + 999).min(*last);
                    let i: Int64Array = (*next..=to).collect();
                    *next = to + 1;
                    batch(vec![("i", Arc::new(i) as ArrayRef)]).map(Some)
                }
            }
        })
    }

    fn close(handle: u32) {
        CALLS.with_borrow_mut(|(_, calls)| calls.remove(&handle));
    }

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

/// One row's arguments' first column.
fn first(arguments: &[u8]) -> Result<ArrayRef, String> {
    read(arguments)?.columns().first().cloned().ok_or_else(|| "a call has one column per argument".to_string())
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> Result<Vec<u8>, String> {
    let schema = Arc::new(Schema::new(columns.iter().map(|(n, c)| Field::new(*n, c.data_type().clone(), true)).collect::<Vec<_>>()));
    let batch = RecordBatch::try_new(schema.clone(), columns.into_iter().map(|(_, c)| c).collect()).map_err(|e| e.to_string())?;
    let mut out = vec![];
    let mut writer = StreamWriter::try_new(&mut out, &schema).map_err(|e| e.to_string())?;
    writer.write(&batch).map_err(|e| e.to_string())?;
    writer.finish().map_err(|e| e.to_string())?;
    drop(writer);
    Ok(out)
}

export!(Words);
