//! Avro object container files, read (Iceberg's manifest lists and manifests: ADR-026): each
//! record decoded by the schema the file carries, into JSON. Bytes and fixed values come as hex
//! text. Blocks may be deflated, Snappy'd or Zstandard'd, as Iceberg's writers leave them.
use anyhow::{bail, ensure, Context, Result};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io::Read;

/// The records of an Avro object container file.
pub fn records(file: &[u8]) -> Result<Vec<Value>> {
    ensure!(file.starts_with(b"Obj\x01"), "not an Avro file");
    let mut r = Reader { b: file, at: 4 };
    let mut meta = HashMap::new();
    loop {
        let n = r.count()?;
        if n == 0 {
            break;
        }
        for _ in 0..n {
            let k = r.string()?;
            meta.insert(k, r.bytes()?.to_vec());
        }
    }
    let schema: Value = serde_json::from_slice(meta.get("avro.schema").context("an Avro file without a schema")?)?;
    let codec = meta.get("avro.codec").map(|c| String::from_utf8_lossy(c).into_owned()).unwrap_or_else(|| "null".into());
    let sync = r.take(16)?.to_vec();
    let mut names = HashMap::new();
    named(&schema, &mut names);
    let mut out = vec![];
    while r.at < r.b.len() {
        let (n, size) = (r.long()?, r.long()? as usize);
        let raw = r.take(size)?;
        let block = match codec.as_str() {
            "null" => raw.to_vec(),
            "deflate" => {
                let mut v = vec![];
                flate2::read::DeflateDecoder::new(raw).read_to_end(&mut v)?;
                v
            }
            "snappy" => snap::raw::Decoder::new().decompress_vec(&raw[..raw.len().saturating_sub(4)])?, // (its CRC after it)
            "zstandard" => zstd::decode_all(raw)?,
            c => bail!("Avro blocks compressed as {c} aren't read"),
        };
        let mut br = Reader { b: &block, at: 0 };
        for _ in 0..n {
            out.push(br.value(&schema, &names)?);
        }
        ensure!(r.take(16)? == sync.as_slice(), "an Avro file out of step (its sync marker)");
    }
    Ok(out)
}

/// Every named type in a schema, by name (and full name), for later references to it.
fn named(s: &Value, out: &mut HashMap<String, Value>) {
    match s {
        Value::Object(o) => {
            if let (Some(n), Some("record" | "enum" | "fixed")) = (o.get("name").and_then(Value::as_str), o.get("type").and_then(Value::as_str)) {
                out.insert(n.to_string(), s.clone());
                if let Some(ns) = o.get("namespace").and_then(Value::as_str) {
                    out.insert(format!("{ns}.{n}"), s.clone());
                }
            }
            o.get("fields").and_then(Value::as_array).into_iter().flatten().for_each(|f| named(&f["type"], out));
            ["items", "values", "type"].iter().filter_map(|k| o.get(*k)).filter(|v| !v.is_string() || o.get("type") != Some(v)).for_each(|v| named(v, out));
        }
        Value::Array(a) => a.iter().for_each(|v| named(v, out)),
        _ => {}
    }
}

struct Reader<'a> {
    b: &'a [u8],
    at: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let s = self.b.get(self.at..self.at + n).context("an Avro file cut short")?;
        self.at += n;
        Ok(s)
    }

    fn long(&mut self) -> Result<i64> {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let byte = *self.take(1)?.first().expect("a byte");
            v |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok(((v >> 1) as i64) ^ -((v & 1) as i64));
            }
            shift += 7;
            ensure!(shift < 64, "an Avro number too long");
        }
    }

    /// A block's count of items (a negative count is followed by the block's size).
    fn count(&mut self) -> Result<usize> {
        let n = self.long()?;
        if n < 0 {
            self.long()?;
        }
        Ok(n.unsigned_abs() as usize)
    }

    fn bytes(&mut self) -> Result<&'a [u8]> {
        let n = self.long()? as usize;
        self.take(n)
    }

    fn string(&mut self) -> Result<String> { Ok(String::from_utf8(self.bytes()?.to_vec())?) }

    fn value(&mut self, s: &Value, names: &HashMap<String, Value>) -> Result<Value> {
        let hex = |b: &[u8]| Value::String(b.iter().map(|x| format!("{x:02x}")).collect());
        Ok(match s {
            Value::String(t) => match t.as_str() {
                "null" => Value::Null,
                "boolean" => Value::Bool(self.take(1)?[0] != 0),
                "int" | "long" => Value::from(self.long()?),
                "float" => Value::from(f32::from_le_bytes(self.take(4)?.try_into()?) as f64),
                "double" => Value::from(f64::from_le_bytes(self.take(8)?.try_into()?)),
                "bytes" => hex(self.bytes()?),
                "string" => Value::String(self.string()?),
                name => return self.value(names.get(name).with_context(|| format!("an Avro type {name} never defined"))?, names),
            },
            Value::Array(branches) => {
                let i = self.long()? as usize;
                self.value(branches.get(i).context("an Avro union branch out of range")?, names)?
            }
            Value::Object(o) => match o.get("type").and_then(Value::as_str).context("an Avro type without a name")? {
                "record" => {
                    let mut m = Map::new();
                    for f in o.get("fields").and_then(Value::as_array).into_iter().flatten() {
                        m.insert(f["name"].as_str().unwrap_or_default().to_string(), self.value(&f["type"], names)?);
                    }
                    Value::Object(m)
                }
                "enum" => {
                    let i = self.long()? as usize;
                    o["symbols"].get(i).cloned().context("an Avro enum out of range")?
                }
                "array" => {
                    let mut items = vec![];
                    loop {
                        let n = self.count()?;
                        if n == 0 {
                            break Value::Array(items);
                        }
                        for _ in 0..n {
                            items.push(self.value(&o["items"], names)?);
                        }
                    }
                }
                "map" => {
                    let mut m = Map::new();
                    loop {
                        let n = self.count()?;
                        if n == 0 {
                            break Value::Object(m);
                        }
                        for _ in 0..n {
                            let k = self.string()?;
                            m.insert(k, self.value(&o["values"], names)?);
                        }
                    }
                }
                "fixed" => hex(self.take(o["size"].as_u64().context("a fixed without its size")? as usize)?),
                t => self.value(&Value::String(t.to_string()), names)?, // ({"type": "long", "logicalType": …})
            },
            other => bail!("an Avro type {other}"),
        })
    }
}

/// Hex text (as `records` gives bytes) back to bytes.
pub fn unhex(s: &str) -> Result<Vec<u8>> { (0..s.len()).step_by(2).map(|i| Ok(u8::from_str_radix(s.get(i..i + 2).context("odd hex")?, 16)?)).collect() }
