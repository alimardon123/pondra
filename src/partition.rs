//! A table's partition spec (`partition_by`; ADR-029 §6, step 5 I3): fields separated by commas, each a column as it
//! is or one of Iceberg's transforms of one: `year`, `month`, `day` or `hour` of a time, `bucket(n, c)` (Iceberg's
//! own hash, so a published table and an attached one agree) and `truncate(w, c)`. Its columns are stored names.
//!
//! Each file holds one value of every field: its part (`DataFile::part`), each field's value as text. Several are
//! joined by `/`, each with `%` and `/` escaped; one is as it is, as lakes written before name it.
use anyhow::{anyhow, bail, ensure, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Int32Array, RecordBatch, UInt32Array};
use datafusion::arrow::compute::{cast, concat_batches, take_record_batch};
use datafusion::arrow::datatypes::{DataType, Int32Type, Int64Type, Schema, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

/// What a field takes of its column.
#[derive(Debug, Clone, PartialEq)]
pub enum Transform {
    Identity,
    Time(&'static str), // year, month, day or hour
    Bucket(u32),
    Truncate(u32),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Field {
    pub transform: Transform,
    pub column: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Spec {
    pub fields: Vec<Field>,
}

const WORDS: &str = "PARTITION BY: columns, each as it is, year, month, day or hour(a time), bucket(n, a column) or truncate(w, a number or text column)";

impl Spec {
    pub fn parse(text: &str) -> Result<Spec> {
        let (mut fields, mut depth, mut at) = (vec![], 0i32, 0);
        for (i, c) in text.char_indices().chain([(text.len(), ',')]) {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    fields.push(field(text[at..i].trim())?);
                    at = i + 1;
                }
                _ => {}
            }
        }
        ensure!(!fields.is_empty() && depth == 0, "{WORDS}");
        Ok(Spec { fields })
    }

    /// Its columns, each once.
    pub fn columns(&self) -> Vec<&str> {
        let mut out: Vec<&str> = vec![];
        for f in &self.fields {
            if !out.contains(&f.column.as_str()) {
                out.push(&f.column);
            }
        }
        out
    }

    /// Every field names a column of `columns` (name, Arrow type as text) its transform takes, once.
    pub fn check(&self, columns: &[(String, String)]) -> Result<()> {
        for (i, f) in self.fields.iter().enumerate() {
            let t = columns.iter().find(|(n, _)| *n == f.column).map(|(_, t)| t.as_str()).ok_or_else(|| anyhow!("{WORDS}: {} is not a column of the table", f.column))?;
            let (int, time, clock, text, bytes, decimal) = (
                matches!(t, "Int8" | "Int16" | "Int32" | "Int64"),
                t.starts_with("Timestamp") || t.starts_with("Date"),
                t.starts_with("Time32") || t.starts_with("Time64"),
                matches!(t, "Utf8" | "LargeUtf8" | "Utf8View"),
                matches!(t, "Binary" | "LargeBinary" | "BinaryView"),
                t.starts_with("Decimal128"),
            );
            let fits = match f.transform {
                Transform::Identity => true,
                Transform::Time(_) => time,
                Transform::Bucket(_) => int || time || clock || text || bytes || decimal,
                Transform::Truncate(_) => int || text || decimal,
            };
            ensure!(fits, "{WORDS}: {} is a {t}", f.column);
            ensure!(!self.fields[..i].contains(f), "{WORDS}: {} is there twice", text_of(f));
        }
        Ok(())
    }
}

/// `text` with its columns named by `name` (stored names to SQL's), as written: `day(ts), bucket(16, id)`. A text that
/// doesn't parse comes back as it is.
pub fn renamed(text: &str, name: impl Fn(&str) -> String) -> String {
    match Spec::parse(text) {
        Ok(s) => s.fields.into_iter().map(|f| text_of(&Field { column: name(&f.column), ..f })).collect::<Vec<_>>().join(", "),
        Err(_) => text.to_string(),
    }
}

fn field(text: &str) -> Result<Field> {
    let unquote = |c: &str| c.trim().trim_matches('"').to_string();
    let Some((name, rest)) = text.split_once('(') else {
        ensure!(!text.is_empty(), "{WORDS}");
        return Ok(Field { transform: Transform::Identity, column: unquote(text) });
    };
    let args: Vec<&str> = rest.strip_suffix(')').ok_or_else(|| anyhow!("{WORDS}"))?.split(',').map(str::trim).collect();
    let width = |n: &str| n.parse::<u32>().ok().filter(|n| (1..=i32::MAX as u32).contains(n)).ok_or_else(|| anyhow!("{WORDS}: {n} is not a positive whole number"));
    let transform = match (name.trim().to_ascii_lowercase().as_str(), args.len()) {
        ("year", 1) => Transform::Time("year"),
        ("month", 1) => Transform::Time("month"),
        ("day", 1) => Transform::Time("day"),
        ("hour", 1) => Transform::Time("hour"),
        ("bucket", 2) => Transform::Bucket(width(args[0])?),
        ("truncate", 2) => Transform::Truncate(width(args[0])?),
        _ => bail!("{WORDS}: not {text}"),
    };
    Ok(Field { transform, column: unquote(args[args.len() - 1]) })
}

fn text_of(f: &Field) -> String {
    match f.transform {
        Transform::Identity => f.column.clone(),
        Transform::Time(u) => format!("{u}({})", f.column),
        Transform::Bucket(n) => format!("bucket({n}, {})", f.column),
        Transform::Truncate(w) => format!("truncate({w}, {})", f.column),
    }
}

/// A field as SQL over a table of its column of type `t`.
fn expr(f: &Field, t: &DataType) -> String {
    let c = format!("\"{}\"", f.column.replace('"', "\"\""));
    match (&f.transform, t) {
        (Transform::Identity, _) => c,
        (Transform::Time(unit), _) => format!("date_trunc('{unit}', {c})"),
        (Transform::Bucket(n), _) => format!("pondra_bucket({n}, {c})"),
        (Transform::Truncate(w), DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View) => format!("substr({c}, 1, {w})"),
        (Transform::Truncate(w), DataType::Decimal128(p, s)) => {
            // (Iceberg's width is of the unscaled value: 50 at two places is 0.50)
            let s = (*s).max(0) as u32;
            let (whole, part) = (*w as u128 / 10u128.pow(s), *w as u128 % 10u128.pow(s));
            let w = if s == 0 { format!("{whole}") } else { format!("{whole}.{part:0width$}", width = s as usize) };
            let w = format!("CAST('{w}' AS DECIMAL({p}, {s}))");
            format!("({c} - ((({c} % {w}) + {w}) % {w}))")
        }
        (Transform::Truncate(w), _) => format!("({c} - ((({c} % {w}) + {w}) % {w}))"),
    }
}

/// A file's part: its fields' values as text, several joined by `/` with `%` and `/` escaped.
fn part(values: &[&str]) -> String {
    match values {
        [one] => one.to_string(),
        _ => values.iter().map(|v| v.replace('%', "%25").replace('/', "%2F")).collect::<Vec<_>>().join("/"),
    }
}

/// A part's values, one a field of a spec of `n`: `part` read back.
pub fn values(part: &str, n: usize) -> Vec<String> {
    match n {
        1 => vec![part.to_string()],
        _ => part.split('/').map(|v| v.replace("%2F", "/").replace("%25", "%")).collect(),
    }
}

/// A partitioned table's rows, one group per part (`day(ts)`: the day, `col`: the value).
pub async fn split(spec: &str, batches: Vec<RecordBatch>) -> Result<Vec<(String, Vec<RecordBatch>)>> {
    let Some(first) = batches.first() else { return Ok(vec![]) };
    let spec = Spec::parse(spec)?;
    let all = concat_batches(&first.schema(), &batches)?;
    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_udf(ScalarUDF::new_from_impl(Bucket { signature: Signature::any(2, Volatility::Immutable) }));
    ctx.register_batch("t", all.clone())?;
    let exprs = spec.fields.iter().map(|f| Ok(expr(f, all.schema().field_with_name(&f.column)?.data_type()))).collect::<Result<Vec<_>>>()?;
    let values = ctx.sql(&format!("SELECT {} FROM t", exprs.join(", "))).await?.collect().await?;
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut i = 0u32;
    for b in &values {
        let texts = b.columns().iter().map(|c| cast(c, &DataType::Utf8)).collect::<Result<Vec<_>, _>>()?;
        for row in 0..b.num_rows() {
            let each: Vec<&str> = texts.iter().map(|t| { let t = t.as_string::<i32>(); if t.is_null(row) { "null" } else { t.value(row) } }).collect();
            groups.entry(part(&each)).or_default().push(i);
            i += 1;
        }
    }
    groups.into_iter().map(|(p, rows)| Ok((p, vec![take_record_batch(&all, &UInt32Array::from(rows))?]))).collect()
}

/// Which parts a scan with `filters` may read, as a test of a file's part. A bucket field whose column the filters pin
/// to values (`c = v`, `c IN (…)`) keeps the parts of those values' buckets. Ranges prune every other field, since a file
/// of one day or one value has narrow ones; a bucket isn't in order with its column, so only the part says. `name`: a
/// stored column's name as the filters and `schema` say it. None: nothing to skip.
pub fn wanted(spec: &str, filters: &[Expr], name: impl Fn(&str) -> Option<String>, schema: &Schema) -> Option<Box<dyn Fn(&str) -> bool + Send + Sync>> {
    let spec = Spec::parse(spec).ok()?;
    let n = spec.fields.len();
    let mut need: Vec<(usize, HashSet<String>)> = vec![];
    for (i, f) in spec.fields.iter().enumerate() {
        let Transform::Bucket(n) = f.transform else { continue };
        let Some(col) = name(&f.column) else { continue };
        let Ok(t) = schema.field_with_name(&col).map(|f| f.data_type().clone()) else { continue };
        let Some(values) = filters.iter().find_map(|e| pinned(e, &col)) else { continue };
        let Ok(b) = ScalarValue::iter_to_array(values).map_err(anyhow::Error::from).and_then(|a| Ok(cast(&a, &t)?)).and_then(|a| buckets(&a, n as i32)) else { continue };
        need.push((i, b.as_primitive::<Int32Type>().iter().flatten().map(|v| v.to_string()).collect()));
    }
    (!need.is_empty()).then(move || -> Box<dyn Fn(&str) -> bool + Send + Sync> {
        Box::new(move |part: &str| {
            let values = values(part, n);
            need.iter().all(|(i, set)| values.get(*i).is_none_or(|v| set.contains(v)))
        })
    })
}

/// The values a filter pins column `col` to: `col = v`, `v = col`, `col IN (v, …)`; an AND, what either side pins.
fn pinned(e: &Expr, col: &str) -> Option<Vec<ScalarValue>> {
    use datafusion::logical_expr::{expr::InList, BinaryExpr, Operator};
    let is_col = |e: &Expr| matches!(e, Expr::Column(c) if c.name == col);
    let lit = |e: &Expr| match e {
        Expr::Literal(v, _) if !v.is_null() => Some(v.clone()),
        _ => None,
    };
    match e {
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) if is_col(left) => lit(right).map(|v| vec![v]),
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::Eq, right }) if is_col(right) => lit(left).map(|v| vec![v]),
        Expr::BinaryExpr(BinaryExpr { left, op: Operator::And, right }) => pinned(left, col).or_else(|| pinned(right, col)),
        Expr::InList(InList { expr, list, negated: false }) if is_col(expr) => list.iter().map(lit).collect(),
        _ => None,
    }
}

/// Iceberg's bucket of a value: murmur3 (x86, 32 bits, seed 0) of its bytes as Iceberg's spec gives them (Appendix B),
/// `(hash & i32::MAX) % n`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Bucket {
    signature: Signature,
}

impl ScalarUDFImpl for Bucket {
    fn name(&self) -> &str { "pondra_bucket" }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> { Ok(DataType::Int32) }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
        let n = match &args.args[0] {
            ColumnarValue::Scalar(v) => match v.cast_to(&DataType::Int64)? {
                ScalarValue::Int64(Some(n)) => i32::try_from(n).unwrap_or(0),
                _ => 0,
            },
            _ => 0,
        };
        if n < 1 {
            return datafusion::common::exec_err!("bucket(n, c): n is a positive whole number");
        }
        let rows = args.number_rows;
        let a = args.args[1].to_array(rows)?;
        Ok(ColumnarValue::Array(buckets(&a, n).map_err(|e| datafusion::error::DataFusionError::Execution(e.to_string()))?))
    }
}

fn buckets(a: &ArrayRef, n: i32) -> Result<ArrayRef> {
    let of = |bytes: &[u8]| (murmur3(bytes) as i32 & i32::MAX) % n;
    let longs = |a: &ArrayRef| -> Result<Int32Array> { Ok(cast(a, &DataType::Int64)?.as_primitive::<Int64Type>().iter().map(|v| v.map(|v| of(&v.to_le_bytes()))).collect()) };
    let out: Int32Array = match a.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => longs(a)?,
        DataType::Date32 => longs(&cast(a, &DataType::Int32)?)?, // (days)
        DataType::Time32(_) | DataType::Time64(_) => longs(&cast(a, &DataType::Time64(TimeUnit::Microsecond))?)?,
        DataType::Timestamp(_, tz) => longs(&cast(a, &DataType::Timestamp(TimeUnit::Microsecond, tz.clone()))?)?,
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => cast(a, &DataType::Utf8)?.as_string::<i32>().iter().map(|v| v.map(|v| of(v.as_bytes()))).collect(),
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => cast(a, &DataType::Binary)?.as_binary::<i32>().iter().map(|v| v.map(of)).collect(),
        DataType::Decimal128(_, _) => a.as_primitive::<datafusion::arrow::datatypes::Decimal128Type>().iter().map(|v| v.map(|v| of(&unscaled(v)))).collect(),
        t => bail!("bucket(n, c) of a {t}"),
    };
    Ok(Arc::new(out))
}

/// A decimal's unscaled value as Java's `BigInteger.toByteArray`: big-endian two's complement, its fewest bytes.
fn unscaled(v: i128) -> Vec<u8> {
    let b = v.to_be_bytes();
    let mut i = 0;
    while i < 15 && ((b[i] == 0 && b[i + 1] & 0x80 == 0) || (b[i] == 0xff && b[i + 1] & 0x80 != 0)) {
        i += 1;
    }
    b[i..].to_vec()
}

/// MurmurHash3, x86 32-bit, seed 0.
fn murmur3(data: &[u8]) -> u32 {
    let (c1, c2) = (0xcc9e_2d51u32, 0x1b87_3593u32);
    let mut h = 0u32;
    let chunks = data.chunks_exact(4);
    let tail = chunks.remainder();
    for c in chunks {
        let k = u32::from_le_bytes([c[0], c[1], c[2], c[3]]).wrapping_mul(c1).rotate_left(15).wrapping_mul(c2);
        h = (h ^ k).rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    if !tail.is_empty() {
        let k = tail.iter().enumerate().fold(0u32, |k, (i, b)| k ^ ((*b as u32) << (8 * i)));
        h ^= k.wrapping_mul(c1).rotate_left(15).wrapping_mul(c2);
    }
    h ^= data.len() as u32;
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^ (h >> 16)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Date32Array, Decimal128Array, Int64Array, StringArray, TimestampMicrosecondArray};

    /// Iceberg's spec, Appendix B: the hash of each type's example value.
    #[test]
    fn hashes_as_iceberg_does() {
        let h = |b: &[u8]| murmur3(b) as i32;
        assert_eq!(h(&34i64.to_le_bytes()), 2017239379, "int and long 34");
        assert_eq!(h(&17486i64.to_le_bytes()), -653330422, "date 2017-11-16");
        assert_eq!(h(&81068000000i64.to_le_bytes()), -662762989, "time 22:31:08");
        assert_eq!(h(&1510871468000000i64.to_le_bytes()), -2047944441, "timestamp 2017-11-16T22:31:08");
        assert_eq!(h(b"iceberg"), 1210000089, "string");
        assert_eq!(h(&[0, 1, 2, 3]), -188683207, "binary");
        assert_eq!(h(&unscaled(1420)), -500754589, "decimal(9, 2) 14.20");
        assert_eq!((unscaled(-1), unscaled(128), unscaled(0)), (vec![0xff], vec![0, 0x80], vec![0]));
    }

    #[test]
    fn buckets_each_type() {
        let n = |a: ArrayRef| buckets(&a, 16).unwrap().as_primitive::<datafusion::arrow::datatypes::Int32Type>().values().to_vec();
        assert_eq!(n(Arc::new(Int64Array::from(vec![34]))), vec![2017239379 % 16]);
        assert_eq!(n(Arc::new(StringArray::from(vec!["iceberg"]))), vec![1210000089 % 16]);
        assert_eq!(n(Arc::new(Date32Array::from(vec![17486]))), vec![(-653330422i32 & i32::MAX) % 16]);
        assert_eq!(n(Arc::new(TimestampMicrosecondArray::from(vec![1510871468000000]))), vec![(-2047944441i32 & i32::MAX) % 16]);
        assert_eq!(n(Arc::new(Decimal128Array::from(vec![1420]).with_precision_and_scale(9, 2).unwrap())), vec![(-500754589i32 & i32::MAX) % 16]);
    }

    #[test]
    fn reads_every_form() {
        let s = Spec::parse("day(ts), bucket(16, id), truncate(3, \"Name\"), region").unwrap();
        assert_eq!(s.fields.iter().map(text_of).collect::<Vec<_>>(), ["day(ts)", "bucket(16, id)", "truncate(3, Name)", "region"]);
        assert_eq!(s.columns(), ["ts", "id", "Name", "region"]);
        let cols = |t: &str| vec![("ts".to_string(), "Timestamp(Microsecond, None)".to_string()), ("id".to_string(), "Int64".to_string()), ("Name".to_string(), t.to_string()), ("region".to_string(), "Utf8".to_string())];
        s.check(&cols("Utf8")).unwrap();
        assert!(s.check(&cols("Float64")).is_err(), "truncate of a float");
        assert!(Spec::parse("bucket(0, id)").is_err() && Spec::parse("bucket(id)").is_err() && Spec::parse("week(ts)").is_err() && Spec::parse("day(ts").is_err());
        assert!(Spec::parse("id, id").unwrap().check(&cols("Utf8")).is_err(), "a field twice");
        assert!(Spec::parse("day(id)").unwrap().check(&cols("Utf8")).is_err(), "day of a number");
    }

    /// A one-field spec's part is its value as lakes written before name it; several are escaped and joined.
    #[tokio::test]
    async fn splits_by_every_field() {
        use datafusion::arrow::datatypes::{Field as F, Schema};
        let schema = Arc::new(Schema::new(vec![F::new("id", DataType::Int64, true), F::new("s", DataType::Utf8, true)]));
        let b = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![Some(1), Some(-1), Some(12), None])), Arc::new(StringArray::from(vec!["a/b", "a%c", "ab", "x"]))]).unwrap();
        let parts = |spec: &'static str| { let b = b.clone(); async move { split(spec, vec![b]).await.unwrap().into_iter().map(|(p, r)| (p, r[0].num_rows())).collect::<Vec<_>>() } };
        assert_eq!(parts("id").await, [("-1".into(), 1), ("1".into(), 1), ("12".into(), 1), ("null".into(), 1)]);
        assert_eq!(parts("truncate(10, id), truncate(1, s)").await, [("-10/a".into(), 1), ("0/a".into(), 1), ("10/a".into(), 1), ("null/x".into(), 1)]);
        assert_eq!(parts("s").await, [("a%c".into(), 1), ("a/b".into(), 1), ("ab".into(), 1), ("x".into(), 1)]);
        assert_eq!(parts("s, id").await[..2], [("a%25c/-1".into(), 1), ("a%2Fb/1".into(), 1)]);
        let p = parts("bucket(4, id)").await;
        assert_eq!(p.iter().map(|(_, n)| n).sum::<usize>(), 4);
        assert!(p.iter().all(|(b, _)| b == "null" || b.parse::<i32>().is_ok_and(|b| (0..4).contains(&b))));
    }
}
