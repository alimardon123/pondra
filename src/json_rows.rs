//! JSON rows as a table's columns.
use crate::query::strict;
use anyhow::{bail, Result};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Decimal128Array, PrimitiveArray, StringViewArray};
use datafusion::arrow::compute::kernels::cast_utils::parse_decimal;
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Decimal128Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, Schema, SchemaRef, UInt16Type, UInt32Type, UInt64Type, UInt8Type};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// JSON rows as `schema`'s columns. A JSON number is text, as SQL's literals are. An integer column is read as a
/// double, which arrow reads fast, and a double gives a whole number exactly below 2^52 (`exact_whole`). A batch with a
/// value a double can't give exactly is read again with that column as text, and the column stays text from then on:
/// `text_column` works its numbers out, rounded as SQL rounds them (invariant 293). A number or a boolean in a text
/// column is its text, as SQL's `INSERT … VALUES (5)` makes it. Nested types (lists, structs) stay as they are.
pub fn from_json(body: &[u8], schema: &SchemaRef) -> Result<RecordBatch> {
    Ok(json_rows(body, schema, JSON_ROWS)?.0)
}

/// arrow's default number of rows in a batch.
const JSON_ROWS: usize = 1024;

/// `from_json` with `rows` rows to a batch, and the integer columns that ended up read as text (`true`). A batch that
/// doesn't decode with its integer columns as doubles is read again with all of them as text: the text read says what
/// each value is, or refuses it by its own words.
fn json_rows(body: &[u8], schema: &SchemaRef, rows: usize) -> Result<(RecordBatch, Vec<bool>)> {
    let mut text = vec![false; schema.fields().len()];
    let mut decoder = json_decoder(schema, &text, rows)?;
    let mut batches = Vec::new();
    let mut at = 0;
    while at < body.len() {
        // (decode stops at a row's end, so the bytes it used are where the next batch starts)
        let decoded = decoder.decode(&body[at..]).and_then(|used| Ok((used, decoder.flush()?)));
        let (used, batch) = match decoded {
            Ok(decoded) => decoded,
            Err(_) if schema.fields().iter().zip(&text).any(|(f, &t)| f.data_type().is_integer() && !t) => {
                for (t, f) in text.iter_mut().zip(schema.fields()) {
                    *t |= f.data_type().is_integer();
                }
                decoder = json_decoder(schema, &text, rows)?;
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        let Some(batch) = batch else {
            at += used;
            continue;
        };
        match whole_batch(&batch, schema, &mut text)? {
            Some(batch) => {
                batches.push(batch);
                at += used;
            }
            // (a column that a double can't hold is text now: the same rows are read again)
            None => decoder = json_decoder(schema, &text, rows)?,
        }
    }
    Ok((datafusion::arrow::compute::concat_batches(schema, &batches)?, text))
}

/// A decoder for `schema`'s rows, `rows` to a batch, with each integer column read as text where `text` says so and as
/// a double elsewhere.
fn json_decoder(schema: &SchemaRef, text: &[bool], rows: usize) -> Result<datafusion::arrow::json::reader::Decoder> {
    let fields = schema.fields().iter().zip(text).map(|(f, &t)| match (f.data_type().is_integer(), t) {
        (true, true) => Arc::new(f.as_ref().clone().with_data_type(DataType::Utf8View)),
        (true, false) => Arc::new(f.as_ref().clone().with_data_type(DataType::Float64)),
        (false, _) => f.clone(),
    });
    let read = Arc::new(Schema::new(fields.collect::<Vec<_>>()));
    Ok(datafusion::arrow::json::ReaderBuilder::new(read).with_coerce_primitive(true).with_batch_size(rows).build_decoder()?)
}

/// A batch of rows, read as `json_decoder` reads them, as `schema`'s columns with each integer column whole: the doubles'
/// whole numbers or the text's. A double column with a value it can't give exactly, or a value that doesn't fit its
/// type, is flagged as text in `text`, and None is returned: the batch is read again.
fn whole_batch(batch: &RecordBatch, schema: &SchemaRef, text: &mut [bool]) -> Result<Option<RecordBatch>> {
    let mut columns = Vec::with_capacity(schema.fields().len());
    let mut flagged = false;
    for (i, (f, c)) in schema.fields().iter().zip(batch.columns()).enumerate() {
        let t = f.data_type();
        let column = match (t.is_integer(), text[i]) {
            (false, _) => c.clone(),
            (true, true) => text_column(c.as_string_view(), t)?,
            (true, false) => match doubles(c.as_primitive::<Float64Type>(), t) {
                Some(whole) => whole,
                None => {
                    text[i] = true;
                    flagged = true;
                    continue;
                }
            },
        };
        columns.push(column);
    }
    if flagged {
        return Ok(None);
    }
    Ok(Some(RecordBatch::try_new(schema.clone(), columns)?))
}

/// Doubles at and above 2^52 are not all whole, and below it a double holds a whole number exactly (`exact_whole`).
const TWO_52: f64 = 4_503_599_627_370_496.0;

/// The least and greatest of `values`, and whether every one is whole and below 2^52. One pass with no branch per value
/// (the comparisons are selects), so it vectorizes.
fn span(values: &[f64]) -> (f64, f64, bool) {
    let (mut least, mut greatest, mut whole) = (f64::INFINITY, f64::NEG_INFINITY, true);
    for &v in values {
        whole &= (v.trunc() == v) & (v.abs() < TWO_52);
        least = if v < least { v } else { least };
        greatest = if v > greatest { v } else { greatest };
    }
    (least, greatest, whole)
}

/// `$d`'s values as `$t` (whose native type is `$n`) by one cast per value, when `$ok` says every value is whole and fits;
/// otherwise value by value, as `doubles_as` reads them (rounding, the ties to text, the refusals).
macro_rules! typed {
    ($d:expr, $ok:expr, $t:ty, $n:ty) => {
        if $ok {
            let values: Vec<$n> = $d.values().iter().map(|&v| v as $n).collect();
            Some(Arc::new(PrimitiveArray::<$t>::new(values.into(), $d.nulls().cloned())) as ArrayRef)
        } else {
            doubles_as::<$t>($d).map(|a| Arc::new(a) as ArrayRef)
        }
    };
}

/// A double column as the whole numbers of `t`, or None when a value can't be given exactly or doesn't fit `t`. The
/// common case is the cast: when every value is whole and the least and greatest fit `t`, each value is one cast.
fn doubles(d: &PrimitiveArray<Float64Type>, t: &DataType) -> Option<ArrayRef> {
    let (least, greatest, whole) = span(d.values());
    let fits = |min: f64, max: f64| whole && least >= min && greatest <= max;
    match t {
        DataType::Int8 => typed!(d, fits(i8::MIN as f64, i8::MAX as f64), Int8Type, i8),
        DataType::Int16 => typed!(d, fits(i16::MIN as f64, i16::MAX as f64), Int16Type, i16),
        DataType::Int32 => typed!(d, fits(i32::MIN as f64, i32::MAX as f64), Int32Type, i32),
        DataType::Int64 => typed!(d, fits(i64::MIN as f64, i64::MAX as f64), Int64Type, i64),
        DataType::UInt8 => typed!(d, fits(0.0, u8::MAX as f64), UInt8Type, u8),
        DataType::UInt16 => typed!(d, fits(0.0, u16::MAX as f64), UInt16Type, u16),
        DataType::UInt32 => typed!(d, fits(0.0, u32::MAX as f64), UInt32Type, u32),
        DataType::UInt64 => typed!(d, fits(0.0, u64::MAX as f64), UInt64Type, u64),
        _ => None,
    }
}

fn doubles_as<T>(d: &PrimitiveArray<Float64Type>) -> Option<PrimitiveArray<T>>
where
    T: ArrowPrimitiveType,
    T::Native: TryFrom<i64>,
{
    let mut values = Vec::with_capacity(d.len());
    for v in d.iter() {
        values.push(match v {
            Some(v) => T::Native::try_from(exact_whole(v)?).ok()?,
            None => T::Native::default(),
        });
    }
    Some(PrimitiveArray::new(values.into(), d.nulls().cloned()))
}

/// A double as the whole number its text meant, when the double shows it. Below 2^52 a double is within a quarter of
/// its text's value, so a whole double is that text's whole number, and a fraction that isn't near a half rounds as the
/// text does, half away from zero. A tie or a near one is None (2.4999999999999999999 is the double 2.5 too, and the
/// text rounds down), and so is anything from 2^52 up, where a double can't hold a fraction.
fn exact_whole(v: f64) -> Option<i64> {
    let a = v.abs();
    if !(a < TWO_52) {
        return None;
    }
    let fraction = a.fract();
    if fraction != 0.0 && (fraction - 0.5).abs() <= a * f64::EPSILON {
        return None;
    }
    Some(v.round() as i64)
}

/// An integer column's text as `t`'s whole numbers, with its nulls kept. Each value goes in tenths, exactly: an integer
/// is its value times ten, and any other number is parsed to a scale of 1, which cuts its digits past the tenth.
/// `strict` then rounds half away from zero and casts, so a value `t` can't hold is refused, naming it.
fn text_column(text: &StringViewArray, t: &DataType) -> Result<ArrayRef> {
    // (cut to a tenth and then rounded is rounding the exact value: 2.45 → 2.4 → 2, and 2.55 → 2.5 → 3)
    let tenths = text
        .iter()
        .map(|v| -> Result<Option<i128>> {
            let Some(v) = v else { return Ok(None) };
            Ok(Some(match v.parse::<i64>() {
                Ok(n) => i128::from(n) * 10,
                Err(_) if too_long(v) => bail!("{v} is too long, or has too large an exponent, to read as {t}"),
                Err(_) => parse_decimal::<Decimal128Type>(v, 38, 1)?,
            }))
        })
        .collect::<Result<Vec<_>>>()?;
    let array = Decimal128Array::from(tenths).with_precision_and_scale(38, 1)?;
    strict(&(Arc::new(array) as ArrayRef), t)
}

/// arrow's parser keeps a number's digits in an i128 (exact to 38 digits) and counts them in a u8, and counts an
/// exponent in an i16: a longer text, or one with a five-digit exponent, wraps and comes out wrong (1e65536 came out
/// as 1). Those are refused.
fn too_long(text: &str) -> bool {
    let exponent = text.split(['e', 'E']).nth(1).unwrap_or("").trim_start_matches(['+', '-']);
    text.len() > 38 || exponent.len() > 4
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Field;
    use datafusion::arrow::util::display::array_value_to_string;

    /// A schema of nullable columns: `(name, type)` each.
    fn table_of(columns: &[(&str, DataType)]) -> SchemaRef {
        Arc::new(Schema::new(columns.iter().map(|(name, t)| Field::new(*name, t.clone(), true)).collect::<Vec<_>>()))
    }

    fn lines(rows: &[&str]) -> Vec<u8> {
        rows.iter().flat_map(|r| format!("{r}\n").into_bytes()).collect()
    }

    #[test]
    fn json_ties_and_near_ties_round_as_the_text_says() {
        // 2.5 is a tie, which rounds away from zero. 2.4999999999999999999 is the double 2.5 too, but its text rounds down.
        let s = table_of(&[("i", DataType::Int32)]);
        let b = from_json(&lines(&[r#"{"i": 2.5}"#, r#"{"i": -2.5}"#, r#"{"i": 2.4999999999999999999}"#]), &s).unwrap();
        assert_eq!(b.column(0).as_primitive::<Int32Type>().iter().collect::<Vec<_>>(), [Some(3), Some(-3), Some(2)]);
    }

    #[test]
    fn json_integers_from_2_52_up_are_exact() {
        // A double holds no fraction from 2^52 up, so these are read as text: 9007199254740993 is not 9007199254740992.
        let s = table_of(&[("b", DataType::Int64), ("u", DataType::UInt64)]);
        let body = lines(&[r#"{"b": 9007199254740993, "u": 18446744073709551615}"#, r#"{"b": 4503599627370496, "u": 9007199254740993}"#,
                           r#"{"b": -9007199254740993}"#]);
        let b = from_json(&body, &s).unwrap();
        assert_eq!(b.column(0).as_primitive::<Int64Type>().iter().collect::<Vec<_>>(),
                   [Some(9007199254740993), Some(4503599627370496), Some(-9007199254740993)]);
        assert_eq!(b.column(1).as_primitive::<UInt64Type>().iter().collect::<Vec<_>>(), [Some(u64::MAX), Some(9007199254740993), None]);
    }

    #[test]
    fn json_a_column_that_turns_to_text_in_a_later_batch_keeps_its_rows() {
        // Two rows to a batch: the first batch reads as doubles; the second holds a value a double can't hold (in b) and a
        // fraction (in i), so both are read again as text, and the third batch is read as text from its start.
        let s = table_of(&[("b", DataType::Int64), ("i", DataType::Int32)]);
        let body = lines(&[r#"{"b": 1, "i": 1}"#, r#"{"b": 2, "i": 2}"#, r#"{"b": 9007199254740993, "i": 3}"#,
                           r#"{"b": 4, "i": 2.5}"#, r#"{"b": 5, "i": 6}"#]);
        let (b, flags) = json_rows(&body, &s, 2).unwrap();
        assert_eq!(b.column(0).as_primitive::<Int64Type>().iter().collect::<Vec<_>>(), [Some(1), Some(2), Some(9007199254740993), Some(4), Some(5)]);
        assert_eq!(b.column(1).as_primitive::<Int32Type>().iter().collect::<Vec<_>>(), [Some(1), Some(2), Some(3), Some(3), Some(6)]);
        assert_eq!(flags, [true, true]);
    }

    #[test]
    fn json_a_text_column_and_an_integer_column_in_one_schema() {
        let s = table_of(&[("i", DataType::Int32), ("s", DataType::Utf8)]);
        let body = lines(&[r#"{"i": 2.5, "s": 5}"#, r#"{"i": "7", "s": "x"}"#, r#"{"i": 3, "s": null}"#]);
        let b = from_json(&body, &s).unwrap();
        assert_eq!(b.column(0).as_primitive::<Int32Type>().iter().collect::<Vec<_>>(), [Some(3), Some(7), Some(3)]);
        assert_eq!(b.column(1).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("5"), Some("x"), None]);
    }

    #[test]
    fn json_every_integer_width_takes_its_halves_and_refuses_what_is_out() {
        // (type, a half that rounds to the largest value, the largest value, a value past it: a TINYINT refuses 300)
        let widths = [
            (DataType::Int8, "126.5", "127", "300"),
            (DataType::Int16, "32766.5", "32767", "70000"),
            (DataType::Int32, "2147483646.5", "2147483647", "2147483648"),
            (DataType::Int64, "9223372036854775806.5", "9223372036854775807", "9223372036854775808"),
            (DataType::UInt8, "254.5", "255", "256"),
            (DataType::UInt16, "65534.5", "65535", "65536"),
            (DataType::UInt32, "4294967294.5", "4294967295", "4294967296"),
            (DataType::UInt64, "18446744073709551614.5", "18446744073709551615", "18446744073709551616"),
        ];
        for (t, half, max, over) in widths {
            let s = table_of(&[("x", t.clone())]);
            let b = from_json(format!("{{\"x\": {half}}}\n{{\"x\": 2.5}}\n").as_bytes(), &s).unwrap();
            let got = (0..2).map(|row| array_value_to_string(b.column(0), row).unwrap()).collect::<Vec<_>>();
            assert_eq!(got, [max, "3"], "{t}");
            let err = from_json(format!("{{\"x\": {over}}}\n").as_bytes(), &s).unwrap_err();
            let words = match t {
                DataType::Int16 => "smallint out of range".to_string(),
                DataType::Int32 => "integer out of range".to_string(),
                DataType::Int64 => "bigint out of range".to_string(),
                _ => format!("value of {over} is out of range {t}"), // (a width Postgres has no type for: Arrow's words)
            };
            assert!(format!("{err:#}").contains(&words), "{t}: {err:#}");
        }
        // 300 is out of a TINYINT, and in a SMALLINT
        let b = from_json(b"{\"x\": 300}\n", &table_of(&[("x", DataType::Int16)])).unwrap();
        assert_eq!(b.column(0).as_primitive::<Int16Type>().value(0), 300);
    }

    #[test]
    fn json_nulls_in_a_whole_column_and_in_a_text_column() {
        // i stays whole (read as doubles and cast), b holds a value a double can't (so it is text), s is text. Each keeps its nulls.
        let s = table_of(&[("i", DataType::Int32), ("b", DataType::Int64), ("s", DataType::Utf8)]);
        let body = lines(&[r#"{"i": 5, "b": 9007199254740993, "s": null}"#, r#"{"i": null, "b": null, "s": "x"}"#, "{}"]);
        let b = from_json(&body, &s).unwrap();
        assert_eq!(b.column(0).as_primitive::<Int32Type>().iter().collect::<Vec<_>>(), [Some(5), None, None]);
        assert_eq!(b.column(1).as_primitive::<Int64Type>().iter().collect::<Vec<_>>(), [Some(9007199254740993), None, None]);
        assert_eq!(b.column(2).as_string::<i32>().iter().collect::<Vec<_>>(), [None, Some("x"), None]);
    }

    #[test]
    fn json_a_number_in_a_string_column_is_its_text() {
        let s = table_of(&[("s", DataType::Utf8)]);
        let body = lines(&[r#"{"s": 5}"#, r#"{"s": true}"#, r#"{"s": 2.50}"#, r#"{"s": 1e3}"#, r#"{"s": null}"#, r#"{"s": "x"}"#]);
        let b = from_json(&body, &s).unwrap();
        assert_eq!(b.column(0).as_string::<i32>().iter().collect::<Vec<_>>(), [Some("5"), Some("true"), Some("2.50"), Some("1e3"), None, Some("x")]);
    }

    #[test]
    fn json_refusals_name_what_they_refuse() {
        // A body that isn't JSON is refused in arrow's own words.
        let s = table_of(&[("i", DataType::Int32)]);
        let err = from_json(b"not json\n", &s).unwrap_err();
        assert!(format!("{err:#}").contains("Json error"), "{err:#}");
        // A value that isn't a number is refused by its text, and so is one too long to read.
        for (line, word) in [(r#"{"i": "abc"}"#, "abc"), (r#"{"i": 1e65536}"#, "1e65536")] {
            let err = from_json(&lines(&[line]), &s).unwrap_err();
            assert!(format!("{err:#}").contains(word), "{err:#}");
        }
    }

    /// The speed of the JSON door, in memory (run with `--ignored --nocapture`): 1,000,000 NDJSON rows of four integers and
    /// a string, twice: small integers, and `a` near 10^18, which a double can't hold, so that column is read as text. Three
    /// rounds, each figure the best of five: arrow's `ReaderBuilder` with the table's schema (concatenated as `append_rows`
    /// does), `from_json`, and `from_json`'s two parts with the columns as the body left them: the decoding, and the
    /// conversion of its batches. The two readings must give the same batch.
    #[test]
    #[ignore]
    fn json_speed_in_memory() {
        let schema = table_of(&[("a", DataType::Int64), ("b", DataType::Int32), ("c", DataType::Int64), ("d", DataType::Int32), ("s", DataType::Utf8)]);
        for (name, near) in [("small", false), ("a near 1e18", true)] {
            let body = speed_body(near);
            let body = body.as_bytes();
            let old = || {
                let batches = datafusion::arrow::json::ReaderBuilder::new(schema.clone()).build(body).unwrap().collect::<Result<Vec<_>, _>>().unwrap();
                datafusion::arrow::compute::concat_batches(&schema, &batches).unwrap()
            };
            let new = || from_json(body, &schema).unwrap();
            let (_, text) = json_rows(body, &schema, JSON_ROWS).unwrap();
            let decoded = || decode_all(body, &schema, &text);
            let batches = decoded();
            assert_eq!(old(), new(), "from_json and ReaderBuilder must give the same batch for {name}");
            println!("{name}: integer columns read as text {text:?}");
            for round in 1..=3 {
                let (before, after) = (best_of_five(&old), best_of_five(&new));
                let decode = best_of_five(&decoded);
                let kernel = best_of_five(|| batches.iter().map(|b| whole_batch(b, &schema, &mut text.clone()).unwrap()).count());
                let change = 100.0 * (after.as_secs_f64() / before.as_secs_f64() - 1.0);
                println!("{name}, round {round}: main's path {before:?}, from_json {after:?} ({change:+.1}%), decode {decode:?}, kernel {kernel:?}");
            }
        }
    }

    /// `rows` NDJSON rows of random integers, with `a` near 10^18 when `near`, and a word.
    fn speed_body(near: bool) -> String {
        let mut seed = 0x9e37_79b9_7f4a_7c15_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let letters = b"eeeabcdfghijklmnopqrstuvwxyz"; // (some e's, as the words of a sentence have)
        let mut body = String::new();
        for _ in 0..1_000_000 {
            let a = if near { 1_000_000_000_000_000_000 + (next() % 1_000_000_000) as i64 } else { (next() % 1_000_000) as i64 - 500_000 };
            let b = (next() % 2_000_000_000) as i64 - 1_000_000_000;
            let c = (next() % 1_000_000_000_000) as i64;
            let d = (next() % 2_000_000_000) as i64 - 1_000_000_000;
            let n = 8 + (next() % 13) as usize;
            let word: String = (0..n).map(|_| letters[(next() % letters.len() as u64) as usize] as char).collect();
            body += &format!("{{\"a\": {a}, \"b\": {b}, \"c\": {c}, \"d\": {d}, \"s\": \"{word}\"}}\n");
        }
        body
    }

    /// The batches of `body` as `text` says its integer columns are read, with no conversion: the decoding alone.
    fn decode_all(body: &[u8], schema: &SchemaRef, text: &[bool]) -> Vec<RecordBatch> {
        let mut decoder = json_decoder(schema, text, JSON_ROWS).unwrap();
        let (mut at, mut batches) = (0, Vec::new());
        while at < body.len() {
            let used = decoder.decode(&body[at..]).unwrap();
            batches.extend(decoder.flush().unwrap());
            at += used;
        }
        batches
    }

    /// The fastest of five runs of `read`.
    fn best_of_five<R>(mut read: impl FnMut() -> R) -> std::time::Duration {
        (0..5)
            .map(|_| {
                let start = std::time::Instant::now();
                std::hint::black_box(read());
                start.elapsed()
            })
            .min()
            .unwrap()
    }
}
