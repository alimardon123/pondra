//! A file's (or a manifest's) columns' least and greatest values, typed: each in Iceberg's single-value form
//! (its spec's Appendix D), so a reader takes them as they are and nothing parses text a query at a time.
//! `DataFile::stats`, the text form, is what every lake from before holds: `Ranged::range` reads either.
use crate::data_file::Stats;
use base64::Engine;
use datafusion::arrow::datatypes::{DataType, TimeUnit};
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Column (by its stored name: ADR-022) -> (min, max), each in Iceberg's single-value form.
pub type Bounds = BTreeMap<String, (Bound, Bound)>;

/// Column -> its (min, max) as values: a file's as it is written (`manifest::Seen`), a table's whole (`manifest::ranges`).
pub type Values = BTreeMap<String, (ScalarValue, ScalarValue)>;

/// Ranges as a file or a manifest keeps them: typed (`typed`: its lake is at format 8, `format::BOUNDS`, so every node
/// reads them), else as text, which a node from before reads. A string past 64 bytes is left out either way, as `text`
/// leaves it out: an entry stays small.
pub fn kept(values: &Values, typed: bool) -> (Stats, Bounds) {
    let short = |v: &ScalarValue| encode(v).filter(|b| b.len() <= 64 || !matches!(v, ScalarValue::Utf8(_) | ScalarValue::Utf8View(_) | ScalarValue::LargeUtf8(_)));
    match typed {
        true => (Stats::new(), values.iter().filter_map(|(c, (lo, hi))| Some((c.clone(), (Bound(short(lo)?), Bound(short(hi)?))))).collect()),
        false => (values.iter().filter_map(|(c, (lo, hi))| Some((c.clone(), (crate::data_file::text(lo)?, crate::data_file::text(hi)?)))).collect(), Bounds::new()),
    }
}

/// One value's bytes (`encode`), kept in a catalog entry as base64.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bound(pub Vec<u8>);

impl Serialize for Bound {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> { s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(&self.0)) }
}

impl<'de> Deserialize<'de> for Bound {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // (decoded where it lies in the text, not copied out first: a manifest holds thousands)
        struct Base64;
        impl serde::de::Visitor<'_> for Base64 {
            type Value = Bound;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result { f.write_str("a bound in base64") }
            fn visit_str<E: serde::de::Error>(self, s: &str) -> Result<Bound, E> { base64::engine::general_purpose::STANDARD.decode(s).map(Bound).map_err(E::custom) }
        }
        d.deserialize_str(Base64)
    }
}

/// A value's bytes: boolean one byte; int and date 4 little-endian, long, timestamp (µs) and timestamp_ns 8; float and
/// double their IEEE bits little-endian; decimal its unscaled value, two's complement big-endian in the fewest bytes;
/// string its UTF-8. A type without an Iceberg twin goes as the one that holds it exactly (a byte or a short as an int, an
/// unsigned int as a long, seconds and milliseconds as microseconds, a half float as a float), and the rest (an unsigned
/// long, a 64-bit date) as 8 bytes little-endian: `decode` reads them by the column's own type. None: a null, or a type
/// without an order.
pub fn encode(v: &ScalarValue) -> Option<Vec<u8>> {
    use ScalarValue as S;
    let widened = |t: DataType| encode(&v.cast_to(&t).ok()?);
    Some(match v {
        S::Boolean(Some(x)) => vec![*x as u8],
        S::Int32(Some(x)) | S::Date32(Some(x)) => x.to_le_bytes().to_vec(),
        S::Int64(Some(x)) | S::TimestampMicrosecond(Some(x), _) | S::TimestampNanosecond(Some(x), _) | S::Date64(Some(x)) => x.to_le_bytes().to_vec(),
        S::TimestampSecond(Some(x), _) => x.checked_mul(1_000_000)?.to_le_bytes().to_vec(),
        S::TimestampMillisecond(Some(x), _) => x.checked_mul(1_000)?.to_le_bytes().to_vec(),
        S::UInt64(Some(x)) => x.to_le_bytes().to_vec(),
        S::Float32(Some(x)) => x.to_le_bytes().to_vec(),
        S::Float64(Some(x)) => x.to_le_bytes().to_vec(),
        S::Decimal128(Some(x), ..) => minimal(*x),
        S::Utf8(Some(s)) | S::Utf8View(Some(s)) | S::LargeUtf8(Some(s)) => s.as_bytes().to_vec(),
        S::Int8(Some(_)) | S::Int16(Some(_)) | S::UInt8(Some(_)) | S::UInt16(Some(_)) => widened(DataType::Int32)?,
        S::UInt32(Some(_)) => widened(DataType::Int64)?,
        S::Float16(Some(_)) => widened(DataType::Float32)?,
        _ => return None,
    })
}

/// `x` in the fewest bytes of two's complement, big-endian (Iceberg's decimal).
fn minimal(x: i128) -> Vec<u8> {
    let b = x.to_be_bytes();
    let sign = if x < 0 { 0xff } else { 0 };
    // (a leading byte can go while it only repeats the sign, and the next byte's top bit still says it)
    let skip = (0..15).take_while(|&i| b[i] == sign && (b[i + 1] & 0x80 == sign & 0x80)).count();
    b[skip..].to_vec()
}

/// The value bytes `b` hold, as a value of a column of type `t` (`encode`'s inverse). A 4-byte int, float or date read as
/// a long, a double or a 64-bit one, as Iceberg widens a column's type. None: bytes that don't fit the type, or a
/// value out of its range.
pub fn decode(b: &[u8], t: &DataType) -> Option<ScalarValue> {
    use ScalarValue as S;
    let le4 = || Some(i32::from_le_bytes(b.try_into().ok()?));
    let le8 = || Some(i64::from_le_bytes(b.try_into().ok()?));
    let long = || match b.len() { 4 => le4().map(i64::from), _ => le8() };
    let narrowed = |from: S| from.cast_to(t).ok().filter(|v| !v.is_null());
    let exact = |us: i64, per: i64| (us % per == 0).then_some(us / per); // (a bound from seconds or milliseconds)
    Some(match t {
        DataType::Boolean if b.len() == 1 => S::Boolean(Some(b[0] != 0)),
        DataType::Int32 => S::Int32(Some(le4()?)),
        DataType::Date32 => S::Date32(Some(le4()?)),
        DataType::Int64 => S::Int64(Some(long()?)),
        DataType::Date64 => S::Date64(Some(le8()?)),
        DataType::UInt64 => S::UInt64(Some(u64::from_le_bytes(b.try_into().ok()?))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => S::TimestampMicrosecond(Some(le8()?), tz.clone()),
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => S::TimestampNanosecond(Some(le8()?), tz.clone()),
        DataType::Timestamp(TimeUnit::Millisecond, tz) => S::TimestampMillisecond(Some(exact(le8()?, 1_000)?), tz.clone()),
        DataType::Timestamp(TimeUnit::Second, tz) => S::TimestampSecond(Some(exact(le8()?, 1_000_000)?), tz.clone()),
        DataType::Float32 => S::Float32(Some(f32::from_le_bytes(b.try_into().ok()?))),
        DataType::Float64 => match b.len() {
            4 => S::Float64(Some(f32::from_le_bytes(b.try_into().ok()?) as f64)),
            _ => S::Float64(Some(f64::from_le_bytes(b.try_into().ok()?))),
        },
        DataType::Decimal128(p, s) if (1..=16).contains(&b.len()) => {
            let mut full = [if b[0] & 0x80 != 0 { 0xff } else { 0 }; 16];
            full[16 - b.len()..].copy_from_slice(b);
            S::Decimal128(Some(i128::from_be_bytes(full)), *p, *s)
        }
        DataType::Utf8 => S::Utf8(Some(String::from_utf8(b.to_vec()).ok()?)),
        DataType::Utf8View => S::Utf8View(Some(String::from_utf8(b.to_vec()).ok()?)),
        DataType::LargeUtf8 => S::LargeUtf8(Some(String::from_utf8(b.to_vec()).ok()?)),
        DataType::Int8 | DataType::Int16 | DataType::UInt8 | DataType::UInt16 => narrowed(S::Int32(Some(le4()?)))?,
        DataType::UInt32 => narrowed(S::Int64(Some(long()?)))?,
        DataType::Float16 => narrowed(S::Float32(Some(f32::from_le_bytes(b.try_into().ok()?))))?,
        _ => return None,
    })
}

/// What keeps its columns' ranges: a file, a manifest, or a bare map of text ranges. A column's range is read from its
/// typed bounds, else from its text form (a lake from before, or a file not yet rewritten).
pub trait Ranged {
    fn bounds(&self) -> Option<&Bounds>;
    fn stats(&self) -> &Stats;

    /// Column `col`'s (stored name) least value (`max`: greatest), as a value of type `t`.
    fn end(&self, col: &str, t: &DataType, max: bool) -> Option<ScalarValue> {
        match self.bounds().and_then(|b| b.get(col)) {
            Some((lo, hi)) => decode(&if max { hi } else { lo }.0, t),
            None => self.stats().get(col).and_then(|(lo, hi)| ScalarValue::try_from_string(if max { hi } else { lo }.clone(), t).ok()),
        }
    }

    /// Column `col`'s min and max, as values of type `t`.
    fn range(&self, col: &str, t: &DataType) -> Option<(ScalarValue, ScalarValue)> { self.end(col, t, false).zip(self.end(col, t, true)) }

    /// A 64-bit integer column's min and max (a system column's: `_row_id`, `_version`).
    fn longs(&self, col: &str) -> Option<(i64, i64)> {
        match self.range(col, &DataType::Int64)? {
            (ScalarValue::Int64(Some(lo)), ScalarValue::Int64(Some(hi))) => Some((lo, hi)),
            _ => None,
        }
    }
}

impl Ranged for Stats {
    fn bounds(&self) -> Option<&Bounds> { None }
    fn stats(&self) -> &Stats { self }
}

impl Ranged for crate::data_file::DataFile {
    fn bounds(&self) -> Option<&Bounds> { Some(&self.bounds) }
    fn stats(&self) -> &Stats { &self.stats }
}

#[cfg(feature = "lake")]
impl Ranged for crate::manifest::Manifest {
    fn bounds(&self) -> Option<&Bounds> { Some(&self.bounds) }
    fn stats(&self) -> &Stats { &self.stats }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::DataType as T;
    use ScalarValue as S;

    /// Every orderable type's values go to bytes and back to the same value, and the types with an Iceberg twin go to the
    /// bytes Iceberg's spec gives (Appendix D).
    #[test]
    fn a_value_goes_to_icebergs_bytes_and_back() {
        let tz: Option<std::sync::Arc<str>> = Some("UTC".into());
        let cases: Vec<(S, Option<Vec<u8>>)> = vec![
            (S::Boolean(Some(true)), Some(vec![1])),
            (S::Int32(Some(-2)), Some(vec![0xfe, 0xff, 0xff, 0xff])),
            (S::Date32(Some(20_000)), Some(20_000i32.to_le_bytes().to_vec())),
            (S::Int64(Some(1 << 40)), Some((1i64 << 40).to_le_bytes().to_vec())),
            (S::TimestampMicrosecond(Some(1_700_000_000_000_000), tz.clone()), Some(1_700_000_000_000_000i64.to_le_bytes().to_vec())),
            (S::TimestampNanosecond(Some(-5), None), Some((-5i64).to_le_bytes().to_vec())),
            (S::TimestampMillisecond(Some(1_700_000_000_123), None), Some(1_700_000_000_123_000i64.to_le_bytes().to_vec())),
            (S::TimestampSecond(Some(-3), tz), Some((-3_000_000i64).to_le_bytes().to_vec())),
            (S::Float32(Some(1.5)), Some(1.5f32.to_le_bytes().to_vec())),
            (S::Float64(Some(-0.25)), Some((-0.25f64).to_le_bytes().to_vec())),
            (S::Decimal128(Some(12_345), 10, 2), Some(vec![0x30, 0x39])),
            (S::Decimal128(Some(-129), 10, 2), Some(vec![0xff, 0x7f])),
            (S::Decimal128(Some(128), 10, 2), Some(vec![0x00, 0x80])),
            (S::Decimal128(Some(-1), 38, 0), Some(vec![0xff])),
            (S::Decimal128(Some(0), 38, 0), Some(vec![0x00])),
            (S::Decimal128(Some(i128::MIN), 38, 0), Some(i128::MIN.to_be_bytes().to_vec())),
            (S::Utf8(Some("añb".into())), Some("añb".as_bytes().to_vec())),
            (S::Utf8View(Some("x".into())), Some(b"x".to_vec())),
            (S::LargeUtf8(Some(String::new())), Some(vec![])),
            (S::Int8(Some(-7)), Some((-7i32).to_le_bytes().to_vec())),
            (S::Int16(Some(300)), Some(300i32.to_le_bytes().to_vec())),
            (S::UInt8(Some(255)), Some(255i32.to_le_bytes().to_vec())),
            (S::UInt16(Some(65_535)), Some(65_535i32.to_le_bytes().to_vec())),
            (S::UInt32(Some(u32::MAX)), Some((u32::MAX as i64).to_le_bytes().to_vec())),
            (S::UInt64(Some(u64::MAX)), Some(u64::MAX.to_le_bytes().to_vec())),
            (S::Date64(Some(86_400_000)), Some(86_400_000i64.to_le_bytes().to_vec())),
            (S::Int32(None), None),
        ];
        for (v, want) in cases {
            let got = encode(&v);
            assert_eq!(got, want, "{v:?}");
            if let Some(b) = got {
                assert_eq!(decode(&b, &v.data_type()), Some(v.clone()), "{v:?} back");
            }
        }
        let half = S::Float32(Some(0.5)).cast_to(&T::Float16).unwrap();
        assert_eq!(decode(&encode(&half).unwrap(), &T::Float16), Some(half));
    }

    /// Bytes as another engine writes them: a column widened since (an int's 4 bytes read as a long, a float's as a
    /// double), and bytes that don't fit the column's type read as no value.
    #[test]
    fn widened_and_unfit_bytes() {
        assert_eq!(decode(&(-9i32).to_le_bytes(), &T::Int64), Some(S::Int64(Some(-9))));
        assert_eq!(decode(&2.5f32.to_le_bytes(), &T::Float64), Some(S::Float64(Some(2.5))));
        assert_eq!(decode(&[1, 2, 3], &T::Int32), None);
        assert_eq!(decode(&[1, 2], &T::Boolean), None);
        assert_eq!(decode(&[0xff, 0xfe], &T::Utf8), None, "not UTF-8");
        assert_eq!(decode(&300i32.to_le_bytes(), &T::Int8), None, "out of an Int8's range");
        assert_eq!(decode(&1_500i64.to_le_bytes(), &T::Timestamp(TimeUnit::Millisecond, None)), None, "not whole milliseconds");
        assert_eq!(decode(&[], &T::Decimal128(10, 2)), None);
        assert_eq!(decode(&[0; 17], &T::Decimal128(38, 2)), None);
    }

    /// A range is read from the typed bounds where a column has them, else from its text; a bound kept in a catalog entry
    /// is base64.
    #[test]
    fn a_range_reads_the_bounds_then_the_text() {
        let mut f = crate::data_file::DataFile::default();
        f.stats.insert("a".into(), ("1".into(), "9".into()));
        f.stats.insert("b".into(), ("x".into(), "y".into()));
        f.bounds.insert("b".into(), (Bound(b"p".to_vec()), Bound(b"q".to_vec())));
        assert_eq!(f.range("a", &T::Int32), Some((S::Int32(Some(1)), S::Int32(Some(9)))));
        assert_eq!(f.range("b", &T::Utf8), Some((S::Utf8(Some("p".into())), S::Utf8(Some("q".into())))), "the bounds win");
        assert_eq!(f.range("c", &T::Utf8), None);
        assert_eq!(f.longs("a"), Some((1, 9)));
        let json = serde_json::to_string(&f.bounds).unwrap();
        assert_eq!(json, r#"{"b":["cA==","cQ=="]}"#);
        let back: Bounds = serde_json::from_str(&json).unwrap();
        assert_eq!(back, f.bounds);
        let entry = serde_json::to_value(crate::data_file::DataFile::default()).unwrap();
        assert!(entry.get("bounds").is_none(), "a file without bounds says nothing of them");
    }
}
