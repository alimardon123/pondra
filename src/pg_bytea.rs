//! bytea's text as Postgres 16 has it, read and written in one place: the hex form (`\x…`) and the escape form
//! (`byteain`, and `encode(…, 'escape')` the other way), each refusal with its SQLSTATE. That reading is pure and std
//! only, so the literal rewrite, the Postgres port's values and the JSON writer all share it. Below, Postgres's bytea
//! functions (`get_byte`, `encode`, `position` …) and DataFusion's text functions over bytea too, registered in every
//! session.

use crate::engine::bytes as is_binary;
use crate::pg_functions::{function, int_arg, text_arg};
use crate::pg_json::{is_text, type_word};
use crate::pg_json_make::refuse;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray, Int32Array, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{ColumnarValue, Documentation, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use std::sync::{Arc, LazyLock};

/// Why a text isn't a bytea, with Postgres's SQLSTATE.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bad {
    pub code: &'static str,
    pub message: String,
}

impl std::fmt::Display for Bad {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.message) }
}

impl std::error::Error for Bad {}

/// Postgres writes a bytea's hex digits in lower case.
const DIGITS: &[u8; 16] = b"0123456789abcdef";

/// A bytea from Postgres's text for it: the hex form `\x…` or the escape form. Refused as Postgres refuses it.
pub fn parse(text: &str) -> Result<Vec<u8>, Bad> {
    match text.strip_prefix("\\x") {
        Some(hex) => unhex(hex),
        None => unescape(text),
    }
}

/// The escape form alone, as `decode(…, 'escape')` reads it (no hex form: `\x` is a backslash before x, refused).
pub fn unescape(text: &str) -> Result<Vec<u8>, Bad> {
    let b = text.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut at = 0;
    while at < b.len() {
        if b[at] != b'\\' {
            out.push(b[at]);
            at += 1;
        } else if b.get(at + 1) == Some(&b'\\') {
            out.push(b'\\');
            at += 2;
        } else if let Some(byte) = octal(&b[at..]) {
            out.push(byte);
            at += 4;
        } else {
            return Err(invalid());
        }
    }
    Ok(out)
}

/// The byte of an escape `\ooo` at the start of `s`: a backslash and three octal digits, the first 0 to 3, so the
/// byte is at most 0o377. None when `s` doesn't begin with one.
fn octal(s: &[u8]) -> Option<u8> {
    let digit = |i: usize, most: u8| s.get(i).copied().filter(|c| (b'0'..=b'0' + most).contains(c)).map(|c| c - b'0');
    Some(digit(1, 3)? * 64 + digit(2, 7)? * 8 + digit(3, 7)?)
}

/// The hex form's digits (the text after `\x`): a pair of digits a byte, with whitespace (space, tab, newline, CR)
/// allowed between pairs and never inside one. Refused as byteain refuses it, at the first digit that is wrong.
/// `decode(…, 'hex')` reads its text the same way.
pub fn unhex(hex: &str) -> Result<Vec<u8>, Bad> {
    let mut out = Vec::with_capacity(hex.len() / 2);
    let mut chars = hex.chars();
    while let Some(c) = chars.next() {
        if matches!(c, ' ' | '\t' | '\n' | '\r') {
            continue;
        }
        let high = digit(c)?;
        let Some(low) = chars.next() else {
            return Err(bad("22023", "invalid hexadecimal data: odd number of digits"));
        };
        out.push((high << 4) | digit(low)?);
    }
    Ok(out)
}

fn digit(c: char) -> Result<u8, Bad> {
    c.to_digit(16).map(|d| d as u8).ok_or_else(|| bad("22023", format!("invalid hexadecimal digit: \"{c}\"")))
}

fn invalid() -> Bad { bad("22P02", "invalid input syntax for type bytea") }

fn bad(code: &'static str, message: impl Into<String>) -> Bad { Bad { code, message: message.into() } }

/// A bytea as Postgres writes it: `\x` and lower-case hex.
pub fn text(bytes: &[u8]) -> String { format!("\\x{}", digits(bytes)) }

/// A bytea as `encode(…, 'escape')` writes it: a 0x00 byte and every byte from 0x80 up as `\ooo`, a backslash as
/// `\\`, and every other byte as itself.
pub fn escape(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        match b {
            0 | 0x80..=0xff => out.push_str(&format!("\\{b:03o}")),
            b'\\' => out.push_str("\\\\"),
            _ => out.push(char::from(b)),
        }
    }
    out
}

/// A bytea as a SQL literal DataFusion reads as Binary: `X'01ff'` (`X''` for none).
pub fn sql(bytes: &[u8]) -> String { format!("X'{}'", digits(bytes)) }

/// Postgres's functions whose first argument only a bytea takes: a string literal there is a bytea literal, read as
/// `parse` reads it (friendly.rs's `expression`). `get_bit` and `set_bit` are not here: Postgres has them over a bit
/// string too, which wins for a bare literal.
pub const TAKES_BYTEA: &[&str] = &["sha224", "sha256", "sha384", "sha512", "encode", "get_byte", "set_byte", "convert_from"];

/// The hex digits of `bytes`, two a byte, in lower case.
fn digits(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from(DIGITS[usize::from(b >> 4)]));
        out.push(char::from(DIGITS[usize::from(b & 15)]));
    }
    out
}

/// The bytes as UTF-8 text, or Postgres's 22021 naming them (`convert_from`, and text from the Postgres port).
pub fn utf8(bytes: &[u8]) -> Result<String, Bad> { checked_utf8(bytes).map(str::to_string) }

/// `bytes` as text Postgres takes: UTF-8 with no NUL, refused as Postgres words it (22021, `invalid_at`). It
/// borrows, so a 32 MB COPY is checked without a copy.
pub fn checked_utf8(bytes: &[u8]) -> Result<&str, Bad> {
    // NUL is valid UTF-8 but not in Postgres's text, so the first NUL ends the text.
    let nul = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    match std::str::from_utf8(&bytes[..nul]) {
        Ok(text) if nul == bytes.len() => Ok(text),
        Ok(_) => Err(invalid_at(bytes, nul)),
        Err(e) => Err(invalid_at(bytes, e.valid_up_to())),
    }
}

/// Postgres's refusal for the bytes from `at`: as many as the lead byte says its character has (never more than are
/// left), each as `0x..`.
fn invalid_at(bytes: &[u8], at: usize) -> Bad {
    let width = match bytes[at] {
        0xc0..=0xdf => 2,
        0xe0..=0xef => 3,
        0xf0..=0xf7 => 4,
        _ => 1,
    };
    let shown: Vec<String> = bytes[at..].iter().take(width).map(|b| format!("0x{b:02x}")).collect();
    bad("22021", format!("invalid byte sequence for encoding \"UTF8\": {}", shown.join(" ")))
}

// ---------------------------------------------------------------- the functions

/// A DataFusion function Postgres also has over bytea: a call whose first argument is binary is Postgres's bytea
/// function (`over`), any other call DataFusion's own, as it was. It answers to `name` and `aliases`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct OrBytes {
    inner: Arc<ScalarUDF>,
    over: Over,
    signature: Signature,
    name: String,
    aliases: Vec<String>,
}

/// The bytea function a call is: the one of DataFusion's text functions it stands in for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Over {
    Octets,
    Bits,
    Position,
    Trim(Side),
}

/// The ends a trim takes: `btrim` both, `ltrim` the leading one, `rtrim` the trailing one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Side {
    Both,
    Leading,
    Trailing,
}

impl Over {
    /// The arguments a call takes: one for a length, two for a search or a trim.
    fn arity(self) -> usize {
        match self {
            Over::Octets | Over::Bits => 1,
            Over::Position | Over::Trim(_) => 2,
        }
    }

    /// The type of the answer over bytea: a count, or bytes for a trim.
    fn answer(self) -> DataType {
        match self {
            Over::Trim(_) => DataType::Binary,
            _ => DataType::Int32,
        }
    }
}

/// `inner`, which takes bytea too: a call whose first argument is binary is `over`'s, any other call is `inner`'s.
/// It keeps `inner`'s name and aliases.
fn or_bytes(inner: Arc<ScalarUDF>, over: Over) -> ScalarUDF {
    let (name, aliases) = (inner.name().to_string(), inner.aliases().to_vec());
    with_names(inner, over, name, aliases)
}

/// `or_bytes`, named `name` alone, with no aliases: `length` is registered this way, so DataFusion's `character_length`
/// keeps its own names.
fn or_bytes_as(inner: Arc<ScalarUDF>, over: Over, name: &str) -> ScalarUDF {
    with_names(inner, over, name.to_string(), Vec::new())
}

fn with_names(inner: Arc<ScalarUDF>, over: Over, name: String, aliases: Vec<String>) -> ScalarUDF {
    let signature = Signature::user_defined(inner.signature().volatility);
    ScalarUDF::new_from_impl(OrBytes { inner, over, signature, name, aliases })
}

impl OrBytes {
    /// The answer's type for a call over bytea of `n` arguments; a count the function doesn't take is 42883, as Postgres says.
    fn answer(&self, n: usize) -> DfResult<DataType> {
        if n == self.over.arity() {
            return Ok(self.over.answer());
        }
        let args = vec!["bytea"; n].join(", ");
        Err(refuse("42883", format!("function {}({args}) does not exist", self.name())))
    }
}

impl ScalarUDFImpl for OrBytes {
    fn name(&self) -> &str {
        &self.name
    }
    fn aliases(&self) -> &[String] {
        &self.aliases
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
    fn coerce_types(&self, types: &[DataType]) -> DfResult<Vec<DataType>> {
        match types.first() {
            // Binary stays binary; text stays text, which `literal_bytes` reads as Postgres reads a literal. The count is
            // checked where the answer's type is made: DataFusion turns an error from here into text, code and all.
            Some(first) if is_binary(first) => Ok(types.iter().map(|t| if is_text(t) { t.clone() } else { DataType::Binary }).collect()),
            _ => {
                let fields: Vec<_> = types.iter().map(|t| Arc::new(Field::new("a", t.clone(), true))).collect();
                Ok(fields_with_udf(&fields, self.inner.as_ref())?.iter().map(|f| f.data_type().clone()).collect())
            }
        }
    }
    fn return_type(&self, types: &[DataType]) -> DfResult<DataType> {
        if types.first().is_some_and(is_binary) { self.answer(types.len()) } else { self.inner.return_type(types) }
    }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DfResult<FieldRef> {
        if args.arg_fields.first().is_some_and(|f| is_binary(f.data_type())) {
            let answer = self.answer(args.arg_fields.len())?;
            Ok(Arc::new(Field::new(self.name(), answer, true)))
        } else {
            self.inner.return_field_from_args(args)
        }
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        if !args.args.first().is_some_and(|a| is_binary(&a.data_type())) {
            return self.inner.invoke_with_args(args);
        }
        // Planning refuses a count the function doesn't take; this keeps a call that got here from indexing past its arguments.
        self.answer(args.args.len())?;
        let n = args.number_rows;
        let arrays = args.args.iter().map(|a| a.to_array(n)).collect::<DfResult<Vec<ArrayRef>>>()?;
        Ok(ColumnarValue::Array(bytea_answer(self.over, &arrays, n)?))
    }
}

/// Postgres's answer for every row of a bytea call: `a` holds the call's arguments, `n` rows each.
fn bytea_answer(over: Over, a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let hay = literal_bytes(&a[0])?;
    Ok(match over {
        Over::Octets => int32s((0..n).map(|i| value(&hay, i).map(|v| v.len() as i32))),
        Over::Bits => int32s((0..n).map(|i| value(&hay, i).map(|v| 8 * v.len() as i32))),
        Over::Position => {
            let needle = literal_bytes(&a[1])?;
            int32s((0..n).map(|i| Some(position(value(&hay, i)?, value(&needle, i)?))))
        }
        Over::Trim(side) => {
            let set = literal_bytes(&a[1])?;
            binaries((0..n).map(|i| Some(trim(value(&hay, i)?, value(&set, i)?, side).to_vec())))
        }
    })
}

/// `strpos`, `position` and `instr` over bytea and text. `POSITION(… IN …)` plans to this one (`engine`'s
/// `plan_position`), and `register` registers it.
pub fn strpos() -> Arc<ScalarUDF> {
    static STRPOS: LazyLock<Arc<ScalarUDF>> = LazyLock::new(|| Arc::new(or_bytes(datafusion::functions::unicode::strpos(), Over::Position)));
    STRPOS.clone()
}

/// The functions this module adds to every session: DataFusion's text functions, over bytea too, and Postgres's bytea
/// functions that DataFusion doesn't have. `pg_functions::register` calls it.
pub fn register(ctx: &SessionContext) {
    ctx.register_udf(strpos().as_ref().clone());
    for (inner, over) in [
        (datafusion::functions::string::octet_length(), Over::Octets),
        (datafusion::functions::string::bit_length(), Over::Bits),
        (datafusion::functions::string::btrim(), Over::Trim(Side::Both)),
        (datafusion::functions::string::ltrim(), Over::Trim(Side::Leading)),
        (datafusion::functions::string::rtrim(), Over::Trim(Side::Trailing)),
    ] {
        ctx.register_udf(or_bytes(inner, over));
    }
    // Postgres's `length` takes a bytea and counts its bytes; over text it is DataFusion's count of characters. It is
    // registered after the loop and under no alias, so `length` is this one, while `character_length` and
    // `char_length` stay DataFusion's, since Postgres has no `character_length(bytea)`.
    ctx.register_udf(or_bytes_as(datafusion::functions::unicode::character_length(), Over::Octets, "length"));
    let pure = |n: usize| Signature::any(n, Volatility::Immutable);
    for f in [
        function("get_byte", pure(2), |_| DataType::Int32, get_byte),
        function("set_byte", pure(3), |_| DataType::Binary, set_byte),
        function("get_bit", pure(2), |_| DataType::Int32, get_bit),
        function("set_bit", pure(3), |_| DataType::Binary, set_bit),
        function("convert_to", pure(2), |_| DataType::Binary, convert_to),
        function("convert_from", pure(2), |_| DataType::Utf8, convert_from),
        function("encode", pure(2), |_| DataType::Utf8, encode),
        function("decode", pure(2), |_| DataType::Binary, decode),
    ] {
        ctx.register_udf(f);
    }
}

/// A bytea argument as an array of binary: binary as it is, text read as Postgres reads a literal (`'\x01'` is one
/// byte, not four), NULL where the argument is NULL.
fn literal_bytes(a: &ArrayRef) -> DfResult<BinaryArray> {
    if is_binary(a.data_type()) {
        return Ok(cast(a, &DataType::Binary)?.as_binary::<i32>().clone());
    }
    let texts = cast(a, &DataType::Utf8)?;
    let read = texts
        .as_string::<i32>()
        .iter()
        .map(|v| v.map(|s| parse(s).map_err(as_refusal)).transpose())
        .collect::<DfResult<Vec<_>>>()?;
    Ok(BinaryArray::from_iter(read))
}

/// A bytea function's first argument as binary: binary as it is, text as its UTF-8 bytes (a column's or an
/// expression's text, as DataFusion's `encode` reads it; a string literal there was made a bytea where SQL came in,
/// friendly.rs), and anything else refused 42883 as Postgres refuses it.
fn bytes_arg(name: &str, a: &ArrayRef) -> DfResult<BinaryArray> {
    let a = unpacked(a)?;
    match a.data_type() {
        DataType::Null => Ok(BinaryArray::new_null(a.len())),
        t if is_binary(t) => literal_bytes(&a),
        t if is_text(t) => Ok(cast(&a, &DataType::Binary)?.as_binary::<i32>().clone()),
        t => Err(refuse("42883", format!("function {name}({}, …) does not exist", type_word(t)))),
    }
}

/// A text function's first argument (`decode`, `convert_to`) as text: text as it is, a binary's bytes when they are
/// UTF-8 (DataFusion's `decode` takes a binary; bytes that aren't are refused as `utf8` refuses them, 22021), and
/// anything else refused 42883 as Postgres refuses it.
fn text_first(name: &str, a: &ArrayRef) -> DfResult<Vec<Option<String>>> {
    let a = unpacked(a)?;
    match a.data_type() {
        DataType::Null => Ok(vec![None; a.len()]),
        t if is_text(t) => text_arg(&a),
        t if is_binary(t) => {
            let bytes = cast(&a, &DataType::Binary)?;
            let texts = bytes.as_binary::<i32>().iter().map(|v| v.map(|b| utf8(b).map_err(as_refusal)).transpose());
            texts.collect::<DfResult<Vec<Option<String>>>>()
        }
        t => Err(refuse("42883", format!("function {name}({}, …) does not exist", type_word(t)))),
    }
}

/// A dictionary of text or binary as its values (a Hive partition column of `read_parquet` is one); anything else as
/// it is.
fn unpacked(a: &ArrayRef) -> DfResult<ArrayRef> {
    match a.data_type() {
        DataType::Dictionary(_, values) if is_text(values) || is_binary(values) => Ok(cast(a, values)?),
        _ => Ok(a.clone()),
    }
}

/// A `Bad` as the refusal it is: its SQLSTATE and message.
fn as_refusal(b: Bad) -> DataFusionError {
    refuse(b.code, b.message)
}

/// The bytes of a row, None where it is NULL.
fn value(a: &BinaryArray, i: usize) -> Option<&[u8]> {
    (!a.is_null(i)).then(|| a.value(i))
}

fn int32s(answers: impl IntoIterator<Item = Option<i32>>) -> ArrayRef {
    Arc::new(Int32Array::from_iter(answers))
}

fn binaries(answers: impl IntoIterator<Item = Option<Vec<u8>>>) -> ArrayRef {
    Arc::new(BinaryArray::from_iter(answers))
}

/// The 1-based place of the first `needle` in `hay`: 0 for none, and 1 for an empty needle, as Postgres's `byteapos`.
fn position(hay: &[u8], needle: &[u8]) -> i32 {
    if needle.is_empty() {
        return 1;
    }
    hay.windows(needle.len()).position(|w| w == needle).map_or(0, |p| p as i32 + 1)
}

/// `bytes` without the leading and/or trailing run of bytes that are in `set`.
fn trim<'a>(bytes: &'a [u8], set: &[u8], side: Side) -> &'a [u8] {
    let keep = |b: &u8| !set.contains(b);
    let start = match side {
        Side::Trailing => 0,
        _ => bytes.iter().position(keep).unwrap_or(bytes.len()),
    };
    let end = match side {
        Side::Leading => bytes.len(),
        _ => bytes.iter().rposition(keep).map_or(start, |i| i + 1),
    };
    &bytes[start..end]
}

/// The place `at` in a bytea of `last + 1` places (bytes, or bits), or Postgres's refusal of one past its ends.
fn place(at: i64, last: i64) -> DfResult<usize> {
    if (0..=last).contains(&at) {
        return Ok(at as usize);
    }
    Err(refuse("2202E", format!("index {at} out of valid range, 0..{last}")))
}

fn get_byte(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (data, pos) = (bytes_arg("get_byte", &a[0])?, int_arg(&a[1])?);
    let answers = (0..n)
        .map(|i| match (value(&data, i), pos[i]) {
            (Some(v), Some(at)) => Ok(Some(i32::from(v[place(at, v.len() as i64 - 1)?]))),
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(int32s(answers))
}

fn set_byte(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (data, pos, new) = (bytes_arg("set_byte", &a[0])?, int_arg(&a[1])?, int_arg(&a[2])?);
    let answers = (0..n)
        .map(|i| match (value(&data, i), pos[i], new[i]) {
            (Some(v), Some(at), Some(byte)) => {
                let mut out = v.to_vec();
                out[place(at, v.len() as i64 - 1)?] = byte as u8; // (the low eight bits, as Postgres takes them)
                Ok(Some(out))
            }
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(binaries(answers))
}

fn get_bit(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (data, pos) = (bytes_arg("get_bit", &a[0])?, int_arg(&a[1])?);
    let answers = (0..n)
        .map(|i| match (value(&data, i), pos[i]) {
            (Some(v), Some(at)) => {
                let bit = place(at, v.len() as i64 * 8 - 1)?;
                Ok(Some(i32::from((v[bit / 8] >> (bit % 8)) & 1)))
            }
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(int32s(answers))
}

fn set_bit(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (data, pos, new) = (bytes_arg("set_bit", &a[0])?, int_arg(&a[1])?, int_arg(&a[2])?);
    let answers = (0..n)
        .map(|i| match (value(&data, i), pos[i], new[i]) {
            (Some(v), Some(at), Some(bit)) => {
                let k = place(at, v.len() as i64 * 8 - 1)?;
                if bit != 0 && bit != 1 {
                    return Err(refuse("22023", "new bit must be 0 or 1"));
                }
                let mut out = v.to_vec();
                let mask = 1u8 << (k % 8);
                if bit == 1 {
                    out[k / 8] |= mask;
                } else {
                    out[k / 8] &= !mask;
                }
                Ok(Some(out))
            }
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(binaries(answers))
}

/// Postgres's encoding names, read as it reads them: letters and digits only, case not mattering, so `utf-8`, `UTF8`
/// and `unicode` are UTF8. Only UTF8 and SQL_ASCII are taken; text is UTF-8 either way, so they work alike here.
fn check_encoding(func: &str, name: &str) -> DfResult<()> {
    let key: String = name.chars().filter(char::is_ascii_alphanumeric).map(|c| c.to_ascii_lowercase()).collect();
    match key.as_str() {
        "utf8" | "unicode" | "sqlascii" => Ok(()),
        _ => Err(refuse("0A000", format!("{func}: only UTF8 and SQL_ASCII here, not \"{name}\""))),
    }
}

fn convert_to(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (texts, names) = (text_first("convert_to", &a[0])?, text_arg(&a[1])?);
    let answers = (0..n)
        .map(|i| match (&texts[i], &names[i]) {
            (Some(text), Some(name)) => {
                check_encoding("convert_to", name)?;
                Ok(Some(text.as_bytes().to_vec()))
            }
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(binaries(answers))
}

fn convert_from(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (data, names) = (bytes_arg("convert_from", &a[0])?, text_arg(&a[1])?);
    let answers = (0..n)
        .map(|i| match (value(&data, i), &names[i]) {
            (Some(v), Some(name)) => {
                check_encoding("convert_from", name)?;
                utf8(v).map(Some).map_err(as_refusal)
            }
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<Option<String>>>>()?;
    Ok(Arc::new(StringArray::from(answers)))
}

fn encode(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (data, formats) = (bytes_arg("encode", &a[0])?, text_arg(&a[1])?);
    let answers = (0..n)
        .map(|i| match (value(&data, i), &formats[i]) {
            (Some(v), Some(format)) => encode_as(v, format).map(Some),
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<Option<String>>>>()?;
    Ok(Arc::new(StringArray::from(answers)))
}

fn decode(a: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
    let (texts, formats) = (text_first("decode", &a[0])?, text_arg(&a[1])?);
    let answers = (0..n)
        .map(|i| match (&texts[i], &formats[i]) {
            (Some(text), Some(format)) => decode_as(text, format).map(Some),
            _ => Ok(None),
        })
        .collect::<DfResult<Vec<_>>>()?;
    Ok(binaries(answers))
}

/// `encode(bytes, format)`: the bytes as text in one of Postgres's formats. `base64pad` is DataFusion's name, kept.
fn encode_as(bytes: &[u8], format: &str) -> DfResult<String> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => Ok(digits(bytes)),
        "base64" => Ok(base64_lines(bytes)),
        "base64pad" => Ok(STANDARD.encode(bytes)),
        "escape" => Ok(escape(bytes)),
        _ => Err(unknown_format(format)),
    }
}

/// `decode(text, format)`: the bytes of a text in one of its formats. `base64pad` is DataFusion's name, which Postgres
/// doesn't have; it reads as `base64` does.
fn decode_as(text: &str, format: &str) -> DfResult<Vec<u8>> {
    match format.to_ascii_lowercase().as_str() {
        "hex" => unhex(text).map_err(as_refusal),
        "base64" | "base64pad" => unbase64(text),
        "escape" => unescape(text).map_err(as_refusal),
        _ => Err(unknown_format(format)),
    }
}

fn unknown_format(format: &str) -> DataFusionError {
    refuse("22023", format!("unrecognized encoding: \"{format}\""))
}

/// Postgres's base64 writing: padded, with a line break after every 76th character, even when it is the last.
fn base64_lines(bytes: &[u8]) -> String {
    let plain = STANDARD.encode(bytes);
    let mut out = String::with_capacity(plain.len() + plain.len() / 76);
    for (i, c) in plain.chars().enumerate() {
        out.push(c);
        if (i + 1) % 76 == 0 {
            out.push('\n');
        }
    }
    out
}

/// A base64 character's value: its place in A–Z a–z 0–9 + /, None for any other character.
fn base64_value(c: char) -> Option<u32> {
    Some(match c {
        'A'..='Z' => c as u32 - 'A' as u32,
        'a'..='z' => c as u32 - 'a' as u32 + 26,
        '0'..='9' => c as u32 - '0' as u32 + 52,
        '+' => 62,
        '/' => 63,
        _ => return None,
    })
}

/// Postgres 16's base64 reading (`pg_base64_decode`), as hand-written so each refusal says what Postgres says. Spaces,
/// tabs and line breaks are skipped; `=` ends a group of two or three characters, and a group after it gives as many
/// bytes as the padded one did; a last group that isn't whole is refused.
fn unbase64(text: &str) -> DfResult<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3);
    let (mut buf, mut pos, mut end) = (0u32, 0usize, 0usize);
    for c in text.chars() {
        if matches!(c, ' ' | '\t' | '\n' | '\r') {
            continue;
        }
        let value = if c == '=' {
            if end == 0 {
                match pos {
                    2 => end = 1,
                    3 => end = 2,
                    _ => return Err(refuse("22023", "unexpected \"=\" while decoding base64 sequence")),
                }
            }
            0
        } else {
            base64_value(c).ok_or_else(|| refuse("22023", format!("invalid symbol \"{c}\" found while decoding base64 sequence")))?
        };
        buf = (buf << 6) | value;
        pos += 1;
        if pos == 4 {
            out.push((buf >> 16) as u8);
            if end == 0 || end > 1 {
                out.push((buf >> 8) as u8);
            }
            if end == 0 || end > 2 {
                out.push(buf as u8);
            }
            (buf, pos) = (0, 0);
        }
    }
    if pos != 0 {
        return Err(refuse("22023", "invalid base64 end sequence"));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codes::Coded;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::Int32Type;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionConfig;

    /// The SQLSTATE and message of a text that isn't a bytea.
    fn refused(text: &str) -> (&'static str, String) {
        let e = parse(text).expect_err(text);
        (e.code, e.message)
    }

    #[test]
    fn postgres_16_reads_both_forms_as_it_does() {
        assert_eq!(text(&parse(r"\x 01 ff").unwrap()), r"\x01ff");
        assert_eq!(text(&parse("\\x01\n02\t03\r\n04").unwrap()), r"\x01020304");
        assert_eq!(refused(r"\x01 f f"), ("22023", "invalid hexadecimal digit: \" \"".to_string()));
        assert_eq!(text(&parse(r"a\\b\001\377").unwrap()), r"\x615c6201ff");
        assert_eq!(text(&parse("é").unwrap()), r"\xc3a9");
        assert_eq!(text(&parse(r"ab\\c\101").unwrap()), r"\x61625c6341");
        assert_eq!(text(&parse("ff00").unwrap()), r"\x66663030");
        assert_eq!(text(&parse(r"\x").unwrap()), r"\x");
    }

    #[test]
    fn encode_escape_writes_as_postgres_does() {
        assert_eq!(escape(&parse(r"a\\b\001\377 ~").unwrap()), "a\\\\b\u{1}\\377 ~");
        assert_eq!(escape(&[0]), r"\000");
        assert_eq!(escape(&[0x80, 0xff]), r"\200\377");
        assert_eq!(escape(b"a\\b~ "), r"a\\b~ ");
    }

    #[test]
    fn hex_form_refusals_carry_their_code_and_message() {
        assert_eq!(refused(r"\x0g"), ("22023", "invalid hexadecimal digit: \"g\"".to_string()));
        assert_eq!(refused(r"\xg0"), ("22023", "invalid hexadecimal digit: \"g\"".to_string()));
        assert_eq!(refused(r"\x0"), ("22023", "invalid hexadecimal data: odd number of digits".to_string()));
        assert_eq!(refused(r"\x0 "), ("22023", "invalid hexadecimal digit: \" \"".to_string()));
        assert_eq!(refused("\\xé0"), ("22023", "invalid hexadecimal digit: \"é\"".to_string()));
    }

    #[test]
    fn escape_form_refusals_carry_their_code_and_message() {
        let invalid = ("22P02", "invalid input syntax for type bytea".to_string());
        assert_eq!(refused(r"a\b"), invalid);
        assert_eq!(refused(r"\400"), invalid);
        assert_eq!(refused(r"\X01"), invalid);
        assert_eq!(refused(r"\8"), invalid);
        assert_eq!(refused(r"\12"), invalid);
        assert_eq!(refused(r"a\"), invalid);
        assert_eq!(parse(r"a\b").unwrap_err().to_string(), "invalid input syntax for type bytea");
    }

    #[test]
    fn the_escape_form_has_no_hex_form() {
        // (`\x41` is the hex form to `parse`, and a backslash before x to `unescape`)
        assert_eq!(parse(r"\x41").unwrap(), vec![0x41]);
        assert_eq!(unescape(r"\x41").unwrap_err().code, "22P02");
        assert_eq!(unescape(r"\\001").unwrap(), vec![b'\\', b'0', b'0', b'1']);
    }

    #[test]
    fn empty_and_the_letters_of_both_forms() {
        assert_eq!(text(&[]), r"\x");
        assert_eq!(sql(&[]), "X''");
        assert_eq!(escape(&[]), "");
        assert_eq!(parse("").unwrap(), Vec::<u8>::new());
        assert_eq!(parse(r"\x").unwrap(), Vec::<u8>::new());
        assert_eq!(text(&[0xab, 0x01]), r"\xab01");
        assert_eq!(parse(r"\xFF").unwrap(), vec![0xff]);
        assert_eq!(sql(&[0x01, 0xff]), "X'01ff'");
    }

    #[test]
    fn every_byte_round_trips_in_both_forms() {
        let all: Vec<u8> = (0..=255).collect();
        assert_eq!(parse(&text(&all)).unwrap(), all);
        assert_eq!(unescape(&escape(&all)).unwrap(), all);
        for b in 0..=255u8 {
            assert_eq!(parse(&text(&[b])).unwrap(), vec![b]);
            assert_eq!(unescape(&escape(&[b])).unwrap(), vec![b], "byte {b}");
        }
    }

    fn bin(rows: &[Option<&[u8]>]) -> ArrayRef { Arc::new(BinaryArray::from_iter(rows.iter().copied())) }
    fn int(rows: &[Option<i64>]) -> ArrayRef { Arc::new(Int64Array::from(rows.to_vec())) }
    fn txt(rows: &[Option<&str>]) -> ArrayRef { Arc::new(StringArray::from(rows.to_vec())) }

    /// A body's answer as its rows (a panic on a refusal; `refusal` reads those).
    fn ints(r: DfResult<ArrayRef>) -> Vec<Option<i32>> { r.unwrap().as_primitive::<Int32Type>().iter().collect() }
    fn bins(r: DfResult<ArrayRef>) -> Vec<Option<Vec<u8>>> { r.unwrap().as_binary::<i32>().iter().map(|v| v.map(<[u8]>::to_vec)).collect() }
    fn strs(r: DfResult<ArrayRef>) -> Vec<Option<String>> { r.unwrap().as_string::<i32>().iter().map(|v| v.map(str::to_string)).collect() }
    fn refusal(r: DfResult<ArrayRef>) -> (&'static str, String) {
        match r {
            Err(e) => refusal_of(&e),
            Ok(_) => panic!("answered, not refused"),
        }
    }

    /// The SQLSTATE and message of the refusal an error holds (empty for an error that holds none). The analyzer
    /// wraps a rule's error in `Context`, which `iter()` doesn't walk, so the chain is followed here.
    fn refusal_of(e: &DataFusionError) -> (&'static str, String) {
        e.iter().find_map(coded_in).unwrap_or(("", String::new()))
    }

    fn coded_in(e: &DataFusionError) -> Option<(&'static str, String)> {
        match e {
            DataFusionError::External(x) => x.downcast_ref::<Coded>().map(|c| (c.0, c.1.clone())),
            DataFusionError::Context(_, inner) => coded_in(inner),
            DataFusionError::Shared(inner) => coded_in(inner),
            _ => None,
        }
    }

    #[test]
    fn get_and_set_byte_and_bit_as_postgres_do() {
        assert_eq!(ints(get_byte(&[bin(&[Some(&[1, 255])]), int(&[Some(1)])], 1)), vec![Some(255)]);
        assert_eq!(bins(set_byte(&[bin(&[Some(&[1, 255])]), int(&[Some(0)]), int(&[Some(255)])], 1)), vec![Some(vec![255, 255])]);
        assert_eq!(bins(set_byte(&[bin(&[Some(&[1])]), int(&[Some(0)]), int(&[Some(511)])], 1)), vec![Some(vec![255])]);
        assert_eq!(ints(get_bit(&[bin(&[Some(&[1])]), int(&[Some(0)])], 1)), vec![Some(1)]);
        assert_eq!(ints(get_bit(&[bin(&[Some(&[1])]), int(&[Some(7)])], 1)), vec![Some(0)]);
        assert_eq!(bins(set_bit(&[bin(&[Some(&[1])]), int(&[Some(1)]), int(&[Some(1)])], 1)), vec![Some(vec![3])]);
        assert_eq!(bins(set_bit(&[bin(&[Some(&[3])]), int(&[Some(0)]), int(&[Some(0)])], 1)), vec![Some(vec![2])]);
    }

    #[test]
    fn byte_and_bit_refusals_carry_their_words() {
        let at = |bytes: &[u8], pos: i64| -> DfResult<ArrayRef> { get_byte(&[bin(&[Some(bytes)]), int(&[Some(pos)])], 1) };
        assert_eq!(refusal(at(&[1], 5)), ("2202E", "index 5 out of valid range, 0..0".to_string()));
        assert_eq!(refusal(at(&[], 0)), ("2202E", "index 0 out of valid range, 0..-1".to_string()));
        let bit = |pos: i64, new: i64| -> DfResult<ArrayRef> { set_bit(&[bin(&[Some(&[1])]), int(&[Some(pos)]), int(&[Some(new)])], 1) };
        assert_eq!(refusal(bit(8, 1)), ("2202E", "index 8 out of valid range, 0..7".to_string()));
        assert_eq!(refusal(bit(1, 2)), ("22023", "new bit must be 0 or 1".to_string()));
        assert_eq!(refusal(get_bit(&[bin(&[Some(&[1])]), int(&[Some(8)])], 1)), ("2202E", "index 8 out of valid range, 0..7".to_string()));
    }

    #[test]
    fn nulls_in_give_nulls_out() {
        assert_eq!(ints(get_byte(&[bin(&[None]), int(&[Some(0)])], 1)), vec![None]);
        assert_eq!(ints(get_byte(&[bin(&[Some(&[1])]), int(&[None])], 1)), vec![None]);
        assert_eq!(bins(set_bit(&[bin(&[Some(&[1])]), int(&[None]), int(&[Some(1)])], 1)), vec![None]);
        assert_eq!(strs(encode(&[bin(&[None]), txt(&[Some("hex")])], 1)), vec![None]);
    }

    #[test]
    fn position_and_trim_as_postgres_do() {
        let position_of = |hay: &[u8], needle: ArrayRef| ints(bytea_answer(Over::Position, &[bin(&[Some(hay)]), needle], 1));
        assert_eq!(position_of(&[1, 255], bin(&[Some(&[255])])), vec![Some(2)]);
        assert_eq!(position_of(&[1, 255], txt(&[Some("\\xff")])), vec![Some(2)]);
        assert_eq!(position_of(&[1], bin(&[Some(&[])])), vec![Some(1)]);
        assert_eq!(position_of(&[1], bin(&[Some(&[9])])), vec![Some(0)]);
        assert_eq!(trim(&[0, 1, 0, 0], &[0], Side::Both), &[1]);
        assert_eq!(trim(&[0, 1, 0], &[0], Side::Leading), &[1, 0]);
        assert_eq!(trim(&[0, 1, 0], &[0], Side::Trailing), &[0, 1]);
        assert_eq!(trim(&[0, 0], &[0], Side::Both), &[] as &[u8]);
        assert_eq!(trim(&[1, 2], &[], Side::Both), &[1, 2]);
    }

    #[test]
    fn lengths_of_bytes() {
        assert_eq!(ints(bytea_answer(Over::Octets, &[bin(&[Some(&[1, 255])])], 1)), vec![Some(2)]);
        assert_eq!(ints(bytea_answer(Over::Octets, &[bin(&[Some(&[0xc3, 0xa9])])], 1)), vec![Some(2)]);
        assert_eq!(ints(bytea_answer(Over::Bits, &[bin(&[Some(&[1, 255])])], 1)), vec![Some(16)]);
    }

    #[test]
    fn a_text_argument_is_read_as_a_literal() {
        let read = literal_bytes(&txt(&[Some("\\x01"), Some("\\xc3a9"), None])).unwrap();
        assert_eq!(read.iter().map(|v| v.map(<[u8]>::to_vec)).collect::<Vec<_>>(), vec![Some(vec![1]), Some(vec![0xc3, 0xa9]), None]);
        match literal_bytes(&txt(&[Some("\\x0")])) {
            Err(e) => assert_eq!(refusal_of(&e).0, "22023"),
            Ok(_) => panic!("an odd hex digit count answered"),
        }
    }

    #[test]
    fn encodings_as_postgres_spells_them() {
        assert_eq!(bins(convert_to(&[txt(&[Some("josé")]), txt(&[Some("UTF8")])], 1)), vec![Some("josé".as_bytes().to_vec())]);
        assert!(check_encoding("convert_to", "utf-8").is_ok());
        assert!(check_encoding("convert_to", "UNICODE").is_ok());
        assert!(check_encoding("convert_to", "SQL_ASCII").is_ok());
        assert_eq!(refusal_of(&check_encoding("convert_to", "LATIN1").unwrap_err()), ("0A000", "convert_to: only UTF8 and SQL_ASCII here, not \"LATIN1\"".to_string()));
    }

    #[test]
    fn utf8_is_checked_and_refused_by_bytes() {
        assert_eq!(strs(convert_from(&[bin(&[Some(&[0xc3, 0xa9])]), txt(&[Some("UTF8")])], 1)), vec![Some("é".to_string())]);
        let refused_for = |bytes: &[u8]| refusal(convert_from(&[bin(&[Some(bytes)]), txt(&[Some("UTF8")])], 1));
        assert_eq!(refused_for(&[0xff]), ("22021", "invalid byte sequence for encoding \"UTF8\": 0xff".to_string()));
        assert_eq!(refused_for(&[0xc3, 0x28]), ("22021", "invalid byte sequence for encoding \"UTF8\": 0xc3 0x28".to_string()));
        assert_eq!(refused_for(&[0xe2, 0x82]), ("22021", "invalid byte sequence for encoding \"UTF8\": 0xe2 0x82".to_string()));
        assert_eq!(refused_for(&[0x00]), ("22021", "invalid byte sequence for encoding \"UTF8\": 0x00".to_string()));
        assert_eq!(refusal(convert_from(&[bin(&[Some(&[0x61])]), txt(&[Some("LATIN1")])], 1)).0, "0A000");
    }

    #[test]
    fn base64_breaks_its_lines_at_76_as_postgres_does() {
        assert_eq!(encode_as(&[0; 57], "base64").unwrap(), format!("{}\n", "A".repeat(76)));
        assert_eq!(encode_as(&[0; 58], "base64").unwrap(), format!("{}\nAA==", "A".repeat(76)));
        assert_eq!(encode_as(&[0; 57], "base64pad").unwrap(), "A".repeat(76));
        assert_eq!(encode_as(&[0], "base64").unwrap(), "AA==");
    }

    #[test]
    fn base64_reads_what_postgres_reads() {
        assert_eq!(decode_as("YWJj", "base64").unwrap(), b"abc");
        assert_eq!(decode_as("YW Jj\n", "BASE64").unwrap(), b"abc");
        let refused = |text: &str| refusal_of(&decode_as(text, "base64").unwrap_err());
        assert_eq!(refused("Y=Q="), ("22023", "unexpected \"=\" while decoding base64 sequence".to_string()));
        assert_eq!(refused("Y!"), ("22023", "invalid symbol \"!\" found while decoding base64 sequence".to_string()));
        assert_eq!(refused("YWJjé"), ("22023", "invalid symbol \"é\" found while decoding base64 sequence".to_string()));
        assert_eq!(refused("Y"), ("22023", "invalid base64 end sequence".to_string()));
    }

    #[test]
    fn unbase64_reads_as_postgres_16_does() {
        assert_eq!(unbase64("YQ==").unwrap(), b"a");
        assert_eq!(unbase64("YWI=").unwrap(), b"ab");
        assert_eq!(unbase64("YWJj").unwrap(), b"abc");
        assert_eq!(unbase64("YW Jj\n").unwrap(), b"abc");
        assert_eq!(unbase64("YQ==YQ==").unwrap(), b"aa");
        assert_eq!(unbase64("WB==").unwrap(), b"X");
        assert_eq!(unbase64("").unwrap(), Vec::<u8>::new());
        assert_eq!(unbase64("dG9tbXk=").unwrap(), b"tommy");
    }

    #[test]
    fn unbase64_refuses_as_postgres_16_does() {
        let refused = |text: &str| refusal_of(&unbase64(text).unwrap_err());
        let end = ("22023", "invalid base64 end sequence".to_string());
        for text in ["invalid", "YQ", "YQ==Y", "cXdlcXc=="] {
            assert_eq!(refused(text), end, "{text}");
        }
        assert_eq!(refused("Y=Q="), ("22023", "unexpected \"=\" while decoding base64 sequence".to_string()));
        assert_eq!(refused("YQ$="), ("22023", "invalid symbol \"$\" found while decoding base64 sequence".to_string()));
        assert_eq!(refused("YQé="), ("22023", "invalid symbol \"é\" found while decoding base64 sequence".to_string()));
    }

    #[test]
    fn hex_and_escape_decode_and_the_unknown_format() {
        assert_eq!(decode_as("0102", "hex").unwrap(), vec![1, 2]);
        assert_eq!(refusal_of(&decode_as("0", "hex").unwrap_err()).0, "22023");
        assert_eq!(decode_as("a\\001", "escape").unwrap(), vec![b'a', 1]);
        assert_eq!(refusal_of(&encode_as(&[1], "nope").unwrap_err()), ("22023", "unrecognized encoding: \"nope\"".to_string()));
        assert_eq!(refusal_of(&decode_as("", "nope").unwrap_err()).1, "unrecognized encoding: \"nope\"");
        assert_eq!(encode_as(&[1, 255], "HEX").unwrap(), "01ff");
    }

    /// Runs `sql` in a session and returns its one value, as the session's rows show it.
    async fn value_of(ctx: &SessionContext, sql: &str) -> String {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        array_value_to_string(batches[0].column(0), 0).unwrap()
    }

    /// The refusal `sql` raises, when planning or running it.
    async fn refused_by(ctx: &SessionContext, sql: &str) -> (&'static str, String) {
        match ctx.sql(sql).await {
            Err(e) => refusal_of(&e),
            Ok(df) => match df.collect().await {
                Err(e) => refusal_of(&e),
                Ok(_) => panic!("{sql} answered, not refused"),
            },
        }
    }

    #[tokio::test]
    async fn a_session_answers_bytea_as_postgres_does() {
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let cases = [
            ("SELECT position(X'ff' IN X'01ff')", "2"),
            ("SELECT position('\\xff' IN X'01ff')", "2"),
            ("SELECT trim(both X'00' from X'000100')", "01"),
            ("SELECT ltrim(X'0001', X'00')", "01"),
            ("SELECT octet_length(X'01ff')", "2"),
            ("SELECT length(X'c3a9')", "2"),
            ("SELECT strpos(X'01ff', X'ff')", "2"),
            ("SELECT bit_length(X'01ff')", "16"),
            ("SELECT substring(X'0102ff' from 2)", "02ff"),
            ("SELECT get_byte(X'01ff', 1)", "255"),
            ("SELECT encode(set_byte(X'01ff', 0, 255), 'hex')", "ffff"),
            ("SELECT get_bit(X'01', 0)", "1"),
            ("SELECT encode(set_bit(X'01', 1, 1), 'hex')", "03"),
            ("SELECT encode(X'00', 'base64')", "AA=="),
            ("SELECT encode(X'01ff', 'escape')", "\u{1}\\377"),
            ("SELECT encode(decode('YWJj', 'base64'), 'hex')", "616263"),
            ("SELECT convert_from(X'c3a9', 'UTF8')", "é"),
            ("SELECT encode(convert_to('josé', 'UTF8'), 'hex')", "6a6f73c3a9"),
            ("SELECT strpos('josé', 'é')", "4"),
            ("SELECT position('é' IN 'josé')", "4"),
            ("SELECT length('josé')", "4"),
            ("SELECT octet_length('josé')", "5"),
            ("SELECT trim('  a ')", "a"),
            ("SELECT encode('abc', 'hex')", "616263"),
        ];
        for (sql, want) in cases {
            assert_eq!(value_of(&ctx, sql).await, want, "{sql}");
        }
        assert_eq!(refused_by(&ctx, "SELECT substring(X'01' from 1 for -1)").await.0, "22011");
        assert_eq!(refused_by(&ctx, "SELECT get_byte(X'01', 5)").await.0, "2202E");
        assert_eq!(refused_by(&ctx, "SELECT convert_from(X'ff', 'UTF8')").await.0, "22021");
        assert_eq!(refused_by(&ctx, "SELECT btrim(X'00')").await.0, "42883");
    }

    /// Whether `sql`'s one answer is NULL.
    async fn null_of(ctx: &SessionContext, sql: &str) -> bool {
        let batches = ctx.sql(sql).await.unwrap().collect().await.unwrap();
        batches[0].column(0).is_null(0)
    }

    #[tokio::test]
    async fn a_session_refuses_and_answers_arguments_as_postgres_16_does() {
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        assert_eq!(refused_by(&ctx, "SELECT encode(12, 'hex')").await, ("42883", "function encode(integer, …) does not exist".to_string()));
        assert_eq!(refused_by(&ctx, "SELECT convert_to(12, 'UTF8')").await.0, "42883");
        assert_eq!(refused_by(&ctx, "SELECT decode(12, 'hex')").await.0, "42883");
        assert_eq!(refused_by(&ctx, "SELECT decode(X'8f50', 'base64')").await.0, "22021");
        assert_eq!(refused_by(&ctx, "SELECT decode('invalid', 'base64')").await.0, "22023");
        refused_by(&ctx, "SELECT char_length(X'c328')").await;
        for sql in ["SELECT encode(NULL, 'hex')", "SELECT decode(NULL, 'hex')", "SELECT get_byte(NULL, 0)"] {
            assert!(null_of(&ctx, sql).await, "{sql} is not NULL");
        }
        assert_eq!(value_of(&ctx, "SELECT decode(X'59513d3d', 'base64')").await, "61");
        assert_eq!(value_of(&ctx, "SELECT length(X'c328')").await, "2");
        assert_eq!(value_of(&ctx, "SELECT length('é')").await, "1");
        assert_eq!(value_of(&ctx, "SELECT octet_length(X'c328')").await, "2");
        assert_eq!(value_of(&ctx, "SELECT encode(arrow_cast('tom', 'Utf8View'), 'base64')").await, "dG9t");
        // (a dictionary of text, as a Hive partition column of read_parquet is, reads as its values)
        assert_eq!(value_of(&ctx, "SELECT encode(arrow_cast('tom', 'Dictionary(Int32, Utf8)'), 'hex')").await, "746f6d");
    }
}
