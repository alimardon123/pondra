//! COPY FROM's rows as Postgres reads them: the text format's lines, fields and escapes (text_columns), and each
//! field's text as its column's type (typed), a bytea's as Postgres reads one.

use crate::codes::coded;
use crate::pg_bytea;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BinaryArray, StringBuilder};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

/// The fields of `chunk`, in COPY's text format, as one text array per column (`names`, which says how many),
/// read as Postgres reads them: a line a row (a `\r` before its `\n` dropped); fields split at `delimiter` except
/// where a backslash escapes it; a field that is `null` as written is NULL;
/// then `\b \f \n \r \t \v`, a backslash and one to three octal digits, `\x` and one or two hex digits (a byte
/// each), and a backslash before anything else, a newline too, is that thing. `header` skips the first line.
pub(crate) fn text_columns(chunk: &str, names: &[String], delimiter: u8, null: &str, header: bool) -> anyhow::Result<Vec<ArrayRef>> {
    let bytes = chunk.as_bytes();
    let mut columns: Vec<StringBuilder> = names.iter().map(|_| StringBuilder::new()).collect();
    let (mut fields, mut read) = (Vec::new(), Vec::new());
    let (mut at, mut first) = (0, true);
    while at < bytes.len() {
        at = split_line(bytes, at, delimiter, &mut fields);
        if std::mem::replace(&mut first, false) && header {
            continue;
        }
        if fields.len() != names.len() {
            return Err(width(names, fields.len()));
        }
        for ((range, escaped), column) in fields.iter().zip(columns.iter_mut()) {
            let raw = &bytes[range.clone()];
            if raw == null.as_bytes() {
                column.append_null();
            } else if *escaped {
                unescape(raw, &mut read);
                column.append_value(pg_bytea::checked_utf8(&read).map_err(|b| coded(b.code, b.message))?);
            } else {
                column.append_value(chunk.get(range.clone()).ok_or_else(|| anyhow::anyhow!("a COPY field splits inside a character"))?);
            }
        }
    }
    Ok(columns.into_iter().map(|mut c| Arc::new(c.finish()) as ArrayRef).collect())
}

/// The line at `at` as its fields: each field's bytes as written (its range) and whether a backslash is among them.
/// A backslash takes the byte after it, so `\<delimiter>` and `\<newline>` stay in the field; a `\r` just before the
/// line's newline is not part of the field. Returns where the next line starts.
fn split_line(bytes: &[u8], at: usize, delimiter: u8, fields: &mut Vec<(Range<usize>, bool)>) -> usize {
    fields.clear();
    let (mut start, mut escaped, mut cr, mut i) = (at, false, false, at);
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' {
            escaped = true;
            cr = false;
            i += 2; // (a backslash that is the chunk's last byte is kept as it is: the loop ends here)
            continue;
        }
        if b == b'\n' {
            let end = if cr { i - 1 } else { i };
            fields.push((start..end, escaped));
            return i + 1;
        }
        if b == delimiter {
            fields.push((start..i, escaped));
            (start, escaped, cr) = (i + 1, false, false);
        } else {
            cr = b == b'\r';
        }
        i += 1;
    }
    fields.push((start..bytes.len(), escaped));
    bytes.len()
}

/// A field's bytes with its escapes read, as Postgres's text format reads them (`out` is cleared and filled).
fn unescape(raw: &[u8], out: &mut Vec<u8>) {
    out.clear();
    let mut i = 0;
    while i < raw.len() {
        if raw[i] != b'\\' || i + 1 == raw.len() {
            out.push(raw[i]); // (a backslash last in the chunk is one)
            i += 1;
            continue;
        }
        let e = raw[i + 1];
        i += 2;
        match e {
            b'b' => out.push(8),
            b'f' => out.push(12),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(11),
            b'0'..=b'7' => {
                let mut value = u32::from(e - b'0');
                for _ in 0..2 {
                    match raw.get(i) {
                        Some(&d) if (b'0'..=b'7').contains(&d) => {
                            value = value * 8 + u32::from(d - b'0');
                            i += 1;
                        }
                        _ => break,
                    }
                }
                out.push(value as u8);
            }
            b'x' => {
                let (mut value, mut digits) = (0u32, 0);
                while digits < 2 {
                    match raw.get(i).and_then(|&d| char::from(d).to_digit(16)) {
                        Some(d) => {
                            value = value * 16 + d;
                            i += 1;
                            digits += 1;
                        }
                        None => break,
                    }
                }
                if digits == 0 {
                    out.push(b'x'); // (`\x` with no digit after it is an x)
                } else {
                    out.push(value as u8);
                }
            }
            _ => out.push(e),
        }
    }
}

/// Whether the byte at `at` is not escaped: an even run of backslashes right before it.
fn unescaped(data: &[u8], at: usize) -> bool { data[..at].iter().rev().take_while(|&&b| b == b'\\').count() % 2 == 0 }

/// Where the last whole row of `data` ends, just after its last newline that doesn't end a row's data: in text
/// format one that no backslash escapes, in CSV one outside quotes (an even number of `"` before it); 0 when there is none.
pub(crate) fn rows_end(data: &[u8], csv: bool) -> usize {
    if csv {
        // (the quotes before each newline, counted back from the end a stretch at a time: each byte is counted once)
        let mut quotes = data.iter().filter(|&&b| b == b'"').count();
        let mut stretch = data.len();
        while let Some(i) = data[..stretch].iter().rposition(|&b| b == b'\n') {
            quotes -= data[i..stretch].iter().filter(|&&b| b == b'"').count();
            stretch = i;
            if quotes % 2 == 0 {
                return i + 1;
            }
        }
        return 0;
    }
    let mut at = data.len();
    while let Some(i) = data[..at].iter().rposition(|&b| b == b'\n') {
        if unescaped(data, i) {
            return i + 1;
        }
        at = i; // (the newline is a row's data: look for the row's end before it)
    }
    0
}

/// The first `\.` in `data` at or after `from`, at its backslash.
fn next_dot(data: &[u8], mut from: usize) -> Option<usize> {
    while let Some(p) = data.get(from..)?.iter().position(|&b| b == b'\\') {
        let at = from + p;
        if data.get(at + 1) == Some(&b'.') {
            return Some(at);
        }
        from = at + 1;
    }
    None
}

/// Where COPY's end marker `\.` starts in `data`, or None: in text format an unescaped one followed by a newline or the
/// data's end (other text after it is 22P04), in CSV one that is a line of its own outside quotes; any other `\.` is data.
pub(crate) fn data_end(data: &[u8], csv: bool) -> anyhow::Result<Option<usize>> {
    if !data.contains(&b'\\') {
        return Ok(None); // (no backslash, no marker: a fast check first, since most COPY data has no escapes)
    }
    let (mut at, mut quoted_to, mut quotes) = (0, 0, 0);
    while let Some(i) = next_dot(data, at) {
        let after = &data[i + 2..];
        let ends = after.is_empty() || after.starts_with(b"\n") || after.starts_with(b"\r\n");
        if csv {
            quotes += data[quoted_to..i].iter().filter(|&&b| b == b'"').count();
            quoted_to = i;
            if (i == 0 || (data[i - 1] == b'\n' && quotes % 2 == 0)) && ends {
                return Ok(Some(i));
            }
        } else if unescaped(data, i) {
            if ends {
                return Ok(Some(i));
            }
            return Err(coded("22P04", "end-of-copy marker corrupt"));
        }
        at = i + 1;
    }
    Ok(None)
}

/// `text` (CSV, starting at a line's start outside quotes) with `""` written at each blank line outside quotes.
/// Postgres reads a blank line as one empty unquoted field, which is NULL under the default NULL; Arrow's reader
/// skips a blank line, but reads `""` as empty text, which its NULL test makes NULL the same way.
pub(crate) fn csv_blank_lines(text: &str) -> Cow<'_, str> {
    // (a blank line is a newline that starts a line, so it follows a newline or opens the text: look for those first)
    if !(text.starts_with('\n') || text.starts_with("\r\n") || text.contains("\n\n") || text.contains("\n\r\n")) {
        return Cow::Borrowed(text);
    }
    let bytes = text.as_bytes();
    let (mut quoted, mut line, mut copied) = (false, 0, 0); // (`line`: where the current line starts, outside quotes)
    let (mut out, mut written) = (String::new(), false);
    let mut at = 0;
    while let Some(p) = bytes[at..].iter().position(|&b| b == b'\n' || b == b'"') {
        let i = at + p;
        at = i + 1;
        if bytes[i] == b'"' {
            quoted = !quoted;
            continue;
        }
        if !quoted {
            if line == i || (line + 1 == i && bytes[line] == b'\r') {
                out.push_str(&text[copied..line]);
                out.push_str("\"\"");
                (copied, written) = (line, true);
            }
            line = i + 1;
        }
    }
    if !written {
        return Cow::Borrowed(text);
    }
    out.push_str(&text[copied..]);
    Cow::Owned(out)
}

/// Postgres's refusal for a COPY line with `got` fields where `names` has its columns (22P04): a line short of
/// one names the column it lacks, a line with a field too many doesn't.
pub(crate) fn width(names: &[String], got: usize) -> anyhow::Error {
    match names.get(got) {
        Some(name) => coded("22P04", format!("missing data for column \"{name}\"")),
        None => coded("22P04", "extra data after last expected column"),
    }
}

/// `columns`, text as a COPY's reader gave them, as `schema`'s columns: a column already of its type as it is, a
/// bytea's text read as Postgres reads one (`pg_bytea::parse`), and anything else cast as INSERT casts a value
/// (`as_type`).
pub(crate) fn typed(columns: Vec<ArrayRef>, schema: &SchemaRef) -> anyhow::Result<RecordBatch> {
    let cols = columns.iter().zip(schema.fields()).map(|(column, field)| as_type(column, field.as_ref())).collect::<anyhow::Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

/// `column` as `field`'s type, by `query::strict` (the write cast, which refuses a value as Postgres does: its
/// SQLSTATE kept), with the column named as Postgres names it in a COPY's CONTEXT.
fn as_type(column: &ArrayRef, field: &Field) -> anyhow::Result<ArrayRef> {
    let target = field.data_type();
    if column.data_type() == target {
        return Ok(column.clone());
    }
    if matches!(target, DataType::Binary | DataType::LargeBinary | DataType::BinaryView) {
        let texts = cast(column.as_ref(), &DataType::Utf8)?;
        let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(texts.len());
        for text in texts.as_string::<i32>().iter() {
            values.push(match text {
                Some(text) => Some(pg_bytea::parse(text).map_err(|b| coded(b.code, format!("{} (column {}: \"{text}\")", b.message, field.name())))?),
                None => None,
            });
        }
        let bytes = BinaryArray::from_iter(values.iter().map(|v| v.as_deref()));
        return Ok(cast(&bytes, target)?);
    }
    crate::query::strict(column, target).map_err(|e| {
        let e = anyhow::Error::from(e);
        coded(crate::codes::of(&e), format!("{} (column {})", crate::codes::words(&e), field.name()))
    })
}

/// Postgres's refusal of a COPY option that must be one byte (DELIMITER, QUOTE, ESCAPE) and isn't, `'é'` or `''`:
/// sqlparser takes one byte there and refuses the statement, so this names the option as Postgres does (0A000).
pub(crate) fn not_one_byte(sql: &str) -> Option<anyhow::Error> {
    static OPTION: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\b(delimiter|quote|escape)\s+(?:as\s+)?'((?:[^']|'')*)'").expect("a pattern"));
    let c = OPTION.captures_iter(sql).find(|c| c[2].replace("''", "'").len() != 1)?;
    Some(coded("0A000", format!("COPY {} must be a single one-byte character", c[1].to_lowercase())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int64Array, StringArray};
    use datafusion::arrow::datatypes::Schema;

    fn names() -> [String; 2] { ["id".to_string(), "s".to_string()] }

    /// The columns of `text` (delimited by `delimiter`), each as its values: None for a NULL.
    fn columns(text: &str, delimiter: u8) -> Vec<Vec<Option<String>>> {
        let given = text_columns(text, &names(), delimiter, r"\N", false).expect("columns");
        given.iter().map(|c| c.as_string::<i32>().iter().map(|v| v.map(str::to_string)).collect()).collect()
    }

    /// Values as they are written in a test: `None` is NULL.
    fn sv(xs: &[Option<&str>]) -> Vec<Option<String>> { xs.iter().map(|x| x.map(str::to_string)).collect() }

    /// The SQLSTATE and message of a COPY text that is refused.
    fn refused(text: &str, delimiter: u8) -> (&'static str, String) {
        let Err(e) = text_columns(text, &names(), delimiter, r"\N", false) else { panic!("{text:?} answered") };
        (crate::codes::of(&e), e.to_string())
    }

    fn one(name: &str, t: DataType) -> SchemaRef { Arc::new(Schema::new(vec![Field::new(name, t, true)])) }

    #[test]
    fn text_format_reads_escapes_as_postgres_does() {
        let text = r"1	a\tb
2	line\nbreak
3	back\\slash
4	\N
5	\\N
";
        let got = columns(text, b'\t');
        assert_eq!(got[0], sv(&[Some("1"), Some("2"), Some("3"), Some("4"), Some("5")]));
        assert_eq!(got[1], sv(&[Some("a\tb"), Some("line\nbreak"), Some(r"back\slash"), None, Some(r"\N")]));
    }

    #[test]
    fn octal_hex_and_an_escaped_delimiter_in_a_field() {
        let got = columns(r"1	a\|b\101\x41\q
", b'\t');
        assert_eq!(got[1], sv(&[Some("a|bAAq")]));
        let got = columns(r"x\|y|z
", b'|');
        assert_eq!(got[0], sv(&[Some("x|y")]));
        assert_eq!(got[1], sv(&[Some("z")]));
    }

    #[test]
    fn hex_takes_up_to_two_digits_and_an_x_without_one_is_an_x() {
        assert_eq!(columns(r"1	\x4g
", b'\t')[1], sv(&[Some("\u{4}g")]));
        assert_eq!(columns(r"1	\xg
", b'\t')[1], sv(&[Some("xg")]));
    }

    #[test]
    fn a_backslash_before_a_newline_puts_the_newline_in_the_field() {
        let got = columns(r"1	a\
b
2	c
", b'\t');
        assert_eq!(got[0], sv(&[Some("1"), Some("2")]));
        assert_eq!(got[1], sv(&[Some("a\nb"), Some("c")]));
    }

    #[test]
    fn rows_end_is_after_the_last_row_a_newline_ends() {
        assert_eq!(rows_end(b"1\tx\n2\ty", false), 4);
        assert_eq!(rows_end(br"1	x\
", false), 0);
        let data = br"1	x\\
";
        assert_eq!(rows_end(data, false), data.len());
        assert_eq!(rows_end(b"1\tx", false), 0);
    }

    #[test]
    fn csv_rows_end_is_after_the_last_newline_outside_quotes() {
        assert_eq!(rows_end(b"\"a\nb\"\nc", true), 6);
        assert_eq!(rows_end(b"\"a\nb\nc", true), 0);
        assert_eq!(rows_end(b"1,x\n2,\"y\n", true), 4);
    }

    #[test]
    fn data_end_in_text_format() {
        assert_eq!(data_end(b"\\.\n1\n", false).unwrap(), Some(0));
        assert_eq!(data_end(b"1\n\\.\n", false).unwrap(), Some(2));
        assert_eq!(data_end(b"1\n\\.", false).unwrap(), Some(2));
        assert_eq!(data_end(b"1\\.\n2\n", false).unwrap(), Some(1));
        assert_eq!(data_end(b"a\\\n\\.\nb\n", false).unwrap(), Some(3));
        assert_eq!(data_end(b"1\n\\\\.\n2\n", false).unwrap(), None);
        assert_eq!(data_end(b"1\n\\.\r\n2\n", false).unwrap(), Some(2));
        assert_eq!(data_end(b"1\n2\n", false).unwrap(), None);
        assert_eq!(crate::codes::of(&data_end(b"1\n\\.\\.\n", false).expect_err("\\.\\.")), "22P04");
        let e = data_end(b"1\\.x\n2\n", false).expect_err("\\.x");
        assert_eq!((crate::codes::of(&e), e.to_string()), ("22P04", "end-of-copy marker corrupt".to_string()));
    }

    #[test]
    fn data_end_in_csv_format() {
        assert_eq!(data_end(b"a\n\\.\n", true).unwrap(), Some(2));
        assert_eq!(data_end(b"a\n\\.\nb\n", true).unwrap(), Some(2));
        assert_eq!(data_end(b"a\r\n\\.\r\nb\n", true).unwrap(), Some(3));
        assert_eq!(data_end(b"a\n\\.x\nb\n", true).unwrap(), None);
        assert_eq!(data_end(b" \\.\nb\n", true).unwrap(), None);
        assert_eq!(data_end(b"\"a\n\\.\nb\"\nc\n", true).unwrap(), None);
        assert_eq!(data_end(b"\"a\n\"\n\\.\n", true).unwrap(), Some(5));
        assert_eq!(data_end(b"1,x\n2,y\n", true).unwrap(), None);
    }

    #[test]
    fn csv_blank_lines_get_an_empty_quoted_field() {
        assert_eq!(&*csv_blank_lines("a\n\nb\n"), "a\n\"\"\nb\n");
        assert_eq!(&*csv_blank_lines("\n\n"), "\"\"\n\"\"\n");
        assert_eq!(&*csv_blank_lines("a\n\r\nb\n"), "a\n\"\"\r\nb\n");
        assert_eq!(&*csv_blank_lines("\r\nx\n"), "\"\"\r\nx\n");
        assert_eq!(&*csv_blank_lines(" \na\n"), " \na\n");
        assert!(matches!(csv_blank_lines("\"a\n\nb\"\nc\n"), Cow::Borrowed(_)));
        assert_eq!(&*csv_blank_lines("\"a\n\"\n\n"), "\"a\n\"\n\"\"\n");
        assert_eq!(&*csv_blank_lines("é\n\nü\n"), "é\n\"\"\nü\n");
    }

    #[test]
    fn csv_rows_end_counts_quotes_from_the_end() {
        assert_eq!(rows_end(b"\"a\nb\"\nc\n\"x\n", true), 8);
        assert_eq!(rows_end(b"a\n\"b\nc\n", true), 2);
        assert_eq!(rows_end(b"a\n\"b\nc\"\n", true), 8);
    }

    #[test]
    fn crlf_line_ends_and_the_header() {
        let got = columns("1\tx\r\n2\ty\r\n", b'\t');
        assert_eq!(got[0], sv(&[Some("1"), Some("2")]));
        assert_eq!(got[1], sv(&[Some("x"), Some("y")]));
        let with_header = text_columns("id\ts\n1\ta\n", &names(), b'\t', r"\N", true).expect("header");
        assert_eq!(with_header[0].as_string::<i32>().iter().collect::<Vec<_>>(), vec![Some("1")]);
        let data = b"1\ta\n\\.\n2\tb\n";
        let end = data_end(data, false).expect("marker").expect("found");
        assert_eq!(columns(std::str::from_utf8(&data[..end]).unwrap(), b'\t')[0], sv(&[Some("1")]));
    }

    #[test]
    fn an_out_of_range_number_is_22003_and_other_text_is_22p02() {
        let text = |xs: &[Option<&str>]| -> ArrayRef { Arc::new(StringArray::from(xs.to_vec())) };
        let refusal = |xs: &[Option<&str>], t: DataType| {
            let e = typed(vec![text(xs)], &one("n", t)).expect_err("refused");
            (crate::codes::of(&e), e.to_string())
        };
        assert_eq!(refusal(&[Some("1"), Some("99999999999")], DataType::Int32), ("22003", "value \"99999999999\" is out of range for type integer (column n)".to_string()));
        assert_eq!(refusal(&[Some("40000")], DataType::Int16).0, "22003");
        assert_eq!(refusal(&[Some("-40000")], DataType::Int16), ("22003", "value \"-40000\" is out of range for type smallint (column n)".to_string()));
        assert_eq!(refusal(&[Some("99999999999999999999")], DataType::Int64), ("22003", "value \"99999999999999999999\" is out of range for type bigint (column n)".to_string()));
        assert_eq!(refusal(&[Some("12345")], DataType::Decimal128(3, 1)), ("22003", "numeric field overflow (column n)".to_string()));
        assert_eq!(refusal(&[Some("abc")], DataType::Int32), ("22P02", "invalid input syntax for type integer: \"abc\" (column n)".to_string()));
        assert_eq!(refusal(&[Some("99999999999"), Some("abc")], DataType::Int32).0, "22003", "the first refused text decides");
        assert_eq!(refusal(&[Some("abc"), Some("99999999999")], DataType::Int32).0, "22P02");
        assert!(typed(vec![text(&[Some("1.2")])], &one("n", DataType::Decimal128(3, 1))).is_ok());
        match typed(vec![text(&[Some(" 12 ")])], &one("n", DataType::Int32)) {
            Ok(b) => assert_eq!(b.column(0).as_primitive::<datafusion::arrow::datatypes::Int32Type>().value(0), 12),
            Err(e) => assert_eq!(crate::codes::of(&e), "22P02"),
        }
    }

    #[test]
    fn refusals_carry_postgres_codes_and_words() {
        assert_eq!(refused("1\n", b'\t'), ("22P04", "missing data for column \"s\"".to_string()));
        assert_eq!(refused("1\t2\t3\n", b'\t'), ("22P04", "extra data after last expected column".to_string()));
        assert_eq!(refused(r"1	\x80
", b'\t'), ("22021", "invalid byte sequence for encoding \"UTF8\": 0x80".to_string()));
        assert_eq!(refused(r"1	\000
", b'\t'), ("22021", "invalid byte sequence for encoding \"UTF8\": 0x00".to_string()));
    }

    #[test]
    fn checked_utf8_refuses_what_postgres_refuses() {
        let e = pg_bytea::checked_utf8(b"1\t\xc3(").unwrap_err();
        assert_eq!((e.code, e.message.as_str()), ("22021", "invalid byte sequence for encoding \"UTF8\": 0xc3 0x28"));
        assert_eq!(pg_bytea::checked_utf8("é".as_bytes()).unwrap(), "é");
    }

    #[test]
    fn typed_reads_bytea_text_and_refuses_what_it_cannot_read() {
        let text: ArrayRef = Arc::new(StringArray::from(vec![Some(r"\x01ff"), Some("abc"), None]));
        let b = typed(vec![text], &one("v", DataType::Binary)).expect("bytea");
        assert_eq!(b.column(0).as_binary::<i32>().iter().collect::<Vec<_>>(), vec![Some(&[1u8, 255][..]), Some(&b"abc"[..]), None]);
        let bad: ArrayRef = Arc::new(StringArray::from(vec![Some(r"ab\c")]));
        let e = typed(vec![bad], &one("v", DataType::Binary)).expect_err("ab\\c");
        assert_eq!(crate::codes::of(&e), "22P02");
    }

    #[test]
    fn typed_casts_numbers_and_booleans_and_refuses_the_rest() {
        let text = |xs: &[Option<&str>]| -> ArrayRef { Arc::new(StringArray::from(xs.to_vec())) };
        let b = typed(vec![text(&[Some("12")])], &one("n", DataType::Int32)).expect("int");
        assert_eq!(b.column(0).as_primitive::<datafusion::arrow::datatypes::Int32Type>().value(0), 12);
        let e = typed(vec![text(&[Some("x")])], &one("n", DataType::Int32)).expect_err("x");
        assert_eq!(crate::codes::of(&e), "22P02");
        let b = typed(vec![text(&[Some("t")])], &one("b", DataType::Boolean)).expect("bool");
        assert!(b.column(0).as_boolean().value(0));
    }

    #[test]
    fn a_column_already_of_its_type_is_kept_as_it_is() {
        let column: ArrayRef = Arc::new(Int64Array::from(vec![1, 2]));
        let b = typed(vec![column.clone()], &one("n", DataType::Int64)).expect("kept");
        assert!(Arc::ptr_eq(b.column(0), &column));
    }

    #[test]
    fn an_option_of_more_or_less_than_one_byte_is_refused_as_postgres_refuses_it() {
        let refused = |sql: &str| super::not_one_byte(sql).map(|e| (crate::codes::of(&e), e.to_string()));
        assert_eq!(refused("COPY t FROM STDIN WITH (DELIMITER 'é')"), Some(("0A000", "COPY delimiter must be a single one-byte character".into())));
        assert_eq!(refused("COPY t FROM STDIN WITH (FORMAT csv, QUOTE '')").map(|r| r.1), Some("COPY quote must be a single one-byte character".into()));
        assert_eq!(refused("copy t from stdin delimiter as '||'").map(|r| r.1), Some("COPY delimiter must be a single one-byte character".into()));
        assert_eq!(refused("COPY t FROM STDIN WITH (FORMAT csv, ESCAPE '''', DELIMITER ';')"), None, "'''' is one quote");
    }
}
