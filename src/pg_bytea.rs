//! bytea's text as Postgres 16 has it, read and written in one place: the hex form (`\x…`) and the escape form
//! (`byteain`, and `encode(…, 'escape')` the other way), each refusal with its SQLSTATE. Pure and std only, so the
//! literal rewrite, the Postgres port's values and the JSON writer all share one reading.

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
        Some(hex) => hex_form(hex),
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
fn hex_form(hex: &str) -> Result<Vec<u8>, Bad> {
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

/// The hex digits of `bytes`, two a byte, in lower case.
fn digits(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from(DIGITS[usize::from(b >> 4)]));
        out.push(char::from(DIGITS[usize::from(b & 15)]));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
