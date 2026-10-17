//! `pondra.diff('a', 'b')` in a FROM: table b's rows that differ from a's (ADR-047), written as SQL where SQL comes in.
use anyhow::{bail, Result};

/// `pondra.diff('a', 'b')` in a FROM: the rows of table b that differ from a's, in the change
/// feed's words (`_change_type`: insert, delete, update_preimage, update_postimage). A branch's
/// rows keep their `_row_id` and `_version` (ADR-020), so the rows both hold alike match.
pub fn diffs(sql: &str) -> Result<String> {
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"(?i)\bpondra\.diff\s*\(\s*'([^']*)'\s*,\s*'([^']*)'\s*\)"#).expect("a regex"));
    if !sql.to_ascii_lowercase().contains("pondra.diff") {
        return Ok(sql.to_string());
    }
    static NAME: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"^[\w"$]+(\.[\w"$]+){0,2}$"#).expect("a regex"));
    let mut bad = None;
    let out = CALL.replace_all(sql, |c: &regex::Captures| {
        let (a, b) = (c[1].trim(), c[2].trim());
        if !NAME.is_match(a) || !NAME.is_match(b) {
            bad = Some(format!("pondra.diff takes two tables' names: pondra.diff('prod.sales.orders', 'dev.sales.orders'), not {a:?}, {b:?}"));
        }
        let arm = |kind: &str, t: &str, other: &str, test: &str, changed: bool| {
            let changed = if changed { " AND y._version <> x._version" } else { "" };
            format!("SELECT * FROM (SELECT '{kind}' AS _change_type, x.*, x._row_id, x._version FROM {t} x WHERE {test} (SELECT 1 FROM {other} y WHERE y._row_id = x._row_id{changed}))")
        };
        format!("({} UNION ALL {} UNION ALL {} UNION ALL {})", arm("insert", b, a, "NOT EXISTS", false), arm("update_preimage", a, b, "EXISTS", true),
            arm("update_postimage", b, a, "EXISTS", true), arm("delete", a, b, "NOT EXISTS", false))
    });
    match bad {
        Some(e) => bail!(e),
        None => Ok(out.into_owned()),
    }
}
