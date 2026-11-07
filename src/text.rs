//! SQL as text: names as SQL resolves them, a table's name inside the lake, a statement written back.
use anyhow::{bail, ensure, Result};
use datafusion::sql::sqlparser::ast::{self, Expr, FunctionArg, FunctionArgExpr, Query, SelectItem, SetExpr, Statement, Value};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::keywords::Keyword;
use datafusion::sql::sqlparser::tokenizer::{Location, Span, Token, Tokenizer};
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::LazyLock;

/// A name as SQL resolves it: its parts, unquoted ones in lower case, joined by dots.
pub fn object(n: &ast::ObjectName) -> String {
    let part = |p: &ast::ObjectNamePart| p.as_ident().map(ident).unwrap_or_else(|| p.to_string());
    n.0.iter().map(part).collect::<Vec<_>>().join(".")
}

/// A name as SQL resolves it (`S` is `s`, `"S"` is `S`), as `nextval('…')`, `ALTER` and `DROP TYPE` give it.
pub fn object_of(name: &str) -> String { parts(name).join(".") }

/// A name's parts as SQL reads them, written as text: a quoted part as written (`"My.s"` is one part, `"a""b"` is `a"b`),
/// an unquoted one in lower case, split at dots and spaces. The one splitter of a name in text. (An unquoted part is
/// anything but those: `my-db`, as Pondra's names after a statement's head take it, `path::NAME`.)
pub(crate) fn parts(name: &str) -> Vec<String> {
    static PART: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#""(?:[^"]|"")*"|[^."\s]+"#).expect("a regex"));
    PART.find_iter(name).map(|m| match m.as_str().strip_prefix('"').and_then(|p| p.strip_suffix('"')) {
        Some(quoted) => quoted.replace("\"\"", "\""),
        None => m.as_str().to_lowercase(),
    }).collect()
}

/// A database's name as written (`"Dev"`, `dev`): a database is named in lower case, quoted or not.
pub(crate) fn database(name: &str) -> String { name.trim_matches('"').to_lowercase() }

/// One part of a name: as written if quoted, else in lower case.
pub fn ident(i: &ast::Ident) -> String { if i.quote_style.is_some() { i.value.clone() } else { i.value.to_lowercase() } }

/// `sql` from its first word on: past leading whitespace and comments (`-- …`, `/* … */`, which nest as the tokenizer
/// reads them: `/* a /* b */ c */` is one). Every look at where a statement's code starts reads it here (`status.rs`,
/// `audit.rs`; 11-06 audit M7: each skipped comments its own way).
pub fn first_word(sql: &str) -> &str {
    let mut s = sql.trim_start();
    loop {
        if let Some(rest) = s.strip_prefix("--") {
            s = rest.find('\n').map_or("", |i| &rest[i + 1..]).trim_start();
        } else if s.starts_with("/*") {
            s = s[comment_end(s)..].trim_start();
        } else {
            return s;
        }
    }
}

/// Where the comment `s` starts with ends: past the `*/` that closes its first `/*`, those between nesting (the text's
/// end, when none does).
fn comment_end(s: &str) -> usize {
    let (b, mut depth, mut i) = (s.as_bytes(), 0, 0);
    while i + 1 < b.len() {
        match (b[i], b[i + 1]) {
            (b'/', b'*') => (depth, i) = (depth + 1, i + 2),
            (b'*', b'/') => {
                (depth, i) = (depth - 1, i + 2);
                if depth == 0 {
                    return i;
                }
            }
            _ => i += 1,
        }
    }
    s.len()
}

/// The first `n` words of `sql` past its leading comments, upper-cased, with a `;` at its end taken off: the words
/// `control` reads, so a word is whole (`merged` is not `MERGE`).
pub fn first_words(sql: &str, n: usize) -> Vec<String> {
    first_word(sql).trim_end_matches(';').split_whitespace().take(n).map(|w| w.to_uppercase()).collect()
}

/// Whether `words` begin with `pattern`'s words (`first_words` gives them upper-cased, as a pattern is written).
pub fn begins(words: &[String], pattern: &[&str]) -> bool {
    words.len() >= pattern.len() && words.iter().zip(pattern).all(|(word, p)| word == p)
}

/// The words a `BEGIN` may take after its first (`control`).
const BEGIN_WORDS: [&str; 14] = ["WORK", "TRANSACTION", "ISOLATION", "LEVEL", "READ", "WRITE", "ONLY", "REPEATABLE", "SERIALIZABLE", "COMMITTED", "UNCOMMITTED", "SNAPSHOT", "DEFERRABLE", "NOT"];

/// Is a ROLLBACK's first words `[WORK | TRANSACTION] TO`? (`words` holds at most three.)
fn to_point(words: &[String]) -> bool {
    let at = |i: usize| words.get(i).map(String::as_str);
    at(1) == Some("TO") || (matches!(at(1), Some("WORK" | "TRANSACTION")) && at(2) == Some("TO"))
}

/// Is a BEGIN's or COMMIT's first words SQL Server's `TRAN` (or `TRANSACTION` with a name after it)? (`words` holds at most three.)
fn tsql_tran(words: &[String]) -> bool {
    match words.get(1).map(String::as_str) {
        Some("TRAN") => true,
        Some("TRANSACTION") => words.get(2).is_some(),
        _ => false,
    }
}

/// `BEGIN`, `START TRANSACTION`, `COMMIT`, `END`, `ROLLBACK`, `ABORT`, and the savepoint words: which, if `sql` is one.
/// (`SET TRANSACTION` is `set.rs`'s.)
pub fn control(sql: &str) -> Option<&'static str> {
    // (past leading comments: dbt sends `/* {"app": "dbt"} */ commit;`)
    let words = first_words(sql, 3);
    let rest_ok = |from: usize| words.get(from..).unwrap_or(&[]).iter().all(|w| BEGIN_WORDS.contains(&w.as_str()) || w.starts_with("ISOLATION"));
    match words.first().map(String::as_str) {
        // (SQL Server's `BEGIN TRAN [name]` and `BEGIN TRANSACTION name`: a transaction, its name labelling nothing here)
        Some("BEGIN") if rest_ok(1) || tsql_tran(&words) => Some("BEGIN"),
        Some("START") if words.get(1).map(String::as_str) == Some("TRANSACTION") => Some("BEGIN"),
        // (SQL Server's `COMMIT TRAN [name]`: a COMMIT)
        Some("COMMIT") if rest_ok(1) || tsql_tran(&words) => Some("COMMIT"),
        Some("END") if rest_ok(1) => Some("COMMIT"),
        // ROLLBACK [WORK | TRANSACTION] TO [SAVEPOINT] name: a savepoint's, not the transaction's
        Some("ROLLBACK") if to_point(&words) => Some("ROLLBACK TO"),
        // (SQL Server's `ROLLBACK TRAN` with no name is a ROLLBACK; `ROLLBACK TRAN name` is refused as it always was)
        Some("ROLLBACK") if rest_ok(1) || (words.len() == 2 && words[1] == "TRAN") => Some("ROLLBACK"),
        Some("ABORT") if rest_ok(1) => Some("ROLLBACK"),
        Some("SAVEPOINT") => Some("SAVEPOINT"),
        Some("RELEASE") => Some("RELEASE"),
        _ => None,
    }
}

#[cfg_attr(not(feature = "lake"), allow(dead_code))]
pub(crate) fn text(stmts: &[Statement]) -> String { stmts.iter().map(sql).collect::<Vec<_>>().join(";\n") }

/// A statement as SQL text again. sqlparser writes `- -3` (or `- $x`, `$x` bound to -3) as `--3`,
/// which reads back as a comment, so a minus before anything written with a minus first gets
/// parentheses.
pub fn sql(s: &Statement) -> String {
    // (sqlparser writes a macro with no parameters as `CREATE MACRO m AS …`, which it doesn't read back: its `()` kept)
    if let Statement::CreateMacro { args: None, .. } = s {
        let mut s = s.clone();
        if let Statement::CreateMacro { args, .. } = &mut s {
            *args = Some(vec![]);
        }
        return sql(&s);
    }
    let twice = |e: &Expr| matches!(e, Expr::UnaryOp { op: ast::UnaryOperator::Minus, expr } if expr.to_string().starts_with('-'));
    let mut doubled = false;
    let _ = crate::walk::visit_expressions(s, |e| {
        doubled |= twice(e);
        ControlFlow::<()>::Continue(())
    });
    if !doubled {
        return s.to_string();
    }
    let mut s = s.clone();
    let _ = crate::walk::visit_expressions_mut(&mut s, |e| {
        if let (true, Expr::UnaryOp { expr, .. }) = (twice(e), &mut *e) {
            **expr = Expr::Nested(Box::new(std::mem::replace(&mut **expr, Expr::Value(ast::Value::Null.into()))));
        }
        ControlFlow::<()>::Continue(())
    });
    s.to_string()
}

pub const PUBLIC: &str = "public";

/// A table's schema, and its name within it.
pub fn split(name: &str) -> (&str, &str) { name.split_once('.').unwrap_or((PUBLIC, name)) }

/// The name a table of `schema` has inside the lake.
pub fn join(schema: &str, table: &str) -> String {
    if schema == PUBLIC { table.to_string() } else { format!("{schema}.{table}") }
}

/// Might `sql` name table `t`? A word match on its last part (a session registers the tables that
/// might be read: one too many costs nothing).
pub fn mentions(sql: &str, t: &str) -> bool {
    let last = split(t).1.to_lowercase();
    sql.to_lowercase().split(|c: char| !(c.is_alphanumeric() || c == '_' || c == '-')).any(|w| w == last)
}

/// The statement's first word, lowercased (`create` for `CREATE TABLE …`, past comments): what a
/// statement's own regexes are looked at for, so a query compiles none of them.
pub fn head(sql: &str) -> String {
    first_word(sql).split(|c: char| !c.is_ascii_alphabetic()).next().unwrap_or_default().to_ascii_lowercase()
}

/// Does `hay` hold `needle`, whatever its case? (A byte scan: a query is not lower-cased to ask.)
pub fn contains_ci(hay: &str, needle: &str) -> bool { hay.as_bytes().windows(needle.len()).any(|w| w.eq_ignore_ascii_case(needle.as_bytes())) }

/// The word of the write a query makes (`WITH … INSERT`, `… UPDATE`, `… DELETE`, `… MERGE`), or None for one that only
/// reads. sqlparser reads each of those as a query whose body is the write, so the body is what says it.
pub fn write_of(q: &Query) -> Option<&'static str> {
    match *q.body {
        SetExpr::Insert(_) => Some("INSERT"),
        SetExpr::Update(_) => Some("UPDATE"),
        SetExpr::Delete(_) => Some("DELETE"),
        SetExpr::Merge(_) => Some("MERGE"),
        _ => None,
    }
}

/// `local` by the lake's name (the front has no `Lake`).
pub fn local_in(lake: &str, name: &str) -> Option<String> {
    match name.split('.').collect::<Vec<_>>()[..] {
        [t] => Some(t.to_string()),
        [s, t] => Some(join(s, t)),
        [l, s, t] if l == lake => Some(join(s, t)),
        _ => None,
    }
}

/// A call's arguments as written: those by position, then those by name (`day => …`, or `day := …`),
/// each name once. Every procedure and table function of Pondra's own reads its arguments through
/// this, so `=>` means one thing everywhere.
pub struct Args<'a> {
    pub given: Vec<&'a Expr>,
    pub named: Vec<(String, &'a Expr)>,
}

/// `what` names the call in errors (`run`, `read_csv`, …).
pub fn args<'a>(what: &str, args: &'a [FunctionArg]) -> Result<Args<'a>> {
    let mut out = Args { given: vec![], named: vec![] };
    for a in args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                ensure!(out.named.is_empty(), "{what}: arguments by position come before those by name");
                out.given.push(e);
            }
            FunctionArg::Named { name, arg: FunctionArgExpr::Expr(e), .. } | FunctionArg::ExprNamed { name: Expr::Identifier(name), arg: FunctionArgExpr::Expr(e), .. } => {
                let n = ident(name);
                ensure!(!out.named.iter().any(|(m, _)| *m == n), "{what}: {n} given twice");
                out.named.push((n, e));
            }
            _ => bail!("{what}: an argument is an expression, or name => expression"),
        }
    }
    Ok(out)
}

pub const ROW_ID: &str = "_row_id";
pub const VERSION: &str = "_version";
pub const CREATED: &str = "_created_at";
pub const UPDATED: &str = "_updated_at";
pub const NAMES: [&str; 4] = [ROW_ID, VERSION, CREATED, UPDATED];

/// VARIANT (JSON) values go to Python as what they hold, and come back as JSON.
pub fn is_json_type(sql_type: Option<&str>) -> bool { sql_type.is_some_and(|t| ["VARIANT", "JSON"].contains(&t.trim().to_uppercase().as_str())) }

/// A parameter that takes any value as it comes, not cast: none given, `ANY` (Postgres's
/// `anyelement`), or VARIANT / JSON (a JSON document's text, or any other value).
pub fn loose(sql_type: Option<&str>) -> bool { sql_type.is_none_or(|t| is_json_type(Some(t)) || ["ANY", "ANYELEMENT"].contains(&t.trim().to_uppercase().as_str())) }

/// `name(args…)` as SQL's tree has a call.
pub fn function(name: &str, args: Vec<ast::Expr>) -> ast::Expr {
    ast::Expr::Function(ast::Function {
        name: ast::ObjectName::from(vec![ast::Ident::new(name)]),
        uses_odbc_syntax: false,
        parameters: ast::FunctionArguments::None,
        args: ast::FunctionArguments::List(ast::FunctionArgumentList {
            duplicate_treatment: None,
            args: args.into_iter().map(|a| ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(a))).collect(),
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
    })
}

/// `s` is a uuid as Postgres writes one, the only way a uuid column holds it: 36 characters, lower
/// case, hyphens after the 8th, 12th, 16th and 20th digits.
pub fn is_uuid(s: &str) -> bool {
    s.len() == 36 && s.bytes().enumerate().all(|(i, b)| if matches!(i, 8 | 13 | 18 | 23) { b == b'-' } else { b.is_ascii_digit() || (b'a'..=b'f').contains(&b) })
}

/// The hex digits of `bytes`, two a byte, in lower case.
pub fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(char::from(DIGITS[usize::from(b >> 4)]));
        out.push(char::from(DIGITS[usize::from(b & 15)]));
    }
    out
}

/// The functions that make JSON: the Postgres port traces their arguments (`pg_trace`), so a JSON or
/// whole-row argument is written as JSON.
pub const JSON_MAKERS: &[&str] = &["to_json", "to_jsonb", "row_to_json", "json_build_object", "jsonb_build_object", "json_build_array",
    "jsonb_build_array", "json_agg", "jsonb_agg", "json_object_agg", "jsonb_object_agg"];

/// Whether `sql` calls a JSON maker (`JSON_MAKERS`), as a name followed by `(`. Only a statement with one has its arguments traced.
pub fn makes_json(sql: &str) -> bool {
    static CALL: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(&format!(r"(?i)\b(?:{})\s*\(", JSON_MAKERS.join("|"))).expect("a regex"));
    CALL.is_match(sql)
}

/// Could `sql` name a type of one's own, in a cast or one of the enum functions? (Only then is
/// it parsed for one: the built-in types' names are left alone.) A cast to uuid, json or jsonb,
/// an array type, a set function (which may read a column: `lateral::functions`), a JSON maker's
/// call, a `||` that may meet a jsonb document, and a string compared with a column that reads it as jsonb (a document, or JSON
/// written otherwise than jsonb writes it) are parsed too.
pub fn names_a_type(sql: &str) -> bool {
    if sql.contains("[]") {
        return true; // (an array type: a string cast to one reads as a Postgres array literal, `pg_text::array_literals`)
    }
    // (a set function: one in a FROM that reads a column is LATERAL's, `lateral::functions`; JSON's give a document's
    // rows, `pg_json_sets::sets`)
    static SET_FUNCTION: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i)\b(?:generate_series|range|unnest|jsonb?_(?:array_elements(?:_text)?|each(?:_text)?|object_keys|to_record(?:set)?))\s*\(").expect("a regex")
    });
    if SET_FUNCTION.is_match(sql) || makes_json(sql) {
        return true;
    }
    // (`||` merges two jsonb documents: a statement with one is read when it may name a table or JSON, since a
    // `SELECT 'n' || 5` has no document in it and is text's)
    static TABLE_OR_JSON: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\b(?:from|update|into)\b|json").expect("a regex"));
    if sql.contains("||") && TABLE_OR_JSON.is_match(sql) {
        return true;
    }
    // (a string compared with a column, `doc = '…'`, `'…' <> doc`, `doc IN ('…', …)`, `CASE doc WHEN '…'`, `doc IS DISTINCT FROM '…'`:
    // a jsonb column reads it as jsonb's text (`pg_trace::rewrite`) when it is a document, or JSON jsonb writes otherwise than
    // as written: `doc = '1e2'` finds 100, `doc = ' 100 '` too. A canonical scalar (`'100'`, `'"x"'`, `'true'`) compares as it is
    // written, and a string that isn't JSON (`'x'`, `'alice'`) is not read at all, so a statement with only those is not parsed
    // for it. A document written into a column isn't read: `VALUES (1, '{…}')` stays unparsed)
    static COMPARED: LazyLock<regex::Regex> = LazyLock::new(|| {
        regex::Regex::new(r"(?i)(?:=|<>|!=|\bdistinct\s+from|\bwhen)\s*'((?:[^']|'')*)'|'((?:[^']|'')*)'\s*(?:=|<>|!=)|\bin\s*\(((?:\s*'(?:[^']|'')*'\s*,?)*)\s*\)").expect("a regex")
    });
    static STRING: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"'((?:[^']|'')*)'").expect("a regex"));
    let read_as_jsonb = |written: &str| {
        let v = if written.contains("''") { std::borrow::Cow::Owned(written.replace("''", "'")) } else { std::borrow::Cow::Borrowed(written) };
        v.trim_start().starts_with(['{', '[']) || crate::pg_json_make::jsonb_text_if(&v).is_some_and(|text| text != v.as_ref())
    };
    let compared = COMPARED.captures_iter(sql).any(|c| match c.get(3) {
        Some(list) => STRING.captures_iter(list.as_str()).any(|s| read_as_jsonb(&s[1])),
        None => c.get(1).or(c.get(2)).is_some_and(|s| read_as_jsonb(s.as_str())),
    });
    if compared {
        return true;
    }
    static NAMED: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r#"(?i)(?:::|\bas)\s*"?([a-z_][a-z0-9_]*)"?\s*(?:[.]\s*"?([a-z_][a-z0-9_]*))?|\b(?:enum_(?:range|first|last)|pg_typeof)\s*\(|'(\{?[0-9a-fA-F]{8}-?(?:[0-9a-fA-F]{4}-?){3}[0-9a-fA-F]{12}\}?)'"#).expect("a regex"));
    const BUILT_IN: &[&str] = &["int", "integer", "bigint", "smallint", "tinyint", "int2", "int4", "int8", "hugeint", "ubigint", "uinteger", "usmallint", "utinyint", "float", "float4",
        "float8", "real", "double", "decimal", "numeric", "dec", "text", "varchar", "char", "character", "string", "bpchar", "bool", "boolean", "date", "time", "timestamp", "timestamptz",
        "datetime", "interval", "json", "jsonb", "uuid", "bytea", "blob", "binary", "varbinary", "bytes", "regclass", "regtype", "regproc", "regnamespace", "oid", "name", "variant", "bit"];
    // (a name after AS is a type only in a cast's parentheses: `SELECT x AS mood` names a column, `CAST(x AS mood)` a type)
    // (and only a text with a cast's call is tokenized to tell, once for all its AS: most have none)
    static CAST_CALL: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"(?i)\b(?:try_|safe_)?cast\s*\(").expect("a regex"));
    let calls = std::cell::OnceCell::new();
    let mut casts: Option<Casts> = None;
    let mut a_type = |c: &regex::Captures| c.get(0).is_some_and(|m| m.as_str().starts_with("::") || (*calls.get_or_init(|| CAST_CALL.is_match(sql)) && casts.get_or_insert_with(|| Casts::of(sql)).as_at(m.start())));
    NAMED.captures_iter(sql).any(|c| match (c.get(1), c.get(2)) {
        _ if c.get(3).is_some() => c.get(3).is_some_and(|s| !is_uuid(s.as_str())), // (`'A0EE…'`: read as a uuid where one is compared with it or written into one)
        (None, _) => true, // (enum_range(…), pg_typeof(…))
        (Some(_), Some(_)) => a_type(&c),
        // (a built-in type's name is looked at first: `CAST(x AS VARCHAR)` asks nothing more; uuid, json and jsonb are written as text)
        (Some(n), None) => (["uuid", "json", "jsonb"].iter().any(|t| n.as_str().eq_ignore_ascii_case(t)) || !BUILT_IN.contains(&n.as_str().to_lowercase().as_str())) && a_type(&c),
    })
}

/// Whether each `AS` of a text, asked in the order they come, is a cast's (`CAST(x AS t)`, `TRY_CAST`, `SAFE_CAST`): the
/// innermost parenthesis open there follows one of those words. Not an alias's, a CREATE's or a WITH query's. The tokens are
/// walked once for all of them: walked from the start for each, a `pondra.find` over 1,000 tables (a VALUES of nine casts a
/// row, past `parsed::LONGEST` so tokenized for each too) took minutes. When the tokenizer refuses the text, yes, so the
/// parser is the one to say.
struct Casts {
    tokens: Option<std::sync::Arc<Tokens>>,
    next: usize,     // (the first token not walked yet)
    open: Vec<bool>, // (each open parenthesis: a cast's?)
    after_cast: bool, // (the last token walked, whitespace aside, is the word cast, try_cast or safe_cast)
}

impl Casts {
    fn of(sql: &str) -> Casts {
        Casts { tokens: tokens(sql), next: 0, open: vec![], after_cast: false }
    }

    /// Whether the `AS` at byte `at` is a cast's: `at` comes after every place asked before.
    fn as_at(&mut self, at: usize) -> bool {
        let Some(t) = &self.tokens else { return true };
        while self.next < t.toks.len() && t.start[self.next] < at {
            match &t.toks[self.next] {
                Token::Whitespace(_) => {}
                Token::LParen => {
                    self.open.push(self.after_cast);
                    self.after_cast = false;
                }
                Token::RParen => {
                    self.open.pop();
                    self.after_cast = false;
                }
                Token::Word(w) => self.after_cast = w.quote_style.is_none() && ["cast", "try_cast", "safe_cast"].iter().any(|k| w.value.eq_ignore_ascii_case(k)),
                _ => self.after_cast = false,
            }
            self.next += 1;
        }
        self.open.last().copied().unwrap_or(false)
    }
}

/// An expression's place in its statement, which tells it from another of the same text.
pub(crate) fn at(e: &Expr) -> usize { std::ptr::from_ref(e) as usize }

/// Runs `change` over `stmt`, then gives each item it changed that has no alias (in a SELECT, or a RETURNING)
/// its text as written for a name, so a change made where SQL comes in never renames a column:
/// `SELECT json_agg(t) FROM t` still answers `json_agg(t)`. Items are found by their places, so `change` must
/// change expressions where they are (moving a query is fine; a query it clones is new to it). Whether
/// `change` changed anything.
pub(crate) fn names_kept(stmt: &mut Statement, change: impl FnOnce(&mut Statement) -> bool) -> bool {
    let mut written = HashMap::new();
    let _ = crate::walk::visit_mut(stmt, &mut Items(|item: &mut SelectItem| {
        if let SelectItem::UnnamedExpr(e) = item {
            written.insert(at(e), e.to_string());
        }
    }));
    if !change(stmt) {
        return false;
    }
    let _ = crate::walk::visit_mut(stmt, &mut Items(|item: &mut SelectItem| {
        if let SelectItem::UnnamedExpr(e) = item {
            if let Some(text) = written.get(&at(e)).filter(|text| **text != e.to_string()) {
                let expr = std::mem::replace(e, Expr::value(Value::Null));
                *item = SelectItem::ExprWithAlias { expr, alias: ast::Ident::with_quote('"', text.clone()) };
            }
        }
    }));
    true
}

/// Calls its function with each item of every query in a statement (both sides of a UNION) and of every
/// RETURNING.
struct Items<F>(F);

impl<F: FnMut(&mut SelectItem)> ast::VisitorMut for Items<F> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        fn items(body: &mut SetExpr, f: &mut impl FnMut(&mut SelectItem)) {
            match body {
                SetExpr::Select(s) => s.projection.iter_mut().for_each(f),
                SetExpr::SetOperation { left, right, .. } => {
                    items(left, f);
                    items(right, f);
                }
                _ => {} // (a query in parentheses is a query of its own: this visits it too)
            }
        }
        items(&mut q.body, &mut self.0);
        ControlFlow::Continue(())
    }

    fn pre_visit_statement(&mut self, s: &mut Statement) -> ControlFlow<()> {
        let returning = match s {
            Statement::Insert(i) => i.returning.as_mut(),
            Statement::Update(u) => u.returning.as_mut(),
            Statement::Delete(d) => d.returning.as_mut(),
            _ => None,
        };
        returning.into_iter().flatten().for_each(&mut self.0);
        ControlFlow::Continue(())
    }
}

/// A text's tokens as sqlparser's generic dialect reads them, whitespace and comments among them, each with the bytes it
/// covers (`start[i]..end[i]`, one after the other, so a text edited between tokens keeps the rest as written). What the
/// front's passes and every other look at a statement's words read (`tokens`), so none tokenizes on its own.
pub(crate) struct Tokens {
    pub(crate) toks: Vec<Token>,
    pub(crate) start: Vec<usize>,
    pub(crate) end: Vec<usize>,
}

/// A token that is neither whitespace nor a comment, with its bytes (`Tokens::solid`).
#[derive(Clone, Copy)]
pub(crate) struct Solid<'a> {
    pub(crate) token: &'a Token,
    pub(crate) start: usize,
    pub(crate) end: usize,
}

impl Tokens {
    /// The tokens that are neither whitespace nor a comment, in order: a word's neighbours are the next ones.
    pub(crate) fn solid(&self) -> Vec<Solid<'_>> {
        (0..self.toks.len()).filter(|&i| !matches!(self.toks[i], Token::Whitespace(_))).map(|i| Solid { token: &self.toks[i], start: self.start[i], end: self.end[i] }).collect()
    }

    /// The tokens `solid` gives, without their places.
    pub(crate) fn solid_refs(&self) -> Vec<&Token> { self.toks.iter().filter(|t| !matches!(t, Token::Whitespace(_))).collect() }

    /// The tokens `solid` gives, copied, for a look that keeps them.
    pub(crate) fn solid_tokens(&self) -> Vec<Token> { self.toks.iter().filter(|t| !matches!(t, Token::Whitespace(_))).cloned().collect() }

    /// The next token after `i` that is neither whitespace nor a comment (or the count of them all).
    pub(crate) fn next(&self, i: usize) -> usize { (i + 1..self.toks.len()).find(|&j| !matches!(self.toks[j], Token::Whitespace(_))).unwrap_or(self.toks.len()) }

    /// Is token `i` the unquoted keyword `k`.
    pub(crate) fn word(&self, i: usize, k: Keyword) -> bool { matches!(self.toks.get(i), Some(Token::Word(w)) if w.keyword == k && w.quote_style.is_none()) }
}

// (the last texts' tokens: kept as the parses are, `parsed::kept`; a per-node static, so a node's stop lets them go. An
// interim: the redesign's step 8 has the statement's own context carry its parse and tokens through the pipeline's phases,
// and this static and `parsed::LAST` go then, so step 2's sweep of node statics leaves them to it)
crate::per_node!(static TOKENS: std::sync::Mutex<std::collections::VecDeque<(String, Option<std::sync::Arc<Tokens>>)>> = std::sync::Mutex::new(std::collections::VecDeque::new()));

/// `sql`'s tokens (None: the tokenizer refuses it, an unclosed string), tokenized once while it is among the last few
/// texts asked for: the front's passes over one statement, and the looks after them, share them.
pub(crate) fn tokens(sql: &str) -> Option<std::sync::Arc<Tokens>> {
    if sql.len() > crate::parsed::LONGEST {
        return tokenized(sql).map(std::sync::Arc::new); // (a bulk INSERT's rows: tokenized each time, as they are parsed)
    }
    crate::parsed::kept(&TOKENS, sql, |sql| tokenized(sql).map(std::sync::Arc::new))
}

/// Where MySQL's executable comment `/*! … */` starts in `sql`, when the tokenizer reads one. sqlparser reads its words as
/// code (`supports_multiline_comment_hints`), Postgres and DuckDB as a comment, so a text with one would run otherwise
/// than it reads: `SELECT /*! 1 + */ 2` would be 3, and `SELECT 1 /*! ; DROP TABLE t */` two statements. None for one in
/// a string, a quoted name or another comment (a token that starts before it holds it), and when the tokenizer refuses
/// the text. (Every door refuses a text with one, `dialect_stmts.rs`; the audit log classes it as the parser reads it.)
pub(crate) fn hint(sql: &str) -> Option<usize> {
    if !sql.contains("/*!") {
        return None;
    }
    let t = tokens(sql)?;
    // (a hint's own words are given places from where it starts on, so up to the first hint the tokens are in order)
    let held = |p: usize| {
        let j = t.start.partition_point(|&s| s < p);
        j > 0 && t.end[j - 1] > p
    };
    sql.match_indices("/*!").map(|(p, _)| p).find(|&p| !held(p))
}

fn tokenized(sql: &str) -> Option<Tokens> {
    let spans = Tokenizer::new(&GenericDialect {}, sql).tokenize_with_location().ok()?;
    let at = Offsets::of(sql);
    let mut t = Tokens { toks: Vec::with_capacity(spans.len()), start: Vec::with_capacity(spans.len()), end: Vec::with_capacity(spans.len()) };
    for s in spans {
        t.start.push(at.byte(&s.span.start));
        t.end.push(at.byte(&s.span.end)); // (a span's end is just past its token)
        t.toks.push(s.token);
    }
    Some(t)
}

/// What a stretch of SQL text is, as the tokenizer reads it (`stretches`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Stretch {
    /// Words, numbers, operators and the whitespace between them.
    Code,
    /// A `-- …` or `/* … */` comment.
    Comment,
    /// A string of any kind, a quoted name or a dollar body.
    Quoted,
}

/// The stretch token `t` is part of (`quiet_token` reads it with its text).
fn stretch_of(t: &Token) -> Stretch {
    use datafusion::sql::sqlparser::tokenizer::Whitespace;
    match t {
        Token::Whitespace(Whitespace::Space | Whitespace::Newline | Whitespace::Tab) => Stretch::Code,
        Token::Whitespace(_) => Stretch::Comment,
        Token::Word(w) if w.quote_style.is_some() => Stretch::Quoted,
        Token::SingleQuotedString(_) | Token::DoubleQuotedString(_) | Token::TripleSingleQuotedString(_) | Token::TripleDoubleQuotedString(_) | Token::DollarQuotedString(_)
        | Token::SingleQuotedByteStringLiteral(_) | Token::DoubleQuotedByteStringLiteral(_) | Token::TripleSingleQuotedByteStringLiteral(_)
        | Token::TripleDoubleQuotedByteStringLiteral(_) | Token::SingleQuotedRawStringLiteral(_) | Token::DoubleQuotedRawStringLiteral(_)
        | Token::TripleSingleQuotedRawStringLiteral(_) | Token::TripleDoubleQuotedRawStringLiteral(_) | Token::NationalStringLiteral(_)
        | Token::QuoteDelimitedStringLiteral(_) | Token::NationalQuoteDelimitedStringLiteral(_) | Token::EscapedStringLiteral(_)
        | Token::UnicodeStringLiteral(_) | Token::HexStringLiteral(_) => Stretch::Quoted,
        _ => Stretch::Code,
    }
}

/// `text` in stretches, each with its bytes, one after the other, as the tokenizer reads it: `f` is given each in turn.
/// Read by hand at a byte loop's speed (a bulk INSERT's 7 MB in about 10 ms, where the tokenizer takes about 200), and
/// from where the two might read it otherwise (`differs`) on, by the tokenizer's tokens; if it refuses the text (an
/// unclosed string), by hand throughout. (One exception: MySQL's `/*! … */`, which sqlparser reads as code, is a comment
/// here both ways, as Postgres and DuckDB read it; every door refuses a text with one, `hint`.) Each look at a script's text by hand reads it here: its split at `;`s
/// (`routines::pieces`), its blocks (`script::tokens`), its `$name`s (`vars::names`). So none tells strings and comments
/// apart its own way (11-06 audit M7).
pub(crate) fn stretches(text: &str, mut f: impl FnMut(Stretch, std::ops::Range<usize>)) {
    let (b, mut from, mut at, mut raw) = (text.as_bytes(), 0, 0, false);
    while let Some(j) = b[at..].iter().position(|&c| MARKS[c as usize]) {
        let i = at + j;
        if !raw && differs(text, i) {
            match tokens(text) {
                Some(t) => return by_tokens(text, &t, from, f),
                None => raw = true,
            }
        }
        match quiet_at(text, i) {
            Some((end, s)) => {
                if i > from {
                    f(Stretch::Code, from..i);
                }
                f(s, i..end);
                (from, at) = (end, end);
            }
            None => at = i + 1,
        }
    }
    if from < text.len() {
        f(Stretch::Code, from..text.len());
    }
}

/// Token `j` of `text`'s tokens `t` as a stretch that isn't code, with its bytes: past a string's prefix, which the hand
/// reads as code (the `E` of `E'…'`, the `N` of `N'…'`: a word to each look). None for code, `0xFF` among it (a hex
/// string to the tokenizer, with no quote in it).
pub(crate) fn quiet_token(text: &str, t: &Tokens, j: usize) -> Option<(Stretch, std::ops::Range<usize>)> {
    let (s, r) = (stretch_of(&t.toks[j]), t.start[j]..t.end[j]);
    let open = match s {
        Stretch::Code => return None,
        Stretch::Comment => 0,
        Stretch::Quoted => text[r.clone()].find(['\'', '"', '`', '$'])?,
    };
    Some((s, r.start + open..r.end))
}

/// `stretches` from byte `from` on, by the tokenizer's tokens (`from` ends what the two read alike: a token starts there).
/// MySQL's `/*! … */` is a comment here too, as the hand reads it (`hints`): the tokenizer reads its words as code, so
/// without this a text was split at a `;` in one only when a mark before it had handed the text to the tokenizer
/// (`SELECT E'x' /*! ; SELECT 2 */` two pieces, `SELECT 1 /*! ; SELECT 2 */` one: 11-07 audit L8).
fn by_tokens(text: &str, t: &Tokens, from: usize, mut f: impl FnMut(Stretch, std::ops::Range<usize>)) {
    let (hints, mut h, mut code) = (hints(text, t, from), 0, from);
    let mut quiet = |s: Stretch, r: std::ops::Range<usize>, code: &mut usize| {
        if r.start > *code {
            f(Stretch::Code, *code..r.start);
        }
        *code = r.end;
        f(s, r);
    };
    for j in t.start.partition_point(|&s| s < from)..t.toks.len() {
        while h < hints.len() && hints[h].end <= t.start[j] {
            quiet(Stretch::Comment, hints[h].clone(), &mut code);
            h += 1;
        }
        if h < hints.len() && t.start[j] >= hints[h].start {
            continue; // (a hint's own words: the tokenizer places them from where it starts on)
        }
        if let Some((s, r)) = quiet_token(text, t, j) {
            quiet(s, r, &mut code);
        }
    }
    for r in &hints[h..] {
        quiet(Stretch::Comment, r.clone(), &mut code);
    }
    if code < text.len() {
        f(Stretch::Code, code..text.len());
    }
}

/// The bytes of each MySQL `/*! … */` in `text` from byte `from` on, as the tokenizer reads one (`hint`: not in a token
/// that starts before it), each to its comment's end.
fn hints(text: &str, t: &Tokens, from: usize) -> Vec<std::ops::Range<usize>> {
    let mut out: Vec<std::ops::Range<usize>> = vec![];
    if !text[from..].contains("/*!") {
        return out;
    }
    for (p, _) in text[from..].match_indices("/*!").map(|(p, m)| (from + p, m)) {
        let j = t.start.partition_point(|&s| s < p);
        if out.last().is_some_and(|r| p < r.end) || j > 0 && t.end[j - 1] > p {
            continue; // (inside a hint, or inside a string, a quoted name or another comment)
        }
        out.push(p..p + comment_end(&text[p..]));
    }
    out
}

// (the bytes a comment, a string, a quoted name or a dollar body starts with: none is a byte of a longer character)
static MARKS: [bool; 256] = {
    let (mut t, mut k, m) = ([false; 256], 0, b"-/'\"`$");
    while k < m.len() {
        t[m[k] as usize] = true;
        k += 1;
    }
    t
};

/// The hand's reading of the comment, string, quoted name or dollar body that starts at byte `i`, with the byte past it
/// (None: byte `i` is code). Each ends at its first closing mark (a doubled quote reads as two strings, one after the
/// other, as long as the tokenizer's one); comments nest; an unclosed one runs to the end.
fn quiet_at(text: &str, i: usize) -> Option<(usize, Stretch)> {
    let rest = &text[i..];
    let past = |close: &str, from: usize| i + rest[from..].find(close).map_or(rest.len(), |e| from + e + close.len());
    Some(match rest.as_bytes()[0] {
        b'-' if rest.starts_with("--") => (past("\n", 2), Stretch::Comment),
        b'/' if rest.starts_with("/*") => (i + comment_end(rest), Stretch::Comment),
        q @ (b'\'' | b'"' | b'`') => (rest[1..].find(q as char).map_or(text.len(), |e| i + e + 2), Stretch::Quoted),
        b'$' => {
            let tag = dollar_tag(rest)?;
            (past(tag, tag.len()), Stretch::Quoted)
        }
        _ => return None,
    })
}

/// The `$tag$` (or `$$`) `s` starts with, as Postgres reads one: a tag of letters, digits and `_`, not starting with a
/// digit.
fn dollar_tag(s: &str) -> Option<&str> {
    let end = s[1..].find('$')? + 2;
    let inner = &s[1..end - 1];
    (inner.chars().all(|c| c.is_alphanumeric() || c == '_') && !inner.starts_with(|c: char| c.is_ascii_digit())).then(|| &s[..end])
}

/// Might the tokenizer read byte `i` otherwise than `quiet_at` does? Read off sqlparser 0.62's tokenizer for the generic
/// dialect: a quote after a prefix that makes another kind of string of it (`E'…'` and `X'…'` take `\` escapes; `q'[…]'`,
/// `r'''…'''`, `r"""…"""`, `U&'…'`) or after a `#` (`#'x` is a name to it); a `--` or `/*` whose first mark an operator
/// or name before it takes (`#-`, `@-`, `@/`, `//`, `|/`); any of them after `@@`; a dollar body after a name (`a$b$` and `@($b$` are names) or under a tag
/// that starts with a digit (`$1$ … $1$`, not one to Postgres). On the safe side: a byte this names is read by the
/// tokenizer, which is never wrong. `tests::stretches_are_read_as_the_tokenizer_reads_them` tries each.
fn differs(text: &str, i: usize) -> bool {
    let (b, rest) = (text.as_bytes(), &text[i..]);
    let prev = i.checked_sub(1).map_or(b' ', |j| b[j]);
    if i >= 2 && &b[i - 2..i] == b"@@" {
        return true; // (`@@` and the character after it start a name to it, whatever that is: `@@'`)
    }
    match b[i] {
        b'\'' => matches!(prev, b'e' | b'E' | b'q' | b'Q' | b'r' | b'R' | b'x' | b'X' | b'&' | b'#'),
        b'"' => matches!(prev, b'r' | b'R' | b'#'),
        b'`' => prev == b'#',
        b'-' => matches!(prev, b'#' | b'@') && rest.starts_with("--"),
        b'/' => matches!(prev, b'/' | b'|' | b'#' | b'@') && rest.starts_with("/*"),
        b'$' => match dollar_tag(rest) {
            Some(_) => prev.is_ascii_alphanumeric() || matches!(prev, b'_' | b'$' | b'#' | b'@') || prev >= 0x80 || i >= 2 && matches!(b[i - 2], b'#' | b'@'), // (`@(` starts a name to it too)
            None => rest[1..].starts_with(|c: char| c.is_ascii_digit()) && rest[1..].trim_start_matches(|c: char| c.is_alphanumeric() || c == '_').starts_with('$'),
        },
        _ => false,
    }
}

/// Is the `$` at byte `i` of code part of a name, as the tokenizer reads it? `a$b` is one name, as `x1$b` is: neither
/// uses `$b`. `1$b`'s is a placeholder's.
pub(crate) fn in_name(text: &str, i: usize) -> bool {
    let before = &text[..i];
    let run = before.trim_end_matches(|c: char| c.is_alphabetic() || c.is_ascii_digit() || matches!(c, '_' | '$' | '#' | '@'));
    before[run.len()..].contains(|c: char| !c.is_ascii_digit())
}

/// Bytes for the places sqlparser gives tokens and nodes: lines from 1, columns from 1 counted in characters.
pub(crate) struct Offsets<'a> {
    pub(crate) sql: &'a str,
    lines: Vec<usize>,
    ascii: bool, // (a column is then a byte: no line is walked for it)
}

impl<'a> Offsets<'a> {
    pub(crate) fn of(sql: &'a str) -> Self { Offsets { sql, lines: std::iter::once(0).chain(sql.match_indices('\n').map(|(i, _)| i + 1)).collect(), ascii: sql.is_ascii() } }

    /// The byte at `l`: the text's end for a place just past its last character.
    pub(crate) fn byte(&self, l: &Location) -> usize {
        let start = self.lines.get(l.line.saturating_sub(1) as usize).copied().unwrap_or(self.sql.len());
        let column = l.column.saturating_sub(1) as usize;
        match self.ascii {
            true => (start + column).min(self.sql.len()),
            false => self.sql[start..].char_indices().nth(column).map_or(self.sql.len(), |(i, _)| start + i),
        }
    }

    /// A node's span as bytes, None when the parser gave it none.
    pub(crate) fn range(&self, s: Span) -> Option<(usize, usize)> {
        (s != Span::empty() && s.start.line > 0).then(|| (self.byte(&s.start), self.byte(&s.end))).filter(|(a, b)| a < b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_word_is_past_comments_as_the_tokenizer_reads_them() {
        // (11-06 audit M7: a comment in a comment ended at its first `*/`, and `c */` was the statement's first word)
        assert_eq!(first_word("/* a /* b */ c */ SELECT 1"), "SELECT 1");
        assert_eq!(first_word("  -- x\n/* y */\n  GRANT SELECT ON t TO ana"), "GRANT SELECT ON t TO ana");
        assert_eq!(first_word("/**/SELECT 1"), "SELECT 1");
        assert_eq!(first_word("/* never closed SELECT 1"), "");
        assert_eq!(first_word("SELECT 1 /* a */"), "SELECT 1 /* a */");
    }

    #[test]
    fn a_hint_is_found_where_the_tokenizer_reads_one() {
        // (MySQL's `/*! … */`, which the parser reads as code: 11-07)
        assert_eq!(hint("SELECT /*! 1 + */ 2"), Some(7));
        assert_eq!(hint("SELECT 1 AS x /*! ; DROP TABLE t */"), Some(14));
        assert_eq!(hint("SELECT 1 /*!50110 KEY_BLOCK_SIZE = 1024 */"), Some(9));
        assert_eq!(hint("/*!\nDROP TABLE t */ SELECT 1"), Some(0));
        let late = "SELECT '/*!', /* /*! */ */ a, /*! 1 + */ 2";
        assert_eq!(hint(late), late.rfind("/*!"));
        for quiet in ["SELECT '/*! 1 + */ 2'", "SELECT \"/*!\" FROM t", "SELECT 1 -- /*! 2\n", "/* a /*! b */ c */ SELECT 1", "SELECT $$/*! 1$$", "SELECT E'\\'/*! 1 */'", "SELECT 1 /* ! */", "SELECT '/*! 1", "SELECT 1"] {
            assert_eq!(hint(quiet), None, "{quiet}");
        }
    }

    #[test]
    fn a_name_in_text_splits_as_sql_reads_it() {
        let cases: [(&str, &[&str]); 9] = [
            ("orders", &["orders"]),
            ("Sales.Orders", &["sales", "orders"]),
            ("\"Sales\".\"Orders\"", &["Sales", "Orders"]),
            ("\"My.s\".t", &["My.s", "t"]),
            ("\"a\"\"b\"", &["a\"b"]),
            (" lake.public.T ", &["lake", "public", "t"]),
            ("my-db.Sales", &["my-db", "sales"]),
            ("\"$user\"", &["$user"]),
            ("", &[]),
        ];
        for (name, want) in cases {
            assert_eq!(parts(name), want, "{name}");
        }
        assert_eq!(object_of("\"S\".Seq"), "S.seq");
        assert_eq!(database("\"Dev\""), "dev");
    }

    #[test]
    fn a_texts_tokens_cover_it_one_after_the_other_as_the_tokenizer_reads_them() {
        let texts = ["SELECT a, b FROM t WHERE c = 'x' -- a comment\n  AND d > 1", "SELECT 'é',\n\t\"Ünïcode\" /* ü\nö */ FROM t;  ", "x", "", "SELECT $1, e'\\n' || 'a''b'"];
        for sql in texts {
            let t = tokens(sql).expect("tokenized");
            let want = Tokenizer::new(&GenericDialect {}, sql).tokenize().expect("tokenized");
            assert_eq!(t.toks, want, "{sql}");
            assert_eq!(t.start.first().copied().unwrap_or(0), 0, "{sql}");
            for i in 0..t.toks.len() {
                let next = t.start.get(i + 1).copied().unwrap_or(sql.len());
                assert_eq!(t.end[i], next, "{sql}: token {i} ({:?}) ends where the next starts", t.toks[i]);
                if let Token::Word(w) = &t.toks[i] {
                    assert!(sql[t.start[i]..t.end[i]].contains(w.value.as_str()), "{sql}: {:?} at {}..{}", w, t.start[i], t.end[i]);
                }
            }
            // (asked again at once, the same tokens; tried a few times, as other tests running beside this one share the
            // cache and may push a text out between two asks)
            assert!((0..10).any(|_| std::sync::Arc::ptr_eq(&tokens(sql).expect("tokenized"), &tokens(sql).expect("kept"))), "{sql}: asked again, the same tokens");
        }
        assert!(tokens("SELECT 'unclosed").is_none());
        let solid: Vec<String> = tokens("SELECT /* c */ a ,b").expect("tokenized").solid().iter().map(|s| s.token.to_string()).collect();
        assert_eq!(solid, ["SELECT", "a", ",", "b"]);
        let long = format!("SELECT '{}'", "x".repeat(crate::parsed::LONGEST));
        assert_eq!(tokens(&long).expect("tokenized").toks.len(), 3, "a text past LONGEST is tokenized, not kept");
    }

    #[test]
    fn stretches_are_read_as_the_tokenizer_reads_them() {
        // (11-06 audit M7) Each byte's stretch by `stretches` and by the tokenizer's tokens: the same, wherever the
        // tokenizer reads the text.
        let by_tokens = |sql: &str| -> Option<Vec<Stretch>> {
            let t = tokens(sql)?;
            let mut out = vec![Stretch::Code; sql.len()];
            for j in 0..t.toks.len() {
                if let Some((s, r)) = quiet_token(sql, &t, j) {
                    out[r].fill(s); // (a string's prefix, the N of N'…', is code, and so is 0xFF)
                }
            }
            Some(out)
        };
        let read = |sql: &str| -> Vec<Stretch> {
            let (mut out, mut at) = (vec![], 0);
            stretches(sql, |s, r| {
                assert_eq!(r.start, at, "{sql:?}: one after the other");
                at = r.end;
                out.extend(std::iter::repeat_n(s, r.len()));
            });
            assert_eq!(at, sql.len(), "{sql:?}: to its end");
            out
        };
        // (each one the hand alone reads otherwise than the tokenizer, `differs`; the last two it reads alike)
        for sql in [
            "SELECT E'it\\'s' AS s",
            "SELECT X'\\'' AS b",
            "SELECT q'[it's]' AS s",
            "SELECT r'''a'b''' AS s",
            "SELECT R\"\"\"a\"b\"\"\" AS s",
            "SELECT U&'d\\0061' AS s",
            "SELECT #'a' FROM t'",
            "SELECT 4 //* c */",
            "SELECT 4 |/* c */",
            "SELECT @-- c\n1",
            "SELECT #-- c\n1",
            "SELECT a$b$ FROM t -- $b$",
            "SELECT $1$ a $1$",
            "SELECT @($t$ AS x",
            "SELECT @@'a' AS b', 0xFF",
            "SELECT /* a /* b */ c */ 1",
            "SELECT 'it''s', \"a\"\"b\", `c``d`, $$e$$, $f$g$f$, N'h', B'i', b\"j\" -- k\n",
        ] {
            assert_eq!(Some(read(sql)), by_tokens(sql), "{sql}");
        }
        // (11-07 audit L8: MySQL's `/*! … */` is a comment both ways, where the hand reads it and after a mark that hands
        // the text to the tokenizer, which reads its words as code)
        for sql in ["SELECT 1 /*! ; SELECT 2 */ x", "SELECT E'x' /*! ; SELECT 2 */ x", "SELECT E'x' /*! 'a;b' */ ; 3 /*!*/", "SELECT E'x', '/*!' /* /*! */ */ /*!50110 a */"] {
            let got = read(sql);
            let hand: Vec<Stretch> = {
                let (mut out, mut at) = (vec![Stretch::Code; sql.len()], 0);
                while let Some(j) = sql[at..].find(|c: char| c.is_ascii() && MARKS[c as usize]) {
                    match quiet_at(sql, at + j) {
                        Some((end, st)) => {
                            out[at + j..end].fill(st);
                            at = end;
                        }
                        None => at += j + 1,
                    }
                }
                out
            };
            assert_eq!(got, hand, "{sql}: as the hand reads it");
            for (p, _) in sql.match_indices("/*!").filter(|(p, _)| !sql[..*p].ends_with("'") && !sql[..*p].ends_with("/* ")) {
                assert_eq!(got[p], Stretch::Comment, "{sql}: the `/*!` at {p} is a comment");
            }
        }
        let parts = ["SELECT 1", ";", " ", "'a'", "'it''s'", "E'x\\''", "X'\\''", "\"c\"", "`e`", "-- g\n", "/* h */", "/* i /* j */ */", "$$k$$", "$t$m$t$", "v$x", "q'[o'p]'", "r'''q'r'''", "U&'d\\0061'", "DATE'2026'", "#", "@", "/", "|", "-", "*", "\n", "x", "(", "$1", "$1$", "$a$", "é", "e", "'"];
        let mut seed = 20261107u64;
        let mut next = || {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            (seed >> 33) as usize
        };
        let mut compared = 0;
        for _ in 0..30000 {
            let sql: String = (0..1 + next() % 12).map(|_| parts[next() % parts.len()]).collect();
            if let Some(want) = by_tokens(&sql) {
                assert_eq!(read(&sql), want, "{sql:?}");
                compared += 1;
            }
        }
        assert!(compared > 10000, "{compared}");
        // (one the tokenizer refuses: the hand's reading)
        let mut unclosed = vec![];
        stretches("SELECT 'a", |s, r| unclosed.push((s, r)));
        assert_eq!(unclosed, [(Stretch::Code, 0..7), (Stretch::Quoted, 7..9)]);
        assert!(in_name("SELECT a$b", 8) && in_name("SELECT x1$b", 9) && !in_name("SELECT 1$b", 8) && !in_name("SELECT $b", 7));
    }
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};

    /// `sql` as one statement, read by the generic dialect (the SQL these tests use is plain).
    fn one(sql: &str) -> Option<Statement> {
        let mut parsed = Parser::parse_sql(&GenericDialect {}, sql).ok()?;
        (parsed.len() == 1).then(|| parsed.pop()).flatten()
    }

    #[test]
    fn hex_writes_two_lower_case_digits_a_byte() {
        assert_eq!(hex(&[]), "");
        assert_eq!(hex(&[0x0f]), "0f");
        assert_eq!(hex(&[0xab, 0x00]), "ab00");
    }

    #[test]
    fn a_concatenation_is_parsed_only_when_it_may_meet_a_document() {
        for sql in ["SELECT 'n' || 5", "SELECT 'a' || 'b' || 'c'", "SELECT name || 'x'"] {
            assert!(!names_a_type(sql), "{sql}");
        }
        for sql in ["SELECT 'n' || name FROM t", "UPDATE t SET doc = doc || '{}'", "INSERT INTO t SELECT 'a' || 'b'", "SELECT '{}' || '{}'::jsonb", "SELECT to_jsonb(1) || '{}'"] {
            assert!(names_a_type(sql), "{sql}");
        }
        assert!(names_a_type("SELECT json_agg(x) FROM t") && names_a_type("SELECT * FROM generate_series(1, t.n)") && names_a_type("SELECT * FROM jsonb_each(t.doc)") && names_a_type("SELECT '{a}'::text[]"));
    }

    #[test]
    fn a_name_after_as_is_a_type_only_in_a_cast() {
        // (a column's or a table's alias, a CREATE's AS and a WITH query's name send no statement through the parser)
        for sql in ["SELECT 77 AS k", "SELECT g AS mood, count(*) AS n FROM t AS a GROUP BY g", "SELECT x AS uuid FROM t", "CREATE TABLE c AS SELECT 1",
            "WITH q AS MATERIALIZED (SELECT 1) SELECT * FROM q", "SELECT CAST(x AS INT) AS mood FROM t", "SELECT f(x) AS mood FROM t", "SELECT 'cast(x as mood)' AS a"] {
            assert!(!names_a_type(sql), "{sql}");
        }
        for sql in ["SELECT CAST(x AS mood) FROM t", "SELECT try_cast(x AS mood)", "SELECT SAFE_CAST(x AS s.mood)", "SELECT CAST((x) AS mood)", "SELECT CAST(f(x, y) AS mood) AS m",
            "SELECT upper(CAST(CAST(x AS mood) AS TEXT))", "SELECT x::mood AS m", "SELECT CAST(x AS uuid) AS id", "SELECT CAST(x AS jsonb)"] {
            assert!(names_a_type(sql), "{sql}");
        }
        assert!(names_a_type("SELECT CAST(x AS mood), 'open"), "the tokenizer refuses an unclosed string: the parser is the one to say");
        assert!(names_a_type("SELECT CAST(a AS INT) AS m1, f(CAST(b AS s.mood)) AS m2"), "the second cast, after an alias");
        assert!(!names_a_type("SELECT CAST(CAST(a AS INT) AS TEXT) AS mood, (b) AS m, CAST((c) AS VARCHAR) AS t"));
        // (a long text with an AS in each of thousands of items, as `pondra.find`'s rows are: its tokens walked once, not once an AS)
        let items: Vec<String> = (0..6000).map(|i| format!("CAST(f(a, '{i}') AS VARCHAR) AS c{i}")).collect();
        let long = format!("SELECT {} FROM t", items.join(", "));
        assert!(long.len() > crate::parsed::LONGEST);
        let started = std::time::Instant::now();
        assert!(!names_a_type(&long), "built-in types and aliases only");
        assert!(names_a_type(&format!("{long} WHERE CAST(x AS mood) = 'happy'")), "and a type of one's own at its end");
        assert!(started.elapsed() < std::time::Duration::from_secs(5), "{:?}", started.elapsed());
    }

    #[test]
    fn a_document_compared_with_a_column_is_parsed_and_one_written_into_a_column_is_not() {
        for sql in [r#"SELECT 1 FROM t WHERE doc = '{ "a" : 1 }'"#, r#"DELETE FROM t WHERE doc <> '[1]'"#, r#"SELECT 1 FROM t WHERE doc IN ('{"a":1}', 'x')"#,
            r#"SELECT 1 FROM t WHERE '{"a": 1}' = doc"#, r#"SELECT 1 FROM t WHERE doc IS DISTINCT FROM '{}'"#] {
            assert!(names_a_type(sql), "{sql}");
        }
        for sql in [r#"INSERT INTO t VALUES (1, '{"a": 1}')"#, r#"INSERT INTO t (id, doc) VALUES (1, '[1, 2]'), (2, '{}')"#, "SELECT 1 FROM t WHERE name = 'x'"] {
            assert!(!names_a_type(sql), "{sql}");
        }
    }

    #[test]
    fn a_string_compared_with_a_column_is_parsed_when_jsonb_reads_it_otherwise_than_as_written() {
        for sql in ["SELECT 1 FROM t WHERE doc = '1e2'", "SELECT 1 FROM t WHERE '1e2' = doc", "SELECT 1 FROM t WHERE doc IN ('x', ' 100 ')",
            "SELECT CASE doc WHEN '1e2' THEN 1 END FROM t", "SELECT 1 FROM t WHERE doc IS DISTINCT FROM ' 1'"] {
            assert!(names_a_type(sql), "{sql}");
        }
        // (a canonical scalar compares as written, and a string that isn't JSON is not read)
        for sql in ["SELECT 1 FROM t WHERE doc = 'x'", "SELECT 1 FROM t WHERE doc = '100'", r#"SELECT 1 FROM t WHERE doc = '"x"'"#,
            "SELECT 1 FROM t WHERE name = 'it''s'", "INSERT INTO t VALUES (1, '1e2')"] {
            assert!(!names_a_type(sql), "{sql}");
        }
    }

    #[test]
    fn names_kept_by_place() {
        // (every `upper(…)` becomes 'x': the items it changed keep their names, the others are as they were)
        let upper = |s: &mut Statement| {
            let mut changed = false;
            let _ = crate::walk::visit_expressions_mut(s, |e| {
                if matches!(e, Expr::Function(f) if object(&f.name) == "upper") {
                    *e = Expr::value(Value::SingleQuotedString("x".into()));
                    changed = true;
                }
                ControlFlow::<()>::Continue(())
            });
            changed
        };
        let kept = |sql: &str| {
            let mut s = one(sql).unwrap();
            let changed = names_kept(&mut s, upper);
            (changed, s.to_string())
        };
        assert_eq!(
            kept("SELECT upper(a), upper(a) AS b, lower(a), (SELECT upper(a) FROM t) FROM t UNION ALL SELECT upper(a), 'y', 'z', 'w' FROM t"),
            (true, r#"SELECT 'x' AS "upper(a)", 'x' AS b, lower(a), (SELECT 'x' AS "upper(a)" FROM t) AS "(SELECT upper(a) FROM t)" FROM t UNION ALL SELECT 'x' AS "upper(a)", 'y', 'z', 'w' FROM t"#.to_string())
        );
        assert_eq!(kept("WITH c AS (SELECT upper(a) FROM t) SELECT * FROM c"), (true, r#"WITH c AS (SELECT 'x' AS "upper(a)" FROM t) SELECT * FROM c"#.to_string()));
        assert_eq!(kept("UPDATE t SET a = upper(b) RETURNING upper(a), a"), (true, r#"UPDATE t SET a = 'x' RETURNING 'x' AS "upper(a)", a"#.to_string()));
        assert_eq!(kept("INSERT INTO t SELECT upper(a) FROM s RETURNING upper(a)"), (true, r#"INSERT INTO t SELECT 'x' AS "upper(a)" FROM s RETURNING 'x' AS "upper(a)""#.to_string()));
        assert_eq!(kept("SELECT lower(a) FROM t"), (false, "SELECT lower(a) FROM t".to_string()));
        // (a name holding a double quote is written so it reads back as itself)
        let (_, text) = kept(r#"SELECT upper("A") FROM t"#);
        match &one(&text).unwrap() {
            Statement::Query(q) => match &*q.body {
                SetExpr::Select(s) => assert!(matches!(&s.projection[0], SelectItem::ExprWithAlias { alias, .. } if alias.value == r#"upper("A")"#), "{text}"),
                _ => panic!("{text}"),
            },
            _ => panic!("{text}"),
        }
    }
}
