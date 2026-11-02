//! Other databases' statements for what Pondra writes another way, refused by name (0A000) with what to write, at
//! every door in the same words (`statement::run`, and the Postgres port's `pg.rs`), before anything else reads them.
//! Each would otherwise fail with the parser's words, or, for MySQL's `UPDATE … JOIN … SET`, with a column it can't see.
//!
//! A statement is matched by its words as tokens, so a string or a comment holding them is not:
//! - Snowflake's and Spark's `DESCRIBE TABLE t`; Spark's `EXPLAIN EXTENDED`, Snowflake's `EXPLAIN USING TEXT`, MySQL's
//!   `EXPLAIN FORMAT=JSON`; MySQL's, Snowflake's and Spark's `SHOW TABLES LIKE … / IN … / FROM …`;
//! - SQL Server's `CREATE [NON]CLUSTERED INDEX`, `SET NOCOUNT ON` and its kind, `DECLARE @x`,
//!   `CROSS APPLY` and `OUTER APPLY`, and `[name]` quoting (also of a `PIVOT`'s values, `IN ([paid], …)`);
//! - MySQL's `SET @x = …`, `PREPARE p FROM '…'`, `DELETE t FROM t JOIN …`, `UPDATE t JOIN … SET …`, `REPLACE INTO`,
//!   `ALTER TABLE t CHANGE / MODIFY …` (Pondra's `ALTER COLUMN c TYPE …`, `ALTER COLUMN c AFTER b`), and `DATABASE()`,
//!   `USER()` and their kind; BigQuery's `DECLARE x TYPE DEFAULT v`;
//! - SQL Server's `OUTPUT inserted.…`, which would otherwise run with the clause left out, and `OPTION (…)`;
//! - Snowflake's `CREATE STREAM … ON TABLE` and `FLATTEN(INPUT => …)`;
//! - DuckDB's and SQLite's `PRAGMA …`, CockroachDB's `UPSERT INTO`, and Postgres's `CREATE EXTENSION` (Pondra's
//!   INSTALL and LOAD).
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use regex::Regex;
use std::sync::LazyLock;

/// SQL Server's `SET name ON|OFF` options, which nothing here reads.
const SQL_SERVER_SETS: &[&str] = &["NOCOUNT", "ANSI_NULLS", "QUOTED_IDENTIFIER", "XACT_ABORT", "ANSI_PADDING", "ANSI_WARNINGS", "ARITHABORT", "CONCAT_NULL_YIELDS_NULL", "NUMERIC_ROUNDABORT", "IDENTITY_INSERT", "STATISTICS", "SHOWPLAN_ALL", "SHOWPLAN_XML", "IMPLICIT_TRANSACTIONS", "FMTONLY"];

/// The words for MySQL's `SET @v = …` (`settings.rs` says them too, for a SET it reads).
pub(crate) fn user_variable(v: &str) -> String { format!("SET @{v} is MySQL's for a variable: here a variable is ${v} = …, and ${v} reads it") }

/// Refused (0A000) with what to write, when a statement of `sql` is one of these.
pub fn refused(sql: &str) -> anyhow::Result<()> {
    match refusal(sql) {
        Some(why) => Err(crate::codes::coded("0A000", why)),
        None => Ok(()),
    }
}

/// The words of the refusal, or None. A text with none of these words is not tokenized.
fn refusal(sql: &str) -> Option<String> {
    static LOOK: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(concat!(
            r"(?is)\b(?:desc|describe)\s+table\b|\bexplain\s+(?:extended|formatted|codegen|cost|using|format\s*=)|\bshow\s+(?:full\s+)?tables\s+(?:like|in|from)\b",
            r"|\b(?:non)?clustered\b|\bset\s+(?:@|\w+\s+(?:on|off)\b|identity_insert\b|statistics\b)|\bprepare\s+\w+\s+from\b|\bdeclare\s+@|\bdeclare\b[^;]*\bdefault\b",
            r"|\bdelete\s+\w+\s+from\b|\bupdate\b[^;]*\bjoin\b[^;]*\bset\b|\balter\s+table\b[^;]*\b(?:change|modify)\b|\b(?:cross|outer)\s+apply\b",
            r"|\]\s*\.|\.\s*\[|\[dbo\]|\b(?:from|join|into|update|table)\s+\[",
            r"|\boutput\s+(?:inserted|deleted)\s*\.|\bflatten\s*\(\s*input\b|\b(?:database|schema|user|current_user|session_user|system_user)\s*\(\s*\)",
            r"|\breplace\s+into\b|\boption\s*\(|\bcreate\s+(?:or\s+replace\s+)?stream\b|\bpragma\b|\bupsert\b|\bcreate\s+extension\b|\b(?:un)?pivot\b[^;]*\bin\s*\(\s*\["))
            .expect("a regex")
    });
    if !LOOK.is_match(sql) {
        return None;
    }
    let tokens = Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?;
    let tokens: Vec<Token> = tokens.into_iter().filter(|t| !matches!(t, Token::Whitespace(_))).collect();
    tokens.split(|t| *t == Token::SemiColon).find_map(statement)
}

/// Where a statement starts: DuckDB's and SQLite's `PRAGMA …` (`PRAGMA table_info('t')` is `DESCRIBE t`; a setting is
/// SET and SHOW), CockroachDB's `UPSERT INTO t …` (Pondra's two upserts: a plain INSERT of a key that is there is
/// refused, so neither is read in its place) and Postgres's `CREATE EXTENSION [IF NOT EXISTS] name …` (Pondra's INSTALL
/// and LOAD, `extension.rs`, or the functions built in for the extensions most scripts ask for).
fn start(s: &[Token], first: &str) -> Option<String> {
    let w = |i: usize| word(s.get(i));
    match first {
        "PRAGMA" => {
            let name = match s.get(1) {
                Some(Token::Word(n)) => n.value.to_lowercase(),
                _ => String::new(),
            };
            // (the table between the parentheses, as written: `'orders'` or `sales.orders`)
            let table = match (s.get(2), s.iter().position(|t| *t == Token::RParen)) {
                (Some(Token::LParen), Some(close)) if close > 3 || (close == 3 && s.len() > 3) => match &s[3..close] {
                    [Token::SingleQuotedString(q)] => q.clone(),
                    inner => inner.iter().map(|t| t.to_string()).collect(),
                },
                _ => "t".to_string(),
            };
            let ours = match name.as_str() {
                "table_info" | "show" => format!("DESCRIBE {table}"),
                "show_tables" | "show_tables_expanded" => "SHOW TABLES".to_string(),
                "database_list" | "show_databases" => "SHOW DATABASES".to_string(),
                "version" => "SELECT version()".to_string(),
                _ => format!("SET {0} = … to change a setting, SHOW {0} to read it", if name.is_empty() { "name" } else { &name }),
            };
            Some(format!("PRAGMA {name} is DuckDB's and SQLite's: write {ours}"))
        }
        "UPSERT" => {
            let name = match (w(1).as_deref(), s.get(2)) {
                (Some("INTO"), Some(Token::Word(t))) => t.to_string(),
                _ => "t".to_string(),
            };
            Some(format!("UPSERT is CockroachDB's: write INSERT OR REPLACE INTO {name} …, or INSERT INTO {name} … ON CONFLICT (key) DO UPDATE SET …"))
        }
        "CREATE" if w(1).as_deref() == Some("EXTENSION") => {
            let at = if w(2).as_deref() == Some("IF") { 5 } else { 2 }; // (IF NOT EXISTS)
            let name = match s.get(at) {
                Some(Token::Word(n)) => n.value.clone(),
                _ => "name".to_string(),
            };
            let ours = match name.to_lowercase().as_str() {
                "uuid-ossp" => "gen_random_uuid() is built in".to_string(),
                "pgcrypto" => "gen_random_uuid(), digest(text, 'sha256'), sha256() and md5() are built in".to_string(),
                "plpgsql" => "a procedure's body is Pondra's own SQL script (CREATE PROCEDURE … AS BEGIN … END), with nothing to install".to_string(),
                _ => format!("a Pondra extension is installed from its file, INSTALL {name} FROM 'path or URL', then LOAD {name}"),
            };
            Some(format!("CREATE EXTENSION {name} is Postgres's: {ours}"))
        }
        _ => None,
    }
}

/// The word `t` is, upper-cased, when it is one and not quoted.
fn word(t: Option<&Token>) -> Option<String> {
    match t {
        Some(Token::Word(w)) if w.quote_style.is_none() => Some(w.value.to_uppercase()),
        _ => None,
    }
}

/// A name as written from `i` (`a.b`), and the place after it.
fn name(s: &[Token], mut i: usize) -> (String, usize) {
    let mut parts = Vec::new();
    while let Some(Token::Word(w)) = s.get(i) {
        parts.push(w.to_string());
        i += 1;
        if s.get(i) != Some(&Token::Period) {
            break;
        }
        i += 1;
    }
    (if parts.is_empty() { "t".into() } else { parts.join(".") }, i)
}

fn statement(s: &[Token]) -> Option<String> {
    let w = |i: usize| word(s.get(i));
    let is = |i: usize, x: &str| w(i).as_deref() == Some(x);
    let first = w(0).unwrap_or_default();
    if let Some(why) = start(s, &first) {
        return Some(why);
    }
    let mut depth = 0i32; // (how deep in parentheses token `i` is)
    // (the depth just inside a `PIVOT (` or `UNPIVOT (`, with which, and just inside its `IN (`)
    let (mut pivot, mut listed): (Option<(i32, bool)>, Option<i32>) = (None, None);
    // (anywhere in it: SQL Server's APPLY, OUTPUT, OPTION and quoting of names; Snowflake's FLATTEN; MySQL's DATABASE()
    // and USER(). `t FOR SYSTEM_TIME AS OF …` is Pondra's: `past_names.rs`)
    for (i, t) in s.iter().enumerate() {
        match t {
            Token::LParen => depth += 1,
            Token::RParen => {
                depth -= 1;
                pivot = pivot.filter(|(d, _)| depth >= *d);
                listed = listed.filter(|d| depth >= *d);
            }
            _ => {}
        }
        if (is(i, "PIVOT") || is(i, "UNPIVOT")) && s.get(i + 1) == Some(&Token::LParen) {
            pivot = Some((depth + 1, is(i, "PIVOT")));
        }
        if pivot.is_some_and(|(d, _)| d == depth) && is(i, "IN") && s.get(i + 1) == Some(&Token::LParen) {
            listed = Some(depth + 1);
        }
        // (SQL Server's `PIVOT (… FOR status IN ([paid], [shipped]))` names the values, and `UNPIVOT (… IN ([a], [b]))` the
        // columns, in its quoting: here `[paid]` would be a list of a column)
        if let (Some(d), Some((_, values)), Token::LBracket, Some(Token::RBracket)) = (listed, pivot, t, s.get(i + 2)) {
            let bare = match s.get(i + 1) {
                Some(Token::Word(n)) if n.quote_style.is_none() => Some((n.value.clone(), format!("'{}'", n.value))),
                Some(Token::Number(n, _)) => Some((n.clone(), n.clone())),
                _ => None,
            };
            let starts = s.get(i.wrapping_sub(1)) == Some(&Token::LParen); // (the list's first: not `m[k]`, nor `([1, 2], [3])`)
            if let (true, true, Some((bare, value))) = (d == depth, starts, bare) {
                return Some(if values {
                    format!("PIVOT (… IN ([{bare}], …)) is SQL Server's: write the values themselves, IN ({value}, …)")
                } else {
                    format!("[{bare}] is SQL Server's quoting of a name: write \"{bare}\" (or the name alone)")
                });
            }
        }
        if (is(i, "CROSS") || is(i, "OUTER")) && is(i + 1, "APPLY") {
            return Some(format!("{} APPLY is SQL Server's: write CROSS JOIN LATERAL (SELECT …) AS x for CROSS APPLY, and LEFT JOIN LATERAL (SELECT …) AS x ON true for OUTER APPLY", w(i).unwrap_or_default()));
        }
        if is(i, "OUTPUT") && matches!(w(i + 1).as_deref(), Some("INSERTED" | "DELETED")) && s.get(i + 2) == Some(&Token::Period) {
            return Some(format!("OUTPUT {}.… is SQL Server's: end the statement with RETURNING and its columns (RETURNING * for all of them)", w(i + 1).unwrap_or_default().to_lowercase()));
        }
        if is(i, "OPTION") && depth == 0 && s.get(i + 1) == Some(&Token::LParen) && ["SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE"].contains(&first.as_str()) {
            let hint = w(i + 2).unwrap_or_default();
            let more = if hint == "MAXRECURSION" { " (WITH RECURSIVE … stops when a pass adds no rows)" } else { "" };
            return Some(format!("OPTION ({hint} …) is SQL Server's hint to its planner: leave it out{more}"));
        }
        if is(i, "FLATTEN") && s.get(i + 1) == Some(&Token::LParen) && is(i + 2, "INPUT") && s.get(i + 3) == Some(&Token::RArrow) {
            return Some("FLATTEN(INPUT => …) is Snowflake's: unnest the array in the FROM (… CROSS JOIN unnest(string_to_array(tags, ',')) AS f(tag)), or unnest(json_extract(doc, '$.items')) for a JSON array".into());
        }
        // (a call, not a name being made: `CREATE FUNCTION user()`, `x.user()`)
        let called = s.get(i + 1) == Some(&Token::LParen) && s.get(i + 2) == Some(&Token::RParen) && (i == 0 || s[i - 1] != Token::Period && !matches!(word(s.get(i - 1)).as_deref(), Some("FUNCTION" | "MACRO" | "PROCEDURE")));
        if called {
            let ours = match w(i).as_deref() {
                Some("DATABASE") => Some("current_database()"),
                Some("SCHEMA") => Some("current_schema()"),
                Some("USER" | "CURRENT_USER" | "SESSION_USER" | "SYSTEM_USER") => Some("current_user (no parentheses)"),
                _ => None,
            };
            if let Some(ours) = ours {
                return Some(format!("{}() is MySQL's: write {ours}", w(i).unwrap_or_default()));
            }
        }
        // (`items[i].sku` is a subscript: after a name, `)` or `]`, a `[` is one; after a word that starts what follows
        // it, a name SQL Server quotes)
        const BEFORE_A_NAME: &[&str] = &["SELECT", "FROM", "JOIN", "WHERE", "ON", "AND", "OR", "NOT", "BY", "AS", "WHEN", "THEN", "ELSE", "CASE", "IN", "INTO",
            "UPDATE", "TABLE", "SET", "HAVING", "DISTINCT", "ALL", "RETURNING", "VALUES", "LIKE", "ILIKE", "IS", "BETWEEN", "USING", "WITH", "RETURN", "EXISTS",
            "UNION", "EXCEPT", "INTERSECT", "LIMIT", "OFFSET", "TOP", "DELETE", "INSERT", "MERGE", "VIEW", "INDEX", "APPLY", "OUTPUT", "EXEC", "EXECUTE"];
        let subscript = i > 0 && match &s[i - 1] {
            Token::RParen | Token::RBracket => true,
            Token::Word(w) => w.quote_style.is_some() || !BEFORE_A_NAME.contains(&w.value.to_uppercase().as_str()),
            _ => false,
        };
        if let (Token::LBracket, Some(Token::Word(n)), Some(Token::RBracket), false) = (t, s.get(i + 1), s.get(i + 2), subscript) {
            let dotted = s.get(i + 3) == Some(&Token::Period) || (i > 0 && s[i - 1] == Token::Period);
            let named = i > 0 && matches!(word(s.get(i - 1)).as_deref(), Some("FROM" | "JOIN" | "INTO" | "UPDATE" | "TABLE"));
            if n.quote_style.is_none() && (dotted || named || n.value.eq_ignore_ascii_case("dbo")) {
                return Some(format!("[{}] is SQL Server's quoting of a name: write \"{}\" (or the name alone)", n.value, n.value));
            }
        }
    }
    match first.as_str() {
        "DESC" | "DESCRIBE" if is(1, "TABLE") => Some(format!("DESCRIBE TABLE is Snowflake's and Spark's: write DESCRIBE {}", name(s, 2).0)),
        "EXPLAIN" => {
            let how = w(1)?;
            match how.as_str() {
                "EXTENDED" | "FORMATTED" | "CODEGEN" | "COST" => Some(format!("EXPLAIN {how} is Spark's: write EXPLAIN VERBOSE query, or EXPLAIN ANALYZE query to run it and see its numbers")),
                "USING" => Some(format!("EXPLAIN USING {} is Snowflake's: write EXPLAIN query, or EXPLAIN FORMAT pgjson query for JSON", w(2).unwrap_or_default())),
                "FORMAT" if s.get(2) == Some(&Token::Eq) => Some(format!("EXPLAIN FORMAT={} is MySQL's: write EXPLAIN FORMAT pgjson query for JSON, or EXPLAIN FORMAT tree query", w(3).unwrap_or_default())),
                _ => None,
            }
        }
        "SHOW" => {
            let at = if is(1, "FULL") { 2 } else { 1 };
            let by = w(at + 1).filter(|_| is(at, "TABLES")).filter(|b| ["LIKE", "IN", "FROM"].contains(&b.as_str()))?;
            Some(format!("SHOW TABLES {by} … is MySQL's, Snowflake's and Spark's: SHOW TABLES lists them all, and SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'ord%' picks"))
        }
        "CREATE" => {
            let at = if is(1, "OR") && is(2, "REPLACE") { 3 } else { 1 };
            if is(at, "STREAM") && matches!(w(name(s, at + 1).1).as_deref(), Some("ON")) {
                let (t, _) = name(s, name(s, at + 1).1 + 2);
                return Some(format!("CREATE STREAM … ON TABLE is Snowflake's: here GET /watch/{t}?changes=true follows a table's changes as they commit (db.watch('{t}', changes=True) in Python)"));
            }
            let at = if is(1, "UNIQUE") { 2 } else { 1 };
            let kind = w(at).filter(|k| k == "CLUSTERED" || k == "NONCLUSTERED")?;
            Some(format!("{kind} INDEX is SQL Server's: write CREATE INDEX name ON t (columns); CLUSTER BY orders a table's rows"))
        }
        "REPLACE" if is(1, "INTO") => {
            let t = name(s, 2).0;
            Some(format!("REPLACE INTO is MySQL's: write INSERT OR REPLACE INTO {t} …, or INSERT INTO {t} … ON CONFLICT (key) DO UPDATE SET …"))
        }
        "SET" => match s.get(1) {
            Some(Token::Word(v)) if v.value.starts_with('@') && !v.value.starts_with("@@") => Some(user_variable(v.value.trim_start_matches('@'))),
            _ => {
                let option = w(1).filter(|o| SQL_SERVER_SETS.contains(&o.as_str()))?;
                (is(2, "ON") || is(2, "OFF") || option == "IDENTITY_INSERT" || option == "STATISTICS")
                    .then(|| format!("SET {option} is SQL Server's, and nothing here reads it: a setting is SET name = value, and SHOW ALL lists them"))
            }
        },
        "PREPARE" if is(2, "FROM") => Some(format!("PREPARE … FROM is MySQL's: write PREPARE {} AS statement, with $1, $2 … for its ? marks", w(1).map_or("name".into(), |n| n.to_lowercase()))),
        "DECLARE" => match s.get(1) {
            Some(Token::Word(v)) if v.value.starts_with('@') => {
                let x = v.value.trim_start_matches('@');
                Some(format!("DECLARE {} is SQL Server's: write DECLARE ${x} = value, then ${x}", v.value))
            }
            Some(Token::Word(v)) if v.quote_style.is_none() && !v.value.eq_ignore_ascii_case("PARAMETER") && !(2..s.len()).any(|i| matches!(w(i).as_deref(), Some("CURSOR" | "BINARY" | "INSENSITIVE" | "ASENSITIVE" | "SCROLL"))) && (2..s.len()).any(|i| is(i, "DEFAULT")) => {
                let x = v.value.to_lowercase();
                Some(format!("DECLARE {} … DEFAULT is BigQuery's: write DECLARE ${x} = value, then ${x}", v.value))
            }
            _ => None,
        },
        "DELETE" if !is(1, "FROM") && is(2, "FROM") => Some(format!("DELETE {} FROM … JOIN is MySQL's and SQL Server's: write DELETE FROM t USING other WHERE … (the join's condition in the WHERE)", name(s, 1).0)),
        "UPDATE" => {
            let set = (1..s.len()).find(|&i| is(i, "SET"))?;
            (1..set).any(|i| is(i, "JOIN")).then(|| format!("UPDATE … JOIN … SET is MySQL's: write UPDATE {} SET … FROM other WHERE … (the join's condition in the WHERE)", name(s, 1).0))
        }
        "ALTER" if is(1, "TABLE") => {
            let at = if is(2, "IF") && is(3, "EXISTS") { 4 } else { 2 };
            let (t, after) = name(s, at);
            match w(after).as_deref() {
                Some("CHANGE") => Some(format!("ALTER TABLE … CHANGE is MySQL's: write ALTER TABLE {t} RENAME COLUMN a TO b, then ALTER TABLE {t} ALTER COLUMN b TYPE type")),
                Some("MODIFY") => Some(format!("ALTER TABLE … MODIFY is MySQL's: write ALTER TABLE {t} ALTER COLUMN c TYPE type (SET NOT NULL, SET DEFAULT v; AFTER b or FIRST moves it)")),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn other_databases_statements_are_refused_with_what_to_write() {
        let said = |sql: &str| refusal(sql).unwrap_or_else(|| panic!("{sql}: not refused"));
        assert_eq!(said("DESC TABLE customers"), "DESCRIBE TABLE is Snowflake's and Spark's: write DESCRIBE customers");
        assert!(said("EXPLAIN EXTENDED SELECT 1").starts_with("EXPLAIN EXTENDED is Spark's: write EXPLAIN VERBOSE query"));
        assert!(said("EXPLAIN USING TEXT SELECT 1").starts_with("EXPLAIN USING TEXT is Snowflake's"));
        assert!(said("explain format=json select 1").starts_with("EXPLAIN FORMAT=JSON is MySQL's: write EXPLAIN FORMAT pgjson query"));
        assert!(said("SHOW TABLES LIKE 'ORD%' IN SCHEMA PUBLIC").contains("information_schema.tables"));
        assert!(said("SHOW TABLES IN default LIKE 'orders*'").starts_with("SHOW TABLES IN …"));
        assert!(said("CREATE NONCLUSTERED INDEX ix ON t (a) INCLUDE (b)").starts_with("NONCLUSTERED INDEX is SQL Server's"));
        assert!(said("SET NOCOUNT ON").starts_with("SET NOCOUNT is SQL Server's"));
        assert_eq!(said("SET @min_amount := 50"), "SET @min_amount is MySQL's for a variable: here a variable is $min_amount = …, and $min_amount reads it");
        assert!(said("PREPARE by_status FROM 'SELECT id FROM orders WHERE status = ?'").starts_with("PREPARE … FROM is MySQL's: write PREPARE by_status AS statement"));
        assert!(said("DECLARE @n INT = 5").starts_with("DECLARE @n is SQL Server's: write DECLARE $n = value"));
        assert!(said("DECLARE min_amt NUMERIC DEFAULT 50").starts_with("DECLARE min_amt … DEFAULT is BigQuery's: write DECLARE $min_amt = value"));
        assert!(said("DELETE t FROM t_case_402 t JOIN customers c ON c.id = t.customer_id WHERE c.country IS NULL").starts_with("DELETE t FROM … JOIN is MySQL's"));
        assert!(said("UPDATE orders o JOIN customers c ON c.id = o.customer_id SET o.tags = 'uk'").starts_with("UPDATE … JOIN … SET is MySQL's: write UPDATE orders SET … FROM other"));
        assert!(said("ALTER TABLE t_case_401 CHANGE COLUMN flag is_flagged TINYINT(1)").contains("RENAME COLUMN a TO b, then ALTER TABLE t_case_401 ALTER COLUMN b TYPE type"));
        assert!(said("ALTER TABLE s.t MODIFY note TEXT").contains("ALTER TABLE s.t ALTER COLUMN c TYPE type"));
        assert!(said("SELECT c.name, x.id FROM customers AS c CROSS APPLY (SELECT TOP 1 o.id FROM orders AS o) AS x").starts_with("CROSS APPLY is SQL Server's: write CROSS JOIN LATERAL"));
        assert!(said("SELECT [o].[id] FROM [dbo].[orders] AS [o]").starts_with("[o] is SQL Server's quoting of a name: write \"o\""));
        let pivot = "SELECT * FROM (SELECT customer_id, status, amount FROM orders) AS src PIVOT (SUM(amount) FOR status IN ([paid], [shipped])) AS p";
        assert_eq!(said(pivot), "PIVOT (… IN ([paid], …)) is SQL Server's: write the values themselves, IN ('paid', …)");
        assert_eq!(said("SELECT * FROM s PIVOT (sum(n) FOR y IN ([2024], [2025])) p"), "PIVOT (… IN ([2024], …)) is SQL Server's: write the values themselves, IN (2024, …)");
        assert!(said("SELECT * FROM s UNPIVOT (v FOR c IN ([amount], [qty])) u").starts_with("[amount] is SQL Server's quoting of a name: write \"amount\""));
        assert!(said("SELECT 1; SET NOCOUNT ON").starts_with("SET NOCOUNT"), "any statement of a script");
        assert_eq!(said("REPLACE INTO t_case_401 (id, note) VALUES (1, 'refund')"), "REPLACE INTO is MySQL's: write INSERT OR REPLACE INTO t_case_401 …, or INSERT INTO t_case_401 … ON CONFLICT (key) DO UPDATE SET …");
        assert!(said("INSERT INTO t (a, b) OUTPUT inserted.id, inserted.b VALUES (3, N'x')").starts_with("OUTPUT inserted.… is SQL Server's: end the statement with RETURNING"));
        assert!(said("DELETE FROM t OUTPUT deleted.* WHERE a = 1").starts_with("OUTPUT deleted.…"));
        assert!(said("WITH n AS (SELECT 1 AS n UNION ALL SELECT n + 1 FROM n WHERE n < 5) SELECT n FROM n OPTION (MAXRECURSION 10)").ends_with("leave it out (WITH RECURSIVE … stops when a pass adds no rows)"));
        assert_eq!(said("SELECT 1 AS x OPTION (RECOMPILE)"), "OPTION (RECOMPILE …) is SQL Server's hint to its planner: leave it out");
        assert!(said("SELECT o.id, f.value FROM orders o, LATERAL FLATTEN(INPUT => SPLIT(o.tags, ',')) f").starts_with("FLATTEN(INPUT => …) is Snowflake's: unnest the array"));
        assert_eq!(said("SELECT VERSION(), DATABASE(), CURRENT_USER()"), "DATABASE() is MySQL's: write current_database()");
        assert_eq!(said("select user()"), "USER() is MySQL's: write current_user (no parentheses)");
        assert!(said("CREATE OR REPLACE STREAM orders_stream ON TABLE orders").contains("GET /watch/orders?changes=true"));
    }

    #[test]
    fn pragma_upsert_and_create_extension_are_refused_with_pondras_statements() {
        let said = |sql: &str| refusal(sql).unwrap_or_else(|| panic!("{sql}: not refused"));
        let e = refused("PRAGMA table_info('orders')").unwrap_err();
        assert!(crate::codes::of(&e) == "0A000" && e.to_string() == "PRAGMA table_info is DuckDB's and SQLite's: write DESCRIBE orders", "{e:#}");
        assert!(said("-- columns\npragma show(sales.orders)").contains("write DESCRIBE sales.orders"));
        assert!(said("SELECT 1; PRAGMA show_tables").contains("write SHOW TABLES"));
        assert!(said("PRAGMA threads = 4").contains("write SET threads = … to change a setting, SHOW threads to read it"));
        assert_eq!(said("UPSERT INTO orders VALUES (5, 'new')"), "UPSERT is CockroachDB's: write INSERT OR REPLACE INTO orders …, or INSERT INTO orders … ON CONFLICT (key) DO UPDATE SET …");
        assert!(said("SELECT 1; upsert into t values (1)").starts_with("UPSERT is CockroachDB's"));
        assert!(said("CREATE EXTENSION IF NOT EXISTS pg_trgm").contains("CREATE EXTENSION pg_trgm is Postgres's: a Pondra extension is installed from its file, INSTALL pg_trgm FROM 'path or URL', then LOAD pg_trgm"));
        assert!(said(r#"create extension "uuid-ossp""#).contains("CREATE EXTENSION uuid-ossp is Postgres's: gen_random_uuid() is built in"));
        assert!(said("SELECT 1; CREATE EXTENSION pgcrypto WITH SCHEMA public").contains("digest(text, 'sha256')"));
        // (a column, a string or a comment called so is no statement)
        for sql in ["SELECT pragma FROM t -- pragma", "SELECT 'PRAGMA x'", "SELECT upsert FROM t", "SELECT 'create extension x'"] {
            assert_eq!(refusal(sql), None, "{sql}");
        }
    }

    #[test]
    fn pondras_own_statements_are_left_as_they_are() {
        for sql in [
            "DESCRIBE customers", "DESC customers", "EXPLAIN VERBOSE SELECT 1", "EXPLAIN FORMAT tree SELECT 1", "SHOW TABLES", "CREATE OR REPLACE VIEW v AS SELECT 1", "CREATE OR ALTER TABLE t (a INT)",
            "CREATE UNIQUE INDEX i ON t (a)", "SET search_path = s", "SET TIME ZONE 'UTC'", "PREPARE p AS SELECT $1", "DECLARE $x = 5", "DECLARE $n BIGINT DEFAULT 3", "DECLARE PARAMETER $top BIGINT DEFAULT 3", "DECLARE c CURSOR FOR SELECT 1",
            "DECLARE c NO SCROLL CURSOR WITH HOLD FOR SELECT x DEFAULT FROM t", "DELETE FROM t USING o WHERE t.id = o.id", "UPDATE t SET a = 1 FROM o WHERE t.id = o.id",
            "UPDATE t SET a = (SELECT b FROM o JOIN p ON o.k = p.k LIMIT 1)", "ALTER TABLE t ALTER COLUMN change TYPE INT", "ALTER TABLE t RENAME COLUMN modify TO m",
            "SELECT l[1] FROM t", "SELECT [1, 2] AS a, x.apply FROM t x", "SELECT 'CROSS APPLY' AS s", "SELECT 1 -- SET NOCOUNT ON", "SELECT a FROM t JOIN u ON t.k = u.k",
            "INSERT OR REPLACE INTO t VALUES (1)", "CREATE OR REPLACE MACRO m(x) AS x + 1", "CREATE TEMP TABLE t (a INT)", "CREATE TEMP VIEW v AS SELECT 1", "CREATE TEMP MACRO m(x) AS x + 1", "CREATE OR REPLACE TEMPORARY FUNCTION f(x INT) RETURN x", "ALTER TABLE t ADD COLUMN after INT",
            "ALTER TABLE t ADD COLUMN first INT", "ALTER TABLE t ADD COLUMN channel VARCHAR(20) AFTER note", "ALTER TABLE t ADD c INT FIRST", "ALTER TABLE t ALTER COLUMN c AFTER b", "ALTER VIEW v ALTER COLUMN c FIRST", "ALTER MATERIALIZED VIEW m ALTER c AFTER b",
            "ALTER TABLE t ALTER c FIRST", "SELECT current_user, current_database(), current_schema()", "CREATE FUNCTION user() AS 1", "SELECT flatten([[1], [2]])",
            "SELECT items[i].sku FROM t", "SELECT (l)[k].x, m[j].y FROM t", "ALTER TABLE t ADD a INT, ADD COLUMN after TEXT",
            "SELECT * FROM orders AT (TIMESTAMP => now())", "SELECT count(*) FROM past FOR SYSTEM_TIME AS OF (now())", "SELECT * FROM t FOR UPDATE", "CREATE TABLE t (a INT) WITH (option = 'x')", "INSERT INTO t (output) VALUES (1) RETURNING output",
            "SELECT * FROM s PIVOT (sum(n) FOR status IN ('paid', 'shipped')) p", "SELECT * FROM s PIVOT (sum(n) FOR l IN ([1, 2], [3])) p", "SELECT * FROM s UNPIVOT (v FOR c IN (a, b)) u",
            "SELECT * FROM s PIVOT (sum(n) FOR k IN (m[k])) p", "SELECT [x] IN ([x]) FROM t",
        ] {
            assert_eq!(refusal(sql), None, "{sql}");
        }
    }
}
