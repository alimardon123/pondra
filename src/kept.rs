//! The statements 1.1 brings, refused by name in 1.0 at every door and form (ADR-072): pipelines, dashboards, the grants on
//! them, and `GRANT EXECUTE`; and connections (1.1, one object for every outside place), shown the `ATTACH` and the secret 1.0 writes. One list, read at the front (`front::expand`, which every node door and the
//! page pass through), so a word added here is refused at every door in the same words. A pipeline's STEP, PARALLEL and
//! WAIT FOR are refused where a statement starts: here, and in a script before any of it runs (`script.rs`, with its
//! line). A statement that starts with none of the heads below is not read further, and a query pays nothing.
//! The names kept free for later (`CREATE TABLE … USING x`, `VECTOR(n)` and the other AI types) are refused here too, by name, and only a
//! statement that spells one of them is tokenised. The list also holds what Postgres has and Pondra doesn't yet: advisory
//! locks, and LISTEN, UNLISTEN and NOTIFY. Migration tools send those first, so the answer says what to write instead. A
//! function is looked for anywhere in a statement, but only one that spells `advisory` or `pg_notify` is tokenised.

use crate::text::contains_ci;
use datafusion::sql::sqlparser::{dialect::GenericDialect, tokenizer::{Token, Tokenizer}};

/// What a table's `USING x` says, where a table is made; `{}` is the format named.
const USING: &str = "CREATE TABLE … USING {}: tables kept in another format come with extensions (1.2); Pondra keeps its tables in its own lake, published as Delta or Iceberg with WITH (publish = …)";
/// The types kept free for after 1.0 (ADR-051), each a word written before `(`, and what it says: a vector or a tensor is
/// FLOAT[] until then. 1.1 gives them their meaning without changing what anything meant (designs/ai-data-plan.md).
const TYPES: &[(&str, &str)] = &[
    ("vector", "VECTOR(n): comes with vector indexes (ADR-051, after 1.0); a vector is FLOAT[] until then"),
    ("halfvec", "HALFVEC(n): half-precision vectors come with vector indexes (ADR-051, after 1.0); a vector is FLOAT[] until then"),
    ("sparsevec", "SPARSEVEC(n): sparse vectors come with vector indexes (ADR-051, after 1.0); a vector is FLOAT[] until then"),
    ("tensor", "TENSOR(…): tensors come after 1.0 (ADR-051); a tensor is a FLOAT[] list until then"),
];

/// What a pipeline's statements say; `{}` is the statement's words.
const PIPELINE: &str = "{}: pipelines come in 1.1; until then, tasks that follow others run statements in order (CREATE TASK … AFTER …)";
/// What a dashboard's statements say; `{}` is the statement's words.
const DASHBOARD: &str = "{}: dashboards come in 1.1; until then, a SQL file's Chart tab in the console draws an answer";
/// A pipeline's STEP, where a script's statement starts.
const STEP: &str = "STEP: pipelines come in 1.1";
/// A pipeline's PARALLEL, where a script's statement starts.
const PARALLEL: &str = "PARALLEL … END PARALLEL is a pipeline's, coming in 1.1; in a script, ASYNC … AWAIT ALL runs statements side by side, and FOR … PARALLEL n a loop's passes";
/// A pipeline's WAIT FOR, where a script's statement starts.
const WAIT_FOR: &str = "WAIT FOR is a pipeline's, coming in 1.1";
const GRANT_EXECUTE: &str = "GRANT EXECUTE comes in 1.1, with pipelines";
/// What a connection's statements say; `{}` is the statement's words. A connection, one object for every outside place (a
/// database, a bucket, SFTP, HTTP), comes in 1.1 with `ATTACH` as its one-line form for tables (Alimardon, 10-31 15:28;
/// `designs/etl-and-bi-tools.md` §3.7), so the words show what 1.0 writes instead.
const CONNECTION: &str = "{} comes in 1.1. Today a lake or catalog is attached: ATTACH 'url' AS name (TYPE delta, iceberg, kafka or share; a Pondra lake takes none); a bucket's or an API's credentials are a SECRET with a SCOPE: CREATE SECRET … SCOPE 's3://bucket'";
/// `SHOW CONNECTIONS`: what `ATTACH` makes is listed as a database.
const SHOW_CONNECTIONS: &str = "SHOW CONNECTIONS: connections come in 1.1; SHOW DATABASES lists what ATTACH makes";
/// What a Postgres advisory-lock function says; `{}` is its name. Migration tools take one first (Rails, Prisma Migrate,
/// Flyway, golang-migrate), so the answer says what locks a row instead.
const ADVISORY: &str = "{}: advisory locks aren't built yet (planned for 1.1); until then, a transaction locks a row: SELECT … FROM a lock table WHERE … FOR UPDATE, held until its COMMIT";
/// What LISTEN, UNLISTEN, NOTIFY and `pg_notify` say; `{}` is the statement's words or the function's name. Migration tools
/// send these first, so the answer says what streams rows instead.
const LISTEN: &str = "{}: LISTEN and NOTIFY aren't built yet (planned for 1.1); until then, GET /watch/{table} streams a table's rows as they commit";

/// A step's `ON FAILURE [OF step]` or `ALWAYS` in a task's head (`runs::task`), or the older `AFTER … FAILED`: a pipeline
/// step's, not a task's.
#[cfg_attr(not(feature = "lake"), allow(dead_code))]
pub(crate) const ON_FAILURE: &str = "ON FAILURE [OF step] and ALWAYS are a pipeline step's, coming in 1.1: a task after others runs when they all succeeded";
/// A task's `ON FILES` or `ON CHANGE OF`, where a task's SCHEDULE or AFTER goes (`runs::task`).
#[cfg_attr(not(feature = "lake"), allow(dead_code))]
pub(crate) const ON_FILES: &str = "ON FILES and ON CHANGE OF start a pipeline, coming in 1.1: a task runs on SCHEDULE '…' or AFTER task";

/// The statements refused by their first words (`text::first_words`, upper-cased), and what each says.
const REFUSED: &[(&[&str], &str)] = &[
    (&["CREATE", "PIPELINE"], PIPELINE),
    (&["CREATE", "OR", "REPLACE", "PIPELINE"], PIPELINE),
    (&["ALTER", "PIPELINE"], PIPELINE),
    (&["DROP", "PIPELINE"], PIPELINE),
    (&["SHOW", "CREATE", "PIPELINE"], PIPELINE),
    (&["SHOW", "PIPELINES"], PIPELINE),
    (&["EXPLAIN", "PIPELINE"], PIPELINE),
    (&["COMMENT", "ON", "PIPELINE"], PIPELINE),
    (&["CREATE", "DASHBOARD"], DASHBOARD),
    (&["CREATE", "OR", "REPLACE", "DASHBOARD"], DASHBOARD),
    (&["ALTER", "DASHBOARD"], DASHBOARD),
    (&["DROP", "DASHBOARD"], DASHBOARD),
    (&["SHOW", "CREATE", "DASHBOARD"], DASHBOARD),
    (&["SHOW", "DASHBOARDS"], DASHBOARD),
    (&["COMMENT", "ON", "DASHBOARD"], DASHBOARD),
    (&["CREATE", "CONNECTION"], CONNECTION),
    (&["CREATE", "OR", "REPLACE", "CONNECTION"], CONNECTION),
    (&["ALTER", "CONNECTION"], CONNECTION),
    (&["DROP", "CONNECTION"], CONNECTION),
    (&["SHOW", "CREATE", "CONNECTION"], CONNECTION),
    (&["SHOW", "CONNECTIONS"], SHOW_CONNECTIONS),
    (&["COMMENT", "ON", "CONNECTION"], CONNECTION),
    (&["GRANT", "EXECUTE", "ON"], GRANT_EXECUTE), // (a bare `GRANT execute TO u` grants a role, as it did)
    (&["REVOKE", "EXECUTE", "ON"], GRANT_EXECUTE),
    (&["LISTEN"], LISTEN),
    (&["UNLISTEN"], LISTEN),
    (&["NOTIFY"], LISTEN),
];

/// The first words a refused statement can start with: a statement that starts with none of them is not read further.
const HEADS: &[&str] = &["create", "alter", "drop", "show", "explain", "comment", "grant", "revoke", "execute", "listen", "unlisten", "notify"];

/// What is refused in `sql`, if it is one of 1.1's statements: the words that say what to write instead. None for any other.
pub fn refusal(sql: &str) -> Option<String> {
    let first = crate::text::head(sql);
    if matches!(first.as_str(), "step" | "parallel" | "wait") {
        let next = crate::text::first_words(sql, 2).into_iter().nth(1);
        return pipeline_part(&first, next.as_deref()).map(str::to_string); // (a page has no scripts: it reads these here)
    }
    if let Some(why) = type_kept(sql, &first) {
        return Some(why);
    }
    if let Some(why) = function_kept(sql) {
        return Some(why);
    }
    if !HEADS.contains(&first.as_str()) {
        return None;
    }
    // (a GRANT, a REVOKE and an EXECUTE name their object further on, so they read the whole statement)
    let whole = matches!(first.as_str(), "grant" | "revoke" | "execute");
    let words = crate::text::first_words(sql, if whole { usize::MAX } else { 4 });
    match REFUSED.iter().find(|(pattern, _)| crate::text::begins(&words, pattern)) {
        Some((pattern, what)) => Some(what.replace("{}", &pattern.join(" "))),
        None => grant_on(&words).or_else(|| execute_pipeline(&words)),
    }
}

/// The names kept free for later, refused where a statement spells one: a type of `TYPES` anywhere (a column's or a cast's),
/// and `CREATE TABLE … USING x` (`head` is the statement's first word). Read from the tokens, so a string or a join's
/// `USING (id)` is not one. A statement that spells none, the common case, is not tokenised.
fn type_kept(sql: &str, head: &str) -> Option<String> {
    let typed = contains_ci(sql, "vec") || contains_ci(sql, "tensor"); // (vector, halfvec, sparsevec, tensor)
    let using = head == "create" && contains_ci(sql, "using");
    if !typed && !using {
        return None;
    }
    let tokens = tokens_of(sql)?;
    if let Some(format) = table_using(&tokens) {
        return Some(USING.replace("{}", &format));
    }
    tokens.windows(2).filter(|w| matches!(w[1], Token::LParen)).find_map(|w| TYPES.iter().find(|(name, _)| is_word(&w[0], name))).map(|(_, says)| says.to_string())
}

/// What a function named in `sql` says, when it is one Postgres has and Pondra doesn't yet: `pg_notify`, or an advisory
/// lock (`pg_advisory_lock`, `pg_try_advisory_xact_lock`, `pg_advisory_unlock_all`…). Read anywhere in a statement, since
/// a call can sit in a SELECT's list or a WHERE. Only a statement that spells `advisory` or `pg_notify` is tokenised, so
/// the rest pay nothing; a string or a column named so is no call, and the tokens show it.
fn function_kept(sql: &str) -> Option<String> {
    if !contains_ci(sql, "advisory") && !contains_ci(sql, "pg_notify") {
        return None;
    }
    let tokens = tokens_of(sql)?;
    tokens.windows(2).find_map(|w| match (&w[0], &w[1]) {
        (Token::Word(word), Token::LParen) if word.quote_style.is_none() => {
            let name = word.value.to_lowercase();
            if name == "pg_notify" {
                Some(LISTEN.replace("{}", &name))
            } else if name.starts_with("pg_") && name.contains("advisory") {
                Some(ADVISORY.replace("{}", &name))
            } else {
                None
            }
        }
        _ => None,
    })
}

/// The tokens of `sql` without its whitespace, so a word's neighbours are the next tokens. None when the tokeniser refuses it.
fn tokens_of(sql: &str) -> Option<Vec<Token>> {
    Some(Tokenizer::new(&GenericDialect {}, sql).tokenize().ok()?.into_iter().filter(|t| !matches!(t, Token::Whitespace(_))).collect())
}

/// The format a `CREATE TABLE … USING x` names (`x`): a USING in the table's header, which ends at an AS, a SELECT or a VALUES
/// outside the columns' parentheses. A join's `USING (id)` after that is another statement's, and None.
fn table_using(tokens: &[Token]) -> Option<String> {
    let (mut depth, mut table) = (0, false);
    for (i, t) in tokens.iter().enumerate() {
        match t {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth > 0 => {}
            _ if is_word(t, "table") => table = true,
            _ if is_word(t, "as") || is_word(t, "select") || is_word(t, "values") => return None,
            _ if is_word(t, "using") && table => {
                if let Some(Token::Word(format)) = tokens.get(i + 1) {
                    return Some(format.value.clone());
                }
            }
            _ => {}
        }
    }
    None
}

/// Is `t` the unquoted word `name`, in any case.
fn is_word(t: &Token, name: &str) -> bool { matches!(t, Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(name)) }

/// A name: a word that starts with a letter, `_` or `"`, and is not `TO` or `FROM` (what a grant says after its object).
fn is_name(word: &str) -> bool {
    !matches!(word, "TO" | "FROM") && word.starts_with(|c: char| c.is_alphabetic() || c == '_' || c == '"')
}

/// `GRANT` or `REVOKE` … ON PIPELINE name, … ON DASHBOARD name or … ON CONNECTION name, where the name follows the first
/// ON. `GRANT SELECT ON pipeline TO r` is a table's grant, as it was.
fn grant_on(words: &[String]) -> Option<String> {
    let verb = words.first()?;
    if verb != "GRANT" && verb != "REVOKE" {
        return None;
    }
    let at = words.iter().position(|w| w == "ON")?;
    let named = words.get(at + 2).is_some_and(|n| is_name(n));
    match words.get(at + 1)?.as_str() {
        "PIPELINE" if named => Some(PIPELINE.replace("{}", &format!("{verb} ON PIPELINE"))),
        "DASHBOARD" if named => Some(DASHBOARD.replace("{}", &format!("{verb} ON DASHBOARD"))),
        "CONNECTION" if named => Some(CONNECTION.replace("{}", &format!("{verb} ON CONNECTION"))),
        _ => None,
    }
}

/// `EXECUTE PIPELINE name`, with a name after the word. `EXECUTE pipeline` and `EXECUTE pipeline (1)` are a prepared statement
/// called pipeline, as Postgres's EXECUTE is, and stay so.
fn execute_pipeline(words: &[String]) -> Option<String> {
    match words {
        [execute, pipeline, name, ..] if execute == "EXECUTE" && pipeline == "PIPELINE" && is_name(name) => Some(PIPELINE.replace("{}", "EXECUTE PIPELINE")),
        _ => None,
    }
}

/// A pipeline's word where a script's statement starts (`script.rs`, lower-cased): STEP, PARALLEL, and WAIT only before FOR
/// (any other WAIT is a statement like the rest, and None). `next` is the word after it.
pub fn pipeline_part(word: &str, next: Option<&str>) -> Option<&'static str> {
    match word {
        "step" => Some(STEP),
        "parallel" => Some(PARALLEL),
        "wait" if next.is_some_and(|n| n.eq_ignore_ascii_case("for")) => Some(WAIT_FOR),
        _ => None,
    }
}

/// The refusal for `sql` (0A000) if it is one of 1.1's statements: `refusal`, as an error every door answers.
pub fn refused(sql: &str) -> anyhow::Result<()> {
    match refusal(sql) {
        Some(why) => Err(crate::codes::coded("0A000", why)),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_what_1_1_takes() {
        assert!(refusal("CREATE PIPELINE p AS BEGIN STEP a AS SELECT 1; END").unwrap().starts_with("CREATE PIPELINE: pipelines come in 1.1"));
        assert!(refusal("create or replace pipeline p as begin end").unwrap().starts_with("CREATE OR REPLACE PIPELINE: pipelines come in 1.1"));
        assert!(refusal("SHOW PIPELINES").unwrap().starts_with("SHOW PIPELINES: pipelines come in 1.1"));
        assert!(refusal("DROP DASHBOARD sales").unwrap().starts_with("DROP DASHBOARD: dashboards come in 1.1"));
        assert!(refusal("GRANT SELECT ON PIPELINE p TO r").unwrap().starts_with("GRANT ON PIPELINE: pipelines come in 1.1"));
        assert!(refusal("GRANT SELECT, INSERT ON DASHBOARD d TO r").unwrap().starts_with("GRANT ON DASHBOARD: dashboards come in 1.1"));
        assert!(refusal("REVOKE SELECT ON DASHBOARD d FROM r").unwrap().starts_with("REVOKE ON DASHBOARD: dashboards come in 1.1"));
        assert!(refusal("GRANT EXECUTE ON PIPELINE p TO r").unwrap().starts_with("GRANT EXECUTE comes in 1.1"));
        assert!(refusal("REVOKE EXECUTE ON FUNCTION f FROM r").unwrap().starts_with("GRANT EXECUTE comes in 1.1"));
        assert!(refusal("EXECUTE PIPELINE sales.orders (day => 1)").unwrap().starts_with("EXECUTE PIPELINE: pipelines come in 1.1"));
        assert!(refusal("EXECUTE PIPELINE nightly").unwrap().starts_with("EXECUTE PIPELINE: pipelines come in 1.1"));
        assert!(refusal("CREATE CONNECTION crm (TYPE postgres, HOST $host)").unwrap().starts_with("CREATE CONNECTION comes in 1.1. Today a lake or catalog is attached: ATTACH"));
        assert!(refusal("ALTER CONNECTION crm SET (db => 'eu')").unwrap().contains("CREATE SECRET … SCOPE 's3://bucket'"));
        assert_eq!(refusal("show connections").as_deref(), Some(SHOW_CONNECTIONS));
        assert!(refusal("GRANT USAGE ON CONNECTION crm TO r").unwrap().starts_with("GRANT ON CONNECTION comes in 1.1"));
        assert_eq!(refusal("GRANT SELECT ON connection TO r"), None); // (a table called connection)
        assert_eq!(refusal("STEP a AS SELECT 1").as_deref(), Some(STEP));
        assert_eq!(refusal("parallel SELECT 1").as_deref(), Some(PARALLEL));
        assert_eq!(refusal("WAIT FOR p").as_deref(), Some(WAIT_FOR));
    }

    #[test]
    fn script_words_name_their_part() {
        assert_eq!(pipeline_part("step", Some("a")), Some(STEP));
        assert_eq!(pipeline_part("parallel", None), Some(PARALLEL));
        assert_eq!(pipeline_part("wait", Some("for")), Some(WAIT_FOR));
        assert_eq!(pipeline_part("wait", Some("x")), None);
        assert_eq!(pipeline_part("wait", None), None);
        assert_eq!(pipeline_part("select", Some("1")), None);
    }

    #[test]
    fn leaves_the_rest_alone() {
        for sql in [
            "CREATE TABLE pipeline (a INT)",
            "CREATE TABLE dashboard (a INT)",
            "SELECT * FROM pipeline",
            "GRANT SELECT ON pipeline TO r",
            "REVOKE SELECT ON pipeline FROM r",
            "GRANT SELECT ON TABLE pipeline TO r",
            "GRANT execute TO u",
            "REVOKE execute FROM u",
            "EXECUTE pipeline",
            "EXECUTE pipeline(1)",
            "EXECUTE pipeline (1)",
            "EXECUTE TASK nightly",
            "SELECT step, parallel FROM t",
            "SELECT 1 AS parallel",
            "WAIT 5",
        ] {
            assert!(refusal(sql).is_none(), "{sql}");
        }
    }

    /// `CREATE TABLE … USING x` names the format a table is kept in, and `VECTOR(n)` a column's or a cast's type: both
    /// refused by name, with the words that say when they come.
    #[test]
    fn names_the_types_kept_free() {
        let using = "CREATE TABLE … USING delta: tables kept in another format come with extensions (1.2)";
        assert!(refusal("CREATE TABLE t (id INT, name VARCHAR) USING delta").unwrap().starts_with(using));
        assert!(refusal("create or replace table if not exists t (a int) using Lance").unwrap().starts_with("CREATE TABLE … USING Lance: tables kept"));
        assert!(refusal("CREATE TABLE t USING parquet AS SELECT 1 AS a").unwrap().starts_with("CREATE TABLE … USING parquet: tables kept"));
        assert!(refusal("CREATE TABLE t (a INT) USING delta").unwrap().ends_with("published as Delta or Iceberg with WITH (publish = …)"));
        let vector = "VECTOR(n): comes with vector indexes (ADR-051, after 1.0); a vector is FLOAT[] until then";
        assert_eq!(refusal("CREATE TABLE t (id INT, v VECTOR(3))").as_deref(), Some(vector));
        assert_eq!(refusal("ALTER TABLE t ADD COLUMN v VECTOR(3)").as_deref(), Some(vector));
        assert_eq!(refusal("SELECT CAST(x AS VECTOR(FLOAT, 3)) FROM t").as_deref(), Some(vector));
        // (the AI types kept free too, designs/ai-data-plan.md)
        assert!(refusal("CREATE TABLE t (v HALFVEC(768))").unwrap().starts_with("HALFVEC(n): half-precision vectors come with vector indexes"));
        assert!(refusal("ALTER TABLE t ADD COLUMN v sparsevec(30000)").unwrap().starts_with("SPARSEVEC(n): sparse vectors"));
        assert!(refusal("CREATE TABLE t (x TENSOR(FLOAT, 3, 224, 224))").unwrap().starts_with("TENSOR(…): tensors come after 1.0"));
        for sql in [
            "CREATE TABLE t AS SELECT * FROM a JOIN b USING (id)",
            "CREATE TABLE t (a INT) WITH (publish = 'delta')",
            "SELECT * FROM a JOIN b USING (id)",
            "SELECT 'VECTOR(3)' AS s",
            "SELECT vector FROM t",
            "SELECT tensor, halfvec FROM t",
            "SELECT * FROM vectors(3)",
            "CREATE INDEX i ON t USING hnsw (v)",
        ] {
            let refused = refusal(sql);
            assert!(refused.as_deref().is_none_or(|w| !w.contains("1.2") && !w.contains("ADR-051")), "{sql}: {refused:?}");
        }
    }

    /// Advisory locks and LISTEN/NOTIFY, which Postgres has and Pondra doesn't yet: refused by name, with the words that say
    /// what to write instead, since a migration tool sends them first. A string, a column or a table named so is not one.
    #[test]
    fn names_what_postgres_has_and_pondra_lacks() {
        let advisory = |name: &str| ADVISORY.replace("{}", name);
        assert_eq!(refusal("SELECT pg_advisory_lock(1)"), Some(advisory("pg_advisory_lock")));
        assert_eq!(refusal("select pg_try_advisory_xact_lock(42)"), Some(advisory("pg_try_advisory_xact_lock")));
        assert_eq!(refusal("SELECT pg_advisory_unlock_all()"), Some(advisory("pg_advisory_unlock_all")));
        assert_eq!(refusal("LISTEN jobs"), Some(LISTEN.replace("{}", "LISTEN")));
        assert_eq!(refusal("notify jobs, 'x'"), Some(LISTEN.replace("{}", "NOTIFY")));
        assert_eq!(refusal("UNLISTEN *"), Some(LISTEN.replace("{}", "UNLISTEN")));
        assert_eq!(refusal("SELECT pg_notify('c', 'p')"), Some(LISTEN.replace("{}", "pg_notify")));
        assert!(refusal("LISTEN jobs").unwrap().starts_with("LISTEN: LISTEN and NOTIFY aren't built yet (planned for 1.1)"));
        assert!(refusal("SELECT pg_advisory_lock(1)").unwrap().contains("a transaction locks a row"));
        for sql in ["SELECT 'pg_advisory_lock(1)'", "SELECT advisory FROM t", "SELECT * FROM listen_log", "SELECT \"pg_advisory_lock\" FROM t"] {
            assert_eq!(refusal(sql), None, "{sql}");
        }
    }
}
