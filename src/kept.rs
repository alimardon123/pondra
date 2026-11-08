//! The statements 1.1 brings, refused by name in 0.80 at every door and form (ADR-072): pipelines, dashboards, the grants on
//! them, and `GRANT EXECUTE` (connections are the `connections` extension's now: `connect.rs`). One list, read at the front (`front::expand`, which every node door and the
//! page pass through), so a word added here is refused at every door in the same words. A pipeline's STEP, PARALLEL and
//! WAIT FOR are refused where a statement starts: here, and in a script before any of it runs (`script.rs`, with its
//! line). A statement that starts with none of the heads below is not read further, and a query pays nothing.
//! The names kept free for later (`CREATE TABLE … USING x`, `VECTOR(n)` and the other AI types) are refused here too, by name, and only a
//! statement that spells one of them is tokenised. The list also holds what Postgres has and Pondra doesn't yet: advisory
//! locks, and LISTEN, UNLISTEN and NOTIFY. Migration tools send those first, so the answer says what to write instead. A
//! function is looked for anywhere in a statement, but only one that spells `advisory`, `pg_notify` or a name of `FUNCTIONS`
//! is tokenised. `FUNCTIONS` are names kept free for 1.1 (`ai_agent`): refused where one is called or made, and as the name
//! of a function a Flight server runs (`POST /functions/<name>`).

use crate::text::contains_ci;
use datafusion::sql::sqlparser::tokenizer::Token;

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
/// The function names kept free for 1.1, and what each says. 1.1 gives each its meaning without changing what anything
/// meant, so none can be called, made (`CREATE FUNCTION`, `CREATE PROCEDURE`) or registered before then. `ai_agent`: a
/// model that calls the lake's tools in a loop, called from a task (designs/ai-data-plan.md §9).
const FUNCTIONS: &[(&str, &str)] = &[(
    "ai_agent",
    "ai_agent(…): comes with models in 1.1; until then, a task calls ai_complete(…) on a SCHEDULE, and an agent outside works through the MCP server",
)];

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
    let tokens = crate::text::tokens(sql)?.solid_tokens();
    if let Some(format) = table_using(&tokens) {
        return Some(USING.replace("{}", &format));
    }
    let typed = |i: usize| matches!(tokens.get(i + 1), Some(Token::LParen)) && !names_a_relation(&tokens, i);
    (0..tokens.len()).filter(|&i| typed(i)).find_map(|i| TYPES.iter().find(|(name, _)| is_word(&tokens[i], name))).map(|(_, says)| says.to_string())
}

/// What a function named in `sql` says, when it is one Postgres has and Pondra doesn't yet: `pg_notify`, or an advisory
/// lock (`pg_advisory_lock`, `pg_try_advisory_xact_lock`, `pg_advisory_unlock_all`…); or one kept free (`FUNCTIONS`),
/// called or made. Read anywhere in a statement, since a call can sit in a SELECT's list, a WHERE or a CALL. Only a
/// statement that spells one of them is tokenised, so the rest pay nothing; a string or a column named so is no call,
/// and the tokens show it.
fn function_kept(sql: &str) -> Option<String> {
    if !contains_ci(sql, "advisory") && !contains_ci(sql, "pg_notify") && !FUNCTIONS.iter().any(|(name, _)| contains_ci(sql, name)) {
        return None;
    }
    let tokens = crate::text::tokens(sql)?.solid_tokens();
    // (a call, or a function made: a word before `(` that names no table, and no alias after AS)
    let called = |i: usize| matches!(tokens.get(i + 1), Some(Token::LParen)) && !names_a_relation(&tokens, i) && !before(&tokens, i).is_some_and(|t| is_word(t, "as"));
    (0..tokens.len()).filter(|&i| called(i)).find_map(|i| match &tokens[i] {
        // (a quoted name is the same name only as it is stored, lower case: `"ai_agent"(…)` is the function, `"AI_AGENT"` another)
        Token::Word(word) if word.quote_style.is_some() => FUNCTIONS.iter().find(|(name, _)| word.value == *name).map(|(_, says)| says.to_string()),
        Token::Word(word) => {
            let name = word.value.to_lowercase();
            if let Some(says) = function_named(&name) {
                Some(says.to_string())
            } else if name == "pg_notify" {
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

/// The token before the name that ends at `at`, past its `schema.` and `catalog.` (None at the start).
fn before(tokens: &[Token], at: usize) -> Option<&Token> {
    let mut i = at;
    while i >= 2 && matches!(tokens[i - 1], Token::Period) && matches!(tokens[i - 2], Token::Word(_)) {
        i -= 2;
    }
    i.checked_sub(1).map(|i| &tokens[i])
}

/// Does the word at `at`, before a `(`, name a table, a view or a WITH query, where a kept name is free to use: after TABLE,
/// EXISTS, INTO, COPY, REFERENCES, ON or VIEW (`CREATE TABLE ai_agent (…)`, `INSERT INTO vector (a)`), or a WITH query's
/// `name (columns) AS (…)`. Anywhere else it calls a function or names a type.
fn names_a_relation(tokens: &[Token], at: usize) -> bool {
    if before(tokens, at).is_some_and(|t| ["table", "exists", "into", "copy", "references", "on", "view"].iter().any(|w| is_word(t, w))) {
        return true;
    }
    let mut depth = 0;
    for (i, t) in tokens.iter().enumerate().skip(at + 1) {
        match t {
            Token::LParen => depth += 1,
            Token::RParen if depth == 1 => {
                let then = tokens.get(i + 2);
                return tokens.get(i + 1).is_some_and(|t| is_word(t, "as")) && then.is_some_and(|t| matches!(t, Token::LParen) || is_word(t, "materialized") || is_word(t, "not"));
            }
            Token::RParen => depth -= 1,
            _ => {}
        }
    }
    false
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

/// `GRANT` or `REVOKE` … ON PIPELINE name or … ON DASHBOARD name, where the name follows the first
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

/// What a function named `name` says, when the name is kept free (`FUNCTIONS`), in any case: for a call's name, and for
/// the name a Flight server's function is registered under (`POST /functions/<name>`).
pub fn function_named(name: &str) -> Option<&'static str> { FUNCTIONS.iter().find(|(kept, _)| name.eq_ignore_ascii_case(kept)).map(|(_, says)| *says) }

/// The front's pass of this module (`passes.rs`): `refused`, triggered by a statement's first word, a kept type's name, or
/// a kept function's (a lock's `advisory`, `pg_notify`).
pub(crate) fn pass() -> crate::passes::Pass {
    let words = HEADS.iter().copied().chain(["step", "parallel", "wait"]).chain(TYPES.iter().map(|(n, _)| *n)).collect();
    let marks = ["advisory", "pg_notify"].into_iter().chain(FUNCTIONS.iter().map(|(n, _)| *n)).map(|m| -> &'static [&'static str] { Box::leak(Box::new([m])) }).collect();
    crate::passes::Pass { name: "kept.refused", order: 10, words, marks, run: crate::passes::Run::Refuse(refused) }
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
        assert_eq!(refusal("CREATE CONNECTION crm TYPE s3"), None); // (the `connections` extension's: `connect.rs`)
        assert_eq!(refusal("GRANT READ ON CONNECTION crm TO r"), None);
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

    /// `ai_agent`, kept free for 1.1: refused where it is called (a query, a CALL, a task's body) or made, in any case, and
    /// as a Flight function's name. A table, a view, a WITH query or a column named so is not one, and neither is a string.
    /// A table named `vector` is a table, not the type.
    #[test]
    fn keeps_ai_agent_free() {
        let says = function_named("ai_agent").unwrap();
        assert!(says.starts_with("ai_agent(…): comes with models in 1.1"), "{says}");
        for sql in [
            "SELECT ai_agent('Find why refunds jumped')",
            "CALL ai_agent('Find why refunds jumped', model => 'fast')",
            "CREATE TASK watch_refunds SCHEDULE '5 minutes' WHEN (SELECT count(*) > 50 FROM refunds_today) AS CALL ai_agent('Find why refunds jumped', model => 'fast')",
            "select AI_AGENT(x) from t",
            "SELECT \"ai_agent\"(1)",
            "SELECT pondra.ai_agent('x')",
            "SELECT * FROM t WHERE ai_agent(body) IS NOT NULL",
            "CREATE FUNCTION ai_agent(goal VARCHAR) RETURNS VARCHAR AS $$ SELECT goal $$",
            "CREATE OR REPLACE PROCEDURE crm.ai_agent() AS $$ SELECT 1 $$",
        ] {
            assert_eq!(refusal(sql).as_deref(), Some(says), "{sql}");
        }
        assert_eq!(function_named("AI_Agent"), Some(says));
        for sql in [
            "SELECT 'ai_agent(1)'",
            "SELECT ai_agent FROM t",
            "SELECT \"AI_AGENT\"(1)",
            "SELECT ai_agents(1)",
            "CREATE TABLE ai_agent (id INT, note VARCHAR)",
            "CREATE TABLE IF NOT EXISTS ops.ai_agent (id INT)",
            "INSERT INTO ai_agent (id) VALUES (1)",
            "COPY ai_agent (id) FROM STDIN",
            "CREATE VIEW ai_agent (a) AS SELECT 1",
            "CREATE TABLE runs (agent INT REFERENCES ai_agent (id))",
            "CREATE INDEX i ON ai_agent (id)",
            "WITH ai_agent (n) AS (SELECT 1) SELECT n FROM ai_agent",
            "WITH a AS (SELECT 1), ai_agent (n) AS MATERIALIZED (SELECT 2) SELECT * FROM ai_agent",
            "SELECT * FROM t AS ai_agent (a, b)",
        ] {
            assert_eq!(refusal(sql), None, "{sql}");
        }
        for sql in ["CREATE TABLE vector (a INT)", "INSERT INTO vector (a) VALUES (1)", "CREATE TABLE pg_notify (a INT)"] {
            assert_eq!(refusal(sql), None, "{sql}");
        }
    }

    /// The names kept free are no function a session has today, in any set (DataFusion's, ours or an extension's): 1.1 gives
    /// each its meaning, so none may already mean something.
    #[test]
    fn kept_names_are_no_function_yet() {
        let list = crate::lists::all().iter().find(|l| l.name == "functions").expect("the engine lists its functions");
        let rows = crate::lists::rows(list);
        assert!(rows.iter().any(|r| r["name"] == "ai_complete"), "the list has the ai functions");
        for (name, _) in FUNCTIONS {
            assert!(!rows.iter().any(|r| r["name"].as_str().is_some_and(|n| n.eq_ignore_ascii_case(name))), "{name} is a function already");
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
