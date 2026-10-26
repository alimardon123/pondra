//! The statements 1.1 brings, refused by name in 1.0 at every door and form (ADR-072): pipelines and dashboards, the
//! grants on them, and `GRANT EXECUTE`. One list, read at the front (`front::expand`, which every node door and the
//! page pass through), so a word added here is refused at every door in the same words. A pipeline's STEP, PARALLEL and
//! WAIT FOR are refused where a statement starts: here, and in a script before any of it runs (`script.rs`, with its
//! line). A statement that starts with none of the heads below is not read further, and a query pays nothing.

/// What a pipeline's statements say; `{}` is the statement's words.
const PIPELINE: &str = "{}: pipelines come in 1.1; until then, tasks that follow others run statements in order (CREATE TASK … AFTER …)";
/// What a dashboard's statements say; `{}` is the statement's words.
const DASHBOARD: &str = "{}: dashboards come in 1.1; until then, a SQL file's Chart tab in the console draws an answer";
/// A pipeline's STEP, where a script's statement starts.
const STEP: &str = "STEP is a pipeline's, coming in 1.1 (CREATE PIPELINE … AS BEGIN STEP … END); in a script each statement is already a step";
/// A pipeline's PARALLEL, where a script's statement starts.
const PARALLEL: &str = "PARALLEL … END PARALLEL is a pipeline's, coming in 1.1; in a script, ASYNC … AWAIT ALL runs statements side by side, and FOR … PARALLEL n a loop's passes";
/// A pipeline's WAIT FOR, where a script's statement starts.
const WAIT_FOR: &str = "WAIT FOR is a pipeline's, coming in 1.1";
const GRANT_EXECUTE: &str = "GRANT EXECUTE comes in 1.1, with pipelines";

/// A task's `AFTER … FAILED` or `AFTER … ALWAYS` (`runs::task`): a pipeline's step, not a task's.
#[cfg_attr(not(feature = "lake"), allow(dead_code))]
pub(crate) const AFTER_FAILED: &str = "AFTER … FAILED and AFTER … ALWAYS are a pipeline step's, coming in 1.1: a task after others runs when they all succeeded";
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
];

/// The first words a refused statement can start with: a statement that starts with none of them is not read further.
const HEADS: &[&str] = &["create", "alter", "drop", "show", "explain", "comment", "grant", "revoke", "execute"];

/// What is refused in `sql`, if it is one of 1.1's statements: the words that say what to write instead. None for any other.
pub fn refusal(sql: &str) -> Option<String> {
    let first = crate::text::head(sql);
    if matches!(first.as_str(), "step" | "parallel" | "wait") {
        let next = crate::text::first_words(sql, 2).into_iter().nth(1);
        return pipeline_part(&first, next.as_deref()).map(str::to_string); // (a page has no scripts: it reads these here)
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

/// A name: a word that starts with a letter, `_` or `"`, and is not `TO` or `FROM` (what a grant says after its object).
fn is_name(word: &str) -> bool {
    !matches!(word, "TO" | "FROM") && word.starts_with(|c: char| c.is_alphabetic() || c == '_' || c == '"')
}

/// `GRANT` or `REVOKE` … ON PIPELINE name, or … ON DASHBOARD name, where the name follows the first ON. `GRANT SELECT ON
/// pipeline TO r` is a table's grant, as it was.
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
}
