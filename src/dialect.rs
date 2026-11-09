//! The front's fallback dialect (step 8, PR 3a): GenericDialect, but for the hooks the registry's extensions add (the
//! passes section's `Parse` phase). The front parses a statement with GenericDialect, and parses again with this one only
//! a text that GenericDialect refuses (`parsed::read`). So a text generic reads never meets a hook, and its tree is
//! generic's: the one-parse path (the kept tree is DataFusion's parse) stays exact. Each hook reads only texts generic
//! refuses, and says which (`Parse::reads`, held to it by a test).
//!
//! Every other method is GenericDialect's (`generic!`), and so is its `TypeId`, which sqlparser's `dialect_of!` asks.
//! The corpus test holds the two to one reading of every text generic reads.
use crate::passes::{Hook, Parse};
use datafusion::sql::sqlparser::ast::{Expr, Statement};
use datafusion::sql::sqlparser::dialect::{Dialect, GenericDialect};
use datafusion::sql::sqlparser::parser::{Parser, ParserError};
use std::cell::RefCell;

/// The fallback dialect: this build's hooks, and the ones that read something in the text parsed with it.
#[derive(Debug)]
pub(crate) struct Fallback {
    hooks: Vec<(&'static str, Parse)>,
    used: RefCell<Vec<&'static str>>,
}

/// `sql` parsed with the fallback dialect and this build's hooks, and the names of the hooks that read part of it.
pub(crate) fn parse(sql: &str) -> (Result<Vec<Statement>, ParserError>, Vec<&'static str>) {
    let d = Fallback { hooks: crate::passes::get().hooks().collect(), used: RefCell::new(vec![]) };
    let mut parsed = Parser::parse_sql(&d, sql);
    let used = d.used.into_inner();
    if let Ok(stmts) = &mut parsed {
        // (an infix hook's form, read as an expression, as generic reads its stored text: `AS MEASURE m`'s alias)
        for (name, h) in &d.hooks {
            if let Hook::Infix { settle, .. } = h.hook {
                if used.contains(name) {
                    stmts.iter_mut().for_each(settle);
                }
            }
        }
    }
    (parsed, used)
}

/// GenericDialect's own answer to each method it answers otherwise than the trait's default (sqlparser 0.62's
/// `impl Dialect for GenericDialect`): those with `&self`, then those with a character. Copied by hand, so a sqlparser
/// with another version fails `the_generic_list_is_sqlparsers_own` until the list is compared with its impl again.
macro_rules! generic {
    ($($f:ident),* ; $($c:ident),*) => {
        $(fn $f(&self) -> bool { GenericDialect {}.$f() })*
        $(fn $c(&self, ch: char) -> bool { GenericDialect {}.$c(ch) })*
    };
}

impl Dialect for Fallback {
    fn dialect(&self) -> std::any::TypeId { std::any::TypeId::of::<GenericDialect>() }

    fn parse_prefix(&self, parser: &mut Parser) -> Option<Result<Expr, ParserError>> {
        for (name, h) in &self.hooks {
            let Hook::Prefix(read) = h.hook else { continue };
            if let Some(read) = read(parser) {
                self.used.borrow_mut().push(name);
                return Some(read);
            }
        }
        None
    }

    fn get_next_precedence(&self, parser: &Parser) -> Option<Result<u8, ParserError>> {
        self.hooks.iter().find_map(|(_, h)| match h.hook {
            Hook::Infix { next, .. } => next(parser).map(Ok),
            _ => None,
        })
    }

    fn parse_infix(&self, parser: &mut Parser, expr: &Expr, _precedence: u8) -> Option<Result<Expr, ParserError>> {
        for (name, h) in &self.hooks {
            let Hook::Infix { read, .. } = h.hook else { continue };
            if let Some(read) = read(parser, expr) {
                self.used.borrow_mut().push(name);
                return Some(read);
            }
        }
        None
    }

    fn parse_statement(&self, parser: &mut Parser) -> Option<Result<Statement, ParserError>> {
        for (name, h) in &self.hooks {
            let Hook::Statement(read) = h.hook else { continue };
            if let Some(read) = read(self, parser) {
                self.used.borrow_mut().push(name);
                return Some(read);
            }
        }
        None
    }

    generic!(
        supports_unicode_string_literal, supports_partition_by_after_order_by, supports_array_join_syntax,
        supports_group_by_expr, supports_group_by_with_modifier, supports_left_associative_joins_without_parens,
        supports_connect_by, supports_match_recognize, supports_pipe_operator, supports_start_transaction_modifier,
        supports_window_function_null_treatment_arg, supports_dictionary_syntax,
        supports_window_clause_named_window_reference, supports_parenthesized_set_variables,
        supports_select_wildcard_except, support_map_literal_syntax, allow_extract_custom, allow_extract_single_quotes,
        supports_extract_comma_syntax, supports_create_view_comment_syntax, supports_parens_around_table_factor,
        supports_values_as_table_factor, supports_create_index_with_clause, supports_explain_with_utility_options,
        supports_limit_comma, supports_update_order_by, supports_from_first_select,
        supports_projection_trailing_commas, supports_asc_desc_in_column_definition, supports_try_convert,
        supports_bitwise_shift_operators, supports_comment_on, supports_load_extension,
        supports_named_fn_args_with_assignment_operator, supports_struct_literal, supports_empty_projections,
        supports_nested_comments, supports_multiline_comment_hints, supports_user_host_grantee,
        supports_string_escape_constant, supports_array_typedef_with_brackets, supports_match_against,
        supports_set_names, supports_comma_separated_set_assignments, supports_filter_during_aggregation,
        supports_select_wildcard_exclude, supports_data_type_signed_suffix, supports_interval_options,
        supports_quote_delimited_string, supports_select_wildcard_replace, supports_select_wildcard_ilike,
        supports_select_wildcard_rename, supports_optimize_table, supports_install, supports_detach, supports_prewhere,
        supports_with_fill, supports_limit_by, supports_interpolate, supports_settings, supports_select_format,
        supports_comment_optimizer_hint, supports_constraint_keyword_without_name, supports_key_column_option,
        supports_comma_separated_trim, supports_cte_without_as, supports_select_item_multi_column_alias,
        supports_xml_expressions
        ;
        is_delimited_identifier_start, is_identifier_start, is_identifier_part
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn generic(sql: &str) -> Result<Vec<Statement>, ParserError> { Parser::parse_sql(&GenericDialect {}, sql) }

    /// `generic!` copies the methods sqlparser 0.62.0's GenericDialect answers itself: a new override in another version
    /// would fall back to the trait's default here without a word. So this build's sqlparser must be the one the list was
    /// compared with; on an upgrade, compare `impl Dialect for GenericDialect` with the list, then change the version here.
    #[test]
    fn the_generic_list_is_sqlparsers_own() {
        let lock = include_str!("../Cargo.lock");
        let version = lock.split("[[package]]").find_map(|p| p.contains("\nname = \"sqlparser\"\n").then(|| p.lines().find_map(|l| l.strip_prefix("version = "))).flatten());
        assert_eq!(version, Some("\"0.62.0\""), "a sqlparser of another version: compare GenericDialect's methods with `generic!`");
    }

    /// Each hook of this build reads only texts GenericDialect refuses: every text it says it reads (`Parse::reads`) is
    /// refused by generic and read by the fallback dialect, with that hook. When a sqlparser upgrade starts reading one,
    /// this fails, and the hook yields to sqlparser's reading or goes.
    #[test]
    fn a_hook_reads_only_what_generic_refuses() {
        let hooks = crate::passes::get().hooks().collect::<Vec<_>>();
        assert!(!hooks.is_empty(), "this build has hooks");
        for (name, h) in hooks {
            assert!(!h.reads.is_empty(), "{name}: it says what it reads");
            for sql in h.reads {
                assert!(generic(sql).is_err(), "{name}: generic reads {sql}, so the hook must not");
                let (read, used) = parse(sql);
                assert!(read.is_ok(), "{name}: the fallback reads {sql}: {read:?}");
                assert!(used.contains(&name), "{name}: it read part of {sql} (used: {used:?})");
            }
        }
    }

    /// The fallback dialect reads every text GenericDialect reads as generic does, no hook asked: over the repository's
    /// SQL (`passes::tests::corpus`), so a statement that mixes a hook's form with `col -> 'k'` reads the rest as generic.
    #[test]
    fn the_fallback_reads_what_generic_reads_as_generic_does() {
        let corpus = crate::passes::tests::corpus();
        let mut read = 0;
        for sql in &corpus {
            let Ok(want) = generic(sql) else { continue };
            let (theirs, used) = parse(sql);
            assert_eq!(theirs.as_ref(), Ok(&want), "the fallback reads otherwise: {sql}");
            assert!(used.is_empty(), "a hook read a text generic reads: {used:?} in {sql}");
            read += 1;
        }
        assert!(read > 4000, "the corpus's texts generic reads: {read}");
    }

    /// A list comprehension reads as the tree generic reads the text its old text pass wrote (`array_transform(…)`), so the
    /// tree passes and the engine take it as before: inside out, with IF as `array_filter`.
    #[test]
    fn a_comprehension_reads_as_its_text_pass_wrote() {
        for (sql, wrote) in [
            ("SELECT [x * 2 FOR x IN [1, 2, 3]] AS v", "SELECT array_transform(([1, 2, 3]), x -> (x * 2)) AS v"),
            ("SELECT [x FOR x IN l IF x > 1] FROM t", "SELECT array_transform(array_filter(l, x -> (x > 1)), x -> (x)) FROM t"),
            ("SELECT [[y * 2 FOR y IN x] FOR x IN [[1, 2], [3]]] AS v", "SELECT array_transform(([[1, 2], [3]]), x -> (array_transform((x), y -> (y * 2)))) AS v"),
            ("SELECT [x FOR x IN [[1], [2]][1]] AS v, x -> 'k' AS j FROM t", "SELECT array_transform(([[1], [2]][1]), x -> (x)) AS v, x -> 'k' AS j FROM t"),
            ("SELECT ['a' FOR x IN [1]] AS v, '[a for b in c]' AS s", "SELECT array_transform(([1]), x -> ('a')) AS v, '[a for b in c]' AS s"),
        ] {
            assert_eq!(crate::parsed::read(sql), generic(wrote), "{sql}");
        }
        // (DuckDB's LAMBDA, read as the old text pass's arrow: an AND after the body is the lambda's at planning, `lambda`)
        for (sql, wrote) in [
            ("SELECT list_transform(l, LAMBDA x: x + 1) FROM t", "SELECT list_transform(l, x -> x + 1) FROM t"),
            ("SELECT list_reduce(l, LAMBDA a, b: a + b) FROM t", "SELECT list_reduce(l, (a, b) -> a + b) FROM t"),
            ("SELECT list_transform(l, LAMBDA x: x * 3), lambda FROM t", "SELECT list_transform(l, x -> x * 3), lambda FROM t"),
            ("SELECT list_filter(l, LAMBDA x: x > 1 AND x < 5) FROM t", "SELECT list_filter(l, x -> x > 1 AND x < 5) FROM t"),
            ("SELECT list_transform(l, LAMBDA x: [y FOR y IN x]) FROM t", "SELECT list_transform(l, x -> array_transform((x), y -> (y))) FROM t"),
        ] {
            assert_eq!(crate::parsed::read(sql), generic(wrote), "{sql}");
        }
        // (outside a function's argument a LAMBDA is grouped, so its tree prints as itself: the text it is kept under,
        // `text::text_kept`, reads back to it. 11-08 audit L11)
        for (sql, prints) in [
            ("SELECT list_transform(l, LAMBDA x: LAMBDA y: x + y) FROM t", "SELECT list_transform(l, x -> (y -> x + y)) FROM t"),
            ("SELECT a + LAMBDA x: x FROM t", "SELECT a + (x -> x) FROM t"),
            ("SELECT LAMBDA x: x AS f", "SELECT (x -> x) AS f"),
        ] {
            let tree = crate::parsed::read(sql).unwrap();
            assert_eq!(crate::text::text(&tree), prints, "{sql}");
            assert_eq!(generic(prints).unwrap(), tree, "{sql}");
        }
        // (a FOR in parentheses is its function's: the bracket's own decides)
        let sub = "SELECT [substring(s FROM 1 FOR 2) FOR s IN l] AS v";
        assert_eq!(crate::parsed::read(sub), generic("SELECT array_transform((l), s -> (substring(s FROM 1 FOR 2))) AS v"));
        // (an array is an array: the hook leaves it to the dialect)
        for sql in ["SELECT [1, 2] AS a, [x, y][1] AS b", "SELECT [substring(s FROM 1 FOR 2), 'x'] AS a, [{'k': 1}, [2]] AS b, [] AS c"] {
            assert_eq!(parse(sql).0, generic(sql), "{sql}");
        }
    }

    /// A bracket is read once (`friendly::comprehends`), so nested ones take time in proportion to the text, with or
    /// without a FOR in each: reading each one's first element twice doubled the time at each, 2^24 steps at 24.
    #[test]
    fn nested_brackets_are_read_once() {
        for depth in [20, 24] {
            let (open, plain) = ("[".repeat(depth), "]".repeat(depth));
            for sql in [
                format!("SELECT {open}1{plain} +"),
                format!("SELECT {open}1{}", ", 1 FOR x IN l]".repeat(depth)),
                format!("SELECT {open}1{}", " j FOR x IN l]".repeat(depth)),
            ] {
                let t = std::time::Instant::now();
                assert!(parse(&sql).0.is_err(), "{sql}");
                assert!(t.elapsed() < std::time::Duration::from_millis(500), "{depth} brackets: {:?} for {sql}", t.elapsed());
            }
        }
        let deep = format!("SELECT {}x * 2{} AS v", "[".repeat(16), " FOR x IN l]".repeat(16)); // (each one's body the next)
        assert!(parse(&deep).0.is_ok(), "{deep}");
    }

    /// When both refuse, the error is the one of the parse that read further: a missing `]` after a comprehension points
    /// past it, where generic stopped at its FOR. On a tie, generic's.
    #[test]
    fn the_error_is_the_one_that_read_further() {
        let e = crate::parsed::read("SELECT [x * 2 FOR x IN [1,2,3] AS v").unwrap_err().to_string();
        assert!(e.contains("Expected: ], found: AS at Line: 1, Column: 32"), "{e}");
        let e = crate::parsed::read("SELECT [x FOR (a, b) IN l] AS v").unwrap_err().to_string();
        assert!(e.contains("a list comprehension takes one name: [… FOR x IN list] at Line: 1, Column: 15"), "{e}");
        assert_eq!(crate::parsed::read("SELEC 1"), generic("SELEC 1"));
        assert_eq!(crate::parsed::read("SELECT 1 +"), generic("SELECT 1 +"));
    }

    /// The forms the front still writes in the text before the parse, where sqlparser has no hook to read them, each with
    /// its pass and the upstream proposal that would end it; and ALTER COLUMN's place, which a statement entry with two
    /// names reads by a walk (`write::placed`). A baseline that may only fall: when sqlparser reads one, its example
    /// parses, this fails, and the pass's part for it is deleted (for ALTER COLUMN's place, the entry is the tree's, with
    /// one name).
    #[test]
    fn the_text_forms_left_are_counted_and_still_refused() {
        const LEFT: &[(&str, &str, &str, &str)] = &[
            ("t AT (VERSION => n)", "past_names.syntax", "SELECT * FROM t AT (VERSION => 3)", "a table-factor hook"),
            ("DuckDB's ASOF JOIN … ON", "friendly.text", "SELECT * FROM a ASOF JOIN b ON a.k = b.k AND a.t >= b.t", "ASOF JOIN taking ON beside MATCH_CONDITION"),
            ("USING SAMPLE", "friendly.text", "SELECT * FROM t USING SAMPLE 10%", "DuckDB's USING SAMPLE clause"),
            ("FOR NO KEY UPDATE, FOR KEY SHARE", "friendly.text", "SELECT * FROM t FOR NO KEY UPDATE", "Postgres's four lock strengths"),
            ("a subquery before MATCH_RECOGNIZE", "pattern.text", "SELECT * FROM (SELECT * FROM t) MATCH_RECOGNIZE (ORDER BY ts PATTERN (a+) DEFINE a AS true)", "MATCH_RECOGNIZE after a derived table"),
            ("FROM t WHERE … (no SELECT)", "front's SELECT * reading", "FROM t WHERE x > 1", "FromFirstNoSelect reading the clauses after FROM"),
            ("ALTER TABLE t ALTER COLUMN c FIRST | AFTER b", "write::placed", "ALTER TABLE t ALTER COLUMN c AFTER b", "MySQL's column place, which it reads for ADD and MODIFY, in ALTER COLUMN"),
        ];
        assert!(LEFT.len() <= 7, "the forms left may only fall: {}", LEFT.len());
        for (form, pass, example, upstream) in LEFT {
            assert!(generic(example).is_err(), "sqlparser reads {form} now ({upstream}): delete {pass}'s part for it");
            assert!(parse(example).0.is_err(), "no hook reads {form}: it is {pass}'s");
        }
    }
}
