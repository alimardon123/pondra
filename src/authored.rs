//! A view's query as its author wrote it: its `--` and `/* … */` comments and its line breaks, kept beside the query it
//! runs (Alimardon 2026-11-02: people document views in their SQL). `SHOW CREATE` gives it, and a column moved
//! (`ALTER VIEW v ALTER COLUMN c FIRST`) is moved in it too, each item with the comments that go with it.
//!
//! It is kept only when it reads as exactly the query the view keeps (`proved`), and shown only while that query is
//! still the view's (`Authored::of`): anything that changes the query another way leaves it behind, and the view is
//! shown as its query reads. So what is shown is never another query than the one that runs; at worst its notes are
//! gone, as they were before.
//!
//! The text is the statement's as it was sent (`door`, around every statement at every door: `audit::statement`; `sent`,
//! around each statement of a script as it reads there, before its macros are read in: `script.rs`), so a statement
//! made up inside Pondra has none.

use datafusion::sql::sqlparser::{
    ast::{self, Spanned},
    dialect::GenericDialect,
    keywords::Keyword,
    parser::Parser,
    tokenizer::{Span, Token, Whitespace},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// A view's query as written, and the query it reads as.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Authored {
    /// From after its statement's `AS` to its end, as written.
    pub text: String,
    /// The view's query it was proved to be (`proved`): it is shown while the view's query is still this one.
    pub of: String,
}

impl Authored {
    /// `a`'s text, while the view's query is still `query`.
    pub fn of<'a>(a: &'a Option<Authored>, query: &str) -> Option<&'a str> { a.as_ref().filter(|a| a.of == query).map(|a| a.text.as_str()) }
}

tokio::task_local! {
    static SENT: Arc<str>;
}

/// `f`, run for the statement `sql` as it was sent: a script's statement as it reads there, before its macros are read
/// in (`script.rs`).
/// (`f` goes to the heap at once: a statement's future is large, and a door's worker thread has a fixed stack)
pub fn sent<T>(sql: &str, f: impl std::future::Future<Output = T>) -> impl std::future::Future<Output = T> { SENT.scope(Arc::from(sql), Box::pin(f)) }

/// `sent`, unless what runs is already a statement as it was sent (a script's, whose statements a door runs one by one
/// once their macros are read in: `audit::statement`).
pub fn door<T>(sql: &str, f: impl std::future::Future<Output = T>) -> impl std::future::Future<Output = T> {
    let (sql, f): (Arc<str>, _) = (Arc::from(sql), Box::pin(f));
    async move {
        match SENT.try_with(|_| ()) {
            Ok(()) => f.await,
            Err(_) => SENT.scope(sql, f).await,
        }
    }
}

/// The query of `CREATE [OR REPLACE] [MATERIALIZED] VIEW name … AS query` as the statement being run wrote it, `query`
/// as it reads. When the text sent makes `name` more than once (a script), the one that reads as `query`, else none.
pub fn candidate(name: &str, materialized: bool, query: &str) -> Option<String> {
    let sql = SENT.try_with(|s| s.clone()).ok()?;
    let mut all = query_texts(&sql, name, materialized);
    match all.iter().position(|t| reprint(t).as_deref() == Some(query)) {
        Some(i) => Some(all.swap_remove(i)),
        None if all.len() == 1 => all.pop(),
        None => None,
    }
}

/// `sql`'s tokens with where each starts and ends (bytes), whitespace and comments among them (`text::tokens`, copied).
fn located(sql: &str) -> Option<Vec<(Token, usize, usize)>> {
    let t = crate::text::tokens(sql)?;
    Some((0..t.toks.len()).map(|i| (t.toks[i].clone(), t.start[i], t.end[i])).collect())
}

fn trivia(t: &Token) -> bool { matches!(t, Token::Whitespace(_)) }
fn blank(t: &Token) -> bool { matches!(t, Token::Whitespace(Whitespace::Space | Whitespace::Tab | Whitespace::Newline)) }
fn comment(t: &Token) -> bool { matches!(t, Token::Whitespace(Whitespace::SingleLineComment { .. } | Whitespace::MultiLineComment(_))) }

/// The answer's notice when a view's text has comments that can't be kept: it isn't the query as the view reads it.
pub const UNKEPT: &str = "its comments aren't kept: its query doesn't read back as written, so SHOW CREATE gives it as it reads";
/// The answer's notice when a moved column's comments couldn't be moved with it.
pub const UNMOVED: &str = "its comments couldn't be moved with its columns: SHOW CREATE gives its query as it reads, without them";

/// Has `text` a `--` or `/* … */` comment?
pub fn commented(text: &str) -> bool { located(text).is_some_and(|t| t.iter().any(|t| comment(&t.0))) }

fn query_texts(sql: &str, name: &str, materialized: bool) -> Vec<String> {
    let Some(toks) = located(sql) else { return vec![] };
    let mut found = vec![];
    let mut depth = 0i32;
    let mut start = 0;
    for i in 0..=toks.len() {
        match toks.get(i).map(|t| &t.0) {
            Some(Token::LParen) => depth += 1,
            Some(Token::RParen) => depth -= 1,
            Some(Token::SemiColon) if depth == 0 => {}
            None => {}
            _ => continue,
        }
        if i == toks.len() || toks[i].0 == Token::SemiColon {
            found.extend(one(sql, &toks[start..i], name, materialized));
            start = i + 1;
        }
    }
    found
}

/// One statement's query text, when it makes the view `name`.
fn one(sql: &str, toks: &[(Token, usize, usize)], name: &str, materialized: bool) -> Option<String> {
    let solid: Vec<&(Token, usize, usize)> = toks.iter().filter(|t| !trivia(&t.0)).collect();
    let kw = |i: usize, k: Keyword| matches!(solid.get(i).map(|t| &t.0), Some(Token::Word(w)) if w.quote_style.is_none() && w.keyword == k);
    let word = |i: usize| matches!(solid.get(i).map(|t| &t.0), Some(Token::Word(_)));
    let mut i = 0;
    if !kw(i, Keyword::CREATE) {
        return None;
    }
    i += 1;
    if kw(i, Keyword::OR) && kw(i + 1, Keyword::REPLACE) {
        i += 2;
    }
    if kw(i, Keyword::MATERIALIZED) != materialized {
        return None;
    }
    i += usize::from(materialized);
    if !kw(i, Keyword::VIEW) {
        return None;
    }
    i += 1;
    if kw(i, Keyword::IF) && kw(i + 1, Keyword::NOT) && kw(i + 2, Keyword::EXISTS) {
        i += 3;
    }
    // (its name: `v`, `s.v`, `"V"`)
    if !word(i) {
        return None;
    }
    let mut j = i + 1;
    while solid.get(j).is_some_and(|t| t.0 == Token::Period) && word(j + 1) {
        j += 2;
    }
    let written = Parser::new(&GenericDialect {}).try_with_sql(&sql[solid[i].1..solid[j - 1].2]).ok()?.parse_object_name(false).ok()?;
    // (as written, or as the statement run names it: `public.v` once its schema is written in, `path.rs`)
    let written = crate::text::object(&written);
    if written != name && !name.ends_with(&format!(".{written}")) {
        return None;
    }
    // (its column list, key facts and options, then its AS)
    let mut depth = 0i32;
    while let Some(t) = solid.get(j) {
        match t.0 {
            Token::LParen => depth += 1,
            Token::RParen => depth -= 1,
            _ if depth == 0 && kw(j, Keyword::AS) => break,
            _ => {}
        }
        j += 1;
    }
    let from = solid.get(j)?.2;
    let to = toks.last()?.2;
    let q = sql[from..to].trim();
    ended(q)
}

/// `q`, with a line break after it when it ends in a `--` comment: what follows it (`;`) stays out of the comment.
fn ended(q: &str) -> Option<String> {
    if q.is_empty() {
        return None;
    }
    let last = located(q)?.into_iter().rev().find(|t| !blank(&t.0));
    Some(match last {
        Some((Token::Whitespace(Whitespace::SingleLineComment { .. }), ..)) => format!("{}\n", q.trim_end()),
        _ => q.to_string(),
    })
}

/// `q` read back as one query, printed as Pondra keeps a view's query.
pub(crate) fn reprint(q: &str) -> Option<String> { Parser::new(&GenericDialect {}).try_with_sql(q).and_then(|mut p| p.parse_query()).map(|q| q.to_string()).ok() }

/// Does `text` read as exactly `query`? As it is (its comments and layout left out), or, for a statement the front reads
/// in Pondra's words (another database's forms: `friendly.rs`), once read so. A text that calls a routine isn't taken
/// in that second way: the query keeps the routine's body as it was, and the text would read as the routine is now.
#[cfg(feature = "lake")]
pub async fn proved(lake: &crate::store::Lake, text: &str, query: &str, materialized: bool, scope: &crate::rights::Scope) -> bool {
    if reprint(text).as_deref() == Some(query) {
        return true;
    }
    use crate::front::Host;
    let host = crate::host::Asking::of(lake, scope);
    let Ok(routines) = host.routines().await else { return false };
    if routines.own.keys().any(|n| crate::text::mentions(text, n)) {
        return false;
    }
    let kind = if materialized { "MATERIALIZED VIEW" } else { "VIEW" };
    let Ok(read) = crate::front::expand(&host, &format!("CREATE {kind} pondra_authored AS {text}"), &std::collections::HashMap::new()).await else { return false };
    matches!(Parser::parse_sql(&GenericDialect {}, &read).as_deref(), Ok([ast::Statement::CreateView(v)]) if v.query.to_string() == query)
}

/// `text` with its outputs in `order` (the place each now comes from), as `rules::alter::moved` puts them: each item
/// of a select list (every branch of a UNION), of a VALUES row, or of a `SELECT * FROM (…) x(names)` list moved with
/// its comments (those on the lines above it, in it, and after it on its line), and an `ORDER BY 2` or `GROUP BY 1`
/// renumbered. None when it can't be followed so; the caller proves what it gives (`proved`).
pub fn moved(text: &str, order: &[usize]) -> Option<String> {
    let q = Parser::new(&GenericDialect {}).try_with_sql(text).and_then(|mut p| p.parse_query()).ok()?;
    let toks = located(text)?;
    let mut m = Mover { at: crate::text::Offsets::of(text), toks, order, edits: vec![] };
    m.query(&q)?;
    let mut edits = m.edits;
    edits.sort_by_key(|e| e.0);
    if edits.windows(2).any(|w| w[0].1 > w[1].0) {
        return None;
    }
    let mut out = text.to_string();
    for (from, to, with) in edits.into_iter().rev() {
        out.replace_range(from..to, &with);
    }
    Some(out)
}

struct Mover<'a> {
    at: crate::text::Offsets<'a>,
    toks: Vec<(Token, usize, usize)>,
    order: &'a [usize],
    edits: Vec<(usize, usize, String)>,
}

impl Mover<'_> {
    fn query(&mut self, q: &ast::Query) -> Option<()> {
        self.body(&q.body)?;
        if let Some(ast::OrderByKind::Expressions(by)) = q.order_by.as_ref().map(|o| &o.kind) {
            by.iter().try_for_each(|b| self.place(&b.expr))?;
        }
        Some(())
    }

    fn body(&mut self, b: &ast::SetExpr) -> Option<()> {
        match b {
            ast::SetExpr::Select(s) => self.select(s),
            ast::SetExpr::Query(q) => self.query(q),
            ast::SetExpr::SetOperation { left, right, .. } => self.body(left).and_then(|_| self.body(right)),
            ast::SetExpr::Values(v) => v.rows.iter().try_for_each(|r| self.list(r.content.iter().map(|e| e.span()).collect())),
            _ => None,
        }
    }

    fn select(&mut self, s: &ast::Select) -> Option<()> {
        if s.projection.iter().any(|i| matches!(i, ast::SelectItem::Wildcard(_) | ast::SelectItem::QualifiedWildcard(..))) {
            let Some(ast::TableFactor::Derived { subquery, alias, .. }) = s.from.first().map(|f| &f.relation) else { return None };
            self.query(subquery)?;
            if let Some(a) = alias.as_ref().filter(|a| !a.columns.is_empty()) {
                self.list(a.columns.iter().map(|c| c.name.span).collect())?;
            }
        } else {
            self.list(s.projection.iter().map(|i| i.span()).collect())?;
        }
        if let ast::GroupByExpr::Expressions(by, _) = &s.group_by {
            by.iter().try_for_each(|e| self.place(e))?;
        }
        Some(())
    }

    /// An output named by its place (`ORDER BY 2`): the place it has now.
    fn place(&mut self, e: &ast::Expr) -> Option<()> {
        if let ast::Expr::Value(v) = e {
            if let ast::Value::Number(n, _) = &v.value {
                if let Some(was) = n.parse::<usize>().ok().and_then(|n| n.checked_sub(1)).filter(|&n| n < self.order.len()) {
                    let (from, to) = self.at.range(v.span)?;
                    let now = self.order.iter().position(|&i| i == was)? + 1;
                    self.edits.push((from, to, now.to_string()));
                }
            }
        }
        Some(())
    }

    /// The items at `spans`, one per output, put in `order` with their comments; the commas and the layout stay where
    /// they are.
    fn list(&mut self, spans: Vec<Span>) -> Option<()> {
        let n = spans.len();
        if n != self.order.len() {
            return None;
        }
        let cores: Vec<(usize, usize)> = spans.into_iter().map(|s| self.at.range(s)).collect::<Option<_>>()?;
        if cores.windows(2).any(|w| w[0].1 > w[1].0) {
            return None;
        }
        let text = self.at.sql;
        let toks = &self.toks;
        let tok_at = |from: usize, to: usize| toks.iter().filter(move |t| t.1 >= from && t.2 <= to);
        // (between two items: comments, blanks and one comma)
        let mut commas = vec![];
        for k in 0..n - 1 {
            let between: Vec<&(Token, usize, usize)> = tok_at(cores[k].1, cores[k + 1].0).collect();
            let comma: Vec<usize> = between.iter().filter(|t| t.0 == Token::Comma).map(|t| t.1).collect();
            if comma.len() != 1 || between.iter().any(|t| t.0 != Token::Comma && !trivia(&t.0)) {
                return None;
            }
            commas.push(comma[0]);
        }
        // (an item's comment after it on its line: a `--` one after its comma, or after it when it is the last, where a
        // `/* … */` on its line is its too; a `/* … */` after a comma is the next item's)
        let trail = |after: usize, last: bool| -> (usize, bool) {
            for t in toks.iter().filter(|t| t.1 >= after) {
                match &t.0 {
                    Token::Whitespace(Whitespace::Space | Whitespace::Tab) => continue,
                    Token::Whitespace(Whitespace::SingleLineComment { .. }) => return (if text[..t.2].ends_with('\n') { t.2 - 1 } else { t.2 }, true),
                    Token::Whitespace(Whitespace::MultiLineComment(_)) if last && !text[t.1..t.2].contains('\n') => return (t.2, false),
                    _ => return (after, false),
                }
            }
            (after, false)
        };
        // (the first item's comments start after what comes before the list: SELECT, DISTINCT, `(`)
        let first = toks.iter().filter(|t| t.2 <= cores[0].0 && !trivia(&t.0)).map(|t| t.2).last()?;
        // (each item: the blanks before it, which stay; its comments and itself, which move; and its comment after it)
        struct Item {
            blank: (usize, usize),
            body: (usize, usize),
            mid: (usize, usize),
            trail: (usize, usize),
            line: bool,
        }
        let mut items = Vec::with_capacity(n);
        let mut from = first;
        for k in 0..n {
            let blank_end = tok_at(from, cores[k].0).find(|t| !blank(&t.0)).map_or(cores[k].0, |t| t.1);
            let (mid, after) = match commas.get(k) {
                Some(&c) => ((cores[k].1, c), c + 1),
                None => ((cores[k].1, cores[k].1), cores[k].1),
            };
            let (trail_end, line) = trail(after, k + 1 == n);
            items.push(Item { blank: (from, blank_end), body: (blank_end, cores[k].1), mid, trail: (after, trail_end), line });
            from = trail_end;
        }
        let end = from;
        let piece = |r: (usize, usize)| &text[r.0..r.1];
        let mut out = String::new();
        for (j, &p) in self.order.iter().enumerate() {
            let blank = piece(items[j].blank);
            if j > 0 && items[self.order[j - 1]].line && !blank.starts_with('\n') && !out.ends_with('\n') {
                out.push('\n'); // (a `--` comment moved before what was on its line)
            }
            out.push_str(blank);
            out.push_str(piece(items[p].body));
            out.push_str(piece(items[p].mid));
            // (its `/* … */` stays before its comma, where it reads as its; a `--` one can only follow it)
            let (before, after) = if items[p].line { ("", piece(items[p].trail)) } else { (piece(items[p].trail), "") };
            out.push_str(before);
            if j + 1 < n {
                out.push(',');
            }
            out.push_str(after);
        }
        if items[self.order[n - 1]].line && !text[end..].starts_with('\n') {
            out.push('\n');
        }
        self.edits.push((first, end, out));
        Some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query_text(sql: &str, name: &str, materialized: bool) -> Option<String> { query_texts(sql, name, materialized).into_iter().next() }

    #[test]
    fn a_script_making_a_view_twice_gives_the_text_that_reads_as_its_query() {
        let sql = "CREATE OR REPLACE VIEW v AS SELECT 1 AS a -- first\n; CREATE OR REPLACE VIEW v AS SELECT 2 AS a -- second\n";
        let pick = |query: &str| SENT.sync_scope(Arc::from(sql), || candidate("v", false, query));
        assert_eq!(pick("SELECT 2 AS a").as_deref(), Some("SELECT 2 AS a -- second\n"));
        assert_eq!(pick("SELECT 1 AS a").as_deref(), Some("SELECT 1 AS a -- first\n"));
        assert_eq!(pick("SELECT 3 AS a"), None); // (neither: not a guess)
        assert_eq!(SENT.sync_scope(Arc::from("CREATE VIEW w AS SELECT 3 AS a /* one */"), || candidate("w", false, "SELECT 0")).as_deref(), Some("SELECT 3 AS a /* one */"));
        assert_eq!(candidate("v", false, "SELECT 1 AS a"), None); // (no statement sent: one made inside Pondra)
        assert!(commented("SELECT 1 -- x") && commented("SELECT /* x */ 1") && !commented("SELECT '-- x', '/* y */'"));
    }

    #[test]
    fn a_views_query_is_found_as_written() {
        let sql = "-- the readings\nCREATE OR REPLACE VIEW s.per_site AS\n-- per site\nSELECT site, /* the mean */ avg(c) AS mean\nFROM r; SELECT 1";
        assert_eq!(query_text(sql, "s.per_site", false).as_deref(), Some("-- per site\nSELECT site, /* the mean */ avg(c) AS mean\nFROM r"));
        assert_eq!(query_text(sql, "per_site", false), None); // (another name)
        assert_eq!(query_text(sql, "s.per_site", true), None); // (not a materialized view)
        let mv = "CREATE MATERIALIZED VIEW IF NOT EXISTS m (CONSTRAINT c EXPECT (n > 0)) WITH (refresh = 'full') AS SELECT a, count(*) AS n FROM t GROUP BY a -- by a";
        assert_eq!(query_text(mv, "m", true).as_deref(), Some("SELECT a, count(*) AS n FROM t GROUP BY a -- by a\n"));
        assert_eq!(query_text("CREATE VIEW \"V\" AS SELECT 1 AS one", "V", false).as_deref(), Some("SELECT 1 AS one"));
        assert_eq!(query_text("CREATE VIEW v AS SELECT 1 AS one", "public.v", false).as_deref(), Some("SELECT 1 AS one")); // (its schema written in)
        assert_eq!(query_text("CREATE VIEW v AS SELECT 1 AS one", "public.w", false), None);
    }

    fn moves(text: &str, order: &[usize]) -> String {
        let out = moved(text, order).unwrap_or_else(|| panic!("{text}: not moved"));
        // (it reads as the query moved: `rules::alter::moved`)
        assert_eq!(reprint(&out), crate::rules::alter::moved(text, order).ok(), "{out}");
        out
    }

    #[test]
    fn a_moved_column_takes_its_comments_along() {
        assert_eq!(moves("SELECT site, avg(c) AS mean, count(*) AS n FROM r", &[0, 2, 1]), "SELECT site, count(*) AS n, avg(c) AS mean FROM r");
        let laid = "SELECT\n  -- the site\n  site,\n  avg(c) AS mean, -- the mean\n  count(*) AS n -- how many\nFROM r\nGROUP BY 1 -- by site\nORDER BY 3 DESC";
        assert_eq!(moves(laid, &[0, 2, 1]), "SELECT\n  -- the site\n  site,\n  count(*) AS n, -- how many\n  avg(c) AS mean -- the mean\nFROM r\nGROUP BY 1 -- by site\nORDER BY 2 DESC");
        assert_eq!(moves(laid, &[2, 0, 1]), "SELECT\n  count(*) AS n, -- how many\n  -- the site\n  site,\n  avg(c) AS mean -- the mean\nFROM r\nGROUP BY 2 -- by site\nORDER BY 1 DESC");
        // (a `--` comment that was the last item's, now before what was on its line, ends its line)
        assert_eq!(moves("SELECT a, b -- the b\nFROM t", &[1, 0]), "SELECT b, -- the b\n a\nFROM t");
        assert_eq!(moves("SELECT a, -- the a\n b FROM t", &[1, 0]), "SELECT b,\n a -- the a\n FROM t");
        assert_eq!(moves("SELECT a /* one */, b FROM t", &[1, 0]), "SELECT b, a /* one */ FROM t");
        assert_eq!(moves("SELECT a, b /* the b */ FROM t", &[1, 0]), "SELECT b /* the b */, a FROM t"); // (before its comma: still the b's)
        assert_eq!(moves("SELECT a, b FROM t UNION ALL\n/* then */ SELECT x, y FROM u", &[1, 0]), "SELECT b, a FROM t UNION ALL\n/* then */ SELECT y, x FROM u");
        assert_eq!(moves("VALUES (1, 'a'), (2, /* two */ 'b')", &[1, 0]), "VALUES ('a', 1), (/* two */ 'b', 2)");
        assert_eq!(moves("SELECT * FROM (SELECT a, b FROM t) AS x (p, q) -- named", &[1, 0]), "SELECT * FROM (SELECT b, a FROM t) AS x (q, p) -- named");
        assert_eq!(moves("SELECT DISTINCT a, sum(b) OVER (PARTITION BY a, c) AS s FROM t", &[1, 0]), "SELECT DISTINCT sum(b) OVER (PARTITION BY a, c) AS s, a FROM t");
        assert_eq!(moves("SELECT 'x, y' AS a, b -- 'z'\nFROM t", &[1, 0]), "SELECT b, -- 'z'\n 'x, y' AS a\nFROM t");
        assert_eq!(moved("SELECT * FROM t", &[1, 0]), None); // (a table's `*`)
    }

    #[test]
    fn a_text_ending_in_a_line_comment_ends_its_line() {
        assert_eq!(ended("SELECT 1 -- one").as_deref(), Some("SELECT 1 -- one\n"));
        assert_eq!(ended("SELECT 1 /* one */").as_deref(), Some("SELECT 1 /* one */"));
        assert_eq!(ended(""), None);
    }
}
