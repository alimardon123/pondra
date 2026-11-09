//! `pondra.diff(a, b)` in a FROM (ADR-047, Trellis T1): the rows of b that differ from a's, in the change feed's words
//! (`_change_type`: insert, delete, update_preimage, update_postimage), and on an update's two rows the names of the
//! columns whose values differ (`_changed`, NULL on the others).
//!
//! Either side is a table's or a view's name, as a string (`'prod.sales.orders'`), or a query (`(SELECT …)`).
//! - Two tables match by their rows' `_row_id` and `_version` (ADR-020): a branch's rows keep both, so the rows both hold
//!   alike match, and a row whose version moved is an update. The answer keeps both columns.
//! - Two sides with a key match by it (Trellis T1b): the key `key => 'c1, c2'` names, else the one a side declares (a
//!   table's PRIMARY KEY, or a view's PRIMARY KEY … NOT ENFORCED fact; `pondra.parts` lists both). A row of b's whose key
//!   a lacks is an insert, one of a's whose key b lacks a delete, and a key both hold with another value in any other
//!   column an update. Keys match NULL to NULL, and a side holding one key twice is refused (`21000`) unless the engine
//!   keeps that key. Two tables match by row unless `key =>` is given.
//! - Any other two match whole, as multisets: a row of b's that a lacks, or has fewer copies of, is an insert (once for
//!   each copy more), and one of a's that b lacks a delete. They have no update rows.
//!
//! `data => 'db'` (Trellis T1c) runs both sides' definitions over `db`'s tables, in one statement, so only the code moves
//! rows: a view's stored query, or a query side, with its own database's tables named in `db` and the views it reads
//! inlined, each read the same way (`over`, `redirect.rs`). A table side reads `db`'s table of its name, and two tables are refused
//! (`0A000`: a table has no definition to run).
//!
//! The front reads it on the parsed tree (`rewrite`): each call becomes the query that answers it, a subquery in its place,
//! so a view it names is expanded as any other. Each side's columns come from planning it (`Host::ask`, nothing run). The
//! two must have the same columns, each of one type on both (`42804` names the first that differs); the answer has b's,
//! in b's order.
use crate::front::{Host, Relation};
use crate::redirect::{part, redirect, temporary};
use anyhow::Result;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::sql::sqlparser::ast::{self, Expr, Statement, TableFactor, Value, VisitorMut};
use std::ops::ControlFlow;

/// The front's pass of this module (`passes.rs`): a call to `pondra.diff` or `pondra.diff_steps` means the tree has
/// something to rewrite.
pub(crate) fn pass() -> crate::passes::Pass {
    crate::passes::Pass { name: "diff.diffs", order: 40, words: vec![], marks: vec![&["pondra", ".", "diff"]], run: crate::passes::Run::Wants(mentioned) }
}

fn mentioned(sql: &str) -> bool {
    static CALL: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"(?i)\bpondra\s*\.\s*diff(_steps)?\b").expect("a regex"));
    CALL.is_match(sql)
}

/// The front's tree pass (`front::Trees`): each `pondra.diff` and `pondra.diff_steps` call as the query that answers it,
/// whose views the expander's pass then expands.
pub(crate) fn tree_pass() -> crate::front::TreePass { crate::front::TreePass { name: "diff.calls", order: 10, run: tree } }

fn tree<'a>(front: &'a (dyn crate::front::Front + 'static), stmts: &'a mut [Statement], facts: &'a crate::front::Facts<'_>) -> futures::future::BoxFuture<'a, Result<bool>> {
    Box::pin(async move {
        let done = rewrite(front, stmts).await?;
        if done {
            facts.expand();
        }
        Ok(done)
    })
}

/// Which of this module's functions a call is.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Asked {
    /// `pondra.diff`: the rows that differ.
    Rows,
    /// `pondra.diff_steps`: which step moved them (`diff_steps.rs`).
    Steps,
}

impl Asked {
    fn name(self) -> &'static str {
        match self {
            Asked::Rows => "pondra.diff",
            Asked::Steps => "pondra.diff_steps",
        }
    }
}

/// One side of a diff: a table's or a view's name, or a query.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Side {
    Name(String),
    Query(String),
}

impl Side {
    /// What a FROM reads it as.
    fn from(&self) -> String {
        match self {
            Side::Name(n) => n.clone(),
            Side::Query(q) => format!("({q})"),
        }
    }

    /// How an error names it.
    pub(crate) fn shown(&self) -> String {
        match self {
            Side::Name(n) => n.clone(),
            Side::Query(q) if q.chars().count() > 40 => format!("({}…)", q.chars().take(40).collect::<String>()),
            Side::Query(q) => format!("({q})"),
        }
    }
}

/// A call's arguments: its two sides, the key's columns when `key =>` names them, and the database `data =>` names.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Call {
    pub(crate) a: Side,
    pub(crate) b: Side,
    key: Option<Vec<String>>,
    pub(crate) data: Option<String>,
}

/// How a call's two sides are matched.
#[derive(Clone, Debug, PartialEq)]
enum Match {
    /// Two tables, by their rows' `_row_id` and `_version`.
    Rows,
    /// By these columns' values.
    Key(Vec<String>),
    /// Whole, as multisets.
    Whole,
}

/// Each `pondra.diff(a, b)` and `pondra.diff_steps(a, b)` in `stmts` as the query that answers it, innermost first (a
/// side's query may diff too).
/// Whether there was one.
pub async fn rewrite<H: Host + ?Sized>(host: &H, stmts: &mut [Statement]) -> Result<bool> {
    let mut any = false;
    loop {
        let mut first = First { found: None };
        for s in stmts.iter_mut() {
            if let ControlFlow::Break(e) = crate::walk::visit_mut(s, &mut first) {
                return Err(e);
            }
        }
        let Some((asked, call)) = first.found else { return Ok(any) };
        let text = match asked {
            Asked::Rows => Box::pin(answer_of(host, &call)).await?, // (its sides are asked through the front again)
            Asked::Steps => Box::pin(crate::diff_steps::answer_of(host, &call)).await?,
        };
        let answer = crate::expander::parse_query(&text)?;
        let mut put = Put { answer: Some(answer) };
        for s in stmts.iter_mut() {
            if let ControlFlow::Break(e) = crate::walk::visit_mut(s, &mut put) {
                return Err(e);
            }
        }
        any = true;
    }
}

/// Which of this module's functions a table factor calls, if it calls one.
fn asked(t: &TableFactor) -> Option<Asked> {
    let TableFactor::Table { name, args: Some(_), .. } = t else { return None };
    [Asked::Rows, Asked::Steps].into_iter().find(|a| crate::text::object(name).eq_ignore_ascii_case(a.name()))
}

/// The first call to `pondra.diff` or `pondra.diff_steps` in the walk's order (a call's sides before it), with its sides.
struct First {
    found: Option<(Asked, Call)>,
}

impl VisitorMut for First {
    type Break = anyhow::Error;

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        let Some(asked) = asked(t).filter(|_| self.found.is_none()) else { return ControlFlow::Continue(()) };
        let TableFactor::Table { args: Some(args), .. } = t else { return ControlFlow::Continue(()) };
        match call(asked, &args.args) {
            Ok(s) => {
                self.found = Some((asked, s));
                ControlFlow::Continue(())
            }
            Err(e) => ControlFlow::Break(e),
        }
    }
}

/// Puts the answer in place of the first call `First` found.
struct Put {
    answer: Option<ast::Query>,
}

impl VisitorMut for Put {
    type Break = anyhow::Error;

    fn post_visit_table_factor(&mut self, t: &mut TableFactor) -> ControlFlow<Self::Break> {
        if asked(t).is_none() {
            return ControlFlow::Continue(());
        }
        let Some(answer) = self.answer.take() else { return ControlFlow::Continue(()) };
        let TableFactor::Table { alias, .. } = t else { return ControlFlow::Continue(()) };
        *t = TableFactor::Derived { lateral: false, subquery: Box::new(answer), alias: alias.take(), sample: None };
        ControlFlow::Continue(())
    }
}

/// A call's arguments: two sides, each a string naming a table or a view, or a query; `key => 'c1, c2'` (`pondra.diff`'s);
/// and `data => 'db'`.
fn call(asked: Asked, args: &[ast::FunctionArg]) -> Result<Call> {
    static NAME: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"^[\w"$]+(\.[\w"$]+){0,2}$"#).expect("a regex"));
    static DATABASE: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r#"^([\w$-]+|"[^"]+")$"#).expect("a regex"));
    let form = match asked {
        Asked::Rows => "pondra.diff takes two sides, each a table's or a view's name or a query, a key and a database to read: pondra.diff('prod.sales.orders', 'dev.sales.orders' [, key => 'id'] [, data => 'dev'])",
        Asked::Steps => "pondra.diff_steps takes two sides, each a view's name or a query, and a database to read: pondra.diff_steps('prod.sales.daily', 'dev.sales.daily' [, data => 'dev'])",
    };
    let what = asked.name();
    let a = crate::text::args(what, args)?;
    let (mut key, mut data) = (None, None);
    for (n, e) in &a.named {
        match (n.to_ascii_lowercase().as_str(), e) {
            ("key", _) if asked == Asked::Steps => return Err(crate::codes::coded("42883", format!("{what} has no argument key (its steps are matched by name): {form}"))),
            ("key", Expr::Value(v)) if matches!(&v.value, Value::SingleQuotedString(_)) => {
                let Value::SingleQuotedString(k) = &v.value else { unreachable!() };
                key = Some(names(k).ok_or_else(|| crate::codes::coded("22023", format!("pondra.diff's key => names the key's columns: key => 'id, day', not '{k}'")))?);
            }
            ("key", _) => return Err(crate::codes::coded("22023", format!("pondra.diff's key => names the key's columns as text: key => 'id, day', not {e}"))),
            ("data", Expr::Value(v)) if matches!(&v.value, Value::SingleQuotedString(d) if DATABASE.is_match(d.trim())) => {
                let Value::SingleQuotedString(d) = &v.value else { unreachable!() };
                data = Some(d.trim().to_string());
            }
            ("data", _) => return Err(crate::codes::coded("22023", format!("{what}'s data => names one database whose tables both sides read: data => 'dev', not {e}"))),
            _ => return Err(crate::codes::coded("42883", format!("{what} has no argument {n}: {form}"))), // (as Postgres names a call it has no function for)
        }
    }
    let side = |e: &Expr| -> Result<Side> {
        match e {
            Expr::Value(v) => match &v.value {
                Value::SingleQuotedString(s) if NAME.is_match(s.trim()) => Ok(Side::Name(s.trim().to_string())),
                _ => Err(crate::codes::coded("42883", format!("{form}, not {e}"))),
            },
            Expr::Subquery(q) => Ok(Side::Query(q.to_string())),
            _ => Err(crate::codes::coded("42883", format!("{form}, not {e}"))),
        }
    };
    match a.given[..] {
        [x, y] => Ok(Call { a: side(x)?, b: side(y)?, key, data }),
        _ => Err(crate::codes::coded("42883", form)),
    }
}

/// The column names a list says, `id, "Day"`, each as SQL reads it (bare, or in double quotes); None if one is empty.
fn names(list: &str) -> Option<Vec<String>> {
    let mut out = vec![];
    let (mut name, mut quoted, mut chars) = (String::new(), false, list.chars().peekable());
    while let Some(c) = chars.next() {
        match c {
            '"' if quoted && chars.peek() == Some(&'"') => {
                chars.next();
                name.push('"');
            }
            '"' => quoted = !quoted,
            ',' if !quoted => out.push(std::mem::take(&mut name)),
            c => name.push(c),
        }
    }
    out.push(name);
    let out: Vec<String> = out.iter().map(|n| n.trim().to_string()).collect();
    (!quoted && out.iter().all(|n| !n.is_empty())).then_some(out)
}

/// A side's columns, with their types, as `SELECT *` gives them, and whether it is a table whose rows keep their
/// `_row_id` and `_version` (a name a query can read both of).
async fn columns_of<H: Host + ?Sized>(host: &H, side: &Side) -> Result<(Vec<(String, DataType)>, bool)> {
    let fields = |df: &datafusion::prelude::DataFrame| df.schema().fields().iter().map(|f| (f.name().clone(), f.data_type().clone())).collect::<Vec<_>>();
    if let Side::Name(n) = side {
        if let Ok(df) = host.ask(&format!("SELECT *, _row_id, _version FROM {n} LIMIT 0")).await {
            let mut all = fields(&df);
            all.truncate(all.len().saturating_sub(2));
            return Ok((all, true));
        }
    }
    Ok((fields(&host.ask(&format!("SELECT * FROM {} LIMIT 0", side.from())).await?), false))
}

/// The query that answers `pondra.diff(a, b)`.
async fn answer_of<H: Host + ?Sized>(host: &H, call: &Call) -> Result<String> {
    // (what each side runs: as named, or over `data =>`'s tables; a key is the one each name declares, either way)
    let (named_a, named_b) = (&call.a, &call.b);
    let (ran_a, ran_b) = match &call.data {
        Some(db) => sides_over(host, named_a, named_b, db).await?,
        None => (named_a.clone(), named_b.clone()),
    };
    let (a, b) = (&ran_a, &ran_b);
    let ((of_a, rows_a), (of_b, rows_b)) = (columns_of(host, a).await?, columns_of(host, b).await?);
    let cols = alike(named_a, &of_a, named_b, &of_b)?; // (an error names each side as the call does)
    let how = match &call.key {
        None if rows_a && rows_b => Match::Rows,
        given => {
            let (kept_a, kept_b) = (declared(host, named_a).await?, declared(host, named_b).await?);
            let key = match (given, &kept_a, &kept_b) {
                (Some(k), _, _) => Some(k.clone()),
                (None, Some((x, _)), Some((y, _))) if x != y => {
                    return Err(crate::codes::coded("42P10", format!("pondra.diff matches rows by one key, and {}'s is ({}) and {}'s ({}): key => '{}' picks one", named_a.shown(), x.join(", "), named_b.shown(), y.join(", "), y.join(", "))));
                }
                (None, Some((x, _)), _) | (None, None, Some((x, _))) => Some(x.clone()),
                (None, None, None) => None,
            };
            match key {
                Some(key) => {
                    if let Some(c) = key.iter().find(|c| !cols.contains(c)) {
                        return Err(crate::codes::coded("42703", format!("pondra.diff's key: {c} is not a column of the two sides ({})", cols.join(", "))));
                    }
                    // (a side whose engine keeps this key holds each value once already: the other is counted)
                    for (side, named, kept) in [(a, named_a, &kept_a), (b, named_b, &kept_b)] {
                        if !matches!(kept, Some((k, true)) if *k == key) {
                            once_each(host, side, named, &key).await?;
                        }
                    }
                    Match::Key(key)
                }
                None => Match::Whole,
            }
        }
    };
    Ok(answer(&a.from(), &b.from(), &cols, &how))
}

/// The two sides as they read over `db`'s tables (`data => 'db'`). Two tables are refused: neither has a definition to run.
async fn sides_over<H: Host + ?Sized>(host: &H, a: &Side, b: &Side, db: &str) -> Result<(Side, Side)> {
    let table = |r: &Option<Relation<H>>| matches!(r, Some(Relation::Columns(_)));
    let (ra, rb) = (relation_of(host, a).await, relation_of(host, b).await);
    if table(&ra) && table(&rb) {
        return Err(crate::codes::coded("0A000", format!("pondra.diff's data => runs two sides' definitions over {db}'s tables, and {} and {} are tables, which have none: name a view or a query, or leave data => out", a.shown(), b.shown())));
    }
    Ok((over(host, a, ra, db).await?, over(host, b, rb, db).await?))
}

/// The database another database's object is named in here: its name's first part (`dev` of `dev.sales.v` and of
/// `dev.v`), the name it is attached by, which its own lake's name needn't be.
pub(crate) fn database_of(name: &str) -> String { crate::text::parts(name).into_iter().next().unwrap_or_default() }

pub(crate) async fn relation_of<H: Host + ?Sized>(host: &H, side: &Side) -> Option<Relation<H>> {
    match side {
        Side::Name(n) => host.relation(n).await,
        Side::Query(_) => None,
    }
}

/// One side over `db`'s tables: a view's stored query or a query side, read by `redirect`; a table, `db`'s of its name
/// (a session's temporary table, no database's, as it is).
async fn over<H: Host + ?Sized>(host: &H, side: &Side, relation: Option<Relation<H>>, db: &str) -> Result<Side> {
    Ok(match (side, relation) {
        (Side::Name(n), Some(Relation::Columns(_))) if temporary(n) => side.clone(),
        (Side::Name(n), Some(Relation::Columns(_))) => {
            let (schema, table) = match &crate::text::parts(n)[..] {
                [.., s, t] => (s.clone(), t.clone()),
                [t] => (crate::text::PUBLIC.to_string(), t.clone()),
                [] => return Ok(side.clone()),
            };
            Side::Name(ast::ObjectName::from(vec![part(&crate::text::database(db)), part(&schema), part(&table)]).to_string())
        }
        (Side::Name(_), Some(Relation::View { sql, lake: Some(other) })) => Side::Query(redirect(&*other, sql, db.to_string(), 0).await?),
        (Side::Name(_), Some(Relation::View { sql, lake: None })) => Side::Query(redirect(host, sql, db.to_string(), 0).await?),
        (Side::Query(q), _) => Side::Query(redirect(host, q.clone(), db.to_string(), 0).await?),
        (Side::Name(_), None) => side.clone(), // (not a relation here: the planner says what it is)
    })
}

/// A side's declared key, when it is a table's or a view's name: its PRIMARY KEY's columns (a table's key, or a fact
/// said NOT ENFORCED), and whether the engine keeps it. Read from `pondra.parts`, which lists every database's objects.
async fn declared<H: Host + ?Sized>(host: &H, side: &Side) -> Result<Option<(Vec<String>, bool)>> {
    let Side::Name(n) = side else { return Ok(None) };
    let Some(parts) = name_parts(n) else { return Ok(None) };
    let text = |s: &String| Value::SingleQuotedString(s.clone()).to_string();
    let (lake, schema, object) = match &parts[..] {
        [l, s, o] => (text(l), text(s), text(o)),
        [s, o] => ("current_database()".into(), text(s), text(o)),
        [o] => ("current_database()".into(), "current_schema()".into(), text(o)),
        _ => return Ok(None),
    };
    let sql = format!("SELECT definition FROM pondra.parts WHERE kind = 'key' AND lake = {lake} AND schema = {schema} AND object = {object} AND starts_with(definition, 'PRIMARY KEY (')");
    let Ok(frame) = host.ask(&sql).await else { return Ok(None) }; // (a host that lists no parts declares no key)
    let batches = host.collect(frame).await?;
    let Some(def) = batches.iter().find(|b| b.num_rows() > 0).map(|b| ScalarValue::try_from_array(b.column(0), 0)).transpose()? else { return Ok(None) };
    let def = def.to_string();
    let Some((list, rest)) = def.strip_prefix("PRIMARY KEY (").and_then(|d| d.rsplit_once(')')) else { return Ok(None) };
    Ok(names(list).map(|k| (k, rest.trim().is_empty()))) // (a fact ends NOT ENFORCED)
}

/// A name's parts, each as SQL reads it: `prod."Sales".orders` is prod, Sales, orders.
fn name_parts(name: &str) -> Option<Vec<String>> { names(&name.replace('.', ",")).filter(|p| p.len() <= 3) }

/// Refuses a side that holds one key's values on more than one row (`21000`, Postgres's for more rows than one where
/// one was meant), naming the first and the side as the call names it: a keyed match pairs one row of a side with one of
/// the other.
async fn once_each<H: Host + ?Sized>(host: &H, side: &Side, named: &Side, key: &[String]) -> Result<()> {
    let k = key.iter().map(|c| ast::Ident::with_quote('"', c).to_string()).collect::<Vec<_>>().join(", ");
    let batches = host.collect(host.ask(&format!("SELECT {k} FROM {} x GROUP BY {k} HAVING count(*) > 1 LIMIT 1", side.from())).await?).await?;
    let Some(b) = batches.iter().find(|b| b.num_rows() > 0) else { return Ok(()) };
    let mut shown = vec![];
    for (i, c) in key.iter().enumerate() {
        let v = ScalarValue::try_from_array(b.column(i), 0)?;
        shown.push(match v.data_type() {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View if !v.is_null() => format!("{c} = {}", Value::SingleQuotedString(v.to_string())),
            _ => format!("{c} = {v}"),
        });
    }
    Err(crate::codes::coded("21000", format!("pondra.diff matches rows by their key ({}), and {} holds {} on more than one row: key => names columns that tell its rows apart", key.join(", "), named.shown(), shown.join(", "))))
}

/// b's columns, when a has the same ones of the same types: else `42804`, naming the first that differs, in b's order
/// and then a's. Text of each width is one type, and so are bytes: a view's text and its table's are alike.
fn alike(a: &Side, of_a: &[(String, DataType)], b: &Side, of_b: &[(String, DataType)]) -> Result<Vec<String>> {
    let kind = |t: &DataType| match t {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => DataType::Utf8,
        DataType::Binary | DataType::LargeBinary | DataType::BinaryView => DataType::Binary,
        t => t.clone(),
    };
    let differs = |m: String| Err(crate::codes::coded("42804", format!("pondra.diff compares two sides of the same columns: {m}")));
    for (c, t) in of_b {
        match of_a.iter().find(|(n, _)| n == c) {
            None => return differs(format!("{c} is a column of {} and not of {}", b.shown(), a.shown())),
            Some((_, u)) if kind(u) != kind(t) => return differs(format!("{c} is {u} in {} and {t} in {}", a.shown(), b.shown())),
            Some(_) => {}
        }
    }
    if let Some((c, _)) = of_a.iter().find(|(n, _)| !of_b.iter().any(|(m, _)| m == n)) {
        return differs(format!("{c} is a column of {} and not of {}", a.shown(), b.shown()));
    }
    Ok(of_b.iter().map(|(c, _)| c.clone()).collect())
}

/// The answer over `a` and `b` (what a FROM reads), of the columns `cols`, matched as `how` says.
fn answer(a: &str, b: &str, cols: &[String], how: &Match) -> String {
    let q = |c: &String| ast::Ident::with_quote('"', c).to_string();
    let list = |t: &str| cols.iter().map(|c| format!("{t}{}", q(c))).collect::<Vec<_>>().join(", ");
    let none = "CAST(NULL AS VARCHAR[])";
    // (each named column's name in a one-name list when its two values differ, an empty one when not, all put together)
    let changed = |of: &[&String]| {
        let each: Vec<String> = of.iter().map(|c| format!("CASE WHEN x.{0} IS DISTINCT FROM y.{0} THEN [{1}] ELSE CAST([] AS VARCHAR[]) END", q(c), Value::SingleQuotedString(c.to_string()))).collect();
        match each.len() {
            1 => each[0].clone(),
            _ => format!("array_concat({})", each.join(", ")),
        }
    };
    let (x, y) = (list("x."), list("y."));
    let key = match how {
        Match::Whole => {
            let (l, ins, del) = (list(""), format!("SELECT {y} FROM {b} y EXCEPT ALL SELECT {x} FROM {a} x"), format!("SELECT {x} FROM {a} x EXCEPT ALL SELECT {y} FROM {b} y"));
            return format!("SELECT 'insert' AS _change_type, {l}, {none} AS _changed FROM ({ins}) d UNION ALL SELECT 'delete', {l}, {none} FROM ({del}) d");
        }
        Match::Key(key) => key,
        Match::Rows => {
            let changed = changed(&cols.iter().collect::<Vec<_>>());
            let moved = format!("FROM {a} x JOIN {b} y ON x._row_id = y._row_id AND x._version <> y._version");
            return format!(
                "SELECT 'insert' AS _change_type, {y}, y._row_id, y._version, {none} AS _changed FROM {b} y WHERE NOT EXISTS (SELECT 1 FROM {a} x WHERE x._row_id = y._row_id) \
                 UNION ALL SELECT 'update_preimage', {x}, x._row_id, x._version, {changed} {moved} \
                 UNION ALL SELECT 'update_postimage', {y}, y._row_id, y._version, {changed} {moved} \
                 UNION ALL SELECT 'delete', {x}, x._row_id, x._version, {none} FROM {a} x WHERE NOT EXISTS (SELECT 1 FROM {b} y WHERE y._row_id = x._row_id)"
            );
        }
    };
    // (a key one side lacks: its rows joined to the other's keys, each with a mark, find no mark; keys match NULL to NULL)
    let on = key.iter().map(|k| format!("(x.{0} IS NOT DISTINCT FROM y.{0})", q(k))).collect::<Vec<_>>().join(" AND ");
    let keys = |t: &str| key.iter().map(|k| format!("{t}.{}", q(k))).collect::<Vec<_>>().join(", ");
    let (keys_a, keys_b) = (format!("(SELECT {}, true AS __diff_m FROM {a} x0) x", keys("x0")), format!("(SELECT {}, true AS __diff_m FROM {b} y0) y", keys("y0")));
    let mut arms = vec![format!("SELECT 'insert' AS _change_type, {y}, {none} AS _changed FROM {b} y LEFT JOIN {keys_a} ON {on} WHERE x.__diff_m IS NULL")];
    let rest: Vec<&String> = cols.iter().filter(|c| !key.contains(c)).collect();
    if !rest.is_empty() {
        let (changed, differs) = (changed(&rest), rest.iter().map(|c| format!("(x.{0} IS DISTINCT FROM y.{0})", q(c))).collect::<Vec<_>>().join(" OR "));
        let both = format!("FROM {a} x JOIN {b} y ON {on} WHERE {differs}");
        arms.push(format!("SELECT 'update_preimage', {x}, {changed} {both}"));
        arms.push(format!("SELECT 'update_postimage', {y}, {changed} {both}"));
    }
    arms.push(format!("SELECT 'delete', {x}, {none} FROM {a} x LEFT JOIN {keys_b} ON {on} WHERE y.__diff_m IS NULL"));
    arms.join(" UNION ALL ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;

    /// A session holding `t`, `u` (a branch of t: two rows changed, one gone, one new), `w` (t's rows, by value, with no
    /// row ids: a copy), and `ka` and `kb`, by an id (NULL on one row of each).
    async fn ctx() -> SessionContext {
        let ctx = SessionContext::new_with_state(datafusion::execution::SessionStateBuilder::new().with_default_features().with_analyzer_rules(crate::optimize::analyzer_rules()).build());
        for sql in [
            "CREATE TABLE t AS SELECT * FROM (VALUES (1, 'a', 10, 1, 1), (2, 'b', 20, 2, 1), (3, 'c', 30, 3, 1), (4, 'd', 40, 4, 1)) AS v(id, name, n, _row_id, _version)",
            "CREATE TABLE u AS SELECT * FROM (VALUES (1, 'a', 10, 1, 1), (2, 'B', 20, 2, 2), (3, 'C', 31, 3, 2), (5, 'e', 50, 5, 1)) AS v(id, name, n, _row_id, _version)",
            "CREATE TABLE w AS SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 20), (2, 'b', 20), (3, 'c', 30)) AS v(id, name, n)",
            "CREATE TABLE ka AS SELECT * FROM (VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30), (4, 'd', 40), (NULL, 'z', 0)) AS v(id, name, n)",
            "CREATE TABLE kb AS SELECT * FROM (VALUES (1, 'a', 10), (2, 'B', 20), (3, 'c', 31), (5, 'e', 50), (NULL, 'z', 1)) AS v(id, name, n)",
        ] {
            ctx.sql(sql).await.unwrap().collect().await.unwrap();
        }
        ctx
    }

    async fn rows(ctx: &SessionContext, sql: &str) -> Vec<String> {
        let batches = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}")).collect().await.unwrap_or_else(|e| panic!("{sql}: {e}"));
        let mut out: Vec<String> = batches
            .iter()
            .flat_map(|b| (0..b.num_rows()).map(move |i| b.columns().iter().map(|c| if c.is_null(i) { "NULL".into() } else { array_value_to_string(c, i).unwrap() }).collect::<Vec<_>>().join(" ")))
            .collect();
        out.sort();
        out
    }

    fn cols() -> Vec<String> { ["id", "name", "n"].map(String::from).to_vec() }

    #[tokio::test]
    async fn two_tables_match_by_row_and_name_what_changed() {
        let ctx = ctx().await;
        let sql = format!("SELECT _change_type, id, name, n, _changed FROM ({}) d", answer("t", "u", &cols(), &Match::Rows));
        assert_eq!(
            rows(&ctx, &sql).await,
            [
                "delete 4 d 40 NULL",
                "insert 5 e 50 NULL",
                "update_postimage 2 B 20 [name]",
                "update_postimage 3 C 31 [name, n]",
                "update_preimage 2 b 20 [name]",
                "update_preimage 3 c 30 [name, n]",
            ]
        );
    }

    #[tokio::test]
    async fn other_sides_match_whole_counting_copies() {
        let ctx = ctx().await;
        // (w holds b twice and t once: one insert; t's d is not in w: a delete; no update rows)
        let sql = format!("SELECT _change_type, id, name, n, _changed FROM ({}) d", answer("(SELECT id, name, n FROM t)", "w", &cols(), &Match::Whole));
        assert_eq!(rows(&ctx, &sql).await, ["delete 4 d 40 NULL", "insert 2 b 20 NULL"]);
        // (one column, and a row whose version moved with it unchanged: an update that names no column)
        let one = ["n".to_string()];
        assert_eq!(
            rows(&ctx, &format!("SELECT _change_type, n, _changed FROM ({}) d", answer("t", "u", &one, &Match::Rows))).await,
            ["delete 40 NULL", "insert 50 NULL", "update_postimage 20 []", "update_postimage 31 [n]", "update_preimage 20 []", "update_preimage 30 [n]"]
        );
    }

    #[tokio::test]
    async fn sides_with_a_key_match_by_it() {
        let ctx = ctx().await;
        let keyed = |key: &[&str]| format!("SELECT _change_type, id, name, n, _changed FROM ({}) d", answer("ka", "kb", &cols(), &Match::Key(key.iter().map(|k| k.to_string()).collect())));
        // (a key b lacks, one a lacks, two changed rows each naming what changed, and NULL keys matched to each other)
        assert_eq!(
            rows(&ctx, &keyed(&["id"])).await,
            [
                "delete 4 d 40 NULL",
                "insert 5 e 50 NULL",
                "update_postimage 2 B 20 [name]",
                "update_postimage 3 c 31 [n]",
                "update_postimage NULL z 1 [n]",
                "update_preimage 2 b 20 [name]",
                "update_preimage 3 c 30 [n]",
                "update_preimage NULL z 0 [n]",
            ]
        );
        // (a key of two columns: b's B is another key, so a delete and an insert; a key of every column has no updates)
        assert_eq!(
            rows(&ctx, &keyed(&["id", "name"])).await,
            ["delete 2 b 20 NULL", "delete 4 d 40 NULL", "insert 2 B 20 NULL", "insert 5 e 50 NULL", "update_postimage 3 c 31 [n]", "update_postimage NULL z 1 [n]", "update_preimage 3 c 30 [n]", "update_preimage NULL z 0 [n]"]
        );
        assert_eq!(rows(&ctx, &keyed(&["id", "name", "n"])).await.len(), 8);
    }

    #[test]
    fn sides_are_names_or_queries_and_the_columns_must_agree() {
        let parsed = |sql: &str| -> Result<Call> {
            let q = crate::expander::parse_query(sql).unwrap();
            let ast::SetExpr::Select(s) = *q.body else { panic!() };
            let TableFactor::Table { args: Some(args), .. } = &s.from[0].relation else { panic!() };
            call(asked(&s.from[0].relation).unwrap(), &args.args)
        };
        assert_eq!(parsed("SELECT * FROM pondra.diff('prod.sales.orders', (SELECT 1 AS a))").unwrap(), Call { a: Side::Name("prod.sales.orders".into()), b: Side::Query("SELECT 1 AS a".into()), key: None, data: None });
        assert_eq!(parsed("SELECT * FROM pondra.diff('prod.v', 'dev.v', data => 'dev')").unwrap().data.as_deref(), Some("dev"));
        assert_eq!(parsed(r#"SELECT * FROM pondra.diff('prod.v', 'dev.v', data => '"Dev"')"#).unwrap().data.as_deref(), Some(r#""Dev""#));
        for bad in ["data => ''", "data => 'a.b'", "data => 1", "data => 'x; DROP TABLE t'"] {
            assert_eq!(crate::codes::of(&parsed(&format!("SELECT * FROM pondra.diff('a', 'b', {bad})")).unwrap_err()), "22023", "{bad}");
        }
        assert_eq!(parsed(r#"SELECT * FROM pondra.diff('a', 'b', key => 'id, "Day"')"#).unwrap().key, Some(vec!["id".to_string(), "Day".to_string()]));
        assert!(parsed("SELECT * FROM pondra.diff('a', 'b', keys => 'id')").unwrap_err().to_string().contains("no argument keys"));
        assert_eq!(crate::codes::of(&parsed("SELECT * FROM pondra.diff('a', 'b', key => 'id,')").unwrap_err()), "22023");
        assert_eq!(crate::codes::of(&parsed("SELECT * FROM pondra.diff('a', 'b', key => 1)").unwrap_err()), "22023");
        assert!(parsed("SELECT * FROM pondra.diff('a; DROP TABLE t', 'b')").is_err());
        assert!(parsed("SELECT * FROM pondra.diff('a')").is_err());
        assert_eq!(parsed("SELECT * FROM PONDRA.Diff_Steps('prod.v', 'dev.v', data => 'dev')").unwrap().data.as_deref(), Some("dev"));
        let keyed = parsed("SELECT * FROM pondra.diff_steps('a', 'b', key => 'id')").unwrap_err();
        assert!(crate::codes::of(&keyed) == "42883" && keyed.to_string().contains("pondra.diff_steps has no argument key"), "{keyed}");
        assert!(parsed("SELECT * FROM pondra.diff_steps('a')").unwrap_err().to_string().contains("pondra.diff_steps takes two sides"));
        assert!(mentioned("SELECT * FROM pondra . diff_steps('a', 'b')") && mentioned("SELECT * FROM pondra.diff('a', 'b')") && !mentioned("SELECT pondra.diffs"));
        assert_eq!(names(r#"a, "b,c", "say ""hi""""#), Some(vec!["a".to_string(), "b,c".to_string(), r#"say "hi""#.to_string()]));
        assert_eq!(names(r#""open"#), None);
        assert_eq!(name_parts(r#"prod."Sales".orders"#), Some(vec!["prod".to_string(), "Sales".to_string(), "orders".to_string()]));
        let (a, b) = (Side::Name("prod.v".into()), Side::Name("dev.v".into()));
        let int = |c: &str| (c.to_string(), DataType::Int64);
        let text = |c: &str, t: DataType| (c.to_string(), t);
        assert_eq!(alike(&a, &[int("x"), text("s", DataType::Utf8)], &b, &[text("s", DataType::Utf8View), int("x")]).unwrap(), ["s", "x"]);
        let e = alike(&a, &[int("x")], &b, &[int("x"), int("y")]).unwrap_err();
        assert_eq!(crate::codes::of(&e), "42804");
        assert!(e.to_string().contains("y is a column of dev.v and not of prod.v"), "{e}");
        let e = alike(&a, &[int("x"), int("z")], &b, &[int("x")]).unwrap_err();
        assert!(e.to_string().contains("z is a column of prod.v and not of dev.v"), "{e}");
        let e = alike(&a, &[(String::from("x"), DataType::Int32)], &b, &[int("x")]).unwrap_err();
        assert!(e.to_string().contains("x is Int32 in prod.v and Int64 in dev.v"), "{e}");
    }
}
