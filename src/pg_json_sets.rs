//! Postgres's functions that expand a JSON document into rows (round 35, brief D1): `json_array_elements` and its
//! `_text`, `json_each` and its `_text`, `json_object_keys`, `json_to_recordset` and `json_to_record`, and their jsonb
//! forms. Each is a list function here (`pondra_<name>`) that reads a document once and answers its rows. Where SQL
//! comes in (`sets`), a call in a SELECT list becomes `unnest` of that list, a call in a FROM a derived table of its
//! rows, and each place Postgres refuses a call is refused by Postgres's words and SQLSTATE before anything changes.
use crate::codes::coded;
use crate::pg_functions::{function, text_arg};
use crate::pg_json::{answer, json_refusal, kind, opened, JSON_SPACE};
use crate::pg_json_make::fail;
use crate::text::names_kept;
use datafusion::arrow::array::{ArrayRef, ListArray, StringArray, StructArray};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, Expr, FunctionArg, LimitClause, Query, Select, SetExpr, Statement, TableAlias, TableAliasColumnDef, TableFactor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use jiter::Peek;
use std::collections::{BTreeMap, HashSet};
use std::ops::ControlFlow;
use std::sync::Arc;

/// What a set-returning function answers, by how a row of it is written.
#[derive(Clone, Copy, PartialEq)]
enum How {
    Elements,
    ElementsText,
    ObjectKeys,
    Each,
    EachText,
    ToRecordset,
    ToRecord,
}

/// One of Postgres's set-returning JSON functions: `name` as SQL calls it, `list` the function that answers its rows
/// here, and whether it is a jsonb one (its values come out as jsonb prints them).
struct Set {
    name: &'static str,
    list: &'static str,
    how: How,
    jsonb: bool,
}

static SETS: [Set; 14] = [
    Set { name: "json_array_elements", list: "pondra_json_array_elements", how: How::Elements, jsonb: false },
    Set { name: "jsonb_array_elements", list: "pondra_jsonb_array_elements", how: How::Elements, jsonb: true },
    Set { name: "json_array_elements_text", list: "pondra_json_array_elements_text", how: How::ElementsText, jsonb: false },
    Set { name: "jsonb_array_elements_text", list: "pondra_jsonb_array_elements_text", how: How::ElementsText, jsonb: true },
    Set { name: "json_object_keys", list: "pondra_json_object_keys", how: How::ObjectKeys, jsonb: false },
    Set { name: "jsonb_object_keys", list: "pondra_jsonb_object_keys", how: How::ObjectKeys, jsonb: true },
    Set { name: "json_each", list: "pondra_json_each", how: How::Each, jsonb: false },
    Set { name: "jsonb_each", list: "pondra_jsonb_each", how: How::Each, jsonb: true },
    Set { name: "json_each_text", list: "pondra_json_each_text", how: How::EachText, jsonb: false },
    Set { name: "jsonb_each_text", list: "pondra_jsonb_each_text", how: How::EachText, jsonb: true },
    Set { name: "json_to_recordset", list: "pondra_json_to_recordset", how: How::ToRecordset, jsonb: false },
    Set { name: "jsonb_to_recordset", list: "pondra_jsonb_to_recordset", how: How::ToRecordset, jsonb: true },
    Set { name: "json_to_record", list: "pondra_json_to_record", how: How::ToRecord, jsonb: false },
    Set { name: "jsonb_to_record", list: "pondra_jsonb_to_record", how: How::ToRecord, jsonb: true },
];

/// Registers the list functions, and makes `json_object_keys` the set-returning function: DataFusion's JSON library
/// answers it as a list, which is `json_keys` here (DuckDB's name for it, with the same arguments). `pg_json::register`
/// calls this after the library's functions are in.
pub fn register(ctx: &SessionContext) {
    for set in SETS.iter() {
        ctx.register_udf(set.udf());
    }
    // The library's scalar json_object_keys is DuckDB's json_keys, and Postgres's json_object_keys is the set-returning
    // one above. DataFusion's deregister also drops a function's aliases, so the scalar is renamed rather than aliased.
    if let Ok(keys) = ctx.state().udf("json_object_keys") {
        ctx.deregister_udf("json_object_keys");
        ctx.register_udf(ScalarUDF::new_from_impl(JsonKeys(keys.as_ref().clone())));
    }
}

/// The library's `json_object_keys`, under the name `json_keys` (DuckDB's name for it, with the same arguments).
#[derive(Debug, PartialEq, Eq, Hash)]
struct JsonKeys(ScalarUDF);

impl ScalarUDFImpl for JsonKeys {
    fn name(&self) -> &str { "json_keys" }
    fn signature(&self) -> &Signature { self.0.signature() }
    fn return_type(&self, arg_types: &[DataType]) -> datafusion::error::Result<DataType> { self.0.return_type(arg_types) }
    fn return_field_from_args(&self, args: ReturnFieldArgs) -> datafusion::error::Result<FieldRef> { self.0.return_field_from_args(args) }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> datafusion::error::Result<ColumnarValue> { self.0.invoke_with_args(args) }
}

impl Set {
    /// Whether a row of it is a `(key, value)` struct, not text.
    fn pairs(&self) -> bool { matches!(self.how, How::Each | How::EachText) }

    /// Whether it is a record function (`json_to_record`, `json_to_recordset`): its columns are named in a FROM.
    fn records(&self) -> bool { matches!(self.how, How::ToRecordset | How::ToRecord) }

    /// Whether it takes a document of this kind (`kind` names it): an object, or an array.
    fn takes(&self, found: &str) -> bool {
        match self.how {
            How::Elements | How::ElementsText | How::ToRecordset => found == "array",
            How::ObjectKeys | How::Each | How::EachText | How::ToRecord => found == "object",
        }
    }

    /// The list function, `(document)` in, its rows out: text per row, or structs for the `_each` ones.
    fn udf(&'static self) -> ScalarUDF {
        let returns: fn(&[DataType]) -> DataType = if self.pairs() { pairs_type } else { texts_type };
        function(self.list, Signature::any(1, Volatility::Immutable), returns, move |a, n| self.rows(a, n).map_err(fail))
    }

    /// The rows of each document. A NULL document has no rows, except `to_record`'s: `jsonb_to_record(NULL)` is one row
    /// of NULLs, as Postgres answers it.
    fn rows(&self, a: &[ArrayRef], n: usize) -> anyhow::Result<ArrayRef> {
        let docs = text_arg(&a[0])?;
        if self.pairs() {
            let rows = (0..n).map(|i| docs[i].as_deref().map(|doc| self.pairs_of(doc)).transpose()).collect::<anyhow::Result<Vec<_>>>()?;
            return pairs_array(&rows);
        }
        let rows = (0..n)
            .map(|i| match docs[i].as_deref() {
                Some(doc) => self.texts_of(doc).map(Some),
                None if self.how == How::ToRecord => Ok(Some(vec![None])),
                None => Ok(None),
            })
            .collect::<anyhow::Result<Vec<Option<Vec<Option<String>>>>>>()?;
        texts_array(&rows)
    }

    /// The text rows of one document (None: a JSON null, for the `_text` ones). A document of another kind is
    /// Postgres's refusal.
    fn texts_of(&self, doc: &str) -> anyhow::Result<Vec<Option<String>>> {
        let found = kind(doc)?;
        if !self.takes(found) {
            return Err(refused(self, found));
        }
        let invalid = || coded("22P02", json_refusal(doc));
        match self.how {
            // (an element is written as it is: a jsonb one is marked and normalized by `pondra_as_jsonb` in the SELECT)
            How::Elements => Ok(elements(doc).ok_or_else(invalid)?.into_iter().map(|e| Some(e.to_string())).collect()),
            How::ElementsText => elements(doc).ok_or_else(invalid)?.into_iter().map(|e| answer(e, true, self.jsonb)).collect::<anyhow::Result<_>>(),
            How::ObjectKeys => Ok(members(doc, self.jsonb).ok_or_else(invalid)?.into_iter().map(|(key, _)| Some(key)).collect()),
            How::ToRecordset => {
                let items = elements(doc).ok_or_else(invalid)?;
                for item in &items {
                    if kind(item)? != "object" {
                        return Err(coded("22023", format!("argument of {} must be an array of objects", self.name)));
                    }
                }
                items.into_iter().map(|e| answer(e, false, self.jsonb)).collect::<anyhow::Result<_>>()
            }
            How::ToRecord => Ok(vec![answer(doc.trim_matches(JSON_SPACE), false, self.jsonb)?]),
            How::Each | How::EachText => unreachable!("pairs_of answers the _each functions"),
        }
    }

    /// The `(key, value)` rows of one object. A value is text for `_each_text`, else its JSON text.
    fn pairs_of(&self, doc: &str) -> anyhow::Result<Vec<(String, Option<String>)>> {
        let found = kind(doc)?;
        if !self.takes(found) {
            return Err(refused(self, found));
        }
        let members = members(doc, self.jsonb).ok_or_else(|| coded("22P02", json_refusal(doc)))?;
        members
            .into_iter()
            .map(|(key, raw)| -> anyhow::Result<(String, Option<String>)> { Ok((key, answer(raw, self.how == How::EachText, self.jsonb)?)) })
            .collect()
    }

    /// The call as a SELECT list has it: its rows unnested, an element marked JSON (`pondra_as_json`, or
    /// `pondra_as_jsonb`, which normalizes a jsonb once). The mark doesn't survive `unnest`, so the wrap is outside it.
    fn in_list(&self, args: Vec<Expr>) -> Expr {
        let rows = crate::text::function("unnest", vec![crate::text::function(self.list, args)]);
        match (self.how, self.jsonb) {
            (How::Elements, false) => crate::text::function("pondra_as_json", vec![rows]),
            (How::Elements, true) => crate::text::function("pondra_as_jsonb", vec![rows]),
            _ => rows,
        }
    }
}

fn texts_type(_: &[DataType]) -> DataType { DataType::new_list(DataType::Utf8, true) }

fn pair_fields() -> Fields { Fields::from(vec![Field::new("key", DataType::Utf8, true), Field::new("value", DataType::Utf8, true)]) }

fn pairs_type(_: &[DataType]) -> DataType { DataType::new_list(DataType::Struct(pair_fields()), true) }

/// The text rows of each document as a list per document (None: a NULL document, which has no list).
fn texts_array(rows: &[Option<Vec<Option<String>>>]) -> anyhow::Result<ArrayRef> {
    let lengths: Vec<Option<usize>> = rows.iter().map(|r| r.as_ref().map(Vec::len)).collect();
    let values = StringArray::from_iter(rows.iter().flatten().flatten().cloned());
    list_of(Field::new_list_field(DataType::Utf8, true), Arc::new(values), &lengths)
}

/// The `(key, value)` rows of each document as a list of structs per document.
fn pairs_array(rows: &[Option<Vec<(String, Option<String>)>>]) -> anyhow::Result<ArrayRef> {
    let lengths: Vec<Option<usize>> = rows.iter().map(|r| r.as_ref().map(Vec::len)).collect();
    // Collected first: arrow's from_iter_values needs an iterator with a known length.
    let pairs: Vec<&(String, Option<String>)> = rows.iter().flatten().flatten().collect();
    let keys = StringArray::from_iter_values(pairs.iter().map(|(key, _)| key.as_str()));
    let values = StringArray::from_iter(pairs.iter().map(|(_, value)| value.clone()));
    let fields = pair_fields();
    let items = StructArray::try_new(fields.clone(), vec![Arc::new(keys), Arc::new(values)], None)?;
    list_of(Field::new_list_field(DataType::Struct(fields), true), Arc::new(items), &lengths)
}

/// A list per row, `lengths[i]` items of `values` for row `i` (None: a NULL row, which takes none).
fn list_of(item: Field, values: ArrayRef, lengths: &[Option<usize>]) -> anyhow::Result<ArrayRef> {
    let mut offsets = vec![0i32];
    for len in lengths {
        offsets.push(offsets.last().copied().unwrap_or(0) + len.unwrap_or(0) as i32);
    }
    let nulls = NullBuffer::from(lengths.iter().map(Option::is_some).collect::<Vec<bool>>());
    Ok(Arc::new(ListArray::try_new(Arc::new(item), OffsetBuffer::new(ScalarBuffer::from(offsets)), values, Some(nulls))?))
}

/// Postgres's words and SQLSTATE for a document of the wrong kind (`found`, as `kind` names it) for `set`.
fn refused(set: &Set, found: &str) -> anyhow::Error {
    let name = set.name;
    let message = match (set.how, set.jsonb) {
        (How::Elements | How::ElementsText, false) if found == "object" => format!("cannot call {name} on a non-array"),
        (How::Elements | How::ElementsText, false) => format!("cannot call {name} on a scalar"),
        (How::Elements | How::ElementsText, true) if found == "object" => "cannot extract elements from an object".to_string(),
        (How::Elements | How::ElementsText, true) => "cannot extract elements from a scalar".to_string(),
        (How::ObjectKeys, _) | (How::ToRecordset, false) => format!("cannot call {name} on {}", article(found)),
        (How::Each | How::EachText, false) if found == "array" => "cannot deconstruct an array as an object".to_string(),
        (How::Each | How::EachText, false) => "cannot deconstruct a scalar".to_string(),
        (How::Each | How::EachText, true) => format!("cannot call {name} on a non-object"),
        (How::ToRecordset, true) => format!("cannot call {name} on a non-array"),
        (How::ToRecord, _) => format!("cannot call populate_composite on {}", article(found)),
    };
    coded("22023", message)
}

fn article(found: &str) -> &'static str {
    match found {
        "array" => "an array",
        "object" => "an object",
        _ => "a scalar",
    }
}

/// The elements of the array `doc`, as written (each trimmed of JSON's spaces).
fn elements(doc: &str) -> Option<Vec<&str>> {
    let mut jiter = opened(doc, Peek::Array)?;
    let mut out = vec![];
    let mut peek = jiter.known_array().ok()?;
    while let Some(p) = peek {
        let start = jiter.current_index();
        jiter.known_skip(p).ok()?;
        out.push(doc.get(start..jiter.current_index())?.trim_matches(JSON_SPACE));
        peek = jiter.array_step().ok()?;
    }
    Some(out)
}

/// The members of the object `doc` with each value as written. JSON keeps every member in its order, duplicates too;
/// jsonb keeps one per key, the last of a duplicate, in its own order (by length, then bytes).
fn members(doc: &str, jsonb: bool) -> Option<Vec<(String, &str)>> {
    let mut jiter = opened(doc, Peek::Object)?;
    let mut out = vec![];
    let mut next = jiter.known_object().ok()?.map(str::to_string);
    while let Some(key) = next {
        let peek = jiter.peek().ok()?;
        let start = jiter.current_index();
        jiter.known_skip(peek).ok()?;
        out.push((key, doc.get(start..jiter.current_index())?.trim_matches(JSON_SPACE)));
        next = jiter.next_key().ok()?.map(str::to_string);
    }
    if !jsonb {
        return Some(out);
    }
    let by_key: BTreeMap<(usize, String), &str> = out.into_iter().map(|(key, raw)| ((key.len(), key), raw)).collect();
    Some(by_key.into_iter().map(|((_, key), raw)| (key, raw)).collect())
}

/// The set call `e` is, if it is one (a window call is not).
fn set_of_expr(e: &Expr) -> Option<&'static Set> {
    match e {
        Expr::Function(f) if f.over.is_none() => set_of(&f.name),
        _ => None,
    }
}

/// The set a function name is, by its one part, in any letter case.
fn set_of(name: &ast::ObjectName) -> Option<&'static Set> {
    let [ast::ObjectNamePart::Identifier(ident)] = name.0.as_slice() else { return None };
    SETS.iter().find(|set| set.name.eq_ignore_ascii_case(&ident.value))
}

/// Whether `name` is one of the set-returning JSON functions: a FROM item of that name is a call, not a relation.
pub fn is_set_name(name: &str) -> bool { SETS.iter().any(|set| set.name.eq_ignore_ascii_case(name)) }

/// The set call `e` is with its arguments, if it is one.
fn call_of(e: &Expr) -> Option<(&'static Set, Vec<Expr>)> {
    let set = set_of_expr(e)?;
    let Expr::Function(f) = e else { return None };
    let ast::FunctionArguments::List(list) = &f.args else { return Some((set, vec![])) };
    let args = list
        .args
        .iter()
        .filter_map(|arg| match arg {
            FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) => Some(e.clone()),
            _ => None,
        })
        .collect();
    Some((set, args))
}

fn is_column(e: &Expr) -> bool { matches!(e, Expr::Identifier(_) | Expr::CompoundIdentifier(_)) }

/// The first call `wanted` names in `node`, as SQL writes it. A query inside `node` is its own (its calls are allowed
/// where it stands), so the walk skips it: `depth` counts the queries it is in.
struct Look<F> {
    wanted: F,
    depth: usize,
    found: Option<String>,
}

impl<F: Fn(&Expr) -> bool> ast::Visitor for Look<F> {
    type Break = ();

    fn pre_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.depth += 1;
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.depth -= 1;
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        if self.depth == 0 && (self.wanted)(e) {
            self.found = Some(e.to_string());
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

fn look(node: &impl ast::Visit, wanted: impl Fn(&Expr) -> bool) -> Option<String> {
    let mut look = Look { wanted, depth: 0, found: None };
    let _ = ast::Visit::visit(node, &mut look);
    look.found
}

fn is_set(e: &Expr) -> bool { set_of_expr(e).is_some() }

/// Refuses a set-returning call anywhere in `node`, where Postgres refuses them: `word` names the clause.
fn refuse(node: &impl ast::Visit, word: &str) -> anyhow::Result<()> {
    match look(node, is_set) {
        Some(_) => Err(coded("0A000", format!("set-returning functions are not allowed in {word}"))),
        None => Ok(()),
    }
}

/// Refuses a call in a clause Postgres takes calls in and this engine doesn't yet, naming the call and the clause.
fn later(node: &impl ast::Visit, word: &str) -> anyhow::Result<()> {
    match look(node, is_set) {
        Some(call) => Err(coded("0A000", format!("{call} in {word} isn't taken yet: call it in a subquery's SELECT list, then {word} its column"))),
        None => Ok(()),
    }
}

const IN_CASE: &str = "set-returning functions are not allowed in CASE";
const IN_COALESCE: &str = "set-returning functions are not allowed in COALESCE";
const IN_AGGREGATE: &str = "aggregate function calls cannot contain set-returning function calls";
const IN_WINDOW: &str = "window function calls cannot contain set-returning function calls";

/// The message for a call inside this expression's own arguments, if it has such arguments (a CASE, COALESCE, an
/// aggregate, a window function): Postgres refuses a set-returning call in them.
fn within(e: &Expr) -> Option<&'static str> {
    match e {
        Expr::Case { .. } => Some(IN_CASE),
        Expr::Function(f) if f.over.is_some() => Some(IN_WINDOW),
        Expr::Function(f) if crate::measures::aggregate(&f.name) => Some(IN_AGGREGATE),
        Expr::Function(f) if f.name.to_string().eq_ignore_ascii_case("coalesce") => Some(IN_COALESCE),
        _ => None,
    }
}

/// The refusals, before anything is rewritten. `found` says whether the statement has a set-returning call at all.
#[derive(Default)]
struct Scan {
    /// What each open expression says of the calls inside it, and each query (None: a call is allowed there again).
    within: Vec<Option<&'static str>>,
    /// The address of an INSERT's source query: its VALUES take calls in Postgres, and not yet here.
    insert_source: usize,
    found: bool,
}

/// Turns a refusal into the walk's stop.
fn flow(result: anyhow::Result<()>) -> ControlFlow<anyhow::Error> {
    match result {
        Ok(()) => ControlFlow::Continue(()),
        Err(e) => ControlFlow::Break(e),
    }
}

impl ast::Visitor for Scan {
    type Break = anyhow::Error;

    fn pre_visit_statement(&mut self, s: &Statement) -> ControlFlow<anyhow::Error> {
        if let Statement::Insert(insert) = s {
            self.insert_source = insert.source.as_deref().map_or(0, |q| std::ptr::from_ref(q) as usize);
        }
        flow(self.statement(s))
    }

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<anyhow::Error> {
        self.within.push(None);
        flow(self.query(q))
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<anyhow::Error> {
        self.within.pop();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<anyhow::Error> { flow(self.select(s)) }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<anyhow::Error> { flow(self.expr(e)) }

    fn post_visit_expr(&mut self, e: &Expr) -> ControlFlow<anyhow::Error> {
        if within(e).is_some() {
            self.within.pop();
        }
        ControlFlow::Continue(())
    }
}

impl Scan {
    fn statement(&mut self, s: &Statement) -> anyhow::Result<()> {
        match s {
            Statement::Insert(insert) => refuse(&insert.returning, "RETURNING"),
            Statement::Update(update) => {
                refuse(&update.assignments, "UPDATE")?;
                refuse(&update.selection, "WHERE")?;
                refuse(&update.returning, "RETURNING")
            }
            Statement::Delete(delete) => {
                refuse(&delete.selection, "WHERE")?;
                refuse(&delete.returning, "RETURNING")
            }
            _ => Ok(()),
        }
    }

    fn query(&mut self, q: &Query) -> anyhow::Result<()> {
        if let Some(order) = &q.order_by {
            later(order, "ORDER BY")?;
        }
        let (limit, offset) = match &q.limit_clause {
            Some(LimitClause::LimitOffset { limit, offset, .. }) => (limit.as_ref(), offset.as_ref().map(|o| &o.value)),
            Some(LimitClause::OffsetCommaLimit { offset, limit }) => (Some(limit), Some(offset)),
            None => (None, None),
        };
        if let Some(e) = limit {
            refuse(e, "LIMIT")?;
        }
        if let Some(e) = offset {
            refuse(e, "OFFSET")?;
        }
        if let SetExpr::Values(values) = q.body.as_ref() {
            match std::ptr::from_ref(q) as usize == self.insert_source {
                true => later(values, "VALUES")?,
                false => refuse(values, "VALUES")?,
            }
        }
        Ok(())
    }

    fn select(&mut self, s: &Select) -> anyhow::Result<()> {
        if let Some(e) = &s.selection {
            refuse(e, "WHERE")?;
        }
        later(&s.group_by, "GROUP BY")?;
        if let Some(e) = &s.having {
            refuse(e, "HAVING")?;
        }
        if let Some(e) = &s.qualify {
            refuse(e, "QUALIFY")?;
        }
        for twj in &s.from {
            for join in &twj.joins {
                refuse(&join.join_operator, "JOIN conditions")?;
            }
        }
        self.from(s)
    }

    /// The FROM calls: a record's columns must be named (42601). A call that reads a column of the FROM is written as a
    /// LATERAL subquery by `Rewrite::table`, as Postgres takes it.
    fn from(&mut self, s: &Select) -> anyhow::Result<()> {
        for twj in &s.from {
            for factor in std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation)) {
                self.factor(factor)?;
            }
        }
        Ok(())
    }

    fn factor(&mut self, factor: &TableFactor) -> anyhow::Result<()> {
        let (name, alias, with_ordinality) = match factor {
            TableFactor::Table { name, args: Some(_), alias, with_ordinality, .. } | TableFactor::Function { name, alias, with_ordinality, .. } => (name, alias, *with_ordinality),
            _ => return Ok(()),
        };
        let Some(set) = set_of(name) else { return Ok(()) };
        self.found = true;
        if set.records() {
            record_columns(alias.as_ref())?;
            // Postgres 16 takes WITH ORDINALITY over a record's columns only in ROWS FROM(), which isn't here.
            if with_ordinality && alias.as_ref().is_some_and(|a| !a.columns.is_empty()) {
                return Err(coded("42601", "WITH ORDINALITY cannot be used with a column definition list"));
            }
        }
        Ok(())
    }

    /// Checks one expression: a call Postgres refuses where it stands, then the calls inside it are placed.
    fn expr(&mut self, e: &Expr) -> anyhow::Result<()> {
        if let Expr::Function(f) = e {
            if let Some(over) = &f.over {
                later(over, "window definitions")?;
            }
        }
        if let Some(set) = set_of_expr(e) {
            self.found = true;
            if let Some(Some(message)) = self.within.last().copied() {
                return Err(coded("0A000", message));
            }
            if set.records() {
                return Err(coded("0A000", format!("could not determine row type for result of {}", set.name)));
            }
        }
        if let Some(message) = within(e) {
            self.within.push(Some(message));
        }
        Ok(())
    }
}

/// The rewrite, after `Scan` has passed the statement: a call in a SELECT list becomes what `Set::in_list` says, and a
/// call in a FROM the derived table `derived` makes.
#[derive(Default)]
struct Rewrite {
    failed: Option<anyhow::Error>,
}

impl ast::VisitorMut for Rewrite {
    type Break = ();

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Some((set, args)) = call_of(e) {
            *e = set.in_list(args);
        }
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, s: &mut Select) -> ControlFlow<()> {
        for twj in s.from.iter_mut() {
            self.table(&mut twj.relation);
            for join in twj.joins.iter_mut() {
                self.table(&mut join.relation);
            }
        }
        ControlFlow::Continue(())
    }
}

impl Rewrite {
    fn table(&mut self, factor: &mut TableFactor) {
        let made = match &*factor {
            TableFactor::Table { name, args: Some(args), alias, with_ordinality, .. } => set_of(name).map(|set| derived(set, &args.args, alias.as_ref(), reads(&args.args), *with_ordinality)),
            TableFactor::Function { name, args, alias, with_ordinality, .. } => set_of(name).map(|set| derived(set, args, alias.as_ref(), reads(args), *with_ordinality)),
            _ => None,
        };
        match made {
            Some(Ok(table)) => *factor = table,
            Some(Err(e)) => {
                if self.failed.is_none() {
                    self.failed = Some(e);
                }
            }
            None => {}
        }
    }
}

/// Whether a call's arguments read a column of the FROM: such a call is LATERAL, as Postgres takes it.
fn reads(args: &Vec<FunctionArg>) -> bool { look(args, is_column).is_some() }

/// A FROM call as the derived table of its rows, with the names Postgres gives them: `value` for the elements, the
/// alias's name for the keys (or the function's), `key` and `value` for each member, and a record's columns named by
/// the alias's column definitions. The derived table takes the alias, and its column list when a non-record has one.
/// `reads`: the arguments read a column, so the table is LATERAL (the rows are made from each row of the FROM before it).
/// `ordinality`: WITH ORDINALITY, so the table's last column is each row's position in the list (named `ordinality`; the
/// alias's column list may rename it). A record's columns take none (`Scan::factor` refuses that).
fn derived(set: &Set, args: &[FunctionArg], alias: Option<&TableAlias>, reads: bool, ordinality: bool) -> anyhow::Result<TableFactor> {
    let list = format!("{}({})", set.list, args.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "));
    let name = alias.map_or_else(|| ast::Ident::new(set.name), |a| a.name.clone());
    // Under WITH ORDINALITY the list is computed once, in a FROM of its own, and each row's position in it is unnested beside
    // its elements (`random()` in the list is read once). Without it the bodies are as they were.
    let (elems, from, pos) = match ordinality {
        true => ("l", format!(" FROM (SELECT {list} AS l) AS __l"), crate::lateral::positions("l")),
        false => (list.as_str(), String::new(), String::new()),
    };
    let here = if ordinality { format!(", {pos} AS ordinality") } else { String::new() };
    let nested = if ordinality { format!(", {pos} AS o") } else { String::new() };
    let outer = if ordinality { ", o AS ordinality" } else { "" };
    let body = match set.how {
        How::Elements => format!("SELECT {} AS value{here}{from}", as_json(set, &format!("unnest({elems})"))),
        How::ElementsText => format!("SELECT unnest({elems}) AS value{here}{from}"),
        How::ObjectKeys => format!("SELECT unnest({elems}) AS {name}{here}{from}"),
        How::Each | How::EachText => {
            let value = match set.how {
                How::Each => as_json(set, "get_field(s, 'value')"),
                _ => "get_field(s, 'value')".to_string(),
            };
            format!("SELECT get_field(s, 'key') AS key, {value} AS value{outer} FROM (SELECT unnest({elems}) AS s{nested}{from}) AS u")
        }
        How::ToRecordset | How::ToRecord => format!("SELECT {} FROM (SELECT unnest({list}) AS e) AS s", record_columns(alias)?),
    };
    let columns = match set.records() {
        true => vec![],
        false => alias.map_or_else(Vec::new, |a| a.columns.iter().map(|c| TableAliasColumnDef { name: c.name.clone(), data_type: None }).collect()),
    };
    Ok(TableFactor::Derived {
        lateral: reads,
        subquery: parse(&body)?,
        alias: Some(TableAlias { explicit: true, name, columns, at: None }),
        sample: None,
    })
}

/// `expr` marked as the set's elements are: `pondra_as_jsonb` for jsonb (which normalizes it once), else `pondra_as_json`.
fn as_json(set: &Set, expr: &str) -> String {
    format!("{}({expr})", if set.jsonb { "pondra_as_jsonb" } else { "pondra_as_json" })
}

/// A record's columns as SQL writes them over the rows `e`: a json or jsonb column is its member as JSON (marked), any
/// other its text cast to the column's type. Needs a column definition for every column (42601 otherwise).
fn record_columns(alias: Option<&TableAlias>) -> anyhow::Result<String> {
    let columns = alias.map_or(&[][..], |a| a.columns.as_slice());
    let listed = columns.iter().map(|c| c.data_type.as_ref().map(|t| column(&c.name, t))).collect::<Option<Vec<String>>>();
    match listed {
        Some(listed) if !listed.is_empty() => Ok(listed.join(", ")),
        _ => Err(coded("42601", "a column definition list is required for functions returning \"record\"")),
    }
}

fn column(name: &ast::Ident, t: &ast::DataType) -> String {
    let key = fold(name);
    let lit = key.replace('\'', "''");
    let value = match t {
        ast::DataType::JSON => format!("pondra_as_json(e -> '{lit}')"),
        ast::DataType::JSONB => format!("pondra_as_jsonb(e -> '{lit}')"),
        _ => format!("CAST(e ->> '{lit}' AS {t})"),
    };
    format!("{value} AS \"{}\"", key.replace('"', "\"\""))
}

/// A column's name as Postgres folds it: an unquoted name in lower case.
fn fold(name: &ast::Ident) -> String {
    match name.quote_style {
        Some(_) => name.value.clone(),
        None => name.value.to_lowercase(),
    }
}

fn parse(sql: &str) -> anyhow::Result<Box<Query>> {
    let mut parser = Parser::new(&GenericDialect {}).try_with_sql(sql)?;
    Ok(parser.parse_query()?)
}

/// The name a FROM item's rows are known by, when its rows are values: the JSON calls that answer one element each
/// (`Elements`, `ElementsText`). The name is the alias, folded as Postgres folds names, or the function's own name. Its
/// rows are a column called `value`, so a bare use of the name in its SELECT is `name.value`. An alias with a column list
/// renames that column, so it has no such name (a bare use stays as it is).
fn value_alias(factor: &TableFactor) -> Option<String> {
    let (name, alias, ordinality) = match factor {
        TableFactor::Table { name, args: Some(_), alias, with_ordinality, .. } | TableFactor::Function { name, alias, with_ordinality, .. } => (name, alias, *with_ordinality),
        _ => return None,
    };
    // Under WITH ORDINALITY the bare name is the whole row (value and ordinality), as Postgres has it: not a value.
    if ordinality {
        return None;
    }
    let set = set_of(name).filter(|set| matches!(set.how, How::Elements | How::ElementsText))?;
    match alias {
        None => Some(set.name.to_string()),
        Some(alias) if alias.columns.is_empty() => Some(fold(&alias.name)),
        Some(_) => None,
    }
}

/// The value names of a SELECT's FROM.
fn value_aliases(s: &Select) -> Vec<String> { from_items(s).filter_map(value_alias).collect() }

/// The FROM items of a SELECT: its first relation and the ones joined to it.
fn from_items(s: &Select) -> impl Iterator<Item = &TableFactor> {
    s.from.iter().flat_map(|twj| std::iter::once(&twj.relation).chain(twj.joins.iter().map(|join| &join.relation)))
}

/// Whether a FROM item's columns are unknown here: a derived table, or anything but a table or view and a JSON call. Such
/// an item may have a column of any name.
fn unknown_columns(factor: &TableFactor) -> bool {
    match factor {
        TableFactor::Table { args: None, .. } => false,
        TableFactor::Table { name, .. } | TableFactor::Function { name, .. } => !is_set_name(&name.to_string()),
        _ => true,
    }
}

/// How many of a SELECT's FROM items have unknown columns (`unknown_columns`).
fn unknown_in(s: &Select) -> usize { from_items(s).filter(|factor| unknown_columns(factor)).count() }

/// A query or a SELECT in scope, as a walk stands in it: its value names, whether an item with unknown columns is in scope
/// from an enclosing level (`outer`), and how many of its own FROM items have unknown columns (`unknown`).
#[derive(Default)]
struct Level {
    names: Vec<String>,
    outer: bool,
    unknown: usize,
}

impl Level {
    /// A bare name here may be a column of an item whose columns aren't known, so it is not a value.
    fn blocked(&self) -> bool { self.outer || self.unknown > 0 }
}

/// The value names in scope where a walk stands: those of the innermost query or SELECT, never an outer one's. A
/// subquery's bare use of an outer name is an outer reference, which `lateral.rs` refuses, so it is left as it is.
/// Both walks of a bare name (`Bare` writes it, `Found` says whether there is one) enter and leave their scopes here,
/// so what a scope holds is written once. Postgres takes a column of any level over a FROM alias's whole row, so a level
/// with an item of unknown columns, here or in an enclosing level, has no value names.
#[derive(Default)]
struct Scopes {
    levels: Vec<Level>,
    /// Set by a derived table's FROM item, for the query that comes next (its own query, `enter_query` clears it).
    pending: bool,
}

impl Scopes {
    /// Notes a FROM item: a derived table's query is the next one entered.
    fn enter_factor(&mut self, factor: &TableFactor) { self.pending = matches!(factor, TableFactor::Derived { .. }); }

    /// Enters a query: its body's value names and unknown items, when the body is one SELECT (its ORDER BY sees that
    /// FROM). A derived table's query is blocked when a sibling of its FROM item may be in scope (every sibling, LATERAL
    /// or not: more is blocked than needed, which is safe). Any other query is blocked where its enclosing level is.
    fn enter_query(&mut self, q: &Query) {
        let derived = std::mem::take(&mut self.pending);
        let outer = match (self.levels.last(), derived) {
            (None, _) => false,
            (Some(parent), true) => parent.outer || parent.unknown > 1,
            (Some(parent), false) => parent.blocked(),
        };
        let (names, unknown) = match &*q.body {
            SetExpr::Select(s) => (value_aliases(s), unknown_in(s)),
            _ => (vec![], 0),
        };
        self.levels.push(Level { names, outer, unknown });
    }

    /// Enters a SELECT: the value names and unknown items of its FROM. It is in its query's scope.
    fn enter_select(&mut self, s: &Select) {
        let outer = self.levels.last().is_some_and(|query| query.outer);
        self.levels.push(Level { names: value_aliases(s), outer, unknown: unknown_in(s) });
    }

    /// Leaves the query or SELECT entered last.
    fn leave(&mut self) { self.levels.pop(); }

    /// The identifier `e` is, when it is a value name in scope (before the FROM's columns are checked: `Bare` does that).
    fn bare(&self, e: &Expr) -> Option<ast::Ident> {
        let Expr::Identifier(ident) = e else { return None };
        let level = self.levels.last().filter(|level| !level.blocked())?;
        level.names.contains(&fold(ident)).then(|| ident.clone())
    }
}

/// Writes a bare value name as `name.value`, where SQL's scoping gives it that meaning (`sets`). A bare name is a column of
/// the FROM first, so a name that some relation has as a column is left alone: `columns` holds the lower-case names.
struct Bare<'a> {
    columns: &'a HashSet<String>,
    scopes: Scopes,
}

impl Bare<'_> {
    /// The identifier `e` is, when it is a bare value name this rewrites.
    fn bare(&self, e: &Expr) -> Option<ast::Ident> {
        self.scopes.bare(e).filter(|ident| !self.columns.contains(&fold(ident).to_lowercase()))
    }
}

impl ast::VisitorMut for Bare<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, q: &mut Query) -> ControlFlow<()> {
        self.scopes.enter_query(q);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &mut Query) -> ControlFlow<()> {
        self.scopes.leave();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &mut Select) -> ControlFlow<()> {
        self.scopes.enter_select(s);
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _: &mut Select) -> ControlFlow<()> {
        self.scopes.leave();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &mut TableFactor) -> ControlFlow<()> {
        self.scopes.enter_factor(factor);
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Some(ident) = self.bare(e) {
            *e = Expr::CompoundIdentifier(vec![ident, ast::Ident::new("value")]);
        }
        ControlFlow::Continue(())
    }
}

/// Whether a SELECT of a statement has a value name written bare (`bare_aliases`).
#[derive(Default)]
struct Found {
    scopes: Scopes,
}

impl ast::Visitor for Found {
    type Break = ();

    fn pre_visit_query(&mut self, q: &Query) -> ControlFlow<()> {
        self.scopes.enter_query(q);
        ControlFlow::Continue(())
    }

    fn post_visit_query(&mut self, _: &Query) -> ControlFlow<()> {
        self.scopes.leave();
        ControlFlow::Continue(())
    }

    fn pre_visit_select(&mut self, s: &Select) -> ControlFlow<()> {
        self.scopes.enter_select(s);
        ControlFlow::Continue(())
    }

    fn post_visit_select(&mut self, _: &Select) -> ControlFlow<()> {
        self.scopes.leave();
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
        self.scopes.enter_factor(factor);
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, e: &Expr) -> ControlFlow<()> {
        match self.scopes.bare(e) {
            Some(_) => ControlFlow::Break(()),
            None => ControlFlow::Continue(()),
        }
    }
}

/// Whether some SELECT of `stmt` has a value name of a JSON call written bare, as `e` and not `e.value`. `types::rewrite`
/// asks the catalog for the columns only for such a statement.
pub fn bare_aliases(stmt: &Statement) -> bool { ast::Visit::visit(stmt, &mut Found::default()).is_break() }

/// Rewrites a statement's set-returning calls where SQL comes in (`types::rewrite`), and whether it changed any. What
/// Postgres refuses, and what this engine doesn't take yet, is refused before anything changes. The names of changed
/// SELECT items are kept as written (`names_kept`). `columns` are the lower-case names of the columns the statement's
/// relations have (`pg_types::columns_named`): a bare value name (`bare_aliases`) is written `name.value` unless one of
/// them is that name. When they are unknown (None), no bare name is written.
pub fn sets(stmt: &mut Statement, columns: Option<&HashSet<String>>) -> anyhow::Result<bool> {
    let mut scan = Scan::default();
    if let ControlFlow::Break(e) = ast::Visit::visit(&*stmt, &mut scan) {
        return Err(e);
    }
    if !scan.found {
        return Ok(false);
    }
    let mut rewrite = Rewrite::default();
    names_kept(stmt, |s| {
        if let Some(columns) = columns {
            let _ = ast::VisitMut::visit(s, &mut Bare { columns, scopes: Scopes::default() });
        }
        let _ = ast::VisitMut::visit(s, &mut rewrite);
        true
    });
    match rewrite.failed {
        Some(e) => Err(e),
        None => Ok(true),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The statement after `sets` with the columns it is given, as text, or the refusal.
    fn rewritten_with(sql: &str, columns: Option<&[&str]>) -> anyhow::Result<String> {
        let mut stmt = Parser::parse_sql(&GenericDialect {}, sql)?.remove(0);
        let columns: Option<HashSet<String>> = columns.map(|names| names.iter().map(|name| name.to_string()).collect());
        sets(&mut stmt, columns.as_ref())?;
        Ok(stmt.to_string())
    }

    /// The statement after `sets`, as text, or the refusal (no columns known).
    fn rewritten(sql: &str) -> anyhow::Result<String> { rewritten_with(sql, None) }

    fn refusal(sql: &str) -> String { rewritten(sql).expect_err("refused").to_string() }

    /// The library's scalar is `json_keys` now, and `json_object_keys` names only the set-returning function here.
    #[test]
    fn json_keys_is_the_scalar_under_its_own_name() {
        let mut ctx = SessionContext::new();
        datafusion_functions_json::register_all(&mut ctx).expect("JSON functions register");
        register(&ctx);
        assert!(ctx.state().udf("json_keys").is_ok());
        assert!(ctx.state().udf("json_object_keys").is_err());
    }

    #[test]
    fn select_items_unnest_their_rows() {
        let json = rewritten("SELECT json_array_elements(doc) FROM t").unwrap();
        assert!(json.contains("pondra_as_json(unnest(pondra_json_array_elements(doc)))"), "{json}");
        let text = rewritten("SELECT upper(jsonb_array_elements_text(doc)) FROM t").unwrap();
        assert!(text.contains("upper(unnest(pondra_jsonb_array_elements_text(doc)))"), "{text}");
        let keys = rewritten("SELECT jsonb_object_keys(doc -> 'a') FROM t").unwrap();
        assert!(keys.contains("unnest(pondra_jsonb_object_keys(doc -> 'a'))"), "{keys}");
    }

    #[test]
    fn from_calls_become_derived_tables() {
        let elements = rewritten("SELECT * FROM jsonb_array_elements('[1,2]') AS e").unwrap();
        assert!(elements.contains("(SELECT pondra_as_jsonb(unnest(pondra_jsonb_array_elements('[1,2]'))) AS value) AS e"), "{elements}");
        let keys = rewritten("SELECT * FROM jsonb_object_keys('{\"a\":1}') AS k").unwrap();
        assert!(keys.contains("(SELECT unnest(pondra_jsonb_object_keys('{\"a\":1}')) AS k) AS k"), "{keys}");
        let each = rewritten("SELECT * FROM jsonb_each('{\"a\":1}') AS e(k, v)").unwrap();
        assert!(each.contains("get_field(s, 'key') AS key, pondra_as_jsonb(get_field(s, 'value')) AS value"), "{each}");
        assert!(each.contains(") AS e (k, v)"), "{each}"); // (sqlparser prints a column list after a space)
        let text = rewritten("SELECT * FROM json_each_text('{}')").unwrap();
        assert!(text.contains("get_field(s, 'value') AS value") && text.contains(") AS json_each_text"), "{text}");
    }

    #[test]
    fn record_functions_name_their_columns() {
        let rows = rewritten("SELECT * FROM jsonb_to_recordset('[]') AS x(a int, b json, c text)").unwrap();
        assert!(rows.contains("CAST(e ->> 'a' AS INT) AS \"a\""), "{rows}");
        assert!(rows.contains("pondra_as_json(e -> 'b') AS \"b\""), "{rows}");
        assert!(rows.contains("CAST(e ->> 'c' AS TEXT) AS \"c\""), "{rows}");
        assert!(rows.contains(") AS x"), "{rows}");
        let record = rewritten("SELECT * FROM json_to_record('{}') AS x(a jsonb)").unwrap();
        assert!(record.contains("pondra_as_jsonb(e -> 'a') AS \"a\""), "{record}");
    }

    #[test]
    fn places_postgres_refuses_are_refused_by_its_words() {
        assert!(refusal("SELECT 1 FROM t WHERE jsonb_array_elements(doc) IS NULL").contains("set-returning functions are not allowed in WHERE"));
        assert!(refusal("SELECT CASE WHEN true THEN jsonb_array_elements('[1]') END").contains("not allowed in CASE"));
        assert!(refusal("SELECT COALESCE(jsonb_array_elements('[1]'), '1'::jsonb)").contains("not allowed in COALESCE"));
        assert!(refusal("SELECT count(jsonb_array_elements('[1]')) FROM t").contains("aggregate function calls cannot contain"));
        assert!(refusal("SELECT sum(jsonb_array_elements('[1]')::int) OVER () FROM t").contains("window function calls cannot contain"));
        assert!(refusal("SELECT 1 HAVING jsonb_array_elements('[1]') IS NULL").contains("not allowed in HAVING"));
        assert!(refusal("SELECT 1 LIMIT jsonb_array_elements('[1]')").contains("not allowed in LIMIT"));
        assert!(refusal("UPDATE t SET id = 1 WHERE jsonb_array_elements('[1]') IS NULL").contains("not allowed in WHERE"));
        assert!(refusal("UPDATE t SET doc = jsonb_array_elements('[1]')").contains("not allowed in UPDATE"));
        assert!(refusal("DELETE FROM t RETURNING jsonb_array_elements('[1]')").contains("not allowed in RETURNING"));
        assert!(refusal("SELECT 1 FROM t JOIN u ON jsonb_array_elements('[1]') IS NULL").contains("not allowed in JOIN conditions"));
    }

    #[test]
    fn places_not_taken_yet_are_named() {
        let order = refusal("SELECT 1 ORDER BY jsonb_array_elements('[1]')");
        assert!(order.contains("jsonb_array_elements('[1]') in ORDER BY isn't taken yet"), "{order}");
        assert!(refusal("SELECT 1 FROM t GROUP BY jsonb_array_elements(doc)").contains("in GROUP BY isn't taken yet"));
        assert!(refusal("SELECT sum(x) OVER (PARTITION BY jsonb_array_elements('[1]')) FROM t").contains("in window definitions isn't taken yet"));
        assert!(refusal("INSERT INTO t (id) VALUES (jsonb_array_elements('[1]')::text::int)").contains("in VALUES isn't taken yet"));
    }

    /// A FROM call that reads a column of the FROM is LATERAL, as Postgres takes it: written once here, planned by
    /// `lateral.rs` (which refuses a LEFT JOIN over a call whose outputs aren't its unnest's own, by name).
    #[test]
    fn from_calls_reading_a_column_are_lateral() {
        let implicit = rewritten("SELECT * FROM t, jsonb_array_elements(t.doc) e").unwrap();
        assert!(implicit.contains("LATERAL (SELECT pondra_as_jsonb(unnest(pondra_jsonb_array_elements(t.doc))) AS value) AS e"), "{implicit}");
        let cross = rewritten("SELECT * FROM t CROSS JOIN LATERAL jsonb_each(t.doc) AS kv").unwrap();
        assert!(cross.contains("CROSS JOIN LATERAL (SELECT get_field(s, 'key') AS key"), "{cross}");
        let left = rewritten("SELECT * FROM t LEFT JOIN LATERAL json_array_elements_text(t.doc) AS x ON true").unwrap();
        assert!(left.contains("LEFT JOIN LATERAL (SELECT unnest(pondra_json_array_elements_text(t.doc)) AS value) AS x ON true"), "{left}");
        let record = rewritten("SELECT * FROM t CROSS JOIN LATERAL jsonb_to_recordset(t.doc) AS r(a int, b text)").unwrap();
        assert!(record.contains("CAST(e ->> 'a' AS INT) AS \"a\""), "{record}");
        assert!(record.contains("LATERAL (SELECT CAST(e ->> 'a' AS INT)"), "{record}");
        let constant = rewritten("SELECT * FROM jsonb_array_elements('[1]') e").unwrap();
        assert!(!constant.contains("LATERAL"), "{constant}");
    }

    #[test]
    fn lateral_and_record_errors() {
        let record = refusal("SELECT * FROM jsonb_to_recordset('[{\"a\":1}]') AS x(a, b)");
        assert!(record.contains("a column definition list is required"), "{record}");
        assert!(refusal("SELECT * FROM jsonb_to_record('{}')").contains("a column definition list is required"));
        assert!(refusal("SELECT jsonb_to_record('{\"a\":1}')").contains("could not determine row type for result of jsonb_to_record"));
    }

    /// WITH ORDINALITY numbers each list's elements beside them, the list computed once in the FROM of the body; a
    /// record's columns take none, as Postgres 16 has it (ROWS FROM() isn't here), and a bare alias is the whole row.
    #[test]
    fn with_ordinality_numbers_the_list_beside_its_elements() {
        let elements = rewritten("SELECT * FROM jsonb_array_elements('[1,2]') WITH ORDINALITY AS e").unwrap();
        assert!(elements.contains("AS ordinality FROM (SELECT pondra_jsonb_array_elements('[1,2]') AS l) AS __l"), "{elements}");
        assert!(elements.contains("unnest(generate_series(1, CAST(cardinality(l) AS BIGINT))) AS ordinality"), "{elements}");
        let each = rewritten("SELECT * FROM jsonb_each('{\"a\":1}') WITH ORDINALITY AS kv(k, v, n)").unwrap();
        assert!(each.contains(", o AS ordinality FROM (SELECT unnest(l) AS s, unnest(generate_series(1, CAST(cardinality(l) AS BIGINT))) AS o FROM"), "{each}");
        let record = refusal("SELECT * FROM jsonb_to_recordset('[]') WITH ORDINALITY AS x(a int)");
        assert!(record.contains("WITH ORDINALITY cannot be used with a column definition list"), "{record}");
        assert!(refusal("SELECT * FROM jsonb_to_recordset('[]') WITH ORDINALITY AS x").contains("a column definition list is required"));
        let whole_row = rewritten("SELECT e FROM jsonb_array_elements('[1]') WITH ORDINALITY AS e").unwrap();
        assert!(!whole_row.contains("e.value"), "{whole_row}");
    }

    #[test]
    fn queries_inside_a_call_keep_their_own_places() {
        let inner = rewritten("SELECT (SELECT jsonb_array_elements_text('[1]') LIMIT 1) FROM t").unwrap();
        assert!(inner.contains("unnest(pondra_jsonb_array_elements_text('[1]'))"), "{inner}");
        assert!(rewritten("SELECT CASE WHEN (SELECT true FROM (SELECT jsonb_array_elements('[1]') AS x) s) THEN 1 END").is_ok());
    }

    /// A bare value name is its value where SQL's scoping says so: in a FROM set call's own SELECT, ORDER BY included, and
    /// not where a relation has a column of that name, nor for a function whose rows are records.
    #[test]
    fn a_bare_value_name_is_its_value() {
        let filter = rewritten_with("SELECT t.id FROM t, jsonb_array_elements(t.doc) AS e WHERE e ->> 'k' = 'x'", Some(&["id", "doc"])).unwrap();
        assert!(filter.contains("e.value ->> 'k' = 'x'"), "{filter}");
        let item = rewritten_with("SELECT e FROM jsonb_array_elements('[1,2]') e", Some(&[])).unwrap();
        assert!(item.contains("e.value AS \"e\""), "{item}"); // (the item keeps its name, as Postgres names it)
        let order = rewritten_with("SELECT 1 FROM jsonb_array_elements('[1]') e ORDER BY e", Some(&[])).unwrap();
        assert!(order.contains("ORDER BY e.value"), "{order}");
        let exists = rewritten_with("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM jsonb_array_elements(t.doc) e WHERE e ->> 'k' = 'x')", Some(&["id", "doc"])).unwrap();
        assert!(exists.contains("e.value ->> 'k' = 'x'"), "{exists}");
        let shadowed = rewritten_with("SELECT e FROM t, jsonb_array_elements(t.doc) AS e", Some(&["e", "doc"])).unwrap();
        assert!(!shadowed.contains("e.value"), "{shadowed}");
        let unknown = rewritten_with("SELECT e FROM jsonb_array_elements('[1]') e WHERE e ->> 'k' = 'x'", None).unwrap();
        assert!(!unknown.contains("e.value") && unknown.contains("e ->> 'k'"), "{unknown}");
        let record = rewritten_with("SELECT e FROM jsonb_each('{\"a\":1}') AS e", Some(&[])).unwrap();
        assert!(!record.contains("e.value"), "{record}");
        let listed = rewritten_with("SELECT e FROM jsonb_array_elements('[1]') AS e(x)", Some(&[])).unwrap();
        assert!(!listed.contains("e.value"), "{listed}"); // (the column list renames the value's column)
        let default = rewritten_with("SELECT jsonb_array_elements FROM jsonb_array_elements('[1]')", Some(&[])).unwrap();
        assert!(default.contains("jsonb_array_elements.value AS \"jsonb_array_elements\""), "{default}");
    }

    /// A derived table is a scope of its own: a bare name in its query is its value, unless an item of unknown columns is in
    /// scope there, in its own FROM or an enclosing one (Postgres reads a column of any level before a FROM alias's row).
    #[test]
    fn a_bare_value_name_inside_a_derived_table_is_its_value() {
        let alone = rewritten_with("SELECT s.sku FROM (SELECT e ->> 'sku' AS sku FROM jsonb_array_elements('[1]') e) s", Some(&[])).unwrap();
        assert!(alone.contains("e.value ->> 'sku'"), "{alone}");
        let lateral = rewritten_with("SELECT d.id, s.sku FROM docs d LEFT JOIN LATERAL (SELECT e ->> 'sku' AS sku FROM jsonb_array_elements(d.doc -> 'items') e) s ON true", Some(&["id", "doc"])).unwrap();
        assert!(lateral.contains("e.value ->> 'sku'"), "{lateral}");
        let outer = rewritten_with("SELECT e ->> 'k' FROM (SELECT '{}' AS e) s, jsonb_array_elements('[1]') e", Some(&[])).unwrap();
        assert!(!outer.contains("e.value"), "{outer}"); // (the derived table's column `e` is the one Postgres reads)
        let sibling = rewritten_with("SELECT b.x FROM (SELECT 1 AS e) a, LATERAL (SELECT e AS x FROM jsonb_array_elements('[1]') e) b", Some(&[])).unwrap();
        assert!(!sibling.contains("e.value"), "{sibling}"); // (Postgres reads that `e` as `a.e`)
        let subquery = rewritten_with("SELECT (SELECT e ->> 'k' FROM jsonb_array_elements('[1]') e) FROM (SELECT 1 AS e) s", Some(&[])).unwrap();
        assert!(!subquery.contains("e.value"), "{subquery}"); // (an outer column `e` is read before the inner alias's row)
    }

    #[test]
    fn a_bare_value_name_is_found_before_the_catalog_is_asked() {
        let parse = |sql: &str| Parser::parse_sql(&GenericDialect {}, sql).expect("parses").remove(0);
        assert!(bare_aliases(&parse("SELECT e FROM jsonb_array_elements('[1]') e")));
        assert!(bare_aliases(&parse("SELECT s.sku FROM (SELECT e ->> 'sku' AS sku FROM jsonb_array_elements('[1]') e) s")));
        assert!(bare_aliases(&parse("SELECT 1 FROM t, jsonb_array_elements_text(t.doc) WHERE jsonb_array_elements_text IS NULL")));
        assert!(!bare_aliases(&parse("SELECT e.value FROM jsonb_array_elements('[1]') e")));
        assert!(!bare_aliases(&parse("SELECT e FROM jsonb_each('{}') e")));
        assert!(!bare_aliases(&parse("SELECT e FROM t")));
    }

    #[test]
    fn statements_without_calls_stay_as_they_are() {
        let mut stmt = Parser::parse_sql(&GenericDialect {}, "SELECT json_keys(doc), json_extract_path(doc, 'a') FROM t").unwrap().remove(0);
        let before = stmt.to_string();
        assert!(!sets(&mut stmt, None).unwrap());
        assert_eq!(stmt.to_string(), before);
        assert!(!crate::text::names_a_type("SELECT json_keys(doc) FROM t"));
        assert!(crate::text::names_a_type("SELECT * FROM JSONB_EACH(doc)"));
    }

    /// `text.rs` keeps its own pattern of these names (it is in every build, and this module is the server's): a name added
    /// to `SETS` that the pattern doesn't name would be read as a plain query.
    #[test]
    fn the_gate_names_every_set_function() {
        for set in SETS.iter() {
            assert!(crate::text::names_a_type(&format!("SELECT * FROM {}('[]')", set.name)), "{}", set.name);
        }
    }

    /// JSON's spaces around a document change no row: the items, the keys and the values of each expanding function come
    /// out as they do without the spaces, and a function given an object or an array of the other kind refuses it.
    #[test]
    fn spaces_around_a_document_change_no_row() {
        for doc in [" [1, \"x\"]", "\t[1, \"x\"]", "\n\r \t[1, \"x\"]", "[1, \"x\"]\n ", " [1, \"x\"] \t\r\n"] {
            assert_eq!(elements(doc), Some(vec!["1", "\"x\""]), "{doc:?}");
        }
        assert_eq!(elements(" []"), Some(vec![]));
        assert_eq!(elements(" {}"), None, "an object is no array");

        let object = " \n{\"b\": 1, \"a\": {\"x\": 2}}";
        assert_eq!(members(object, false), Some(vec![("b".to_string(), "1"), ("a".to_string(), "{\"x\": 2}")]));
        assert_eq!(members(object, false), members("{\"b\": 1, \"a\": {\"x\": 2}}", false));
        assert_eq!(members("\t{\"b\":1,\"a\":2,\"b\":3} ", true), members(r#"{"b":1,"a":2,"b":3}"#, true));
        assert_eq!(members(" [1]", false), None, "an array is no object");

        let texts = |name: &str, doc: &str| SETS.iter().find(|set| set.name == name).expect("a set").texts_of(doc).expect("rows");
        assert_eq!(texts("jsonb_array_elements_text", " [1.50, {\"b\":1,\"a\":2,\"a\":3}]"), vec![Some("1.50".to_string()), Some("{\"a\": 3, \"b\": 1}".to_string())]);
        assert_eq!(texts("json_array_elements_text", "\t[1.50, {\"b\":1,\"a\":2,\"a\":3}]"), vec![Some("1.50".to_string()), Some("{\"b\":1,\"a\":2,\"a\":3}".to_string())]);
        assert_eq!(texts("jsonb_object_keys", "\n{\"b\":1,\"a\":2}"), vec![Some("a".to_string()), Some("b".to_string())]);
        assert_eq!(texts("json_object_keys", " {\"b\":1,\"a\":2,\"b\":3}"), vec![Some("b".to_string()), Some("a".to_string()), Some("b".to_string())]);
        assert_eq!(texts("jsonb_to_recordset", " [{\"a\":1,\"b\":\"x\"}]"), vec![Some("{\"a\": 1, \"b\": \"x\"}".to_string())]);
        assert!(SETS.iter().find(|set| set.name == "jsonb_object_keys").expect("a set").texts_of(" [1]").is_err(), "an array of keys is refused");

        let pairs = |name: &str, doc: &str| SETS.iter().find(|set| set.name == name).expect("a set").pairs_of(doc).expect("rows");
        assert_eq!(
            pairs("jsonb_each_text", " {\"b\": 1, \"a\": {\"x\": 2}}"),
            vec![("a".to_string(), Some("{\"x\": 2}".to_string())), ("b".to_string(), Some("1".to_string()))]
        );
        assert_eq!(pairs("jsonb_each", "\t{\"b\":1,\"a\":{\"x\":2}}\n"), vec![("a".to_string(), Some("{\"x\": 2}".to_string())), ("b".to_string(), Some("1".to_string()))]);
    }
}
