//! Postgres's functions that expand a JSON document into rows (round 35, brief D1): `json_array_elements` and its
//! `_text`, `json_each` and its `_text`, `json_object_keys`, `json_to_recordset` and `json_to_record`, and their jsonb
//! forms. Each is a list function here (`pondra_<name>`) that reads a document once and answers its rows. Where SQL
//! comes in (`sets`), a call in a SELECT list becomes `unnest` of that list, a call in a FROM a derived table of its
//! rows, and each place Postgres refuses a call is refused by Postgres's words and SQLSTATE before anything changes.
use crate::codes::coded;
use crate::pg_catalog::{function, text_arg};
use crate::pg_json::{answer, kind, JSON_SPACE};
use crate::pg_json_make::fail;
use crate::pg_types::names_kept;
use crate::types::json_refusal;
use datafusion::arrow::array::{ArrayRef, ListArray, StringArray, StructArray};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields};
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{self, Expr, FunctionArg, LimitClause, Query, Select, SetExpr, Statement, TableAlias, TableAliasColumnDef, TableFactor};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use jiter::Jiter;
use std::collections::BTreeMap;
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
        let rows = crate::types::function("unnest", vec![crate::types::function(self.list, args)]);
        match (self.how, self.jsonb) {
            (How::Elements, false) => crate::types::function("pondra_as_json", vec![rows]),
            (How::Elements, true) => crate::types::function("pondra_as_jsonb", vec![rows]),
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
    let mut jiter = Jiter::new(doc.as_bytes());
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
    let mut jiter = Jiter::new(doc.as_bytes());
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

/// A call as SQL writes it, for a message: `name(arg, arg)`.
fn call_text(name: &ast::ObjectName, args: &[FunctionArg]) -> String {
    format!("{name}({})", args.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "))
}

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

    /// The FROM calls: each must read no column (LATERAL isn't taken yet), and a record's columns must be named.
    fn from(&mut self, s: &Select) -> anyhow::Result<()> {
        let table = match s.from.first().map(|t| &t.relation) {
            Some(TableFactor::Table { name, .. }) => name.to_string(),
            _ => "t".to_string(),
        };
        for twj in &s.from {
            for factor in std::iter::once(&twj.relation).chain(twj.joins.iter().map(|j| &j.relation)) {
                self.factor(factor, &table)?;
            }
        }
        Ok(())
    }

    fn factor(&mut self, factor: &TableFactor, table: &str) -> anyhow::Result<()> {
        match factor {
            TableFactor::Table { name, args: Some(args), alias, with_ordinality, .. } => {
                let Some(set) = set_of(name) else { return Ok(()) };
                self.found = true;
                let call = call_text(name, &args.args);
                if *with_ordinality {
                    return Err(coded("0A000", format!("WITH ORDINALITY isn't taken yet: {call}")));
                }
                if look(&args.args, is_column).is_some() {
                    return Err(coded("0A000", format!("{call} in FROM needs LATERAL, not yet: call it in the SELECT list (SELECT {call} FROM {table})")));
                }
                if set.records() {
                    record_columns(alias.as_ref())?;
                }
                Ok(())
            }
            TableFactor::Function { name, args, .. } if set_of(name).is_some() => {
                self.found = true;
                let call = call_text(name, args);
                Err(coded("0A000", format!("{call} in FROM with LATERAL isn't taken yet: call it in the SELECT list (SELECT {call} FROM {table})")))
            }
            _ => Ok(()),
        }
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
        let TableFactor::Table { name, args: Some(args), alias, .. } = factor else { return };
        let Some(set) = set_of(name) else { return };
        match derived(set, &args.args, alias.as_ref()) {
            Ok(table) => *factor = table,
            Err(e) => {
                if self.failed.is_none() {
                    self.failed = Some(e);
                }
            }
        }
    }
}

/// A FROM call as the derived table of its rows, with the names Postgres gives them: `value` for the elements, the
/// alias's name for the keys (or the function's), `key` and `value` for each member, and a record's columns named by
/// the alias's column definitions. The derived table takes the alias, and its column list when a non-record has one.
fn derived(set: &Set, args: &[FunctionArg], alias: Option<&TableAlias>) -> anyhow::Result<TableFactor> {
    let list = format!("{}({})", set.list, args.iter().map(ToString::to_string).collect::<Vec<_>>().join(", "));
    let name = alias.map_or_else(|| ast::Ident::new(set.name), |a| a.name.clone());
    let body = match set.how {
        How::Elements => format!("SELECT {} AS value", as_json(set, &format!("unnest({list})"))),
        How::ElementsText => format!("SELECT unnest({list}) AS value"),
        How::ObjectKeys => format!("SELECT unnest({list}) AS {name}"),
        How::Each | How::EachText => {
            let value = match set.how {
                How::Each => as_json(set, "get_field(s, 'value')"),
                _ => "get_field(s, 'value')".to_string(),
            };
            format!("SELECT get_field(s, 'key') AS key, {value} AS value FROM (SELECT unnest({list}) AS s) AS u")
        }
        How::ToRecordset | How::ToRecord => format!("SELECT {} FROM (SELECT unnest({list}) AS e) AS s", record_columns(alias)?),
    };
    let columns = match set.records() {
        true => vec![],
        false => alias.map_or_else(Vec::new, |a| a.columns.iter().map(|c| TableAliasColumnDef { name: c.name.clone(), data_type: None }).collect()),
    };
    Ok(TableFactor::Derived {
        lateral: false,
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

/// Rewrites a statement's set-returning calls where SQL comes in (`types::rewrite`), and whether it changed any. What
/// Postgres refuses, and what this engine doesn't take yet, is refused before anything changes. The names of changed
/// SELECT items are kept as written (`names_kept`).
pub fn sets(stmt: &mut Statement) -> anyhow::Result<bool> {
    let mut scan = Scan::default();
    if let ControlFlow::Break(e) = ast::Visit::visit(&*stmt, &mut scan) {
        return Err(e);
    }
    if !scan.found {
        return Ok(false);
    }
    let mut rewrite = Rewrite::default();
    names_kept(stmt, |s| {
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

    /// The statement after `sets`, as text, or the refusal.
    fn rewritten(sql: &str) -> anyhow::Result<String> {
        let mut stmt = Parser::parse_sql(&GenericDialect {}, sql)?.remove(0);
        sets(&mut stmt)?;
        Ok(stmt.to_string())
    }

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

    #[test]
    fn lateral_and_record_errors() {
        let lateral = refusal("SELECT * FROM t, jsonb_array_elements(t.doc) e");
        assert!(lateral.contains("jsonb_array_elements(t.doc) in FROM needs LATERAL, not yet"), "{lateral}");
        assert!(lateral.contains("(SELECT jsonb_array_elements(t.doc) FROM t)"), "{lateral}");
        assert!(refusal("SELECT * FROM jsonb_array_elements('[1]') WITH ORDINALITY AS e").contains("WITH ORDINALITY isn't taken yet"));
        let record = refusal("SELECT * FROM jsonb_to_recordset('[{\"a\":1}]') AS x(a, b)");
        assert!(record.contains("a column definition list is required"), "{record}");
        assert!(refusal("SELECT * FROM jsonb_to_record('{}')").contains("a column definition list is required"));
        assert!(refusal("SELECT jsonb_to_record('{\"a\":1}')").contains("could not determine row type for result of jsonb_to_record"));
    }

    #[test]
    fn queries_inside_a_call_keep_their_own_places() {
        let inner = rewritten("SELECT (SELECT jsonb_array_elements_text('[1]') LIMIT 1) FROM t").unwrap();
        assert!(inner.contains("unnest(pondra_jsonb_array_elements_text('[1]'))"), "{inner}");
        assert!(rewritten("SELECT CASE WHEN (SELECT true FROM (SELECT jsonb_array_elements('[1]') AS x) s) THEN 1 END").is_ok());
    }

    #[test]
    fn statements_without_calls_stay_as_they_are() {
        let mut stmt = Parser::parse_sql(&GenericDialect {}, "SELECT json_keys(doc), json_extract_path(doc, 'a') FROM t").unwrap().remove(0);
        let before = stmt.to_string();
        assert!(!sets(&mut stmt).unwrap());
        assert_eq!(stmt.to_string(), before);
        assert!(!crate::text::names_a_type("SELECT json_keys(doc) FROM t"));
        assert!(crate::text::names_a_type("SELECT * FROM JSONB_EACH(doc)"));
    }
}
