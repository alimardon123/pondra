//! Postgres's functions that change a JSON document (round 35): `jsonb_set`, `jsonb_insert`, `jsonb_concat` (jsonb's
//! `||`, as a function), `jsonb_delete` (`-`), `jsonb_delete_path` (`#-`), `jsonb_strip_nulls`, `json_strip_nulls` and
//! `jsonb_pretty`. A jsonb change reads its document into the tree `pg_json_make` writes (`Node`), so its answer is
//! jsonb's text: keys in jsonb's order, `, ` and `: ` between members. `json_strip_nulls` copies a json document's tokens
//! as written (no spaces between them, duplicate keys kept), as Postgres's does. The answers follow Postgres 16, down to
//! the ones that look odd: a path that reaches nothing changes nothing, and only a path element that is NULL or not an
//! integer where an array is, or a path into a scalar, is refused. `pg_json::operator` plans `-` and `#-` onto these.
use crate::codes::coded;
use crate::pg_catalog::{function, marked, texts_out};
use crate::pg_json::{index_of, is_integer, lists};
use crate::pg_json_make::{depth_check, fail, json_string, parse, refuse, write, Node};
use crate::types::json_refusal;
use datafusion::arrow::array::{Array, ArrayRef, AsArray, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Int64Type};
use datafusion::common::Result;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::prelude::SessionContext;
use jiter::{Jiter, Peek};
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

/// The functions this file makes; `pg_json` finds the ones its operators call here too.
pub(crate) static FUNCTIONS: LazyLock<Vec<Arc<ScalarUDF>>> = LazyLock::new(|| {
    let any = |n| Signature::any(n, Volatility::Immutable);
    let two_or_three = || Signature::one_of(vec![TypeSignature::Any(3), TypeSignature::Any(4)], Volatility::Immutable);
    vec![
        Arc::new(marked("jsonb_set", two_or_three(), |_| DataType::Utf8, Some("jsonb"), set_body)),
        Arc::new(marked("jsonb_insert", two_or_three(), |_| DataType::Utf8, Some("jsonb"), insert_body)),
        Arc::new(marked("jsonb_concat", any(2), |_| DataType::Utf8, Some("jsonb"), concat_body)),
        Arc::new(marked("jsonb_delete", any(2), |_| DataType::Utf8, Some("jsonb"), delete_body)),
        Arc::new(marked("jsonb_delete_path", any(2), |_| DataType::Utf8, Some("jsonb"), delete_path_body)),
        Arc::new(marked("jsonb_strip_nulls", any(1), |_| DataType::Utf8, Some("jsonb"), strip_body)),
        Arc::new(marked("json_strip_nulls", any(1), |_| DataType::Utf8, Some("json"), strip_json_body)),
        Arc::new(function("jsonb_pretty", any(1), |_| DataType::Utf8, pretty_body)),
    ]
});

/// Adds the functions to a session. `pg_json::register` calls it, so they are in every session.
pub fn register(ctx: &SessionContext) {
    for f in FUNCTIONS.iter() {
        ctx.register_udf(f.as_ref().clone());
    }
}

// ---- the functions' bodies: one row at a time over whole columns, NULL where any argument is NULL (they are strict) ----

/// `jsonb_set(target, path, new_value [, create])`: the value at the path is replaced, or made when `create` (true
/// by default) and the path's last step is new.
fn set_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let (docs, values) = (texts(&a[0])?, texts(&a[2])?);
    let (docs, values) = (docs.as_string::<i32>(), values.as_string::<i32>());
    let paths = lists(&a[1]).map_err(fail)?;
    let create = flags(a, 3, n, true)?;
    rows(n, |i| {
        let (Some(doc), Some(path), Some(value), Some(create)) = (text_at(docs, i), &paths[i], text_at(values, i), create[i]) else {
            return Ok(None);
        };
        edit_doc(doc, path, &Edit::Set { value: parse(value)?, create }).map(Some)
    })
}

/// `jsonb_insert(target, path, new_value [, insert_after])`: a new member or element at the path; `insert_after` (false
/// by default) puts an element after the step it names. A key that is there already is refused.
fn insert_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let (docs, values) = (texts(&a[0])?, texts(&a[2])?);
    let (docs, values) = (docs.as_string::<i32>(), values.as_string::<i32>());
    let paths = lists(&a[1]).map_err(fail)?;
    let after = flags(a, 3, n, false)?;
    rows(n, |i| {
        let (Some(doc), Some(path), Some(value), Some(after)) = (text_at(docs, i), &paths[i], text_at(values, i), after[i]) else {
            return Ok(None);
        };
        edit_doc(doc, path, &Edit::Insert { value: parse(value)?, after }).map(Some)
    })
}

/// `jsonb_concat(a, b)`: jsonb's `||`.
fn concat_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let (lefts, rights) = (texts(&a[0])?, texts(&a[1])?);
    let (lefts, rights) = (lefts.as_string::<i32>(), rights.as_string::<i32>());
    rows(n, |i| match (text_at(lefts, i), text_at(rights, i)) {
        (Some(l), Some(r)) => concat(l, r).map(Some),
        _ => Ok(None),
    })
}

/// `jsonb_delete(doc, key)`, `jsonb_delete(doc, index)` and `jsonb_delete(doc, keys)`: `-`, by the right side's type.
fn delete_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let docs = texts(&a[0])?;
    let docs = docs.as_string::<i32>();
    if is_integer(a[1].data_type()) {
        let indexes = cast(&a[1], &DataType::Int64)?;
        let indexes = indexes.as_primitive::<Int64Type>();
        return rows(n, |i| match (text_at(docs, i), indexes.is_valid(i).then(|| indexes.value(i))) {
            (Some(doc), Some(at)) => delete_index(doc, at).map(Some),
            _ => Ok(None),
        });
    }
    if matches!(a[1].data_type(), DataType::List(_) | DataType::LargeList(_)) {
        let keys = lists(&a[1]).map_err(fail)?;
        return rows(n, |i| match (text_at(docs, i), &keys[i]) {
            (Some(doc), Some(keys)) => {
                let names: Vec<&str> = keys.iter().flatten().map(String::as_str).collect();
                delete_keys(doc, &names).map(Some)
            }
            _ => Ok(None),
        });
    }
    let keys = texts(&a[1])?;
    let keys = keys.as_string::<i32>();
    rows(n, |i| match (text_at(docs, i), text_at(keys, i)) {
        (Some(doc), Some(key)) => delete_keys(doc, &[key]).map(Some),
        _ => Ok(None),
    })
}

/// `jsonb_delete_path(doc, path)`: `#-`.
fn delete_path_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let docs = texts(&a[0])?;
    let docs = docs.as_string::<i32>();
    let paths = lists(&a[1]).map_err(fail)?;
    rows(n, |i| match (text_at(docs, i), &paths[i]) {
        (Some(doc), Some(path)) => edit_doc(doc, path, &Edit::Delete).map(Some),
        _ => Ok(None),
    })
}

fn strip_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let docs = texts(&a[0])?;
    let docs = docs.as_string::<i32>();
    rows(n, |i| text_at(docs, i).map(strip_nulls).transpose())
}

fn strip_json_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let docs = texts(&a[0])?;
    let docs = docs.as_string::<i32>();
    rows(n, |i| text_at(docs, i).map(strip_json).transpose())
}

fn pretty_body(a: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    let docs = texts(&a[0])?;
    let docs = docs.as_string::<i32>();
    rows(n, |i| text_at(docs, i).map(pretty).transpose())
}

/// An argument as text, whichever string type it came as.
fn texts(a: &ArrayRef) -> Result<ArrayRef> { Ok(cast(a, &DataType::Utf8)?) }

fn text_at(s: &StringArray, i: usize) -> Option<&str> { (!s.is_null(i)).then(|| s.value(i)) }

/// An optional boolean argument per row: `default` where the argument is absent. Its NULL is NULL.
fn flags(a: &[ArrayRef], at: usize, n: usize, default: bool) -> Result<Vec<Option<bool>>> {
    match a.get(at) {
        None => Ok(vec![Some(default); n]),
        Some(flag) => Ok(cast(flag, &DataType::Boolean)?.as_boolean().iter().collect()),
    }
}

/// One value per row: `row(i)` is row `i`'s text, None where it is NULL.
fn rows(n: usize, row: impl Fn(usize) -> Result<Option<String>>) -> Result<ArrayRef> {
    Ok(texts_out((0..n).map(row).collect::<Result<Vec<_>>>()?))
}

// ---- the changes, on one document's text: each returns the answer's text ----

/// What a path does to the value it reaches.
enum Edit {
    /// `jsonb_set`: replaces the value, or makes it at a new member (or at a position past an array's end) when `create`.
    Set { value: Node, create: bool },
    /// `jsonb_insert`: a new member, or an element before (or after) the position named.
    Insert { value: Node, after: bool },
    /// `jsonb_delete_path`: what the path reaches goes.
    Delete,
}

impl Edit {
    /// The edit at a member named `key` of an object.
    fn in_object(&self, members: &mut BTreeMap<(usize, String), Node>, key: (usize, String)) -> Result<()> {
        match self {
            Edit::Set { value, create } => {
                if let Some(slot) = members.get_mut(&key) {
                    *slot = value.clone();
                } else if *create {
                    members.insert(key, value.clone());
                }
            }
            Edit::Insert { value, .. } => {
                if members.contains_key(&key) {
                    return Err(refuse("22023", "cannot replace existing key"));
                }
                members.insert(key, value.clone());
            }
            Edit::Delete => {
                members.remove(&key);
            }
        }
        Ok(())
    }

    /// The edit at element `raw` of an array (a negative one counts from the end).
    fn in_list(&self, items: &mut Vec<Node>, raw: i64) -> Result<()> {
        match self {
            Edit::Set { value, create } => match position(items.len(), raw) {
                Some(at) => items[at] = value.clone(),
                None if *create => items.insert(insert_point(items.len(), raw, false), value.clone()),
                None => {}
            },
            Edit::Insert { value, after } => items.insert(insert_point(items.len(), raw, *after), value.clone()),
            Edit::Delete => {
                if let Some(at) = position(items.len(), raw) {
                    items.remove(at);
                }
            }
        }
        Ok(())
    }
}

/// The document with `edit` made at `path`. An empty path is the document itself (as jsonb writes it); a path that
/// reaches nothing leaves it as it is. A path into a scalar, and a NULL step, are refused.
fn edit_doc(doc: &str, path: &[Option<String>], edit: &Edit) -> Result<String> {
    let mut root = parse(doc)?;
    let steps = path_steps(path)?;
    if steps.is_empty() {
        return text_of(&root);
    }
    if !matches!(root, Node::Object(_) | Node::List(_)) {
        let verb = if matches!(edit, Edit::Delete) { "delete" } else { "set" };
        return Err(refuse("22023", format!("cannot {verb} path in scalar")));
    }
    walk(&mut root, &steps, edit)?;
    text_of(&root)
}

/// The path's steps as text. A NULL step is refused, as Postgres refuses it (22004).
fn path_steps(path: &[Option<String>]) -> Result<Vec<String>> {
    path.iter()
        .enumerate()
        .map(|(k, step)| step.clone().ok_or_else(|| refuse("22004", format!("path element at position {} is null", k + 1))))
        .collect()
}

/// The edit at the end of `path` below `node`. A step that reaches nothing (a missing member, an index past an array's
/// end, a scalar on the way) changes nothing; an array's step that isn't an integer is refused.
fn walk(node: &mut Node, path: &[String], edit: &Edit) -> Result<()> {
    let Some((step, rest)) = path.split_first() else { return Ok(()) };
    match node {
        Node::Object(members) => {
            if rest.is_empty() {
                return edit.in_object(members, (step.len(), step.clone()));
            }
            match members.get_mut(&(step.len(), step.clone())) {
                Some(child) => walk(child, rest, edit),
                None => Ok(()),
            }
        }
        Node::List(items) => {
            let raw = index_step(step)?;
            if rest.is_empty() {
                return edit.in_list(items, raw);
            }
            match position(items.len(), raw) {
                Some(at) => walk(&mut items[at], rest, edit),
                None => Ok(()),
            }
        }
        _ => Ok(()),
    }
}

/// An array's path step as an integer (`index_of` reads it as Postgres does).
fn index_step(step: &str) -> Result<i64> {
    index_of(step).ok_or_else(|| refuse("22P02", format!("path element is not an integer: \"{step}\"")))
}

/// The element a step names in an array of `len`, if it is there (a negative step counts from the end).
fn position(len: usize, raw: i64) -> Option<usize> {
    let at = if raw < 0 { len as i64 + raw } else { raw };
    (0..len as i64).contains(&at).then_some(at as usize)
}

/// Where an element goes for a step that names a place in an array: past either end it goes to the end (or the start),
/// and `after` puts it one place later.
fn insert_point(len: usize, raw: i64, after: bool) -> usize {
    let at = if raw < 0 { len as i64 + raw } else { raw };
    if at < 0 {
        0
    } else if at >= len as i64 {
        len
    } else {
        at as usize + usize::from(after)
    }
}

/// `jsonb_concat`: two objects merge (the right one's members win, not deeply); two arrays join; an array with
/// anything else takes it as one element; anything else makes a two-element array.
fn concat(left: &str, right: &str) -> Result<String> { text_of(&concat_nodes(parse(left)?, parse(right)?)) }

fn concat_nodes(left: Node, right: Node) -> Node {
    match (left, right) {
        (Node::Object(mut a), Node::Object(b)) => {
            a.extend(b);
            Node::Object(a)
        }
        (Node::List(mut a), Node::List(b)) => {
            a.extend(b);
            Node::List(a)
        }
        (Node::List(mut a), other) => {
            a.push(other);
            Node::List(a)
        }
        (other, Node::List(b)) => {
            let mut a = vec![other];
            a.extend(b);
            Node::List(a)
        }
        (a, b) => Node::List(vec![a, b]),
    }
}

/// `jsonb_delete(doc, key)` or `(doc, keys)`: an object loses its members with those keys; an array loses the strings
/// that equal them. A scalar has nothing to delete from.
fn delete_keys(doc: &str, keys: &[&str]) -> Result<String> {
    let mut node = parse(doc)?;
    match &mut node {
        Node::Object(members) => members.retain(|key, _| !keys.contains(&key.1.as_str())),
        Node::List(items) => items.retain(|item| !matches!(item, Node::Text(s) if keys.contains(&s.as_str()))),
        _ => return Err(refuse("22023", "cannot delete from scalar")),
    }
    text_of(&node)
}

/// `jsonb_delete(doc, index)`: an array loses its element at the index (a negative one counts from the end; past the
/// end it is unchanged). An object can't take an index.
fn delete_index(doc: &str, at: i64) -> Result<String> {
    let mut node = parse(doc)?;
    match &mut node {
        Node::Object(_) => return Err(refuse("22023", "cannot delete from object using integer index")),
        Node::List(items) => {
            if let Some(gone) = position(items.len(), at) {
                items.remove(gone);
            }
        }
        _ => return Err(refuse("22023", "cannot delete from scalar")),
    }
    text_of(&node)
}

/// `jsonb_strip_nulls`: the document with every object member whose value is null left out (array elements stay).
fn strip_nulls(doc: &str) -> Result<String> { text_of(&without_nulls(parse(doc)?)) }

fn without_nulls(node: Node) -> Node {
    match node {
        Node::Object(members) => Node::Object(
            members
                .into_iter()
                .filter(|(_, value)| !matches!(value, Node::Null))
                .map(|(key, value)| (key, without_nulls(value)))
                .collect(),
        ),
        Node::List(items) => Node::List(items.into_iter().map(without_nulls).collect()),
        other => other,
    }
}

/// `json_strip_nulls`: the document's tokens copied as written, without the space between them and with every object
/// member whose value is null left out. A duplicate key is kept, as json keeps it. Invalid text is 22P02.
fn strip_json(doc: &str) -> Result<String> {
    depth_check(doc)?;
    let mut jiter = Jiter::new(doc.as_bytes());
    let mut out = String::with_capacity(doc.len());
    let peek = jiter.peek().map_err(|_| invalid(doc))?;
    stripped(&mut jiter, doc, peek, &mut out).ok_or_else(|| invalid(doc))?;
    jiter.finish().map_err(|_| invalid(doc))?;
    Ok(out)
}

fn invalid(doc: &str) -> DataFusionError { fail(coded("22P02", json_refusal(doc))) }

/// The value at the reader's place (its first byte is `peek`), copied to `out` as it is written. A member whose value
/// is null is dropped with its comma, so the value after a dropped one starts clean.
fn stripped(jiter: &mut Jiter, doc: &str, peek: Peek, out: &mut String) -> Option<()> {
    if peek == Peek::Array {
        out.push('[');
        let mut next = jiter.known_array().ok()?;
        let mut first = true;
        while let Some(item) = next {
            if !first {
                out.push(',');
            }
            first = false;
            stripped(jiter, doc, item, out)?;
            next = jiter.array_step().ok()?;
        }
        out.push(']');
    } else if peek == Peek::Object {
        out.push('{');
        let mut first = true;
        // (a key's bytes are read from the document and copied before the next read moves on)
        let mut key = jiter.next_object_bytes().ok()?.map(<[u8]>::to_vec);
        while let Some(name) = key {
            let mark = out.len();
            if !first {
                out.push(',');
            }
            out.push('"');
            out.push_str(std::str::from_utf8(&name).ok()?);
            out.push_str("\":");
            let value = jiter.peek().ok()?;
            if value == Peek::Null {
                jiter.known_null().ok()?;
                out.truncate(mark);
            } else {
                stripped(jiter, doc, value, out)?;
                first = false;
            }
            key = jiter.next_key_bytes().ok()?.map(<[u8]>::to_vec);
        }
        out.push('}');
    } else {
        let start = jiter.current_index();
        jiter.known_skip(peek).ok()?;
        out.push_str(doc.get(start..jiter.current_index())?.trim_matches(JSON_SPACE));
    }
    Some(())
}

const JSON_SPACE: &[char] = &[' ', '\t', '\n', '\r'];

/// `jsonb_pretty`: jsonb's text indented four spaces a level, a member or element to a line, an empty object or
/// array as `{` and a line and `}`.
fn pretty(doc: &str) -> Result<String> {
    let mut out = String::new();
    pretty_into(&mut out, &parse(doc)?, 0).ok_or_else(overflow)?;
    Ok(out)
}

fn pretty_into(out: &mut String, node: &Node, depth: usize) -> Option<()> {
    match node {
        Node::List(items) => {
            out.push('[');
            for (k, item) in items.iter().enumerate() {
                out.push_str(if k == 0 { "\n" } else { ",\n" });
                indent(out, depth + 1);
                pretty_into(out, item, depth + 1)?;
            }
            out.push('\n');
            indent(out, depth);
            out.push(']');
        }
        Node::Object(members) => {
            out.push('{');
            for (k, ((_, name), item)) in members.iter().enumerate() {
                out.push_str(if k == 0 { "\n" } else { ",\n" });
                indent(out, depth + 1);
                json_string(out, name);
                out.push_str(": ");
                pretty_into(out, item, depth + 1)?;
            }
            out.push('\n');
            indent(out, depth);
            out.push('}');
        }
        scalar => write(out, scalar)?,
    }
    Some(())
}

fn indent(out: &mut String, depth: usize) {
    for _ in 0..depth {
        out.push_str("    ");
    }
}

/// jsonb's text for a tree.
fn text_of(node: &Node) -> Result<String> {
    let mut out = String::new();
    write(&mut out, node).ok_or_else(overflow)?;
    Ok(out)
}

fn overflow() -> DataFusionError { refuse("22003", "value overflows numeric format") }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codes::Coded;
    use crate::pg_values::array_literal;

    /// A path literal (`{b,1}`) as the functions take it.
    fn path(text: &str) -> Vec<Option<String>> { array_literal(text).unwrap() }

    /// The SQLSTATE of a refusal, or "" for an error that isn't one.
    fn refused(r: Result<String>) -> &'static str {
        match r {
            Err(DataFusionError::External(e)) => e.downcast_ref::<Coded>().map_or("", |c| c.0),
            Err(e) => panic!("an error that is no refusal: {e}"),
            Ok(answer) => panic!("answered {answer}, not refused"),
        }
    }

    fn set(doc: &str, at: &str, value: &str, create: bool) -> Result<String> { edit_doc(doc, &path(at), &Edit::Set { value: parse(value)?, create }) }

    fn insert(doc: &str, at: &str, value: &str, after: bool) -> Result<String> { edit_doc(doc, &path(at), &Edit::Insert { value: parse(value)?, after }) }

    fn delete_at(doc: &str, at: &str) -> Result<String> { edit_doc(doc, &path(at), &Edit::Delete) }

    fn strip_json_text(doc: &str) -> String { strip_json(doc).unwrap() }

    #[test]
    fn set_changes_a_member_or_an_element_as_postgres_does() {
        assert_eq!(set(r#"{"a":1,"b":[1,2]}"#, "{b,1}", "9", true).unwrap(), r#"{"a": 1, "b": [1, 9]}"#);
        assert_eq!(set(r#"{"a":1}"#, "{a}", "2", false).unwrap(), r#"{"a": 2}"#, "an existing member changes whether or not create is set");
        assert_eq!(set(r#"{"a":{"b":{"c":1}}}"#, "{a,b,c}", "true", false).unwrap(), r#"{"a": {"b": {"c": true}}}"#);
    }

    #[test]
    fn set_makes_a_missing_member_only_when_asked() {
        assert_eq!(set(r#"{"a":1}"#, "{c}", r#""x""#, true).unwrap(), r#"{"a": 1, "c": "x"}"#);
        assert_eq!(set(r#"{"a":1}"#, "{c}", r#""x""#, false).unwrap(), r#"{"a": 1}"#);
    }

    #[test]
    fn set_on_an_array_appends_or_prepends_past_its_ends() {
        assert_eq!(set("[1,2]", "{5}", "9", true).unwrap(), "[1, 2, 9]");
        assert_eq!(set("[1,2]", "{5}", "9", false).unwrap(), "[1, 2]", "past the end without create: unchanged");
        assert_eq!(set("[1,2]", "{-5}", "9", true).unwrap(), "[9, 1, 2]");
        assert_eq!(set("[1,2]", "{-1}", "9", false).unwrap(), "[1, 9]", "a negative step counts from the end");
        assert_eq!(set(r#"{"a":[1,2]}"#, "{a,-9}", "9", true).unwrap(), r#"{"a": [9, 1, 2]}"#);
    }

    #[test]
    fn set_on_a_missing_path_changes_nothing() {
        assert_eq!(set("[[1]]", "{5,0}", "9", true).unwrap(), "[[1]]", "a missing step before the last");
        assert_eq!(set(r#"{"a":1}"#, "{a,b}", "2", true).unwrap(), r#"{"a": 1}"#, "a path through a scalar");
        assert_eq!(set(r#"{"a":1}"#, "{}", "2", true).unwrap(), r#"{"a": 1}"#, "an empty path is the document");
        assert_eq!(set(r#"{"a":1}"#, "{x,y}", "2", true).unwrap(), r#"{"a": 1}"#, "a missing member before the last");
    }

    #[test]
    fn set_refuses_what_postgres_refuses() {
        assert_eq!(refused(set(r#"{"a":1}"#, "{NULL}", "2", true)), "22004", "a NULL path element");
        assert_eq!(refused(set(r#"{"a":[1,2]}"#, "{a,x}", "2", true)), "22P02", "a key on an array");
        assert_eq!(refused(set("1", "{a}", "2", true)), "22023", "a path into a scalar");
        assert_eq!(refused(set("{", "{a}", "2", true)), "22P02", "a document that isn't JSON");
        assert_eq!(refused(set("{}", "{a}", "x", true)), "22P02", "a value that isn't JSON");
    }

    #[test]
    fn insert_adds_before_or_after_the_place_named() {
        assert_eq!(insert(r#"{"a":[1,2]}"#, "{a,0}", "0", false).unwrap(), r#"{"a": [0, 1, 2]}"#);
        assert_eq!(insert(r#"{"a":[1,2]}"#, "{a,0}", "0", true).unwrap(), r#"{"a": [1, 0, 2]}"#);
        assert_eq!(insert("[1,2]", "{9}", "0", false).unwrap(), "[1, 2, 0]");
        assert_eq!(insert("[1,2]", "{-1}", "0", true).unwrap(), "[1, 2, 0]");
        assert_eq!(insert("[1,2]", "{-1}", "0", false).unwrap(), "[1, 0, 2]");
        assert_eq!(insert("[1,2]", "{-9}", "0", false).unwrap(), "[0, 1, 2]");
        assert_eq!(insert(r#"{"a":1}"#, "{b}", "2", false).unwrap(), r#"{"a": 1, "b": 2}"#);
        assert_eq!(insert(r#"{"a":1}"#, "{}", "2", false).unwrap(), r#"{"a": 1}"#, "an empty path is the document");
    }

    #[test]
    fn insert_refuses_an_existing_key_and_a_scalar() {
        assert_eq!(refused(insert(r#"{"a":1}"#, "{a}", "2", false)), "22023", "cannot replace existing key");
        assert_eq!(refused(insert("1", "{a}", "2", false)), "22023", "a path into a scalar");
    }

    #[test]
    fn concat_merges_objects_and_joins_arrays_as_postgres_does() {
        assert_eq!(concat(r#"{"a":1}"#, r#"{"b":2}"#).unwrap(), r#"{"a": 1, "b": 2}"#);
        assert_eq!(concat(r#"{"a":{"x":1}}"#, r#"{"a":{"y":2}}"#).unwrap(), r#"{"a": {"y": 2}}"#, "not deep: the right one's member wins");
        assert_eq!(concat("[1]", "[2]").unwrap(), "[1, 2]");
        assert_eq!(concat("[1]", "3").unwrap(), "[1, 3]");
        assert_eq!(concat(r#"{"a":1}"#, "[2]").unwrap(), r#"[{"a": 1}, 2]"#);
        assert_eq!(concat("1", "2").unwrap(), "[1, 2]");
        assert_eq!(concat("{}", "[]").unwrap(), "[{}]");
        assert_eq!(concat("[]", "[]").unwrap(), "[]");
    }

    #[test]
    fn delete_takes_keys_elements_and_indexes() {
        assert_eq!(delete_keys(r#"{"a":1,"b":2}"#, &["a"]).unwrap(), r#"{"b": 2}"#);
        assert_eq!(delete_keys(r#"{"a":1,"b":2}"#, &["a", "b"]).unwrap(), "{}");
        assert_eq!(delete_keys(r#"["a","b","a"]"#, &["a"]).unwrap(), r#"["b"]"#, "every string element equal to the key");
        assert_eq!(delete_keys(r#"["a",null,"a"]"#, &["a"]).unwrap(), "[null]");
        assert_eq!(delete_keys(r#"[1,"1"]"#, &["1"]).unwrap(), "[1]", "a number is no string");
        assert_eq!(delete_index("[1,2,3]", 1).unwrap(), "[1, 3]");
        assert_eq!(delete_index("[1,2,3]", -1).unwrap(), "[1, 2]");
        assert_eq!(delete_index("[1,2,3]", 7).unwrap(), "[1, 2, 3]", "past the end: unchanged");
        assert_eq!(refused(delete_keys("1", &["a"])), "22023", "a scalar");
        assert_eq!(refused(delete_index(r#"{"a":1}"#, 0)), "22023", "an object by index");
    }

    #[test]
    fn delete_path_takes_what_a_path_reaches() {
        assert_eq!(delete_at(r#"{"a":{"b":1,"c":2}}"#, "{a,b}").unwrap(), r#"{"a": {"c": 2}}"#);
        assert_eq!(delete_at(r#"{"a":[1,{"b":2}]}"#, "{a,1,b}").unwrap(), r#"{"a": [1, {}]}"#);
        assert_eq!(delete_at(r#"{"a":1}"#, "{a}").unwrap(), "{}");
        assert_eq!(delete_at("[1,2]", "{-1}").unwrap(), "[1]");
        assert_eq!(delete_at(r#"{"a":1}"#, "{b,c}").unwrap(), r#"{"a": 1}"#, "a missing member: unchanged");
        assert_eq!(delete_at(r#"{"a":1}"#, "{a,b}").unwrap(), r#"{"a": 1}"#, "a path through a scalar: unchanged");
        assert_eq!(delete_at("[1,2]", "{5}").unwrap(), "[1, 2]", "past the end: unchanged");
        assert_eq!(refused(delete_at("[1]", "{a}")), "22P02", "a key on an array");
        assert_eq!(refused(delete_at("1", "{0}")), "22023", "a scalar");
        assert_eq!(refused(delete_at(r#"{"a":[1]}"#, "{NULL}")), "22004", "a NULL path element");
    }

    #[test]
    fn strip_nulls_drops_null_members_in_jsonb_order() {
        assert_eq!(strip_nulls(r#"{"a":null,"b":1}"#).unwrap(), r#"{"b": 1}"#);
        assert_eq!(strip_nulls("[null,{\"a\":null}]").unwrap(), "[null, {}]", "an array's nulls stay");
        assert_eq!(strip_nulls("null").unwrap(), "null");
        assert_eq!(strip_nulls(r#"{"a":{"b":null}}"#).unwrap(), r#"{"a": {}}"#);
        assert_eq!(strip_nulls(r#"{"bb":1,"a":null,"c":2}"#).unwrap(), r#"{"c": 2, "bb": 1}"#, "keys in jsonb's order");
    }

    #[test]
    fn json_strip_nulls_copies_tokens_as_written() {
        assert_eq!(strip_json_text(r#"{"a":null,"b":[null,{"c":null}]}"#), r#"{"b":[null,{}]}"#);
        assert_eq!(strip_json_text(r#"{ "a" : null , "b" : [ 1 , null ] }"#), r#"{"b":[1,null]}"#);
        assert_eq!(strip_json_text("[null]"), "[null]");
        assert_eq!(strip_json_text(r#"{"a":1.50e2,"b" : [ null , { "c" : null } ]}"#), r#"{"a":1.50e2,"b":[null,{}]}"#, "a number as written");
        assert_eq!(strip_json_text(r#"{"a":null,"a":1,"b":"xA"}"#), r#"{"a":1,"b":"xA"}"#, "a duplicate key and an escape kept");
        assert_eq!(strip_json_text("null"), "null");
        assert_eq!(refused(strip_json(r#"{"a":1"#)), "22P02");
        let deep = "[".repeat(10_001) + &"]".repeat(10_001);
        assert_eq!(refused(strip_json(&deep)), "54001", "nested past the limit");
    }

    #[test]
    fn pretty_indents_as_jsonb_pretty_does() {
        assert_eq!(pretty("{}").unwrap(), "{\n}");
        assert_eq!(pretty("[]").unwrap(), "[\n]");
        assert_eq!(pretty("1").unwrap(), "1");
        assert_eq!(pretty(r#""x""#).unwrap(), r#""x""#);
        assert_eq!(pretty("[[], {}]").unwrap(), "[\n    [\n    ],\n    {\n    }\n]");
        assert_eq!(
            pretty(r#"{"a":[1,{"b":2}],"c":{}}"#).unwrap(),
            "{\n    \"a\": [\n        1,\n        {\n            \"b\": 2\n        }\n    ],\n    \"c\": {\n    }\n}"
        );
    }

    #[test]
    fn the_functions_are_named_as_postgres_names_them() {
        let names: Vec<&str> = FUNCTIONS.iter().map(|f| f.name()).collect();
        for name in ["jsonb_set", "jsonb_insert", "jsonb_concat", "jsonb_delete", "jsonb_delete_path", "jsonb_strip_nulls", "json_strip_nulls", "jsonb_pretty"] {
            assert!(names.contains(&name), "{name} is registered");
        }
    }
}
