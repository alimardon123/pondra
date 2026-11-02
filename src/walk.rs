//! Every walk of the SQL tree in the core goes through here, so the tree is walked once per break type and not
//! once per visitor (`tools/per_node_check.py` checks it).
//!
//! sqlparser's `Visit` and `VisitMut` are generic over the visitor, so each walk is compiled again for every
//! visitor type, and every closure or visitor struct is a type of its own: about 9,000 functions, each time.
//! Here a walk hands its visitor over as a `dyn` one, so the tree is compiled once for each break type
//! (`ControlFlow<B>`). A call through `dyn` costs a little per node, which planning doesn't measure. Each hook is
//! forwarded where sqlparser calls it, in sqlparser's order, so a visitor does what it did before. sqlparser's other
//! helpers (`visit_relations_mut`, `visit_statements(_mut)`) join these here the day a module needs one.

use core::ops::ControlFlow;

use datafusion::sql::sqlparser::ast::{
    Expr, ObjectName, Query, Select, Statement, TableFactor, ValueWithSpan, Visit, VisitMut, Visitor, VisitorMut,
};

/// A visitor walked as `Visit` walks a tree: every hook is passed on to the visitor it holds.
pub(crate) struct Dyn<'a, B>(&'a mut dyn Visitor<Break = B>);

impl<B> Visitor for Dyn<'_, B> {
    type Break = B;

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<B> {
        self.0.pre_visit_query(query)
    }

    fn post_visit_query(&mut self, query: &Query) -> ControlFlow<B> {
        self.0.post_visit_query(query)
    }

    fn pre_visit_select(&mut self, select: &Select) -> ControlFlow<B> {
        self.0.pre_visit_select(select)
    }

    fn post_visit_select(&mut self, select: &Select) -> ControlFlow<B> {
        self.0.post_visit_select(select)
    }

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<B> {
        self.0.pre_visit_relation(relation)
    }

    fn post_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<B> {
        self.0.post_visit_relation(relation)
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<B> {
        self.0.pre_visit_table_factor(table_factor)
    }

    fn post_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<B> {
        self.0.post_visit_table_factor(table_factor)
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<B> {
        self.0.pre_visit_expr(expr)
    }

    fn post_visit_expr(&mut self, expr: &Expr) -> ControlFlow<B> {
        self.0.post_visit_expr(expr)
    }

    fn pre_visit_statement(&mut self, statement: &Statement) -> ControlFlow<B> {
        self.0.pre_visit_statement(statement)
    }

    fn post_visit_statement(&mut self, statement: &Statement) -> ControlFlow<B> {
        self.0.post_visit_statement(statement)
    }

    fn pre_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<B> {
        self.0.pre_visit_value(value)
    }

    fn post_visit_value(&mut self, value: &ValueWithSpan) -> ControlFlow<B> {
        self.0.post_visit_value(value)
    }
}

/// A mutable visitor walked as `VisitMut` walks a tree: every hook is passed on to the visitor it holds.
pub(crate) struct DynMut<'a, B>(&'a mut dyn VisitorMut<Break = B>);

impl<B> VisitorMut for DynMut<'_, B> {
    type Break = B;

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<B> {
        self.0.pre_visit_query(query)
    }

    fn post_visit_query(&mut self, query: &mut Query) -> ControlFlow<B> {
        self.0.post_visit_query(query)
    }

    fn pre_visit_select(&mut self, select: &mut Select) -> ControlFlow<B> {
        self.0.pre_visit_select(select)
    }

    fn post_visit_select(&mut self, select: &mut Select) -> ControlFlow<B> {
        self.0.post_visit_select(select)
    }

    fn pre_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<B> {
        self.0.pre_visit_relation(relation)
    }

    fn post_visit_relation(&mut self, relation: &mut ObjectName) -> ControlFlow<B> {
        self.0.post_visit_relation(relation)
    }

    fn pre_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<B> {
        self.0.pre_visit_table_factor(table_factor)
    }

    fn post_visit_table_factor(&mut self, table_factor: &mut TableFactor) -> ControlFlow<B> {
        self.0.post_visit_table_factor(table_factor)
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<B> {
        self.0.pre_visit_expr(expr)
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<B> {
        self.0.post_visit_expr(expr)
    }

    fn pre_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<B> {
        self.0.pre_visit_statement(statement)
    }

    fn post_visit_statement(&mut self, statement: &mut Statement) -> ControlFlow<B> {
        self.0.post_visit_statement(statement)
    }

    fn pre_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<B> {
        self.0.pre_visit_value(value)
    }

    fn post_visit_value(&mut self, value: &mut ValueWithSpan) -> ControlFlow<B> {
        self.0.post_visit_value(value)
    }
}

/// Walks `node` with `v`: the one way a read-only walk of the tree is made.
pub(crate) fn visit<N: Visit, B>(node: &N, v: &mut dyn Visitor<Break = B>) -> ControlFlow<B> {
    Visit::visit(node, &mut Dyn(v))
}

/// Walks `node` with `v`, which may change it: the one way a mutable walk of the tree is made.
pub(crate) fn visit_mut<N: VisitMut, B>(node: &mut N, v: &mut dyn VisitorMut<Break = B>) -> ControlFlow<B> {
    VisitMut::visit(node, &mut DynMut(v))
}

/// The closure of `visit_relations` as a visitor. It is one type for each break type, whatever the closure is.
struct Relations<'a, E>(&'a mut dyn FnMut(&ObjectName) -> ControlFlow<E>);

impl<E> Visitor for Relations<'_, E> {
    type Break = E;

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<E> {
        (self.0)(relation)
    }
}

/// The closure of `visit_expressions` as a visitor (see `Relations`).
struct Expressions<'a, E>(&'a mut dyn FnMut(&Expr) -> ControlFlow<E>);

impl<E> Visitor for Expressions<'_, E> {
    type Break = E;

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<E> {
        (self.0)(expr)
    }
}

/// The closure of `visit_expressions_mut` as a visitor (see `Relations`).
struct ExpressionsMut<'a, E>(&'a mut dyn FnMut(&mut Expr) -> ControlFlow<E>);

impl<E> VisitorMut for ExpressionsMut<'_, E> {
    type Break = E;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<E> {
        (self.0)(expr)
    }
}

/// sqlparser's `visit_relations`: `f` sees each relation (table name) before its children, until it breaks.
pub(crate) fn visit_relations<V, E, F>(v: &V, mut f: F) -> ControlFlow<E>
where
    V: Visit,
    F: FnMut(&ObjectName) -> ControlFlow<E>,
{
    visit(v, &mut Relations(&mut f))
}

/// sqlparser's `visit_expressions`: `f` sees each expression before its children, until it breaks.
pub(crate) fn visit_expressions<V, E, F>(v: &V, mut f: F) -> ControlFlow<E>
where
    V: Visit,
    F: FnMut(&Expr) -> ControlFlow<E>,
{
    visit(v, &mut Expressions(&mut f))
}

/// sqlparser's `visit_expressions_mut`: `f` may change each expression, after its children, until it breaks.
pub(crate) fn visit_expressions_mut<V, E, F>(v: &mut V, mut f: F) -> ControlFlow<E>
where
    V: VisitMut,
    F: FnMut(&mut Expr) -> ControlFlow<E>,
{
    visit_mut(v, &mut ExpressionsMut(&mut f))
}
