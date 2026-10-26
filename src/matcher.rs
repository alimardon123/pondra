//! `MATCH_RECOGNIZE`'s matcher (ADR-073): `pondra_match` and `pondra_matches`, window functions that answer, for each
//! row of a partition (sorted by the query's ORDER BY), the match the row is in, and `pondra_nav`, which reads a
//! measure's value at the rows of a match. `pattern.rs` writes them: the pattern goes in as its text (in the spec, a
//! JSON literal, which `read` reads), and each symbol's DEFINE as a Boolean column (a symbol with none is always true).
//!
//! The pattern runs as a Pike VM: threads in the standard's order of preference (the leftmost start; a greedy
//! repetition takes more and a reluctant one fewer; the first alternative before the next), one thread per state per
//! row. So a partition takes time in proportion to its rows, and the match is the one the standard asks for.

use crate::codes::coded;
use crate::fsum;
use anyhow::{anyhow, bail, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, StringArray, StructArray, UInt32Array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::kernels::numeric::sub;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, PartitionEvaluator, Projection, Signature, Volatility, WindowUDF, WindowUDFImpl};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use serde::Deserialize;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The most partial matches a search holds at once when a DEFINE reads the match so far (the ADR's number): past it the
/// query is refused (54000). A test may use a smaller cap, through `Part`.
const STATE_CAP: usize = 100_000;

/// The spec `pattern.rs` writes: the pattern's text (`read` reads it and names its symbols), the skip rule, and the
/// DEFINEs that read the match so far, with the columns their glue reads (`rows`: the row-only parts; `values`: the values
/// the navigations and aggregates read).
#[derive(Deserialize)]
struct Spec {
    pattern: String,
    skip: Skip,
    /// WITH UNMATCHED ROWS: a row in no match is given once more, as `pondra_matches`' one NULL item.
    #[serde(default)]
    unmatched: bool,
    /// WITHIN: each row is within the interval of its match's first row, by the first ORDER BY key (the time argument).
    #[serde(default)]
    within: bool,
    #[serde(default)]
    defines: Vec<DefineSpec>,
    #[serde(default)]
    rows: usize,
    #[serde(default)]
    values: usize,
}

/// A DEFINE that reads the match so far: its symbol, its glue (SQL over `__mr_r{j}` and `__mr_s{index}`), and its slots.
#[derive(Deserialize)]
struct DefineSpec {
    symbol: usize,
    glue: String,
    slots: Vec<SlotSpec>,
}

/// A slot of a glue, by its index (`__mr_s{index}`): a navigation (the row of `var`'s m-th first or last row, moved by
/// `offset` rows, and the value column it reads), an aggregate over `var`'s rows (a value column, or none for count(*)),
/// or MATCH_NUMBER().
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum SlotSpec {
    Nav { index: usize, var: Option<usize>, first: bool, m: usize, offset: i64, value: usize },
    Agg { index: usize, fun: String, var: Option<usize>, value: Option<usize> },
    MatchNumber { index: usize },
}

impl SlotSpec {
    fn index(&self) -> usize {
        match self {
            SlotSpec::Nav { index, .. } | SlotSpec::Agg { index, .. } | SlotSpec::MatchNumber { index } => *index,
        }
    }

    /// The variable whose rows the slot takes (None: every row of the match).
    fn var(&self) -> Option<usize> {
        match self {
            SlotSpec::Nav { var, .. } | SlotSpec::Agg { var, .. } => *var,
            SlotSpec::MatchNumber { .. } => None,
        }
    }

    /// The value column the slot reads, if any.
    fn value(&self) -> Option<usize> {
        match self {
            SlotSpec::Nav { value, .. } => Some(*value),
            SlotSpec::Agg { value, .. } => *value,
            SlotSpec::MatchNumber { .. } => None,
        }
    }
}

/// A pattern as a program, with its symbols, its skip rule, and its DEFINEs that read the match (by symbol, with their
/// slots by index into `slots`).
#[derive(Debug)]
struct Plan {
    syms: Vec<String>,
    code: Vec<Inst>,
    skip: Skip,
    unmatched: bool,
    within: bool,
    defines: Vec<Define>,
    slots: Vec<SlotSpec>,
    rows: usize,
    values: usize,
}

/// A DEFINE of symbol `symbol` that reads the match: its glue, and the indices of its slots.
#[derive(Debug)]
struct Define {
    symbol: usize,
    glue: String,
    slots: Vec<usize>,
}

/// The plan of a spec, refused by name when it is malformed or too big.
fn compile(text: &str) -> Result<Plan> {
    let spec: Spec = serde_json::from_str(text).map_err(|e| anyhow!("MATCH_RECOGNIZE: the pattern's spec doesn't read: {e}"))?;
    let (pattern, syms) = read(&spec.pattern)?;
    let (defines, slots) = defines_of(&spec, syms.len())?;
    let mut c = Compiler { code: vec![] };
    c.emit(&pattern, false)?;
    c.push(Inst::Match)?;
    Ok(Plan {
        syms,
        code: c.code,
        skip: spec.skip,
        unmatched: spec.unmatched,
        within: spec.within,
        defines,
        slots,
        rows: spec.rows,
        values: spec.values,
    })
}

/// The spec's DEFINEs and slots, checked: the slots are numbered from 0, each once; each DEFINE names one of the pattern's
/// symbols; and each slot reads a symbol and a value the spec has.
fn defines_of(spec: &Spec, syms: usize) -> Result<(Vec<Define>, Vec<SlotSpec>)> {
    let total: usize = spec.defines.iter().map(|d| d.slots.len()).sum();
    let mut table: Vec<Option<SlotSpec>> = vec![None; total];
    let mut defines = vec![];
    for d in &spec.defines {
        if d.symbol >= syms {
            bail!("MATCH_RECOGNIZE: a DEFINE of the spec names no symbol of its pattern");
        }
        let mut ids = vec![];
        for s in &d.slots {
            let index = s.index();
            let cell = table.get_mut(index).context("MATCH_RECOGNIZE: the spec's slots don't number from 0")?;
            if cell.is_some() {
                bail!("MATCH_RECOGNIZE: the spec's slot {index} is given twice");
            }
            *cell = Some(s.clone());
            ids.push(index);
        }
        defines.push(Define { symbol: d.symbol, glue: d.glue.clone(), slots: ids });
    }
    let slots: Vec<SlotSpec> = table.into_iter().collect::<Option<Vec<SlotSpec>>>().context("MATCH_RECOGNIZE: the spec's slots don't number from 0")?;
    for s in &slots {
        if s.var().is_some_and(|v| v >= syms) || s.value().is_some_and(|v| v >= spec.values) {
            bail!("MATCH_RECOGNIZE: a slot of the spec reads a symbol or a value the spec doesn't have");
        }
    }
    Ok((defines, slots))
}

/// The states a pattern may compile to. A count in `{n,m}` multiplies its body, so past this a pattern is refused by
/// name rather than run slowly.
const STATES: usize = 10_000;

/// AFTER MATCH SKIP: where the search for the next match goes on. A symbol is named, as the pattern names it.
#[derive(Debug, Deserialize)]
#[serde(tag = "to", rename_all = "snake_case")]
enum Skip {
    PastLast,
    NextRow,
    First { symbol: String },
    Last { symbol: String },
}

/// A pattern as `read` reads it: a symbol is its index in the symbols `read` returns, and `^` and `$` are the anchors.
#[derive(Debug, PartialEq)]
pub enum Pattern {
    Sym(usize),
    Start,
    End,
    Exclude(Box<Pattern>),
    Seq(Vec<Pattern>),
    Alt(Vec<Pattern>),
    Permute(Vec<Pattern>),
    /// Repeated `min` to `max` times (no bound for None), greedy unless reluctant.
    Rep { body: Box<Pattern>, min: u32, max: Option<u32>, greedy: bool },
}

/// One instruction of the program a pattern compiles to.
#[derive(Debug, Clone, Copy)]
enum Inst {
    /// The row is this symbol; `true` inside `{- … -}`, where the row is in the match but not in the output.
    Sym(usize, bool),
    /// Go on at the first target, or else at the second.
    Split(usize, usize),
    Jmp(usize),
    /// `^`: holds at the partition's first row only.
    Start,
    /// `$`: holds at the partition's end only.
    End,
    Match,
}

/// Writes a pattern's program: a sequence is its parts in order, and a choice is a Split that prefers its first branch.
struct Compiler {
    code: Vec<Inst>,
}

impl Compiler {
    /// Appends an instruction, and returns where it is. Past the state limit the pattern is refused (SQLSTATE 54000).
    fn push(&mut self, i: Inst) -> Result<usize> {
        if self.code.len() >= STATES {
            return Err(coded("54000", "MATCH_RECOGNIZE: the pattern compiles to more than 10,000 states: use smaller counts in {n,m}"));
        }
        self.code.push(i);
        Ok(self.code.len() - 1)
    }

    fn emit(&mut self, n: &Pattern, excluded: bool) -> Result<()> {
        match n {
            Pattern::Sym(k) => {
                self.push(Inst::Sym(*k, excluded))?;
            }
            Pattern::Start => {
                self.push(Inst::Start)?;
            }
            Pattern::End => {
                self.push(Inst::End)?;
            }
            Pattern::Exclude(x) => self.emit(x, true)?,
            Pattern::Seq(v) => {
                for x in v {
                    self.emit(x, excluded)?;
                }
            }
            Pattern::Alt(v) => {
                let alts: Vec<Vec<&Pattern>> = v.iter().map(|x| vec![x]).collect();
                self.alternatives(&alts, excluded)?;
            }
            Pattern::Permute(v) => self.permute(v, excluded)?,
            Pattern::Rep { body, min, max, greedy } => self.repeat(body, *min, *max, *greedy, excluded)?,
        }
        Ok(())
    }

    /// Each alternative in order, the first preferred: a Split before each but the last, and a Jmp to the end after
    /// each but the last.
    fn alternatives(&mut self, alts: &[Vec<&Pattern>], excluded: bool) -> Result<()> {
        let mut jumps = vec![];
        for (k, alt) in alts.iter().enumerate() {
            let split = match k + 1 < alts.len() {
                true => Some(self.push(Inst::Split(0, 0))?),
                false => None,
            };
            for n in alt {
                self.emit(n, excluded)?;
            }
            if let Some(s) = split {
                jumps.push(self.push(Inst::Jmp(0))?);
                let next = self.code.len();
                self.code[s] = Inst::Split(s + 1, next);
            }
        }
        let end = self.code.len();
        for j in jumps {
            self.code[j] = Inst::Jmp(end);
        }
        Ok(())
    }

    /// PERMUTE: the alternation of every order of its items, in the standard's order (lexicographic by position).
    fn permute(&mut self, v: &[Pattern], excluded: bool) -> Result<()> {
        let orders: u64 = (1..=v.len() as u64).product();
        if orders > STATES as u64 {
            return Err(coded("54000", format!("MATCH_RECOGNIZE: PERMUTE of {} items is {orders} orders: the pattern compiles to more than 10,000 states: use fewer items", v.len())));
        }
        let mut order: Vec<usize> = (0..v.len()).collect();
        let mut alts = vec![];
        loop {
            alts.push(order.iter().map(|&i| &v[i]).collect::<Vec<&Pattern>>());
            if !next_order(&mut order) {
                break;
            }
        }
        self.alternatives(&alts, excluded)
    }

    /// A repetition: its minimum copies, then either a loop (no maximum) or a nest of optional copies that all leave
    /// together. Greedy takes a copy first; reluctant leaves first.
    fn repeat(&mut self, body: &Pattern, min: u32, max: Option<u32>, greedy: bool, excluded: bool) -> Result<()> {
        for _ in 0..min {
            self.emit(body, excluded)?;
        }
        match max {
            None => {
                let top = self.push(Inst::Split(0, 0))?;
                self.emit(body, excluded)?;
                self.push(Inst::Jmp(top))?;
                let exit = self.code.len();
                self.code[top] = choice(top + 1, exit, greedy);
            }
            Some(m) => {
                if m < min {
                    return Err(coded("42601", format!("MATCH_RECOGNIZE: a repetition of at most {m} after at least {min}")));
                }
                let mut opts = vec![];
                for _ in min..m {
                    opts.push(self.push(Inst::Split(0, 0))?);
                    self.emit(body, excluded)?;
                }
                let exit = self.code.len();
                for s in opts {
                    self.code[s] = choice(s + 1, exit, greedy);
                }
            }
        }
        Ok(())
    }
}

/// A Split that goes to `body` first when greedy, and to `exit` first when not.
fn choice(body: usize, exit: usize, greedy: bool) -> Inst {
    match greedy {
        true => Inst::Split(body, exit),
        false => Inst::Split(exit, body),
    }
}

/// The next order of positions after `v` in lexicographic order, or false when `v` was the last one.
fn next_order(v: &mut [usize]) -> bool {
    let Some(i) = (1..v.len()).rev().find(|&i| v[i - 1] < v[i]) else { return false };
    let j = (i..v.len()).rev().find(|&j| v[j] > v[i - 1]).unwrap_or(i);
    v.swap(i - 1, j);
    v[i..].reverse();
    true
}

/// A thread's path: the steps that led to its state, newest first. Threads that came the same way share it, so a step
/// costs one allocation.
struct Step {
    sym: usize,
    excluded: bool,
    prev: Path,
}

type Path = Option<Rc<Step>>;

/// A thread's state: one entry per slot of the spec, in its order (`Thread::state`).
type State = Vec<Acc>;

/// A thread: a state, the row its match started at, its path, and the state of its slots (empty when no DEFINE reads the
/// match, which keeps the search on its stamps).
#[derive(Clone)]
struct Thread {
    pc: usize,
    start: usize,
    path: Path,
    state: Rc<State>,
}

/// What a slot holds of the match so far. A navigation keeps the rows it reads (so equal values on different rows stay
/// apart); a count, a sum, an average and a min or max keep their values.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum Acc {
    /// The rows of a navigation: the first m+1 (FIRST) or the last m+1 (LAST), oldest first.
    Rows(VecDeque<usize>),
    Count(i64),
    /// A sum: None until a value is taken (a sum over no value is NULL).
    Sum(Option<Num>),
    /// An average: the f64 sum of its values (bits) and their number.
    Avg(u64, i64),
    /// A min or max: None until a value is taken.
    Extreme(Option<ScalarValue>),
    /// MATCH_NUMBER(), which keeps nothing.
    Unit,
}

/// A sum, in the type it is added in: wrapping integers, a decimal's unscaled value, a DOUBLE as fsum's (hi, lo) pair (bits),
/// or a REAL. Floats are kept by their bits, so two sums are equal when their bits are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Num {
    Int(i64),
    Uint(u64),
    Dec(i128),
    Float(u64, u64),
    Single(u32),
}

/// Adds a value to a sum (`None`: the first value).
fn num_add(acc: Option<Num>, v: &ScalarValue) -> Result<Num> {
    Ok(match (acc, v) {
        (None, ScalarValue::Int64(Some(x))) => Num::Int(*x),
        (Some(Num::Int(s)), ScalarValue::Int64(Some(x))) => Num::Int(s.wrapping_add(*x)),
        (None, ScalarValue::UInt64(Some(x))) => Num::Uint(*x),
        (Some(Num::Uint(s)), ScalarValue::UInt64(Some(x))) => Num::Uint(s.wrapping_add(*x)),
        (None, ScalarValue::Decimal128(Some(x), _, _)) => Num::Dec(*x),
        (Some(Num::Dec(s)), ScalarValue::Decimal128(Some(x), _, _)) => Num::Dec(s.wrapping_add(*x)),
        (None, ScalarValue::Float64(Some(x))) => float_add(0.0, 0.0, *x),
        (Some(Num::Float(h, l)), ScalarValue::Float64(Some(x))) => float_add(f64::from_bits(h), f64::from_bits(l), *x),
        (None, ScalarValue::Float32(Some(x))) => Num::Single(x.to_bits()),
        (Some(Num::Single(s)), ScalarValue::Float32(Some(x))) => Num::Single((f32::from_bits(s) + *x).to_bits()),
        _ => bail!("MATCH_RECOGNIZE: a sum of {v} doesn't add to the sum so far"),
    })
}

/// fsum's two-sum step, from the sum `hi` and its rounding error `lo`, as a sum's bits.
fn float_add(hi: f64, lo: f64, x: f64) -> Num {
    let (mut hi, mut lo) = (hi, lo);
    fsum::add(&mut hi, &mut lo, x);
    Num::Float(hi.to_bits(), lo.to_bits())
}

impl Num {
    /// The sum as a value of its result type (a decimal keeps the result's precision and scale).
    fn scalar(self, result: &DataType) -> Result<ScalarValue> {
        Ok(match (self, result) {
            (Num::Int(v), _) => ScalarValue::Int64(Some(v)),
            (Num::Uint(v), _) => ScalarValue::UInt64(Some(v)),
            (Num::Dec(v), DataType::Decimal128(p, s)) => ScalarValue::Decimal128(Some(v), *p, *s),
            (Num::Float(h, l), _) => ScalarValue::Float64(Some(fsum::total(f64::from_bits(h), f64::from_bits(l)))),
            (Num::Single(b), _) => ScalarValue::Float32(Some(f32::from_bits(b))),
            _ => bail!("MATCH_RECOGNIZE: a sum's result is {result}, which its value isn't"),
        })
    }
}

/// A slot's types as planned: its result's type, and the type its value is cast to first (a sum's or an average's
/// coerced argument; a value's own type otherwise).
#[derive(Clone, Debug)]
struct Typed {
    arg: DataType,
    result: DataType,
}

impl Typed {
    fn same(t: DataType) -> Typed {
        Typed { arg: t.clone(), result: t }
    }
}

/// A DEFINE's glue as planned: the schema it reads (`__mr_r{j}` for each row-only part, then its slots' columns), its
/// physical expression, and its slots' indices.
#[derive(Debug)]
struct Glued {
    schema: SchemaRef,
    slots: Vec<usize>,
    expr: Arc<dyn PhysicalExpr>,
}

/// A spec's glue as planned against the engine's functions: each DEFINE's glue (by its position), the DEFINE of each
/// symbol, and each slot's types.
#[derive(Debug)]
struct Planned {
    glued: Vec<Glued>,
    by_symbol: Vec<Option<usize>>,
    types: Vec<Typed>,
}

/// One partition as the search reads it: each symbol's row-only conditions, WITHIN's times, the glue's row-only columns and
/// value columns, the planned glue (when a DEFINE reads the match), and the cap on the partial matches a search holds.
struct Part<'a> {
    cond: &'a [Vec<bool>],
    within: Option<&'a Within>,
    n: usize,
    rows: &'a [ArrayRef],
    values: &'a [ArrayRef],
    planned: Option<&'a Planned>,
    cap: usize,
}

/// A thread that took row `i`, waiting for its glue (`check`: the glue's index and its place in that glue's batch) when it
/// has one. Its path and state are as they stand after the row.
struct Decision {
    pc: usize,
    start: usize,
    path: Path,
    state: Rc<State>,
    check: Option<(usize, usize)>,
}

/// The partial matches one generation has reached: by (state, start when WITHIN) for a state-carrying pattern, by
/// (state, start) for WITHIN without one.
#[derive(Default)]
struct Reached {
    pairs: HashSet<(usize, usize)>,
    keys: HashSet<(usize, usize, Rc<State>)>,
}

/// Which thread reaches a state first, in one generation of threads. Without WITHIN and without a DEFINE that reads the
/// match, a state holds one thread (its stamp is the generation). With WITHIN two starts' threads at a state have different
/// futures, so a state holds one thread per start. A DEFINE that reads the match puts the state itself in the key.
enum Seen<'a> {
    Stamp(&'a mut [usize], usize),
    Start(&'a mut HashSet<(usize, usize)>),
    Keys(&'a mut HashSet<(usize, usize, Rc<State>)>, bool),
}

impl<'a> Seen<'a> {
    fn of(w: Option<&Within>, stated: bool, stamp: &'a mut [usize], gen: usize, reached: &'a mut Reached) -> Seen<'a> {
        match (stated, w.is_some()) {
            (true, within) => Seen::Keys(&mut reached.keys, within),
            (false, true) => Seen::Start(&mut reached.pairs),
            (false, false) => Seen::Stamp(stamp, gen),
        }
    }

    /// Whether a thread that started at `start` in `state` is the first of this generation to reach `pc`.
    fn first(&mut self, pc: usize, start: usize, state: &Rc<State>) -> bool {
        match self {
            Seen::Stamp(stamp, gen) => {
                let fresh = stamp[pc] != *gen;
                stamp[pc] = *gen;
                fresh
            }
            Seen::Start(pairs) => pairs.insert((pc, start)),
            Seen::Keys(keys, within) => keys.insert((pc, if *within { start } else { 0 }, state.clone())),
        }
    }
}

/// Adds the threads a state reaches at a row (its epsilon closure) to `out`, in priority order (a Split's first target
/// first). A state holds the first thread to reach it, which is the one preferred (see `Seen`).
#[allow(clippy::too_many_arguments)]
fn close(code: &[Inst], out: &mut Vec<Thread>, seen: &mut Seen<'_>, pc: usize, start: usize, path: Path, state: &Rc<State>, at: usize, n: usize) {
    let mut todo = vec![pc];
    while let Some(pc) = todo.pop() {
        if !seen.first(pc, start, state) {
            continue;
        }
        match code[pc] {
            Inst::Jmp(x) => todo.push(x),
            Inst::Split(x, y) => {
                todo.push(y);
                todo.push(x);
            }
            Inst::Start => {
                if at == 0 {
                    todo.push(pc + 1);
                }
            }
            Inst::End => {
                if at == n {
                    todo.push(pc + 1);
                }
            }
            Inst::Sym(..) | Inst::Match => out.push(Thread { pc, start, path: path.clone(), state: state.clone() }),
        }
    }
}

/// The state of a thread that starts at a row: nothing taken yet.
fn start_state(plan: &Plan) -> State {
    plan.slots
        .iter()
        .map(|slot| match slot {
            SlotSpec::Nav { .. } => Acc::Rows(VecDeque::new()),
            SlotSpec::Agg { fun, .. } => match fun.as_str() {
                "count" => Acc::Count(0),
                "sum" => Acc::Sum(None),
                "avg" => Acc::Avg(0f64.to_bits(), 0),
                _ => Acc::Extreme(None),
            },
            SlotSpec::MatchNumber { .. } => Acc::Unit,
        })
        .collect()
}

/// The state once row `i` is taken as symbol `k`: each slot of that symbol, or of every row, takes the row.
fn take(state: &State, plan: &Plan, part: &Part, k: usize, i: usize) -> Result<State> {
    let types = &part.planned.context("MATCH_RECOGNIZE: a DEFINE has no glue planned")?.types;
    let mut out = state.clone();
    for (s, slot) in plan.slots.iter().enumerate() {
        if matches!(slot, SlotSpec::MatchNumber { .. }) || slot.var().is_some_and(|v| v != k) {
            continue;
        }
        out[s].take_row(slot, &types[s], i, part)?;
    }
    Ok(out)
}

/// The most preferred match that starts at or after `from`: the Pike VM's leftmost answer. A row's threads run in
/// priority order; a match cuts the threads after it, and a higher-priority thread that matches later replaces it.
/// No new thread starts once a match is found.
/// `stamp` and `last` are the partition's, so a search costs its rows, not a clear of every state. With WITHIN a thread that
/// would take a row too late dies (`within_ok`), and the threads of each start are kept apart (`Seen`).
/// A row is taken in three phases: each thread at a symbol takes the row and decides its row-only conditions (phase 1);
/// each glue is evaluated once over its batch (phase 2); and the passing threads go on, in the order they came (phase 3).
/// `m` is MATCH_NUMBER(): the matches so far, plus one.
fn search(plan: &Plan, part: &Part, from: usize, stamp: &mut [usize], last: &mut usize, m: i64) -> Result<Option<Found>> {
    let code = &plan.code;
    let (n, w) = (part.n, part.within);
    let stated = !plan.defines.is_empty();
    let cap = if stated { part.cap } else { MAX_THREADS };
    let initial: Rc<State> = Rc::new(start_state(plan));
    let glued = part.planned.map_or(0, |p| p.glued.len());
    let mut best: Option<Found> = None;
    let mut now: Vec<Thread> = vec![];
    let mut now_reached = Reached::default();
    let mut gen = *last + 1;
    let mut i = from;
    loop {
        // (a new thread starts at this row last in line, so it is the least preferred)
        if best.is_none() && i < n {
            let mut seen = Seen::of(w, stated, stamp, gen, &mut now_reached);
            close(code, &mut now, &mut seen, 0, i, None, &initial, i, n);
        }
        // phase 1: the threads at a symbol, in order, up to the first that matches
        let mut decisions: Vec<Decision> = vec![];
        let mut batch: Vec<Vec<usize>> = vec![vec![]; glued];
        for t in &now {
            match code[t.pc] {
                Inst::Match => {
                    best = Some(Found::of(t, i));
                    break;
                }
                Inst::Sym(k, excluded) => {
                    if i < n && part.cond[k][i] && within_ok(w, t.start, i) {
                        let state = match stated {
                            true => Rc::new(take(&t.state, plan, part, k, i)?),
                            false => t.state.clone(),
                        };
                        let step = Rc::new(Step { sym: k, excluded, prev: t.path.clone() });
                        let waits = match part.planned.and_then(|p| p.by_symbol[k]) {
                            Some(g) => {
                                batch[g].push(decisions.len());
                                Some((g, batch[g].len() - 1))
                            }
                            None => None,
                        };
                        decisions.push(Decision { pc: t.pc + 1, start: t.start, path: Some(step), state, check: waits });
                    }
                }
                _ => {}
            }
        }
        // phase 2: each glue once over its batch, at this row
        let mut passed: Vec<Vec<bool>> = vec![];
        if let Some(p) = part.planned {
            for (g, members) in batch.iter().enumerate() {
                let states: Vec<&State> = members.iter().map(|&d| &*decisions[d].state).collect();
                let out = match states.is_empty() {
                    true => vec![],
                    false => check(part, plan, &p.glued[g], &p.types, &states, i, m)?,
                };
                passed.push(out);
            }
        }
        // phase 3: the passing threads go on, in their order
        let gen_next = gen + 1;
        let mut next: Vec<Thread> = vec![];
        let mut next_reached = Reached::default();
        {
            let mut seen = Seen::of(w, stated, stamp, gen_next, &mut next_reached);
            for d in &decisions {
                let pass = match d.check {
                    Some((g, pos)) => passed[g][pos],
                    None => true,
                };
                if pass {
                    close(code, &mut next, &mut seen, d.pc, d.start, d.path.clone(), &d.state, i + 1, n);
                }
            }
        }
        if next.len() > cap {
            return Err(cap_error(stated, w.is_some(), cap));
        }
        if i == n || (best.is_some() && next.is_empty()) {
            break;
        }
        now = next;
        now_reached = next_reached;
        gen = gen_next;
        i += 1;
    }
    *last = gen + 1;
    Ok(best)
}

/// The refusal of a search past its cap. A pattern with no DEFINE that reads the match says what it said before; one that
/// does says to narrow the conditions, and to narrow WITHIN too when there is one.
fn cap_error(stated: bool, within: bool, cap: usize) -> anyhow::Error {
    let msg = match (stated, within) {
        (false, _) => format!("MATCH_RECOGNIZE: the search holds more than {cap} partial matches at once: narrow WITHIN or the conditions"),
        (true, true) => format!("MATCH_RECOGNIZE: the search holds more than {cap} partial matches at once: narrow WITHIN or the conditions"),
        (true, false) => format!("MATCH_RECOGNIZE: the search holds more than {cap} partial matches at once: make the conditions narrower"),
    };
    coded("54000", msg)
}

/// The glue's answer for each thread of its batch at row `i` (NULL is false): the row-only columns of row `i`, broadcast, and
/// each slot's value in each thread's state, as the glue's columns.
fn check(part: &Part, plan: &Plan, glued: &Glued, types: &[Typed], states: &[&State], i: usize, m: i64) -> Result<Vec<bool>> {
    let len = states.len();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(part.rows.len() + glued.slots.len());
    for column in part.rows {
        columns.push(ScalarValue::try_from_array(column.as_ref(), i)?.to_array_of_size(len)?);
    }
    for &s in &glued.slots {
        let values = states
            .iter()
            .map(|st| st[s].scalar(&plan.slots[s], &types[s], part, m))
            .collect::<Result<Vec<ScalarValue>>>()?;
        columns.push(ScalarValue::iter_to_array(values)?);
    }
    let batch = RecordBatch::try_new(glued.schema.clone(), columns)?;
    let out = glued.expr.evaluate(&batch)?.into_array(len)?;
    bools(&out)
}

/// The partition's matches, as each row's memberships (see `search`, with `partition`'s skip rule between them).
/// Matches go on from the skip rule's place; an empty match goes on one row later.
fn partition(plan: &Plan, part: &Part) -> Result<Vec<Vec<Mem>>> {
    let n = part.n;
    let mut rows: Vec<Vec<Mem>> = vec![Vec::new(); n];
    let (mut from, mut m) = (0usize, 0i64);
    let (mut stamp, mut gen) = (vec![0usize; plan.code.len()], 0usize);
    while from < n {
        // (MATCH_NUMBER is the matches so far, plus one: the one this search finds)
        let Some(f) = search(plan, part, from, &mut stamp, &mut gen, m + 1)? else { break };
        m += 1;
        if f.start == f.end {
            rows[f.start].push(Mem { m, sym: None, seq: None, excluded: false });
            from = f.start + 1;
        } else {
            for (j, &(sym, excluded)) in f.steps.iter().enumerate() {
                rows[f.start + j].push(Mem { m, sym: Some(sym), seq: Some(j as i64 + 1), excluded });
            }
            from = skip(plan, &f, m)?;
        }
    }
    Ok(rows)
}

/// A match: its rows `start..end`, and each row's symbol and excluded flag in order.
struct Found {
    start: usize,
    end: usize,
    steps: Vec<(usize, bool)>,
}

impl Found {
    fn of(t: &Thread, end: usize) -> Found {
        let mut steps = vec![];
        let mut at = t.path.clone();
        while let Some(s) = at {
            steps.push((s.sym, s.excluded));
            at = s.prev.clone();
        }
        steps.reverse();
        Found { start: t.start, end, steps }
    }
}

/// The most threads a search may hold at once: past it the search is refused (54000), as WITHIN keeps one per start.
const MAX_THREADS: usize = 1_000_000;

/// WITHIN's times of one partition, in ticks of the time's unit (a timestamp's count from the epoch), and each row's bound:
/// its time less the interval, so a row can be in a match only when the match's first row is no earlier than the bound.
/// None is NULL.
struct Within {
    time: Vec<Option<i64>>,
    low: Vec<Option<i64>>,
}

/// Whether row `i` may be taken by a thread that started at `start`: its time is within WITHIN's interval of the start's
/// time. A NULL time is never within.
fn within_ok(w: Option<&Within>, start: usize, i: usize) -> bool {
    match w {
        None => true,
        Some(w) => matches!((w.time[start], w.low[i]), (Some(t), Some(low)) if t >= low),
    }
}

/// A row's membership in one match: the match's number, the row's symbol (None for the row of an empty match), its
/// place in the match, and whether `{- -}` leaves it out of the output.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Mem {
    m: i64,
    sym: Option<usize>,
    seq: Option<i64>,
    excluded: bool,
}

/// Where the search goes on after a match, by the skip rule (AFTER MATCH SKIP). `TO FIRST v` and `TO LAST v` go to
/// v's first or last row of the match, which must be a row after the match's first one, or the rule can't be kept.
fn skip(plan: &Plan, f: &Found, m: i64) -> Result<usize> {
    let (name, first) = match &plan.skip {
        Skip::PastLast => return Ok(f.end),
        Skip::NextRow => return Ok(f.start + 1),
        Skip::First { symbol } => (symbol, true),
        Skip::Last { symbol } => (symbol, false),
    };
    let sym = plan.syms.iter().position(|s| s == name).context("MATCH_RECOGNIZE: AFTER MATCH SKIP names no symbol of PATTERN")?;
    let way = if first { "FIRST" } else { "LAST" };
    let rows: Vec<usize> = f.steps.iter().enumerate().filter(|(_, s)| s.0 == sym).map(|(j, _)| f.start + j).collect();
    let Some(target) = (if first { rows.first() } else { rows.last() }).copied() else {
        return Err(coded("22000", format!("MATCH_RECOGNIZE: AFTER MATCH SKIP TO {way} {name}: {name} has no row in match {m}")));
    };
    if target == f.start {
        return Err(coded("22000", format!("MATCH_RECOGNIZE: AFTER MATCH SKIP TO {way} {name} goes back to where match {m} started: skip to a later symbol, or PAST LAST ROW")));
    }
    Ok(target)
}

/// The struct a membership is, of the four fields `pattern.rs` reads.
fn fields() -> Fields {
    Fields::from(vec![
        Field::new("match", DataType::Int64, true),
        Field::new("symbol", DataType::Utf8, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("excluded", DataType::Boolean, true),
    ])
}

fn struct_type() -> DataType { DataType::Struct(fields()) }

/// Memberships as a struct column, NULL for an item that is None (a row in no match).
fn structs(items: &[Option<Mem>], syms: &[String]) -> Result<StructArray> {
    let valid = NullBuffer::from(items.iter().map(Option::is_some).collect::<Vec<bool>>());
    let m: Int64Array = items.iter().map(|x| x.map(|x| x.m)).collect();
    let sym: StringArray = items.iter().map(|x| x.and_then(|x| x.sym).map(|k| syms[k].as_str())).collect();
    let seq: Int64Array = items.iter().map(|x| x.and_then(|x| x.seq)).collect();
    let excluded: BooleanArray = items.iter().map(|x| x.map(|x| x.excluded)).collect();
    let arrays: Vec<ArrayRef> = vec![Arc::new(m), Arc::new(sym), Arc::new(seq), Arc::new(excluded)];
    Ok(StructArray::try_new(fields(), arrays, Some(valid))?)
}

/// The answer for each row: its first membership (`pondra_match`), or all of them (`pondra_matches`, a list per row). With
/// `unmatched`, a row in no match gives a list of one NULL item, so `unnest` keeps the row with its fields NULL.
fn answer(rows: &[Vec<Mem>], syms: &[String], all: bool, unmatched: bool) -> Result<ArrayRef> {
    if !all {
        let first: Vec<Option<Mem>> = rows.iter().map(|r| r.first().copied()).collect();
        return Ok(Arc::new(structs(&first, syms)?) as ArrayRef);
    }
    let mut flat: Vec<Option<Mem>> = vec![];
    let mut lens = vec![];
    for r in rows {
        let items: Vec<Option<Mem>> = match r.is_empty() && unmatched {
            true => vec![None],
            false => r.iter().map(|m| Some(*m)).collect(),
        };
        lens.push(items.len());
        flat.extend(items);
    }
    let item = Arc::new(Field::new("item", struct_type(), true));
    let values: ArrayRef = Arc::new(structs(&flat, syms)?);
    let lists = ListArray::try_new(item, OffsetBuffer::from_lengths(lens), values, None)?;
    Ok(Arc::new(lists) as ArrayRef)
}

/// A condition column as its rows' booleans; NULL is false.
fn bools(column: &ArrayRef) -> Result<Vec<bool>> {
    let b = column.as_any().downcast_ref::<BooleanArray>().context("MATCH_RECOGNIZE: a DEFINE is a condition")?;
    Ok(b.iter().map(|v| v == Some(true)).collect())
}

/// The spec's text, from the first row of its column (for a call whose spec is not a literal).
fn spec_text(column: &ArrayRef) -> Result<String> {
    let text = cast(column, &DataType::Utf8)?;
    let text = text.as_any().downcast_ref::<StringArray>().context("MATCH_RECOGNIZE: the spec is text")?;
    Ok(text.value(0).to_string())
}

/// WITHIN reads the first ORDER BY as a time: a date or a timestamp, else 42804.
fn check_time_type(t: &DataType) -> Result<()> {
    match t {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => Ok(()),
        other => Err(coded(
            "42804",
            format!("MATCH_RECOGNIZE: WITHIN reads the first ORDER BY as a time, and {other} isn't one: order by a date or a timestamp"),
        )),
    }
}

/// WITHIN's times and bounds for a partition (see `Within`), from its time column and its interval column, which holds the
/// interval in each row. A timestamp keeps its own unit and zone, since a zone's days and months are its own (a DST change
/// moves a difference by an hour otherwise); a date becomes a timestamp of microseconds. The subtraction is arrow's, so an
/// interval of months stays exact.
fn within_of(time: &ArrayRef, interval: &ArrayRef) -> Result<Within> {
    check_time_type(time.data_type())?;
    let time: ArrayRef = match time.data_type() {
        DataType::Date32 | DataType::Date64 => cast(time, &DataType::Timestamp(TimeUnit::Microsecond, None))?,
        _ => time.clone(),
    };
    let low = sub(&time.as_ref(), &interval.as_ref())?;
    Ok(Within { time: ticks(&time)?, low: ticks(&low)? })
}

/// A column of whole numbers as its values (a timestamp's ticks, of its own unit); NULL stays None.
fn ticks(column: &ArrayRef) -> Result<Vec<Option<i64>>> {
    let ints = cast(column, &DataType::Int64)?;
    let ints = ints.as_any().downcast_ref::<Int64Array>().context("MATCH_RECOGNIZE: WITHIN's times are whole numbers")?;
    Ok(ints.iter().collect())
}

/// A window's error keeps its SQLSTATE (SKIP TO's 22000, a state limit's 54000): DataFusion would wrap it as XX000 otherwise.
fn external(e: anyhow::Error) -> DataFusionError {
    match e.downcast::<crate::codes::Coded>() {
        Ok(c) => DataFusionError::External(Box::new(c)),
        Err(e) => DataFusionError::External(e.into()),
    }
}

impl Acc {
    /// Takes row `i` into the slot: a navigation keeps the row, a count counts it (when its value is not NULL), a sum or an
    /// average adds its value, and a min or max keeps the smaller or larger. A NULL value is not taken by an aggregate.
    fn take_row(&mut self, slot: &SlotSpec, types: &Typed, i: usize, part: &Part) -> Result<()> {
        match (slot, self) {
            (SlotSpec::Nav { first, m, .. }, Acc::Rows(rows)) => {
                let keep = *m + 1;
                if *first {
                    if rows.len() < keep {
                        rows.push_back(i);
                    }
                } else {
                    rows.push_back(i);
                    if rows.len() > keep {
                        rows.pop_front();
                    }
                }
            }
            (SlotSpec::Agg { fun, value, .. }, acc) => match (fun.as_str(), acc) {
                ("count", Acc::Count(c)) => {
                    let counts = match *value {
                        None => true,
                        Some(j) => part.values.get(j).context("MATCH_RECOGNIZE: a value is missing")?.is_valid(i),
                    };
                    if counts {
                        *c += 1;
                    }
                }
                ("sum", Acc::Sum(total)) => {
                    if let Some(v) = value_at(part, *value, i, &types.arg)? {
                        *total = Some(num_add(*total, &v)?);
                    }
                }
                ("avg", Acc::Avg(sum, n)) => {
                    if let Some(ScalarValue::Float64(Some(x))) = value_at(part, *value, i, &DataType::Float64)? {
                        *sum = (f64::from_bits(*sum) + x).to_bits();
                        *n += 1;
                    }
                }
                (name @ ("min" | "max"), Acc::Extreme(best)) => {
                    if let Some(v) = value_at(part, *value, i, &types.arg)? {
                        let better = match best.as_ref() {
                            None => true,
                            Some(b) => match order(&v, b) {
                                Ordering::Less => name == "min",
                                Ordering::Greater => name == "max",
                                Ordering::Equal => false,
                            },
                        };
                        if better {
                            *best = Some(v);
                        }
                    }
                }
                _ => bail!("MATCH_RECOGNIZE: a DEFINE's state doesn't match its slot"),
            },
            (SlotSpec::MatchNumber { .. }, _) => {}
            _ => bail!("MATCH_RECOGNIZE: a DEFINE's state doesn't match its slot"),
        }
        Ok(())
    }

    /// The slot's value now, as a value of its result type: a navigation reads its value column at the row it names, moved by
    /// its offset (NULL outside the partition or with no such row); an aggregate is its state's value; MATCH_NUMBER is `m`.
    fn scalar(&self, slot: &SlotSpec, types: &Typed, part: &Part, m: i64) -> Result<ScalarValue> {
        let null = ScalarValue::try_from(&types.result)?;
        Ok(match (slot, self) {
            (SlotSpec::MatchNumber { .. }, _) => ScalarValue::Int64(Some(m)),
            (SlotSpec::Nav { first, m: want, offset, value, .. }, Acc::Rows(rows)) => {
                let row = match rows.len() == *want + 1 {
                    false => None,
                    true => match *first {
                        true => rows.get(*want).copied(),
                        false => rows.front().copied(),
                    },
                };
                match row.map(|r| r as i64 + *offset) {
                    Some(p) if p >= 0 && (p as usize) < part.n => {
                        let column = part.values.get(*value).context("MATCH_RECOGNIZE: a value is missing")?;
                        ScalarValue::try_from_array(column.as_ref(), p as usize)?
                    }
                    _ => null,
                }
            }
            (_, Acc::Count(c)) => ScalarValue::Int64(Some(*c)),
            (_, Acc::Sum(None)) => null,
            (_, Acc::Sum(Some(x))) => x.scalar(&types.result)?,
            (_, Acc::Avg(sum, n)) => ScalarValue::Float64(match *n {
                0 => None,
                n => Some(f64::from_bits(*sum) / n as f64),
            }),
            (_, Acc::Extreme(Some(v))) => v.clone(),
            (_, Acc::Extreme(None)) => null,
            _ => bail!("MATCH_RECOGNIZE: a DEFINE's state doesn't match its slot"),
        })
    }
}

/// The value of value column `value` at row `i`, cast to `to`; None when it is NULL there.
fn value_at(part: &Part, value: Option<usize>, i: usize, to: &DataType) -> Result<Option<ScalarValue>> {
    let j = value.context("MATCH_RECOGNIZE: an aggregate reads a value")?;
    let column = part.values.get(j).context("MATCH_RECOGNIZE: a value is missing")?;
    if !column.is_valid(i) {
        return Ok(None);
    }
    Ok(Some(ScalarValue::try_from_array(column.as_ref(), i)?.cast_to(to)?))
}

/// The order MIN and MAX use: a float's NaN is the largest (as arrow's), and any other value is compared as SQL compares.
fn order(a: &ScalarValue, b: &ScalarValue) -> Ordering {
    match (a, b) {
        (ScalarValue::Float64(Some(x)), ScalarValue::Float64(Some(y))) => float_order(*x, *y),
        (ScalarValue::Float32(Some(x)), ScalarValue::Float32(Some(y))) => float_order(f64::from(*x), f64::from(*y)),
        _ => a.partial_cmp(b).unwrap_or(Ordering::Equal),
    }
}

fn float_order(x: f64, y: f64) -> Ordering {
    match (x.is_nan(), y.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ => x.partial_cmp(&y).unwrap_or(Ordering::Equal),
    }
}

/// The evaluator of one partition. The whole partition is matched at once: a match may look at any of its rows, so
/// the window's frame and a bounded run don't apply.
#[derive(Debug)]
struct Evaluator {
    all: bool,
    planner: Arc<Planner>,
    /// The spec's text, and its plan, once read.
    text: String,
    plan: Option<Plan>,
    /// The spec's glue, planned against the input types, when a DEFINE reads the match.
    planned: Option<Arc<Planned>>,
}

impl PartitionEvaluator for Evaluator {
    fn uses_window_frame(&self) -> bool { false }

    fn supports_bounded_execution(&self) -> bool { false }

    fn evaluate_all(&mut self, values: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
        self.run(values, n).map_err(external)
    }
}

impl Evaluator {
    /// The answer for `n` rows. `values[0]` is the spec, then one condition column per symbol, WITHIN's time and interval when
    /// within, the glue's row-only columns, and its value columns.
    fn run(&mut self, values: &[ArrayRef], n: usize) -> Result<ArrayRef> {
        if n == 0 {
            return answer(&[], &[], self.all, false);
        }
        if self.plan.is_none() {
            let text = spec_text(values.first().context("MATCH_RECOGNIZE: no spec")?)?;
            self.plan = Some(compile(&text)?);
            self.text = text;
        }
        let plan = self.plan.as_ref().context("MATCH_RECOGNIZE: no plan")?;
        if !plan.defines.is_empty() && self.planned.is_none() {
            let types: Vec<DataType> = values.iter().map(|v| v.data_type().clone()).collect();
            self.planned = Some(self.planner.planned(&self.text, plan, &types)?);
        }
        let mut cond = vec![];
        for k in 0..plan.syms.len() {
            cond.push(bools(values.get(k + 1).context("MATCH_RECOGNIZE: a symbol without its condition")?)?);
        }
        // WITHIN's time and interval come after the symbols' conditions
        let within = match plan.within {
            true => {
                let time = values.get(plan.syms.len() + 1).context("MATCH_RECOGNIZE: WITHIN has no time")?;
                let interval = values.get(plan.syms.len() + 2).context("MATCH_RECOGNIZE: WITHIN has no interval")?;
                Some(within_of(time, interval)?)
            }
            false => None,
        };
        // the glue's row-only columns, then its value columns
        let first = 1 + plan.syms.len() + if plan.within { 2 } else { 0 };
        let rows = values.get(first..first + plan.rows).context("MATCH_RECOGNIZE: the glue's row columns are missing")?;
        let columns = values
            .get(first + plan.rows..first + plan.rows + plan.values)
            .context("MATCH_RECOGNIZE: the glue's value columns are missing")?;
        let part = Part {
            cond: &cond,
            within: within.as_ref(),
            n,
            rows,
            values: columns,
            planned: self.planned.as_deref(),
            cap: STATE_CAP,
        };
        answer(&partition(plan, &part)?, &plan.syms, self.all, plan.unmatched)
    }
}

/// Plans the glue of specs against the engine's functions. `base` is the session's snapshot that `register` takes. A plan is
/// kept per spec text and input types, so the partitions of one query plan once.
struct Planner {
    base: Arc<SessionState>,
    cache: Mutex<HashMap<(String, Vec<DataType>), Arc<Planned>>>,
}

impl std::fmt::Debug for Planner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Planner")
    }
}

impl Planner {
    /// The spec's glue planned for these input types (see `plan_glue`), from the cache when it is there.
    fn planned(&self, text: &str, plan: &Plan, types: &[DataType]) -> Result<Arc<Planned>> {
        let key = (text.to_string(), types.to_vec());
        if let Some(p) = lock(&self.cache).get(&key) {
            return Ok(p.clone());
        }
        let p = Arc::new(plan_glue(&self.base, plan, types)?);
        lock(&self.cache).insert(key, p.clone());
        Ok(p)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A spec's glue planned: each DEFINE's columns typed as the call's arguments say, and each slot's types. The call's arguments,
/// in order: the spec, one condition per symbol, WITHIN's time and interval when within, the row-only columns, the values.
/// A glue that names a function the engine lacks, or a slot the engine can't add up, is refused with the DEFINE's name.
fn plan_glue(base: &SessionState, plan: &Plan, types: &[DataType]) -> Result<Planned> {
    let first = 1 + plan.syms.len() + if plan.within { 2 } else { 0 };
    let rows = types.get(first..first + plan.rows).context("MATCH_RECOGNIZE: the glue's row columns are missing")?;
    let values = types
        .get(first + plan.rows..first + plan.rows + plan.values)
        .context("MATCH_RECOGNIZE: the glue's value columns are missing")?;
    let mut typed: Vec<Option<Typed>> = vec![None; plan.slots.len()];
    let mut glued = vec![];
    let mut by_symbol: Vec<Option<usize>> = vec![None; plan.syms.len()];
    for d in &plan.defines {
        let sym = plan.syms[d.symbol].as_str();
        let mut fields: Vec<Field> = rows.iter().enumerate().map(|(j, t)| Field::new(format!("__mr_r{j}"), t.clone(), true)).collect();
        for &s in &d.slots {
            let t = slot_types(&plan.slots[s], values, base).map_err(|e| define_error(sym, e))?;
            fields.push(Field::new(format!("__mr_s{s}"), t.result.clone(), true));
            typed[s] = Some(t);
        }
        let schema: SchemaRef = Arc::new(Schema::new(fields.clone()));
        let frame = DFSchema::from_unqualified_fields(Fields::from(fields), HashMap::new()).map_err(|e| define_error(sym, e.into()))?;
        let expr = glue_expr(base, &d.glue, &frame).map_err(|e| define_error(sym, e))?;
        by_symbol[d.symbol] = Some(glued.len());
        glued.push(Glued { schema, slots: d.slots.clone(), expr });
    }
    let typed_slots = typed.into_iter().collect::<Option<Vec<Typed>>>().context("MATCH_RECOGNIZE: a slot belongs to no DEFINE")?;
    Ok(Planned { glued, by_symbol, types: typed_slots })
}

/// A glue as a physical expression over `frame`: planned as SQL, run through the engine's analyzer (its rules, as on the row
/// path), and made physical.
fn glue_expr(base: &SessionState, text: &str, frame: &DFSchema) -> Result<Arc<dyn PhysicalExpr>> {
    let expr = base.create_logical_expr(text, frame)?;
    let input = LogicalPlan::EmptyRelation(EmptyRelation { produce_one_row: false, schema: Arc::new(frame.clone()) });
    let plan = LogicalPlan::Projection(Projection::try_new(vec![expr], Arc::new(input))?);
    let checked = base.analyzer().execute_and_check(plan, base.config_options(), |_, _| {})?;
    let LogicalPlan::Projection(p) = checked else { bail!("MATCH_RECOGNIZE: a DEFINE's glue didn't plan as an expression") };
    let expr = match p.expr.into_iter().next() {
        Some(Expr::Alias(a)) => *a.expr,
        Some(e) => e,
        None => bail!("MATCH_RECOGNIZE: a DEFINE's glue is empty"),
    };
    // The optimizer rewrites COALESCE and its kin to CASE before a plan runs; a glue has no plan, so it is simplified here.
    let info = SimplifyContext::builder().with_schema(Arc::new(frame.clone())).build();
    let expr = ExprSimplifier::new(info).simplify(expr)?;
    Ok(base.create_physical_expr(expr, frame)?)
}

/// A DEFINE's error, named by its variable. The SQLSTATE is the error's own: a refusal's, or the one its words give (a
/// function the engine lacks is 42883).
fn define_error(sym: &str, e: anyhow::Error) -> anyhow::Error {
    let code = crate::codes::of(&e);
    coded(code, format!("MATCH_RECOGNIZE: DEFINE {sym}: {e}"))
}

/// A slot's types: a navigation's and a MIN's or MAX's are their value's; a count's and MATCH_NUMBER's are Int64. A sum's and an
/// average's are the argument and result DataFusion's own aggregate gives the value. A sum of a type not added here, and an
/// average of a DECIMAL, are refused.
fn slot_types(slot: &SlotSpec, values: &[DataType], base: &SessionState) -> Result<Typed> {
    Ok(match slot {
        SlotSpec::MatchNumber { .. } => Typed::same(DataType::Int64),
        SlotSpec::Nav { value, .. } => Typed::same(values.get(*value).context("MATCH_RECOGNIZE: a value is missing")?.clone()),
        SlotSpec::Agg { fun, value, .. } => match fun.as_str() {
            "count" => Typed::same(DataType::Int64),
            name => {
                let j = (*value).context("MATCH_RECOGNIZE: an aggregate reads a value")?;
                let vt = values.get(j).context("MATCH_RECOGNIZE: a value is missing")?;
                match name {
                    "min" | "max" => Typed::same(vt.clone()),
                    "sum" => {
                        let (arg, result) = aggregate_types(base, "sum", vt)?;
                        let ok = match (&arg, &result) {
                            (DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Float32, _) => true,
                            (DataType::Decimal128(_, s), DataType::Decimal128(_, r)) => s == r,
                            _ => false,
                        };
                        if !ok {
                            return Err(coded(
                                "0A000",
                                format!("sum of {arg} isn't supported in a DEFINE yet: cast the value to DOUBLE or BIGINT, as sum(CAST(a.x AS DOUBLE))"),
                            ));
                        }
                        Typed { arg, result }
                    }
                    _ => {
                        if matches!(vt, DataType::Decimal128(_, _) | DataType::Decimal256(_, _)) {
                            return Err(coded(
                                "0A000",
                                "avg of a DECIMAL isn't supported in a DEFINE yet: cast the value to DOUBLE, as avg(CAST(a.x AS DOUBLE))",
                            ));
                        }
                        Typed { arg: DataType::Float64, result: DataType::Float64 }
                    }
                }
            }
        },
    })
}

/// The argument type DataFusion's aggregate `name` coerces a value of type `value` to, and the aggregate's result type.
fn aggregate_types(base: &SessionState, name: &str, value: &DataType) -> Result<(DataType, DataType)> {
    let udaf = base.udaf(name)?;
    let field: FieldRef = Arc::new(Field::new("value", value.clone(), true));
    let coerced = fields_with_udf(&[field], udaf.as_ref())?;
    let arg = coerced.first().context("MATCH_RECOGNIZE: an aggregate takes no argument")?.data_type().clone();
    let result = udaf.return_type(std::slice::from_ref(&arg))?;
    Ok((arg, result))
}

/// The two window functions: `pondra_match` (a row's first membership) and `pondra_matches` (all of them, for matches
/// that may share rows). The planner is the session's snapshot; it is not part of a function's identity.
#[derive(Debug)]
struct Matcher {
    all: bool,
    signature: Signature,
    planner: Arc<Planner>,
}

impl PartialEq for Matcher {
    fn eq(&self, other: &Self) -> bool {
        self.all == other.all && self.signature == other.signature
    }
}

impl Eq for Matcher {}

impl Hash for Matcher {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.all.hash(state);
        self.signature.hash(state);
    }
}

impl WindowUDFImpl for Matcher {
    fn name(&self) -> &str {
        match self.all {
            true => "pondra_matches",
            false => "pondra_match",
        }
    }

    fn signature(&self) -> &Signature { &self.signature }

    fn partition_evaluator(&self, args: PartitionEvaluatorArgs) -> DfResult<Box<dyn PartitionEvaluator>> {
        let types: Vec<DataType> = args.input_fields().iter().map(|f| f.data_type().clone()).collect();
        // (the spec is a literal, as `pattern.rs` writes it: compiled and planned here; another form is read at the first row)
        let literal = match args.input_exprs().first().and_then(|e| e.downcast_ref::<Literal>()).map(|l| l.value()) {
            Some(ScalarValue::Utf8(Some(text))) => Some(text.clone()),
            _ => None,
        };
        let mut evaluator = Evaluator { all: self.all, planner: self.planner.clone(), text: String::new(), plan: None, planned: None };
        if let Some(text) = literal {
            let plan = compile(&text).map_err(external)?;
            // (WITHIN's time and interval are checked here, at planning; `run` checks the time too)
            if plan.within {
                let fields = args.input_fields();
                if let Some(time) = fields.get(plan.syms.len() + 1) {
                    check_time_type(time.data_type()).map_err(external)?;
                }
                // a backstop for a spec written by hand: pattern.rs writes an interval literal, which is an interval type
                if let Some(interval) = fields.get(plan.syms.len() + 2) {
                    if !matches!(interval.data_type(), DataType::Interval(_)) {
                        return Err(external(coded("42601", "MATCH_RECOGNIZE: WITHIN takes an interval, as INTERVAL '1' HOUR")));
                    }
                }
            }
            if !plan.defines.is_empty() {
                evaluator.planned = Some(self.planner.planned(&text, &plan, &types).map_err(external)?);
            }
            evaluator.text = text;
            evaluator.plan = Some(plan);
        }
        Ok(Box::new(evaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let t = match self.all {
            true => DataType::List(Arc::new(Field::new("item", struct_type(), true))),
            false => struct_type(),
        };
        Ok(Arc::new(Field::new(field_args.name(), t, true)))
    }
}

/// Registers `pondra_match`, `pondra_matches` and `pondra_nav` with a session (`engine::session`). A DEFINE's glue is planned
/// against the session as it stands here, so it sees the functions registered before this call and no later one.
pub fn register(ctx: &SessionContext) {
    let planner = Arc::new(Planner { base: Arc::new(ctx.state()), cache: Mutex::new(HashMap::new()) });
    for all in [false, true] {
        let signature = Signature::variadic_any(Volatility::Immutable);
        ctx.register_udwf(WindowUDF::from(Matcher { all, signature, planner: planner.clone() }));
    }
    ctx.register_udwf(WindowUDF::from(Navigate { signature: Signature::any(5, Volatility::Immutable) }));
}

/// `pondra_nav(value, keep, m, first, final)`, called over one match's rows in their order: at each row, `value` at the
/// m-th (from 0) of the rows where `keep` is true, counted from the first (`first`) or from the last, among the rows up to
/// this one (or all the match's, when `final`). NULL where there are not m+1. DataFusion's own first_value and last_value
/// refuse a FILTER in a window, which is why this exists.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Navigate {
    signature: Signature,
}

impl WindowUDFImpl for Navigate {
    fn name(&self) -> &str { "pondra_nav" }

    fn signature(&self) -> &Signature { &self.signature }

    fn partition_evaluator(&self, _args: PartitionEvaluatorArgs) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(NavEvaluator))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let t = field_args.input_fields()[0].data_type().clone();
        Ok(Arc::new(Field::new(field_args.name(), t, true)))
    }
}

/// The evaluator of `pondra_nav`: the whole partition at once, as `Evaluator` reads its matches.
#[derive(Debug)]
struct NavEvaluator;

impl PartitionEvaluator for NavEvaluator {
    fn uses_window_frame(&self) -> bool { false }

    fn supports_bounded_execution(&self) -> bool { false }

    fn evaluate_all(&mut self, values: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
        nav(values, n).map_err(external)
    }
}

/// `pondra_nav` over `n` rows; `values` are its five columns. The kept rows are listed once, and each row then takes its
/// answer's place from the list, so a partition costs its rows (`take` moves the values, with no frame scans).
fn nav(values: &[ArrayRef], n: usize) -> Result<ArrayRef> {
    if n == 0 {
        return Ok(values[0].slice(0, 0));
    }
    let keep = bools(&values[1])?;
    let m = usize::try_from(first_int(&values[2])?).context("MATCH_RECOGNIZE: pondra_nav's m is a count of rows")?;
    let first = bools(&values[3])?[0];
    let fin = bools(&values[4])?[0];
    let kept: Vec<usize> = (0..n).filter(|&i| keep[i]).collect();
    let mut up_to = 0;
    let mut at: Vec<Option<u32>> = Vec::with_capacity(n);
    for &kept_here in &keep {
        up_to += usize::from(kept_here);
        let count = if fin { kept.len() } else { up_to };
        let pick = match (m < count, first) {
            (false, _) => None,
            (true, true) => kept.get(m).copied(),
            (true, false) => kept.get(count - 1 - m).copied(),
        };
        at.push(pick.map(|j| j as u32));
    }
    Ok(datafusion::arrow::compute::take(values[0].as_ref(), &UInt32Array::from(at), None)?)
}

/// A literal argument (the same in every row), read from its first row as a whole number.
fn first_int(column: &ArrayRef) -> Result<i64> {
    let ints = cast(column, &DataType::Int64)?;
    let ints = ints.as_any().downcast_ref::<Int64Array>().context("MATCH_RECOGNIZE: pondra_nav reads a whole number")?;
    ints.iter().next().flatten().context("MATCH_RECOGNIZE: pondra_nav's m is NULL")
}


/// A PATTERN's text as a pattern, and the symbols it names in the order they first appear (a symbol's number is its place
/// here: the DEFINE columns go by it, and so do the matcher's conditions). Read here, not by sqlparser, which refuses forms
/// standard SQL has: `{- b -}` of more than a symbol, and PERMUTE of alternatives. A refusal is 42601 and quotes the pattern.
pub fn read(text: &str) -> Result<(Pattern, Vec<String>)> {
    let mut p = Pat { src: text, at: 0, syms: vec![] };
    let node = p.alt()?;
    if let Some(c) = p.look() {
        return Err(p.unexpected(c));
    }
    Ok((node, p.syms))
}

/// The reader's place in a pattern: `at` is a byte index into `src`.
struct Pat<'a> {
    src: &'a str,
    at: usize,
    syms: Vec<String>,
}

impl<'a> Pat<'a> {
    fn rest(&self) -> &'a str {
        &self.src[self.at..]
    }

    fn space(&mut self) {
        self.at += self.rest().len() - self.rest().trim_start().len();
    }

    /// The next character, after the spaces, without taking it.
    fn look(&mut self) -> Option<char> {
        self.space();
        self.rest().chars().next()
    }

    /// Takes `token` when it is next, after the spaces.
    fn take(&mut self, token: &str) -> bool {
        self.space();
        let hit = self.rest().starts_with(token);
        if hit {
            self.at += token.len();
        }
        hit
    }

    /// What was read from `from` to here, trimmed: what came before a missing token.
    fn since(&self, from: usize) -> &'a str {
        self.src[from..self.at].trim()
    }

    fn fail(&self, what: impl Display) -> anyhow::Error {
        coded("42601", format!("MATCH_RECOGNIZE: PATTERN ({}): {what}", self.src))
    }

    /// A character the pattern can't have at this place, with its place counted in characters.
    fn unexpected(&self, c: char) -> anyhow::Error {
        self.fail(format!("unexpected {c} at character {}", self.src[..self.at].chars().count() + 1))
    }

    /// alt := seq ('|' seq)*
    fn alt(&mut self) -> Result<Pattern> {
        let mut alts = vec![self.seq()?];
        while self.take("|") {
            alts.push(self.seq()?);
        }
        Ok(if alts.len() == 1 { alts.remove(0) } else { Pattern::Alt(alts) })
    }

    /// seq := term*, where none is an empty pattern (a sequence of no rows).
    fn seq(&mut self) -> Result<Pattern> {
        let mut terms = vec![];
        loop {
            match self.look() {
                None | Some('|' | ')' | ',') => break,
                Some('-') if self.rest().starts_with("-}") => break,
                Some(_) => terms.push(self.term()?),
            }
        }
        Ok(if terms.len() == 1 { terms.remove(0) } else { Pattern::Seq(terms) })
    }

    /// term := primary quantifier, and a `?` after a quantifier makes it reluctant. A second quantifier is refused:
    /// the standard has none, so the author groups the first.
    fn term(&mut self) -> Result<Pattern> {
        let node = self.primary()?;
        let Some((min, max)) = self.quantifier()? else { return Ok(node) };
        let greedy = !self.take("?");
        if matches!(self.look(), Some('*' | '+' | '?')) || self.counts_next() {
            return Err(self.fail("a quantifier can't follow a quantifier: group it, as (a+)*"));
        }
        Ok(Pattern::Rep { body: Box::new(node), min, max, greedy })
    }

    /// primary := a name | '^' | '$' | '(' alt ')' | '{-' alt '-}' | PERMUTE '(' alt (',' alt)* ')'
    fn primary(&mut self) -> Result<Pattern> {
        if self.take("^") {
            return Ok(Pattern::Start);
        }
        if self.take("$") {
            return Ok(Pattern::End);
        }
        if self.take("(") {
            let from = self.at;
            let node = self.alt()?;
            if !self.take(")") {
                return Err(self.fail(format!("expected ) after {}", self.since(from))));
            }
            return Ok(node);
        }
        if self.take("{-") {
            let from = self.at;
            let node = self.alt()?;
            if !self.take("-}") {
                return Err(self.fail(format!("expected -}} after {}", self.since(from))));
            }
            return Ok(Pattern::Exclude(Box::new(node)));
        }
        let Some(c) = self.look() else { return Err(self.fail("expected a symbol")) };
        if c == '"' || c.is_alphabetic() || c == '_' {
            let (name, quoted) = self.name()?;
            if !quoted && name == "permute" && self.take("(") {
                let from = self.at;
                let mut items = vec![self.alt()?];
                while self.take(",") {
                    items.push(self.alt()?);
                }
                if !self.take(")") {
                    return Err(self.fail(format!("expected ) after {}", self.since(from))));
                }
                return Ok(Pattern::Permute(items));
            }
            let k = match self.syms.iter().position(|s| *s == name) {
                Some(k) => k,
                None => {
                    self.syms.push(name);
                    self.syms.len() - 1
                }
            };
            return Ok(Pattern::Sym(k));
        }
        Err(self.unexpected(c))
    }

    /// The quantifier after a term: its minimum and its maximum (None for no maximum), or None when there is none.
    fn quantifier(&mut self) -> Result<Option<(u32, Option<u32>)>> {
        Ok(Some(match self.look() {
            Some('*') => {
                self.at += 1;
                (0, None)
            }
            Some('+') => {
                self.at += 1;
                (1, None)
            }
            Some('?') => {
                self.at += 1;
                (0, Some(1))
            }
            Some('{') if self.counts_next() => self.count()?,
            _ => return Ok(None),
        }))
    }

    /// Whether a count comes next: a `{` then a digit or a comma (`{-` is an exclusion, not a count).
    fn counts_next(&self) -> bool {
        let mut it = self.rest().chars();
        it.next() == Some('{') && matches!(it.next(), Some(c) if c.is_ascii_digit() || c == ',')
    }

    /// `{n}`, `{n,}`, `{,m}` or `{n,m}`: the minimum and the maximum (None for no maximum).
    fn count(&mut self) -> Result<(u32, Option<u32>)> {
        self.at += 1;
        let from = self.at;
        let min = self.number()?;
        let (min, max) = if self.take(",") {
            let max = self.number()?;
            if min.is_none() && max.is_none() {
                return Err(self.fail("a count needs a number, as {2}, {2,} or {,3}"));
            }
            (min.unwrap_or(0), max)
        } else {
            let Some(n) = min else { return Err(self.fail("a count needs a number, as {2}, {2,} or {,3}")) };
            (n, Some(n))
        };
        if !self.take("}") {
            return Err(self.fail(format!("expected }} after {}", self.since(from))));
        }
        Ok((min, max))
    }

    /// A whole number written here, with no space before it; None when no digit is.
    fn number(&mut self) -> Result<Option<u32>> {
        let rest = self.rest();
        let digits = rest.len() - rest.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if digits == 0 {
            return Ok(None);
        }
        let n = rest[..digits].parse::<u32>().map_err(|_| self.fail(format!("a count is too big: {}", &rest[..digits])))?;
        self.at += digits;
        Ok(Some(n))
    }

    /// A name: a plain one, which SQL folds to lower case, or a quoted one kept as written (`""` is a quote in it).
    fn name(&mut self) -> Result<(String, bool)> {
        let rest = self.rest();
        let Some(body) = rest.strip_prefix('"') else {
            let n = rest.find(|c: char| !(c.is_alphanumeric() || c == '_')).unwrap_or(rest.len());
            self.at += n;
            return Ok((rest[..n].to_lowercase(), false));
        };
        let mut out = String::new();
        let mut chars = body.char_indices().peekable();
        while let Some((i, c)) = chars.next() {
            if c != '"' {
                out.push(c);
            } else if chars.peek().is_some_and(|&(_, d)| d == '"') {
                chars.next();
                out.push('"');
            } else {
                self.at += 1 + i + 1;
                return Ok((out, true));
            }
        }
        Err(self.fail("a quoted name isn't closed with \""))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rows' conditions: `rows[r]` lists the symbols row r meets.
    fn conds(rows: &[Vec<usize>], syms: usize) -> Vec<Vec<bool>> {
        (0..syms).map(|k| rows.iter().map(|r| r.contains(&k)).collect()).collect()
    }

    /// Each row's memberships as text: `1a2` (match 1, symbol a, place 2), `1-` for an empty match's row.
    fn shown(spec: &str, rows: &[Vec<usize>]) -> Result<Vec<String>> {
        let plan = compile(spec)?;
        let cond = conds(rows, plan.syms.len());
        let part = Part { cond: &cond, within: None, n: rows.len(), rows: &[], values: &[], planned: None, cap: STATE_CAP };
        Ok(texts(&plan, &partition(&plan, &part)?))
    }

    /// `shown` under WITHIN: `times` are the rows' times (None is NULL), and each row must be within `interval` of its
    /// match's first row.
    fn shown_within(pattern: &str, rows: &[Vec<usize>], times: &[Option<i64>], interval: i64) -> Vec<String> {
        let plan = compile(&within_spec(pattern)).unwrap();
        let low = times.iter().map(|t| t.map(|t| t - interval)).collect();
        let w = Within { time: times.to_vec(), low };
        let cond = conds(rows, plan.syms.len());
        let part = Part { cond: &cond, within: Some(&w), n: rows.len(), rows: &[], values: &[], planned: None, cap: STATE_CAP };
        texts(&plan, &partition(&plan, &part).unwrap())
    }


    fn texts(plan: &Plan, found: &[Vec<Mem>]) -> Vec<String> {
        found
            .iter()
            .map(|r| {
                r.iter()
                    .map(|m| match m.sym {
                        Some(k) => format!("{}{}{}", m.m, plan.syms[k], m.seq.unwrap_or(0)),
                        None => format!("{}-", m.m),
                    })
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    }

    fn within_spec(pattern: &str) -> String {
        format!(r#"{{"pattern":"{pattern}","skip":{},"within":true}}"#, PAST)
    }

    const PAST: &str = r#"{"to":"past_last"}"#;

    fn spec(pattern: &str, skip: &str) -> String {
        format!(r#"{{"pattern":"{pattern}","skip":{skip}}}"#)
    }

    /// The pattern a text reads as, and the names it reads with.
    fn parsed(text: &str) -> Pattern { read(text).expect("a pattern").0 }

    fn names(text: &str) -> Vec<String> { read(text).expect("a pattern").1 }

    fn rep(body: Pattern, min: u32, max: Option<u32>, greedy: bool) -> Pattern {
        Pattern::Rep { body: Box::new(body), min, max, greedy }
    }

    /// A pattern that is refused: it is 42601, and its words are returned.
    fn refused(text: &str) -> String {
        let err = read(text).expect_err("a refusal");
        assert_eq!(crate::codes::of(&err), "42601");
        err.to_string()
    }

    #[test]
    fn a_then_one_or_more_b_then_c() {
        let rows = vec![vec![0], vec![1], vec![1], vec![2], vec![]];
        assert_eq!(shown(&spec("a b+ c", PAST), &rows).unwrap(), ["1a1", "1b2", "1b3", "1c4", ""]);
    }

    #[test]
    fn alternation_prefers_the_first() {
        assert_eq!(shown(&spec("a | b", PAST), &[vec![0, 1]]).unwrap(), ["1a1"]);
        assert_eq!(shown(&spec("a | b", PAST), &[vec![1]]).unwrap(), ["1b1"]);
    }

    #[test]
    fn reluctant_takes_fewer() {
        let rows = vec![vec![0], vec![0]];
        assert_eq!(shown(&spec("a+", PAST), &rows).unwrap(), ["1a1", "1a2"]);
        assert_eq!(shown(&spec("a+?", PAST), &rows).unwrap(), ["1a1", "2a1"]);
    }

    #[test]
    fn bounded_repetition() {
        let rows = vec![vec![0]; 5];
        assert_eq!(shown(&spec("a{2,3}", PAST), &rows).unwrap(), ["1a1", "1a2", "1a3", "2a1", "2a2"]);
    }

    #[test]
    fn anchors() {
        assert_eq!(shown(&spec("^ a", PAST), &[vec![0], vec![0]]).unwrap(), ["1a1", ""]);
        assert_eq!(shown(&spec("a $", PAST), &[vec![0], vec![0]]).unwrap(), ["", "1a1"]);
    }

    #[test]
    fn permute_in_the_standards_order() {
        assert_eq!(shown(&spec("PERMUTE(a, b)", PAST), &[vec![1], vec![0]]).unwrap(), ["1b1", "1a2"]);
        assert_eq!(shown(&spec("PERMUTE(a, b)", PAST), &[vec![0], vec![1]]).unwrap(), ["1a1", "1b2"]);
    }

    #[test]
    fn empty_matches_mark_their_row() {
        assert_eq!(shown(&spec("a*", PAST), &[vec![1], vec![0]]).unwrap(), ["1-", "2a1"]);
    }

    #[test]
    fn skip_past_last_row() {
        let rows = vec![vec![0], vec![0], vec![1]];
        assert_eq!(shown(&spec("a+", PAST), &rows).unwrap(), ["1a1", "1a2", ""]);
    }

    #[test]
    fn skip_to_next_row_overlaps() {
        let rows = vec![vec![0]; 3];
        let next = r#"{"to":"next_row"}"#;
        assert_eq!(shown(&spec("a+", next), &rows).unwrap(), ["1a1", "1a2 2a1", "1a3 2a2 3a1"]);
    }

    #[test]
    fn skip_to_first_symbol() {
        let rows = vec![vec![0], vec![1], vec![1], vec![0], vec![1]];
        let to_b = r#"{"to":"first","symbol":"b"}"#;
        assert_eq!(shown(&spec("a b+", to_b), &rows).unwrap(), ["1a1", "1b2", "1b3", "2a1", "2b2"]);
    }

    #[test]
    fn skip_to_a_start_row_is_refused() {
        let to_a = r#"{"to":"first","symbol":"a"}"#;
        let err = shown(&spec("a b+", to_a), &[vec![0], vec![1]]).unwrap_err();
        assert_eq!(crate::codes::of(&err), "22000");
    }

    #[test]
    fn skip_to_a_symbol_with_no_row_is_refused() {
        let to_c = r#"{"to":"last","symbol":"c"}"#;
        let err = shown(&spec("a+ | c", to_c), &[vec![0]]).unwrap_err();
        assert_eq!(crate::codes::of(&err), "22000");
    }

    #[test]
    fn too_many_states_are_refused() {
        let err = compile(&spec("a{10001}", PAST)).unwrap_err();
        assert_eq!(crate::codes::of(&err), "54000");
    }

    #[test]
    fn each_form_reads_as_its_node() {
        assert_eq!(parsed("a"), Pattern::Sym(0));
        assert_eq!(parsed("a b"), Pattern::Seq(vec![Pattern::Sym(0), Pattern::Sym(1)]));
        assert_eq!(parsed("a | b c"), Pattern::Alt(vec![Pattern::Sym(0), Pattern::Seq(vec![Pattern::Sym(1), Pattern::Sym(2)])]));
        assert_eq!(parsed("^ a $"), Pattern::Seq(vec![Pattern::Start, Pattern::Sym(0), Pattern::End]));
        assert_eq!(
            parsed("a{- b -}c"),
            Pattern::Seq(vec![Pattern::Sym(0), Pattern::Exclude(Box::new(Pattern::Sym(1))), Pattern::Sym(2)])
        );
        assert_eq!(
            parsed("PERMUTE(a | b, c)"),
            Pattern::Permute(vec![Pattern::Alt(vec![Pattern::Sym(0), Pattern::Sym(1)]), Pattern::Sym(2)])
        );
        assert_eq!(parsed("(a b)+"), rep(Pattern::Seq(vec![Pattern::Sym(0), Pattern::Sym(1)]), 1, None, true));
    }

    #[test]
    fn counts_and_reluctant_forms() {
        assert_eq!(parsed("a*"), rep(Pattern::Sym(0), 0, None, true));
        assert_eq!(parsed("a+"), rep(Pattern::Sym(0), 1, None, true));
        assert_eq!(parsed("a?"), rep(Pattern::Sym(0), 0, Some(1), true));
        assert_eq!(parsed("a{2}"), rep(Pattern::Sym(0), 2, Some(2), true));
        assert_eq!(parsed("a{2,}"), rep(Pattern::Sym(0), 2, None, true));
        assert_eq!(parsed("a{,3}"), rep(Pattern::Sym(0), 0, Some(3), true));
        assert_eq!(parsed("a{1,3}"), rep(Pattern::Sym(0), 1, Some(3), true));
        assert_eq!(parsed("a+?"), rep(Pattern::Sym(0), 1, None, false));
        assert_eq!(parsed("a??"), rep(Pattern::Sym(0), 0, Some(1), false));
        assert_eq!(parsed("a{2,}?"), rep(Pattern::Sym(0), 2, None, false));
        assert_eq!(parsed("(a+)?"), rep(rep(Pattern::Sym(0), 1, None, true), 0, Some(1), true));
    }

    #[test]
    fn names_fold_unless_quoted() {
        assert_eq!(names("Down UP down"), vec!["down", "up"]);
        assert_eq!(names(r#""My Var" "a""b""#), vec!["My Var", "a\"b"]);
        assert_eq!(names(r#""Down" down"#), vec!["Down", "down"]);
    }

    #[test]
    fn the_empty_pattern_is_an_empty_sequence() {
        assert_eq!(parsed(""), Pattern::Seq(vec![]));
        assert!(names("  ").is_empty());
    }

    #[test]
    fn refusals_quote_the_pattern_and_say_what_is_missing() {
        assert_eq!(refused("a {- b c"), "MATCH_RECOGNIZE: PATTERN (a {- b c): expected -} after b c");
        assert_eq!(refused("(a b"), "MATCH_RECOGNIZE: PATTERN ((a b): expected ) after a b");
        assert_eq!(refused("a)"), "MATCH_RECOGNIZE: PATTERN (a)): unexpected ) at character 2");
        assert_eq!(refused("PERMUTE(a, b"), "MATCH_RECOGNIZE: PATTERN (PERMUTE(a, b): expected ) after a, b");
        assert_eq!(refused("a{2x}"), "MATCH_RECOGNIZE: PATTERN (a{2x}): expected } after 2");
        assert_eq!(
            refused("a++"),
            "MATCH_RECOGNIZE: PATTERN (a++): a quantifier can't follow a quantifier: group it, as (a+)*"
        );
        assert_eq!(refused("*a"), "MATCH_RECOGNIZE: PATTERN (*a): unexpected * at character 1");
        assert_eq!(
            refused("a{,}"),
            "MATCH_RECOGNIZE: PATTERN (a{,}): a count needs a number, as {2}, {2,} or {,3}"
        );
        assert_eq!(refused("\"a"), "MATCH_RECOGNIZE: PATTERN (\"a): a quoted name isn't closed with \"");
        assert_eq!(refused("a{-b"), "MATCH_RECOGNIZE: PATTERN (a{-b): expected -} after b");
    }

    /// `pondra_nav` over one partition's rows: the value, the keep flags, then m, first and final for each row.
    fn nav_of(value: &[Option<i64>], keep: &[Option<bool>], m: i64, first: bool, fin: bool) -> Vec<Option<i64>> {
        let n = value.len();
        let cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(value.to_vec())),
            Arc::new(BooleanArray::from(keep.to_vec())),
            Arc::new(Int64Array::from(vec![m; n])),
            Arc::new(BooleanArray::from(vec![first; n])),
            Arc::new(BooleanArray::from(vec![fin; n])),
        ];
        let out = nav(&cols, n).unwrap();
        out.as_any().downcast_ref::<Int64Array>().unwrap().iter().collect()
    }

    const VALUE: [Option<i64>; 4] = [Some(10), Some(20), Some(30), Some(40)];
    const KEEP: [Option<bool>; 4] = [Some(true), Some(false), Some(true), Some(true)];

    #[test]
    fn nav_running_reads_up_to_each_row() {
        assert_eq!(nav_of(&VALUE, &KEEP, 0, true, false), [Some(10); 4]);
        assert_eq!(nav_of(&VALUE, &KEEP, 0, false, false), [Some(10), Some(10), Some(30), Some(40)]);
    }

    #[test]
    fn nav_final_reads_the_whole_match() {
        assert_eq!(nav_of(&VALUE, &KEEP, 0, true, true), [Some(10); 4]);
        assert_eq!(nav_of(&VALUE, &KEEP, 0, false, true), [Some(40); 4]);
    }

    #[test]
    fn nav_counts_m_from_the_first_or_the_last() {
        assert_eq!(nav_of(&VALUE, &KEEP, 2, true, false), [None, None, None, Some(40)]);
        assert_eq!(nav_of(&VALUE, &KEEP, 2, false, true), [Some(10); 4]);
        assert_eq!(nav_of(&VALUE, &KEEP, 1, false, false), [None, None, Some(10), Some(30)]);
    }

    #[test]
    fn nav_takes_a_null_keep_as_false() {
        let keep = [None, Some(true), None];
        assert_eq!(nav_of(&[Some(1), Some(2), Some(3)], &keep, 0, true, false), [None, Some(2), Some(2)]);
    }

    #[test]
    fn nav_with_no_kept_row_is_null() {
        assert_eq!(nav_of(&[Some(1), Some(2)], &[Some(false), None], 0, false, true), [None, None]);
    }

    #[test]
    fn with_unmatched_rows_pondra_matches_gives_a_null_item() {
        let syms = vec!["a".to_string()];
        let rows = vec![vec![], vec![Mem { m: 1, sym: Some(0), seq: Some(1), excluded: false }]];
        let out = answer(&rows, &syms, true, true).unwrap();
        let lists = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value(0).len(), 1);
        assert!(lists.value(0).is_null(0));
        assert!(lists.value(1).is_valid(0));
        let plain = answer(&rows, &syms, true, false).unwrap();
        assert_eq!(plain.as_any().downcast_ref::<ListArray>().unwrap().value(0).len(), 0);
    }

    #[test]
    fn the_spec_reads_unmatched() {
        assert!(compile(r#"{"pattern":"a","skip":{"to":"past_last"},"unmatched":true}"#).unwrap().unmatched);
        assert!(!compile(&spec("a", PAST)).unwrap().unmatched);
    }

    #[test]
    fn the_spec_reads_within() {
        assert!(compile(&within_spec("a")).unwrap().within);
        assert!(!compile(&spec("a", PAST)).unwrap().within);
    }

    #[test]
    fn within_takes_the_shorter_match_when_the_longer_runs_out() {
        // four rows, one time unit apart, within 2: the greedy a+ from row 0 can't take row 3, so it stops at row 2
        let rows = vec![vec![0]; 4];
        assert_eq!(shown_within("a+", &rows, &[Some(0), Some(1), Some(2), Some(3)], 2), ["1a1", "1a2", "1a3", "2a1"]);
    }

    #[test]
    fn within_gives_the_match_to_a_later_start_when_the_earlier_runs_out() {
        // from row 0, b at time 4 is 4 after a's time 0, more than 3: that thread dies, and the start at row 1 (time 2) wins
        let rows = vec![vec![0], vec![0], vec![1]];
        assert_eq!(shown_within("a+ b", &rows, &[Some(0), Some(2), Some(4)], 3), ["", "1a1", "1b2"]);
    }

    #[test]
    fn within_never_takes_a_null_time() {
        // the row at NULL can't start a match, so the match is the next row's
        let rows = vec![vec![0], vec![0]];
        assert_eq!(shown_within("a", &rows, &[None, Some(0)], 3), ["", "1a1"]);
    }

    #[test]
    fn within_reads_a_date_or_a_timestamp_only() {
        assert!(check_time_type(&DataType::Timestamp(TimeUnit::Nanosecond, None)).is_ok());
        assert!(check_time_type(&DataType::Date32).is_ok());
        let err = check_time_type(&DataType::Int32).expect_err("a refusal");
        assert_eq!(crate::codes::of(&err), "42804");
    }

    #[test]
    fn within_of_reads_dates_and_zoned_timestamps_alike() {
        use datafusion::arrow::array::{Date32Array, IntervalMonthDayNanoArray, TimestampMicrosecondArray};
        use datafusion::arrow::datatypes::IntervalMonthDayNanoType;
        // a date is its day's microseconds, and an interval of one day moves each time back by a day
        let day = IntervalMonthDayNanoType::make_value(0, 1, 0);
        let interval: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![day, day]));
        let dates: ArrayRef = Arc::new(Date32Array::from(vec![0, 1]));
        let w = within_of(&dates, &interval).unwrap();
        assert_eq!(w.time, [Some(0), Some(86_400_000_000)]);
        assert_eq!(w.low, [Some(-86_400_000_000), Some(0)]);
        // a timestamp keeps its zone: the same instants give the same ticks and bounds as they do in UTC
        let micros = vec![3_600_000_000i64, 7_200_000_000];
        let zoned: ArrayRef = Arc::new(TimestampMicrosecondArray::from(micros.clone()).with_timezone("+02:00"));
        let utc: ArrayRef = Arc::new(TimestampMicrosecondArray::from(micros));
        let (a, b) = (within_of(&zoned, &interval).unwrap(), within_of(&utc, &interval).unwrap());
        assert_eq!(a.time, b.time);
        assert_eq!(a.low, b.low);
    }

    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::execution::runtime_env::RuntimeEnv;
    use datafusion::prelude::SessionConfig;

    /// A query's rows as text, each row's values joined by `|`, in the order the query gives. Its MATCH_RECOGNIZE is written
    /// as the engine's front writes it (`pattern::rewritten`), and the query runs in a session.
    async fn rows_of(sql: &str) -> Result<Vec<String>> {
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let batches = ctx.sql(&crate::pattern::rewritten(sql)?).await?.collect().await?;
        let mut out = vec![];
        for b in &batches {
            for r in 0..b.num_rows() {
                let cells = b
                    .columns()
                    .iter()
                    .map(|c| array_value_to_string(c.as_ref(), r))
                    .collect::<std::result::Result<Vec<String>, _>>()?;
                out.push(cells.join("|"));
            }
        }
        Ok(out)
    }

    #[tokio::test]
    async fn a_define_that_counts_the_match_counts_the_row_it_tests() -> Result<()> {
        // b counts the match with its own row in it, so (b | a) c takes b for row 1: the answer is b c, not a c
        let rows = rows_of(
            "WITH t(day) AS (VALUES (1), (2)) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day MEASURES CLASSIFIER() AS sym \
             ALL ROWS PER MATCH PATTERN ((b | a) c) DEFINE b AS count(*) >= 1) ORDER BY day",
        )
        .await?;
        assert_eq!(rows, ["1|b", "2|c"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_variable_with_no_rows_is_null_in_the_glue() -> Result<()> {
        // when b is first tested, a has no row: a.x is NULL, COALESCE makes it 0, and b takes every row (a? first fails at row 2)
        let rows = rows_of(
            "WITH t(day, x) AS (VALUES (1, 1), (2, 2), (3, 3)) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES CLASSIFIER() AS sym ALL ROWS PER MATCH PATTERN (a? b+) DEFINE b AS COALESCE(a.x, 0) = 0) ORDER BY day",
        )
        .await?;
        assert_eq!(rows, ["1|b", "2|b", "3|b"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_double_sum_in_a_define_is_fsums() -> Result<()> {
        // ten 0.1s sum to 0.9999999999999999 when added one by one, and to 1 as fsum sums them: the DEFINE sees the 1
        let rows = rows_of(
            "WITH t(day, x) AS (VALUES (1, CAST(0.1 AS DOUBLE)), (2, CAST(0.1 AS DOUBLE)), (3, CAST(0.1 AS DOUBLE)), \
             (4, CAST(0.1 AS DOUBLE)), (5, CAST(0.1 AS DOUBLE)), (6, CAST(0.1 AS DOUBLE)), (7, CAST(0.1 AS DOUBLE)), \
             (8, CAST(0.1 AS DOUBLE)), (9, CAST(0.1 AS DOUBLE)), (10, CAST(0.1 AS DOUBLE)), (11, CAST(0.0 AS DOUBLE))) \
             SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day MEASURES CLASSIFIER() AS sym ALL ROWS PER MATCH \
             PATTERN (a{10} b) DEFINE b AS sum(a.x) = CAST(1 AS DOUBLE)) ORDER BY day",
        )
        .await?;
        assert_eq!(rows.last().map(String::as_str), Some("11|b"), "{rows:?}");
        assert_eq!(rows.len(), 11, "{rows:?}");
        Ok(())
    }

    #[tokio::test]
    async fn integer_and_decimal_sums_in_a_define() -> Result<()> {
        let ints = rows_of(
            "WITH t(day, n) AS (VALUES (1, 1), (2, 2), (3, 3), (4, 0)) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES CLASSIFIER() AS sym ALL ROWS PER MATCH PATTERN (a{3} b) DEFINE b AS sum(a.n) = 6) ORDER BY day",
        )
        .await?;
        assert_eq!(ints, ["1|a", "2|a", "3|a", "4|b"]);
        let decimals = rows_of(
            "WITH t(day, amt) AS (VALUES (1, CAST(0.10 AS DECIMAL(10,2))), (2, CAST(0.20 AS DECIMAL(10,2))), \
             (3, CAST(0.30 AS DECIMAL(10,2))), (4, CAST(0.00 AS DECIMAL(10,2)))) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES CLASSIFIER() AS sym ALL ROWS PER MATCH PATTERN (a{3} b) DEFINE b AS sum(a.amt) = 0.60) ORDER BY day",
        )
        .await?;
        assert_eq!(decimals, ["1|a", "2|a", "3|a", "4|b"]);
        Ok(())
    }

    #[tokio::test]
    async fn min_and_max_of_a_float_with_nan() -> Result<()> {
        // NaN is the largest: the min of 1, NaN and 2 is 1, and the max is NaN
        let rows = rows_of(
            "WITH t(day, x) AS (VALUES (1, CAST(1.0 AS DOUBLE)), (2, CAST('NaN' AS DOUBLE)), (3, CAST(2.0 AS DOUBLE)), \
             (4, CAST(0.0 AS DOUBLE))) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day MEASURES CLASSIFIER() AS sym \
             ALL ROWS PER MATCH PATTERN (a{3} b) DEFINE b AS min(a.x) = CAST(1 AS DOUBLE) AND CAST(max(a.x) AS VARCHAR) = 'NaN') \
             ORDER BY day",
        )
        .await?;
        assert_eq!(rows, ["1|a", "2|a", "3|a", "4|b"]);
        Ok(())
    }

    #[tokio::test]
    async fn match_number_counts_the_matches_so_far() -> Result<()> {
        // each row is a match of its own, and x is tested for the match number it would have, which is its day
        let rows = rows_of(
            "WITH t(day) AS (VALUES (1), (2), (3)) SELECT day, m FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES MATCH_NUMBER() AS m ALL ROWS PER MATCH PATTERN (x) DEFINE x AS MATCH_NUMBER() = day) ORDER BY day",
        )
        .await?;
        assert_eq!(rows, ["1|1", "2|2", "3|3"]);
        Ok(())
    }

    #[tokio::test]
    async fn a_navigation_of_equal_values_on_other_rows_is_kept_apart() -> Result<()> {
        // (d, a) and (a, d) reach the loop with LAST(a.x) = 2 on rows 2 and 1, equal values: NEXT(LAST(a.x)) is 5 after the
        // first and 2 after the second, so the answer is a d b, and merging on the value would give none
        let rows = rows_of(
            "WITH t(day, x) AS (VALUES (1, 2), (2, 2), (3, 5)) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES CLASSIFIER() AS sym ALL ROWS PER MATCH PATTERN ((d | a)+ b) DEFINE b AS NEXT(LAST(a.x)) = 2) ORDER BY day",
        )
        .await?;
        assert_eq!(rows, ["1|a", "2|d", "3|b"]);
        Ok(())
    }

    #[tokio::test]
    async fn within_with_a_define_that_counts_the_match() -> Result<()> {
        // a takes two days of each match: the third would make count(*) 3, and it is within the interval too
        let rows = rows_of(
            "WITH t(dt, n) AS (VALUES (DATE '2024-01-01', 1), (DATE '2024-01-02', 2), (DATE '2024-01-03', 3), (DATE '2024-01-04', 4)) \
             SELECT dt, m FROM t MATCH_RECOGNIZE (ORDER BY dt MEASURES MATCH_NUMBER() AS m ALL ROWS PER MATCH PATTERN (a+) \
             WITHIN INTERVAL '2' DAY DEFINE a AS count(*) < 3) ORDER BY dt",
        )
        .await?;
        assert_eq!(rows, ["2024-01-01|1", "2024-01-02|1", "2024-01-03|2", "2024-01-04|2"]);
        Ok(())
    }

    #[tokio::test]
    async fn the_cap_refuses_a_search_past_100000_partial_matches() -> Result<()> {
        // 600 rows: (a | b)+ c keeps a thread for each pair of counts, about i * i / 2 at row i, so past 100,000 by row 450
        let err = rows_of(
            "WITH t AS (SELECT value AS day FROM generate_series(1, 600)) SELECT n FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES count(*) AS n AFTER MATCH SKIP PAST LAST ROW PATTERN ((a | b)+ c) \
             DEFINE a AS count(a.*) >= 0, b AS count(b.*) >= 0, c AS count(c.*) < 0)",
        )
        .await
        .expect_err("the cap");
        assert!(err.to_string().contains("partial matches at once"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn a_glue_function_the_engine_lacks_is_refused_by_name() -> Result<()> {
        let err = rows_of(
            "WITH t(day, x) AS (VALUES (1, 1), (2, 2)) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES CLASSIFIER() AS sym ALL ROWS PER MATCH PATTERN (a+) DEFINE a AS frobnicate(LAST(a.x)) > 0) ORDER BY day",
        )
        .await
        .expect_err("a refusal");
        assert!(err.to_string().contains("MATCH_RECOGNIZE: DEFINE a: "), "{err}");
        Ok(())
    }

    #[test]
    fn a_slot_of_a_glue_reads_the_rows_of_its_variable() {
        // a count over b's rows of a match, and a navigation of a's last row, are two slots with their own variables
        let plan = compile(
            r#"{"pattern":"a b","skip":{"to":"past_last"},"defines":[{"symbol":1,"glue":"(__mr_s0 > __mr_s1)","slots":[{"index":0,"kind":"agg","fun":"count","var":1,"value":null},{"index":1,"kind":"nav","var":0,"first":false,"m":0,"offset":0,"value":0}]}],"rows":0,"values":1}"#,
        )
        .unwrap();
        assert_eq!(plan.slots.len(), 2);
        assert_eq!(plan.defines[0].slots, [0, 1]);
        assert_eq!(plan.slots[0].var(), Some(1));
        assert_eq!(plan.slots[1].value(), Some(0));
    }
}
