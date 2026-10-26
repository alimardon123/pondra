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
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, IntervalMonthDayNanoArray, ListArray, StringArray, StructArray, UInt32Array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::kernels::numeric::sub;
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, IntervalMonthDayNanoType, IntervalUnit, Schema, SchemaRef, TimeUnit};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::execution::{FunctionRegistry, SessionState};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::simplify::SimplifyContext;
use datafusion::logical_expr::type_coercion::functions::fields_with_udf;
use datafusion::logical_expr::expr::WindowFunction;
use datafusion::logical_expr::function::WindowFunctionSimplification;
use datafusion::logical_expr::{EmptyRelation, Expr, LogicalPlan, PartitionEvaluator, Projection, Signature, Volatility, WindowFunctionDefinition, WindowUDF, WindowUDFImpl};
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::expressions::Literal;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::prelude::SessionContext;
use serde::Deserialize;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::rc::Rc;
use std::sync::atomic::{self, AtomicBool};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The most partial matches a search holds at once when a DEFINE reads the match so far (the ADR's number): past it the
/// query is refused (54000). A test may use a smaller cap, through `Part`.
const STATE_CAP: usize = 100_000;

/// The most entries each of the planner's caches holds (the plans, and the glue by spec and types). A full cache is emptied
/// before an insert, so a node that sees more specs than this starts again rather than grow.
const CACHE_CAP: usize = 1_024;

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
    /// Open mode: the most rows any PREV reaches back (`pattern.rs` computes it). It sets how many rows a run keeps.
    #[serde(default)]
    look: usize,
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
    look: usize,
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
    let mut c = Compiler { code: vec![], parts: 0 };
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
        look: spec.look,
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

/// The parts a pattern may compile to, each emit counted. An empty group pushes no state, so STATES doesn't see a nest of
/// empty counts, which runs for hours; counting every part does. Past it the pattern is refused by name.
const PARTS: usize = 100_000;

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
    /// The parts emitted so far (see `PARTS`).
    parts: usize,
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
        self.parts += 1;
        if self.parts > PARTS {
            return Err(coded("54000", format!("MATCH_RECOGNIZE: the pattern repeats its parts more than {} times: use smaller counts in {{n,m}}", thousands(PARTS))));
        }
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

thread_local! {
    /// The path steps alive on this thread: a step adds one when it is made, and takes one off as it is freed (see
    /// `Step::new` and its `Drop`). A search runs on one thread, so a partition reads it as its own count.
    static LIVE_STEPS: Cell<usize> = const { Cell::new(0) };
}

/// The path steps alive on this thread (see `LIVE_STEPS`).
fn live_steps() -> usize {
    LIVE_STEPS.with(Cell::get)
}

impl Step {
    /// A step made now, counted as live until it is freed.
    fn new(sym: usize, excluded: bool, prev: Path) -> Rc<Step> {
        LIVE_STEPS.with(|c| c.set(c.get() + 1));
        Rc::new(Step { sym, excluded, prev })
    }
}

impl Drop for Step {
    /// Frees the steps before this one in a loop, not by recursion: a path of a million steps would overflow a 2 MB stack
    /// that way. A step another path still holds is left to that path.
    fn drop(&mut self) {
        LIVE_STEPS.with(|c| c.set(c.get() - 1));
        let mut next = self.prev.take();
        while let Some(step) = next {
            match Rc::try_unwrap(step) {
                Ok(mut owned) => next = owned.prev.take(),
                Err(_) => break,
            }
        }
    }
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

/// A sum, in the type it is added in: wrapping integers, a decimal's unscaled value, or a DOUBLE as fsum's (hi, lo) pair
/// (bits). A REAL is summed as a DOUBLE, since sum's argument is coerced to one. Floats are kept by their bits, so two sums
/// are equal when their bits are.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Num {
    Int(i64),
    Uint(u64),
    Dec(i128),
    Float(u64, u64),
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
/// value columns, the planned glue (when a DEFINE reads the match), the cap on the partial matches a search holds, the two
/// budgets (`keep`: the rows kept past n; `reads`: the rows read past `READS_PER_ROW` × n, the searches' own), and the
/// statement's stop flag when one is bound (`Matcher::stop`).
struct Part<'a> {
    cond: &'a [Vec<bool>],
    within: Option<&'a Within>,
    n: usize,
    rows: &'a [ArrayRef],
    values: &'a [ArrayRef],
    planned: Option<&'a Planned>,
    cap: usize,
    keep: usize,
    reads: usize,
    stop: Option<&'a AtomicBool>,
}

/// What is left of a partition's budgets (see `Part`) as its searches draw on them. `live` is the live steps when the
/// partition started, `keep` the rows it may still keep past those, and `reads` the rows its searches may still read.
struct Room {
    live: usize,
    keep: usize,
    reads: usize,
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

/// How a search ends: `Closed` answers its match, as `pondra_match` takes it. `Open` (open mode) also finds a search that a
/// later row could still change, and carries WITHIN's bound for the watermark (`low`, None when NULL).
#[derive(Clone, Copy)]
enum Ending {
    Closed,
    Open { low: Option<i64> },
}

/// A search's answer: its match, when it is decided, and the resume start of the search when it is open (`open_end`).
struct Searched {
    found: Option<Found>,
    resume: Option<usize>,
}

/// The most preferred match that starts at or after `from`: the Pike VM's leftmost answer. A row's threads run in
/// priority order; a match cuts the threads after it, and a higher-priority thread that matches later replaces it.
/// No new thread starts once a match is found.
/// `stamp` and `last` are the partition's, so a search costs its rows, not a clear of every state. With WITHIN a thread that
/// would take a row too late dies (`within_ok`), and the threads of each start are kept apart (`Seen`).
/// A row is taken in three phases: each thread at a symbol takes the row and decides its row-only conditions (phase 1);
/// each glue is evaluated once over its batch (phase 2); and the passing threads go on, in the order they came (phase 3).
/// `m` is MATCH_NUMBER(): the matches so far, plus one. `room` is the partition's budgets, which each row draws on (`Part`).
fn search(plan: &Plan, part: &Part, from: usize, stamp: &mut [usize], last: &mut usize, m: i64, ending: Ending, room: &mut Room) -> Result<Searched> {
    let code = &plan.code;
    let (n, w) = (part.n, part.within);
    let stated = !plan.defines.is_empty();
    let cap = if stated { part.cap } else { MAX_THREADS };
    let initial: Rc<State> = Rc::new(start_state(plan));
    let glued = part.planned.map_or(0, |p| p.glued.len());
    // the best match so far, as its start, its end and its path (built into a `Found` once, when the search returns)
    let mut best: Option<(usize, usize, Path)> = None;
    let mut now: Vec<Thread> = vec![];
    let mut next: Vec<Thread> = vec![];
    let mut decisions: Vec<Decision> = vec![];
    let mut now_reached = Reached::default();
    let mut gen = *last + 1;
    let mut i = from;
    loop {
        if is_stopped(part) {
            return Err(stopped());
        }
        // the budgets, once per row: a row read, and the rows kept (the live steps since the partition began)
        if i < n {
            if room.reads == 0 {
                return Err(reads_error(part));
            }
            room.reads -= 1;
        }
        if live_steps().saturating_sub(room.live) > room.keep {
            return Err(kept_error(part));
        }
        // (a new thread starts at this row last in line, so it is the least preferred)
        if best.is_none() && i < n {
            let mut seen = Seen::of(w, stated, stamp, gen, &mut now_reached);
            close(code, &mut now, &mut seen, 0, i, None, &initial, i, n);
        }
        // phase 1: the threads at a symbol, in order, up to the first that matches (`decisions` is empty: see the end of the row)
        let mut batch: Vec<Vec<usize>> = vec![vec![]; glued];
        for t in &now {
            match code[t.pc] {
                Inst::Match => {
                    best = Some((t.start, i, t.path.clone()));
                    break;
                }
                Inst::Sym(k, excluded) => {
                    if i < n && part.cond[k][i] && within_ok(w, t.start, i) {
                        let state = match stated {
                            true => Rc::new(take(&t.state, plan, part, k, i)?),
                            false => t.state.clone(),
                        };
                        let step = Step::new(k, excluded, t.path.clone());
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
        // (the row's decisions are freed here, and the threads go on in the buffer the row swapped out, emptied at once)
        decisions.clear();
        std::mem::swap(&mut now, &mut next);
        next.clear();
        now_reached = next_reached;
        gen = gen_next;
        i += 1;
    }
    *last = gen + 1;
    // open mode: at the partition's end, a search with a thread still alive can change, so it is open rather than decided
    let resume = match ending {
        Ending::Open { low } if i == n => open_end(&plan.code, part, &now, low),
        _ => None,
    };
    let found = if resume.is_some() { None } else { best.map(|(start, end, path)| Found::of(start, end, path)) };
    Ok(Searched { found, resume })
}

/// A count with its thousands set off by commas, as the cap messages print it (100000 is 100,000).
fn thousands(n: usize) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The refusal of a search past its cap. A pattern with no DEFINE that reads the match says what it said before; one that
/// does says to narrow the conditions, and to narrow WITHIN too when there is one.
fn cap_error(stated: bool, within: bool, cap: usize) -> anyhow::Error {
    let cap = thousands(cap);
    let msg = match (stated, within) {
        (false, _) => format!("MATCH_RECOGNIZE: the search holds more than {cap} partial matches at once: narrow WITHIN or the conditions"),
        (true, true) => format!("MATCH_RECOGNIZE: the search holds more than {cap} partial matches at once: narrow WITHIN or the conditions"),
        (true, false) => format!("MATCH_RECOGNIZE: the search holds more than {cap} partial matches at once: make the conditions narrower"),
    };
    coded("54000", msg)
}

/// The refusal of a partition that keeps more rows than its budget: n plus `keep` (see `Part`).
fn kept_error(part: &Part) -> anyhow::Error {
    let limit = thousands(part.n + part.keep);
    coded(
        "54000",
        format!("MATCH_RECOGNIZE: the search keeps more than {limit} rows of matches and partial matches: use AFTER MATCH SKIP PAST LAST ROW, or narrow WITHIN or the conditions"),
    )
}

/// The refusal of a partition whose searches read more rows than their budget: `READS_PER_ROW` × n plus `reads` (see `Part`).
fn reads_error(part: &Part) -> anyhow::Error {
    let limit = thousands(READS_PER_ROW * part.n + part.reads);
    coded(
        "54000",
        format!("MATCH_RECOGNIZE: the searches read more than {limit} rows, as the matches overlap or a preferred partial match runs on past them: use AFTER MATCH SKIP PAST LAST ROW, or bound the pattern with WITHIN or a count {{n,m}}"),
    )
}

/// The stop flag of the statement being planned (`audit::stop`). The page has no statements, so it has none.
#[cfg(feature = "lake")]
fn statement_stop() -> Option<Arc<AtomicBool>> { crate::audit::stop() }

#[cfg(not(feature = "lake"))]
fn statement_stop() -> Option<Arc<AtomicBool>> { None }

/// Whether the statement a search runs for has been stopped (a timeout or a cancel set its flag). Read once per row.
fn is_stopped(part: &Part) -> bool { part.stop.is_some_and(|s| s.load(atomic::Ordering::Relaxed)) }

/// The refusal of a search its statement stopped: 57014, as a statement's own stop says it.
fn stopped() -> anyhow::Error { coded("57014", "canceling statement: its MATCH_RECOGNIZE search was stopped") }

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
    // (the budgets start here: the live steps now are the base the kept steps are counted from)
    let mut room = Room { live: live_steps(), keep: n + part.keep, reads: READS_PER_ROW * n + part.reads };
    while from < n {
        if is_stopped(part) {
            return Err(stopped());
        }
        // (MATCH_NUMBER is the matches so far, plus one: the one this search finds)
        let Some(f) = search(plan, part, from, &mut stamp, &mut gen, m + 1, Ending::Closed, &mut room)?.found else { break };
        m += 1;
        from = record(plan, &mut rows, &f, m, &mut room)?;
    }
    Ok(rows)
}

/// Writes a found match's memberships into `rows`: one for each row of the match, or one for an empty match's row. Returns
/// where the search goes on, by the skip rule. Each membership is taken from the kept-rows budget (`room.keep`).
fn record(plan: &Plan, rows: &mut [Vec<Mem>], f: &Found, m: i64, room: &mut Room) -> Result<usize> {
    if f.start == f.end {
        rows[f.start].push(Mem { m, sym: None, seq: None, excluded: false });
        room.keep = room.keep.saturating_sub(1);
        return Ok(f.start + 1);
    }
    room.keep = room.keep.saturating_sub(f.steps.len());
    for (j, &(sym, excluded)) in f.steps.iter().enumerate() {
        rows[f.start + j].push(Mem { m, sym: Some(sym), seq: Some(j as i64 + 1), excluded });
    }
    skip(plan, f, m)
}

/// Open mode's inputs for one partition: its look-back rows (`k`), the matches the key found in earlier runs (`m`), and,
/// under WITHIN, the watermark's bound (`low`: WITHIN's low for W + 1 tick, None when NULL) and the kernel that subtracts
/// the interval (`kernel`).
struct OpenArgs {
    k: usize,
    m: i64,
    low: Option<i64>,
    kernel: Option<Kernel>,
}

/// Open mode's marker, the extra item on a partition's last row: how many of the last rows the next run keeps, how many of
/// those are look-back rows, the match count, and under WITHIN the least watermark at which the open search dies.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Marker {
    keep: i64,
    k: i64,
    next_m: i64,
    due: Option<i64>,
}

/// Open mode for one partition. Decided matches are written, and the first open search stops the loop, so nothing from its
/// resume start on gets a membership. A decided search with no match ends the loop too: the next run starts at the end.
/// Returns each row's memberships, and the marker.
fn partition_open(plan: &Plan, part: &Part, args: &OpenArgs) -> Result<(Vec<Vec<Mem>>, Marker)> {
    let n = part.n;
    let mut rows: Vec<Vec<Mem>> = vec![Vec::new(); n];
    let (mut from, mut m) = (args.k, args.m);
    let (mut stamp, mut gen) = (vec![0usize; plan.code.len()], 0usize);
    let mut room = Room { live: live_steps(), keep: n + part.keep, reads: READS_PER_ROW * n + part.reads };
    let mut resume = None;
    while from < n {
        if is_stopped(part) {
            return Err(stopped());
        }
        let s = search(plan, part, from, &mut stamp, &mut gen, m + 1, Ending::Open { low: args.low }, &mut room)?;
        if s.resume.is_some() {
            resume = s.resume;
            break;
        }
        let Some(f) = s.found else { break };
        m += 1;
        from = record(plan, &mut rows, &f, m, &mut room)?;
    }
    // the resume point is the open search's, or the end of the partition when no search is open
    let r = resume.unwrap_or(n);
    let keep_from = r.saturating_sub(plan.look);
    let due = match (resume, part.within) {
        (Some(s), Some(w)) => match w.time[s] {
            Some(t) => Some(due_of(args.kernel.as_ref().context("MATCH_RECOGNIZE: open mode's WITHIN has no kernel")?, t)?),
            None => None,
        },
        _ => None,
    };
    let marker = Marker { keep: (n - keep_from) as i64, k: (r - keep_from) as i64, next_m: m, due };
    Ok((rows, marker))
}

/// WITHIN's subtraction for one partition, as `within_of` runs it: the time column's type (a date's is a timestamp of
/// microseconds), the interval (one row, of its own type), and the interval's months, days and nanoseconds, which bound the
/// search `due_of` makes.
#[derive(Clone)]
struct Kernel {
    time: DataType,
    interval: ArrayRef,
    months: i32,
    days: i32,
    nanos: i64,
}

impl Kernel {
    /// The kernel for a WITHIN's time and interval columns (the first row of the interval, which is the same on every row).
    fn of(time: &ArrayRef, interval: &ArrayRef) -> Result<Kernel> {
        let time = timed(time)?.data_type().clone();
        let interval = interval.slice(0, 1);
        let (months, days, nanos) = interval_parts(&interval)?;
        Ok(Kernel { time, interval, months, days, nanos })
    }

    /// WITHIN's low for a time of `tick` ticks: the same subtraction `within_of` makes.
    fn low(&self, tick: i64) -> Result<Option<i64>> {
        let at = cast(&Int64Array::from(vec![tick]), &self.time)?;
        let low = sub(&at.as_ref(), &self.interval.as_ref())?;
        Ok(ticks(&low)?[0])
    }
}

/// Whether a thread that started at row `start` is alive at the partition's end: under WITHIN its start's time is no earlier
/// than `low`. A NULL time, or a NULL `low`, is never alive.
fn alive(part: &Part, start: usize, low: Option<i64>) -> bool {
    match part.within {
        None => true,
        Some(w) => matches!((w.time[start], low), (Some(t), Some(l)) if t >= l),
    }
}

/// Open mode's test at a search's end: the search is open when a thread that waits at a symbol is alive and comes before the
/// first match, or, with no match, when any alive thread is left. Returns the resume start: the least start of those threads.
fn open_end(code: &[Inst], part: &Part, now: &[Thread], low: Option<i64>) -> Option<usize> {
    let mut resume: Option<usize> = None;
    for t in now {
        match code[t.pc] {
            Inst::Match => break,
            Inst::Sym(..) if alive(part, t.start, low) => resume = Some(resume.map_or(t.start, |r| r.min(t.start))),
            _ => {}
        }
    }
    resume
}

/// `due`: the least watermark at which a thread that started at time `t` is dead. It is dead at W when `t` is before
/// `low(W + 1 tick)`, which only moves on, so the least is found by bisection on the tick. The bisection starts from `t`
/// (alive, for an interval of a tick or more) and runs up to a bound surely dead: a month is 31 days, a day 25 hours, the
/// nanoseconds as they are, and a day more.
fn due_of(kernel: &Kernel, t: i64) -> Result<i64> {
    let dead = |w: i128| -> Result<bool> {
        let tick = i64::try_from(w + 1).context("MATCH_RECOGNIZE: WITHIN's watermark is out of range")?;
        Ok(kernel.low(tick)?.is_none_or(|low| t < low))
    };
    let start = t as i128;
    if dead(start)? {
        return Ok(t);
    }
    const DAY_NS: i128 = 86_400_000_000_000;
    const HOUR_NS: i128 = 3_600_000_000_000;
    let bound_ns = kernel.months.unsigned_abs() as i128 * 31 * DAY_NS
        + kernel.days.unsigned_abs() as i128 * 25 * HOUR_NS
        + (kernel.nanos as i128).abs()
        + DAY_NS;
    let tick_ns = ns_per_tick(&kernel.time)?;
    let mut step = (bound_ns + tick_ns - 1) / tick_ns;
    let mut hi = start + step;
    let mut tries = 0;
    while !dead(hi)? {
        tries += 1;
        if tries > 64 {
            bail!("MATCH_RECOGNIZE: WITHIN's death point isn't found within 64 doublings");
        }
        step *= 2;
        hi = start + step;
    }
    let mut lo = start;
    while hi - lo > 1 {
        let mid = lo + (hi - lo) / 2;
        if dead(mid)? {
            hi = mid;
        } else {
            lo = mid;
        }
    }
    Ok(hi as i64)
}

/// Nanoseconds in one tick of a time type (its timestamp unit).
fn ns_per_tick(time: &DataType) -> Result<i128> {
    Ok(match time {
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        other => bail!("MATCH_RECOGNIZE: WITHIN's time is {other}, which has no tick"),
    })
}

/// An interval's months, days and nanoseconds, from its first row (zeros when it is NULL there).
fn interval_parts(interval: &ArrayRef) -> Result<(i32, i32, i64)> {
    if interval.is_empty() || interval.is_null(0) {
        return Ok((0, 0, 0));
    }
    let month_day_nano = cast(interval, &DataType::Interval(IntervalUnit::MonthDayNano))?;
    let values = month_day_nano
        .as_any()
        .downcast_ref::<IntervalMonthDayNanoArray>()
        .context("MATCH_RECOGNIZE: WITHIN's interval isn't an interval")?;
    Ok(IntervalMonthDayNanoType::to_parts(values.value(0)))
}

/// A WITHIN time column as its ticks' type: a date becomes a timestamp of microseconds, and a timestamp keeps its unit and zone.
fn timed(time: &ArrayRef) -> Result<ArrayRef> {
    Ok(match time.data_type() {
        DataType::Date32 | DataType::Date64 => cast(time, &DataType::Timestamp(TimeUnit::Microsecond, None))?,
        _ => time.clone(),
    })
}

/// The first row's time of a column, in the ticks of `like` (the time's type), None when it is NULL there.
fn tick_of(column: &ArrayRef, like: &DataType) -> Result<Option<i64>> {
    let at = timed(&cast(column, like)?)?;
    Ok(ticks(&at)?.first().copied().flatten())
}

/// An open-mode argument that is a whole number (k or m), read from its first row: the same on every row.
fn open_int(values: &[ArrayRef], at: usize, name: &str) -> Result<i64> {
    let column = values.get(at).with_context(|| format!("MATCH_RECOGNIZE: pondra_open has no {name}"))?;
    let ints = cast(column, &DataType::Int64)?;
    let ints = ints.as_any().downcast_ref::<Int64Array>().context("MATCH_RECOGNIZE: pondra_open's arguments are whole numbers")?;
    ints.iter().next().flatten().with_context(|| format!("MATCH_RECOGNIZE: pondra_open's {name} is NULL"))
}

/// The struct fields of an open-mode answer: a match's four, then the marker's four (NULL on a membership).
fn open_fields() -> Fields {
    Fields::from(vec![
        Field::new("match", DataType::Int64, true),
        Field::new("symbol", DataType::Utf8, true),
        Field::new("seq", DataType::Int64, true),
        Field::new("excluded", DataType::Boolean, true),
        Field::new("keep", DataType::Int64, true),
        Field::new("k", DataType::Int64, true),
        Field::new("next_m", DataType::Int64, true),
        Field::new("due", DataType::Int64, true),
    ])
}

fn open_struct_type() -> DataType { DataType::Struct(open_fields()) }

/// Open mode's items as a struct column: a membership (its marker fields NULL), the marker (its match, symbol and excluded
/// NULL, its seq -1), or nothing (a NULL struct, which `answer_open` never makes).
fn structs_open(items: &[(Option<Mem>, Option<Marker>)], syms: &[String]) -> Result<StructArray> {
    let valid = NullBuffer::from(items.iter().map(|(x, mk)| x.is_some() || mk.is_some()).collect::<Vec<bool>>());
    let m: Int64Array = items.iter().map(|(x, _)| x.map(|x| x.m)).collect();
    let sym: StringArray = items.iter().map(|(x, _)| x.and_then(|x| x.sym).map(|k| syms[k].as_str())).collect();
    let seq: Int64Array = items
        .iter()
        .map(|(x, mk)| match mk {
            Some(_) => Some(-1),
            None => x.and_then(|x| x.seq),
        })
        .collect();
    let excluded: BooleanArray = items.iter().map(|(x, _)| x.map(|x| x.excluded)).collect();
    let keep: Int64Array = items.iter().map(|(_, mk)| mk.map(|mk| mk.keep)).collect();
    let k: Int64Array = items.iter().map(|(_, mk)| mk.map(|mk| mk.k)).collect();
    let next_m: Int64Array = items.iter().map(|(_, mk)| mk.map(|mk| mk.next_m)).collect();
    let due: Int64Array = items.iter().map(|(_, mk)| mk.and_then(|mk| mk.due)).collect();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(m),
        Arc::new(sym),
        Arc::new(seq),
        Arc::new(excluded),
        Arc::new(keep),
        Arc::new(k),
        Arc::new(next_m),
        Arc::new(due),
    ];
    Ok(StructArray::try_new(open_fields(), arrays, Some(valid))?)
}

/// Open mode's answer: each row's memberships, and on the partition's last row the marker after them.
fn answer_open(rows: &[Vec<Mem>], marker: Option<Marker>, syms: &[String]) -> Result<ArrayRef> {
    let mut items: Vec<(Option<Mem>, Option<Marker>)> = vec![];
    let mut lens = vec![];
    for (i, r) in rows.iter().enumerate() {
        let before = items.len();
        items.extend(r.iter().map(|m| (Some(*m), None)));
        if i + 1 == rows.len() {
            items.extend(marker.map(|mk| (None, Some(mk))));
        }
        lens.push(items.len() - before);
    }
    let item = Arc::new(Field::new("item", open_struct_type(), true));
    let values: ArrayRef = Arc::new(structs_open(&items, syms)?);
    Ok(Arc::new(ListArray::try_new(item, OffsetBuffer::from_lengths(lens), values, None)?) as ArrayRef)
}

/// A match: its rows `start..end`, and each row's symbol and excluded flag in order.
struct Found {
    start: usize,
    end: usize,
    steps: Vec<(usize, bool)>,
}

impl Found {
    /// The match from row `start` to `end`, with the steps of its path (`path`), in order.
    fn of(start: usize, end: usize, path: Path) -> Found {
        let mut steps = vec![];
        let mut at = path;
        while let Some(s) = at {
            steps.push((s.sym, s.excluded));
            at = s.prev.clone();
        }
        steps.reverse();
        Found { start, end, steps }
    }
}

/// The most threads a search may hold at once: past it the search is refused (54000), as WITHIN keeps one per start.
const MAX_THREADS: usize = 1_000_000;

/// The rows a partition may keep past its own n, and the rows its searches may read past `READS_PER_ROW` × n (see `Part`).
/// Overlapping matches and stated threads that never merge keep or read a row many times over, so the work grows as the
/// square of the partition's rows; past this the query is refused (54000) rather than run for hours.
const KEEP: usize = 8_000_000;

/// The rows a partition's searches may read per row of the partition, before `KEEP`. A preferred partial match runs on
/// past the row it ends at, so the searches read more rows than the answer has.
const READS_PER_ROW: usize = 16;

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
    let time = timed(time)?;
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
    plan: Option<Arc<Plan>>,
    /// The spec's glue, planned against the input types, when a DEFINE reads the match.
    planned: Option<Arc<Planned>>,
    /// The statement's stop flag, when one is bound.
    stop: Option<Arc<AtomicBool>>,
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
        let plan = self.ready(values)?;
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
            keep: KEEP,
            reads: KEEP,
            stop: self.stop.as_deref(),
        };
        answer(&partition(&plan, &part)?, &plan.syms, self.all, plan.unmatched)
    }

    /// The spec's plan, read from the call's first value once, and its glue planned when a DEFINE reads the match.
    fn ready(&mut self, values: &[ArrayRef]) -> Result<Arc<Plan>> {
        if self.plan.is_none() {
            let text = spec_text(values.first().context("MATCH_RECOGNIZE: no spec")?)?;
            self.plan = Some(self.planner.plan(&text)?);
            self.text = text;
        }
        let plan = self.plan.clone().context("MATCH_RECOGNIZE: no plan")?;
        if !plan.defines.is_empty() && self.planned.is_none() {
            let types: Vec<DataType> = values.iter().map(|v| v.data_type().clone()).collect();
            self.planned = Some(self.planner.planned(&self.text, &plan, &types)?);
        }
        Ok(plan)
    }

    /// `pondra_open` over `n` rows: `run`'s values, then k, m and w, the watermark (in WITHIN's time's type). It answers in
    /// open mode (`partition_open`): the decided matches, and the marker on the last row.
    fn run_open(&mut self, values: &[ArrayRef], n: usize) -> Result<ArrayRef> {
        if n == 0 {
            return answer_open(&[], None, &[]);
        }
        let plan = self.ready(values)?;
        if plan.unmatched {
            return Err(coded("0A000", "MATCH_RECOGNIZE: an open search doesn't take WITH UNMATCHED ROWS"));
        }
        let mut cond = vec![];
        for k in 0..plan.syms.len() {
            cond.push(bools(values.get(k + 1).context("MATCH_RECOGNIZE: a symbol without its condition")?)?);
        }
        // WITHIN's time and interval come after the symbols' conditions
        let (within, kernel) = match plan.within {
            true => {
                let time = values.get(plan.syms.len() + 1).context("MATCH_RECOGNIZE: WITHIN has no time")?;
                let interval = values.get(plan.syms.len() + 2).context("MATCH_RECOGNIZE: WITHIN has no interval")?;
                (Some(within_of(time, interval)?), Some(Kernel::of(time, interval)?))
            }
            false => (None, None),
        };
        let first = 1 + plan.syms.len() + if plan.within { 2 } else { 0 };
        let rows = values.get(first..first + plan.rows).context("MATCH_RECOGNIZE: the glue's row columns are missing")?;
        let columns = values
            .get(first + plan.rows..first + plan.rows + plan.values)
            .context("MATCH_RECOGNIZE: the glue's value columns are missing")?;
        // the last three arguments: k, m, and the watermark w (unused without WITHIN)
        let at = first + plan.rows + plan.values;
        let k = usize::try_from(open_int(values, at, "k")?).context("MATCH_RECOGNIZE: pondra_open's k is a count of rows")?;
        let m = open_int(values, at + 1, "m")?;
        if k > n {
            return Err(coded("22000", "MATCH_RECOGNIZE: pondra_open's k is more rows than the partition has"));
        }
        let low = match &kernel {
            Some(kn) => {
                let w = values.get(at + 2).context("MATCH_RECOGNIZE: pondra_open has no watermark")?;
                let tick = tick_of(w, &kn.time)?.context("MATCH_RECOGNIZE: pondra_open's watermark is NULL")?;
                kn.low(tick.checked_add(1).context("MATCH_RECOGNIZE: pondra_open's watermark is out of range")?)?
            }
            None => None,
        };
        let part = Part {
            cond: &cond,
            within: within.as_ref(),
            n,
            rows,
            values: columns,
            planned: self.planned.as_deref(),
            cap: STATE_CAP,
            stop: self.stop.as_deref(),
        };
        let (found, marker) = partition_open(&plan, &part, &OpenArgs { k, m, low, kernel })?;
        answer_open(&found, Some(marker), &plan.syms)
    }
}

/// The evaluator of `pondra_open`: an `Evaluator` answered in open mode.
#[derive(Debug)]
struct OpenEvaluator(Evaluator);

impl PartitionEvaluator for OpenEvaluator {
    fn uses_window_frame(&self) -> bool { false }

    fn supports_bounded_execution(&self) -> bool { false }

    fn evaluate_all(&mut self, values: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
        self.0.run_open(values, n).map_err(external)
    }
}

/// Plans the glue of specs against the engine's functions, and keeps each spec's plan. `base` is the session's snapshot that
/// `register` takes. The plan is kept per spec text, and the glue per spec text and input types, so the partitions of one
/// query compile and plan once. No lock is held while a spec compiles or plans: two threads may each do the work, and the
/// one that lands first is kept (`remember`).
struct Planner {
    base: Arc<SessionState>,
    plans: Mutex<HashMap<String, Arc<Plan>>>,
    cache: Mutex<HashMap<(String, Vec<DataType>), Arc<Planned>>>,
}

impl std::fmt::Debug for Planner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Planner")
    }
}

impl Planner {
    fn new(base: Arc<SessionState>) -> Planner {
        Planner { base, plans: Mutex::new(HashMap::new()), cache: Mutex::new(HashMap::new()) }
    }

    /// The plan of a spec, compiled once: from the cache when it is there.
    fn plan(&self, text: &str) -> Result<Arc<Plan>> {
        if let Some(p) = lock(&self.plans).get(text) {
            return Ok(p.clone());
        }
        let p = Arc::new(compile(text)?);
        Ok(remember(&self.plans, text.to_string(), p))
    }

    /// The spec's glue planned for these input types (see `plan_glue`), from the cache when it is there.
    fn planned(&self, text: &str, plan: &Plan, types: &[DataType]) -> Result<Arc<Planned>> {
        let key = (text.to_string(), types.to_vec());
        if let Some(p) = lock(&self.cache).get(&key) {
            return Ok(p.clone());
        }
        let p = Arc::new(plan_glue(&self.base, plan, types)?);
        Ok(remember(&self.cache, key, p))
    }
}

/// Puts `value` under `key` in a cache of at most `CACHE_CAP` entries: a full cache is emptied first. Returns the value the
/// cache keeps, which is the one already there when another thread put one in first.
fn remember<K: Eq + Hash, V: Clone>(cache: &Mutex<HashMap<K, V>>, key: K, value: V) -> V {
    let mut map = lock(cache);
    if map.len() >= CACHE_CAP && !map.contains_key(&key) {
        map.clear();
    }
    map.entry(key).or_insert(value).clone()
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
    // An async function (a Python or external one, file_read) runs only as a plan step of its own, which a glue hasn't.
    let mut called = None;
    expr.apply(|e| {
        if let Expr::ScalarFunction(f) = e {
            if f.func.as_async().is_some() {
                called = Some(f.func.name().to_string());
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    if let Some(name) = called {
        return Err(coded("0A000", format!("{name} is a Python or external function, which a DEFINE that reads the match (FIRST, LAST, an aggregate) can't call yet: call it on the row's own values, as {name}(b.price, PREV(b.price))")));
    }
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
                            (DataType::Int64 | DataType::UInt64 | DataType::Float64, _) => true,
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
/// that may share rows). The planner is the session's snapshot, and `stop` the statement's flag once bound (see `simplify`);
/// neither is part of a function's identity, so the bound and unbound functions are equal.
#[derive(Debug)]
struct Matcher {
    all: bool,
    signature: Signature,
    planner: Arc<Planner>,
    stop: Option<Arc<AtomicBool>>,
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

    /// Binds the statement's stop flag. The optimizer plans a statement in the statement's own task, where `audit::stop` is
    /// its flag, so the function a statement plans carries it into its search. The bound function has nothing to simplify,
    /// so the optimizer reaches its fixpoint. Outside a statement nothing changes.
    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        if self.stop.is_some() {
            return None;
        }
        let stop = statement_stop()?;
        let (all, signature, planner) = (self.all, self.signature.clone(), self.planner.clone());
        Some(Box::new(move |mut wf: WindowFunction, _: &SimplifyContext| -> DfResult<Expr> {
            let bound = Matcher { all, signature: signature.clone(), planner: planner.clone(), stop: Some(stop.clone()) };
            wf.fun = WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::from(bound)));
            Ok(Expr::WindowFunction(Box::new(wf)))
        }))
    }

    fn partition_evaluator(&self, args: PartitionEvaluatorArgs) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(evaluator_of(self.all, &self.planner, &self.stop, args)?))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let t = match self.all {
            true => DataType::List(Arc::new(Field::new("item", struct_type(), true))),
            false => struct_type(),
        };
        Ok(Arc::new(Field::new(field_args.name(), t, true)))
    }
}

/// The evaluator of a call of `pondra_match`, `pondra_matches` or `pondra_open`: the spec's plan and glue, planned here when the
/// spec is a literal (so a bad one is refused at planning), and the statement's stop flag.
fn evaluator_of(all: bool, planner: &Arc<Planner>, stop: &Option<Arc<AtomicBool>>, args: PartitionEvaluatorArgs) -> DfResult<Evaluator> {
    let types: Vec<DataType> = args.input_fields().iter().map(|f| f.data_type().clone()).collect();
    // (the spec is a literal, as `pattern.rs` writes it: planned here, from the planner's cache; another form is read at the first row)
    let literal = match args.input_exprs().first().and_then(|e| e.downcast_ref::<Literal>()).map(|l| l.value()) {
        Some(ScalarValue::Utf8(Some(text))) => Some(text.clone()),
        _ => None,
    };
    let mut evaluator = Evaluator { all, planner: planner.clone(), text: String::new(), plan: None, planned: None, stop: stop.clone() };
    if let Some(text) = literal {
        let plan = planner.plan(&text).map_err(external)?;
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
            evaluator.planned = Some(planner.planned(&text, &plan, &types).map_err(external)?);
        }
        evaluator.text = text;
        evaluator.plan = Some(plan);
    }
    Ok(evaluator)
}

/// `pondra_open`: the matcher in open mode (`partition_open`). It takes `pondra_matches`' arguments, then k, m and w, and
/// answers a list per row, the marker on the partition's last row.
#[derive(Debug)]
struct OpenMatcher {
    signature: Signature,
    planner: Arc<Planner>,
    stop: Option<Arc<AtomicBool>>,
}

impl PartialEq for OpenMatcher {
    fn eq(&self, other: &Self) -> bool {
        self.signature == other.signature
    }
}

impl Eq for OpenMatcher {}

impl Hash for OpenMatcher {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
    }
}

impl WindowUDFImpl for OpenMatcher {
    fn name(&self) -> &str { "pondra_open" }

    fn signature(&self) -> &Signature { &self.signature }

    /// Binds the statement's stop flag, as `Matcher::simplify` does.
    fn simplify(&self) -> Option<WindowFunctionSimplification> {
        if self.stop.is_some() {
            return None;
        }
        let stop = crate::audit::stop()?;
        let (signature, planner) = (self.signature.clone(), self.planner.clone());
        Some(Box::new(move |mut wf: WindowFunction, _: &SimplifyContext| -> DfResult<Expr> {
            let bound = OpenMatcher { signature: signature.clone(), planner: planner.clone(), stop: Some(stop.clone()) };
            wf.fun = WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::from(bound)));
            Ok(Expr::WindowFunction(Box::new(wf)))
        }))
    }

    fn partition_evaluator(&self, args: PartitionEvaluatorArgs) -> DfResult<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(OpenEvaluator(evaluator_of(true, &self.planner, &self.stop, args)?)))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let t = DataType::List(Arc::new(Field::new("item", open_struct_type(), true)));
        Ok(Arc::new(Field::new(field_args.name(), t, true)))
    }
}

/// Registers `pondra_match`, `pondra_matches`, `pondra_open` and `pondra_nav` with a session (`engine::session`, for every form). A DEFINE's
/// glue is planned against the session as it stands here, so it sees the functions registered before this call and no later
/// one. A lake's statement that reads a pattern registers it again, last (`query::session_at`), so its DEFINEs see the AI,
/// lake, Python and per-session functions too.
pub fn register(ctx: &SessionContext) {
    let planner = Arc::new(Planner::new(Arc::new(ctx.state())));
    for all in [false, true] {
        let signature = Signature::variadic_any(Volatility::Immutable);
        ctx.register_udwf(WindowUDF::from(Matcher { all, signature, planner: planner.clone(), stop: None }));
    }
    let signature = Signature::variadic_any(Volatility::Immutable);
    ctx.register_udwf(WindowUDF::from(OpenMatcher { signature, planner: planner.clone(), stop: None }));
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
    use std::time::{Duration, Instant};

    /// The rows' conditions: `rows[r]` lists the symbols row r meets.
    fn conds(rows: &[Vec<usize>], syms: usize) -> Vec<Vec<bool>> {
        (0..syms).map(|k| rows.iter().map(|r| r.contains(&k)).collect()).collect()
    }

    /// Each row's memberships as text: `1a2` (match 1, symbol a, place 2), `1-` for an empty match's row.
    fn shown(spec: &str, rows: &[Vec<usize>]) -> Result<Vec<String>> {
        let plan = compile(spec)?;
        let cond = conds(rows, plan.syms.len());
        let part = Part { cond: &cond, within: None, n: rows.len(), rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: None };
        Ok(texts(&plan, &partition(&plan, &part)?))
    }

    /// `shown` under WITHIN: `times` are the rows' times (None is NULL), and each row must be within `interval` of its
    /// match's first row.
    fn shown_within(pattern: &str, rows: &[Vec<usize>], times: &[Option<i64>], interval: i64) -> Vec<String> {
        let plan = compile(&within_spec(pattern)).unwrap();
        let low = times.iter().map(|t| t.map(|t| t - interval)).collect();
        let w = Within { time: times.to_vec(), low };
        let cond = conds(rows, plan.syms.len());
        let part = Part { cond: &cond, within: Some(&w), n: rows.len(), rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: None };
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

    const NEXT: &str = r#"{"to":"next_row"}"#;

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
    fn a_nest_of_empty_counts_is_refused_by_its_parts() {
        // () is an empty group, which pushes no state: four counts of 1000 nest it to 10^12 empty repeats, so the parts are counted
        let started = Instant::now();
        let err = compile(&spec("(((((()){1000}){1000}){1000}){1000} a)", PAST)).expect_err("a refusal");
        assert_eq!(crate::codes::of(&err), "54000");
        assert!(err.to_string().contains("repeats its parts more than 100,000 times"), "{err}");
        assert!(started.elapsed() < Duration::from_secs(1), "refused in {:?}", started.elapsed());
    }

    #[test]
    fn a_pattern_of_8000_states_still_compiles() {
        let plan = compile(&spec("(a b){4000}", PAST)).unwrap();
        assert_eq!(plan.code.len(), 8_001);
    }

    /// `a+` with the default skip over `n` rows that are all `a`: the partition's memberships, and how long it took.
    fn greedy_run(n: usize) -> (Vec<Vec<Mem>>, Duration) {
        let plan = compile(&spec("a+", PAST)).unwrap();
        let cond = vec![vec![true; n]];
        let part = Part { cond: &cond, within: None, n, rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: None };
        let started = Instant::now();
        let rows = partition(&plan, &part).unwrap();
        (rows, started.elapsed())
    }

    /// Whether the memberships are one match of every row: each row is its one membership, the match's first, at place j+1.
    fn one_match(rows: &[Vec<Mem>]) -> bool {
        rows.iter().enumerate().all(|(j, r)| r.len() == 1 && r[0].m == 1 && r[0].sym == Some(0) && r[0].seq == Some(j as i64 + 1))
    }

    #[test]
    fn a_long_greedy_match_is_built_once() {
        // a+ reaches its match on every row: built once, when the search returns, 200,000 rows take well under a second
        // (built on every row, they took hours; the bound leaves room for CI, which runs the tests side by side)
        let (rows, took) = greedy_run(200_000);
        assert!(one_match(&rows));
        assert!(took < Duration::from_secs(5), "took {took:?}");
    }

    #[test]
    fn a_greedy_match_of_a_million_rows_is_one_match() {
        let (rows, _) = greedy_run(1_000_000);
        assert!(one_match(&rows));
    }

    #[test]
    fn overlapping_matches_hit_the_kept_rows_budget() {
        // a+ with SKIP TO NEXT ROW over 10,000 rows of a: each search's match is every row from its start, so the kept rows
        // pass n + KEEP after about 840 searches, and the refusal comes in about a second (it ran for minutes before; the
        // bound leaves room for CI, which runs the tests side by side)
        let plan = compile(&spec("a+", NEXT)).unwrap();
        let n = 10_000;
        let cond = vec![vec![true; n]];
        let part = Part { cond: &cond, within: None, n, rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: None };
        let started = Instant::now();
        let err = partition(&plan, &part).expect_err("the kept rows' refusal");
        let took = started.elapsed();
        assert_eq!(crate::codes::of(&err), "54000");
        assert!(err.to_string().contains("keeps more than 8,010,000 rows of matches and partial matches"), "{err}");
        assert!(took < Duration::from_secs(10), "refused in {took:?}");
    }

    #[test]
    fn a_partial_match_that_runs_on_hits_the_rows_read_budget() {
        // a b+ c | a with a and b true on every row and c on none: each search runs to the partition's end for the first
        // branch, and takes the one-row a, so the rows read pass 16 × n + KEEP while the kept rows stay few
        let plan = compile(&spec("a b+ c | a", PAST)).unwrap();
        let n = 20_000;
        let cond = vec![vec![true; n], vec![true; n], vec![false; n]];
        let part = Part { cond: &cond, within: None, n, rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: None };
        let started = Instant::now();
        let err = partition(&plan, &part).expect_err("the rows read's refusal");
        let took = started.elapsed();
        assert_eq!(crate::codes::of(&err), "54000");
        assert!(err.to_string().contains("the searches read more than 8,320,000 rows"), "{err}");
        assert!(took < Duration::from_secs(10), "refused in {took:?}"); // (as the kept rows' test: room for CI)
    }

    #[test]
    fn stated_threads_that_never_merge_hit_the_kept_rows_budget() {
        // a+ b, with a DEFINE that counts a's rows (true on every row) and b never true: each start keeps a thread that
        // never merges with another, so the threads' paths grow as the square of the rows. With keep at 1,000 that is
        // refused past n + 1,000 kept rows.
        let text = r#"{"pattern":"a+ b","skip":{"to":"past_last"},"defines":[{"symbol":0,"glue":"(__mr_s0 < 1000000000)","slots":[{"index":0,"kind":"agg","fun":"count","var":0,"value":null}]}],"rows":0,"values":0}"#;
        let plan = compile(text).unwrap();
        let planned = planner().planned(text, &plan, &[DataType::Utf8, DataType::Boolean, DataType::Boolean]).unwrap();
        let n = 200;
        let cond = vec![vec![true; n], vec![false; n]];
        let part = Part { cond: &cond, within: None, n, rows: &[], values: &[], planned: Some(&*planned), cap: STATE_CAP, keep: 1_000, reads: super::KEEP, stop: None };
        let err = partition(&plan, &part).expect_err("the kept rows' refusal");
        assert_eq!(crate::codes::of(&err), "54000");
        assert!(err.to_string().contains("keeps more than 1,200 rows of matches and partial matches"), "{err}");
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
        rows_in(&ctx, &crate::pattern::rewritten(sql)?).await
    }

    /// The rows of a rewritten query (`pattern::rewritten`) that runs in `ctx`, as `rows_of` gives them.
    async fn rows_in(ctx: &SessionContext, sql: &str) -> Result<Vec<String>> {
        let batches = ctx.sql(sql).await?.collect().await?;
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

    #[test]
    fn the_caps_print_with_commas() {
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(100_000), "100,000");
        assert_eq!(thousands(1_000_000), "1,000,000");
        let msg = cap_error(true, false, STATE_CAP).to_string();
        assert!(msg.contains("more than 100,000 partial matches at once"), "{msg}");
        let msg = cap_error(false, false, MAX_THREADS).to_string();
        assert!(msg.contains("more than 1,000,000 partial matches at once"), "{msg}");
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
        assert!(err.to_string().contains("more than 100,000 partial matches at once"), "{err}");
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

    /// A planner over a session with nothing registered but DataFusion's own functions.
    fn planner() -> Planner { Planner::new(Arc::new(SessionContext::new().state())) }

    #[test]
    fn a_search_stops_when_its_statement_flag_is_set() {
        // a flag set before the search runs stops it at its first row, with 57014; unset, the same search answers
        let text = spec("a", PAST);
        let values: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![text.as_str()])), Arc::new(BooleanArray::from(vec![true; 3]))];
        let stop = Arc::new(AtomicBool::new(true));
        let mut stopped = Evaluator { all: false, planner: Arc::new(planner()), text: String::new(), plan: None, planned: None, stop: Some(stop.clone()) };
        let Err(err) = stopped.run(&values, 3) else { panic!("a stopped search answered") };
        assert_eq!(crate::codes::of(&err), "57014");
        assert!(err.to_string().contains("its MATCH_RECOGNIZE search was stopped"), "{err}");
        stop.store(false, atomic::Ordering::Relaxed);
        let mut free = Evaluator { all: false, planner: Arc::new(planner()), text: String::new(), plan: None, planned: None, stop: Some(stop) };
        assert!(free.run(&values, 3).is_ok());
    }

    #[test]
    fn a_search_stopped_from_another_thread_ends_at_its_next_row() {
        // (a b* c) with c true on the last row only: the search from each row reads to the partition's end, so the partition
        // takes about n² / 2 rows. (With c never true there is one linear search, which ends by itself.) The flag set 20 ms
        // in, from another thread, ends the search at its next row.
        let n = 4_000;
        let plan = compile(&spec("a b* c", NEXT)).unwrap();
        let cond = vec![vec![true; n], vec![true; n], (0..n).map(|i| i + 1 == n).collect::<Vec<bool>>()];
        let stop = Arc::new(AtomicBool::new(false));
        let setter = {
            let stop = stop.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(20));
                let at = Instant::now();
                stop.store(true, atomic::Ordering::Relaxed);
                at
            })
        };
        let part = Part { cond: &cond, within: None, n, rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: Some(&*stop) };
        let Err(err) = partition(&plan, &part) else { panic!("a stopped search answered") };
        let returned = Instant::now();
        let set_at = setter.join().unwrap();
        assert_eq!(crate::codes::of(&err), "57014");
        let latency = returned.duration_since(set_at);
        // the same search with no flag bound, which runs to its end
        let free = Part { cond: &cond, within: None, n, rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: None };
        let start = Instant::now();
        let answered = partition(&plan, &free).is_ok();
        let full = start.elapsed();
        assert!(answered);
        assert!(latency < full / 2, "a stopped search ({latency:?}) should end well before the whole one ({full:?})");
    }

    #[tokio::test]
    async fn simplify_binds_the_statements_flag_once() {
        let m = Matcher { all: false, signature: Signature::variadic_any(Volatility::Immutable), planner: Arc::new(planner()), stop: None };
        assert!(m.simplify().is_none(), "nothing is bound outside a statement");
        let flag = Arc::new(AtomicBool::new(false));
        crate::audit::stoppable(flag.clone(), async {
            let simplify = m.simplify().expect("a statement binds its flag");
            let unbound = Matcher { all: false, signature: m.signature.clone(), planner: m.planner.clone(), stop: None };
            let call = WindowFunction::new(WindowFunctionDefinition::WindowUDF(Arc::new(WindowUDF::from(unbound))), vec![]);
            let Expr::WindowFunction(out) = simplify(call, &SimplifyContext::builder().build()).expect("simplified") else {
                panic!("a window function")
            };
            let WindowFunctionDefinition::WindowUDF(bound) = &out.fun else { panic!("a window UDF") };
            assert!(bound.simplify().is_none(), "the bound function has nothing left to simplify");
        })
        .await;
    }

    #[tokio::test]
    async fn a_query_whose_flag_is_set_stops_its_search() -> Result<()> {
        // the flag is bound when the query plans, so the search stops on the flag its statement set, and answers unset
        let sql = crate::pattern::rewritten(
            "WITH t(day) AS (VALUES (1), (2), (3)) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day MEASURES CLASSIFIER() AS sym \
             ALL ROWS PER MATCH PATTERN (a+) DEFINE a AS day > 0) ORDER BY day",
        )?;
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let set = Arc::new(AtomicBool::new(true));
        let stopped = crate::audit::stoppable(set, async {
            let df = ctx.sql(&sql).await?;
            df.collect().await
        })
        .await;
        let Err(err) = stopped else { panic!("a stopped query answered") };
        assert!(err.to_string().contains("canceling statement: its MATCH_RECOGNIZE search was stopped"), "{err}");
        let free = crate::audit::stoppable(Arc::new(AtomicBool::new(false)), async {
            let df = ctx.sql(&sql).await?;
            df.collect().await
        })
        .await?;
        assert_eq!(free.iter().map(|b| b.num_rows()).sum::<usize>(), 3);
        Ok(())
    }

    #[test]
    fn a_spec_is_compiled_once_for_all_its_partitions() {
        let planner = planner();
        let text = spec("a b", PAST);
        let first = planner.plan(&text).unwrap();
        assert!(Arc::ptr_eq(&first, &planner.plan(&text).unwrap()));
    }

    #[test]
    fn the_planner_caches_stay_under_their_cap_and_answer_the_same() {
        let planner = planner();
        let text = spec("a b", PAST);
        let rows = vec![vec![0], vec![0, 1], vec![1]];
        let before = shown(&text, &rows).unwrap();
        // (CACHE_CAP + 1 spellings of one spec: the same plan, under a key of its own each)
        for i in 0..=CACHE_CAP {
            planner.plan(&format!("{}{}", text, " ".repeat(i))).unwrap();
        }
        assert!(lock(&planner.plans).len() <= CACHE_CAP);
        // (the same for the glue: a DEFINE of row-only columns, spelled CACHE_CAP + 1 ways)
        let glue = |i: usize| format!(r#"{{"pattern":"a","skip":{},"defines":[{{"symbol":0,"glue":"__mr_r0 > 0{}","slots":[]}}],"rows":1}}"#, PAST, " ".repeat(i));
        let types = [DataType::Utf8, DataType::Boolean, DataType::Int64];
        for i in 0..=CACHE_CAP {
            let t = glue(i);
            let plan = planner.plan(&t).unwrap();
            planner.planned(&t, &plan, &types).unwrap();
        }
        assert!(lock(&planner.cache).len() <= CACHE_CAP);
        assert!(lock(&planner.plans).len() <= CACHE_CAP);
        let plan = planner.plan(&text).unwrap();
        let cond = conds(&rows, plan.syms.len());
        let part = Part { cond: &cond, within: None, n: rows.len(), rows: &[], values: &[], planned: None, cap: STATE_CAP, keep: super::KEEP, reads: super::KEEP, stop: None };
        assert_eq!(texts(&plan, &partition(&plan, &part).unwrap()), before);
    }

    #[tokio::test]
    async fn a_define_sees_the_ai_functions_the_node_registers() -> Result<()> {
        // The matcher registered after the AI functions, as `query::session_at` does, so the glue plans cosine_similarity
        let ctx = crate::store::functions(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        crate::matcher::register(&ctx);
        let sql = crate::pattern::rewritten(
            "WITH t(day, emb) AS (VALUES (1, [1.0, 0.0]), (2, [0.9, 0.1])) SELECT day, sym FROM t MATCH_RECOGNIZE (ORDER BY day \
             MEASURES CLASSIFIER() AS sym ALL ROWS PER MATCH PATTERN (a b) DEFINE b AS cosine_similarity(b.emb, FIRST(a.emb)) > 0.9) \
             ORDER BY day",
        )?;
        assert_eq!(rows_in(&ctx, &sql).await?, ["1|a", "2|b"]);
        Ok(())
    }

    // Open mode (ADR-073 step 8a): the decided rule, the marker, WITHIN's death rule, and the cut property.

    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::datatypes::IntervalMonthDayNano;

    /// A (row's symbols, its members) key of a membership: the match, the row, the symbol, the place and `excluded`.
    type Key = (i64, usize, Option<usize>, Option<i64>, bool);

    fn key_of(m: &Mem, row: usize) -> Key { (m.m, row, m.sym, m.seq, m.excluded) }

    /// A spec in open mode: `pattern` with `skip` (JSON), and a look-back of `look` rows.
    fn open_spec(pattern: &str, skip: &str, look: usize) -> String {
        format!(r#"{{"pattern":"{pattern}","skip":{skip},"look":{look}}}"#)
    }

    /// Open mode over `rows` (each row's symbols), from `k` look-back rows and `m` matches before: each row's memberships
    /// as text, and the marker.
    fn open_of(spec: &str, rows: &[Vec<usize>], k: usize, m: i64) -> Result<(Vec<String>, Marker)> {
        let plan = compile(spec)?;
        let cond = conds(rows, plan.syms.len());
        let part = Part { cond: &cond, within: None, n: rows.len(), rows: &[], values: &[], planned: None, cap: STATE_CAP, stop: None };
        let (found, marker) = partition_open(&plan, &part, &OpenArgs { k, m, low: None, kernel: None })?;
        Ok((texts(&plan, &found), marker))
    }

    /// The microseconds of a UTC date and time, which is a timestamp's ticks.
    fn micros(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> i64 {
        chrono::NaiveDate::from_ymd_opt(y, mo, d).unwrap().and_hms_opt(h, mi, 0).unwrap().and_utc().timestamp_micros()
    }

    /// A kernel for one interval (`value`) over a microsecond time with `zone` (None: no zone).
    fn kernel_for(zone: Option<&str>, value: IntervalMonthDayNano) -> Kernel {
        let time: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0i64]).with_timezone_opt(zone));
        let interval: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![value]));
        Kernel::of(&time, &interval).expect("a kernel")
    }

    /// Whether a thread that started at time `t` is dead at watermark `w` (WITHIN's death rule).
    fn dead_at(kernel: &Kernel, t: i64, w: i64) -> bool {
        kernel.low(w + 1).expect("a low").is_none_or(|low| t < low)
    }

    #[test]
    fn open_a_b_plus_ending_on_a_b_is_open() -> Result<()> {
        // the thread in b+ may take one more row, so nothing is written and the resume is row 0
        let (rows, marker) = open_of(&open_spec("a b+", PAST, 0), &[vec![0], vec![1], vec![1]], 0, 0)?;
        assert_eq!(rows, ["", "", ""]);
        assert_eq!(marker, Marker { keep: 3, k: 0, next_m: 0, due: None });
        Ok(())
    }

    #[test]
    fn open_a_reluctant_b_plus_ending_on_a_b_is_decided() -> Result<()> {
        // b+? prefers to stop, so a b is decided at row 1 and written, and nothing is left open
        let (rows, marker) = open_of(&open_spec("a b+?", PAST, 0), &[vec![0], vec![1]], 0, 0)?;
        assert_eq!(rows, ["1a1", "1b2"]);
        assert_eq!(marker, Marker { keep: 0, k: 0, next_m: 1, due: None });
        Ok(())
    }

    #[test]
    fn open_an_alternation_ending_on_a_waits_for_the_preferred_match() -> Result<()> {
        // (a b | a) on a: the preferred a b may still match, so the search is open and its match is not written
        let (rows, marker) = open_of(&open_spec("(a b | a)", PAST, 0), &[vec![0]], 0, 0)?;
        assert_eq!(rows, [""]);
        assert_eq!(marker, Marker { keep: 1, k: 0, next_m: 0, due: None });
        Ok(())
    }

    #[test]
    fn open_a_star_with_a_false_row_is_decided_as_an_empty_match() -> Result<()> {
        let (rows, marker) = open_of(&open_spec("a*", PAST, 0), &[vec![]], 0, 0)?;
        assert_eq!(rows, ["1-"]);
        assert_eq!(marker, Marker { keep: 0, k: 0, next_m: 1, due: None });
        Ok(())
    }

    #[test]
    fn open_no_match_and_no_thread_left_is_decided_with_the_resume_at_the_end() -> Result<()> {
        let (rows, marker) = open_of(&open_spec("a", PAST, 0), &[vec![1]], 0, 0)?;
        assert_eq!(rows, [""]);
        assert_eq!(marker, Marker { keep: 0, k: 0, next_m: 0, due: None });
        Ok(())
    }

    #[test]
    fn open_the_marker_keeps_the_look_back_rows_when_everything_is_decided() -> Result<()> {
        // two matches of one row each, and a look-back of two: the resume is the end, so the next run keeps the last two rows
        let (rows, marker) = open_of(&open_spec("a", PAST, 2), &[vec![0], vec![0], vec![1]], 0, 0)?;
        assert_eq!(rows, ["1a1", "2a1", ""]);
        assert_eq!(marker, Marker { keep: 2, k: 2, next_m: 2, due: None });
        Ok(())
    }

    #[test]
    fn open_the_marker_keeps_the_rows_from_the_look_back_before_an_open_search() -> Result<()> {
        // a b+ with a look-back of one: the search from row 0 dies at row 1, and the one from row 1 is alive at the end
        let (rows, marker) = open_of(&open_spec("a b+", PAST, 1), &[vec![0], vec![0], vec![1], vec![1]], 0, 0)?;
        assert_eq!(rows, ["", "", "", ""]);
        assert_eq!(marker, Marker { keep: 4, k: 1, next_m: 0, due: None });
        Ok(())
    }

    #[test]
    fn open_the_marker_carries_the_watermark_at_which_the_open_thread_dies() -> Result<()> {
        // a b+ within INTERVAL '1' MONTH: the thread from Jan 31 is alive at a watermark of Feb 1 noon, and dies at Mar 1
        let spec = format!(r#"{{"pattern":"a b+","skip":{PAST},"within":true}}"#);
        let plan = compile(&spec)?;
        let time: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![micros(2026, 1, 31, 0, 0), micros(2026, 2, 1, 0, 0)]));
        let interval: ArrayRef = Arc::new(IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(1, 0, 0); 2]));
        let kernel = Kernel::of(&time, &interval)?;
        let watermark = micros(2026, 2, 1, 12, 0);
        let args = OpenArgs { k: 0, m: 0, low: kernel.low(watermark + 1)?, kernel: Some(kernel) };
        let cond = vec![vec![true, false], vec![false, true]];
        let within = within_of(&time, &interval)?;
        let part = Part { cond: &cond, within: Some(&within), n: 2, rows: &[], values: &[], planned: None, cap: STATE_CAP, stop: None };
        let (_, marker) = partition_open(&plan, &part, &args)?;
        assert_eq!(marker, Marker { keep: 2, k: 0, next_m: 0, due: Some(micros(2026, 3, 1, 0, 0) - 1) });
        Ok(())
    }

    #[test]
    fn open_rows_before_k_start_no_search_and_matches_count_from_m_plus_one() -> Result<()> {
        // rows 0 and 1 are look-back rows: no search starts on them, so the first match is row 2's, and it is match 1
        let (rows, marker) = open_of(&open_spec("a", PAST, 0), &[vec![0], vec![0], vec![0]], 2, 0)?;
        assert_eq!(rows, ["", "", "1a1"]);
        assert_eq!(marker, Marker { keep: 0, k: 0, next_m: 1, due: None });
        // the matches the key found before number on from m + 1
        let (rows, marker) = open_of(&open_spec("a", PAST, 0), &[vec![0], vec![0]], 0, 5)?;
        assert_eq!(rows, ["6a1", "7a1"]);
        assert_eq!(marker, Marker { keep: 0, k: 0, next_m: 7, due: None });
        Ok(())
    }

    #[test]
    fn open_a_month_from_jan_31_is_alive_in_feb_and_dead_from_march_first() {
        // Feb 28 noon less a month is Jan 28, before Jan 31, so the thread lives; Mar 1 less a month is Feb 1, after it
        let kernel = kernel_for(None, IntervalMonthDayNanoType::make_value(1, 0, 0));
        let start = micros(2026, 1, 31, 0, 0);
        assert!(!dead_at(&kernel, start, micros(2026, 2, 28, 12, 0)));
        assert!(dead_at(&kernel, start, micros(2026, 3, 1, 0, 0)));
    }

    #[test]
    fn open_a_day_across_the_new_york_dst_change_is_23_hours() -> Result<()> {
        // on 2026-03-08 the clocks go forward: midnight on March 9 (EDT) less a day is midnight on March 8 (EST), 23 hours back
        let kernel = kernel_for(Some("America/New_York"), IntervalMonthDayNanoType::make_value(0, 1, 0));
        let march_9 = micros(2026, 3, 9, 4, 0);
        let start = micros(2026, 3, 8, 5, 0);
        assert_eq!(kernel.low(march_9)?, Some(start));
        assert_eq!(march_9 - start, 23 * 3_600_000_000);
        // the thread from that midnight dies at the midnight that starts March 9
        assert_eq!(due_of(&kernel, start)?, march_9);
        Ok(())
    }

    #[test]
    fn open_due_is_the_least_watermark_where_the_thread_dies() -> Result<()> {
        let cases = [
            (kernel_for(None, IntervalMonthDayNanoType::make_value(1, 0, 0)), micros(2026, 1, 31, 0, 0)),
            (kernel_for(Some("America/New_York"), IntervalMonthDayNanoType::make_value(0, 1, 0)), micros(2026, 3, 8, 5, 0)),
            (kernel_for(None, IntervalMonthDayNanoType::make_value(0, 2, 90_000_000_000)), micros(2026, 2, 27, 23, 0)),
        ];
        for (kernel, start) in &cases {
            let due = due_of(kernel, *start)?;
            assert!(dead_at(kernel, *start, due), "dead at its due {due}");
            assert!(!dead_at(kernel, *start, due - 1), "alive just before its due {due}");
            assert!(dead_at(kernel, *start, due + 1), "dead after its due {due}");
        }
        Ok(())
    }

    #[test]
    fn open_due_without_months_or_zone_is_the_start_plus_the_interval() -> Result<()> {
        // an interval of a day and 1.5 seconds, over a time with no zone: the thread dies exactly that long after its start
        let kernel = kernel_for(None, IntervalMonthDayNanoType::make_value(0, 1, 1_500_000_000));
        for start in [micros(2026, 1, 1, 0, 0), micros(2026, 3, 8, 5, 0)] {
            assert_eq!(due_of(&kernel, start)?, start + 86_400_000_000 + 1_500_000);
        }
        Ok(())
    }

    /// The xorshift step, so the same partitions come every run (as fsum's and json_rows' tests draw theirs).
    fn xorshift(x: &mut u64) -> u64 {
        *x ^= *x << 13;
        *x ^= *x >> 7;
        *x ^= *x << 17;
        *x
    }

    /// A random row: each of the symbols a, b and c, met with two chances in three.
    fn random_row(x: &mut u64) -> [bool; 3] {
        let a = xorshift(x) % 3 != 0;
        let b = xorshift(x) % 3 != 0;
        let c = xorshift(x) % 3 != 0;
        [a, b, c]
    }

    fn random_quantifier(x: &mut u64) -> &'static str {
        ["", "", "", "*", "+", "?", "*?", "+?", "{2}", "{1,2}", "{0,2}?", "{2,}"][(xorshift(x) % 12) as usize]
    }

    /// A random pattern over the symbols a, b and c: a symbol, a sequence, an alternation, a group with a quantifier, an
    /// exclusion or a PERMUTE, nested `depth` deep.
    fn random_pattern(x: &mut u64, depth: u32) -> String {
        let pick = xorshift(x) % 10;
        if depth == 0 || pick < 3 {
            let sym = ["a", "b", "c"][(xorshift(x) % 3) as usize];
            return format!("{sym}{}", random_quantifier(x));
        }
        let left = random_pattern(x, depth - 1);
        let right = random_pattern(x, depth - 1);
        match pick {
            3 | 4 => format!("{left} {right}"),
            5 => format!("({left} | {right})"),
            6 | 7 => format!("({left}){}", random_quantifier(x)),
            8 => format!("{{- {left} -}}"),
            _ => format!("PERMUTE({left}, {right})"),
        }
    }

    /// The cut property (the brief's core check) for one partition, at every cut: open mode over rows 0..c, then over the rows
    /// the first run keeps and rows c..n, writes the plain matcher's matches that start before the second run's resume point,
    /// each once, with the same MATCH_NUMBERs. Returns the cuts checked, or None when the plain matcher refuses the partition.
    fn cut_property(spec: &str, rows: &[[bool; 3]], times: &[i64], interval: i64) -> Result<Option<usize>> {
        let plan = compile(spec)?;
        let n = rows.len();
        let time: ArrayRef = Arc::new(TimestampMicrosecondArray::from(times.to_vec()));
        let interval_of = |len: usize| -> ArrayRef {
            Arc::new(IntervalMonthDayNanoArray::from(vec![IntervalMonthDayNanoType::make_value(0, 0, interval * 1_000); len]))
        };
        let kernel = match plan.within {
            true => Some(Kernel::of(&time, &interval_of(n))?),
            false => None,
        };
        // WITHIN's low for a watermark: the bound is for W + 1 tick
        let low = |w: i64| -> Result<Option<i64>> {
            match &kernel {
                Some(k) => k.low(w + 1),
                None => Ok(None),
            }
        };
        let cond = |a: usize, b: usize| -> Vec<Vec<bool>> {
            plan.syms
                .iter()
                .map(|s| {
                    let col = (s.as_bytes()[0] - b'a') as usize;
                    rows[a..b].iter().map(|r| r[col]).collect()
                })
                .collect()
        };
        let within = |a: usize, b: usize| -> Result<Option<Within>> {
            match plan.within {
                true => Ok(Some(within_of(&time.slice(a, b - a), &interval_of(b - a))?)),
                false => Ok(None),
            }
        };
        let plain_cond = cond(0, n);
        let plain_within = within(0, n)?;
        let part = Part { cond: &plain_cond, within: plain_within.as_ref(), n, rows: &[], values: &[], planned: None, cap: STATE_CAP, stop: None };
        let Ok(plain) = partition(&plan, &part) else { return Ok(None) };
        let mut checked = 0;
        for c in 1..n {
            // the first run sees rows 0..c; every later row is after the watermark, which is one tick before row c
            let first_cond = cond(0, c);
            let first_within = within(0, c)?;
            let first_part = Part { cond: &first_cond, within: first_within.as_ref(), n: c, rows: &[], values: &[], planned: None, cap: STATE_CAP, stop: None };
            let first_args = OpenArgs { k: 0, m: 0, low: low(times[c] - 1)?, kernel: kernel.clone() };
            let (first, marker) = partition_open(&plan, &first_part, &first_args)?;
            // the second run sees the rows the first keeps, then rows c..n, from the marker's look-back and match count
            let base = c - marker.keep as usize;
            let len = n - base;
            let second_cond = cond(base, n);
            let second_within = within(base, n)?;
            let second_part = Part { cond: &second_cond, within: second_within.as_ref(), n: len, rows: &[], values: &[], planned: None, cap: STATE_CAP, stop: None };
            let second_args = OpenArgs { k: marker.k as usize, m: marker.next_m, low: low(times[n - 1])?, kernel: kernel.clone() };
            let (second, end) = partition_open(&plan, &second_part, &second_args)?;
            // the second run's resume point, in the partition's rows: its look-back start plus the rows before its look-back
            let resume = base + (end.k + (len as i64 - end.keep)) as usize;
            let mut got: Vec<Key> = vec![];
            for (j, mems) in first.iter().enumerate() {
                got.extend(mems.iter().map(|m| key_of(m, j)));
            }
            for (j, mems) in second.iter().enumerate() {
                got.extend(mems.iter().map(|m| key_of(m, base + j)));
            }
            let mut want: Vec<Key> = vec![];
            for (i, mems) in plain.iter().enumerate() {
                for m in mems {
                    let start = match m.seq {
                        Some(s) => i + 1 - s as usize,
                        None => i,
                    };
                    if start < resume {
                        want.push(key_of(m, i));
                    }
                }
            }
            got.sort();
            want.sort();
            assert_eq!(got, want, "spec {spec}: rows {rows:?}, times {times:?}, cut {c}");
            checked += 1;
        }
        Ok(Some(checked))
    }

    #[test]
    fn open_the_cut_property_holds_for_every_cut_and_skip_rule() -> Result<()> {
        // random partitions of up to 25 rows, half under WITHIN, every cut of each, for each skip rule and ALL ROWS. A skip rule
        // that the partition refuses (TO FIRST and TO LAST need their symbol after the match's first row) draws again, until
        // it has 200 checked; the draws are the same for every rule.
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let mut checked = [0usize; 4];
        let mut partitions = 0;
        while checked.iter().any(|&c| c < 200) {
            partitions += 1;
            assert!(partitions <= 2000, "partitions checked per skip rule: {checked:?}");
            let depth = 1 + (xorshift(&mut x) % 3) as u32;
            let pattern = random_pattern(&mut x, depth);
            let syms = names(&pattern);
            let n = 1 + (xorshift(&mut x) % 25) as usize;
            let rows: Vec<[bool; 3]> = (0..n).map(|_| random_row(&mut x)).collect();
            let mut t = 0i64;
            let times: Vec<i64> = (0..n)
                .map(|_| {
                    t += (xorshift(&mut x) % 3) as i64;
                    t
                })
                .collect();
            let interval = 1 + (xorshift(&mut x) % 4) as i64;
            let within = xorshift(&mut x) % 2 == 0;
            let look = (xorshift(&mut x) % 3) as usize;
            let target = syms[(xorshift(&mut x) % syms.len() as u64) as usize].clone();
            let skips = [
                r#"{"to":"past_last"}"#.to_string(),
                r#"{"to":"next_row"}"#.to_string(),
                format!(r#"{{"to":"first","symbol":"{target}"}}"#),
                format!(r#"{{"to":"last","symbol":"{target}"}}"#),
            ];
            for (kind, skip) in skips.iter().enumerate() {
                if checked[kind] >= 200 {
                    continue;
                }
                let spec = format!(r#"{{"pattern":"{pattern}","skip":{skip},"within":{within},"look":{look}}}"#);
                if cut_property(&spec, &rows, &times, interval)?.is_some() {
                    checked[kind] += 1;
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn pondra_open_counts_match_numbers_from_m_plus_one() -> Result<()> {
        // each row is a match of its own, and x is tested for MATCH_NUMBER() = day: with m = 5 the rows are matches 6, 7 and 8
        let spec = r#"{"pattern":"x","skip":{"to":"past_last"},"defines":[{"symbol":0,"glue":"(__mr_s0 = __mr_r0)","slots":[{"index":0,"kind":"match_number"}]}],"rows":1}"#;
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let sql = format!(
            "WITH t(day) AS (VALUES (6), (7), (8)) SELECT day, m['match'] AS mn FROM \
             (SELECT day, unnest(ms) AS m FROM (SELECT day, pondra_open('{spec}', true, day, 0, 5, day) OVER (ORDER BY day) AS ms FROM t)) \
             WHERE m['match'] IS NOT NULL ORDER BY day"
        );
        assert_eq!(rows_in(&ctx, &sql).await?, ["6|6", "7|7", "8|8"]);
        Ok(())
    }

    #[tokio::test]
    async fn pondra_open_returns_eight_fields_with_the_marker_last_on_the_last_row() -> Result<()> {
        // day 3 is in match 2 and carries the marker after it, with seq -1 and its match NULL
        let spec = r#"{"pattern":"a","skip":{"to":"past_last"}}"#;
        let ctx = crate::engine::session(SessionConfig::new(), Arc::new(RuntimeEnv::default()));
        let sql = format!(
            "WITH t(day, a) AS (VALUES (1, true), (2, false), (3, true)) \
             SELECT day, pondra_open('{spec}', a, 0, 0, day) OVER (ORDER BY day) AS ms FROM t ORDER BY day"
        );
        let batches = ctx.sql(&sql).await?.collect().await?;
        let batch = &batches[0];
        let DataType::List(item) = batch.schema().field(1).data_type().clone() else { panic!("a list of structs") };
        let DataType::Struct(fields) = item.data_type() else { panic!("a list of structs") };
        assert_eq!(fields.len(), 8);
        let lists = batch.column(1).as_any().downcast_ref::<ListArray>().expect("a list column");
        assert_eq!(lists.value(1).len(), 0);
        let last = lists.value(2);
        let items = last.as_any().downcast_ref::<StructArray>().expect("structs");
        assert_eq!(items.len(), 2);
        let seq = items.column(2).as_any().downcast_ref::<Int64Array>().expect("seq");
        assert_eq!((seq.value(0), seq.value(1)), (1, -1));
        assert!(items.column(0).is_null(1));
        Ok(())
    }
}
