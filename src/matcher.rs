//! `MATCH_RECOGNIZE`'s matcher (ADR-073): `pondra_match` and `pondra_matches`, window functions that answer, for each
//! row of a partition (sorted by the query's ORDER BY), the match the row is in, and `pondra_nav`, which reads a
//! measure's value at the rows of a match. `pattern.rs` writes them: the pattern goes in as its text (in the spec, a
//! JSON literal, which `read` reads), and each symbol's DEFINE as a Boolean column (a symbol with none is always true).
//!
//! The pattern runs as a Pike VM: threads in the standard's order of preference (the leftmost start; a greedy
//! repetition takes more and a reluctant one fewer; the first alternative before the next), one thread per state per
//! row. So a partition takes time in proportion to its rows, and the match is the one the standard asks for.

use crate::codes::coded;
use anyhow::{anyhow, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, BooleanArray, Int64Array, ListArray, StringArray, StructArray, UInt32Array};
use datafusion::arrow::buffer::{NullBuffer, OffsetBuffer};
use datafusion::arrow::compute::kernels::numeric::sub;
use datafusion::arrow::compute::{cast, take};
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Fields, TimeUnit};
use datafusion::common::ScalarValue;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::{PartitionEvaluatorArgs, WindowUDFFieldArgs};
use datafusion::logical_expr::{PartitionEvaluator, Signature, Volatility, WindowUDF, WindowUDFImpl};
use datafusion::physical_expr::expressions::Literal;
use datafusion::prelude::SessionContext;
use serde::Deserialize;
use std::collections::HashSet;
use std::fmt::Display;
use std::rc::Rc;
use std::sync::Arc;

/// The states a pattern may compile to. A count in `{n,m}` multiplies its body, so past this a pattern is refused by
/// name rather than run slowly.
const STATES: usize = 10_000;

/// The spec `pattern.rs` writes: the pattern's text (`read` reads it and names its symbols) and the skip rule.
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
    /// The DEFINEs that read the match so far (`pattern.rs` writes them; the matcher takes them in its next piece, so a
    /// spec that has any is refused here until then).
    #[serde(default)]
    defines: Vec<serde_json::Value>,
}

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

/// A pattern as a program, with its symbols and skip rule.
#[derive(Debug)]
struct Plan {
    syms: Vec<String>,
    code: Vec<Inst>,
    skip: Skip,
    unmatched: bool,
    within: bool,
}

/// The plan of a spec, refused by name when it is malformed or too big.
fn compile(text: &str) -> Result<Plan> {
    let spec: Spec = serde_json::from_str(text).map_err(|e| anyhow!("MATCH_RECOGNIZE: the pattern's spec doesn't read: {e}"))?;
    if !spec.defines.is_empty() {
        return Err(coded("0A000", "MATCH_RECOGNIZE: a DEFINE that reads the match so far isn't run yet"));
    }
    let (pattern, syms) = read(&spec.pattern)?;
    let mut c = Compiler { code: vec![] };
    c.emit(&pattern, false)?;
    c.push(Inst::Match)?;
    Ok(Plan { syms, code: c.code, skip: spec.skip, unmatched: spec.unmatched, within: spec.within })
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

/// A thread: a state, the row its match started at, and its path.
#[derive(Clone)]
struct Thread {
    pc: usize,
    start: usize,
    path: Path,
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

/// The most preferred match that starts at or after `from`: the Pike VM's leftmost answer. A row's threads run in
/// priority order; a match cuts the threads after it, and a higher-priority thread that matches later replaces it.
/// No new thread starts once a match is found.
/// `stamp` and `gen` are the partition's, so a search costs its rows, not a clear of every state. With WITHIN a thread that
/// would take a row too late dies (`within_ok`), and the threads of each start are kept apart (`Seen`).
fn search(plan: &Plan, cond: &[Vec<bool>], w: Option<&Within>, n: usize, from: usize, stamp: &mut [usize], last: &mut usize) -> Result<Option<Found>> {
    let code = &plan.code;
    let mut best: Option<Found> = None;
    let mut now: Vec<Thread> = vec![];
    let mut now_pairs = HashSet::new();
    let mut gen = *last + 1;
    let mut i = from;
    loop {
        // (a new thread starts at this row last in line, so it is the least preferred)
        if best.is_none() && i < n {
            let mut seen = Seen::of(w, stamp, gen, &mut now_pairs);
            close(code, &mut now, &mut seen, 0, i, None, i, n);
        }
        let mut next = vec![];
        let mut next_pairs = HashSet::new();
        let gen_next = gen + 1;
        {
            let mut seen = Seen::of(w, stamp, gen_next, &mut next_pairs);
            for t in &now {
                match code[t.pc] {
                    Inst::Match => {
                        best = Some(Found::of(t, i));
                        break;
                    }
                    Inst::Sym(k, excluded) => {
                        if i < n && cond[k][i] && within_ok(w, t.start, i) {
                            let step = Rc::new(Step { sym: k, excluded, prev: t.path.clone() });
                            close(code, &mut next, &mut seen, t.pc + 1, t.start, Some(step), i + 1, n);
                        }
                    }
                    _ => {}
                }
            }
        }
        if next.len() > MAX_THREADS {
            return Err(coded(
                "54000",
                format!("MATCH_RECOGNIZE: the search holds more than {MAX_THREADS} partial matches at once: narrow WITHIN or the conditions"),
            ));
        }
        if i == n || (best.is_some() && next.is_empty()) {
            break;
        }
        now = next;
        now_pairs = next_pairs;
        gen = gen_next;
        i += 1;
    }
    *last = gen + 1;
    Ok(best)
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

/// Which thread reaches a state first, in one generation of threads. Without WITHIN a state holds one thread (its stamp
/// is the generation). With WITHIN two starts' threads at a state have different futures (their times differ), so a state
/// holds one thread per start, and `Start` is the generation's set of (state, start).
enum Seen<'a> {
    Stamp(&'a mut [usize], usize),
    Start(&'a mut HashSet<(usize, usize)>),
}

impl<'a> Seen<'a> {
    fn of(w: Option<&Within>, stamp: &'a mut [usize], gen: usize, pairs: &'a mut HashSet<(usize, usize)>) -> Seen<'a> {
        match w {
            Some(_) => Seen::Start(pairs),
            None => Seen::Stamp(stamp, gen),
        }
    }

    /// Whether a thread that started at `start` is the first of this generation to reach `pc`.
    fn first(&mut self, pc: usize, start: usize) -> bool {
        match self {
            Seen::Stamp(stamp, gen) => {
                let fresh = stamp[pc] != *gen;
                stamp[pc] = *gen;
                fresh
            }
            Seen::Start(pairs) => pairs.insert((pc, start)),
        }
    }
}

/// Adds the threads a state reaches at a row (its epsilon closure) to `out`, in priority order (a Split's first target
/// first). A state holds the first thread to reach it, which is the one preferred (see `Seen`).
#[allow(clippy::too_many_arguments)]
fn close(code: &[Inst], out: &mut Vec<Thread>, seen: &mut Seen<'_>, pc: usize, start: usize, path: Path, at: usize, n: usize) {
    let mut todo = vec![pc];
    while let Some(pc) = todo.pop() {
        if !seen.first(pc, start) {
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
            Inst::Sym(..) | Inst::Match => out.push(Thread { pc, start, path: path.clone() }),
        }
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

/// Every match of one partition of `n` rows, as each row's memberships (a row in none has an empty list). Matches go on
/// from the skip rule's place; an empty match goes on one row later.
fn partition(plan: &Plan, cond: &[Vec<bool>], w: Option<&Within>, n: usize) -> Result<Vec<Vec<Mem>>> {
    let mut rows: Vec<Vec<Mem>> = vec![Vec::new(); n];
    let (mut from, mut m) = (0usize, 0i64);
    let (mut stamp, mut gen) = (vec![0usize; plan.code.len()], 0usize);
    while from < n {
        let Some(f) = search(plan, cond, w, n, from, &mut stamp, &mut gen)? else { break };
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

/// The evaluator of one partition. The whole partition is matched at once: a match may look at any of its rows, so
/// the window's frame and a bounded run don't apply.
#[derive(Debug)]
struct Evaluator {
    all: bool,
    plan: Option<Plan>,
}

impl PartitionEvaluator for Evaluator {
    fn uses_window_frame(&self) -> bool { false }

    fn supports_bounded_execution(&self) -> bool { false }

    fn evaluate_all(&mut self, values: &[ArrayRef], n: usize) -> DfResult<ArrayRef> {
        self.run(values, n).map_err(external)
    }
}

impl Evaluator {
    /// The answer for `n` rows. `values[0]` is the spec, then one condition column per symbol.
    fn run(&mut self, values: &[ArrayRef], n: usize) -> Result<ArrayRef> {
        if n == 0 {
            return answer(&[], &[], self.all, false);
        }
        if self.plan.is_none() {
            self.plan = Some(compile(&spec_text(values.first().context("MATCH_RECOGNIZE: no spec")?)?)?);
        }
        let plan = self.plan.as_ref().context("MATCH_RECOGNIZE: no plan")?;
        let mut cond = vec![];
        for k in 0..plan.syms.len() {
            cond.push(bools(values.get(k + 1).context("MATCH_RECOGNIZE: a symbol without its condition")?)?);
        }
        // the time and the interval come after the symbols' conditions
        let within = match plan.within {
            true => {
                let time = values.get(plan.syms.len() + 1).context("MATCH_RECOGNIZE: WITHIN has no time")?;
                let interval = values.get(plan.syms.len() + 2).context("MATCH_RECOGNIZE: WITHIN has no interval")?;
                Some(within_of(time, interval)?)
            }
            false => None,
        };
        answer(&partition(plan, &cond, within.as_ref(), n)?, &plan.syms, self.all, plan.unmatched)
    }
}

/// The two window functions: `pondra_match` (a row's first membership) and `pondra_matches` (all of them, for matches
/// that may share rows).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Matcher {
    all: bool,
    signature: Signature,
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
        // (the spec is a literal, as `pattern.rs` writes it: compiled here; another form is read at the first row)
        let plan = match args.input_exprs().first().and_then(|e| e.downcast_ref::<Literal>()).map(|l| l.value()) {
            Some(ScalarValue::Utf8(Some(text))) => Some(compile(text).map_err(external)?),
            _ => None,
        };
        // (WITHIN's time and interval are checked here, at planning, when the spec is a literal; `run` checks the time too)
        if let Some(p) = plan.as_ref().filter(|p| p.within) {
            let fields = args.input_fields();
            if let Some(time) = fields.get(p.syms.len() + 1) {
                check_time_type(time.data_type()).map_err(external)?;
            }
            // a backstop for a spec written by hand: pattern.rs writes an interval literal, which is an interval type
            if let Some(interval) = fields.get(p.syms.len() + 2) {
                if !matches!(interval.data_type(), DataType::Interval(_)) {
                    return Err(external(coded("42601", "MATCH_RECOGNIZE: WITHIN takes an interval, as INTERVAL '1' HOUR")));
                }
            }
        }
        Ok(Box::new(Evaluator { all: self.all, plan }))
    }

    fn field(&self, field_args: WindowUDFFieldArgs) -> DfResult<FieldRef> {
        let t = match self.all {
            true => DataType::List(Arc::new(Field::new("item", struct_type(), true))),
            false => struct_type(),
        };
        Ok(Arc::new(Field::new(field_args.name(), t, true)))
    }
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
    Ok(take(values[0].as_ref(), &UInt32Array::from(at), None)?)
}

/// A literal argument (the same in every row), read from its first row as a whole number.
fn first_int(column: &ArrayRef) -> Result<i64> {
    let ints = cast(column, &DataType::Int64)?;
    let ints = ints.as_any().downcast_ref::<Int64Array>().context("MATCH_RECOGNIZE: pondra_nav reads a whole number")?;
    ints.iter().next().flatten().context("MATCH_RECOGNIZE: pondra_nav's m is NULL")
}

/// Registers `pondra_match`, `pondra_matches` and `pondra_nav` with a session (`engine::session`).
pub fn register(ctx: &SessionContext) {
    for all in [false, true] {
        ctx.register_udwf(WindowUDF::from(Matcher { all, signature: Signature::variadic_any(Volatility::Immutable) }));
    }
    ctx.register_udwf(WindowUDF::from(Navigate { signature: Signature::any(5, Volatility::Immutable) }));
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
        let found = partition(&plan, &conds(rows, plan.syms.len()), None, rows.len())?;
        Ok(texts(&plan, &found))
    }

    /// `shown` under WITHIN: `times` are the rows' times (None is NULL), and each row must be within `interval` of its
    /// match's first row.
    fn shown_within(pattern: &str, rows: &[Vec<usize>], times: &[Option<i64>], interval: i64) -> Vec<String> {
        let plan = compile(&within_spec(pattern)).unwrap();
        let low = times.iter().map(|t| t.map(|t| t - interval)).collect();
        let w = Within { time: times.to_vec(), low };
        let found = partition(&plan, &conds(rows, plan.syms.len()), Some(&w), rows.len()).unwrap();
        texts(&plan, &found)
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
}
