//! `string_agg` whose one group's string is held to a share of the query memory, refused by name past it (53200).
//!
//! A group's answer is one string, built whole in memory: no pool can spill it, and DataFusion's accumulator grows it by
//! doubling, so a string of n bytes takes up to 3n while it grows (the old buffer and the new one), and n again when it is
//! copied out as the answer. Over a huge group the node was killed before its memory watch could look (B50, 10-31:
//! `string_agg(l_comment, ',')` over TPC-H SF10's 3 return flags, 550 MB a group, killed in 0.6 s under a 2 GB cap; DuckDB
//! 2.0.0 is killed too). So each group's bytes are counted as they arrive, and a group past an eighth of a partition's
//! share of the query memory (`cap`: 21 MB of a 683 MB pool on 4 partitions, 680 MB of 21 GB on 4) stops the query with
//! words saying what to do; the pool is told what the strings hold, not only their lengths (`Groups::size`). A group's partial string is never longer than its whole one, so a partial aggregate that
//! passes the cap refuses early, for the right reason. Every other group, however many there are, is DataFusion's own:
//! the pool sees their total and spills them. Not retried frugally (`query::exhausted`): a group is as long the second
//! time.
use datafusion::arrow::array::{Array, ArrayRef, AsArray, BooleanArray};
use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::functions_aggregate::string_agg::StringAgg;
use datafusion::logical_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion::logical_expr::utils::AggregateOrderSensitivity;
use datafusion::logical_expr::{Accumulator, AggregateUDF, AggregateUDFImpl, Documentation, EmitTo, GroupsAccumulator, ReversedUDAF, Signature};
use datafusion::prelude::SessionContext;
use std::sync::Arc;

/// Replaces `string_agg` in a session, capped by the memory its node has for queries (`env::NodeMemory`).
pub fn register(ctx: &SessionContext) {
    let cap = cap(ctx.state_ref().read().config());
    ctx.register_udaf(AggregateUDF::from(Capped(StringAgg::default(), cap)));
}

/// How long one group's string may grow in a session of `config`, in bytes: an eighth of a partition's share of its node's
/// memory for queries (11-09 audit M9), as it takes up to three times its length while it grows and its length again as
/// the answer, in a partial aggregate and a final one at once. A build with no lake has no pool to share.
fn cap(config: &datafusion::prelude::SessionConfig) -> usize {
    #[cfg(feature = "lake")]
    return {
        let memory = crate::env::node_memory_of(config, crate::store::memory_limit);
        (memory / crate::store::partitions_of(memory).max(1) / 8).max(1 << 20)
    };
    #[cfg(not(feature = "lake"))]
    {
        let _ = config;
        usize::MAX
    }
}

/// What a query whose group passed `cap` answers ("out of memory": 53200 at every door, `codes::by_words`).
fn refused(cap: usize) -> DataFusionError {
    DataFusionError::Execution(format!(
        "out of memory: one group's string_agg passed {} MB, an eighth of a partition's share of this node's memory for queries: group by more columns, or {}",
        cap >> 20,
        crate::env::raise_memory()
    ))
}

/// DataFusion's `string_agg`, and the cap of its session (`cap`).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Capped(StringAgg, usize);

impl AggregateUDFImpl for Capped {
    fn name(&self) -> &str { self.0.name() }
    fn aliases(&self) -> &[String] { self.0.aliases() }
    fn signature(&self) -> &Signature { self.0.signature() }
    fn return_type(&self, types: &[DataType]) -> Result<DataType> { self.0.return_type(types) }
    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> { self.0.state_fields(args) }

    fn accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(One { inner: self.0.accumulator(args)?, cap: self.1 }))
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool { self.0.groups_accumulator_supported(args) }

    fn create_groups_accumulator(&self, args: AccumulatorArgs) -> Result<Box<dyn GroupsAccumulator>> {
        Ok(Box::new(Groups::new(self.0.create_groups_accumulator(args)?, self.1)))
    }

    // (DataFusion's reversed `string_agg` is its own, uncapped: this one, reversed, is this one)
    fn reverse_expr(&self) -> ReversedUDAF { ReversedUDAF::Reversed(Arc::new(AggregateUDF::from(Capped(StringAgg::default(), self.1)))) }
    fn order_sensitivity(&self) -> AggregateOrderSensitivity { self.0.order_sensitivity() }
    fn documentation(&self) -> Option<&Documentation> { self.0.documentation() }
}

/// One group's `string_agg` (no GROUP BY; or one with DISTINCT or its own ORDER BY, kept as a list): its own size, the
/// string's capacity, is the measure.
#[derive(Debug)]
struct One {
    inner: Box<dyn Accumulator>,
    cap: usize,
}

impl One {
    fn held(&self) -> Result<()> {
        match self.inner.size() > self.cap {
            true => Err(refused(self.cap)),
            false => Ok(()),
        }
    }
}

impl Accumulator for One {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.inner.update_batch(values)?;
        self.held()
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.inner.merge_batch(states)?;
        self.held()
    }
    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> { self.inner.retract_batch(values) }
    fn supports_retract_batch(&self) -> bool { self.inner.supports_retract_batch() }
    fn state(&mut self) -> Result<Vec<ScalarValue>> { self.inner.state() }
    fn evaluate(&mut self) -> Result<ScalarValue> { self.inner.evaluate() }
    fn size(&self) -> usize { self.inner.size() + size_of::<Self>() }
}

/// A `string_agg` per group: each group's bytes so far, counted from the strings as they arrive (the accumulator keeps
/// only their total), and what their strings hold, which the pool is told (`size`).
struct Groups {
    inner: Box<dyn GroupsAccumulator>,
    bytes: Vec<usize>,
    cap: usize,
    lens: usize,   // (Σ bytes)
    held: usize,   // (Σ room(bytes)): what the strings hold
    widest: usize, // (the largest room: its next doubling holds it and twice it at once)
}

/// What a string grown by doubling holds for `b` bytes, at most.
fn room(b: usize) -> usize {
    if b == 0 { 0 } else { b.next_power_of_two() }
}

impl Groups {
    fn new(inner: Box<dyn GroupsAccumulator>, cap: usize) -> Self { Groups { inner, bytes: vec![], cap, lens: 0, held: 0, widest: 0 } }

    /// Counts `values` (strings, or the partial strings of a state) into their groups, the rows `filter` passes.
    fn count(&mut self, values: &ArrayRef, groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.bytes.resize(total, 0);
        let lengths: Box<dyn Iterator<Item = Option<usize>>> = match values.data_type() {
            DataType::Utf8 => Box::new(values.as_string::<i32>().iter().map(|s| s.map(str::len))),
            DataType::LargeUtf8 => Box::new(values.as_string::<i64>().iter().map(|s| s.map(str::len))),
            DataType::Utf8View => Box::new(values.as_string_view().iter().map(|s| s.map(str::len))),
            _ => return Ok(()), // (DataFusion's own refuses it)
        };
        for (i, (len, &g)) in lengths.zip(groups).enumerate() {
            let (Some(len), true) = (len, filter.is_none_or(|f| f.is_valid(i) && f.value(i))) else { continue };
            let (was, now) = (self.bytes[g], self.bytes[g] + len + 1); // (a separator's byte, near enough)
            if now > self.cap {
                return Err(refused(self.cap));
            }
            self.bytes[g] = now;
            self.lens += len + 1;
            self.held += room(now) - room(was);
            self.widest = self.widest.max(room(now));
        }
        Ok(())
    }

    /// The groups an emit takes out stop counting.
    fn emitted(&mut self, emit: EmitTo) {
        let gone = emit.take_needed(&mut self.bytes);
        self.lens -= gone.iter().sum::<usize>();
        self.held -= gone.iter().map(|&b| room(b)).sum::<usize>();
        self.widest = self.bytes.iter().map(|&b| room(b)).max().unwrap_or(0);
    }
}

impl GroupsAccumulator for Groups {
    fn update_batch(&mut self, values: &[ArrayRef], groups: &[usize], filter: Option<&BooleanArray>, total: usize) -> Result<()> {
        self.count(&values[0], groups, filter, total)?;
        self.inner.update_batch(values, groups, filter, total)
    }

    fn merge_batch(&mut self, states: &[ArrayRef], groups: &[usize], total: usize) -> Result<()> {
        self.count(&states[0], groups, None, total)?;
        self.inner.merge_batch(states, groups, total)
    }

    fn evaluate(&mut self, emit: EmitTo) -> Result<ArrayRef> {
        self.emitted(emit);
        self.inner.evaluate(emit)
    }

    fn state(&mut self, emit: EmitTo) -> Result<Vec<ArrayRef>> {
        self.emitted(emit);
        self.inner.state(emit)
    }

    fn convert_to_state(&self, values: &[ArrayRef], filter: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> {
        self.inner.convert_to_state(values, filter)
    }

    // (DataFusion tells the pool the strings' lengths; they hold up to twice that, and the widest one's next doubling holds
    // it and twice it at once: under a 2 GB cap three groups' strings killed the node while the pool was told their
    // lengths. Told what they hold, a partial aggregate sends its groups on early and a final one spills them.)
    fn size(&self) -> usize { self.inner.size().saturating_sub(self.lens) + self.held + 2 * self.widest + self.bytes.capacity() * size_of::<usize>() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{LargeStringArray, StringArray};

    fn groups(cap: usize) -> Groups { Groups::new(Box::new(Nothing), cap) }

    /// An accumulator that keeps nothing: the wrapper's counting alone is under test.
    struct Nothing;
    impl GroupsAccumulator for Nothing {
        fn update_batch(&mut self, _: &[ArrayRef], _: &[usize], _: Option<&BooleanArray>, _: usize) -> Result<()> { Ok(()) }
        fn merge_batch(&mut self, _: &[ArrayRef], _: &[usize], _: usize) -> Result<()> { Ok(()) }
        fn evaluate(&mut self, _: EmitTo) -> Result<ArrayRef> { Ok(Arc::new(LargeStringArray::from(Vec::<Option<String>>::new()))) }
        fn state(&mut self, _: EmitTo) -> Result<Vec<ArrayRef>> { Ok(vec![]) }
        fn convert_to_state(&self, values: &[ArrayRef], _: Option<&BooleanArray>) -> Result<Vec<ArrayRef>> { Ok(values.to_vec()) }
        fn size(&self) -> usize { 0 }
    }

    #[test]
    fn a_group_past_its_cap_is_refused_by_name_and_others_are_not() {
        let mut g = groups(10);
        let three: ArrayRef = Arc::new(StringArray::from(vec![Some("abc"), Some("abc"), None, Some("abc")]));
        // (groups 0 and 1 take 4 bytes a value with the separator's: 8 and 4, under 10)
        g.update_batch(&[three.clone()], &[0, 0, 0, 1], None, 2).unwrap();
        assert_eq!((&g.bytes[..], g.lens, g.held, g.widest), (&[8, 4][..], 12, 12, 8));
        // (a filtered row isn't counted)
        let skip = BooleanArray::from(vec![false, false, true, true]);
        g.update_batch(&[three.clone()], &[0, 0, 0, 1], Some(&skip), 2).unwrap();
        assert_eq!((&g.bytes[..], g.held), (&[8, 8][..], 16));
        // (emitted groups start again)
        g.evaluate(EmitTo::First(1)).unwrap();
        assert_eq!((&g.bytes[..], g.lens, g.held, g.widest), (&[8][..], 8, 8, 8));
        let err = g.merge_batch(&[Arc::new(LargeStringArray::from(vec!["abc"]))], &[0], 1).unwrap_err().to_string();
        assert!(err.contains("out of memory: one group's string_agg passed 0 MB"), "{err}");
    }

    #[test]
    fn its_cap_is_an_eighth_of_a_partitions_share() {
        use datafusion::prelude::SessionConfig;
        assert!(cap(&SessionConfig::new()) >= 1 << 20, "a session of no lake's: the process's memory");
        // (a node's own memory, as its lake's sessions carry it: 400 MB over its partitions, at most one per 24 MB)
        let node = SessionConfig::new().with_extension(Arc::new(crate::env::NodeMemory(400 << 20)));
        let parts = crate::store::partitions_of(400 << 20);
        assert_eq!(cap(&node), ((400 << 20) / parts / 8).max(1 << 20), "the node's, not the process's");
    }
}
