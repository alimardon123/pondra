//! A page's identity counters: the sequence each identity column of a page table counts in, and `nextval`, which takes its values.
//! A table's counters are kept with its rules (`Ruled::counters`), in its kept file, and move and drop with it, so a DROP or a
//! RENAME TO needs nothing more. A count is taken as the node takes one (`seq.rs`'s `take`), and a sequence's name is the
//! core's (`identity_sequence`), so the node and the page name the same sequence for the same column.
use super::Ruled;
use crate::rules::create::{identity_of, identity_sequence, serial_type, shape, Identity, Opt};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, Result as DfResult};
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::sql::sqlparser::ast::{CreateTable, DataType as SqlType};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex, PoisonError};

/// One sequence's counter: the value its next `nextval` gives, and its step and bounds as `shape` worked them out. `next` is None
/// once the sequence is past its end: a CYCLE starts it again at its other end, and any other sequence is used up (2200H).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Counter {
    next: Option<i64>,
    increment: i64,
    min: i64,
    max: i64,
    cycle: bool,
}

impl Counter {
    /// The counter an identity column starts at, from its options (`shape`). A SERIAL column's sequence has its type, as Postgres
    /// makes it (`AS integer` for SERIAL), so it counts to that type's maximum.
    pub fn of(identity: &Identity, data_type: &SqlType) -> anyhow::Result<Counter> {
        let mut declared = identity.declared.clone();
        if let Some(t) = serial_type(data_type) {
            if !declared.iter().any(|o| matches!(o, Opt::As(_))) {
                declared.insert(0, Opt::As(t.to_lowercase()));
            }
        }
        let s = shape(&declared)?;
        Ok(Counter { next: Some(s.start), increment: s.increment, min: s.min, max: s.max, cycle: s.cycle })
    }

    /// The next `n` values, in order, each the counter's next and then stepped past it (the node's `take`). Past the end a cycling
    /// counter starts again at its other end; any other is used up, and that is 2200H.
    pub fn take(&mut self, name: &str, n: usize) -> anyhow::Result<Vec<i64>> {
        let mut values = Vec::with_capacity(n);
        for _ in 0..n {
            let value = match self.next {
                Some(v) => v,
                None if self.cycle => if self.increment > 0 { self.min } else { self.max },
                None if self.increment > 0 => return Err(crate::codes::coded("2200H", format!("nextval: reached maximum value of sequence \"{name}\" ({})", self.max))),
                None => return Err(crate::codes::coded("2200H", format!("nextval: reached minimum value of sequence \"{name}\" ({})", self.min))),
            };
            self.next = value.checked_add(self.increment).filter(|v| (self.min..=self.max).contains(v));
            values.push(value);
        }
        Ok(values)
    }
}

/// The counters of one table, by the name of each sequence. A table's rules hold them (`Ruled::counters`), so a copy of the rules
/// is the same counters, and a write that is undone keeps the values it took (a sequence is not rolled back, as Postgres's aren't).
pub type Counters = Arc<Mutex<BTreeMap<String, Counter>>>;

/// An identity column of a CREATE TABLE, as the page keeps it: the column, the sequence it counts in, and the counter that starts it.
pub struct Identified {
    pub column: String,
    pub sequence: String,
    pub counter: Counter,
}

/// The identity columns of a CREATE TABLE `c`, each with its sequence and its counter. A sequence is named by the core's
/// `identity_sequence`, numbered `1`, `2`, … as the node numbers a name a relation has (`seq::owned`) while `held` holds it: a table
/// renamed away keeps the sequence its column counts in, and so may a name an earlier column of this table has.
pub fn identified(c: &CreateTable, held: impl Fn(&str) -> bool) -> anyhow::Result<Vec<Identified>> {
    let table = crate::text::object(&c.name);
    let mut out: Vec<Identified> = vec![];
    for column in &c.columns {
        let Some(identity) = identity_of(column)? else { continue };
        let name = crate::text::ident(&column.name);
        let sequence = numbered(&identity_sequence(&table, &name), |n| held(n) || out.iter().any(|i| i.sequence == n));
        let counter = Counter::of(&identity, &column.data_type)?;
        out.push(Identified { column: name, sequence, counter });
    }
    Ok(out)
}

/// The counters a CREATE TABLE `c` starts its identity columns at, under the core's names: a kept table with no counters note
/// restores so (`ruled_from`), as it was made.
pub fn starting(c: &CreateTable) -> anyhow::Result<BTreeMap<String, Counter>> {
    Ok(identified(c, |_| false)?.into_iter().map(|i| (i.sequence, i.counter)).collect())
}

/// `base`, or `base` numbered `1`, `2`, … after it: the first name `taken` does not hold.
fn numbered(base: &str, taken: impl Fn(&str) -> bool) -> String {
    let mut name = base.to_string();
    let mut n = 0;
    while taken(&name) {
        n += 1;
        name = format!("{base}{n}");
    }
    name
}

/// A sequence's name as `nextval` is given it: read as an object name (lower case unless quoted, as the page's SQL reads names),
/// and without `public.`, as `identity_sequence` writes it.
pub fn sequence_key(text: &str) -> String {
    use datafusion::sql::sqlparser::{dialect::GenericDialect, parser::Parser};
    let name = match Parser::new(&GenericDialect {}).try_with_sql(text).and_then(|mut p| p.parse_object_name(false)) {
        Ok(name) => crate::text::object(&name),
        Err(_) => text.to_lowercase(),
    };
    let (schema, table) = crate::text::split(&name);
    crate::text::join(schema, table)
}

/// `nextval(name)`: the next value of the sequence `name`, from the table whose rules hold its counter (`ruled`, the page's). It is
/// volatile, so the planner never folds a call or shares one, and its name is a constant, as a column's DEFAULT gives it.
pub struct Nextval {
    ruled: Arc<Mutex<HashMap<String, Ruled>>>,
    signature: Signature,
}

impl Nextval {
    pub fn new(ruled: Arc<Mutex<HashMap<String, Ruled>>>) -> Nextval {
        Nextval { ruled, signature: Signature::string(1, Volatility::Volatile) }
    }
}

impl std::fmt::Debug for Nextval {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("nextval")
    }
}

impl PartialEq for Nextval {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.ruled, &other.ruled)
    }
}

impl Eq for Nextval {}

impl Hash for Nextval {
    fn hash<H: Hasher>(&self, h: &mut H) {
        "nextval".hash(h)
    }
}

impl ScalarUDFImpl for Nextval {
    fn name(&self) -> &str {
        "nextval"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> DfResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DfResult<ColumnarValue> {
        let err = |e: anyhow::Error| DataFusionError::External(e.into());
        let [ColumnarValue::Scalar(value)] = &args.args[..] else {
            return exec_err!("nextval takes the name of a sequence as a constant, not a column");
        };
        let Some(text) = value.try_as_str() else {
            return exec_err!("nextval takes the name of a sequence as text");
        };
        let rows = args.number_rows;
        let Some(text) = text else { return Ok(ColumnarValue::Array(Arc::new(Int64Array::new_null(rows)))) };
        let name = sequence_key(text);
        let all = self.ruled.lock().unwrap_or_else(PoisonError::into_inner);
        for table in all.values() {
            let mut counters = table.counters.lock().unwrap_or_else(PoisonError::into_inner);
            if let Some(counter) = counters.get_mut(&name) {
                let values = counter.take(&name, rows).map_err(err)?;
                return Ok(ColumnarValue::Array(Arc::new(Int64Array::from(values))));
            }
        }
        Err(err(crate::codes::coded("42P01", format!("relation \"{name}\" does not exist"))))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_counter_steps_and_is_used_up_past_its_end() {
        let mut c = Counter { next: Some(1), increment: 1, min: 1, max: 3, cycle: false };
        assert_eq!(c.take("s", 2).unwrap(), vec![1, 2]);
        assert_eq!(c.take("s", 1).unwrap(), vec![3]);
        let used = c.take("s", 1).unwrap_err();
        assert_eq!((crate::codes::of(&used), crate::codes::words(&used).as_str()), ("2200H", "nextval: reached maximum value of sequence \"s\" (3)"));
        let mut down = Counter { next: Some(0), increment: -1, min: -2, max: 0, cycle: false };
        assert_eq!(down.take("d", 3).unwrap(), vec![0, -1, -2]);
        assert_eq!(crate::codes::words(&down.take("d", 1).unwrap_err()), "nextval: reached minimum value of sequence \"d\" (-2)");
    }

    #[test]
    fn a_cycling_counter_starts_again_at_its_other_end() {
        let mut c = Counter { next: Some(10), increment: -5, min: 0, max: 10, cycle: true };
        assert_eq!(c.take("s", 3).unwrap(), vec![10, 5, 0]);
        assert_eq!(c.take("s", 2).unwrap(), vec![10, 5]);
    }

    #[test]
    fn a_counter_at_the_largest_integer_is_used_up_without_overflowing() {
        let mut c = Counter { next: Some(i64::MAX - 1), increment: 1, min: i64::MIN, max: i64::MAX, cycle: false };
        assert_eq!(c.take("s", 2).unwrap(), vec![i64::MAX - 1, i64::MAX]);
        assert_eq!(crate::codes::of(&c.take("s", 1).unwrap_err()), "2200H");
    }

    #[test]
    fn a_sequence_is_named_as_the_core_names_it_and_numbered_past_a_taken_name() {
        assert_eq!(numbered("i_id_seq", |_| false), "i_id_seq");
        assert_eq!(numbered("i_id_seq", |n| n == "i_id_seq"), "i_id_seq1");
        assert_eq!(numbered("i_id_seq", |n| n == "i_id_seq" || n == "i_id_seq1"), "i_id_seq2");
        assert_eq!(sequence_key("i_id_seq"), "i_id_seq");
        assert_eq!(sequence_key("Public.I_ID_SEQ"), "i_id_seq");
        assert_eq!(sequence_key("Public.\"I\""), "I");
    }
}
