//! WITHIN's time arithmetic (ADR-073): a row's bound, the times as ticks, and open mode's kernel and due time.

use crate::codes::coded;
use anyhow::{bail, Context, Result};
use datafusion::arrow::array::{Array, ArrayRef, Int64Array, IntervalMonthDayNanoArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::compute::kernels::numeric::sub;
use datafusion::arrow::datatypes::{DataType, IntervalMonthDayNanoType, IntervalUnit, TimeUnit};

/// WITHIN's subtraction for one partition, as `within_of` runs it: the time column's type (a date's is a timestamp of
/// microseconds), the interval (one row, of its own type), and the interval's months, days and nanoseconds, which bound the
/// search `due_of` makes.
#[derive(Clone)]
pub(super) struct Kernel {
    pub(super) time: DataType,
    interval: ArrayRef,
    months: i32,
    days: i32,
    nanos: i64,
}

impl Kernel {
    /// The kernel for a WITHIN's time and interval columns (the first row of the interval, which is the same on every row).
    pub(super) fn of(time: &ArrayRef, interval: &ArrayRef) -> Result<Kernel> {
        let time = timed(time)?.data_type().clone();
        let interval = interval.slice(0, 1);
        let (months, days, nanos) = interval_parts(&interval)?;
        Ok(Kernel { time, interval, months, days, nanos })
    }

    /// WITHIN's low for a time of `tick` ticks: the same subtraction `within_of` makes.
    pub(super) fn low(&self, tick: i64) -> Result<Option<i64>> {
        let at = cast(&Int64Array::from(vec![tick]), &self.time)?;
        let low = sub(&at.as_ref(), &self.interval.as_ref())?;
        Ok(ticks(&low)?[0])
    }
}

/// `due`: the least watermark at which a thread that started at time `t` is dead. It is dead at W when `t` is before
/// `low(W + 1 tick)`, which only moves on, so the least is found by bisection on the tick. The bisection starts from `t`
/// (alive, for an interval of a tick or more) and runs up to a bound surely dead: a month is 31 days, a day 25 hours, the
/// nanoseconds as they are, and a day more.
pub(super) fn due_of(kernel: &Kernel, t: i64) -> Result<i64> {
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
pub(super) fn ns_per_tick(time: &DataType) -> Result<i128> {
    Ok(match time {
        DataType::Timestamp(TimeUnit::Second, _) => 1_000_000_000,
        DataType::Timestamp(TimeUnit::Millisecond, _) => 1_000_000,
        DataType::Timestamp(TimeUnit::Microsecond, _) => 1_000,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => 1,
        other => bail!("MATCH_RECOGNIZE: WITHIN's time is {other}, which has no tick"),
    })
}

/// An interval's months, days and nanoseconds, from its first row (zeros when it is NULL there).
pub(super) fn interval_parts(interval: &ArrayRef) -> Result<(i32, i32, i64)> {
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
pub(super) fn timed(time: &ArrayRef) -> Result<ArrayRef> {
    Ok(match time.data_type() {
        DataType::Date32 | DataType::Date64 => cast(time, &DataType::Timestamp(TimeUnit::Microsecond, None))?,
        _ => time.clone(),
    })
}

/// The first row's time of a column, in the ticks of `like` (the time's type), None when it is NULL there.
pub(super) fn tick_of(column: &ArrayRef, like: &DataType) -> Result<Option<i64>> {
    let at = timed(&cast(column, like)?)?;
    Ok(ticks(&at)?.first().copied().flatten())
}

/// WITHIN's times of one partition, in ticks of the time's unit (a timestamp's count from the epoch), and each row's bound:
/// its time less the interval, so a row can be in a match only when the match's first row is no earlier than the bound.
/// None is NULL.
pub(super) struct Within {
    pub(super) time: Vec<Option<i64>>,
    pub(super) low: Vec<Option<i64>>,
}

/// Whether row `i` may be taken by a thread that started at `start`: its time is within WITHIN's interval of the start's
/// time. A NULL time is never within.
pub(super) fn within_ok(w: Option<&Within>, start: usize, i: usize) -> bool {
    match w {
        None => true,
        Some(w) => matches!((w.time[start], w.low[i]), (Some(t), Some(low)) if t >= low),
    }
}

/// WITHIN reads the first ORDER BY as a time: a date or a timestamp, else 42804.
pub(super) fn check_time_type(t: &DataType) -> Result<()> {
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
pub(super) fn within_of(time: &ArrayRef, interval: &ArrayRef) -> Result<Within> {
    check_time_type(time.data_type())?;
    let time = timed(time)?;
    let low = sub(&time.as_ref(), &interval.as_ref())?;
    Ok(Within { time: ticks(&time)?, low: ticks(&low)? })
}

/// A column of whole numbers as its values (a timestamp's ticks, of its own unit); NULL stays None.
pub(super) fn ticks(column: &ArrayRef) -> Result<Vec<Option<i64>>> {
    let ints = cast(column, &DataType::Int64)?;
    let ints = ints.as_any().downcast_ref::<Int64Array>().context("MATCH_RECOGNIZE: WITHIN's times are whole numbers")?;
    Ok(ints.iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::TimestampMicrosecondArray;
    use datafusion::arrow::datatypes::IntervalMonthDayNano;
    use std::sync::Arc;

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
