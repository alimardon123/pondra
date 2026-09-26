// Scratch: what the system columns cost the Parquet writer (not part of the build).
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::basic::{Compression, Encoding};
use datafusion::parquet::file::properties::WriterProperties;
use std::sync::Arc;
fn main() {
    let (n, seg) = (8_000_000i64, 100_000i64);
    let ts = || DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mut batches = vec![];
    for s in 0..n / seg {
        let ids = Int64Array::from_iter_values((0..seg).map(|i| (s << 32) + i));
        let user = StringArray::from_iter_values((0..seg).map(|i| format!("user-{}", (s * seg + i) % 1000)));
        let amount = Int64Array::from_iter_values(0..seg);
        let ver = Int64Array::from(vec![s; seg as usize]);
        let t = TimestampMicrosecondArray::from(vec![1_758_000_000_000_000 + s * 500_000; seg as usize]).with_timezone("UTC");
        let nul = || Arc::new(TimestampMicrosecondArray::new_null(seg as usize).with_timezone("UTC")) as ArrayRef;
        batches.push((user, amount, ids, ver, t, nul(), Int64Array::new_null(seg as usize)));
    }
    let schema = |sys: usize| {
        let mut f = vec![Field::new("user", DataType::Utf8, false), Field::new("amount", DataType::Int64, false)];
        if sys >= 1 { f.push(Field::new("_row_id", DataType::Int64, true)); }
        if sys >= 2 { f.extend([Field::new("_version", DataType::Int64, true), Field::new("_created_at", ts(), true), Field::new("_updated_at", ts(), true)]); }
        Arc::new(Schema::new(f))
    };
    let run = |name: &str, sys: usize, enc: Option<Encoding>, dict: bool, codec: Compression, nulls: bool| {
        let s = schema(sys);
        let mut props = WriterProperties::builder().set_compression(Compression::LZ4_RAW);
        for c in ["_row_id", "_version", "_created_at", "_updated_at"] {
            let e = if c == "_row_id" { Encoding::DELTA_BINARY_PACKED } else { enc.unwrap_or(Encoding::PLAIN) };
            props = props.set_column_dictionary_enabled(c.into(), dict && c != "_row_id").set_column_compression(c.into(), if c == "_row_id" { Compression::LZ4_RAW } else { codec });
            if !(dict && c != "_row_id") { props = props.set_column_encoding(c.into(), e); }
        }
        let mut best = f64::MAX; let mut size = 0;
        for _ in 0..3 {
            let t = std::time::Instant::now();
            let mut buf = vec![];
            let mut w = ArrowWriter::try_new(&mut buf, s.clone(), Some(props.clone().build())).unwrap();
            for (u, a, i, v, tt, nt, nv) in &batches {
                let mut cols: Vec<ArrayRef> = vec![Arc::new(u.clone()), Arc::new(a.clone())];
                if sys >= 1 { cols.push(Arc::new(i.clone())); }
                if sys >= 2 { if nulls { cols.extend([Arc::new(nv.clone()) as ArrayRef, nt.clone(), nt.clone()]); } else { cols.extend([Arc::new(v.clone()) as ArrayRef, Arc::new(tt.clone()), Arc::new(tt.clone())]); } }
                w.write(&RecordBatch::try_new(s.clone(), cols).unwrap()).unwrap();
            }
            w.close().unwrap();
            best = best.min(t.elapsed().as_secs_f64()); size = buf.len();
        }
        println!("{name:44} {best:.3} s {:.1} MB", size as f64 / 1e6);
    };
    {
        use datafusion::functions_aggregate::min_max::{MaxAccumulator, MinAccumulator};
        use datafusion::logical_expr::Accumulator;
        let cols: Vec<(&str, Vec<ArrayRef>)> = vec![
            ("user", batches.iter().map(|b| Arc::new(b.0.clone()) as ArrayRef).collect()),
            ("amount", batches.iter().map(|b| Arc::new(b.1.clone()) as ArrayRef).collect()),
            ("_row_id", batches.iter().map(|b| Arc::new(b.2.clone()) as ArrayRef).collect()),
            ("_version", batches.iter().map(|b| Arc::new(b.3.clone()) as ArrayRef).collect()),
            ("_created_at", batches.iter().map(|b| Arc::new(b.4.clone()) as ArrayRef).collect()),
        ];
        for (name, cs) in cols {
            let t = std::time::Instant::now();
            let (mut lo, mut hi) = (MinAccumulator::try_new(cs[0].data_type()).unwrap(), MaxAccumulator::try_new(cs[0].data_type()).unwrap());
            for c in &cs { lo.update_batch(&[c.clone()]).unwrap(); hi.update_batch(&[c.clone()]).unwrap(); }
            let _ = (lo.evaluate().unwrap(), hi.evaluate().unwrap());
            println!("stats {name:12} {:.3} s", t.elapsed().as_secs_f64());
            let t = std::time::Instant::now();
            for c in &cs {
                use datafusion::arrow::compute::kernels::aggregate as agg;
                if let Some(a) = c.as_any().downcast_ref::<Int64Array>() { std::hint::black_box((agg::min(a), agg::max(a))); }
                else if let Some(a) = c.as_any().downcast_ref::<TimestampMicrosecondArray>() { std::hint::black_box((agg::min(a), agg::max(a))); }
                else if let Some(a) = c.as_any().downcast_ref::<StringArray>() { std::hint::black_box((agg::min_string(a), agg::max_string(a))); }
            }
            println!("kernel {name:12} {:.3} s", t.elapsed().as_secs_f64());
        }
    }
    run("no system columns", 0, None, false, Compression::LZ4_RAW, false);
    run("_row_id only", 1, None, false, Compression::LZ4_RAW, false);
    run("all four, round 19 (3 plain, lz4)", 2, None, false, Compression::LZ4_RAW, false);
    run("3 delta, lz4", 2, Some(Encoding::DELTA_BINARY_PACKED), false, Compression::LZ4_RAW, false);
    run("3 delta, uncompressed", 2, Some(Encoding::DELTA_BINARY_PACKED), false, Compression::UNCOMPRESSED, false);
    run("3 dictionary", 2, None, true, Compression::LZ4_RAW, false);
    run("3 plain, uncompressed", 2, None, false, Compression::UNCOMPRESSED, false);
    run("3 all null", 2, None, false, Compression::LZ4_RAW, true);
}
#[allow(dead_code)]
fn stats_bench() {}
