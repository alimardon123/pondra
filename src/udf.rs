//! Functions you bring yourself. A function is an Arrow Flight server: Pondra sends it the rows
//! it has as one Arrow batch and reads back one column. The heavy part — a model, a GPU, a Python
//! library — runs in that process, so the binary stays small and a slow model can't take a node
//! down with it.
//!
//!   POST /functions/caption {"flight": "http://127.0.0.1:8815", "args": ["Binary"], "returns": "Utf8"}
//!   SELECT path, caption(file_read(path)) FROM files('photos/') WHERE size < 1000000
//!
//! `tools/udf_server.py` is such a server in forty lines of Python (pyarrow.flight). The
//! definition lives in the catalog, so every node has the function; `DELETE /functions/<name>`
//! takes it away, `GET /functions` lists them.
use crate::store::Lake;
use anyhow::Result;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};

/// A function's definition, as `POST /functions/<name>` gives it.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Hash)]
pub struct Udf {
    pub flight: String,       // where the server is: http://host:port
    pub args: Vec<String>,    // argument types, e.g. ["Binary"] or ["Utf8", "Int64"]
    pub returns: String,      // the column it returns, e.g. "Utf8" or "Float32[]"
    #[serde(default)]
    pub rows: Option<usize>, // rows per call (default: the batch the query has)
}

pub fn key(name: &str) -> String { format!("f/{name}") }

/// The `udf` extension (functions served over Flight), as the registry knows it: its catalog records.
pub static UDF: crate::registry::BuiltIn = crate::registry::BuiltIn {
    name: "udf",
    requires: &[],
    register: |r| {
        r.section_mut::<crate::records::Prefixes>().add(PREFIXES);
        r.section_mut::<crate::lake_state::Registered>().add::<Kept>(); // (its functions' list and their servers' connections)
    },
};

/// The functions' records in the catalog (`records`).
pub const PREFIXES: &[crate::records::Prefix] = &[crate::records::Prefix { prefix: "f/", owner: "udf", holds: "a Flight function's address, by its name (key)" }];

/// Register the lake's functions in a session (they are in the catalog, so on every node).
/// The list is re-read at most once a second: a query must not pay a catalog scan for it.
pub async fn register(lake: &Lake, ctx: &SessionContext) -> Result<()> {
    let kept = lake.state::<Kept>()?;
    for (k, udf) in listed(lake, &kept).await?.iter().cloned() {
        let args = udf.args.iter().map(|t| crate::answer::dtype(t)).collect::<Result<Vec<_>>>()?;
        let f = Flighted { name: k[2..].to_string(), returns: crate::answer::dtype(&udf.returns)?, udf, signature: Signature::exact(args, Volatility::Volatile), open: Open(kept.clone()) };
        ctx.register_udf(AsyncScalarUDF::new(Arc::new(f)).into_scalar_udf());
    }
    Ok(())
}

/// What a lake keeps for its functions (invariant 301): their list as of a moment ago (`listed`), and a connection to each
/// of their servers (`channel`).
#[derive(Default)]
pub struct Kept {
    seen: Mutex<Option<(datafusion::common::instant::Instant, Arc<Vec<(String, Udf)>>)>>,
    #[cfg(not(target_arch = "wasm32"))]
    open: Mutex<std::collections::HashMap<String, tonic::transport::Channel>>, // (by url)
}

impl crate::lake_state::LakeState for Kept {
    const NAME: &'static str = "udf";
    const ABOUT: &'static str = "its Flight functions as of a moment ago, and a connection to each of their servers";
    fn make() -> Self {
        Kept::default()
    }
}

/// A function made or dropped here: the next query sees it (other nodes, within a second).
pub fn forget(lake: &Lake) {
    if let Ok(kept) = lake.state::<Kept>() {
        *kept.seen.lock().unwrap() = None;
    }
}

/// The lake's functions, as of a moment ago.
async fn listed(lake: &Lake, kept: &Kept) -> Result<Arc<Vec<(String, Udf)>>> {
    if let Some((at, fns)) = &*kept.seen.lock().unwrap() {
        if at.elapsed() < std::time::Duration::from_secs(1) {
            return Ok(fns.clone());
        }
    }
    let fns = Arc::new(lake.cat.scan::<Udf>("f/", "f0").await?);
    *kept.seen.lock().unwrap() = Some((datafusion::common::instant::Instant::now(), fns.clone()));
    Ok(fns)
}

/// The lake's connections to its functions' servers, as a function holds them: one function is another's like whatever
/// its lake keeps.
#[derive(Clone)]
struct Open(Arc<Kept>);

impl std::fmt::Debug for Open {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("Open") }
}

impl PartialEq for Open {
    fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) }
}

impl Eq for Open {}

impl std::hash::Hash for Open {
    fn hash<H: std::hash::Hasher>(&self, _: &mut H) {}
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Flighted {
    name: String,
    udf: Udf,
    returns: DataType,
    signature: Signature,
    open: Open,
}

impl ScalarUDFImpl for Flighted {
    fn name(&self) -> &str { &self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> datafusion::common::Result<DataType> { Ok(self.returns.clone()) }
    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> { exec_err!("{} runs on its own server", self.name) }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for Flighted {
    fn ideal_batch_size(&self) -> Option<usize> { self.udf.rows }

    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> datafusion::common::Result<ColumnarValue> {
        let arrays = args.args.iter().map(|a| match a {
            ColumnarValue::Array(a) => Ok(a.clone()),
            ColumnarValue::Scalar(s) => s.to_array_of_size(args.number_rows),
        }).collect::<datafusion::common::Result<Vec<ArrayRef>>>()?;
        let fields = arrays.iter().enumerate().map(|(i, a)| Field::new(format!("arg{i}"), a.data_type().clone(), true));
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.collect::<Vec<_>>())), arrays)?;
        let out = exchange(&self.open, &self.udf.flight, batch).await.map_err(|e| exec_datafusion_err!("{}: {e:#}", self.name))?;
        if out.len() != args.number_rows {
            return exec_err!("{} returned {} values for {} rows", self.name, out.len(), args.number_rows);
        }
        Ok(ColumnarValue::Array(datafusion::arrow::compute::cast(&out, &self.returns)?))
    }
}

/// One round trip: the batch out, one column back.
#[cfg(not(target_arch = "wasm32"))]
async fn exchange(open: &Open, url: &str, batch: RecordBatch) -> Result<ArrayRef> {
    use arrow_flight::encode::FlightDataEncoderBuilder;
    let schema = batch.schema();
    let rows = batch.num_rows();
    let descriptor = arrow_flight::FlightDescriptor::new_path(vec!["args".to_string()]); // (a Flight server expects one on the first message)
    let data = FlightDataEncoderBuilder::new().with_schema(schema).with_flight_descriptor(Some(descriptor)).build(futures::stream::iter([Ok(batch)]));
    let mut client = arrow_flight::FlightClient::new(channel(&open.0, url).await?);
    let back: Vec<RecordBatch> = futures::TryStreamExt::try_collect(client.do_exchange(data).await?).await?;
    let first = back.first().map(|b| b.schema()).ok_or_else(|| anyhow::anyhow!("no rows back"))?;
    anyhow::ensure!(first.fields().len() == 1, "a function returns one column, not {}", first.fields().len());
    let columns: Vec<ArrayRef> = back.iter().map(|b| b.column(0).clone()).collect();
    let out = datafusion::arrow::compute::concat(&columns.iter().map(|c| c.as_ref()).collect::<Vec<_>>())?;
    anyhow::ensure!(out.len() == rows, "{rows} rows out, {} values back", out.len());
    Ok(out)
}

#[cfg(target_arch = "wasm32")]
async fn exchange(_: &Open, _: &str, _: RecordBatch) -> Result<ArrayRef> {
    Err(crate::codes::coded("0A000", "a page calls no function's Flight server: a browser speaks no gRPC"))
}

/// Connections are kept: one per server, shared by every query (gRPC multiplexes them).
#[cfg(not(target_arch = "wasm32"))]
async fn channel(kept: &Kept, url: &str) -> Result<tonic::transport::Channel> {
    let open = &kept.open;
    if let Some(c) = open.lock().unwrap().get(url) {
        return Ok(c.clone());
    }
    let c = tonic::transport::Endpoint::from_shared(url.to_string())?.connect().await?;
    open.lock().unwrap().insert(url.to_string(), c.clone());
    Ok(c)
}
