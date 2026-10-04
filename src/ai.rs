//! Models, in SQL. Pondra carries no model: these call an OpenAI-compatible endpoint (your own
//! vLLM or Ollama, or a hosted one), so the binary stays small and the GPU stays outside it.
//!
//! * `ai_complete(prompt [, model])` — the model's answer as text.
//! * `ai_embed(text [, model])` — the embedding, a `FLOAT[]` to store in a column.
//! * `cosine_similarity(a, b)`, `cosine_distance`, `l2_distance`, `dot_product` — over those columns,
//!   for the "most like this" queries (`ORDER BY cosine_distance(v, $query) LIMIT 10`), under
//!   DuckDB's and DataFusion's names too (`array_cosine_similarity`, `array_distance`, …).
//!
//! Where to call: `PONDRA_AI_URL` (default `http://127.0.0.1:11434/v1`, Ollama's), `PONDRA_AI_KEY`
//! if it wants one, `PONDRA_AI_MODEL` and `PONDRA_EMBED_MODEL` for the default models. A batch asks
//! each distinct text once, embeddings `EMBEDS` to a request; a node has at most
//! `PONDRA_AI_CALLS` requests in flight, whatever its queries, and tries again after a 429, a 5xx
//! or a lost connection. A row whose call still fails is null, so one bad row doesn't lose a query.
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float32Builder, Float64Array, Float64Builder, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{ArrowPrimitiveType, DataType, Field, Float32Type, Float64Type};
use datafusion::common::{exec_err, plan_err, Result, ScalarValue};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{atomic::{AtomicU64, Ordering::Relaxed}, Arc, OnceLock};
use std::time::Duration;

const EMBEDS: usize = 64; // texts in one embeddings request
const TRIES: u32 = 5; // a call, and four more after a 429, a 5xx or a lost connection

pub static CALLS: AtomicU64 = AtomicU64::new(0); // requests to the endpoint that were answered…
pub static FAILED: AtomicU64 = AtomicU64::new(0); // …that failed for good (their rows are null)…
pub static RETRIED: AtomicU64 = AtomicU64::new(0); // …and that were tried again

pub fn register(ctx: &SessionContext) {
    for embed in [false, true] {
        let f = Ai { embed, signature: Signature::one_of(vec![TypeSignature::String(1), TypeSignature::String(2)], Volatility::Volatile) };
        ctx.register_udf(AsyncScalarUDF::new(Arc::new(f)).into_scalar_udf());
    }
    // One function per meaning, under every name people know it by (invariant 126): ours, DuckDB's,
    // DataFusion's and pgvector's. DataFusion's own were a second, slower code for three of them.
    let metrics: [(&'static str, Metric, &[&'static str]); 4] = [
        ("cosine_similarity", Metric::Cosine, &["array_cosine_similarity", "list_cosine_similarity"]),
        ("cosine_distance", Metric::CosineDistance, &["array_cosine_distance", "list_cosine_distance"]),
        ("l2_distance", Metric::L2, &["array_distance", "list_distance"]),
        ("dot_product", Metric::Dot, &["inner_product", "array_inner_product", "list_inner_product", "array_dot_product", "list_dot_product"]),
    ];
    for (name, metric, aliases) in metrics {
        let f = Distance { name, metric, signature: Signature::user_defined(Volatility::Immutable) };
        ctx.register_udf(ScalarUDF::new_from_impl(f).with_aliases(aliases.iter().copied()));
    }
}

/// `ai_complete(prompt [, model])` and `ai_embed(text [, model])`.
#[derive(Debug, PartialEq, Eq, Hash)]
struct Ai {
    embed: bool,
    signature: Signature,
}

impl ScalarUDFImpl for Ai {
    fn name(&self) -> &str { if self.embed { "ai_embed" } else { "ai_complete" } }
    fn signature(&self) -> &Signature { &self.signature }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(match self.embed {
            true => vector(DataType::Float32),
            false => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, _: ScalarFunctionArgs) -> Result<ColumnarValue> { exec_err!("{} is asynchronous", self.name()) }
}

#[async_trait::async_trait]
impl AsyncScalarUDFImpl for Ai {
    async fn invoke_async_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let text = |v: &ColumnarValue| -> Result<ArrayRef> {
            Ok(match v {
                ColumnarValue::Array(a) => datafusion::arrow::compute::cast(a, &DataType::Utf8)?,
                ColumnarValue::Scalar(s) => datafusion::arrow::compute::cast(&s.to_array_of_size(args.number_rows)?, &DataType::Utf8)?,
            })
        };
        let [input, model] = match &args.args[..] {
            [input] => [text(input)?, text(&ColumnarValue::Scalar(default_model(self.embed).into()))?],
            [input, model] => [text(input)?, text(model)?],
            _ => return exec_err!("{}(text [, model])", self.name()),
        };
        let (input, model) = (input.as_string::<i32>(), model.as_string::<i32>());
        // Each distinct (model, text) is asked once: a column of repeated labels, or every row
        // asking the same question, costs one call.
        let mut asked: HashMap<(&str, &str), usize> = HashMap::new();
        let row: Vec<Option<usize>> = (0..input.len()).map(|i| {
            (input.is_valid(i) && model.is_valid(i)).then(|| {
                let n = asked.len();
                *asked.entry((model.value(i), input.value(i))).or_insert(n)
            })
        }).collect();
        let mut questions: Vec<(&str, &str)> = vec![("", ""); asked.len()];
        asked.into_iter().for_each(|(q, n)| questions[n] = q);
        let answers = ask(self.embed, &questions).await;
        let answer = |i: usize| row[i].and_then(|n| answers[n].as_ref());
        Ok(ColumnarValue::Array(match self.embed {
            true => vectors((0..input.len()).map(answer)),
            false => {
                let mut out = StringBuilder::new();
                (0..input.len()).for_each(|i| out.append_option(answer(i).and_then(|v| v.as_str())));
                Arc::new(out.finish())
            }
        }))
    }
}

/// Every question's answer, in order (`None` where its call failed): embeddings `EMBEDS` texts to a
/// request (by model), completions one each, at most `PONDRA_AI_CALLS` requests in flight on the node.
async fn ask(embed: bool, questions: &[(&str, &str)]) -> Vec<Option<Value>> {
    let mut models: Vec<Vec<usize>> = Vec::new(); // (each model's questions)
    for (n, (model, _)) in questions.iter().enumerate() {
        match models.iter_mut().find(|m| questions[m[0]].0 == *model) {
            Some(texts) => texts.push(n),
            None => models.push(vec![n]),
        }
    }
    let size = if embed { EMBEDS } else { 1 };
    let requests: Vec<Vec<usize>> = models.iter().flat_map(|texts| texts.chunks(size).map(<[usize]>::to_vec)).collect();
    let mut out = vec![None; questions.len()];
    let mut done = futures::stream::iter(requests.into_iter().map(|texts| request(embed, questions, texts))).buffer_unordered(usize::MAX);
    while let Some(answered) = done.next().await {
        answered.into_iter().for_each(|(n, a)| out[n] = a);
    }
    out
}

/// One request's answers (its questions all of one model), each with its question's number.
async fn request(embed: bool, questions: &[(&str, &str)], texts: Vec<usize>) -> Vec<(usize, Option<Value>)> {
    let model = questions[texts[0]].0;
    let inputs: Vec<&str> = texts.iter().map(|&n| questions[n].1).collect();
    let _turn = turns().acquire().await;
    let answers = match call(embed, &inputs, model).await {
        Ok(answers) => answers.into_iter().map(Some).collect(),
        Err(e) if inputs.len() > 1 && refused(&e) => {
            // (an endpoint that takes one text a request: each alone)
            let mut out = Vec::new();
            for &text in &inputs {
                out.push(call(embed, &[text], model).await.map_err(|e| failed(embed, &e)).ok().and_then(|a| a.into_iter().next()));
            }
            out
        }
        Err(e) => {
            failed(embed, &e);
            vec![None; texts.len()]
        }
    };
    texts.into_iter().zip(answers).collect::<Vec<_>>()
}

/// A batch the endpoint wouldn't take as it was sent (or answered only in part).
fn refused(e: &anyhow::Error) -> bool {
    let status = e.downcast_ref::<reqwest::Error>().and_then(|e| e.status()).map(|s| s.as_u16());
    matches!(status, Some(400 | 413 | 422)) || e.to_string().starts_with("no answer")
}

/// A call that failed for good: counted, and said on the node's standard error once every ten
/// seconds at most (an endpoint that is down fails every row of every batch).
fn failed(embed: bool, e: &anyhow::Error) {
    static SAID: AtomicU64 = AtomicU64::new(0);
    FAILED.fetch_add(1, Relaxed);
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_secs());
    let last = SAID.load(Relaxed);
    if last + 10 <= now && SAID.compare_exchange(last, now, Relaxed, Relaxed).is_ok() {
        eprintln!("{}: {e:#} (its rows are NULL)", if embed { "ai_embed" } else { "ai_complete" });
    }
}

/// The node's turns at the endpoint (`PONDRA_AI_CALLS`, 16): every query on the node shares them, so
/// many partitions on many nodes never storm a provider.
fn turns() -> &'static tokio::sync::Semaphore {
    static TURNS: OnceLock<tokio::sync::Semaphore> = OnceLock::new();
    TURNS.get_or_init(|| tokio::sync::Semaphore::new(std::env::var("PONDRA_AI_CALLS").ok().and_then(|n| n.parse().ok()).filter(|&n| n > 0).unwrap_or(16)))
}

/// One request to the endpoint: the answers' texts, or the embeddings as JSON arrays, one per input.
/// A 429, a 5xx or a request that never reached it is tried again, after what `Retry-After` says or
/// half a second doubling each time; anything else (a 400: the model's name, say) fails at once.
async fn call(embed: bool, inputs: &[&str], model: &str) -> anyhow::Result<Vec<Value>> {
    let url = std::env::var("PONDRA_AI_URL").unwrap_or_else(|_| "http://127.0.0.1:11434/v1".into());
    let (path, body) = match embed {
        true => ("embeddings", json!({"model": model, "input": inputs})),
        false => ("chat/completions", json!({"model": model, "messages": [{"role": "user", "content": inputs[0]}]})),
    };
    let secs = std::env::var("PONDRA_AI_TIMEOUT_SECS").ok().and_then(|s| s.parse().ok()).unwrap_or(300);
    let mut wait = Duration::from_millis(500);
    for tries in 1.. {
        let mut req = crate::cluster::http_bare().post(format!("{}/{path}", url.trim_end_matches('/'))).json(&body).timeout(Duration::from_secs(secs)); // (not the nodes' client: its token is theirs alone)
        if let Ok(key) = std::env::var("PONDRA_AI_KEY") {
            req = req.bearer_auth(key);
        }
        let reply = req.send().await;
        let again = match &reply {
            Ok(r) if r.status() == 429 || r.status().is_server_error() => {
                let said = r.headers().get("retry-after").and_then(|v| v.to_str().ok()?.parse::<f64>().ok());
                Some(said.map_or(wait, |s| Duration::from_secs_f64(s.clamp(0.0, 60.0))))
            }
            Err(e) if (e.is_connect() || e.is_request()) && !e.is_timeout() => Some(wait),
            _ => None,
        };
        match again {
            Some(after) if tries < TRIES => {
                RETRIED.fetch_add(1, Relaxed);
                tokio::time::sleep(after).await;
                wait *= 2;
                continue;
            }
            _ => {}
        }
        let out: Value = reply?.error_for_status()?.json().await?;
        CALLS.fetch_add(1, Relaxed);
        let answers: Vec<Value> = match embed {
            true => {
                let mut data = out["data"].as_array().cloned().unwrap_or_default();
                data.sort_by_key(|d| d["index"].as_u64().unwrap_or(0)); // (in the inputs' order, whatever order they come in)
                data.into_iter().map(|d| d["embedding"].clone()).collect()
            }
            false => vec![out["choices"][0]["message"]["content"].clone()],
        };
        anyhow::ensure!(answers.len() == inputs.len() && answers.iter().all(|a| !a.is_null()), "no answer in {out}");
        return Ok(answers);
    }
    unreachable!()
}

fn default_model(embed: bool) -> String {
    match embed {
        true => std::env::var("PONDRA_EMBED_MODEL").unwrap_or_else(|_| "nomic-embed-text".into()),
        false => std::env::var("PONDRA_AI_MODEL").unwrap_or_else(|_| "llama3.2".into()),
    }
}

fn vector(of: DataType) -> DataType { DataType::List(Arc::new(Field::new("item", of, true))) }

/// JSON arrays of numbers as a `FLOAT[]` column.
fn vectors<'a>(answers: impl Iterator<Item = Option<&'a Value>>) -> ArrayRef {
    let mut out = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new("item", DataType::Float32, true)));
    for a in answers {
        match a.and_then(|v| v.as_array()) {
            Some(v) => {
                v.iter().for_each(|x| out.values().append_option(x.as_f64().map(|f| f as f32)));
                out.append(true);
            }
            None => out.append_null(),
        }
    }
    Arc::new(out.finish())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Metric {
    Cosine,
    CosineDistance,
    L2,
    Dot,
}

/// Cosine similarity or distance, Euclidean distance or dot product of two vectors, row by row (null
/// where either is null or holds a null, or where their lengths differ).
#[derive(Debug, PartialEq, Eq, Hash)]
struct Distance {
    name: &'static str,
    metric: Metric,
    signature: Signature,
}

impl ScalarUDFImpl for Distance {
    fn name(&self) -> &str { self.name }
    fn signature(&self) -> &Signature { &self.signature }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> { Ok(DataType::Float64) }

    /// Both sides become lists of one float type: `FLOAT` if either is (a query's vector, a list of
    /// doubles, is cast once rather than every row of the column), else `DOUBLE`.
    fn coerce_types(&self, types: &[DataType]) -> Result<Vec<DataType>> {
        let item = |t: &DataType| match t {
            DataType::List(f) | DataType::LargeList(f) | DataType::FixedSizeList(f, _) => Some(f.data_type().clone()),
            _ => None,
        };
        let [a, b] = types else { return plan_err!("{}(a, b): two vectors", self.name) };
        let items = [a, b].map(|t| item(t).or_else(|| t.is_null().then_some(DataType::Null)));
        let ok = |t: &DataType| t.is_numeric() || t.is_null();
        match items {
            [Some(x), Some(y)] if ok(&x) && ok(&y) => {
                let of = if [&x, &y].contains(&&DataType::Float32) || [&x, &y].iter().all(|t| t.is_null()) { DataType::Float32 } else { DataType::Float64 };
                Ok(vec![vector(of.clone()), vector(of)])
            }
            _ => plan_err!("{}(a, b) takes two vectors (lists of numbers), not {a} and {b}", self.name),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let both = args.args.iter().all(|a| matches!(a, ColumnarValue::Scalar(_)));
        let sides = args.args.iter().map(|a| match a {
            ColumnarValue::Array(a) => Ok((a.clone(), false)),
            ColumnarValue::Scalar(s) => Ok((s.to_array_of_size(1)?, true)),
        }).collect::<Result<Vec<_>>>()?;
        let rows = if both { 1 } else { args.number_rows };
        let out = match sides[0].0.data_type() {
            DataType::List(f) if f.data_type() == &DataType::Float32 => over::<Float32Type>(&sides, rows, self.metric),
            _ => over::<Float64Type>(&sides, rows, self.metric),
        };
        Ok(match both {
            true => ColumnarValue::Scalar(ScalarValue::try_from_array(&out, 0)?),
            false => ColumnarValue::Array(Arc::new(out)),
        })
    }
}

/// The metric for each row, straight from the lists' values (no copy of a vector); a side that is
/// one vector for every row (a literal, a parameter) has its length worked out once.
fn over<T: ArrowPrimitiveType>(sides: &[(ArrayRef, bool)], rows: usize, metric: Metric) -> Float64Array
where
    T::Native: Into<f64>,
{
    let lists = [sides[0].0.as_list::<i32>(), sides[1].0.as_list::<i32>()];
    let values = lists.map(|l| l.values().as_primitive::<T>());
    let vector = |s: usize, i: usize| -> Option<&[T::Native]> {
        let i = if sides[s].1 { 0 } else { i };
        let o = lists[s].value_offsets();
        let (from, to) = (o[i] as usize, o[i + 1] as usize);
        let holes = values[s].nulls().is_some_and(|n| n.slice(from, to - from).null_count() > 0);
        (lists[s].is_valid(i) && !holes).then(|| &values[s].values()[from..to])
    };
    let norm = |v: &[T::Native]| sum(v, v, |a, b| a * b).sqrt();
    let once = [0, 1].map(|s| sides[s].1.then(|| vector(s, 0).map(norm)).flatten());
    let mut out = Float64Builder::with_capacity(rows);
    for i in 0..rows {
        let (Some(x), Some(y)) = (vector(0, i), vector(1, i)) else {
            out.append_null();
            continue;
        };
        if x.len() != y.len() || x.is_empty() {
            out.append_null();
            continue;
        }
        let cosine = || {
            match once[0].unwrap_or_else(|| norm(x)) * once[1].unwrap_or_else(|| norm(y)) {
                0.0 => 0.0,
                n => sum(x, y, |a, b| a * b) / n,
            }
        };
        out.append_value(match metric {
            Metric::Dot => sum(x, y, |a, b| a * b),
            Metric::L2 => sum(x, y, |a, b| (a - b) * (a - b)).sqrt(),
            Metric::Cosine => cosine(),
            Metric::CosineDistance => 1.0 - cosine(),
        });
    }
    out.finish()
}

/// Σ f(x[i], y[i]) in doubles, over eight running sums so the compiler can use the CPU's vector
/// instructions (one running sum must add in order, which it can't spread over lanes).
#[inline(always)]
fn sum<N: Copy + Into<f64>>(x: &[N], y: &[N], f: impl Fn(f64, f64) -> f64) -> f64 {
    let mut lanes = [0f64; 8];
    let (xs, ys) = (x.chunks_exact(8), y.chunks_exact(8));
    let (xr, yr) = (xs.remainder(), ys.remainder());
    for (a, b) in xs.zip(ys) {
        for j in 0..8 {
            lanes[j] += f(a[j].into(), b[j].into());
        }
    }
    lanes.iter().sum::<f64>() + xr.iter().zip(yr).map(|(a, b)| f((*a).into(), (*b).into())).sum::<f64>()
}
