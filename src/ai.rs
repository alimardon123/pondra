//! Models, in SQL. Pondra carries no model: these call an OpenAI-compatible endpoint (your own
//! vLLM or Ollama, or a hosted one), so the binary stays small and the GPU stays outside it.
//!
//! * `ai_complete(prompt [, model])` — the model's answer as text.
//! * `ai_embed(text [, model])` — the embedding, a `FLOAT[]` to store in a column.
//! * `cosine_similarity(a, b)`, `l2_distance(a, b)`, `dot_product(a, b)` — over those columns, for
//!   the "most like this" queries (`ORDER BY cosine_similarity(v, $query) DESC LIMIT 10`).
//!
//! Where to call: `PONDRA_AI_URL` (default `http://127.0.0.1:11434/v1`, Ollama's), `PONDRA_AI_KEY`
//! if it wants one, `PONDRA_AI_MODEL` and `PONDRA_EMBED_MODEL` for the default models. Rows are
//! sent `CALLS` at a time, and a row whose call fails is null, so one bad row doesn't lose a query.
use datafusion::arrow::array::{Array, ArrayRef, AsArray, Float32Builder, ListArray, ListBuilder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::{exec_err, Result};
use datafusion::logical_expr::{async_udf::{AsyncScalarUDF, AsyncScalarUDFImpl}, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature, Volatility};
use datafusion::prelude::{create_udf, SessionContext};
use futures::StreamExt;
use serde_json::{json, Value};
use std::sync::Arc;

const CALLS: usize = 8; // rows in flight at once

pub fn register(ctx: &SessionContext) {
    for embed in [false, true] {
        let f = Ai { embed, signature: Signature::one_of(vec![TypeSignature::String(1), TypeSignature::String(2)], Volatility::Volatile) };
        ctx.register_udf(AsyncScalarUDF::new(Arc::new(f)).into_scalar_udf());
    }
    let vector = || DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
    for (name, kind) in [("cosine_similarity", Metric::Cosine), ("l2_distance", Metric::L2), ("dot_product", Metric::Dot)] {
        let f = move |args: &[ColumnarValue]| distance(args, kind);
        ctx.register_udf(create_udf(name, vec![vector(), vector()], DataType::Float64, Volatility::Immutable, Arc::new(f)));
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
            true => DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
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
        let one = |i: usize| async move {
            match input.is_valid(i) {
                true => call(self.embed, input.value(i), model.value(i)).await.ok(),
                false => None,
            }
        };
        let answers: Vec<Option<Value>> = futures::stream::iter((0..input.len()).map(one)).buffered(CALLS).collect().await;
        Ok(ColumnarValue::Array(match self.embed {
            true => vectors(&answers),
            false => {
                let mut out = StringBuilder::new();
                answers.iter().for_each(|a| out.append_option(a.as_ref().and_then(|v| v.as_str())));
                Arc::new(out.finish())
            }
        }))
    }
}

/// One call to the endpoint: the answer's text, or the embedding as a JSON array.
async fn call(embed: bool, input: &str, model: &str) -> anyhow::Result<Value> {
    let url = std::env::var("PONDRA_AI_URL").unwrap_or_else(|_| "http://127.0.0.1:11434/v1".into());
    let (path, body) = match embed {
        true => ("embeddings", json!({"model": model, "input": input})),
        false => ("chat/completions", json!({"model": model, "messages": [{"role": "user", "content": input}]})),
    };
    let mut req = crate::cluster::http().post(format!("{}/{path}", url.trim_end_matches('/'))).json(&body);
    if let Ok(key) = std::env::var("PONDRA_AI_KEY") {
        req = req.bearer_auth(key);
    }
    let out: Value = req.send().await?.error_for_status()?.json().await?;
    let answer = match embed {
        true => out["data"][0]["embedding"].clone(),
        false => out["choices"][0]["message"]["content"].clone(),
    };
    anyhow::ensure!(!answer.is_null(), "no answer in {out}");
    Ok(answer)
}

fn default_model(embed: bool) -> String {
    match embed {
        true => std::env::var("PONDRA_EMBED_MODEL").unwrap_or_else(|_| "nomic-embed-text".into()),
        false => std::env::var("PONDRA_AI_MODEL").unwrap_or_else(|_| "llama3.2".into()),
    }
}

/// JSON arrays of numbers as a `FLOAT[]` column.
fn vectors(answers: &[Option<Value>]) -> ArrayRef {
    let mut out = ListBuilder::new(Float32Builder::new()).with_field(Arc::new(Field::new("item", DataType::Float32, true)));
    for a in answers {
        match a.as_ref().and_then(|v| v.as_array()) {
            Some(v) => {
                v.iter().for_each(|x| out.values().append_option(x.as_f64().map(|f| f as f32)));
                out.append(true);
            }
            None => out.append_null(),
        }
    }
    Arc::new(out.finish())
}

#[derive(Clone, Copy)]
enum Metric {
    Cosine,
    L2,
    Dot,
}

/// Cosine similarity, Euclidean distance or dot product of two vector columns, row by row
/// (null where either is null, or where their lengths differ).
fn distance(args: &[ColumnarValue], metric: Metric) -> Result<ColumnarValue> {
    let rows = args.iter().find_map(|a| match a {
        ColumnarValue::Array(a) => Some(a.len()),
        _ => None,
    });
    let arrays: Vec<ArrayRef> = args.iter().map(|a| match a {
        ColumnarValue::Array(a) => Ok(a.clone()),
        ColumnarValue::Scalar(s) => s.to_array_of_size(rows.unwrap_or(1)),
    }).collect::<Result<_>>()?;
    let [a, b] = match &arrays[..] {
        [a, b] => [lists(a)?, lists(b)?],
        _ => return exec_err!("two vectors"),
    };
    let mut out = datafusion::arrow::array::Float64Builder::new();
    for i in 0..a.len() {
        if a.is_null(i) || b.is_null(i) {
            out.append_null();
            continue;
        }
        let (x, y) = (a.value(i), b.value(i));
        let (x, y) = (x.as_primitive::<datafusion::arrow::datatypes::Float32Type>().values().to_vec(), y.as_primitive::<datafusion::arrow::datatypes::Float32Type>().values().to_vec());
        if x.len() != y.len() || x.is_empty() {
            out.append_null();
            continue;
        }
        let dot: f64 = x.iter().zip(&y).map(|(x, y)| *x as f64 * *y as f64).sum();
        out.append_value(match metric {
            Metric::Dot => dot,
            Metric::L2 => x.iter().zip(&y).map(|(x, y)| (*x as f64 - *y as f64).powi(2)).sum::<f64>().sqrt(),
            Metric::Cosine => {
                let norm = |v: &[f32]| v.iter().map(|x| (*x as f64).powi(2)).sum::<f64>().sqrt();
                match norm(&x) * norm(&y) {
                    0.0 => 0.0,
                    n => dot / n,
                }
            }
        });
    }
    Ok(ColumnarValue::Array(Arc::new(out.finish())))
}

/// A vector column as lists of f32 (a list of any number type casts).
fn lists(a: &ArrayRef) -> Result<ListArray> {
    let want = DataType::List(Arc::new(Field::new("item", DataType::Float32, true)));
    Ok(datafusion::arrow::compute::cast(a, &want)?.as_list::<i32>().clone())
}
