//! Spread a query only when it pays (ADR-020). A spread query moves rows between the nodes — each
//! shuffle step's exchanges, and what reaches the coordinator — so it pays only where that costs
//! less than the work the other nodes take on. In one data centre it does; between GitHub's
//! runners (17–54 ms apart, 50–150 MB/s) three nodes took twice as long as one, every shuffle
//! costing about 0.7 s plus 15 ms a MB.
//!
//! So before a query spreads, the node it came to weighs the two: the bytes its plan would move
//! (DataFusion's estimates at each exchange, compressed as they are on the wire) at the speed of
//! its slowest link to another node, plus a few round trips a step, against the time the query
//! would take here less a node's share of it — what it took here when last asked, else the bytes
//! of the tables it reads at the rate this node has been reading them. Links are measured
//! (`probe`), and so are those times (`ran_here`); until a query has run here, queries stay here.
//! Once a query has run here and spread twice, what each took decides (`ran_spread`): the model
//! only guesses until then. `?spread=1` spreads anyway.
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use datafusion::common::instant::Instant;
use std::time::Duration;

/// A link to another node: round trip (s), bytes per second.
#[derive(Clone, Copy, Debug)]
pub struct Link {
    pub rtt: f64,
    pub rate: f64,
}

const PROBE: usize = 8 << 20; // bytes a link is measured with
const WIRE: f64 = 3.0; // Arrow as a shuffle sends it (ZSTD) is about a third of its size in memory
/// Round trips a shuffle step costs besides its bytes: its plan to every node, buckets written and
/// fetched from each, word of each node's end. Between GitHub's runners a step took about 0.7 s
/// at 17–54 ms apart, some 20 round trips; in one data centre that is a few milliseconds.
const ROUND_TRIPS: f64 = 20.0;

/// What a lake's node has seen of its links and its queries' times: whether spreading a query pays.
pub struct Seen {
    links: Mutex<HashMap<String, (Link, Instant)>>,
    here: Mutex<Option<f64>>, // bytes of tables read per second, one node
    took: Times,
    spread_took: Times,
}

impl Default for Seen {
    fn default() -> Self {
        Seen {
            links: Default::default(),
            here: Mutex::new(None),
            took: Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(4096).unwrap())),
            spread_took: Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(4096).unwrap())),
        }
    }
}

impl crate::lake_state::LakeState for Seen {
    const NAME: &'static str = "guard";
    const ABOUT: &'static str = "the links to the other nodes it measured and how long its queries took, here and spread";
    fn make() -> Self { Seen::default() }
}

/// The body of `GET /cluster/probe?bytes=n`: bytes that don't compress (a link's measure).
pub fn probe(bytes: usize) -> Vec<u8> {
    static NOISE: LazyLock<Vec<u8>> = LazyLock::new(|| {
        let mut x = 0x9E37_79B9_7F4A_7C15u64;
        (0..PROBE).map(|_| { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x as u8 }).collect()
    });
    NOISE[..bytes.min(PROBE)].to_vec()
}

/// The slowest link from here to the other nodes (the longest round trip, the lowest rate): a
/// shuffle waits for it. Measured every 10 minutes, or given (`PONDRA_LINK=ms,MB/s`); None when a
/// node can't be measured.
pub async fn slowest(lake: &crate::store::Lake, nodes: &[String], me: &str) -> Option<Link> {
    if let Some((ms, mb)) = crate::env::get("PONDRA_LINK").and_then(|v| Some((v.split_once(',')?.0.parse::<f64>().ok()?, v.split_once(',')?.1.parse::<f64>().ok()?))) {
        return Some(Link { rtt: ms / 1e3, rate: mb * 1e6 }); // (`ms,MB/s`: a network known, or one to pretend)
    }
    let links = futures::future::join_all(nodes.iter().filter(|n| *n != me).map(|n| link(lake, n))).await;
    links.into_iter().try_fold(Link { rtt: 0.0, rate: f64::MAX }, |a, l| Some(Link { rtt: a.rtt.max(l?.rtt), rate: a.rate.min(l?.rate) }))
}

async fn link(lake: &crate::store::Lake, node: &str) -> Option<Link> {
    let Ok(seen) = lake.state::<Seen>() else { return None };
    if let Some((l, at)) = seen.links.lock().unwrap().get(node) {
        if at.elapsed() < Duration::from_secs(600) {
            return Some(*l);
        }
    }
    let get = |bytes: usize| async move {
        let t = Instant::now();
        let r = crate::dial::call(&lake.node, node, crate::dial::Call::get(format!("/cluster/probe?bytes={bytes}")).timeout(Duration::from_secs(20))).await.ok()?;
        let got = r.bytes().await.ok()?.len();
        (got == bytes).then(|| t.elapsed().as_secs_f64())
    };
    let mut rtt = f64::MAX;
    for _ in 0..3 {
        rtt = rtt.min(get(0).await?);
    }
    let rate = PROBE as f64 / (get(PROBE).await? - rtt).max(1e-4);
    let l = Link { rtt, rate };
    seen.links.lock().unwrap().insert(node.to_string(), (l, Instant::now()));
    Some(l)
}

/// Query `sql`, which read tables of `bytes`, took `took` here: how long it takes (asked again),
/// and the rate queries go at on one node (anything else).
pub fn ran_here(lake: &crate::store::Lake, sql: &str, bytes: u64, took: Duration) {
    let Ok(seen) = lake.state::<Seen>() else { return };
    note(&seen.took, sql, took);
    if bytes < 1 << 20 {
        return; // (too small to say anything about the rate)
    }
    let rate = bytes as f64 / took.as_secs_f64();
    let mut here = seen.here.lock().unwrap();
    *here = Some(here.map_or(rate, |r| 0.7 * r + 0.3 * rate));
}

/// Query `sql` took `took` spread over the nodes.
pub fn ran_spread(lake: &crate::store::Lake, sql: &str, took: Duration) {
    let Ok(seen) = lake.state::<Seen>() else { return };
    note(&seen.spread_took, sql, took)
}

/// A query's last three times, one way. A busy machine makes a run slower, never faster, so the
/// best of them is what that way costs: one slow run doesn't turn the choice.
type Times = Mutex<lru::LruCache<u64, Vec<f64>>>;

fn note(times: &Times, sql: &str, took: Duration) {
    let mut all = times.lock().unwrap();
    let t = all.get_or_insert_mut(key(sql), Vec::new);
    t.push(took.as_secs_f64());
    if t.len() > 3 {
        t.remove(0);
    }
}

fn best(times: &Times, sql: &str) -> Option<f64> { times.lock().unwrap().get(&key(sql)).and_then(|t| t.iter().copied().reduce(f64::min)) }

/// The best of a query's spread times, and how many there are. A first spread run is the other
/// nodes' first sight of the query (their caches cold, the links just measured), so one alone
/// says spreading is slower only if a second agrees: one slow first run kept a query on one node
/// for good (the cluster bench's q1: 2.1 s spread the first time, 1.0 s after, so back to 2.4 s).
fn spread_runs(seen: &Seen, sql: &str) -> (Option<f64>, usize) {
    let mut all = seen.spread_took.lock().unwrap();
    all.get(&key(sql)).map_or((None, 0), |t| (t.iter().copied().reduce(f64::min), t.len()))
}

/// A query as the same query asked again (its comments and spacing aside).
fn key(sql: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    sql.lines().map(|l| l.split("--").next().unwrap_or("")).flat_map(str::split_whitespace).for_each(|w| w.hash(&mut h));
    h.finish()
}

/// Does spreading query `sql` pay: moving `moved` bytes (as they are in memory) in `steps` steps
/// over `link`, to share out a query over tables of `bytes` among `n` nodes? No if nothing has run
/// here yet.
pub fn pays(lake: &crate::store::Lake, sql: &str, link: Link, moved: u64, steps: usize, bytes: u64, n: usize) -> bool {
    let Ok(seen) = lake.state::<Seen>() else { return false };
    let known = best(&seen.took, sql);
    let learned = !crate::env::is_set("PONDRA_LINK"); // (a network pretended: the model alone)
    if let (Some(here), (Some(spread), runs), true) = (known, spread_runs(&seen, sql), learned) {
        if spread < here || runs >= 2 {
            if crate::env::is_set("PONDRA_DEBUG_SPREAD") {
                eprintln!("spread: ran both ways, here {here:.3}s, spread {spread:.3}s ({runs} runs)");
            }
            return spread < here; // (it ran both ways: no need to guess)
        }
    }
    let Some(here) = known.or_else(|| seen.here.lock().unwrap().map(|rate| bytes as f64 / rate)) else { return false };
    let saved = here * (1.0 - 1.0 / n as f64);
    let cost = link.rtt * (2.0 + ROUND_TRIPS * steps as f64) + moved as f64 / WIRE / link.rate;
    if crate::env::is_set("PONDRA_DEBUG_SPREAD") {
        eprintln!("spread: here ~{here:.3}s, spread saves ~{saved:.3}s, costs ~{cost:.3}s ({:.1} MB in {steps} steps; {link:?})", moved as f64 / 1e6);
    }
    cost < saved
}

/// What `plan` puts out, in bytes, as DataFusion estimates it (rows × a row's width where it knows
/// only the rows); None if it doesn't know.
pub fn bytes_out(plan: &Arc<dyn ExecutionPlan>) -> Option<u64> {
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    let s = StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new()).ok()?;
    if let Some(b) = s.total_byte_size.get_value() {
        return Some(*b as u64);
    }
    let width: usize = plan.schema().fields().iter().map(|f| f.data_type().primitive_width().unwrap_or(24)).sum();
    s.num_rows.get_value().map(|rows| (rows * width) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn two_lakes_keep_their_query_times_apart() -> anyhow::Result<()> {
        let dir_a = std::env::temp_dir().join(format!("pondra-two-guards-{}", uuid::Uuid::new_v4()));
        let dir_b = std::env::temp_dir().join(format!("pondra-two-guards-{}", uuid::Uuid::new_v4()));
        let a = crate::store::Lake::open(dir_a.to_str().unwrap(), true, false, crate::node_state::NodeState::unhosted()).await?;
        let b = crate::store::Lake::open(dir_b.to_str().unwrap(), true, false, crate::node_state::NodeState::unhosted()).await?;
        ran_here(&a, "select 1", 0, Duration::from_millis(50));
        let (seen_a, seen_b) = (a.state::<Seen>()?, b.state::<Seen>()?);
        assert!(best(&seen_a.took, "select 1").is_some(), "lake a forgot its query's time");
        assert!(best(&seen_b.took, "select 1").is_none(), "lake b knows a query it never ran");
        a.cat.close().await?;
        b.cat.close().await?;
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
        Ok(())
    }
}
