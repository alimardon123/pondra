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
//! `?spread=1` spreads anyway.
use datafusion::physical_plan::ExecutionPlan;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

/// A link to another node: round trip (s), bytes per second.
#[derive(Clone, Copy, Debug)]
pub struct Link {
    pub rtt: f64,
    pub rate: f64,
}

const PROBE: usize = 8 << 20; // bytes a link is measured with
const WIRE: f64 = 3.0; // Arrow as a shuffle sends it (ZSTD) is about a third of its size in memory

static LINKS: LazyLock<Mutex<HashMap<String, (Link, Instant)>>> = LazyLock::new(Default::default);
static HERE: Mutex<Option<f64>> = Mutex::new(None); // bytes of tables read per second, one node

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
pub async fn slowest(nodes: &[String], me: &str) -> Option<Link> {
    if let Some((ms, mb)) = std::env::var("PONDRA_LINK").ok().and_then(|v| Some((v.split_once(',')?.0.parse::<f64>().ok()?, v.split_once(',')?.1.parse::<f64>().ok()?))) {
        return Some(Link { rtt: ms / 1e3, rate: mb * 1e6 }); // (`ms,MB/s`: a network known, or one to pretend)
    }
    let links = futures::future::join_all(nodes.iter().filter(|n| *n != me).map(|n| link(n))).await;
    links.into_iter().try_fold(Link { rtt: 0.0, rate: f64::MAX }, |a, l| Some(Link { rtt: a.rtt.max(l?.rtt), rate: a.rate.min(l?.rate) }))
}

async fn link(node: &str) -> Option<Link> {
    if let Some((l, at)) = LINKS.lock().unwrap().get(node) {
        if at.elapsed() < Duration::from_secs(600) {
            return Some(*l);
        }
    }
    let get = |bytes: usize| async move {
        let t = Instant::now();
        let r = crate::cluster::http().get(format!("http://{node}/cluster/probe?bytes={bytes}")).timeout(Duration::from_secs(20)).send().await.ok()?;
        let got = r.bytes().await.ok()?.len();
        (got == bytes).then(|| t.elapsed().as_secs_f64())
    };
    let mut rtt = f64::MAX;
    for _ in 0..3 {
        rtt = rtt.min(get(0).await?);
    }
    let rate = PROBE as f64 / (get(PROBE).await? - rtt).max(1e-4);
    let l = Link { rtt, rate };
    LINKS.lock().unwrap().insert(node.to_string(), (l, Instant::now()));
    Some(l)
}

/// Query `sql`, which read tables of `bytes`, took `took` here: how long it takes (asked again),
/// and the rate queries go at on one node (anything else).
pub fn ran_here(sql: &str, bytes: u64, took: Duration) {
    TOOK.lock().unwrap().put(key(sql), took.as_secs_f64());
    if bytes < 1 << 20 {
        return; // (too small to say anything about the rate)
    }
    let rate = bytes as f64 / took.as_secs_f64();
    let mut here = HERE.lock().unwrap();
    *here = Some(here.map_or(rate, |r| 0.7 * r + 0.3 * rate));
}

static TOOK: LazyLock<Mutex<lru::LruCache<u64, f64>>> = LazyLock::new(|| Mutex::new(lru::LruCache::new(std::num::NonZeroUsize::new(4096).unwrap())));

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
pub fn pays(sql: &str, link: Link, moved: u64, steps: usize, bytes: u64, n: usize) -> bool {
    let known = TOOK.lock().unwrap().get(&key(sql)).copied();
    let Some(here) = known.or_else(|| HERE.lock().unwrap().map(|rate| bytes as f64 / rate)) else { return false };
    let saved = here * (1.0 - 1.0 / n as f64);
    let cost = link.rtt * (2.0 + 3.0 * steps as f64) + moved as f64 / WIRE / link.rate;
    if std::env::var_os("PONDRA_DEBUG_SPREAD").is_some() {
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
