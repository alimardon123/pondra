//! The cluster's scenarios in the sim (redesign step 3, pieces 3e and 3f): each on paused time, over a few seeds
//! (`PONDRA_SIM_SEED` runs one alone). The harness's own copies (tools/harness.py) stay for what the sim can't be: real
//! processes, sockets and buckets.
use super::cluster::Cluster;
use crate::terms::LEASE;
use std::time::Duration;

/// The seeds a scenario runs over: `PONDRA_SIM_SEED`'s alone, else 1 to `PONDRA_SIM_SEEDS` (2 by default).
fn seeds() -> Vec<u64> {
    if let Some(one) = crate::env::parse::<u64>("PONDRA_SIM_SEED") {
        return vec![one];
    }
    (1..=crate::env::parse::<u64>("PONDRA_SIM_SEEDS").unwrap_or(2)).collect()
}

/// The rows of `answer` (a `/sql` answer: a JSON array of objects).
fn rows(answer: &serde_json::Value) -> Vec<serde_json::Value> { answer.as_array().cloned().unwrap_or_default() }

/// How many rows member `m` sees in `table`, or none while it can't say.
async fn count(c: &Cluster, m: usize, table: &str) -> Option<i64> {
    let a = c.sql(m, &format!("SELECT count(*) AS n FROM {table}")).await.ok()?;
    rows(&a).first()?["n"].as_i64()
}

/// Two members: the first leads, the second follows it, and a write sent to the follower is the leader's.
#[tokio::test(start_paused = true)]
async fn a_second_member_follows_and_its_writes_go_to_the_leader() {
    for seed in seeds() {
        let c = Cluster::new(seed, 2);
        c.start(0).await.unwrap();
        assert_eq!(c.role(0).await.as_deref(), Some("leader"), "seed {seed}");
        c.sql(0, "CREATE TABLE t (k BIGINT, v TEXT)").await.unwrap();
        c.start(1).await.unwrap();
        assert_eq!(c.role(1).await.as_deref(), Some("follower"), "seed {seed}");
        c.sql(1, "INSERT INTO t VALUES (1, 'from m2'), (2, 'from m2')").await.unwrap();
        c.until("the leader has the follower's rows", Duration::from_secs(30), || async { count(&c, 0, "t").await == Some(2) }).await.unwrap();
        c.until("the follower reads them too", Duration::from_secs(30), || async { count(&c, 1, "t").await == Some(2) }).await.unwrap();
    }
}

/// The harness's `fence`: the leader freezes (its calls wait, as SIGSTOP makes them), the follower that heard it takes
/// over once the lease is out, and the old leader, thawed, has its write refused (fenced), rejoins as a follower, and
/// nothing is lost.
#[tokio::test(start_paused = true)]
async fn a_frozen_leader_is_fenced_and_rejoins_as_a_follower() {
    for seed in seeds() {
        let c = Cluster::new(seed, 2);
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE events (producer TEXT, seq BIGINT)").await.unwrap();
        c.sql(0, "INSERT INTO events VALUES ('a', 1)").await.unwrap();
        c.start(1).await.unwrap();
        c.sql(1, "INSERT INTO events VALUES ('b', 1)").await.unwrap(); // (forwarded to the leader)
        c.until("the follower hears its leader", Duration::from_secs(10), || async { c.known(1).await == 2 }).await.unwrap();
        c.faults(0).freeze();
        let took = c.until("the follower takes over", Duration::from_secs(60), || async { c.role(1).await.as_deref() == Some("leader") }).await.unwrap();
        println!("seed {seed}: the follower took over after {:.1} s on the sim's clock", took.as_secs_f64()); // (the harness's `fence` says it too)
        assert!(took < LEASE + Duration::from_secs(5), "seed {seed}: the follower took {took:?} to take over, the lease being {LEASE:?}");
        c.sql(1, "INSERT INTO events VALUES ('b', 2)").await.unwrap();
        c.faults(0).thaw();
        let stale = c.sql(0, "INSERT INTO events VALUES ('a', 2)").await; // (the old leader still believes it leads)
        c.until("the old leader rejoins as a follower", Duration::from_secs(60), || async { c.role(0).await.as_deref() == Some("follower") }).await.unwrap();
        if stale.is_err() {
            c.sql(0, "INSERT INTO events VALUES ('a', 2)").await.unwrap(); // (refused: sent again, through the new leader)
        }
        let want = serde_json::json!([{"producer": "a", "n": 2}, {"producer": "b", "n": 2}]);
        let got = || async { c.sql(1, "SELECT producer, count(*) AS n FROM events GROUP BY producer ORDER BY producer").await.ok() };
        c.until("every row once", Duration::from_secs(30), || async { got().await.as_ref() == Some(&want) }).await.unwrap();
    }
}

/// A follower that never heard its leader (it may be outside the cluster's network) never deposes a live one: with the
/// leader frozen before the follower's first beat, it takes over only once the leader's alive mark is stale (30 s).
#[tokio::test(start_paused = true)]
async fn a_follower_that_never_heard_its_leader_waits_for_its_mark_to_go_stale() {
    for seed in seeds() {
        let c = Cluster::new(seed, 2);
        c.start(0).await.unwrap();
        c.start(1).await.unwrap();
        c.faults(0).freeze(); // (before the follower's first beat, a second on)
        let took = c.until("the follower takes over", Duration::from_secs(60), || async { c.role(1).await.as_deref() == Some("leader") }).await.unwrap();
        assert!(took > Duration::from_secs(25), "seed {seed}: the follower took over after {took:?}, though it never heard its leader");
    }
}

/// The crash points the harness's `crash` stops its node at, and how often each time it passes one.
const POINTS: [(&str, f64); 3] = [("after_seg_put", 0.03), ("after_commit", 0.01), ("after_parquet_put", 0.2)];

/// Member 0 started again once it is down. A start its crash points keep ending (a tiering round redone before it
/// serves, any file of which may stop it again) is given one start without the tiering's point after 20, as the
/// harness's `crash` does; the next start has it again.
async fn back_up(c: &Cluster) {
    for _ in 0..20 {
        c.kill(0);
        if c.start(0).await.is_ok() {
            return;
        }
    }
    c.faults(0).stop_at("after_parquet_put", 0.0);
    c.kill(0);
    c.start(0).await.expect("a start without the tiering's crash point serves");
    c.faults(0).stop_at("after_parquet_put", 0.2);
}

/// `body` to `path` on member 0 until it is taken, starting the member again whenever it is down.
async fn set_up(c: &Cluster, path: &str, body: &str) {
    for _ in 0..20 {
        if matches!(c.ask(0, "POST", path, body.as_bytes().to_vec()).await, Ok((200..=299, _))) {
            return;
        }
        if !c.up(0) {
            back_up(c).await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{path}: the member kept stopping\n{}", c.met());
}

/// An idempotent producer, as the harness's: each batch sent with its sequence number until it is taken.
async fn produce(c: &Cluster, name: &str, batches: u64, size: u64) {
    for seq in 1..=batches {
        let body: String = (0..size).map(|i| format!("{{\"producer\":\"{name}\",\"seq\":{seq},\"i\":{i},\"ts\":0.0}}\n")).collect();
        while !matches!(c.ask(0, "POST", &format!("/append/events?producer={name}&seq={seq}"), body.clone().into_bytes()).await, Ok((200..=299, _))) {
            tokio::time::sleep(Duration::from_millis(50)).await; // (down, or stopped at a point: the same batch again)
        }
    }
}

/// The harness's `crash`: one member stops at the commit's and the tiering's crash points, and is killed now and then,
/// while three idempotent producers write and a streaming task and two views follow; whenever it is down it is started
/// again. Then, started without them: every event is there once, and so is the task's output and each view's.
#[tokio::test(start_paused = true)]
async fn crashes_and_kills_lose_and_double_nothing() {
    let (producers, batches, size) = (3u64, 20u64, 10u64);
    for seed in seeds() {
        let c = Cluster::new(seed, 1);
        c.set_start(|s| (s.flush_ms, s.tier_secs, s.task_ms, s.retain_secs) = (50, 1.0, 200, 0));
        POINTS.iter().for_each(|(p, odds)| c.faults(0).stop_at(p, *odds));
        back_up(&c).await;
        set_up(&c, "/tables/events", r#"[["producer","Utf8"],["seq","Int64"],["i","Int64"],["ts","Float64"]]"#).await;
        set_up(&c, "/tasks/copy", r#"{"source":"events","target":"events_copy","sql":"SELECT producer, seq, i FROM events WHERE i % 2 = 0"}"#).await;
        set_up(&c, "/views/per_producer", "SELECT producer, count(*) AS n, sum(i) AS s FROM events GROUP BY producer").await;
        set_up(&c, "/views/thirds", "SELECT producer, seq, i FROM events WHERE i % 3 = 0").await;
        let done = std::sync::atomic::AtomicBool::new(false);
        let (mut roll, mut kills) = (seed, 0);
        let mut dice = move |n: u64| {
            roll = roll.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
            (roll >> 33) % n
        };
        let cluster = &c;
        let writes = async {
            futures::future::join_all((0..producers).map(|k| async move { produce(cluster, &format!("p{k}"), batches, size).await }))
            .await;
            done.store(true, std::sync::atomic::Ordering::Relaxed);
        };
        let chaos = async {
            while !done.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_millis(200 + dice(1300))).await;
                if dice(2) == 0 {
                    c.kill(0);
                    kills += 1;
                }
                if !c.up(0) {
                    back_up(&c).await;
                }
            }
        };
        futures::join!(writes, chaos);
        tokio::time::sleep(Duration::from_millis(1500)).await; // (the task catches up)
        let stops = c.faults(0).met().iter().filter(|m| m.starts_with("stopped at")).count();
        c.kill(0);
        c.faults(0).calm();
        c.set_start(|s| (s.tier_secs, s.task_ms) = (0.0, 100));
        c.start(0).await.unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        c.ask(0, "POST", "/tier", vec![]).await.unwrap();
        let one = move |sql: String| async move { rows(&cluster.sql(0, &sql).await.unwrap()).first().cloned().unwrap_or_default() };
        let (want, per) = ((producers * batches * size) as i64, (batches * size) as i64);
        let sums = (batches * (0..size).sum::<u64>()) as i64;
        let thirds = (producers * batches * (0..size).step_by(3).count() as u64) as i64;
        let unique = "count(DISTINCT producer || ':' || seq || ':' || i) AS uniq";
        let events = one(format!("SELECT count(*) AS n, {unique} FROM events")).await;
        let copy = one(format!("SELECT count(*) AS n, {unique} FROM events_copy")).await;
        let third = one(format!("SELECT count(*) AS n, {unique} FROM thirds")).await;
        let views = one("SELECT count(*) AS k, min(n) AS lo, max(n) AS hi, min(s) AS slo, max(s) AS shi FROM per_producer".into()).await;
        println!("seed {seed}: killed {kills}, stopped at a point {stops}: events {events}, task output {copy}, views {views} {third}");
        let met = c.met();
        assert_eq!(events, serde_json::json!({"n": want, "uniq": want}), "seed {seed}: events\n{met}");
        assert_eq!(copy, serde_json::json!({"n": want / 2, "uniq": want / 2}), "seed {seed}: the task's output\n{met}");
        assert_eq!(views, serde_json::json!({"k": producers, "lo": per, "hi": per, "slo": sums, "shi": sums}), "seed {seed}: per_producer\n{met}");
        assert_eq!(third, serde_json::json!({"n": thirds, "uniq": thirds}), "seed {seed}: thirds\n{met}");
    }
}
