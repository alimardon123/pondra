//! The cluster's scenarios in the sim (redesign step 3, pieces 3e and 3f): each on paused time, over a few seeds
//! (`PONDRA_SIM_SEED` runs one alone). The harness's own copies (tools/harness.py) stay for what the sim can't be: real
//! processes, sockets and buckets.
use super::cluster::Cluster;
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

/// The harness's `fence`: the leader freezes (its calls wait, as SIGSTOP makes them), the follower takes over, and the
/// old leader, thawed, has its write refused (fenced), rejoins as a follower, and nothing is lost.
#[tokio::test(start_paused = true)]
async fn a_frozen_leader_is_fenced_and_rejoins_as_a_follower() {
    for seed in seeds() {
        let c = Cluster::new(seed, 2);
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE events (producer TEXT, seq BIGINT)").await.unwrap();
        c.sql(0, "INSERT INTO events VALUES ('a', 1)").await.unwrap();
        c.start(1).await.unwrap();
        c.sql(1, "INSERT INTO events VALUES ('b', 1)").await.unwrap(); // (forwarded to the leader)
        c.faults(0).freeze();
        let took = c.until("the follower takes over", Duration::from_secs(60), || async { c.role(1).await.as_deref() == Some("leader") }).await.unwrap();
        assert!(took < Duration::from_secs(30), "seed {seed}: took over in {took:?}");
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
