//! The cluster's scenarios in the sim (redesign step 3, pieces 3e and 3f): each on paused time, over a few seeds
//! (`PONDRA_SIM_SEED` runs one alone). The harness's own copies (tools/harness.py) stay for what the sim can't be: real
//! processes, sockets and buckets.
use super::cluster::Cluster;
use super::faults::{Faults, Odds};
use crate::terms::LEASE;
use std::time::Duration;

/// The seeds a scenario runs over: `PONDRA_SIM_SEED`'s alone, else `PONDRA_SIM_SEEDS` of them (2 by default) from
/// `PONDRA_SIM_FROM` (1 by default). CI runs 8 a pull request, and 200 a night from one the date names (sim.yml); a
/// failing seed is named in its failure, and `PONDRA_SIM_SEED` runs it again alone.
fn seeds() -> Vec<u64> {
    if let Some(one) = crate::env::parse::<u64>("PONDRA_SIM_SEED") {
        return vec![one];
    }
    let from = crate::env::parse::<u64>("PONDRA_SIM_FROM").unwrap_or(1);
    (from..from + crate::env::parse::<u64>("PONDRA_SIM_SEEDS").unwrap_or(2)).collect()
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
    let (producers, batches, size) = (3u64, 40u64, 10u64);
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
                // (the harness's pace is 0.2 to 1.5 s, but its writes take real time: the sim's take the commits' alone)
                tokio::time::sleep(Duration::from_millis(100 + dice(400))).await;
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

/// The harness's `finals_on_three`: rows appended to three members (whichever answers: the same producer's batch is
/// applied once) while the leader is killed twice; a materialized view's final groups are each kept once, equal to the
/// ad hoc query, on every member.
#[tokio::test(start_paused = true)]
async fn finals_on_three_are_each_kept_once_through_two_leader_kills() {
    let iso = |s: i64| chrono::DateTime::from_timestamp(s, 0).unwrap().format("%Y-%m-%dT%H:%M:%S").to_string();
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        c.set_start(|s| s.tier_secs = 1.0);
        for m in 0..3 {
            c.start(m).await.unwrap();
        }
        c.sql(0, "CREATE TABLE clicks (visitor VARCHAR, ts TIMESTAMP, amount BIGINT)").await.unwrap();
        c.sql(0, "CREATE MATERIALIZED VIEW per_minute WITH (lateness = '2 seconds') AS SELECT date_bin(INTERVAL '1 minute', ts) AS minute, visitor, count(*) AS n, sum(amount) AS total FROM clicks GROUP BY 1, 2 EMIT FINAL").await.unwrap();
        let (base, mut seqs, mut killed) = (1_790_000_000 / 60 * 60, [0u64; 3], 0);
        let append = |rows: Vec<serde_json::Value>, k: usize, seq: u64| {
            let (c, body) = (&c, rows.iter().map(|r| format!("{r}\n")).collect::<String>());
            async move {
                for t in 0..200 {
                    let m = (k + t) % 3;
                    if c.up(m) && matches!(c.ask(m, "POST", &format!("/append/clicks?producer=p{}&seq={seq}", k % 3), body.clone().into_bytes()).await, Ok((200..=299, _))) {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                panic!("seed {seed}: an append was never taken\n{}", c.met());
            }
        };
        for k in 0..120usize {
            // (20 minutes of event time)
            let rows = (0..5).map(|j| serde_json::json!({"visitor": format!("v{}", (k + j) % 4), "ts": iso(base + k as i64 * 10 + j as i64), "amount": k * 10 + j})).collect();
            seqs[k % 3] += 1;
            append(rows, k, seqs[k % 3]).await;
            if k == 40 || k == 80 {
                let mut lead = None;
                for m in 0..3 {
                    if c.role(m).await.as_deref() == Some("leader") {
                        lead = Some(m);
                    }
                }
                let lead = lead.unwrap_or_else(|| panic!("seed {seed}: no member leads\n{}", c.met()));
                c.kill(lead);
                killed += 1;
                tokio::time::sleep(Duration::from_secs(6)).await;
                c.start(lead).await.unwrap();
            }
        }
        seqs[0] += 1;
        append(vec![serde_json::json!({"visitor": "v0", "ts": iso(base + 3600), "amount": 0})], 0, seqs[0]).await; // (the watermark past them all)
        let want = c.sql(0, &format!("SELECT date_bin(INTERVAL '1 minute', ts) AS minute, visitor, count(*) AS n, sum(amount) AS total FROM clicks WHERE ts < '{}' GROUP BY 1, 2 ORDER BY 1, 2", iso(base + 1200))).await.unwrap();
        assert_eq!(rows(&want).len(), 80, "seed {seed}: the ad hoc query's groups");
        for m in 0..3 {
            let got = || async { c.sql(m, "SELECT minute, visitor, n, total FROM per_minute ORDER BY 1, 2").await.ok() };
            c.until(&format!("member {m}'s final groups"), Duration::from_secs(60), || async { got().await.as_ref() == Some(&want) }).await.unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        }
        let groups = c.sql(0, "SELECT count(*) AS g, count(DISTINCT (minute, visitor)) AS d FROM per_minute").await.unwrap();
        assert_eq!(rows(&groups).first(), Some(&serde_json::json!({"g": 80, "d": 80})), "seed {seed}: each group once");
        assert_eq!(killed, 2);
        println!("seed {seed}: 80 final groups once on each of three members, the leader killed twice");
    }
}

/// SurrealDB's R3: keyed writes from three members at once (a follower's go to the leader), while the leader is killed
/// once: no key is ever doubled, every key written is there, and each holds the value one of its writers wrote last
/// (a writer's own writes land in order), on every member.
#[tokio::test(start_paused = true)]
async fn keyed_writes_from_three_members_never_double_a_key() {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        for m in 0..3 {
            c.start(m).await.unwrap();
        }
        c.sql(0, "CREATE TABLE kv (id BIGINT PRIMARY KEY, v BIGINT)").await.unwrap();
        let (cluster, rounds) = (&c, AtomicUsize::new(0));
        let rounds = &rounds;
        let writer = |m: usize| async move {
            let (mut roll, mut last) = (seed.wrapping_mul(31).wrapping_add(m as u64), BTreeMap::new());
            for round in 0..30u64 {
                let rows: BTreeMap<u64, u64> = (0..20)
                    .map(|j| {
                        roll = roll.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
                        ((roll >> 33) % 100, m as u64 * 1_000_000 + round * 100 + j)
                    })
                    .collect();
                let values = rows.iter().map(|(k, v)| format!("({k}, {v})")).collect::<Vec<_>>().join(", ");
                let sql = format!("INSERT INTO kv VALUES {values} ON CONFLICT (id) DO UPDATE SET v = excluded.v");
                // (to its own member until taken: one a kill cut short is sent again, and sets the same values again)
                let mut tries = 0;
                while !cluster.up(m) || {
                    let r = cluster.sql(m, &sql).await;
                    tries += 1;
                    assert!(tries < 100, "seed {seed}: member {m}'s write was never taken: {r:?}\n{}", cluster.met());
                    r.is_err()
                } {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                last.extend(rows);
                rounds.fetch_add(1, Relaxed);
            }
            last
        };
        let chaos = async {
            while rounds.load(Relaxed) < 30 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let mut lead = None;
            for m in 0..3 {
                if c.role(m).await.as_deref() == Some("leader") {
                    lead = Some(m);
                }
            }
            let lead = lead.unwrap_or_else(|| panic!("seed {seed}: no member leads\n{}", c.met()));
            c.kill(lead);
            tokio::time::sleep(Duration::from_secs(6)).await;
            c.start(lead).await.unwrap();
        };
        let (lasts, ()) = futures::join!(futures::future::join_all((0..3).map(writer)), chaos);
        let keys: BTreeSet<u64> = lasts.iter().flat_map(|l| l.keys().copied()).collect();
        for m in 0..3 {
            let seen = || async {
                let got = rows(&c.sql(m, "SELECT id, v FROM kv ORDER BY id").await.ok()?);
                Some(got.iter().map(|r| (r["id"].as_u64().unwrap_or(u64::MAX), r["v"].as_u64().unwrap_or(u64::MAX))).collect::<Vec<_>>())
            };
            let right = |got: &[(u64, u64)]| got.iter().map(|(k, _)| *k).collect::<BTreeSet<_>>() == keys && got.len() == keys.len() && got.iter().all(|(k, v)| lasts.iter().any(|l| l.get(k) == Some(v)));
            c.until(&format!("member {m} holds each key once, at a last write"), Duration::from_secs(30), || async { seen().await.is_some_and(|g| right(&g)) })
                .await
                .unwrap_or_else(|e| panic!("seed {seed}: {e}"));
        }
        let groups = c.sql(0, "SELECT count(*) AS n, count(DISTINCT id) AS d FROM kv").await.unwrap();
        assert_eq!(rows(&groups).first(), Some(&serde_json::json!({"n": keys.len(), "d": keys.len()})), "seed {seed}: a key doubled");
        println!("seed {seed}: {} keys from three members, each once, the leader killed once", keys.len());
    }
}

/// The harness's `sequences`, its cluster part: `nextval` from nine clients on three members at once (a follower's go to
/// the leader), while the leader is killed once: every value handed out is handed out once (a value a kill cut off
/// before its answer is skipped, never given again).
#[tokio::test(start_paused = true)]
async fn nextval_on_three_members_hands_out_each_value_once_through_a_leader_kill() {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        for m in 0..3 {
            c.start(m).await.unwrap();
        }
        c.sql(1, "CREATE SEQUENCE s").await.unwrap();
        let (cluster, taken) = (&c, AtomicUsize::new(0));
        let taken = &taken;
        let client = |k: usize| async move {
            let (m, mut got) = (k % 3, vec![]);
            for _ in 0..40 {
                let mut tries = 0;
                let values = loop {
                    if cluster.up(m) {
                        match cluster.sql(m, "SELECT nextval('s') AS v FROM range(25)").await {
                            Ok(a) => break rows(&a).iter().map(|r| r["v"].as_i64().unwrap_or(-1)).collect::<Vec<_>>(),
                            Err(e) => {
                                tries += 1;
                                assert!(tries < 100, "seed {seed}: client {k}'s nextval never answered: {e}\n{}", cluster.met());
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                };
                got.extend(values);
                taken.fetch_add(1, Relaxed);
            }
            got
        };
        let chaos = async {
            while taken.load(Relaxed) < 120 {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let mut lead = None;
            for m in 0..3 {
                if c.role(m).await.as_deref() == Some("leader") {
                    lead = Some(m);
                }
            }
            let lead = lead.unwrap_or_else(|| panic!("seed {seed}: no member leads\n{}", c.met()));
            c.kill(lead);
            tokio::time::sleep(Duration::from_secs(6)).await;
            c.start(lead).await.unwrap();
        };
        let (got, ()) = futures::join!(futures::future::join_all((0..9).map(client)), chaos);
        let all: Vec<i64> = got.into_iter().flatten().collect();
        let distinct: std::collections::BTreeSet<i64> = all.iter().copied().collect();
        assert_eq!(all.len(), 9000, "seed {seed}: values answered");
        assert_eq!(distinct.len(), all.len(), "seed {seed}: a value handed out twice\n{}", c.met());
        assert!(distinct.first().is_some_and(|v| *v >= 1), "seed {seed}: the least value {:?}", distinct.first());
        println!("seed {seed}: 9,000 values from nine clients on three members, each once (up to {}), the leader killed once", distinct.last().unwrap());
    }
}

/// Guests `0..guests` write at once, each `statements` statements of `size` rows `(g, s, i)` into `t`; a statement that
/// fails is run again with its job, as its user runs it again. How many runs again it took.
async fn guests_write(c: &Cluster, guests: usize, statements: usize, size: usize) -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
    let again = &AtomicUsize::new(0);
    let guest = |g: usize| async move {
        for s in 0..statements {
            let values = (0..size).map(|i| format!("({g}, {s}, {i})")).collect::<Vec<_>>().join(", ");
            let (sql, job) = (format!("INSERT INTO t VALUES {values}"), format!("g{g}-s{s}"));
            let mut tries = 0;
            while let Err(e) = c.guest(g, &sql, &job).await {
                (tries, _) = (tries + 1, again.fetch_add(1, Relaxed));
                assert!(tries < 20, "seed {}: {job} never went: {e:#}\n{}", c.seed, c.met());
            }
        }
    };
    futures::future::join_all((0..guests).map(guest)).await;
    again.load(Relaxed)
}

/// Member `m` sees `all` rows in `t`, each once.
async fn each_once(c: &Cluster, m: usize, all: usize) {
    let a = c.sql(m, "SELECT count(*) AS n, count(DISTINCT g * 1000 + s * 10 + i) AS d FROM t").await.unwrap();
    let (n, d) = (rows(&a)[0]["n"].as_i64(), rows(&a)[0]["d"].as_i64());
    if (n, d) != (Some(all as i64), Some(all as i64)) {
        let each = c.sql(m, "SELECT g, s, count(*) AS n FROM t GROUP BY g, s ORDER BY g, s").await.unwrap();
        tokio::time::sleep(Duration::from_secs(30)).await;
        let later = c.sql(m, "SELECT count(*) AS n FROM t").await.unwrap();
        panic!("seed {}: rows {n:?}, distinct {d:?}, not {all}; 30 s later {later}\neach statement's: {each}\n{}", c.seed, c.met());
    }
}

/// The harness's `serverless`: six guests (`pondra sql`, with no node running) write at once into a lake nobody serves,
/// through a bucket whose calls are slow and now and then fail or lose their answer. Each takes the lake for the moment
/// or waits for whoever has it. Then a member starts on the lake: every row is there once.
#[tokio::test(start_paused = true)]
async fn guests_with_no_node_running_write_each_statement_once() {
    for seed in seeds() {
        let c = Cluster::new(seed, 1);
        c.guest(0, "CREATE TABLE t (g INT, s INT, i INT)", "made").await.unwrap();
        (0..6).for_each(|g| c.guest_faults(g).everywhere(Odds { fail: 0.02, lost: 0.02, slow: Duration::from_millis(50) }));
        let again = guests_write(&c, 6, 5, 10).await;
        c.start(0).await.unwrap();
        each_once(&c, 0, 300).await;
        println!("seed {seed}: 300 rows from 6 guests with no node running, each once ({again} statements run again)");
    }
}

/// A guest stopped at a crash point (its process killed after a segment is put, after its commit, or after a file is
/// tiered) runs again with its job, as `pondra sql` does: three guests with no node running, each statement once.
#[tokio::test(start_paused = true)]
async fn guests_stopped_at_crash_points_write_each_statement_once() {
    for seed in seeds() {
        let c = Cluster::new(seed, 1);
        c.guest(0, "CREATE TABLE t (g INT, s INT, i INT)", "made").await.unwrap();
        for g in 0..3 {
            POINTS.iter().for_each(|(point, _)| c.guest_faults(g).stop_at(point, 0.1));
        }
        let again = guests_write(&c, 3, 8, 10).await;
        let stopped = (0..3).flat_map(|g| c.guest_faults(g).met()).filter(|m| m.starts_with("stopped")).count();
        c.start(0).await.unwrap();
        each_once(&c, 0, 240).await;
        println!("seed {seed}: 240 rows from 3 guests stopped {stopped} times at crash points, each once ({again} statements run again)");
    }
}

/// A guest cut off from a live leader (it beats, but doesn't answer from here) writes through the bucket's inbox, where
/// the leader records it; a guest that reaches the leader writes through it. Three of each at once, through a bucket
/// that now and then fails a call or loses its answer: each statement once.
#[tokio::test(start_paused = true)]
async fn guests_cut_off_from_the_leader_write_through_its_inbox_each_statement_once() {
    for seed in seeds() {
        let c = Cluster::new(seed, 1);
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE t (g INT, s INT, i INT)").await.unwrap();
        for g in 0..6 {
            c.guest_faults(g).everywhere(Odds { fail: 0.01, lost: 0.01, slow: Duration::from_millis(50) });
            if g < 3 {
                c.net.cut(&format!("g{g}"), c.name(0), true);
            }
        }
        let again = guests_write(&c, 6, 5, 10).await;
        each_once(&c, 0, 300).await;
        println!("seed {seed}: 300 rows from 6 guests, 3 through the leader's inbox, each once ({again} statements run again)");
    }
}

/// The 11-03 audit's M9: right after the leader moves a table's columns, an INSERT by position goes to a follower that
/// may not have seen the move yet (the third of three, whose calls are slow). The follower fills the rows itself and the
/// leader records them: they are refused and sent again, or land in the order the move made, never in the old one,
/// where `a` would silently take `b`'s value.
#[tokio::test(start_paused = true)]
async fn an_insert_by_position_right_after_a_column_move_keeps_the_new_order() {
    let rounds = 40i64;
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE t (a BIGINT, b BIGINT)").await.unwrap();
        c.start(1).await.unwrap();
        c.start(2).await.unwrap();
        c.until("the third member hears its leader", Duration::from_secs(10), || async { c.known(2).await == 3 }).await.unwrap();
        c.faults(2).everywhere(Odds { fail: 0.0, lost: 0.0, slow: Duration::from_millis(500) });
        let mut refused = 0;
        // (round k: an odd one puts b first, an even one a; then k goes in the first column, -k in the second)
        for k in 1..=rounds {
            let first = if k % 2 == 1 { "b" } else { "a" };
            c.sql(0, &format!("ALTER TABLE t ALTER COLUMN {first} FIRST")).await.unwrap();
            while let Err(e) = c.sql(2, &format!("INSERT INTO t VALUES ({k}, {})", -k)).await {
                refused += 1;
                assert!(refused < 1000, "seed {seed}: round {k}'s INSERT never went: {e:#}\n{}", c.met());
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let a = c.sql(0, "SELECT a, b FROM t").await.unwrap();
        let mut seen = std::collections::BTreeSet::new();
        for r in rows(&a) {
            let (x, y) = (r["a"].as_i64().unwrap_or(0), r["b"].as_i64().unwrap_or(0));
            let k = x.max(y);
            let (want_a, want_b) = if k % 2 == 1 { (-k, k) } else { (k, -k) };
            assert_eq!((x, y), (want_a, want_b), "seed {seed}: round {k}'s row is in the wrong order\n{}", c.met());
            assert!(seen.insert(k), "seed {seed}: round {k}'s row is there twice");
        }
        assert_eq!(seen.len() as i64, rounds, "seed {seed}: rows");
        println!("seed {seed}: {rounds} INSERTs by position right after a column move, each in the new order ({refused} refused first)");
    }
}

/// How many rows member `m` sees with `seq = k`, or none while it can't say.
async fn seen_with(c: &Cluster, m: usize, k: i64) -> Option<i64> {
    let a = c.sql(m, &format!("SELECT count(*) AS n FROM events WHERE seq = {k}")).await.ok()?;
    rows(&a).first()?["n"].as_i64()
}

/// Row `k` of producer `r`, inserted on member `m` until it is taken (sent again after 100 ms, as a leader changing asks).
async fn insert_round(c: &Cluster, m: usize, k: i64) {
    let mut tries = 0;
    while let Err(e) = c.sql(m, &format!("INSERT INTO events VALUES ('r', {k})")).await {
        tries += 1;
        assert!(tries < 50, "seed {}: row {k} on member {m} was never taken: {e}\n{}", c.seed, c.met());
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// A reader (a member that never leads, runs no tasks and takes no writes) sees each write within 10 s: twenty rows
/// written to the leader, then the leader is killed, and twenty more written to the new leader, each seen on the reader
/// as it lands. The reader then counts exactly the forty rows.
#[tokio::test(start_paused = true)]
async fn a_reader_sees_each_write_soon_also_after_a_failover() {
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE events (producer TEXT, seq BIGINT)").await.unwrap();
        c.start(1).await.unwrap();
        c.set_start(|s| s.reader = true);
        c.start(2).await.unwrap();
        c.set_start(|s| s.reader = false);
        assert_eq!(c.role(2).await.as_deref(), Some("reader"), "seed {seed}\n{}", c.met());
        let (mut before, mut after) = (Duration::ZERO, Duration::ZERO);
        for k in 1..=20 {
            insert_round(&c, 0, k).await;
            let waited = c.until(&format!("the reader sees row {k}"), Duration::from_secs(10), || async { seen_with(&c, 2, k).await == Some(1) }).await.unwrap();
            before = before.max(waited);
        }
        c.until("the second member hears its leader", Duration::from_secs(30), || async { c.known(1).await == 2 }).await.unwrap(); // (a reader beats to no one: the leader knows its two members)
        c.kill(0);
        let took = c.until("the second member leads", Duration::from_secs(30), || async { c.role(1).await.as_deref() == Some("leader") }).await.unwrap();
        for k in 21..=40 {
            insert_round(&c, 1, k).await;
            let waited = c.until(&format!("the reader sees row {k}"), Duration::from_secs(10), || async { seen_with(&c, 2, k).await == Some(1) }).await.unwrap();
            after = after.max(waited);
        }
        let n = count(&c, 2, "events").await;
        assert_eq!(n, Some(40), "seed {seed}: the reader's rows\n{}", c.met());
        println!(
            "seed {seed}: 40 rows, each seen on the reader within 10 s: the longest wait {} ms before the failover, {} ms after (the second member led {:.1} s after the kill)",
            before.as_millis(),
            after.as_millis(),
            took.as_secs_f64()
        );
    }
}

/// The harness's `loads`, its COPY part: two members load one folder's files at once, each file once. Then six more
/// files are copied with the first member due to be stopped after a commit, and each copy goes on until a leader has
/// it: whoever leads holds all 90 rows once, and the sum of the amounts is ten times the sum of the ids.
#[tokio::test(start_paused = true)]
async fn copy_into_from_two_members_and_through_a_kill_loads_each_file_once() {
    for seed in seeds() {
        let dir = std::env::temp_dir().join(format!("pondra-copy-{}", uuid::Uuid::new_v4().simple()));
        std::fs::create_dir_all(&dir).unwrap();
        let write = |name: &str, first: i64| {
            let text: String = std::iter::once("id,amount\n".to_string()).chain((first..first + 5).map(|id| format!("{id},{}\n", id * 10))).collect();
            std::fs::write(dir.join(name), text).unwrap();
        };
        (0..12).for_each(|f| write(&format!("f{f:02}.csv"), f * 5));
        let copy = format!("COPY INTO t FROM '{}/*.csv' (FORMAT csv, HEADER true)", dir.display());
        let c = Cluster::new(seed, 2);
        c.owned(); // (a path on the members' own disk is their owner's to read, as the harness's `owner_key` reads it)
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE t (id BIGINT, amount BIGINT)").await.unwrap();
        c.start(1).await.unwrap();
        let (a, b) = futures::join!(c.sql(0, &copy), c.sql(1, &copy));
        assert!(a.is_ok() && b.is_ok(), "seed {seed}: the COPYs at once: {a:?} and {b:?}\n{}", c.met());
        (0..6).for_each(|g| write(&format!("g{g:02}.csv"), 60 + g * 5));
        c.faults(0).stop_at("after_commit", 0.5);
        let _ = c.sql(0, &copy).await; // (its answer is not waited for: it may be stopped first)
        let stopped = !c.up(0);
        if stopped {
            c.until("the second member leads", Duration::from_secs(30), || async { c.role(1).await.as_deref() == Some("leader") }).await.unwrap();
            let mut tries = 0;
            while let Err(e) = c.sql(1, &copy).await {
                tries += 1;
                assert!(tries < 20, "seed {seed}: the COPY to the second member was never taken: {e}\n{}", c.met());
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
        } else {
            c.faults(0).stop_at("after_commit", 0.0); // (the copy again must be answered: its member is not to stop now)
            let again = c.sql(0, &copy).await;
            assert!(again.is_ok(), "seed {seed}: the COPY to the first member again: {again:?}\n{}", c.met());
        }
        let mut lead = None;
        for m in 0..2 {
            if c.role(m).await.as_deref() == Some("leader") {
                lead = Some(m);
            }
        }
        let lead = lead.unwrap_or_else(|| panic!("seed {seed}: no member leads\n{}", c.met()));
        let total = c.sql(lead, "SELECT count(*) AS n, count(DISTINCT id) AS d, sum(amount) AS s FROM t").await;
        std::fs::remove_dir_all(&dir).ok();
        let total = total.unwrap();
        assert_eq!(rows(&total).first(), Some(&serde_json::json!({"n": 90, "d": 90, "s": 40050})), "seed {seed}: the rows of t on member {lead}\n{}", c.met());
        println!(
            "seed {seed}: member 1 {} mid-way; 90 rows, 90 distinct, sum 40050, read on member {} (the leader)",
            if stopped { "was stopped" } else { "was not stopped" },
            lead + 1
        );
    }
}

/// What member `m` answers `sql` with in session `session`: its JSON, or its status, SQLSTATE and words.
async fn in_session(c: &Cluster, m: usize, session: &str, sql: &str) -> serde_json::Value {
    match c.ask_with(m, "POST", "/sql", &[("x-pondra-session".to_string(), session.to_string())], sql.as_bytes().to_vec()).await {
        Ok((200..=299, _, body)) => serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null),
        Ok((status, code, body)) => serde_json::json!({"status": status, "code": code, "body": String::from_utf8_lossy(&body)}),
        Err(e) => serde_json::json!({"no answer": e.to_string()}),
    }
}

/// The harness's `waits` through a follower, on paused time: a row lock held from m2 makes a waiter on m3 time out
/// (55P03, then 25P02 until it ends), keeps through the holder's idle minutes (its follower renews it), and goes to a
/// waiter within a lease once the holder's follower is killed.
#[tokio::test(start_paused = true)]
async fn row_locks_through_a_follower_wait_time_out_and_go_with_a_killed_follower() {
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE held (id BIGINT, v BIGINT)").await.unwrap();
        c.sql(0, "INSERT INTO held VALUES (1, 10)").await.unwrap();
        c.start(1).await.unwrap();
        c.start(2).await.unwrap();
        c.until("every member knows the three", Duration::from_secs(30), || async { c.known(1).await == 3 && c.known(2).await == 3 }).await.unwrap();
        let upd = "UPDATE held SET v = v + 1 WHERE id = 1";
        for (m, session, sql, want) in [
            (1, "holder-01", "BEGIN", "ok"),
            (1, "holder-01", upd, "ok"),
            (2, "waiter-01", "SET lock_timeout = '200ms'", "ok"),
            (2, "waiter-01", "BEGIN", "ok"),
            (2, "waiter-01", upd, "55P03"),
            (2, "waiter-01", "SELECT 1", "25P02"),
            (2, "waiter-01", "ROLLBACK", "ok"),
        ] {
            let got = c.code_in(m, session, sql).await;
            assert_eq!(got, want, "seed {seed}: session {session} on member {} ran {sql:?}\n{}", m + 1, c.met());
        }
        tokio::time::sleep(Duration::from_secs(180)).await; // (the holder idles: its follower renews its lock)
        assert_eq!(c.code_in(0, "waiter-02", "SET lock_timeout = '1s'").await, "ok", "seed {seed}\n{}", c.met());
        assert_eq!(c.code_in(0, "waiter-02", "BEGIN").await, "ok", "seed {seed}\n{}", c.met());
        let idle = c.code_in(0, "waiter-02", upd).await;
        assert_eq!(idle, "55P03", "seed {seed}: the idle holder keeps its row, asked on member 1\n{}", c.met());
        assert_eq!(c.code_in(0, "waiter-02", "ROLLBACK").await, "ok", "seed {seed}\n{}", c.met());
        c.kill(1);
        let killed = crate::clock::now();
        // (the lock goes once its lease, 60 s, passes with no word from the holder's member; the test's ask waits 30 s at
        // most, as the harness's call does, so the waiter asks 40 s after the kill, and waits the rest)
        tokio::time::sleep(Duration::from_secs(40)).await;
        assert_eq!(c.code_in(2, "waiter-03", "SET lock_timeout = '120s'").await, "ok", "seed {seed}\n{}", c.met());
        assert_eq!(c.code_in(2, "waiter-03", "BEGIN").await, "ok", "seed {seed}\n{}", c.met());
        let got = c.code_in(2, "waiter-03", upd).await;
        let waited = killed.elapsed();
        assert_eq!(got, "ok", "seed {seed}: the waiter's UPDATE after the kill\n{}", c.met());
        assert!(waited < Duration::from_secs(90), "seed {seed}: the waiter got the row {waited:?} after the kill, a lease being 60 s\n{}", c.met());
        let inside = in_session(&c, 2, "waiter-03", "SELECT v FROM held WHERE id = 1").await;
        assert_eq!(rows(&inside), vec![serde_json::json!({"v": 11})], "seed {seed}: v as the waiter's transaction sees it: {inside}\n{}", c.met());
        let done = in_session(&c, 2, "waiter-03", "COMMIT").await;
        assert_eq!(done["transaction"], "commit", "seed {seed}: the waiter's COMMIT answered {done}\n{}", c.met());
        let v = c.sql(0, "SELECT v FROM held WHERE id = 1").await.unwrap();
        assert_eq!(rows(&v), vec![serde_json::json!({"v": 11})], "seed {seed}: v on member 1 after the waiter's COMMIT (10, and the waiter's 1; the killed holder's never lands)\n{}", c.met());
        println!(
            "seed {seed}: 55P03 and 25P02 through member 3, 55P03 through member 1 with the holder idle 180 s; the waiter got the row {:.1} s after the kill; v 11",
            waited.as_secs_f64()
        );
    }
}

/// The harness's `tasks` through a leader stop, on paused time, three members: a graph started from a follower runs to
/// its end although the member that leads may be stopped at a commit of its run, and the leader that is left has each
/// write once, and each run ok.
#[tokio::test(start_paused = true)]
async fn a_task_graph_through_a_leader_stop_writes_each_row_once() {
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        c.start(0).await.unwrap();
        c.start(1).await.unwrap();
        c.start(2).await.unwrap();
        c.until("every member knows the three", Duration::from_secs(30), || async { c.known(1).await == 3 && c.known(2).await == 3 }).await.unwrap();
        c.sql(0, "CREATE TABLE flog (task VARCHAR, v BIGINT)").await.unwrap();
        c.sql(0, "CREATE TASK froot SCHEDULE '1 hour' AS SELECT 1").await.unwrap();
        c.sql(0, "CREATE TASK fload AFTER froot AS BEGIN INSERT INTO flog VALUES ('fload', 1); RETURN 7; END").await.unwrap();
        c.sql(0, "CREATE TASK freport AFTER fload AS INSERT INTO flog VALUES ('freport', pondra.result('fload'))").await.unwrap();
        let started = c.sql(1, "EXECUTE TASK froot").await;
        assert!(started.is_ok(), "seed {seed}: EXECUTE TASK froot on member 2 was refused: {started:?}\n{}", c.met());
        c.faults(0).stop_at("after_commit", 0.5); // (each commit member 1 makes while the graph runs may stop it there)
        let found = std::cell::Cell::new(None);
        c.until("a member that is up leads and has freport ok", Duration::from_secs(120), || async {
            for m in 0..3 {
                if !c.up(m) || c.role(m).await.as_deref() != Some("leader") {
                    continue;
                }
                let answer = c.sql(m, "SELECT status FROM pondra.runs WHERE routine = 'freport'").await.unwrap_or(serde_json::Value::Null);
                if rows(&answer).first().is_some_and(|r| r["status"] == "ok") {
                    found.set(Some(m));
                    return true;
                }
            }
            false
        })
        .await
        .unwrap();
        let lead = found.get().unwrap_or_else(|| panic!("seed {seed}: no leader found\n{}", c.met()));
        let flog = c.sql(lead, "SELECT task, v FROM flog ORDER BY task").await.unwrap();
        assert_eq!(flog, serde_json::json!([{"task": "fload", "v": 1}, {"task": "freport", "v": 7}]), "seed {seed}: flog on member {}\n{}", lead + 1, c.met());
        let runs = c.sql(lead, "SELECT routine, status FROM pondra.runs WHERE routine IN ('froot', 'fload', 'freport') ORDER BY routine").await.unwrap();
        let statuses = serde_json::Value::Array(rows(&runs).iter().map(|r| r["status"].clone()).collect());
        assert_eq!(statuses, serde_json::json!(["ok", "ok", "ok"]), "seed {seed}: the runs of the graph on member {}\n{}", lead + 1, c.met());
        println!("seed {seed}: member 1 {} stopped; member {} led at the end; flog {flog}; runs {statuses}", if c.up(0) { "was not" } else { "was" }, lead + 1);
    }
}

/// A round trip to a far bucket (`Faults::far`): 50 ms, as `tools/faulty_s3.py`'s `slow_ms` sets it to measure one.
const TRIP: Duration = Duration::from_millis(50);

/// How many bucket calls one after another `f` waited on: its time on the sim's clock in round trips of `TRIP`, rounded
/// up. Calls made side by side count once.
/// The calls themselves, each with when it went, are what `faults` (the far one's) shows from then on (`Faults::trips`).
async fn chain<T>(faults: &Faults, f: impl std::future::Future<Output = T>) -> (u128, Vec<String>, T) {
    let began = crate::clock::now();
    let out = f.await;
    (began.elapsed().as_millis().div_ceil(TRIP.as_millis()), faults.trips(began), out)
}

/// Review pick O1 (11-27, with `li1030/A4`): with the bucket 50 ms away, a cold `pondra sql` SELECT, a node's start on a
/// lake that's there, and the node's first and second SELECT each wait on only so many bucket calls one after another.
/// A change that makes one of these chains longer fails here: each call more is 30 to 100 ms more on R2.
#[tokio::test(start_paused = true)]
async fn a_cold_open_waits_on_few_bucket_calls_one_after_another() {
    // (the most each may wait on: the chains measured on 11-28; a change that shortens one lowers its cap here)
    // (`PONDRA_SIM_TRIPS=1` shows each chain's calls, each with when it went)
    const CAPS: [(&str, u128); 4] = [("a cold pondra sql SELECT", 30), ("a node's start", 19), ("its first SELECT", 0), ("its second SELECT", 0)];
    for seed in seeds() {
        let c = Cluster::new(seed, 1);
        c.guest(0, "CREATE TABLE t (id BIGINT)", "made").await.unwrap();
        c.guest(0, "INSERT INTO t VALUES (1), (2), (3)", "rows").await.unwrap();
        c.guest_faults(1).far(TRIP);
        c.faults(0).far(TRIP);
        let select = "SELECT count(*) AS n FROM t";
        let three = |a: &serde_json::Value| rows(a).first().and_then(|r| r["n"].as_i64()) == Some(3);
        let (guest, guest_calls, answer) = chain(&c.guest_faults(1), c.guest(1, select, "read")).await;
        let answer = answer.unwrap();
        assert!(three(&answer), "seed {seed}: the cold pondra sql SELECT answered {answer}\n{}", c.met());
        let (start, start_calls, started) = chain(c.faults(0), c.start(0)).await;
        started.unwrap();
        let (first, first_calls, a) = chain(c.faults(0), c.sql(0, select)).await;
        let (second, second_calls, b) = chain(c.faults(0), c.sql(0, select)).await;
        let (a, b) = (a.unwrap(), b.unwrap());
        assert!(three(&a) && three(&b), "seed {seed}: the node's SELECTs answered {a} and {b}\n{}", c.met());
        let chains = [(guest, guest_calls), (start, start_calls), (first, first_calls), (second, second_calls)];
        for ((what, cap), (n, calls)) in CAPS.iter().zip(&chains) {
            if crate::env::flag("PONDRA_SIM_TRIPS") {
                println!("seed {seed}: {what}: {n} bucket calls one after another, of these:\n{}", calls.join("\n"));
            }
            assert!(n <= cap, "seed {seed}: {what} waited on {n} bucket calls one after another, more than its {cap}; the calls, each with when it went:\n{}\n{}", calls.join("\n"), c.met());
        }
        println!("seed {seed}: bucket calls one after another, 50 ms each: a cold pondra sql SELECT {guest}, a node's start {start}, its first SELECT {first}, its second {second}");
    }
}

/// A statement sent again in its transaction while its first send still waits for a lock: the holder's member is killed,
/// a waiter's UPDATE through a follower gets no answer in 30 s (its client gives up, and the statement waits on), and the
/// same UPDATE sent again takes over the wait. The first one's wait fails the transaction, so the second is refused
/// (25P02) once the row is free, COMMIT rolls back, and the row keeps its value: no write lost or doubled.
#[tokio::test(start_paused = true)]
async fn a_statement_sent_again_in_its_transaction_fails_it_and_writes_nothing() {
    for seed in seeds() {
        let c = Cluster::new(seed, 3);
        c.start(0).await.unwrap();
        c.sql(0, "CREATE TABLE held (id BIGINT, v BIGINT)").await.unwrap();
        c.sql(0, "INSERT INTO held VALUES (1, 10)").await.unwrap();
        c.start(1).await.unwrap();
        c.start(2).await.unwrap();
        c.until("every member knows the three", Duration::from_secs(30), || async { c.known(1).await == 3 && c.known(2).await == 3 }).await.unwrap();
        let upd = "UPDATE held SET v = v + 1 WHERE id = 1";
        for (m, session, sql) in [(1, "holder-01", "BEGIN"), (1, "holder-01", upd)] {
            assert_eq!(c.code_in(m, session, sql).await, "ok", "seed {seed}: session {session} ran {sql:?}\n{}", c.met());
        }
        c.kill(1);
        for sql in ["SET lock_timeout = '120s'", "BEGIN"] {
            assert_eq!(c.code_in(2, "waiter-03", sql).await, "ok", "seed {seed}: the waiter ran {sql:?}\n{}", c.met());
        }
        let first = c.code_in(2, "waiter-03", upd).await;
        assert_eq!(first, "no answer", "seed {seed}: the waiter's first UPDATE, while the lock's lease lasts\n{}", c.met());
        tokio::time::sleep(Duration::from_millis(150)).await;
        let again = in_session(&c, 2, "waiter-03", upd).await;
        assert_eq!(again["code"], "25P02", "seed {seed}: the UPDATE sent again answered {again}\n{}", c.met());
        let done = in_session(&c, 2, "waiter-03", "COMMIT").await;
        assert_eq!(done["transaction"], "rollback", "seed {seed}: the waiter's COMMIT answered {done}\n{}", c.met());
        let v = c.sql(0, "SELECT v FROM held WHERE id = 1").await.unwrap();
        assert_eq!(rows(&v), vec![serde_json::json!({"v": 10})], "seed {seed}: v on member 1 after the rollback\n{}", c.met());
        println!("seed {seed}: the UPDATE sent again after 30 s with no answer was refused 25P02, COMMIT rolled back, v 10");
    }
}
