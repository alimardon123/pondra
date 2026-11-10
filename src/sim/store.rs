//! A sim cluster's bucket (redesign step 3, piece 3d: l7-wip/step3-sim-plan.md). Its members' lake is a bucket's, as on a
//! cloud: `sim://<cluster>-<member>/<lake>`, one bucket name for each member, so that each member reaches the one bucket
//! through its own faults (`Faulty`) and a member can be cut off from it while the others aren't. Every member's name
//! opens the same objects (`Bucket`), each stamped by the sim's clock: a mark's age (`terms::alive`) is then measured on
//! the time the sim's sleeps keep, where object_store's memory stamps it with the machine's.
use super::faults::{Faults, Op, Verdict};
use crate::buckets::{At, Store};
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetResult, ListResult, ObjectMeta, ObjectStore, ObjectStoreExt, PutOptions, PutPayload, PutResult};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

/// The members' buckets in this process, by name: what `sim://name/…` opens (`open`).
static BUCKETS: Mutex<Option<HashMap<String, Store>>> = Mutex::new(None);

/// A sim cluster's one bucket: objects in memory, each stamped by the sim's clock.
pub fn bucket() -> Arc<Bucket> { Arc::new(Bucket { objects: object_store::memory::InMemory::new(), stamps: Default::default() }) }

/// `sim://name` opens `bucket` as `faults` meet its calls, from now on.
pub fn give(name: &str, bucket: Arc<Bucket>, faults: Arc<Faults>) {
    let store: Store = Arc::new(Faulty { bucket, faults });
    lock(&BUCKETS).get_or_insert_with(HashMap::new).insert(name.to_string(), store);
}

/// `sim://name` opens nothing from now on.
pub fn forget(name: &str) {
    if let Some(b) = lock(&BUCKETS).as_mut() {
        b.remove(name);
    }
}

/// The store `sim://name` opens (`hosted::bucket`'s, for the scheme `sim`).
pub(crate) fn open(at: &At) -> anyhow::Result<Store> {
    lock(&BUCKETS).as_ref().and_then(|b| b.get(at.bucket).cloned()).ok_or_else(|| anyhow::anyhow!("{}: no sim bucket has this name (`sim::store::give`)", at.root))
}

/// A sim cluster's bucket: its objects, and when each was last written on the sim's clock.
#[derive(Debug)]
pub struct Bucket {
    objects: object_store::memory::InMemory,
    stamps: Mutex<HashMap<Path, chrono::DateTime<chrono::Utc>>>,
}

impl Bucket {
    /// `meta` as the sim's clock stamped it.
    fn stamped(&self, mut meta: ObjectMeta) -> ObjectMeta {
        if let Some(at) = lock(&self.stamps).get(&meta.location) {
            meta.last_modified = *at;
        }
        meta
    }

    /// `at` was written now.
    fn stamp(&self, at: &Path) {
        let now = chrono::DateTime::from_timestamp_micros(crate::clock::wall_us() as i64).unwrap_or_default();
        lock(&self.stamps).insert(at.clone(), now);
    }
}

/// One member's view of the bucket: each call meets the member's faults first.
#[derive(Debug)]
struct Faulty {
    bucket: Arc<Bucket>,
    faults: Arc<Faults>,
}

impl std::fmt::Display for Faulty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str("sim bucket") }
}

/// The error a call that failed, or lost its answer, gives: what a bucket's timeout or 500 gives.
fn failed(op: Op, at: &(dyn std::fmt::Display + Sync), verdict: Verdict) -> object_store::Error {
    object_store::Error::Generic { store: "sim", source: format!("{op:?} {at}: {verdict:?} (the sim's faults)").into() }
}

impl Faulty {
    /// Runs `call` as the member's faults say: not at all (it fails), or done and its answer lost, or as it comes.
    async fn run<T>(&self, op: Op, at: &(dyn std::fmt::Display + Sync), call: impl std::future::Future<Output = object_store::Result<T>>) -> object_store::Result<T> {
        match self.faults.meet(op, at).await {
            Verdict::Fail => Err(failed(op, at, Verdict::Fail)),
            Verdict::Lose => {
                let _ = call.await;
                Err(failed(op, at, Verdict::Lose))
            }
            Verdict::Go => call.await,
        }
    }
}

#[async_trait::async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(&self, at: &Path, body: PutPayload, opts: PutOptions) -> object_store::Result<PutResult> {
        let op = if matches!(opts.mode, object_store::PutMode::Create) { Op::Create } else { Op::Put };
        self.run(op, at, async {
            let r = self.bucket.objects.put_opts(at, body, opts).await?;
            self.bucket.stamp(at);
            Ok(r)
        })
        .await
    }

    async fn put_multipart_opts(&self, at: &Path, opts: object_store::PutMultipartOptions) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        let upload = self.run(Op::Put, at, self.bucket.objects.put_multipart_opts(at, opts)).await?;
        Ok(Box::new(Parts { upload, at: at.clone(), bucket: self.bucket.clone(), faults: self.faults.clone() }))
    }

    async fn get_opts(&self, at: &Path, opts: GetOptions) -> object_store::Result<GetResult> {
        let mut r = self.run(Op::Get, at, self.bucket.objects.get_opts(at, opts)).await?;
        r.meta = self.bucket.stamped(r.meta);
        Ok(r)
    }

    fn delete_stream(&self, paths: BoxStream<'static, object_store::Result<Path>>) -> BoxStream<'static, object_store::Result<Path>> {
        let (bucket, faults) = (self.bucket.clone(), self.faults.clone());
        paths
            .then(move |p| {
                let (bucket, faults) = (bucket.clone(), faults.clone());
                async move {
                    let p = p?;
                    let me = Faulty { bucket: bucket.clone(), faults };
                    me.run(Op::Delete, &p, async {
                        bucket.objects.delete(&p).await?;
                        lock(&bucket.stamps).remove(&p);
                        Ok(p.clone())
                    })
                    .await
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> { self.listed(prefix, self.bucket.objects.list(prefix)) }

    fn list_with_offset(&self, prefix: Option<&Path>, offset: &Path) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.listed(prefix, self.bucket.objects.list_with_offset(prefix, offset))
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let shown = prefix.map(Path::to_string).unwrap_or_default();
        let mut r = self.run(Op::List, &shown, self.bucket.objects.list_with_delimiter(prefix)).await?;
        r.objects = r.objects.into_iter().map(|m| self.bucket.stamped(m)).collect();
        Ok(r)
    }

    async fn copy_opts(&self, from: &Path, to: &Path, opts: object_store::CopyOptions) -> object_store::Result<()> {
        let op = if matches!(opts.mode, object_store::CopyMode::Create) { Op::Create } else { Op::Put };
        self.run(op, to, async {
            self.bucket.objects.copy_opts(from, to, opts).await?;
            self.bucket.stamp(to);
            Ok(())
        })
        .await
    }
}

impl Faulty {
    /// A listing: it meets the member's faults once, before its first page, and each object in it is stamped.
    fn listed(&self, prefix: Option<&Path>, listing: BoxStream<'static, object_store::Result<ObjectMeta>>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let (bucket, faults, shown) = (self.bucket.clone(), self.faults.clone(), prefix.map(Path::to_string).unwrap_or_default());
        futures::stream::once(async move {
            match faults.meet(Op::List, &shown).await {
                Verdict::Go => listing.map(move |m| m.map(|m| bucket.stamped(m))).boxed(),
                v => futures::stream::once(async move { Err(failed(Op::List, &shown, v)) }).boxed(),
            }
        })
        .flatten()
        .boxed()
    }
}

/// An upload's parts and its end, each a call the member's faults meet.
#[derive(Debug)]
struct Parts {
    upload: Box<dyn object_store::MultipartUpload>,
    at: Path,
    bucket: Arc<Bucket>,
    faults: Arc<Faults>,
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for Parts {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let (part, faults, at) = (self.upload.put_part(data), self.faults.clone(), self.at.clone());
        Box::pin(async move {
            match faults.meet(Op::Put, &at).await {
                Verdict::Go => part.await,
                Verdict::Lose => {
                    let _ = part.await;
                    Err(failed(Op::Put, &at, Verdict::Lose))
                }
                Verdict::Fail => Err(failed(Op::Put, &at, Verdict::Fail)),
            }
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let (faults, at, bucket) = (self.faults.clone(), self.at.clone(), self.bucket.clone());
        let me = Faulty { bucket: bucket.clone(), faults };
        let upload = &mut self.upload;
        me.run(Op::Put, &at, async {
            let r = upload.complete().await?;
            bucket.stamp(&at);
            Ok(r)
        })
        .await
    }

    async fn abort(&mut self) -> object_store::Result<()> { self.upload.abort().await }
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> { m.lock().unwrap_or_else(PoisonError::into_inner) }

#[cfg(test)]
mod tests {
    use super::super::faults::Odds;
    use super::*;
    use std::time::Duration;

    /// Two members open one bucket by their own names: what one writes the other reads, stamped on the sim's clock; one
    /// cut off fails while the other doesn't; a lost answer's write is there all the same.
    #[tokio::test(start_paused = true)]
    async fn members_share_one_bucket_each_through_its_own_faults() {
        let (b, one, two) = (bucket(), Faults::new(1), Faults::new(2));
        let name = format!("t{}", uuid::Uuid::new_v4().simple());
        give(&format!("{name}-m1"), b.clone(), one.clone());
        give(&format!("{name}-m2"), b.clone(), two.clone());
        let m1 = crate::store::open_store(&format!("sim://{name}-m1/lake"), None).unwrap().1;
        let m2 = crate::store::open_store(&format!("sim://{name}-m2/lake"), None).unwrap().1;
        let p = Path::from("cluster/alive/1");
        m1.put(&p, "x".into()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(90)).await;
        let meta = m2.head(&p).await.unwrap();
        let age = crate::clock::wall_ms() as i64 - meta.last_modified.timestamp_millis();
        assert!((89_900..95_000).contains(&age), "stamped on the sim's clock: {age} ms old"); // (the machine's two clocks drift a little apart)

        one.cut(true);
        assert!(m1.head(&p).await.is_err() && m2.head(&p).await.is_ok(), "the cut member alone fails");
        one.cut(false);
        one.set(Op::Put, Odds { lost: 1.0, ..Odds::default() });
        let q = Path::from("t/q");
        assert!(m1.put(&q, "y".into()).await.is_err(), "its answer lost");
        assert!(m2.get(&q).await.is_ok(), "and it was written");
        let listed: Vec<_> = m2.list(Some(&Path::from("cluster"))).collect().await;
        assert_eq!(listed.len(), 1);
        forget(&format!("{name}-m1"));
        forget(&format!("{name}-m2"));
        assert!(crate::store::open_store(&format!("sim://{name}-m1/lake"), None).is_err(), "forgotten: opens nothing");
    }
}
