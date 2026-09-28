//! A store of `object_store` 0.14 (Pondra's own client, with S3, GCS, Azure and HTTP) offered to
//! DataFusion, which reads and writes through 0.13's trait: files outside the lake (ADR-026),
//! read and written by DataFusion as they are, uncached (unlike the lake's, they may change).
use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt, TryStreamExt};
use futures::FutureExt;
use object_store_df as df;
use std::sync::Arc;

#[derive(Debug)]
pub struct Bridge(pub Arc<dyn object_store::ObjectStore>);

impl std::fmt::Display for Bridge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { write!(f, "Bridge({})", self.0) }
}

// A path's text is already escaped (`a%2Fb` names the object `a%2Fb`): parsed as it is, never
// escaped again (`Path::from` would make it `a%252Fb`, another object).
fn path(p: &df::path::Path) -> object_store::path::Path { object_store::path::Path::parse(p.as_ref()).unwrap_or_else(|_| object_store::path::Path::from(p.as_ref())) }

fn df_path(p: &object_store::path::Path) -> df::path::Path { df::path::Path::parse(p.as_ref()).unwrap_or_else(|_| df::path::Path::from(p.as_ref())) }

fn err(e: object_store::Error) -> df::Error {
    use object_store::Error as E;
    match e {
        E::NotFound { path, source } => df::Error::NotFound { path, source },
        E::AlreadyExists { path, source } => df::Error::AlreadyExists { path, source },
        E::Precondition { path, source } => df::Error::Precondition { path, source },
        E::NotModified { path, source } => df::Error::NotModified { path, source },
        e => df::Error::Generic { store: "outside the lake", source: Box::new(e) },
    }
}

fn meta(m: object_store::ObjectMeta) -> df::ObjectMeta {
    df::ObjectMeta { location: df_path(&m.location), last_modified: m.last_modified, size: m.size, e_tag: m.e_tag, version: m.version }
}

fn payload(p: df::PutPayload) -> object_store::PutPayload { p.into_iter().collect::<Vec<Bytes>>().into_iter().collect() }

fn put_result(r: object_store::PutResult) -> df::PutResult { df::PutResult { e_tag: r.e_tag, version: r.version } }

#[derive(Debug)]
struct Upload(Box<dyn object_store::MultipartUpload>);

#[async_trait::async_trait]
impl df::MultipartUpload for Upload {
    fn put_part(&mut self, data: df::PutPayload) -> df::UploadPart { self.0.put_part(payload(data)).map(|r| r.map_err(err)).boxed() }
    async fn complete(&mut self) -> df::Result<df::PutResult> { self.0.complete().await.map(put_result).map_err(err) }
    async fn abort(&mut self) -> df::Result<()> { self.0.abort().await.map_err(err) }
}

#[async_trait::async_trait]
impl df::ObjectStore for Bridge {
    async fn put_opts(&self, at: &df::path::Path, data: df::PutPayload, o: df::PutOptions) -> df::Result<df::PutResult> {
        let mode = match o.mode {
            df::PutMode::Create => object_store::PutMode::Create,
            _ => object_store::PutMode::Overwrite, // (DataFusion writes files whole: create or overwrite)
        };
        self.0.put_opts(&path(at), payload(data), object_store::PutOptions { mode, ..Default::default() }).await.map(put_result).map_err(err)
    }

    async fn put_multipart_opts(&self, at: &df::path::Path, _: df::PutMultipartOptions) -> df::Result<Box<dyn df::MultipartUpload>> {
        Ok(Box::new(Upload(self.0.put_multipart_opts(&path(at), Default::default()).await.map_err(err)?)))
    }

    async fn get_opts(&self, at: &df::path::Path, o: df::GetOptions) -> df::Result<df::GetResult> {
        let range = o.range.map(|r| match r {
            df::GetRange::Bounded(r) => object_store::GetRange::Bounded(r),
            df::GetRange::Offset(o) => object_store::GetRange::Offset(o),
            df::GetRange::Suffix(n) => object_store::GetRange::Suffix(n),
        });
        let opts = object_store::GetOptions { range, head: o.head, if_match: o.if_match, if_none_match: o.if_none_match, if_modified_since: o.if_modified_since, if_unmodified_since: o.if_unmodified_since, version: o.version, ..Default::default() };
        let r = self.0.get_opts(&path(at), opts).await.map_err(err)?;
        let (m, range) = (meta(r.meta.clone()), r.range.clone());
        let payload = df::GetResultPayload::Stream(r.into_stream().map_err(err).boxed());
        Ok(df::GetResult { payload, meta: m, range, attributes: Default::default() })
    }

    fn delete_stream(&self, at: BoxStream<'static, df::Result<df::path::Path>>) -> BoxStream<'static, df::Result<df::path::Path>> {
        let paths = at.filter_map(|p| async move { p.ok().map(|p| Ok(path(&p))) }).boxed();
        self.0.delete_stream(paths).map(|r| r.map(|p| df_path(&p)).map_err(err)).boxed()
    }

    fn list(&self, prefix: Option<&df::path::Path>) -> BoxStream<'static, df::Result<df::ObjectMeta>> {
        self.0.list(prefix.map(path).as_ref()).map(|r| r.map(meta).map_err(err)).boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&df::path::Path>) -> df::Result<df::ListResult> {
        let r = self.0.list_with_delimiter(prefix.map(path).as_ref()).await.map_err(err)?;
        Ok(df::ListResult { common_prefixes: r.common_prefixes.iter().map(df_path).collect(), objects: r.objects.into_iter().map(meta).collect() })
    }

    async fn copy_opts(&self, from: &df::path::Path, to: &df::path::Path, _: df::CopyOptions) -> df::Result<()> {
        self.0.copy_opts(&path(from), &path(to), Default::default()).await.map_err(err)
    }
}
