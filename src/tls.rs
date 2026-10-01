//! TLS on every door (ADR-035 §5). With a certificate (`--tls-cert` and `--tls-key`, or
//! `PONDRA_TLS_CERT` and `PONDRA_TLS_KEY`: PEM files), HTTP, Kafka and Flight take TLS on their
//! own ports, telling it from plain by the first byte (a TLS handshake starts with 22), and
//! Postgres takes it as Postgres does (`sslmode=require`).
//!
//! - **Plain connections** are then taken only from this machine (loopback), unless
//!   `PONDRA_TLS=optional`: a password or a token never crosses a network in the clear.
//! - **Nodes call each other over HTTPS** (`url`, `cluster::http`), trusting `PONDRA_TLS_CA` (or,
//!   without one, the node's own certificate: every node of a lake has the same) beside the
//!   system's authorities.
//! - **Mutual TLS between nodes**: with `PONDRA_TLS_CA`, a node shows its certificate when it calls
//!   another, and a request signed with the nodes' key is taken only over a connection whose
//!   certificate that authority signed (`server::guard`). Clients may show one too; none is asked.
use anyhow::{Context, Result};
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::task::{Context as Cx, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::rustls::{self, pki_types::pem::PemObject, pki_types::CertificateDer, pki_types::PrivateKeyDer, ServerConfig};

/// The doors' TLS settings, read once (`init`).
struct Tls {
    http: tokio_rustls::TlsAcceptor,
    flight: tokio_rustls::TlsAcceptor,
    other: tokio_rustls::TlsAcceptor, // (Kafka, Postgres)
    pem: Vec<u8>,                     // the certificate, to trust when calling another node
    ca: Option<Vec<u8>>,              // PONDRA_TLS_CA
    key: Vec<u8>,
    optional: bool,
}

static TLS: OnceLock<Option<Tls>> = OnceLock::new();

fn tls() -> Option<&'static Tls> { TLS.get_or_init(|| load().unwrap_or_else(|e| panic!("TLS: {e:#}"))).as_ref() }

/// Read the certificate now, so a mistake is said at start (`pondra serve`).
pub fn init() -> Result<()> {
    if TLS.get().is_none() {
        let _ = TLS.set(load()?);
    }
    Ok(())
}

/// Does this node take TLS?
pub fn on() -> bool { tls().is_some() }

/// `http://` or `https://`, for a call to another node.
pub fn url(rest: &str) -> String { format!("{}://{rest}", if on() { "https" } else { "http" }) }

fn load() -> Result<Option<Tls>> {
    let var = |n: &str| std::env::var(n).ok().filter(|v| !v.is_empty());
    let (Some(cert), Some(key)) = (var("PONDRA_TLS_CERT"), var("PONDRA_TLS_KEY")) else {
        anyhow::ensure!(var("PONDRA_TLS_CERT").is_none() && var("PONDRA_TLS_KEY").is_none(), "--tls-cert and --tls-key go together");
        return Ok(None);
    };
    let read = |p: &str| std::fs::read(p).with_context(|| format!("reading {p}"));
    let (pem, key_pem, ca) = (read(&cert)?, read(&key)?, var("PONDRA_TLS_CA").map(|p| read(&p)).transpose()?);
    let certs = CertificateDer::pem_slice_iter(&pem).collect::<Result<Vec<_>, _>>().with_context(|| format!("{cert}: not a PEM certificate"))?;
    anyhow::ensure!(!certs.is_empty(), "{cert} holds no certificate");
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = |alpn: &[&[u8]]| -> Result<tokio_rustls::TlsAcceptor> {
        let builder = ServerConfig::builder_with_provider(provider.clone()).with_safe_default_protocol_versions()?;
        let builder = match &ca {
            Some(ca) => {
                let mut roots = rustls::RootCertStore::empty();
                for c in CertificateDer::pem_slice_iter(ca) {
                    roots.add(c?)?;
                }
                builder.with_client_cert_verifier(rustls::server::WebPkiClientVerifier::builder_with_provider(roots.into(), provider.clone()).allow_unauthenticated().build()?)
            }
            None => builder.with_no_client_auth(),
        };
        let key = PrivateKeyDer::from_pem_slice(&key_pem).with_context(|| format!("{key}: not a PEM private key"))?;
        let mut c = builder.with_single_cert(certs.clone(), key)?;
        c.alpn_protocols = alpn.iter().map(|p| p.to_vec()).collect();
        Ok(tokio_rustls::TlsAcceptor::from(Arc::new(c)))
    };
    let optional = var("PONDRA_TLS").is_some_and(|v| v == "optional");
    let tls = Tls { http: config(&[b"h2", b"http/1.1"])?, flight: config(&[b"h2"])?, other: config(&[])?, pem: pem.clone(), ca, key: key_pem, optional };
    Ok(Some(tls))
}

/// The client nodes call each other with, trusting the lake's certificate and showing it (mTLS).
pub fn client(b: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    let Some(t) = tls() else { return b };
    let roots = reqwest::Certificate::from_pem_bundle(t.ca.as_deref().unwrap_or(&t.pem)).unwrap_or_default();
    let b = roots.into_iter().fold(b, |b, c| b.add_root_certificate(c));
    match (&t.ca, reqwest::Identity::from_pem(&[t.pem.as_slice(), b"\n", t.key.as_slice()].concat())) {
        (Some(_), Ok(me)) => b.identity(me),
        _ => b,
    }
}

/// Which door a connection came to (each says its own protocols in the handshake).
#[derive(Clone, Copy, PartialEq)]
pub enum Door {
    Http,
    Flight,
    Kafka,
}

/// What a connection is: who, over TLS or not, with a certificate the lake's authority signed.
#[derive(Clone, Copy, Debug)]
pub struct Peer {
    pub addr: SocketAddr,
    pub tls: bool,
    pub signed: bool,
    pub local: bool, // (from this machine: loopback, or to its own address)
}

impl Peer {
    /// Is this connection taken? Over TLS; plain only from this machine, or when TLS is off or
    /// optional (a password or a token never crosses a network in the clear).
    pub fn allowed(&self) -> bool { self.tls || self.local || tls().is_none_or(|t| t.optional) }

    /// Is it a node's (for the nodes' key)? With mutual TLS, only with the authority's certificate.
    pub fn may_be_node(&self) -> bool { self.signed || self.local || tls().is_none_or(|t| t.ca.is_none()) }
}

/// Postgres: is a client taken, TLS or not (`pg.rs`, `dbserver.rs`)?
pub fn pg_allowed(secure: bool, addr: SocketAddr) -> bool { Peer { addr, tls: secure, signed: false, local: addr.ip().is_loopback() }.allowed() }

/// What a refused plain connection is told.
pub const PLAIN: &str = "this node takes TLS from other machines (https://, sslmode=require, security.protocol=SSL)";

/// Postgres's acceptor, for `pgwire::tokio::process_socket`.
pub fn pg() -> Option<tokio_rustls::TlsAcceptor> { tls().map(|t| t.other.clone()) }

/// A connection, plain or TLS.
pub enum Conn {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

/// Take a connection: TLS if it starts a handshake and this node has a certificate. A plain one
/// from another machine is refused here, but for HTTP's, which is answered why (`server::guard`).
pub async fn accept(tcp: TcpStream, door: Door) -> std::io::Result<(Conn, Peer)> {
    let addr = tcp.peer_addr()?;
    let local = addr.ip().is_loopback() || tcp.local_addr().is_ok_and(|l| l.ip() == addr.ip());
    let _ = tcp.set_nodelay(true); // (a small answer goes out at once)
    let mut first = [0u8; 1];
    let Some(t) = tls() else { return Ok((Conn::Plain(tcp), Peer { addr, tls: false, signed: false, local })) };
    let n = tokio::time::timeout(std::time::Duration::from_secs(10), tcp.peek(&mut first)).await.map_err(|_| std::io::ErrorKind::TimedOut)??;
    if n == 0 || first[0] != 22 {
        let peer = Peer { addr, tls: false, signed: false, local };
        if door != Door::Http && !peer.allowed() {
            return Err(std::io::ErrorKind::PermissionDenied.into());
        }
        return Ok((Conn::Plain(tcp), peer));
    }
    let acceptor = match door {
        Door::Http => &t.http,
        Door::Flight => &t.flight,
        Door::Kafka => &t.other,
    };
    let s = tokio::time::timeout(std::time::Duration::from_secs(10), acceptor.accept(tcp)).await.map_err(|_| std::io::ErrorKind::TimedOut)??;
    let signed = t.ca.is_some() && s.get_ref().1.peer_certificates().is_some_and(|c| !c.is_empty()); // (verified in the handshake)
    Ok((Conn::Tls(Box::new(s)), Peer { addr, tls: true, signed, local }))
}

/// A door's connections, handshakes done apart so a slow one holds no other up.
pub struct Doors {
    rx: tokio::sync::mpsc::Receiver<(Conn, Peer)>,
    addr: SocketAddr,
}

impl Doors {
    pub async fn bind(addr: &str, door: Door) -> Result<Doors> {
        let listener = TcpListener::bind(addr).await.with_context(|| format!("listening on {addr}"))?;
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let local = listener.local_addr()?;
        crate::panics::spawn(async move {
            loop {
                let tcp = match listener.accept().await {
                    Ok((tcp, _)) => tcp,
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(50)).await; // (out of file handles: a moment)
                        continue;
                    }
                };
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Ok(c) = accept(tcp, door).await {
                        let _ = tx.send(c).await;
                    }
                });
            }
        });
        Ok(Doors { rx, addr: local })
    }

    /// The next connection, handshake done.
    pub async fn next(&mut self) -> Option<(Conn, Peer)> { self.rx.recv().await }

    /// As a stream (tonic's `serve_with_incoming`).
    pub fn stream(self) -> impl futures::Stream<Item = std::io::Result<Conn>> + Send {
        futures::stream::unfold(self, |mut d| async move { d.next().await.map(|(c, _)| (Ok(c), d)) })
    }
}

impl axum::serve::Listener for Doors {
    type Io = Conn;
    type Addr = Peer;
    async fn accept(&mut self) -> (Conn, Peer) {
        match self.next().await {
            Some(c) => c,
            None => std::future::pending().await, // (the accept loop never ends)
        }
    }
    fn local_addr(&self) -> std::io::Result<Peer> { Ok(Peer { addr: self.addr, tls: false, signed: false, local: true }) }
}

impl axum::extract::connect_info::Connected<axum::serve::IncomingStream<'_, Doors>> for Peer {
    fn connect_info(s: axum::serve::IncomingStream<'_, Doors>) -> Peer { *s.remote_addr() }
}

impl tonic::transport::server::Connected for Conn {
    type ConnectInfo = Peer; // (a request's `extensions().get::<Peer>()`: Flight's)
    fn connect_info(&self) -> Peer { self.peer() }
}

impl Conn {
    pub fn is_tls(&self) -> bool { matches!(self, Conn::Tls(_)) }

    /// Who is at the other end.
    pub fn peer(&self) -> Peer {
        let (tcp, signed) = match self {
            Conn::Plain(s) => (s, false),
            Conn::Tls(s) => (s.get_ref().0, s.get_ref().1.peer_certificates().is_some_and(|c| !c.is_empty()) && tls().is_some_and(|t| t.ca.is_some())),
        };
        let addr = tcp.peer_addr().unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)));
        let local = addr.ip().is_loopback() || tcp.local_addr().is_ok_and(|l| l.ip() == addr.ip());
        Peer { addr, tls: self.is_tls(), signed, local }
    }
}

impl AsyncRead for Conn {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Cx<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Conn {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Cx<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }
    fn poll_write_vectored(self: Pin<&mut Self>, cx: &mut Cx<'_>, bufs: &[std::io::IoSlice<'_>]) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_write_vectored(cx, bufs),
        }
    }
    fn is_write_vectored(&self) -> bool {
        match self {
            Conn::Plain(s) => s.is_write_vectored(),
            Conn::Tls(s) => s.is_write_vectored(),
        }
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Cx<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_flush(cx),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Cx<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Conn::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Conn::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}
