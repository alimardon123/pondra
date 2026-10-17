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
//! - **Behind your own proxy** (`PONDRA_TRUSTED_PROXIES`): a request from one of its addresses is
//!   counted by the client it names in `X-Forwarded-For` (`Peer::client`), so one client's failed
//!   sign-ins don't lock a user out for everyone behind it.
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
    if PROXIES.get().is_none() {
        let _ = PROXIES.set(ranges(&crate::env::get(TRUSTED_PROXIES).unwrap_or_default()).context(TRUSTED_PROXIES)?);
    }
    Ok(())
}

/// Does this node take TLS?
pub fn on() -> bool { tls().is_some() }

/// `http://` or `https://`, for a call to another node.
pub fn url(rest: &str) -> String { format!("{}://{rest}", if on() { "https" } else { "http" }) }

fn load() -> Result<Option<Tls>> {
    let (Some(cert), Some(key)) = (crate::env::get("PONDRA_TLS_CERT"), crate::env::get("PONDRA_TLS_KEY")) else {
        anyhow::ensure!(crate::env::get("PONDRA_TLS_CERT").is_none() && crate::env::get("PONDRA_TLS_KEY").is_none(), "--tls-cert and --tls-key go together");
        return Ok(None);
    };
    let read = |p: &str| std::fs::read(p).with_context(|| format!("reading {p}"));
    let (pem, key_pem, ca) = (read(&cert)?, read(&key)?, crate::env::get("PONDRA_TLS_CA").map(|p| read(&p)).transpose()?);
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
    let optional = crate::env::get("PONDRA_TLS").is_some_and(|v| v == "optional");
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

    /// May a password cross this connection? (The rule of `may_send_password`, for a connection in hand.)
    pub fn may_send_password(&self) -> bool { password_ok(self.tls, self.local, tls().is_some()) }

    /// The address a request comes from, as failed sign-ins are counted (`auth::guessing`) and pages on
    /// this machine are told apart (`from_here`): this connection's; from this machine, the client's that
    /// `pondra serve --lakes` names (`CLIENT`) with the key it gave this node; from a proxy of
    /// `PONDRA_TRUSTED_PROXIES`, the client its `X-Forwarded-For` names. Anyone else's word for it is never taken.
    pub fn client(&self, headers: &axum::http::HeaderMap) -> std::net::IpAddr {
        named(headers, server_key().filter(|_| self.local)).or_else(|| forwarded(headers, self.addr.ip(), proxies())).unwrap_or(self.addr.ip())
    }

    /// The scheme and host a browser reached this node at, for a sign-in's redirect address (ADR-074 §5):
    /// the one `pondra serve --lakes` names (`ORIGIN`, with the key it gave this node); from a proxy of
    /// `PONDRA_TRUSTED_PROXIES`, its `X-Forwarded-Proto` and `X-Forwarded-Host`; else this connection's
    /// scheme and the request's `Host`, or over HTTP/2 its URI's authority (browsers send `:authority` there, and
    /// no `Host`). (A provider sends a code only to an address registered with it.)
    pub fn origin(&self, headers: &axum::http::HeaderMap, uri: &axum::http::Uri) -> Option<String> {
        let said = |name: &str| headers.get(name)?.to_str().ok()?.split(',').next().map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
        let named = said(ORIGIN).and_then(|v| v.split_once(' ').map(|(k, o)| (k.to_string(), o.to_string())));
        if let (Some((given, origin)), Some(key)) = (named, server_key().filter(|_| self.local)) {
            if aws_lc_rs::constant_time::verify_slices_are_equal(given.as_bytes(), key.as_bytes()).is_ok() {
                return Some(origin);
            }
        }
        let proxied = proxies().iter().any(|p| within(self.addr.ip(), *p));
        let scheme = proxied.then(|| said("x-forwarded-proto")).flatten().unwrap_or_else(|| if self.tls { "https" } else { "http" }.to_string());
        let host = proxied.then(|| said("x-forwarded-host")).flatten().or_else(|| said("host")).or_else(|| uri.authority().map(|a| a.to_string()))?;
        let plain = !host.is_empty() && host.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':' | b'[' | b']'));
        (matches!(scheme.as_str(), "http" | "https") && plain).then(|| format!("{scheme}://{host}"))
    }

    /// Is the request a page's on this machine (saving the console's settings, choosing the Python a
    /// node runs)? Behind `pondra serve --lakes` the connection is always this machine's, so the client's
    /// own address decides.
    pub fn from_here(&self, headers: &axum::http::HeaderMap) -> bool { self.client(headers).is_loopback() }
}

/// Your own load balancers and ingresses, as addresses and ranges (`PONDRA_TRUSTED_PROXIES`:
/// `10.0.0.0/8,192.168.1.5`), read at start (`init`).
static PROXIES: OnceLock<Vec<(std::net::IpAddr, u8)>> = OnceLock::new();

fn proxies() -> &'static [(std::net::IpAddr, u8)] {
    PROXIES.get_or_init(|| ranges(&crate::env::get(TRUSTED_PROXIES).unwrap_or_default()).unwrap_or_default())
}

/// `10.0.0.0/8, 192.168.1.5, fd00::/8` as (address, prefix length) pairs; an address alone is itself.
fn ranges(text: &str) -> Result<Vec<(std::net::IpAddr, u8)>> {
    let one = |p: &str| -> Result<(std::net::IpAddr, u8)> {
        let (addr, bits) = p.split_once('/').map_or((p, None), |(a, b)| (a, Some(b)));
        let ip: std::net::IpAddr = addr.parse().with_context(|| format!("{p:?} is not an address or a range (10.0.0.0/8)"))?;
        let most = if ip.is_ipv4() { 32 } else { 128 };
        let bits = match bits {
            None => most,
            Some(b) => b.parse::<u8>().ok().filter(|b| *b <= most).with_context(|| format!("{p:?}: a range's length is 0 to {most}"))?,
        };
        Ok((ip.to_canonical(), bits))
    };
    text.split(',').map(str::trim).filter(|p| !p.is_empty()).map(one).collect()
}

/// Is `ip` in the range (`net`, `bits`)?
fn within(ip: std::net::IpAddr, (net, bits): (std::net::IpAddr, u8)) -> bool {
    use std::net::IpAddr::{V4, V6};
    match (ip.to_canonical(), net) {
        (V4(a), V4(n)) => bits == 0 || (u32::from(a) ^ u32::from(n)) >> (32 - bits as u32) == 0,
        (V6(a), V6(n)) => bits == 0 || (u128::from(a) ^ u128::from(n)) >> (128 - bits as u32) == 0,
        _ => false,
    }
}

/// The client a trusted proxy names: in `X-Forwarded-For`, each proxy adds the address it was called
/// from at the end, so the last address that isn't one of ours is the client (what comes before it is
/// the client's own word). None from anyone else, or when a hop isn't an address.
fn forwarded(headers: &axum::http::HeaderMap, from: std::net::IpAddr, proxies: &[(std::net::IpAddr, u8)]) -> Option<std::net::IpAddr> {
    let ours = |ip: std::net::IpAddr| proxies.iter().any(|p| within(ip, *p));
    if !ours(from) {
        return None;
    }
    let hops: Vec<&str> = headers.get_all("x-forwarded-for").iter().filter_map(|v| v.to_str().ok()).flat_map(|v| v.split(',')).map(str::trim).collect();
    for hop in hops.into_iter().rev() {
        // (some proxies add the port: `10.0.0.7:5123`, `[fd00::7]:5123`)
        let ip = hop.parse::<std::net::IpAddr>().ok().or_else(|| hop.parse::<SocketAddr>().ok().map(|a| a.ip()))?.to_canonical();
        if !ours(ip) {
            return Some(ip);
        }
    }
    None
}

/// The address a `CLIENT` header names, when it carries `key` (`<key> <address>`).
fn named(headers: &axum::http::HeaderMap, key: Option<&str>) -> Option<std::net::IpAddr> {
    let (given, addr) = headers.get(CLIENT)?.to_str().ok()?.split_once(' ')?;
    aws_lc_rs::constant_time::verify_slices_are_equal(given.as_bytes(), key?.as_bytes()).ok()?;
    addr.parse().ok()
}

/// May a password cross a connection from `addr`? Over TLS, from this machine, or with no TLS set up at
/// all. Under `PONDRA_TLS=optional` a plain connection from another machine may carry a token, never a
/// password. Postgres's cleartext sign-in calls it (`pg.rs`); Flight's Basic handshake and Kafka's SASL/PLAIN
/// use its `Peer`'s method, the same rule (`Peer::may_send_password`).
pub fn may_send_password(secure: bool, addr: SocketAddr) -> bool { password_ok(secure, addr.ip().is_loopback(), tls().is_some()) }

/// The rule behind both of `may_send_password`, on what they know (a plain function, so a test can
/// state every case without setting TLS up, which can only be done once per process).
fn password_ok(secure: bool, local: bool, tls_set_up: bool) -> bool { secure || local || !tls_set_up }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_password_crosses_only_tls_this_machine_or_with_no_tls_set_up() {
        assert!(password_ok(true, false, true), "over TLS, from anywhere");
        assert!(password_ok(false, true, true), "plain, from this machine");
        assert!(password_ok(false, false, false), "plain from anywhere, with no TLS set up at all");
        assert!(!password_ok(false, false, true), "plain from another machine, with TLS set up: a token only");
    }

    #[test]
    fn a_client_named_is_taken_only_with_the_servers_key() {
        let key = "0123456789abcdef0123";
        let said = |v: &str| { let mut h = axum::http::HeaderMap::new(); h.insert(CLIENT, v.parse().unwrap()); h };
        assert_eq!(named(&said(&format!("{key} 10.0.0.7")), Some(key)), Some([10, 0, 0, 7].into()), "the lakes server, with its key");
        assert_eq!(named(&said("0123456789abcdef0124 10.0.0.7"), Some(key)), None, "another process here, with a key of its own");
        assert_eq!(named(&said("10.0.0.7"), Some(key)), None, "an address alone");
        assert_eq!(named(&said(&format!("{key} 10.0.0.7")), None), None, "a node no server gave a key (or a connection from another machine)");
        let there = Peer { addr: SocketAddr::from(([10, 0, 0, 2], 9)), tls: true, signed: false, local: false };
        assert_eq!(there.client(&said(&format!("{key} 10.0.0.7"))).to_string(), "10.0.0.2", "another machine's word isn't taken");
    }

    #[test]
    fn a_trusted_proxy_names_its_client_and_no_one_else_does() {
        let lb: std::net::IpAddr = [10, 0, 0, 2].into();
        let ours = ranges("10.0.0.0/24, fd00::/8").unwrap();
        let said = |v: &str| { let mut h = axum::http::HeaderMap::new(); h.insert("x-forwarded-for", v.parse().unwrap()); h };
        let ip = |s: &str| s.parse::<std::net::IpAddr>().unwrap();
        assert_eq!(forwarded(&said("203.0.113.9"), lb, &ours), Some(ip("203.0.113.9")), "the client our load balancer names");
        assert_eq!(forwarded(&said("1.2.3.4, 203.0.113.9, 10.0.0.3"), lb, &ours), Some(ip("203.0.113.9")), "past our own proxies, not the client's own word before it");
        assert_eq!(forwarded(&said("203.0.113.9:5123"), lb, &ours), Some(ip("203.0.113.9")), "with a port");
        assert_eq!(forwarded(&said("203.0.113.9"), ip("192.0.2.1"), &ours), None, "from anyone else, not taken");
        assert_eq!(forwarded(&said("not an address"), lb, &ours), None, "a hop that isn't an address");
        assert_eq!(forwarded(&said("10.0.0.3"), lb, &ours), None, "only our proxies named: the connection's own address");
        assert_eq!(forwarded(&said("203.0.113.9"), ip("::ffff:10.0.0.2"), &ours), Some(ip("203.0.113.9")), "an IPv4 proxy seen over IPv6");
        assert_eq!(forwarded(&said("2001:db8::1"), ip("fd00::5"), &ours), Some(ip("2001:db8::1")), "IPv6 ranges");
        assert_eq!(forwarded(&said("203.0.113.9"), lb, &[]), None, "none trusted by default");
        assert!(ranges("0.0.0.0/0").unwrap().iter().all(|r| within(ip("8.8.8.8"), *r)), "a range of every address");
        for bad in ["10.0.0.0/33", "lb.internal", "10.0.0.0/x"] {
            assert!(ranges(bad).is_err(), "{bad} refused at start");
        }
    }

    #[test]
    fn a_plain_connection_from_this_machine_may_send_a_password() {
        let local = SocketAddr::from(([127, 0, 0, 1], 9));
        assert!(may_send_password(false, local));
        assert!(Peer { addr: local, tls: false, signed: false, local: true }.may_send_password());
    }
}

/// Postgres: is a client taken, TLS or not (`pg.rs`, `dbserver.rs`)?
pub fn pg_allowed(secure: bool, addr: SocketAddr) -> bool { Peer { addr, tls: secure, signed: false, local: addr.ip().is_loopback() }.allowed() }

/// What a refused plain connection is told.
pub const PLAIN: &str = "this node takes TLS from other machines (https://, sslmode=require, security.protocol=SSL)";

/// What a password refused on a plain connection from another machine is told (`may_send_password`).
pub const PASSWORD_PLAIN: &str = "a password crosses only TLS from another machine: use https://, or a token";

/// The header `pondra serve --lakes` names its client's address in, on what it forwards to a database's
/// node: that node sees only the server, on this machine (`Peer::client`). It reads `<key> <address>`,
/// with the key the server gave the node in `CLIENT_KEY`, so no other process here can name an address.
pub const CLIENT: &str = "x-pondra-client";

/// The header `pondra serve --lakes` names the scheme and host its client reached it at in, and the
/// database's path (`<key> https://host:port/db/name`), as `CLIENT` names the client (`Peer::origin`).
pub const ORIGIN: &str = "x-pondra-origin";

/// The key `pondra serve --lakes` gave this node for its `CLIENT` and `ORIGIN` headers (none: no such server).
fn server_key() -> Option<&'static str> {
    static KEY: std::sync::OnceLock<Option<String>> = std::sync::OnceLock::new();
    KEY.get_or_init(|| crate::env::get(CLIENT_KEY).filter(|k| k.len() >= 16)).as_deref()
}

/// The variable the lakes server gives each database's node the key of its `CLIENT` header in.
pub const CLIENT_KEY: &str = "PONDRA_CLIENT_KEY";

/// The proxies whose word for their clients' addresses is taken (`proxies`).
pub const TRUSTED_PROXIES: &str = "PONDRA_TRUSTED_PROXIES";

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
                crate::panics::task(async move {
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
