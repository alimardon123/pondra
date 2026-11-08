//! How a node's calls out reach the network (redesign §8, step 2): one client factory, with the policy named by whom a
//! call goes to. Each policy is a security choice made once here, not at each call: the public-only resolver and checked
//! redirects for a URL someone other than the admin named (the SSRF guards), no proxy for this machine's own nodes, and
//! the nodes' token for the lake's nodes alone. A node's state holds its `Net` (`NodeState::net`), installed by the host
//! as the machine is (natively `os::Net`; on a page, its fetch); what calls out with no node at hand (the command line's
//! own calls, a model's API) asks the process's (`process`). A request sets its own time limit (`timeout`): a
//! client has none.
use anyhow::{Result, ensure};
use object_store::client::{ClientOptions, HttpClient, HttpConnector};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::{Arc, OnceLock};

/// Whom a call goes to, which decides how it reaches them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Policy {
    /// The lake's nodes: the lake's TLS (mTLS where it has an authority), the node's token and who calls (ADR-039).
    Cluster,
    /// A URL someone other than the node's admin named (a share's links, a store a reader's temporary secret reaches):
    /// public addresses only, each name checked as it resolves and each redirect as it comes, and never a proxy, which
    /// would resolve the names itself past that check (audit 10-30 M8).
    Public,
    /// A child or the node on this machine: plain HTTP, no proxy (it couldn't reach this machine) and no outside
    /// certificates (a minimal image has none).
    Local,
    /// A service the admin or the user named (a REST catalog, an identity provider, a model's API, git, a node a
    /// command names): the environment's proxy, the system's certificates and no token of ours. `origin_only`: a
    /// redirect is followed on the first request's origin (scheme, host and port) alone, and one elsewhere is handed
    /// back unfollowed (a share provider's own calls: where its links lead is `Public`'s).
    Endpoint { origin_only: bool },
}

impl Policy {
    /// A service someone named, its redirects followed as they come.
    pub const ENDPOINT: Policy = Policy::Endpoint { origin_only: false };
}

/// The clients a node's calls out are made with.
pub trait Net: Send + Sync {
    /// The client for `policy`: pooled, one per policy per node.
    fn client(&self, policy: Policy) -> reqwest::Client;

    /// object_store's HTTP connector for a store reached by `policy`, `headers` sent with every request (an http secret's
    /// bearer token): a store a reader's own temporary secret makes is `Public`'s, any other the admin's (`Endpoint`).
    /// A lake's own bucket has the lake's budget instead (`budget::Budget`).
    fn connector(&self, policy: Policy, headers: reqwest::header::HeaderMap) -> StoreConnector { StoreConnector(Arc::new(Connector { policy, headers })) }

    /// The store of a web origin (`https://host`, with no secret of its own) through the platform's own fetch, where it has
    /// one: a page's, whose browser's server may hide `Content-Range` and lists no folder (WebDAV's PROPFIND), where
    /// object_store's HTTP client needs both (web/'s `fetch.rs`). None by default: a node reads a web URL with
    /// object_store's HTTP client on `connector`'s (`ext::build`).
    fn web(&self, _policy: Policy, _origin: &str) -> Option<Arc<dyn object_store::ObjectStore>> { None }
}

/// A store's connector from a `Net` (`Net::connector`), as object_store's builders take one.
#[derive(Clone, Debug)]
pub struct StoreConnector(pub Arc<dyn HttpConnector>);

impl HttpConnector for StoreConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> { self.0.connect(options) }
}

/// A store's HTTP client for its policy (`Net::connector`). `Public`'s is made as object_store makes its own
/// (`ClientOptions::client`) for what a temporary secret can set, with names resolved and redirects checked as that
/// policy's are (`guarded`) and never a proxy: whoever makes one names any URL and any endpoint with it, and without
/// this the node would fetch for them what only it can reach (the cloud's metadata address, its own network and ports).
/// Any other policy's is object_store's own, from the store's options: the environment's proxy and the system's
/// certificates, as `Endpoint`'s are (no store is this machine's or the nodes'). On a page `Public`'s is the page's
/// fetch, which resolves names and follows redirects itself, under CORS.
#[derive(Debug)]
pub struct Connector {
    policy: Policy,
    headers: reqwest::header::HeaderMap,
}

impl HttpConnector for Connector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        if self.policy != Policy::Public {
            return object_store::client::ReqwestConnector::default().connect(options);
        }
        let client = self.public(options).map_err(|e| object_store::Error::Generic { store: "outside", source: Box::new(e) })?;
        Ok(HttpClient::new(client))
    }
}

impl Connector {
    /// `Public`'s store client (`connect`), plain HTTP too where the store's options allow it.
    #[cfg(not(target_arch = "wasm32"))]
    fn public(&self, options: &ClientOptions) -> reqwest::Result<reqwest::Client> {
        let http = options.get_config_value(&object_store::client::ClientConfigKey::AllowHttp).as_deref() == Some("true");
        guarded(reqwest::Client::builder())
            .user_agent(concat!("pondra/", env!("CARGO_PKG_VERSION")))
            .default_headers(self.headers.clone())
            .connect_timeout(std::time::Duration::from_secs(5)) // (object_store's own defaults)
            .timeout(std::time::Duration::from_secs(30))
            .http1_only()
            .no_gzip().no_brotli().no_zstd().no_deflate() // (sizes are read from Content-Length)
            .https_only(!http)
            .build()
    }

    #[cfg(target_arch = "wasm32")]
    fn public(&self, _: &ClientOptions) -> reqwest::Result<reqwest::Client> {
        reqwest::Client::builder().user_agent(concat!("pondra/", env!("CARGO_PKG_VERSION"))).default_headers(self.headers.clone()).build()
    }
}

/// The policy a URL a statement names gets (review change 7): `Endpoint` for an asker who may reach inside the node's
/// network (`inside`: the program that started the node, an admin's attachment or secret), else `Public`, and refused
/// here if its host is an address that isn't public or this machine by name (`check`; any other name is checked as it
/// resolves). Step 6 makes `inside` a right.
pub fn for_endpoint(url: &url::Url, inside: bool) -> Result<Policy> {
    if inside {
        return Ok(Policy::ENDPOINT);
    }
    check(url)?;
    Ok(Policy::Public)
}

/// `req` with a time limit: a request sets its own, since a policy's client has none. A page's fetch keeps the browser's
/// own limits.
pub fn timeout(req: reqwest::RequestBuilder, limit: std::time::Duration) -> reqwest::RequestBuilder {
    #[cfg(not(target_arch = "wasm32"))]
    let req = req.timeout(limit);
    #[cfg(target_arch = "wasm32")]
    let _ = limit;
    req
}

/// Each policy's client, made as the policy says when first asked: a node's own (`NodeState::new`), and the process's
/// when its host installed no `Net` (a unit test's lakes, a build with no doors). Its `Cluster` client is plain, with no
/// token and no TLS of the lake's: those are the host's (`os::Net`). On a page every policy is the page's fetch, under
/// the browser's own rules (CORS, Private Network Access): it resolves names and follows redirects itself.
#[derive(Default)]
pub struct Clients {
    public: OnceLock<reqwest::Client>,
    local: OnceLock<reqwest::Client>,
    endpoint: OnceLock<reqwest::Client>,
    origin: OnceLock<reqwest::Client>,
}

impl Clients {
    pub const fn new() -> Clients { Clients { public: OnceLock::new(), local: OnceLock::new(), endpoint: OnceLock::new(), origin: OnceLock::new() } }
}

impl Net for Clients {
    #[cfg(not(target_arch = "wasm32"))]
    fn client(&self, policy: Policy) -> reqwest::Client {
        let made = |b: reqwest::ClientBuilder| b.build().expect("an HTTP client");
        match policy {
            Policy::Public => self.public.get_or_init(|| made(guarded(reqwest::Client::builder()))),
            Policy::Local => self.local.get_or_init(|| made(reqwest::Client::builder().no_proxy().tls_certs_only([]))),
            Policy::Endpoint { origin_only: true } => self.origin.get_or_init(|| made(reqwest::Client::builder().redirect(same_origin()))),
            Policy::Endpoint { origin_only: false } | Policy::Cluster => self.endpoint.get_or_init(|| reqwest::Client::builder().build().unwrap_or_default()),
        }
        .clone()
    }

    #[cfg(target_arch = "wasm32")]
    fn client(&self, _: Policy) -> reqwest::Client { self.endpoint.get_or_init(reqwest::Client::new).clone() }
}

// (the process's `Net`: installed once by its host, as its machine is)
static INSTALLED: OnceLock<Arc<dyn Net>> = OnceLock::new();

// (the clients of a process whose host installed none)
static UNINSTALLED: Clients = Clients::new();

/// Installs the process's `Net`, once: a second install keeps the first, which it answers.
pub fn install(net: Arc<dyn Net>) -> Arc<dyn Net> { INSTALLED.get_or_init(|| net).clone() }

/// The process's `Net`, for a call with no node at hand: the one its host installed, else plain clients (`Clients`).
pub fn process() -> &'static dyn Net {
    match INSTALLED.get() {
        Some(n) => n.as_ref(),
        None => &UNINSTALLED,
    }
}

/// A node's `Net` (`NodeState::new`): the one its process's host installed, else clients of its own.
pub fn installed() -> Arc<dyn Net> { INSTALLED.get().cloned().unwrap_or_else(|| Arc::new(Clients::new())) }

/// Is `ip` an address of the public internet? Not this machine's, a private network's, a link's (the clouds' metadata
/// address is one), carrier-grade NAT's, or one kept for something else.
pub fn public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => public_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => public_v4(v4),
            None => public_v6(v6),
        },
    }
}

fn public_v4(ip: Ipv4Addr) -> bool {
    let [a, b, c, _] = ip.octets();
    let kept = a == 0 || a >= 240 // ("this network", and the reserved block with the broadcast address)
        || (a == 100 && (64..128).contains(&b)) // (carrier-grade NAT)
        || (a == 192 && b == 0 && c == 0) // (IETF protocol assignments)
        || (a == 198 && (18..20).contains(&b)); // (benchmarking)
    !(kept || ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_multicast() || ip.is_documentation())
}

fn public_v6(ip: Ipv6Addr) -> bool {
    let s = ip.segments();
    let inside = |hi: u16, lo: u16| public_v4(Ipv4Addr::from(((hi as u32) << 16) | lo as u32));
    match s {
        [0x64, 0xff9b, 0, 0, 0, 0, hi, lo] => inside(hi, lo), // (NAT64: the IPv4 address it reaches)
        [0x2002, hi, lo, ..] => inside(hi, lo),               // (6to4)
        [0, 0, 0, 0, 0xffff, 0, hi, lo] => inside(hi, lo),    // (IPv4-translated, RFC 2765: `to_ipv4_mapped` doesn't take it)
        [0, 0, 0, 0, 0, 0, ..] => false,                      // (unspecified, loopback, IPv4-compatible)
        [0x64, 0xff9b, 1, ..] => false,                       // (local-use NAT64, RFC 8215: a network's own way to its IPv4 ones)
        [0x2001, 0, ..] => false,                             // (Teredo: an IPv4 address inside, through a relay)
        _ => !(ip.is_multicast()
            || s[0] & 0xfe00 == 0xfc00 // (unique local: AWS's metadata address fd00:ec2::254 among them)
            || s[0] & 0xffc0 == 0xfe80 // (link-local)
            || s[0] & 0xffc0 == 0xfec0 // (site-local)
            || (s[0] == 0x2001 && s[1] == 0x0db8)), // (documentation)
    }
}

/// Why `url` may not be asked, if its host is an address that isn't public, or this machine by name.
/// (Any other name is checked as it resolves: `Names`.)
pub fn check(url: &url::Url) -> Result<()> {
    let refused = |what: String| format!("{what}: a temporary secret reaches only the public internet (an admin's CREATE SECRET … SCOPE '…' reaches the rest)");
    match url.host() {
        Some(url::Host::Ipv4(a)) => ensure!(public(IpAddr::V4(a)), "{}", refused(format!("{a} isn't a public address"))),
        Some(url::Host::Ipv6(a)) => ensure!(public(IpAddr::V6(a)), "{}", refused(format!("{a} isn't a public address"))),
        Some(url::Host::Domain(d)) => {
            let d = d.trim_end_matches('.').to_ascii_lowercase();
            ensure!(d != "localhost" && !d.ends_with(".localhost"), "{}", refused(format!("{d} is this machine")));
        }
        None => {}
    }
    Ok(())
}

/// What makes a client reach only the public internet: names checked as they resolve (`Names`), each redirect checked
/// (`redirects`), and never a proxy, which would resolve the names itself, past `Names`' check (audit 10-30 M8). The
/// `Public` policy's client is made with it, and so is a store's that a reader's temporary secret reaches (`Connector`).
#[cfg(not(target_arch = "wasm32"))]
pub fn guarded(b: reqwest::ClientBuilder) -> reqwest::ClientBuilder {
    b.dns_resolver(Names).redirect(redirects()).no_proxy()
}

/// Each redirect checked as the first request was (`check`; a name is checked as it resolves), ten at most.
#[cfg(not(target_arch = "wasm32"))]
fn redirects() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|to| {
        if to.previous().len() >= 10 {
            return to.error("more than 10 redirects");
        }
        match check(to.url()) {
            Ok(()) => to.follow(),
            Err(e) => {
                let why = format!("redirected to {}: {e}", to.url());
                to.error(why)
            }
        }
    })
}

/// A redirect followed on the first request's origin alone, ten at most; one elsewhere is handed back unfollowed.
#[cfg(not(target_arch = "wasm32"))]
fn same_origin() -> reqwest::redirect::Policy {
    reqwest::redirect::Policy::custom(|to| match to.previous().len() >= 10 {
        true => to.error("more than 10 redirects"),
        false if to.previous().first().is_some_and(|first| first.origin() == to.url().origin()) => to.follow(),
        false => to.stop(),
    })
}

/// Resolves a name, and refuses it if any of its addresses isn't public: a connection may be tried on any of them.
#[cfg(not(target_arch = "wasm32"))]
struct Names;

#[cfg(not(target_arch = "wasm32"))]
impl reqwest::dns::Resolve for Names {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let found: Vec<std::net::SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if let Some(a) = found.iter().find(|a| !public(a.ip())) {
                return Err(format!("{host} is {}, not a public address: a temporary secret reaches only the public internet (an admin's CREATE SECRET … SCOPE '…' reaches the rest)", a.ip()).into());
            }
            Ok(Box::new(found.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Runs `asks` in a child of this test binary whose environment names a proxy (all six variables), and fails if any
/// connection reached it. `test` is the calling test's whole name, which the child runs. In a child, since the proxy is
/// named in the environment, which the other tests of this process share (#601).
#[cfg(test)]
pub(crate) fn reaches_no_proxy(test: &str, asks: impl FnOnce()) {
    const CHILD: &str = "PONDRA_TEST_PROXY_CHILD";
    if std::env::var_os(CHILD).is_some() {
        return asks();
    }
    let proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let at = format!("http://{}", proxy.local_addr().unwrap());
    proxy.set_nonblocking(true).unwrap();
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let watch = {
        let done = done.clone();
        // (a request that reaches the proxy is noted and its connection closed at once, so the child's request fails then)
        std::thread::spawn(move || {
            let mut reached = false;
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                match proxy.accept() {
                    Ok(_) => reached = true,
                    Err(_) => std::thread::sleep(std::time::Duration::from_millis(5)),
                }
            }
            reached
        })
    };
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    child.args(["--exact", test, "--test-threads=1"]).env(CHILD, "1");
    for name in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"] {
        child.env(name, &at);
    }
    let out = child.env_remove("NO_PROXY").env_remove("no_proxy").output().unwrap();
    done.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(!watch.join().unwrap(), "a request went to the proxy");
    // (the child ran the test: a name it doesn't know runs none, and passes)
    assert!(out.status.success() && String::from_utf8_lossy(&out.stdout).contains("1 passed"), "the child's own check: {}", String::from_utf8_lossy(&out.stdout));
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A store a reader's temporary secret makes never goes through a proxy the environment names, which would resolve
    /// the names itself, past the `Public` policy's resolver (audit 10-30 M8); an admin's store is object_store's own.
    #[test]
    fn a_temporary_secrets_store_goes_through_no_proxy() {
        reaches_no_proxy("net::tests::a_temporary_secrets_store_goes_through_no_proxy", || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let store = Connector { policy: Policy::Public, headers: Default::default() };
            let asked = runtime.block_on(async { store.public(&ClientOptions::new().with_allow_http(true)).unwrap().get("http://nothing.invalid/x.csv").send().await });
            assert!(asked.is_err(), "nothing.invalid doesn't resolve");
        });
        assert!(Clients::new().connector(Policy::ENDPOINT, Default::default()).connect(&ClientOptions::new()).is_ok(), "an admin's store: object_store's own client");
    }

    #[test]
    fn only_the_public_internet() {
        for a in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "64:ff9b::808:808", "2002:808:808::1", "::ffff:8.8.8.8", "::ffff:0:808:808", "2001:4860:4860::8888"] {
            assert!(public(a.parse().unwrap()), "{a} is public");
        }
        for a in ["127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.64.0.1", "0.0.0.0", "255.255.255.255", "224.0.0.1",
                  "192.0.0.10", "198.18.0.1", "::1", "::", "fd00:ec2::254", "fe80::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "64:ff9b::a9fe:a9fe",
                  "2002:a9fe:a9fe::1", "2001:db8::1", "ff02::1", "::127.0.0.1", "::ffff:0:a9fe:a9fe", "::ffff:0:7f00:1", "64:ff9b:1::a00:1",
                  "2001:0:4136:e378:8000:63bf:3fff:fdd2"] {
            assert!(!public(a.parse().unwrap()), "{a} isn't public");
        }
    }

    #[test]
    fn written_addresses_and_this_machine_refused() {
        let ok = |u: &str| check(&url::Url::parse(u).unwrap()).is_ok();
        assert!(ok("https://example.com/x.csv") && ok("s3://bucket/x") && ok("https://8.8.8.8/x"));
        assert!(!ok("http://169.254.169.254/latest/meta-data/") && !ok("http://[::1]:8080/") && !ok("http://localhost:9000/b") && !ok("http://a.localhost./x"));
        assert!(!ok("http://[::ffff:a9fe:a9fe]/") && !ok("http://2130706433/"), "an address however it is written");
    }

    /// A URL a statement names reaches inside the network only for an asker who may (an admin, until step 6 grants it):
    /// anyone else's is the public internet's, and a written address of a private network or this machine is refused.
    #[test]
    fn a_named_url_is_public_unless_its_asker_may_reach_inside() {
        let url = |u: &str| url::Url::parse(u).unwrap();
        assert_eq!(for_endpoint(&url("https://catalog.example.com/v1"), false).unwrap(), Policy::Public);
        assert_eq!(for_endpoint(&url("https://catalog.example.com/v1"), true).unwrap(), Policy::ENDPOINT);
        for private in ["http://10.0.0.5/v1", "http://127.0.0.1:8181/", "http://localhost:8181/", "http://169.254.169.254/latest/"] {
            assert!(for_endpoint(&url(private), false).is_err(), "{private} refused to an asker outside");
            assert_eq!(for_endpoint(&url(private), true).unwrap(), Policy::ENDPOINT, "{private} reached by an admin");
        }
    }

    /// The public policy refuses a name that resolves to this machine, and a redirect to one, whatever the URL said; a
    /// service the admin named is reached as it is.
    #[tokio::test]
    async fn the_public_policy_refuses_this_machine_by_name_and_by_redirect() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let Ok((mut s, _)) = listener.accept().await else { return };
                tokio::spawn(async move {
                    use tokio::io::{AsyncReadExt, AsyncWriteExt};
                    let mut b = [0u8; 2048];
                    let n = s.read(&mut b).await.unwrap_or(0);
                    let asked = String::from_utf8_lossy(&b[..n]);
                    let answer = match asked.starts_with("GET /away") {
                        true => format!("HTTP/1.1 302 Found\r\nlocation: http://127.0.0.1:{port}/here\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"),
                        false => "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok".to_string(),
                    };
                    let _ = s.write_all(answer.as_bytes()).await;
                });
            }
        });
        let net = Clients::new();
        let by_name = format!("http://localhost:{port}/here");
        let e = net.client(Policy::Public).get(&by_name).send().await.unwrap_err();
        assert!(format!("{e:?}").contains("not a public address"), "a name resolving to this machine: {e:?}");
        assert!(net.client(Policy::ENDPOINT).get(&by_name).send().await.unwrap().status().is_success(), "the admin's service is reached");
        assert!(net.client(Policy::Local).get(&by_name).send().await.unwrap().status().is_success(), "this machine's node is reached");
        // (the redirect's first hop: asked of an address, as `check` refuses before any request; here the client alone is tried)
        let away = format!("http://127.0.0.1:{port}/away");
        let e = net.client(Policy::Public).get(&away).send().await.unwrap_err();
        assert!(e.is_redirect() && format!("{e:?}").contains("isn't a public address"), "a redirect to this machine: {e:?}");
        let r = net.client(Policy::Endpoint { origin_only: true }).get(&away).send().await.unwrap();
        assert!(r.status().is_success(), "a redirect on the same origin is followed");
    }

    /// The public policy never goes through a proxy the environment names, which would resolve the names itself, past
    /// `Names` (audit 10-30 M8); nor does the local one, which couldn't reach this machine through it.
    #[test]
    fn the_public_and_local_policies_go_through_no_proxy() {
        reaches_no_proxy("net::tests::the_public_and_local_policies_go_through_no_proxy", || {
            let net = Clients::new();
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            for policy in [Policy::Public, Policy::Local] {
                let asked = runtime.block_on(async { net.client(policy).get("http://nothing.invalid/x.csv").send().await });
                assert!(asked.is_err(), "nothing.invalid doesn't resolve ({policy:?})");
            }
        });
    }
}
