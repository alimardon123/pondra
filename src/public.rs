//! Requests that reach only the public internet (S6): what a store made from a reader's own
//! temporary secret may ask. Whoever makes one names any URL and any endpoint with it, so without
//! this the node would fetch for them what only the node can reach: the cloud's metadata address
//! (and the machine's credentials there), services on its own network, its own ports. A name is
//! checked as it resolves, so it can't point somewhere else between a check and the request; an
//! address written in a URL, and every redirect, is checked before it is asked.

use anyhow::{Result, ensure};
#[cfg(not(target_arch = "wasm32"))]
use object_store::client::ClientConfigKey;
use object_store::client::{ClientOptions, HttpClient, HttpConnector};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
#[cfg(not(target_arch = "wasm32"))]
use std::net::SocketAddr;

/// Is `ip` an address of the public internet? Not this machine's, a private network's, a link's
/// (the clouds' metadata address is one), carrier-grade NAT's, or one kept for something else.
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
        [0, 0, 0, 0, 0, 0, ..] => false,                      // (unspecified, loopback, IPv4-compatible)
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

/// The store's HTTP client, made as object_store makes its own (`ClientOptions::client`) for what a
/// temporary secret can set, with names resolved by `Names` and redirects checked by `check`.
/// `headers` are sent with every request (an http secret's bearer token).
#[derive(Debug, Default)]
pub struct Connector {
    pub headers: reqwest::header::HeaderMap,
}

impl HttpConnector for Connector {
    #[cfg(not(target_arch = "wasm32"))]
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let http = options.get_config_value(&ClientConfigKey::AllowHttp).as_deref() == Some("true");
        let redirect = reqwest::redirect::Policy::custom(|to| {
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
        });
        let client = reqwest::Client::builder()
            .user_agent(concat!("pondra/", env!("CARGO_PKG_VERSION")))
            .default_headers(self.headers.clone())
            .connect_timeout(std::time::Duration::from_secs(5)) // (object_store's own defaults)
            .timeout(std::time::Duration::from_secs(30))
            .http1_only()
            .no_gzip().no_brotli().no_zstd().no_deflate() // (sizes are read from Content-Length)
            .https_only(!http)
            .dns_resolver(Names)
            .redirect(redirect)
            .build()
            .map_err(|e| object_store::Error::Generic { store: "outside", source: Box::new(e) })?;
        Ok(HttpClient::new(client))
    }

    // (a browser's fetch follows redirects and resolves names itself, under CORS: its client has no redirect policy or resolver)
    #[cfg(target_arch = "wasm32")]
    fn connect(&self, _: &ClientOptions) -> object_store::Result<HttpClient> {
        let client = reqwest::Client::builder()
            .user_agent(concat!("pondra/", env!("CARGO_PKG_VERSION")))
            .default_headers(self.headers.clone())
            .build()
            .map_err(|e| object_store::Error::Generic { store: "outside", source: Box::new(e) })?;
        Ok(HttpClient::new(client))
    }
}

/// Resolves a name, and refuses it if any of its addresses isn't public: a connection may be tried
/// on any of them.
#[cfg(not(target_arch = "wasm32"))]
struct Names;

#[cfg(not(target_arch = "wasm32"))]
impl reqwest::dns::Resolve for Names {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let host = name.as_str().to_string();
        Box::pin(async move {
            let found: Vec<SocketAddr> = tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if let Some(a) = found.iter().find(|a| !public(a.ip())) {
                return Err(format!("{host} is {}, not a public address: a temporary secret reaches only the public internet (an admin's CREATE SECRET … SCOPE '…' reaches the rest)", a.ip()).into());
            }
            Ok(Box::new(found.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_public_internet() {
        for a in ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111", "64:ff9b::808:808", "2002:808:808::1", "::ffff:8.8.8.8"] {
            assert!(public(a.parse().unwrap()), "{a} is public");
        }
        for a in ["127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.64.0.1", "0.0.0.0", "255.255.255.255", "224.0.0.1",
                  "192.0.0.10", "198.18.0.1", "::1", "::", "fd00:ec2::254", "fe80::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "64:ff9b::a9fe:a9fe",
                  "2002:a9fe:a9fe::1", "2001:db8::1", "ff02::1", "::127.0.0.1"] {
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
}
