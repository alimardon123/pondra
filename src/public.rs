//! Requests that reach only the public internet (S6): what a store made from a reader's own
//! temporary secret may ask. Whoever makes one names any URL and any endpoint with it, so without
//! this the node would fetch for them what only the node can reach: the cloud's metadata address
//! (and the machine's credentials there), services on its own network, its own ports. A name is
//! checked as it resolves, so it can't point somewhere else between a check and the request; an
//! address written in a URL, and every redirect, is checked before it is asked. The rules are
//! the `Public` policy's (`net.rs`); this is the store's client made with them.

#[cfg(not(target_arch = "wasm32"))]
use object_store::client::ClientConfigKey;
use object_store::client::{ClientOptions, HttpClient, HttpConnector};

/// The store's HTTP client, made as object_store makes its own (`ClientOptions::client`) for what a
/// temporary secret can set, with names resolved and redirects checked as the `Public` policy's are
/// (`net::guarded`). `headers` are sent with every request (an http secret's bearer token).
#[derive(Debug, Default)]
pub struct Connector {
    pub headers: reqwest::header::HeaderMap,
}

impl HttpConnector for Connector {
    #[cfg(not(target_arch = "wasm32"))]
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        let http = options.get_config_value(&ClientConfigKey::AllowHttp).as_deref() == Some("true");
        let client = self.client(http).map_err(|e| object_store::Error::Generic { store: "outside", source: Box::new(e) })?;
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

#[cfg(not(target_arch = "wasm32"))]
impl Connector {
    /// The store's client (`connect`), plain HTTP too when `http`.
    fn client(&self, http: bool) -> reqwest::Result<reqwest::Client> {
        crate::net::guarded(reqwest::Client::builder())
            .user_agent(concat!("pondra/", env!("CARGO_PKG_VERSION")))
            .default_headers(self.headers.clone())
            .connect_timeout(std::time::Duration::from_secs(5)) // (object_store's own defaults)
            .timeout(std::time::Duration::from_secs(30))
            .http1_only()
            .no_gzip().no_brotli().no_zstd().no_deflate() // (sizes are read from Content-Length)
            .https_only(!http)
            .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A temporary secret's store never goes through a proxy the environment names, which would resolve the names itself,
    /// past the `Public` policy's resolver (audit 10-30 M8 closed it for that policy's client; this is the store's).
    #[test]
    fn a_temporary_secrets_store_goes_through_no_proxy() {
        crate::net::reaches_no_proxy("public::tests::a_temporary_secrets_store_goes_through_no_proxy", || {
            let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
            let asked = runtime.block_on(async { Connector::default().client(true).unwrap().get("http://nothing.invalid/x.csv").send().await });
            assert!(asked.is_err(), "nothing.invalid doesn't resolve");
        });
    }
}
