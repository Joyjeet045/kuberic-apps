//! Native RustFS health observations, owned process lifecycle, and startup topology.
//!
//! These probes do not grant Kuberic authority or prove replication progress,
//! durable acknowledgement, completed healing, or safe topology changes.
//!
//! ```no_run
//! use std::time::Duration;
//! use kuberic_rustfs::{HealthClient, HealthProbe};
//!
//! # async fn observe() -> anyhow::Result<()> {
//! let client = HealthClient::new("http://127.0.0.1:9000", Duration::from_secs(5))?;
//! let status = client.probe(HealthProbe::ClusterWrite).await?;
//! println!("RustFS native write health: {status:?}");
//! # Ok(())
//! # }
//! ```

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode, Url, redirect::Policy};

mod config;
mod process;
mod topology;

pub use config::{BinaryPin, CredentialFiles, LaunchConfig};
pub use process::{ExitReport, RunningRustfs};
pub use topology::Topology;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthProbe {
    Liveness,
    Readiness,
    ClusterRead,
    ClusterWrite,
}

impl HealthProbe {
    fn path(self) -> &'static str {
        match self {
            Self::Liveness => "/health/live",
            Self::Readiness => "/health/ready",
            Self::ClusterRead => "/minio/health/cluster/read",
            Self::ClusterWrite => "/minio/health/cluster",
        }
    }
}

/// The native HTTP verdict, not an adapter-computed quorum decision.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HealthStatus {
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug)]
pub struct HealthClient {
    client: Client,
    endpoint: Url,
}

impl HealthClient {
    /// Use a direct S3-listener origin (not a console or load-balancer URL).
    /// Redirects and environment proxies are disabled to preserve the target node.
    pub fn new(endpoint: &str, timeout: Duration) -> Result<Self> {
        let endpoint = Url::parse(endpoint).context("invalid RustFS health endpoint URL")?;
        ensure!(
            matches!(endpoint.scheme(), "http" | "https")
                && endpoint.host_str().is_some()
                && endpoint.port() != Some(0)
                && endpoint.username().is_empty()
                && endpoint.password().is_none()
                && endpoint.path() == "/"
                && endpoint.query().is_none()
                && endpoint.fragment().is_none(),
            "RustFS health endpoint must be an HTTP(S) origin without credentials, a path, query, or fragment"
        );
        ensure!(!timeout.is_zero(), "RustFS health timeout must be positive");
        ensure!(
            Instant::now().checked_add(timeout).is_some(),
            "RustFS health timeout exceeds the supported deadline range"
        );

        let client = Client::builder()
            .timeout(timeout)
            .connect_timeout(timeout)
            .redirect(Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .context("failed to build RustFS health client")?;
        Ok(Self { client, endpoint })
    }

    /// Makes a fresh GET request. Only 200 and 503 are native health verdicts.
    /// Transport failures and all other status codes are explicit errors.
    /// Bodies are not consumed: RustFS supports both minimal and detailed payloads.
    pub async fn probe(&self, probe: HealthProbe) -> Result<HealthStatus> {
        let mut url = self.endpoint.clone();
        url.set_path(probe.path());
        let response = self
            .client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("RustFS {probe:?} probe failed at {url}"))?;
        match response.status() {
            StatusCode::OK => Ok(HealthStatus::Healthy),
            StatusCode::SERVICE_UNAVAILABLE => Ok(HealthStatus::Unhealthy),
            status => bail!("RustFS {probe:?} probe at {url} returned unexpected HTTP {status}"),
        }
    }
}
