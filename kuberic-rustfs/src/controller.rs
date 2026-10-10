use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use kuberic_controller::native::{NativeNodeApi, NativePlan, NativePlanStatus, reconcile};
use kuberic_native_runtime::native::{
    NativeAuthority, NativeError, NativeObservation, NativeOperation, NativeOperationCommand,
    NativeOperationStatus,
};
use reqwest::{Client, Method};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::HealthClient;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControllerConfig {
    pub token_file: PathBuf,
    pub plan: NativePlan,
}

struct NodeApi {
    client: Client,
    token: String,
}

impl NodeApi {
    async fn request<T: Serialize + Sync, R: DeserializeOwned>(
        &self,
        node: &str,
        path: &str,
        method: Method,
        body: Option<&T>,
    ) -> Result<R, NativeError> {
        async {
            let url = reqwest::Url::parse(node)?.join(path)?;
            let mut request = self.client.request(method, url).bearer_auth(&self.token);
            if let Some(body) = body {
                request = request.json(body);
            }
            let mut response = request.send().await?;
            let status = response.status();
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await? {
                ensure!(
                    bytes.len().saturating_add(chunk.len()) <= 1024 * 1024,
                    "native control response is too large"
                );
                bytes.extend_from_slice(&chunk);
            }
            ensure!(
                status.is_success(),
                "native control {node}{path} returned {status}: {}",
                String::from_utf8_lossy(&bytes)
            );
            if status == reqwest::StatusCode::NO_CONTENT {
                return serde_json::from_slice(b"null")
                    .context("invalid native acknowledgement type");
            }
            serde_json::from_slice(&bytes).context("invalid native control response")
        }
        .await
        .map_err(|error: anyhow::Error| NativeError::Application(format!("{error:#}")))
    }
}

#[async_trait]
impl NativeNodeApi for NodeApi {
    async fn observe(&self, node: &str) -> Result<NativeObservation, NativeError> {
        self.request::<(), _>(node, "/v1/native/observation", Method::GET, None)
            .await
    }

    async fn authorize(&self, node: &str, authority: &NativeAuthority) -> Result<(), NativeError> {
        self.request(node, "/v1/native/authority", Method::POST, Some(authority))
            .await
    }

    async fn status(
        &self,
        node: &str,
        operation: &NativeOperation,
    ) -> Result<Option<NativeOperationStatus>, NativeError> {
        self.request(
            node,
            "/v1/native/operation/status",
            Method::POST,
            Some(operation),
        )
        .await
    }

    async fn apply(
        &self,
        node: &str,
        command: &NativeOperationCommand,
    ) -> Result<NativeOperationStatus, NativeError> {
        self.request(node, "/v1/native/operation", Method::POST, Some(command))
            .await
    }
}

pub async fn reconcile_once(config: &ControllerConfig) -> Result<NativePlanStatus> {
    ensure!(
        (5_000..=30_000).contains(&config.plan.lease_millis),
        "controller leases must be between 5000 and 30000 milliseconds"
    );
    for node in &config.plan.nodes {
        HealthClient::new(&node.node, Duration::from_secs(10))?;
    }
    let token = tokio::fs::read_to_string(&config.token_file).await?;
    ensure!(!token.trim().is_empty(), "native control token is empty");
    let api = NodeApi {
        client: Client::builder()
            .timeout(Duration::from_millis(config.plan.lease_millis / 4))
            .connect_timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()?,
        token: token.trim().into(),
    };
    reconcile(&api, &config.plan).await.map_err(Into::into)
}

pub async fn load_config<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let file = tokio::fs::File::open(path).await?;
    ensure!(
        file.metadata().await?.len() <= 1024 * 1024,
        "configuration exceeds 1 MiB"
    );
    use tokio::io::AsyncReadExt;
    let mut bytes = Vec::new();
    file.take(1024 * 1024 + 1).read_to_end(&mut bytes).await?;
    ensure!(bytes.len() <= 1024 * 1024, "configuration exceeds 1 MiB");
    serde_json::from_slice(&bytes).context("invalid native configuration")
}

pub async fn run(path: &Path) -> Result<()> {
    let initial: ControllerConfig = load_config(path).await?;
    let mut accepted = initial.plan;
    loop {
        let result = async {
            let config: ControllerConfig = load_config(path).await?;
            ensure!(
                config.plan.revision > accepted.revision || config.plan == accepted,
                "native plan revision cannot go backwards or be reused with different input"
            );
            accepted = config.plan.clone();
            reconcile_once(&config).await
        }
        .await;
        match result {
            Ok(NativePlanStatus::Applied) => {
                tracing::debug!(revision = accepted.revision, "Native plan applied")
            }
            Ok(NativePlanStatus::Pending) => {
                tracing::info!(revision = accepted.revision, "Native plan remains pending")
            }
            Err(error) => {
                tracing::error!(%error, "Native reconciliation failed; unavailable or unverified nodes receive no lease renewal")
            }
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use axum::http::StatusCode;
    use axum::routing::{get, post};
    use axum::{Json, Router};
    use kuberic_controller::native::NativeNodePlan;

    use super::*;

    #[tokio::test]
    async fn unresponsive_peer_does_not_consume_healthy_peers_lease_budget() {
        let authorized = Arc::new(AtomicBool::new(false));
        let updated = authorized.clone();
        let healthy = Router::new()
            .route(
                "/v1/native/observation",
                get(|| async {
                    Json(serde_json::json!({
                        "incarnation": "healthy",
                        "revision": 0,
                        "accepting_clients": false,
                        "health": {"state": "unavailable", "error": "native startup"}
                    }))
                }),
            )
            .route(
                "/v1/native/authority",
                post(move || {
                    let updated = updated.clone();
                    async move {
                        updated.store(true, Ordering::SeqCst);
                        StatusCode::NO_CONTENT
                    }
                }),
            );
        let unresponsive = Router::new().route(
            "/v1/native/observation",
            get(|| async {
                std::future::pending::<()>().await;
                StatusCode::SERVICE_UNAVAILABLE
            }),
        );
        let mut nodes = Vec::new();
        for router in [healthy, unresponsive] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            nodes.push(NativeNodePlan {
                node: format!("http://{}", listener.local_addr().unwrap()),
                operation: None,
            });
            tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        }
        let root = tempfile::tempdir().unwrap();
        let token_file = root.path().join("token");
        std::fs::write(&token_file, "controller-test-token").unwrap();
        let config = ControllerConfig {
            token_file,
            plan: NativePlan {
                revision: 1,
                enabled: true,
                lease_millis: 5000,
                nodes,
            },
        };
        let result = tokio::time::timeout(Duration::from_millis(4500), reconcile_once(&config))
            .await
            .expect("unresponsive peer exhausted the renewal budget");
        assert!(result.is_err(), "unresponsive peer was silently credited");
        assert!(authorized.load(Ordering::SeqCst));
    }
}
