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
    pub ca_certificate_file: PathBuf,
    pub plan: NativePlan,
}

struct NodeApi {
    client: Client,
    token: String,
}

impl NodeApi {
    async fn new(config: &ControllerConfig) -> Result<Self> {
        ensure!(
            (5_000..=30_000).contains(&config.plan.lease_millis),
            "controller leases must be between 5000 and 30000 milliseconds"
        );
        for node in &config.plan.nodes {
            HealthClient::new(&node.node, Duration::from_secs(10))?;
            ensure!(
                reqwest::Url::parse(&node.node)?.scheme() == "https",
                "native control origins must use HTTPS"
            );
        }
        let ca = tokio::fs::read(&config.ca_certificate_file).await?;
        let client = Client::builder()
            .timeout(Duration::from_millis(config.plan.lease_millis / 4))
            .connect_timeout(Duration::from_secs(2))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy();
        Ok(Self {
            client: crate::config::trust_ca(client, &ca)?.build()?,
            token: crate::config::control_token(&config.token_file).await?,
        })
    }

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
    let api = NodeApi::new(config).await?;
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

    fn test_tls(root: &Path, name: &str) -> crate::ControlTlsConfig {
        let certificate = rcgen::generate_simple_self_signed(vec![name.into()]).unwrap();
        let config = crate::ControlTlsConfig {
            certificate_file: root.join("tls.crt"),
            private_key_file: root.join("tls.key"),
        };
        std::fs::write(&config.certificate_file, certificate.cert.pem()).unwrap();
        std::fs::write(
            &config.private_key_file,
            certificate.signing_key.serialize_pem(),
        )
        .unwrap();
        config
    }

    struct TestNode {
        origin: String,
        handle: axum_server::Handle<std::net::SocketAddr>,
    }

    impl Drop for TestNode {
        fn drop(&mut self) {
            self.handle.shutdown();
        }
    }

    async fn test_node(tls: &crate::ControlTlsConfig, router: Router) -> TestNode {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("https://{}", listener.local_addr().unwrap());
        let handle = axum_server::Handle::new();
        let server =
            axum_server::from_tcp_rustls(listener.into_std().unwrap(), tls.load().await.unwrap())
                .unwrap()
                .handle(handle.clone());
        tokio::spawn(async move { server.serve(router.into_make_service()).await.unwrap() });
        TestNode { origin, handle }
    }

    fn test_config(root: &Path, ca: PathBuf, nodes: Vec<NativeNodePlan>) -> ControllerConfig {
        let token_file = root.join("token");
        std::fs::write(&token_file, "controller-test-token-with-32-characters").unwrap();
        ControllerConfig {
            token_file,
            ca_certificate_file: ca,
            plan: NativePlan {
                revision: 1,
                enabled: true,
                lease_millis: 5000,
                nodes,
            },
        }
    }

    #[tokio::test]
    async fn control_transport_rejects_plaintext_untrusted_certificates_and_wrong_names() {
        let root = tempfile::tempdir().unwrap();
        let tls = test_tls(root.path(), "127.0.0.1");
        let node = test_node(
            &tls,
            Router::new().route("/probe", get(|| async { Json(true) })),
        )
        .await;
        let mut config = test_config(
            root.path(),
            tls.certificate_file.clone(),
            vec![NativeNodePlan {
                node: node.origin.clone(),
                operation: None,
            }],
        );
        let api = NodeApi::new(&config).await.unwrap();
        let result: bool = api
            .request::<(), _>(&node.origin, "/probe", Method::GET, None)
            .await
            .unwrap();
        assert!(result);

        let plaintext = node.origin.replacen("https:", "http:", 1);
        assert!(
            api.request::<(), bool>(&plaintext, "/probe", Method::GET, None)
                .await
                .is_err()
        );
        config.plan.nodes[0].node = plaintext.clone();
        assert!(
            NodeApi::new(&config)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("HTTPS")
        );
        assert!(
            Client::builder()
                .no_proxy()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap()
                .get(plaintext)
                .send()
                .await
                .is_err(),
            "TLS listener accepted plaintext"
        );
        config.plan.nodes[0].node = node.origin.clone();

        let other = tempfile::tempdir().unwrap();
        config.ca_certificate_file = test_tls(other.path(), "127.0.0.1").certificate_file;
        let untrusted = NodeApi::new(&config).await.unwrap();
        assert!(
            untrusted
                .request::<(), bool>(&node.origin, "/probe", Method::GET, None)
                .await
                .is_err()
        );

        let wrong_name = test_tls(other.path(), "wrong.example");
        let wrong_node = test_node(
            &wrong_name,
            Router::new().route("/probe", get(|| async { Json(true) })),
        )
        .await;
        config.ca_certificate_file = wrong_name.certificate_file;
        config.plan.nodes[0].node = wrong_node.origin.clone();
        let wrong = NodeApi::new(&config).await.unwrap();
        assert!(
            wrong
                .request::<(), bool>(&wrong_node.origin, "/probe", Method::GET, None)
                .await
                .is_err()
        );

        std::fs::write(&config.ca_certificate_file, "").unwrap();
        assert!(NodeApi::new(&config).await.is_err());
    }

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
        let root = tempfile::tempdir().unwrap();
        let tls = test_tls(root.path(), "127.0.0.1");
        let mut servers = Vec::new();
        let mut nodes = Vec::new();
        for router in [healthy, unresponsive] {
            let server = test_node(&tls, router).await;
            nodes.push(NativeNodePlan {
                node: server.origin.clone(),
                operation: None,
            });
            servers.push(server);
        }
        let config = test_config(root.path(), tls.certificate_file, nodes);
        let result = tokio::time::timeout(Duration::from_millis(4500), reconcile_once(&config))
            .await
            .expect("unresponsive peer exhausted the renewal budget");
        assert!(result.is_err(), "unresponsive peer was silently credited");
        assert!(authorized.load(Ordering::SeqCst));
    }
}
