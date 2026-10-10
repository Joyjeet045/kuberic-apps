use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use axum::Router;
use axum::http::{Method, StatusCode, header};
use axum::routing::get;
use kuberic_rustfs::{HealthClient, HealthProbe, HealthStatus};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

const TIMEOUT: Duration = Duration::from_secs(2);
const PROBES: [HealthProbe; 4] = [
    HealthProbe::Liveness,
    HealthProbe::Readiness,
    HealthProbe::ClusterRead,
    HealthProbe::ClusterWrite,
];

struct Server {
    endpoint: String,
    task: JoinHandle<()>,
}

impl Server {
    async fn start(router: Router) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self { endpoint, task }
    }

    fn client(&self) -> HealthClient {
        HealthClient::new(&self.endpoint, TIMEOUT).unwrap()
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[test]
fn accepts_http_and_https_origins_including_ipv6() {
    for endpoint in [
        "http://localhost:9000",
        "http://rustfs-0.example:9000/",
        "https://rustfs-0.example",
        "http://[::1]:9000",
    ] {
        HealthClient::new(endpoint, TIMEOUT).unwrap();
    }
}

#[test]
fn rejects_invalid_or_ambiguous_endpoints_and_invalid_timeouts() {
    for endpoint in [
        "",
        "localhost:9000",
        "ftp://localhost:9000",
        "file:///data",
        "http://localhost:0",
        "http://localhost:invalid",
        "http://localhost:9000/health",
        "http://localhost:9000/?probe=ready",
        "http://localhost:9000/#ready",
        "http://user@localhost:9000",
        "http://user:secret@localhost:9000",
    ] {
        assert!(
            HealthClient::new(endpoint, TIMEOUT).is_err(),
            "accepted {endpoint}"
        );
    }
    let error = HealthClient::new("http://localhost:9000", Duration::ZERO).unwrap_err();
    assert!(error.to_string().contains("timeout must be positive"));
    let error = HealthClient::new("http://localhost:9000", Duration::MAX).unwrap_err();
    assert!(error.to_string().contains("deadline range"));
}

#[tokio::test]
async fn requests_the_four_native_paths_with_get() {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let server = Server::start(Router::new().fallback(
        move |method: Method, uri: axum::http::Uri| {
            let tx = tx.clone();
            async move {
                tx.send((method, uri.to_string())).unwrap();
                StatusCode::OK
            }
        },
    ))
    .await;
    let client = server.client();
    for (probe, expected_path) in [
        (HealthProbe::Liveness, "/health/live"),
        (HealthProbe::Readiness, "/health/ready"),
        (HealthProbe::ClusterRead, "/minio/health/cluster/read"),
        (HealthProbe::ClusterWrite, "/minio/health/cluster"),
    ] {
        assert_eq!(client.probe(probe).await.unwrap(), HealthStatus::Healthy);
        assert_eq!(
            rx.try_recv().unwrap(),
            (Method::GET, expected_path.to_owned())
        );
    }
}

#[tokio::test]
async fn liveness_and_read_health_do_not_imply_ready_or_writable() {
    let server = Server::start(
        Router::new()
            .route("/health/live", get(|| async { StatusCode::OK }))
            .route(
                "/health/ready",
                get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            )
            .route(
                "/minio/health/cluster/read",
                get(|| async { StatusCode::OK }),
            )
            .route(
                "/minio/health/cluster",
                get(|| async { StatusCode::SERVICE_UNAVAILABLE }),
            ),
    )
    .await;
    let client = server.client();
    for (probe, expected) in [
        (HealthProbe::Liveness, HealthStatus::Healthy),
        (HealthProbe::Readiness, HealthStatus::Unhealthy),
        (HealthProbe::ClusterRead, HealthStatus::Healthy),
        (HealthProbe::ClusterWrite, HealthStatus::Unhealthy),
    ] {
        assert_eq!(client.probe(probe).await.unwrap(), expected);
    }
}

#[tokio::test]
async fn preserves_native_unhealthy_verdicts_and_rejects_unexpected_statuses() {
    for status in [201, 204, 301, 302, 307, 401, 403, 404, 429, 500, 503] {
        let status = StatusCode::from_u16(status).unwrap();
        let server = Server::start(Router::new().fallback(move || async move { status })).await;
        let client = server.client();
        for probe in PROBES {
            let result = client.probe(probe).await;
            if status == StatusCode::SERVICE_UNAVAILABLE {
                assert_eq!(result.unwrap(), HealthStatus::Unhealthy);
            } else {
                let error = result.unwrap_err().to_string();
                assert!(error.contains(status.as_str()), "{error}");
                assert!(error.contains(&format!("{probe:?}")), "{error}");
            }
        }
    }
}

#[tokio::test]
async fn does_not_follow_redirects_to_a_healthy_endpoint() {
    let redirected = Arc::new(AtomicUsize::new(0));
    let calls = redirected.clone();
    let server = Server::start(
        Router::new()
            .route(
                "/health/live",
                get(|| async { (StatusCode::FOUND, [(header::LOCATION, "/healthy")]) }),
            )
            .route(
                "/healthy",
                get(move || async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    StatusCode::OK
                }),
            ),
    )
    .await;
    let error = server
        .client()
        .probe(HealthProbe::Liveness)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("302"));
    assert_eq!(redirected.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn performs_a_fresh_probe_without_caching_success() {
    let calls = Arc::new(AtomicUsize::new(0));
    let server = Server::start(Router::new().route(
        "/health/ready",
        get(move || {
            let calls = calls.clone();
            async move {
                if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                    StatusCode::OK
                } else {
                    StatusCode::SERVICE_UNAVAILABLE
                }
            }
        }),
    ))
    .await;
    let client = server.client();
    assert_eq!(
        client.probe(HealthProbe::Readiness).await.unwrap(),
        HealthStatus::Healthy
    );
    assert_eq!(
        client.probe(HealthProbe::Readiness).await.unwrap(),
        HealthStatus::Unhealthy
    );
}

#[tokio::test]
async fn peer_disconnect_is_an_error_not_a_health_verdict() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        assert!(stream.read(&mut request).await.unwrap() > 0);
        stream.shutdown().await.unwrap();
    });
    let error = HealthClient::new(&endpoint, TIMEOUT)
        .unwrap()
        .probe(HealthProbe::ClusterWrite)
        .await
        .unwrap_err();
    server.await.unwrap();
    assert!(error.to_string().contains("ClusterWrite"));
    assert!(error.to_string().contains(&endpoint));
    assert!(!error.downcast_ref::<reqwest::Error>().unwrap().is_timeout());
}

#[tokio::test]
async fn unresponsive_peer_is_bounded_by_the_configured_timeout() {
    let server = Server::start(
        Router::new().fallback(|| async { std::future::pending::<StatusCode>().await }),
    )
    .await;
    let client = HealthClient::new(&server.endpoint, Duration::from_millis(100)).unwrap();
    let error = tokio::time::timeout(TIMEOUT, client.probe(HealthProbe::Readiness))
        .await
        .expect("probe exceeded its deadline")
        .unwrap_err();
    assert!(error.to_string().contains("Readiness"));
    assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_timeout());
}
