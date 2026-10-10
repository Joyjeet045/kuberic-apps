use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, ensure};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use kuberic_native_runtime::native::{
    NativeApplication, NativeAuthority, NativeOperation, NativeOperationStatus,
};
use kuberic_rustfs::adapter::{AdapterConfig, RustfsAdapter, TopologyOperation};
use kuberic_rustfs::{CredentialFiles, Topology};
use reqwest::{Client, Method};
use sha2::{Digest, Sha256};

fn address() -> Result<SocketAddr> {
    Ok(TcpListener::bind("127.0.0.1:0")?.local_addr()?)
}

async fn s3(
    client: &Client,
    endpoint: SocketAddr,
    method: Method,
    path: &str,
    body: &[u8],
) -> Result<Vec<u8>> {
    let url = format!("http://{endpoint}{path}");
    let identity = Credentials::new(
        "adapter-test",
        "adapter-test-secret-value",
        None,
        None,
        "test",
    )
    .into();
    let hash: String = Sha256::digest(body)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region("us-east-1")
        .name("s3")
        .time(SystemTime::now())
        .settings(SigningSettings::default())
        .build()?
        .into();
    let (instructions, _) = sign(
        SignableRequest::new(
            method.as_str(),
            &url,
            [("x-amz-content-sha256", hash.as_str())].into_iter(),
            SignableBody::Bytes(body),
        )?,
        &params,
    )?
    .into_parts();
    let mut request = client
        .request(method, url)
        .header("x-amz-content-sha256", hash)
        .body(body.to_vec());
    for (name, value) in instructions.headers() {
        request = request.header(name, value);
    }
    let response = request.send().await?;
    let status = response.status();
    let bytes = response.bytes().await?;
    ensure!(
        status.is_success(),
        "S3 returned {status}: {}",
        String::from_utf8_lossy(&bytes)
    );
    Ok(bytes.to_vec())
}

async fn ready(adapter: &RustfsAdapter) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            match adapter.observe().await {
                Ok(health) if health.ready && health.writable => return Ok(()),
                Ok(health) => eprintln!("native startup: {health:?}"),
                Err(error) => eprintln!("native startup: {error}"),
            }
            adapter.check_process().await?;
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .context("native readiness deadline exceeded")?
}

#[tokio::test]
#[ignore = "requires the pinned real RustFS executable"]
async fn native_adapter_fences_restarts_replays_and_retains_acknowledged_objects() -> Result<()> {
    let root = tempfile::tempdir()?;
    let state = root.path().join("state");
    let volume = root.path().join("data");
    fs::create_dir(&state)?;
    fs::create_dir(&volume)?;
    let credentials = CredentialFiles {
        access_key: root.path().join("access"),
        secret_key: root.path().join("secret"),
    };
    fs::write(&credentials.access_key, "adapter-test")?;
    fs::write(&credentials.secret_key, "adapter-test-secret-value")?;
    let config = AdapterConfig {
        binary: std::env::var_os("KUBERIC_RUSTFS_TEST_BINARY")
            .context("missing binary")?
            .into(),
        sha256: std::env::var("KUBERIC_RUSTFS_TEST_SHA256")?,
        credentials,
        state_directory: state,
        topology: Topology {
            pools: vec![volume.to_str().context("non-UTF8 test path")?.into()],
            local_node: None,
            erasure_set_drive_count: None,
        },
        native_address: address()?,
        client_address: address()?,
        control_address: address()?,
        control_token_file: root.path().join("token"),
        shutdown_grace_seconds: 3,
    };
    let client = Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .build()?;
    let adapter = RustfsAdapter::start(&config).await?;
    let checked = async {
        ready(&adapter).await?;
        ensure!(
            adapter.admit().await.is_err(),
            "fresh adapter accepted clients"
        );
        let observation = adapter.observation().await?;
        let authority = NativeAuthority {
            incarnation: observation.incarnation,
            revision: 1,
            enabled: true,
            lease_millis: 30_000,
        };
        adapter.authorize(&authority).await?;
        let mut permit = adapter.admit().await?;
        s3(
            &client,
            config.native_address,
            Method::PUT,
            "/adapter-data",
            &[],
        )
        .await?;
        let object = b"acknowledged before the native restart";
        s3(
            &client,
            config.native_address,
            Method::PUT,
            "/adapter-data/object",
            object,
        )
        .await?;
        let operation = NativeOperation {
            id: "restart-1".into(),
            request: serde_json::to_value(TopologyOperation::Restart {
                topology: config.topology.clone(),
            })?,
        };
        let receipt = tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                match adapter.reconcile(&operation).await {
                    Ok(complete @ NativeOperationStatus::Complete { .. }) => {
                        return Ok::<_, anyhow::Error>(complete);
                    }
                    Ok(NativeOperationStatus::Pending) => {}
                    Err(error) => eprintln!("restart pending: {error}"),
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        })
        .await??;
        tokio::time::timeout(Duration::from_secs(1), permit.revoked()).await?;
        ensure!(
            adapter.admit().await.is_err(),
            "restart reopened client authority"
        );
        ensure!(
            adapter.reconcile(&operation).await? == receipt,
            "operation replay changed evidence"
        );
        let conflict = NativeOperation {
            request: serde_json::json!({"kind": "unsupported"}),
            ..operation.clone()
        };
        ensure!(
            adapter.reconcile(&conflict).await.is_err(),
            "conflicting operation id was accepted"
        );
        ensure!(
            s3(
                &client,
                config.native_address,
                Method::GET,
                "/adapter-data/object",
                &[]
            )
            .await?
                == object,
            "acknowledged data changed"
        );
        Ok::<_, anyhow::Error>((authority, operation, receipt))
    }
    .await;
    let closed = adapter.close().await;
    let (old_authority, operation, receipt) = checked?;
    closed?;
    drop(adapter);
    let reopened = RustfsAdapter::start(&config).await?;
    let checked = async {
        ready(&reopened).await?;
        ensure!(
            reopened.authorize(&old_authority).await.is_err(),
            "previous process authority was accepted"
        );
        ensure!(
            reopened.reconcile(&operation).await? == receipt,
            "durable receipt lost after reopening"
        );
        ensure!(
            s3(
                &client,
                config.native_address,
                Method::GET,
                "/adapter-data/object",
                &[]
            )
            .await?
                == b"acknowledged before the native restart",
            "same-root reopen lost acknowledged data"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    let stopped = reopened.close().await;
    checked?;
    stopped?;
    Ok(())
}
