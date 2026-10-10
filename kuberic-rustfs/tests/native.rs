use std::fs;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use kuberic_rustfs::{
    BinaryPin, CredentialFiles, HealthClient, HealthProbe, HealthStatus, LaunchConfig,
    RunningRustfs, Topology,
};

async fn await_readiness(process: &mut RunningRustfs, health: &HealthClient) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            tokio::select! {
                exit = process.wait() => {
                    exit?;
                    bail!("RustFS exited during startup");
                }
                probe = health.probe(HealthProbe::Readiness) => {
                    match probe {
                        Ok(HealthStatus::Healthy) => return Ok(()),
                        Ok(HealthStatus::Unhealthy) => eprintln!("RustFS not yet ready"),
                        Err(error) => eprintln!("RustFS startup probe: {error:#}"),
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .context("RustFS did not become ready before the startup deadline")?
}

#[tokio::test]
#[ignore = "requires a checksum-verified RustFS 1.0.1 executable; see README"]
async fn pinned_native_process_starts_stops_and_reopens_existing_storage() -> Result<()> {
    let binary =
        std::env::var_os("KUBERIC_RUSTFS_TEST_BINARY").context("set KUBERIC_RUSTFS_TEST_BINARY")?;
    let hash =
        std::env::var("KUBERIC_RUSTFS_TEST_SHA256").context("set KUBERIC_RUSTFS_TEST_SHA256")?;
    let root = tempfile::tempdir()?;
    let state = root.path().join("state");
    let data = root.path().join("data");
    fs::create_dir(&state)?;
    fs::create_dir(&data)?;
    let access = root.path().join("access");
    let secret = root.path().join("secret");
    fs::write(&access, "native-smoke-access")?;
    fs::write(
        &secret,
        format!(
            "native-smoke-{}",
            root.path().file_name().unwrap().to_string_lossy()
        ),
    )?;
    let socket = std::net::TcpListener::bind("127.0.0.1:0")?;
    let address = socket.local_addr()?;
    drop(socket);
    let config = LaunchConfig {
        binary: BinaryPin::new(binary.into(), &hash)?,
        credentials: CredentialFiles {
            access_key: access,
            secret_key: secret,
        },
        state_directory: state.clone(),
        topology: Topology {
            pools: vec![data.to_str().context("non-UTF8 temp path")?.into()],
            local_node: None,
            erasure_set_drive_count: None,
        },
        address,
        shutdown_grace: Duration::from_secs(3),
    };
    let health = HealthClient::new(&format!("http://{address}"), Duration::from_secs(2))?;
    let mut process = RunningRustfs::start(config.clone()).await?;
    let initial_ready = await_readiness(&mut process, &health).await;
    let initial_stop = process.shutdown().await;
    initial_ready?;
    initial_stop?;
    assert!(
        fs::read_dir(&data)?.next().is_some(),
        "native engine created no storage"
    );
    assert!(!state.join("process-active").exists());
    let record = fs::read(state.join("topology.json"))?;
    let mut process = RunningRustfs::start(config).await?;
    let reopened = await_readiness(&mut process, &health).await;
    let stopped = process.shutdown().await;
    reopened?;
    stopped?;
    assert_eq!(fs::read(state.join("topology.json"))?, record);
    assert!(!state.join("process-active").exists());
    Ok(())
}
