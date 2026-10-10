use std::fs::{self, File, OpenOptions};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, ensure};
use async_trait::async_trait;
use kuberic_native_runtime::native::{
    NativeApplication, NativeAuthority, NativeError, NativeGate, NativeHealth, NativeObservation,
    NativeOperation, NativeOperationCommand, NativeOperationStatus, NativePermit,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::Mutex;

use crate::admin::AdminClient;
use crate::journal::Journal;
use crate::topology::{
    expansion_volumes, prepare_expansion, recorded_topology, validate_expansion,
};
use crate::{
    BinaryPin, ControlTlsConfig, CredentialFiles, HealthClient, HealthProbe, HealthStatus,
    LaunchConfig, NativeTlsConfig, RunningRustfs, Topology,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub binary: PathBuf,
    pub sha256: String,
    pub credentials: CredentialFiles,
    pub state_directory: PathBuf,
    pub topology: Topology,
    pub native_tls: Option<NativeTlsConfig>,
    pub native_address: SocketAddr,
    pub client_address: SocketAddr,
    pub control_address: SocketAddr,
    pub control_token_file: PathBuf,
    pub control_tls: ControlTlsConfig,
    pub shutdown_grace_seconds: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase", deny_unknown_fields)]
pub enum TopologyOperation {
    Restart {
        topology: Topology,
    },
    Expand {
        previous: Topology,
        target: Topology,
    },
}

struct State {
    config: LaunchConfig,
    process: Option<RunningRustfs>,
    journal: Journal,
    gate: NativeGate,
    closing: bool,
    _lock: File,
}

pub struct RustfsAdapter {
    state: Mutex<State>,
    health: HealthClient,
    admin: AdminClient,
    upstream: SocketAddr,
}

fn native_error(error: anyhow::Error) -> NativeError {
    NativeError::Application(format!("{error:#}"))
}

impl RustfsAdapter {
    pub async fn start(config: &AdapterConfig) -> Result<Self> {
        ensure!(
            config.native_tls.is_some() || config.client_address.ip().is_loopback(),
            "native TLS is required for a non-loopback client gateway"
        );
        ensure!(
            config.client_address.port() != 0
                && config.control_address.port() != 0
                && config.client_address.port() != config.native_address.port()
                && config.control_address.port() != config.native_address.port()
                && config.control_address.port() != config.client_address.port(),
            "native, client, and control listener ports must be nonzero and distinct"
        );
        ensure!(
            config.state_directory.is_absolute()
                && fs::symlink_metadata(&config.state_directory)?.is_dir(),
            "adapter state directory must exist, be absolute, and not be a symlink"
        );
        let root = fs::canonicalize(&config.state_directory)?;
        let mut launch = LaunchConfig {
            binary: BinaryPin::new(config.binary.clone(), &config.sha256)?,
            credentials: config.credentials.clone(),
            state_directory: root.join("engine"),
            topology: config.topology.clone(),
            native_tls: config.native_tls.clone(),
            address: config.native_address,
            shutdown_grace: Duration::from_secs(config.shutdown_grace_seconds),
        };
        launch.validate()?;
        let lock_path = root.join("adapter.lock");
        if lock_path.try_exists()? {
            ensure!(
                fs::symlink_metadata(&lock_path)?.is_file(),
                "invalid adapter lock"
            );
        }
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock).context("adapter state already owned")?;
        let journal_path = root.join("adapter.sqlite");
        if journal_path.try_exists()? {
            ensure!(
                fs::symlink_metadata(&journal_path)?.is_file(),
                "invalid adapter journal"
            );
        }
        let journal = Journal::open(&journal_path, &config.topology)?;
        if let Some(pending) = journal.pending()? {
            serde_json::from_value::<TopologyOperation>(pending.request)
                .context("journal contains an unsupported pending topology operation")?;
        }
        let topology = journal.topology()?;
        let engine = root.join("engine");
        if !engine.try_exists()? {
            fs::create_dir(&engine)?;
        }
        prepare_topology(&engine, &journal, &topology)?;
        launch.topology = topology;
        let ip = match config.native_address.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        let upstream = SocketAddr::new(ip, config.native_address.port());
        let ca = config
            .native_tls
            .as_ref()
            .map(NativeTlsConfig::ca_bundle)
            .transpose()?;
        let scheme = if ca.is_some() { "https" } else { "http" };
        let endpoint = format!("{scheme}://{upstream}");
        let health = match &ca {
            Some(ca) => HealthClient::with_ca_bundle(&endpoint, Duration::from_secs(2), ca)?,
            None => HealthClient::new(&endpoint, Duration::from_secs(2))?,
        };
        let admin = AdminClient::new(&endpoint, config.credentials.clone(), ca.as_deref())?;
        let process = RunningRustfs::start(launch.clone()).await?;
        Ok(Self {
            state: Mutex::new(State {
                config: launch,
                process: Some(process),
                journal,
                gate: NativeGate::new(),
                closing: false,
                _lock: lock,
            }),
            health,
            admin,
            upstream,
        })
    }

    pub fn upstream(&self) -> SocketAddr {
        self.upstream
    }

    pub async fn admit(&self) -> Result<NativePermit, NativeError> {
        self.state.lock().await.gate.admit()
    }

    pub async fn authorize(&self, authority: &NativeAuthority) -> Result<(), NativeError> {
        let mut state = self.state.lock().await;
        if state.closing {
            return Err(NativeError::Closed);
        }
        if authority.enabled {
            if state.journal.pending().map_err(native_error)?.is_some() {
                return Err(NativeError::Application(
                    "native operation is still pending".into(),
                ));
            }
            ensure_process(&mut state).map_err(native_error)?;
        }
        state.gate.authorize(authority)
    }

    pub async fn observation(&self) -> Result<NativeObservation, NativeError> {
        let health = self.observe().await;
        if let Err(error) = &health {
            tracing::warn!(%error, "Native health observation unavailable");
        }
        Ok(self.state.lock().await.gate.observation(health))
    }

    pub async fn pending(&self) -> Result<Option<NativeOperation>> {
        self.state.lock().await.journal.pending()
    }

    pub async fn check_process(&self) -> Result<()> {
        ensure_process(&mut *self.state.lock().await)
    }

    pub async fn operation_status(
        &self,
        operation: &NativeOperation,
    ) -> Result<Option<NativeOperationStatus>> {
        self.state.lock().await.journal.lookup(operation)
    }

    pub async fn execute(&self, command: &NativeOperationCommand) -> Result<NativeOperationStatus> {
        ensure!(
            !command.authority.enabled,
            "topology operations require closed client authority"
        );
        self.reconcile_operation(&command.operation, Some(&command.authority))
            .await
    }

    async fn reconcile_operation(
        &self,
        operation: &NativeOperation,
        authority: Option<&NativeAuthority>,
    ) -> Result<NativeOperationStatus> {
        let request: TopologyOperation = serde_json::from_value(operation.request.clone())
            .context("invalid or unsupported native topology operation")?;
        let mut state = self.state.lock().await;
        ensure!(!state.closing, "native adapter is closing");
        if let Some(authority) = authority {
            state.gate.authorize(authority)?;
        }
        if let Some(complete @ NativeOperationStatus::Complete { .. }) =
            state.journal.lookup(operation)?
        {
            return Ok(complete);
        }
        let desired = state.journal.topology()?;
        let new_topology = match &request {
            TopologyOperation::Expand { previous, target } => {
                validate_expansion(previous, target)?;
                ensure!(
                    desired == *previous || desired == *target,
                    "expansion source does not match the established topology"
                );
                if desired == *previous && state.journal.lookup(operation)?.is_none() {
                    expansion_volumes(&state.config.state_directory, previous, target)?;
                }
                Some(target)
            }
            TopologyOperation::Restart { topology } => {
                ensure!(
                    desired == *topology,
                    "operation topology does not match storage"
                );
                None
            }
        };
        if let complete @ NativeOperationStatus::Complete { .. } =
            state.journal.accept(operation, new_topology)?
        {
            return Ok(complete);
        }
        state.gate.close();
        let target = state.journal.topology()?;
        let must_restart = state.config.topology != target
            || (matches!(request, TopologyOperation::Restart { .. })
                && !state.journal.launched(&operation.id)?);
        if must_restart && let Some(process) = &mut state.process {
            process.shutdown().await?;
            state.process = None;
        }
        if state.config.topology != target {
            prepare_topology(&state.config.state_directory, &state.journal, &target)?;
            state.config.topology = target.clone();
        }
        if state.process.is_none() {
            state.process = Some(RunningRustfs::start(state.config.clone()).await?);
        }
        state.journal.record_launch(&operation.id)?;
        ensure_process(&mut state)?;
        let pools = self.admin.pools().await?;
        ensure!(
            pools.len() == target.pools.len()
                && pools.iter().enumerate().all(|(index, pool)| {
                    pool.id == index && pool.cmdline == target.pools[index]
                }),
            "native pool identity does not yet match the requested ordered topology"
        );
        if let TopologyOperation::Expand { previous, .. } = &request
            && pools[previous.pools.len()..]
                .iter()
                .any(|pool| pool.status != "active")
        {
            return Ok(NativeOperationStatus::Pending);
        }
        if self.health.probe(HealthProbe::Readiness).await? != HealthStatus::Healthy {
            return Ok(NativeOperationStatus::Pending);
        }
        let evidence = json!({
            "poolArguments": target.pools,
            "poolStatus": pools.iter().map(|pool| &pool.status).collect::<Vec<_>>(),
            "processId": state.process.as_ref().context("missing native process")?.id(),
            "nodeReady": true
        });
        state.journal.complete(&operation.id, &evidence)?;
        Ok(NativeOperationStatus::Complete { evidence })
    }
}

fn prepare_topology(directory: &Path, journal: &Journal, target: &Topology) -> Result<()> {
    let Some(recorded) = recorded_topology(directory)?.filter(|old| old != target) else {
        return Ok(());
    };
    let operation = journal
        .pending()?
        .context("topology change is missing its durable operation")?;
    match serde_json::from_value::<TopologyOperation>(operation.request)? {
        TopologyOperation::Expand {
            previous,
            target: requested,
        } if previous == recorded && requested == *target => prepare_expansion(directory, target),
        _ => anyhow::bail!("recorded topology does not match the durable transition"),
    }
}

fn ensure_process(state: &mut State) -> Result<()> {
    let result = match state.process.as_mut() {
        Some(process) => match process.observed_exit() {
            Ok(None) => Ok(()),
            Ok(Some(report)) => Err(anyhow!("RustFS is stopped: {}", report.status)),
            Err(error) => Err(error),
        },
        None => Err(anyhow!("RustFS is not running")),
    };
    if result.is_err() {
        state.gate.close();
    }
    result
}

#[async_trait]
impl NativeApplication for RustfsAdapter {
    async fn observe(&self) -> Result<NativeHealth, NativeError> {
        ensure_process(&mut *self.state.lock().await).map_err(native_error)?;
        let (live, ready, readable, writable) = tokio::try_join!(
            self.health.probe(HealthProbe::Liveness),
            self.health.probe(HealthProbe::Readiness),
            self.health.probe(HealthProbe::ClusterRead),
            self.health.probe(HealthProbe::ClusterWrite),
        )
        .map_err(native_error)?;
        Ok(NativeHealth {
            live: live == HealthStatus::Healthy,
            ready: ready == HealthStatus::Healthy,
            readable: readable == HealthStatus::Healthy,
            writable: writable == HealthStatus::Healthy,
        })
    }

    async fn reconcile(
        &self,
        operation: &NativeOperation,
    ) -> Result<NativeOperationStatus, NativeError> {
        self.reconcile_operation(operation, None)
            .await
            .map_err(native_error)
    }

    async fn close(&self) -> Result<(), NativeError> {
        let mut state = self.state.lock().await;
        state.closing = true;
        state.gate.close();
        if let Some(process) = &mut state.process {
            process.shutdown().await.map_err(native_error)?;
            state.process = None;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::topology::TopologyLease;

    #[tokio::test]
    async fn invalid_bootstrap_does_not_poison_the_adapter_journal() {
        let root = tempfile::tempdir().unwrap();
        let config = AdapterConfig {
            binary: std::env::current_exe().unwrap(),
            sha256: "0".repeat(64),
            credentials: CredentialFiles {
                access_key: root.path().join("access"),
                secret_key: root.path().join("secret"),
            },
            state_directory: root.path().to_owned(),
            topology: Topology {
                pools: Vec::new(),
                local_node: None,
                erasure_set_drive_count: None,
            },
            native_tls: None,
            native_address: "127.0.0.1:9000".parse().unwrap(),
            client_address: "127.0.0.1:9002".parse().unwrap(),
            control_address: "127.0.0.1:9003".parse().unwrap(),
            control_token_file: root.path().join("token"),
            control_tls: ControlTlsConfig {
                certificate_file: root.path().join("tls.crt"),
                private_key_file: root.path().join("tls.key"),
            },
            shutdown_grace_seconds: 3,
        };
        assert!(RustfsAdapter::start(&config).await.is_err());
        assert!(!root.path().join("adapter.sqlite").exists());
        assert!(!root.path().join("engine").exists());
    }

    #[test]
    fn expansion_recovers_accepted_intent_without_deleting_storage() {
        let root = tempfile::tempdir().unwrap();
        let engine = root.path().join("engine");
        fs::create_dir(&engine).unwrap();
        for index in 1..=4 {
            fs::create_dir(root.path().join(format!("data{index}"))).unwrap();
        }
        let previous = Topology {
            pools: vec![format!("{}{{1...2}}", root.path().join("data").display())],
            local_node: None,
            erasure_set_drive_count: Some(2),
        };
        let target = Topology {
            pools: vec![
                previous.pools[0].clone(),
                format!("{}{{3...4}}", root.path().join("data").display()),
            ],
            ..previous.clone()
        };
        drop(TopologyLease::acquire(&engine, &previous).unwrap());
        let data = root.path().join("data1").join("sentinel");
        fs::write(&data, b"retained").unwrap();
        let path = root.path().join("adapter.sqlite");
        let mut journal = Journal::open(&path, &previous).unwrap();
        let restart = NativeOperation {
            id: "restart".into(),
            request: serde_json::to_value(TopologyOperation::Restart {
                topology: previous.clone(),
            })
            .unwrap(),
        };
        journal.accept(&restart, None).unwrap();
        journal
            .complete(&restart.id, &json!({"nodeReady": true}))
            .unwrap();
        assert!(prepare_topology(&engine, &journal, &target).is_err());
        let expand = NativeOperation {
            id: "expand".into(),
            request: serde_json::to_value(TopologyOperation::Expand {
                previous: previous.clone(),
                target: target.clone(),
            })
            .unwrap(),
        };
        journal.accept(&expand, Some(&target)).unwrap();
        let lease = TopologyLease::acquire(&engine, &previous).unwrap();
        assert!(prepare_topology(&engine, &journal, &target).is_err());
        assert_eq!(recorded_topology(&engine).unwrap(), Some(previous.clone()));
        drop(lease);
        drop(journal);
        let journal = Journal::open(&path, &previous).unwrap();
        prepare_topology(&engine, &journal, &target).unwrap();
        prepare_topology(&engine, &journal, &target).unwrap();
        assert_eq!(recorded_topology(&engine).unwrap(), Some(target.clone()));
        drop(TopologyLease::acquire(&engine, &target).unwrap());
        assert_eq!(journal.pending().unwrap(), Some(expand));
        assert_eq!(
            journal.lookup(&restart).unwrap(),
            Some(NativeOperationStatus::Complete {
                evidence: json!({"nodeReady": true})
            })
        );
        assert_eq!(fs::read(data).unwrap(), b"retained");
        assert!(
            fs::read_dir(root.path().join("data3"))
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn unsupported_pool_removal_operations_are_rejected() {
        for kind in ["decommission", "finalizeDecommission", "shrink"] {
            assert!(serde_json::from_value::<TopologyOperation>(json!({"kind": kind})).is_err());
        }
    }
}
