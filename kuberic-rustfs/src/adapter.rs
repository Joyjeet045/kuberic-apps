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

use crate::admin::{AdminClient, validate_decommission_receipt};
use crate::journal::Journal;
use crate::topology::{
    expansion_volumes, prepare_expansion, prepare_retirement, recorded_topology,
    validate_expansion, validate_retirement,
};
use crate::{
    BinaryPin, CredentialFiles, HealthClient, HealthProbe, HealthStatus, LaunchConfig,
    RunningRustfs, Topology,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AdapterConfig {
    pub binary: PathBuf,
    pub sha256: String,
    pub credentials: CredentialFiles,
    pub state_directory: PathBuf,
    pub topology: Topology,
    pub native_address: SocketAddr,
    pub client_address: SocketAddr,
    pub control_address: SocketAddr,
    pub control_token_file: PathBuf,
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
    Decommission {
        topology: Topology,
        pool: usize,
    },
    FinalizeDecommission {
        previous: Topology,
        target: Topology,
        decommission_id: String,
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
        let topology = journal.topology()?;
        let engine = root.join("engine");
        if !engine.try_exists()? {
            fs::create_dir(&engine)?;
        }
        prepare_topology(&engine, &journal, &topology)?;
        let launch = LaunchConfig {
            binary: BinaryPin::new(config.binary.clone(), &config.sha256)?,
            credentials: config.credentials.clone(),
            state_directory: engine,
            topology,
            address: config.native_address,
            shutdown_grace: Duration::from_secs(config.shutdown_grace_seconds),
        };
        let ip = match config.native_address.ip() {
            IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            ip => ip,
        };
        let upstream = SocketAddr::new(ip, config.native_address.port());
        let endpoint = format!("http://{upstream}");
        let health = HealthClient::new(&endpoint, Duration::from_secs(2))?;
        let admin = AdminClient::new(&endpoint, config.credentials.clone())?;
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
            TopologyOperation::FinalizeDecommission {
                previous,
                target,
                decommission_id,
            } => {
                let pool = completed_pool(&state.journal, previous, decommission_id)?;
                validate_retirement(previous, target, pool)?;
                ensure!(
                    desired == *previous || desired == *target,
                    "retirement source does not match the established topology"
                );
                Some(target)
            }
            TopologyOperation::Restart { topology }
            | TopologyOperation::Decommission { topology, .. } => {
                ensure!(
                    desired == *topology,
                    "operation topology does not match storage"
                );
                None
            }
        };
        if let TopologyOperation::Decommission { topology, pool } = &request {
            ensure!(
                topology.pools.len() > 1 && *pool < topology.pools.len(),
                "decommission requires an existing pool in a multi-pool topology"
            );
        }
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
        let evidence = match request {
            TopologyOperation::Decommission { pool, .. } => {
                let Some(evidence) = self.admin.decommission(pool).await? else {
                    return Ok(NativeOperationStatus::Pending);
                };
                evidence
            }
            TopologyOperation::Expand { .. }
            | TopologyOperation::Restart { .. }
            | TopologyOperation::FinalizeDecommission { .. } => {
                if let TopologyOperation::Expand { previous, .. } = &request
                    && pools[previous.pools.len()..]
                        .iter()
                        .any(|pool| pool.status != "active")
                {
                    return Ok(NativeOperationStatus::Pending);
                }
                if matches!(request, TopologyOperation::FinalizeDecommission { .. })
                    && pools.iter().any(|pool| pool.status != "active")
                {
                    return Ok(NativeOperationStatus::Pending);
                }
                if self.health.probe(HealthProbe::Readiness).await? != HealthStatus::Healthy {
                    return Ok(NativeOperationStatus::Pending);
                }
                json!({
                    "poolArguments": target.pools,
                    "poolStatus": pools.iter().map(|pool| &pool.status).collect::<Vec<_>>(),
                    "processId": state.process.as_ref().context("missing native process")?.id(),
                    "nodeReady": true
                })
            }
        };
        state.journal.complete(&operation.id, &evidence)?;
        Ok(NativeOperationStatus::Complete { evidence })
    }
}

fn completed_pool(journal: &Journal, previous: &Topology, id: &str) -> Result<usize> {
    let (operation, evidence) = journal.completed_operation(id)?;
    let TopologyOperation::Decommission { topology, pool } =
        serde_json::from_value(operation.request)?
    else {
        anyhow::bail!("retirement receipt is not for a decommission operation");
    };
    ensure!(
        topology == *previous,
        "retirement receipt is for a different topology"
    );
    validate_decommission_receipt(&evidence)?;
    ensure!(
        evidence.get("id").and_then(serde_json::Value::as_u64) == Some(pool as u64)
            && previous.pools.get(pool).is_some_and(|cmdline| {
                evidence.get("cmdline").and_then(serde_json::Value::as_str)
                    == Some(cmdline.as_str())
            }),
        "native retirement evidence does not identify the completed pool"
    );
    Ok(pool)
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
        TopologyOperation::FinalizeDecommission {
            previous,
            target: requested,
            decommission_id,
        } if previous == recorded && requested == *target => {
            let pool = completed_pool(journal, &previous, &decommission_id)?;
            prepare_retirement(directory, &previous, target, pool)
        }
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

    #[test]
    fn retirement_requires_matching_completion_and_recovers_without_deleting_storage() {
        let root = tempfile::tempdir().unwrap();
        let engine = root.path().join("engine");
        fs::create_dir(&engine).unwrap();
        for index in 1..=4 {
            fs::create_dir(root.path().join(format!("data{index}"))).unwrap();
        }
        let previous = Topology {
            pools: vec![
                format!("{}{{1...2}}", root.path().join("data").display()),
                format!("{}{{3...4}}", root.path().join("data").display()),
            ],
            local_node: None,
            erasure_set_drive_count: Some(2),
        };
        let target = Topology {
            pools: previous.pools[1..].to_vec(),
            ..previous.clone()
        };
        drop(TopologyLease::acquire(&engine, &previous).unwrap());
        let retired_data = root.path().join("data1").join("sentinel");
        let surviving_data = root.path().join("data3").join("sentinel");
        fs::write(&retired_data, b"retained").unwrap();
        fs::write(&surviving_data, b"surviving").unwrap();
        let path = root.path().join("adapter.sqlite");
        let mut journal = Journal::open(&path, &previous).unwrap();
        let decommission = NativeOperation {
            id: "drain".into(),
            request: serde_json::to_value(TopologyOperation::Decommission {
                topology: previous.clone(),
                pool: 0,
            })
            .unwrap(),
        };
        assert!(completed_pool(&journal, &previous, &decommission.id).is_err());
        journal.accept(&decommission, None).unwrap();
        assert!(completed_pool(&journal, &previous, &decommission.id).is_err());
        let evidence = json!({
            "id": 0, "cmdline": previous.pools[0],
            "status": "complete", "poolStatus": "decommissioned",
            "decommissionInfo": {
                "complete": true, "failed": false, "canceled": false,
                "objectsDecommissionedFailed": 0, "bytesDecommissionedFailed": 0
            }
        });
        journal.complete(&decommission.id, &evidence).unwrap();
        assert_eq!(
            completed_pool(&journal, &previous, &decommission.id).unwrap(),
            0
        );
        assert!(completed_pool(&journal, &target, &decommission.id).is_err());
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
        assert!(completed_pool(&journal, &previous, &restart.id).is_err());
        let malformed = NativeOperation {
            id: "malformed".into(),
            request: decommission.request.clone(),
        };
        journal.accept(&malformed, None).unwrap();
        journal
            .complete(&malformed.id, &json!({"status": "complete"}))
            .unwrap();
        assert!(completed_pool(&journal, &previous, &malformed.id).is_err());
        let wrong_pool = NativeOperation {
            id: "wrong-pool".into(),
            request: decommission.request.clone(),
        };
        let mut wrong_evidence = evidence.clone();
        wrong_evidence["cmdline"] = json!(previous.pools[1]);
        journal.accept(&wrong_pool, None).unwrap();
        journal.complete(&wrong_pool.id, &wrong_evidence).unwrap();
        assert!(completed_pool(&journal, &previous, &wrong_pool.id).is_err());
        let finalize = NativeOperation {
            id: "retire".into(),
            request: serde_json::to_value(TopologyOperation::FinalizeDecommission {
                previous: previous.clone(),
                target: target.clone(),
                decommission_id: decommission.id.clone(),
            })
            .unwrap(),
        };
        journal.accept(&finalize, Some(&target)).unwrap();
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
        assert_eq!(journal.pending().unwrap(), Some(finalize));
        assert_eq!(
            journal.lookup(&decommission).unwrap(),
            Some(NativeOperationStatus::Complete { evidence })
        );
        assert_eq!(fs::read(retired_data).unwrap(), b"retained");
        assert_eq!(fs::read(surviving_data).unwrap(), b"surviving");
    }
}
