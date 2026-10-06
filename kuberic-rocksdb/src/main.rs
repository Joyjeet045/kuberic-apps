use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use clap::Parser;
use kuberic_rocksdb::{RocksService, RocksState, rocksdb_router};
use kuberic_runtime::host::KubernetesDnsResolver;
use kuberic_runtime::host::{ApplicationStorageState, ReplicaHost, ReplicaProcessConfig};
use kuberic_runtime::protocol::types::{PodUid, PvcUid, ReplicaId, ResourceUid};

#[derive(Debug, Parser)]
struct Config {
    #[arg(long, env = "KUBERIC_RESOURCE_UID")]
    resource_uid: String,
    #[arg(long, env = "KUBERIC_REPLICA_ID")]
    replica_id: i64,
    #[arg(long, env = "KUBERIC_POD_UID")]
    pod_uid: String,
    #[arg(long, env = "KUBERIC_PVC_UID")]
    pvc_uid: String,
    #[arg(long, env = "KUBERIC_POD_IP")]
    pod_ip: String,
    #[arg(long, env = "KUBERIC_SET_NAME")]
    set_name: String,
    #[arg(long, env = "KUBERIC_NAMESPACE")]
    namespace: String,
    #[arg(long, env = "KUBERIC_AGENT_BEARER_TOKEN")]
    bearer_token: String,
    #[arg(long, env = "KUBERIC_DATA_ROOT", default_value = "/var/lib/kuberic")]
    data_root: PathBuf,
    #[arg(long, env = "KUBERIC_CONTROL_ADDRESS", default_value = "0.0.0.0:50051")]
    control_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_REPLICATION_ADDRESS",
        default_value = "0.0.0.0:50052"
    )]
    replication_address: SocketAddr,
    #[arg(
        long,
        env = "KUBERIC_APPLICATION_ADDRESS",
        default_value = "0.0.0.0:8080"
    )]
    application_address: SocketAddr,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let config = Config::parse();
    if config.replica_id <= 0 {
        anyhow::bail!("KUBERIC_REPLICA_ID must be positive");
    }

    let application_root = config.data_root.join("application");
    let storage = if RocksState::is_fresh_empty(&application_root)? {
        ApplicationStorageState::FreshEmpty
    } else {
        ApplicationStorageState::Established
    };
    let state = Arc::new(RocksState::open(application_root)?);
    let application = Arc::new(RocksService::new(
        state,
        format!(
            "http://{}:{}",
            config.pod_ip,
            config.replication_address.port()
        ),
        format!(
            "http://{}:{}",
            config.pod_ip,
            config.application_address.port()
        ),
    ));
    let mut replica = ReplicaHost::new(
        ReplicaProcessConfig {
            resource_uid: ResourceUid::new(&config.resource_uid),
            replica_id: ReplicaId::new(config.replica_id),
            pod_uid: PodUid::new(&config.pod_uid),
            pvc_uid: PvcUid::new(&config.pvc_uid),
            data_root: config.data_root,
            control_address: config.control_address,
            replication_address: config.replication_address,
            bearer_token: config.bearer_token,
            rpc_deadline: Duration::from_secs(5),
            transport_window_capacity: 256,
        },
        application.clone(),
        storage,
        Arc::new(KubernetesDnsResolver::new(
            ResourceUid::new(&config.resource_uid),
            config.namespace,
        )),
    )
    .start()
    .await?;

    let listener = tokio::net::TcpListener::bind(config.application_address).await?;
    let shutdown = replica.shutdown_signal();
    let mut http = tokio::spawn(
        axum::serve(listener, rocksdb_router(application))
            .with_graceful_shutdown(async move {
                let mut shutdown = shutdown;
                let _ = shutdown.wait_for(|stopped| *stopped).await;
            })
            .into_future(),
    );

    tokio::select! {
        result = replica.wait() => result?,
        result = &mut http => result??,
        result = tokio::signal::ctrl_c() => result?,
    }
    replica.shutdown();
    Ok(())
}
