use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use kuberic_native_runtime::native::{
    NativeApplication, NativeAuthority, NativeOperation, NativeOperationCommand,
};
use subtle::ConstantTimeEq;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::adapter::{AdapterConfig, RustfsAdapter};

#[derive(Clone)]
struct Control {
    adapter: Arc<RustfsAdapter>,
    token: Arc<str>,
    operations: Arc<Semaphore>,
}

impl Control {
    fn authenticate(&self, headers: &HeaderMap) -> bool {
        headers
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|value| bool::from(value.as_bytes().ct_eq(self.token.as_bytes())))
    }
}

fn failure(error: impl std::fmt::Display) -> Response {
    tracing::error!(%error, "RustFS control request failed");
    (StatusCode::CONFLICT, error.to_string()).into_response()
}

async fn observation(State(control): State<Control>, headers: HeaderMap) -> Response {
    if !control.authenticate(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match control.adapter.observation().await {
        Ok(observation) => Json(observation).into_response(),
        Err(error) => failure(error),
    }
}

async fn authority(
    State(control): State<Control>,
    headers: HeaderMap,
    Json(authority): Json<NativeAuthority>,
) -> Response {
    if !control.authenticate(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match control.adapter.authorize(&authority).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(error) => failure(error),
    }
}

async fn operation(
    State(control): State<Control>,
    headers: HeaderMap,
    Json(command): Json<NativeOperationCommand>,
) -> Response {
    if !control.authenticate(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    let Ok(permit) = control.operations.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    let task = tokio::spawn(async move {
        let _permit = permit;
        control.adapter.execute(&command).await
    });
    match task.await {
        Ok(Ok(status)) => Json(status).into_response(),
        Ok(Err(error)) => failure(error),
        Err(error) => failure(error),
    }
}

async fn readiness(State(control): State<Control>) -> StatusCode {
    match control.adapter.observation().await {
        Ok(observation)
            if observation.accepting_clients
                && matches!(&observation.health, kuberic_native_runtime::native::NativeHealthObservation::Observed { health } if health.ready) =>
        {
            StatusCode::OK
        }
        Ok(_) => StatusCode::SERVICE_UNAVAILABLE,
        Err(error) => {
            tracing::warn!(%error, "RustFS readiness observation failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn liveness(State(control): State<Control>) -> StatusCode {
    match control.adapter.check_process().await {
        Ok(()) => StatusCode::OK,
        Err(error) => {
            tracing::error!(%error, "Native process liveness failed");
            StatusCode::SERVICE_UNAVAILABLE
        }
    }
}

async fn operation_status(
    State(control): State<Control>,
    headers: HeaderMap,
    Json(operation): Json<NativeOperation>,
) -> Response {
    if !control.authenticate(&headers) {
        return StatusCode::UNAUTHORIZED.into_response();
    }
    match control.adapter.operation_status(&operation).await {
        Ok(status) => Json(status).into_response(),
        Err(error) => failure(error),
    }
}

async fn gateway(listener: TcpListener, adapter: Arc<RustfsAdapter>) -> Result<()> {
    let capacity = Arc::new(Semaphore::new(1024));
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (mut client, _) = accepted?;
                let Ok(capacity) = capacity.clone().try_acquire_owned() else {
                    tracing::warn!("RustFS client connection limit reached");
                    continue;
                };
                let mut authority = match adapter.admit().await {
                    Ok(permit) => permit,
                    Err(error) => {
                        tracing::debug!(%error, "RustFS client connection fenced");
                        continue;
                    }
                };
                let upstream = adapter.upstream();
                connections.spawn(async move {
                    let _capacity = capacity;
                    tokio::select! {
                        biased;
                        () = authority.revoked() => Ok(()),
                        result = async {
                            let mut native = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(upstream)).await??;
                            tokio::io::copy_bidirectional(&mut client, &mut native).await?;
                            Ok::<_, anyhow::Error>(())
                        } => result,
                    }
                });
            }
            completed = connections.join_next(), if !connections.is_empty() => {
                match completed {
                    Some(Ok(Ok(()))) => {}
                    Some(Ok(Err(error))) => tracing::warn!(%error, "RustFS client connection failed"),
                    Some(Err(error)) => return Err(error).context("RustFS gateway task failed"),
                    None => unreachable!(),
                }
            }
        }
    }
}

async fn monitor(adapter: Arc<RustfsAdapter>, operations: Arc<Semaphore>) -> Result<()> {
    loop {
        if let Ok(_permit) = operations.try_acquire()
            && let Some(pending) = adapter.pending().await?
        {
            match adapter.reconcile(&pending).await {
                Ok(status) => {
                    tracing::info!(operation = pending.id, ?status, "Native operation observed")
                }
                Err(error) => {
                    tracing::warn!(operation = pending.id, %error, "Native operation remains pending")
                }
            }
        }
        adapter.check_process().await?;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            value = term.recv() => anyhow::ensure!(value.is_some(), "termination signal stream closed"),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}

pub async fn serve(config: AdapterConfig) -> Result<()> {
    let token = crate::config::control_token(&config.control_token_file).await?;
    let tls = config.control_tls.load().await?;
    let client = TcpListener::bind(config.client_address).await?;
    let control_handle = axum_server::Handle::new();
    let control_listener = axum_server::from_tcp_rustls(
        TcpListener::bind(config.control_address)
            .await?
            .into_std()?,
        tls,
    )?
    .handle(control_handle.clone());
    let adapter = Arc::new(RustfsAdapter::start(&config).await?);
    let operations = Arc::new(Semaphore::new(1));
    let router = Router::new()
        .route("/v1/native/observation", get(observation))
        .route("/v1/native/authority", post(authority))
        .route("/v1/native/operation", post(operation))
        .route("/v1/native/operation/status", post(operation_status))
        .route("/ready", get(readiness))
        .route("/live", get(liveness))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .with_state(Control {
            adapter: adapter.clone(),
            token: Arc::from(token),
            operations: operations.clone(),
        });
    let result = tokio::select! {
        result = control_listener.serve(router.into_make_service()) => {
            match result {
                Ok(()) => Err(anyhow!("RustFS control listener stopped unexpectedly")),
                Err(error) => Err(error.into()),
            }
        },
        result = gateway(client, adapter.clone()) => result,
        result = monitor(adapter.clone(), operations) => result,
        result = shutdown_signal() => result,
    };
    control_handle.shutdown();
    let stopped = adapter.close().await;
    result?;
    stopped?;
    Ok(())
}
