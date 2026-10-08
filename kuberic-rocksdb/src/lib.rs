mod batch;
mod checkpoint;
mod service;
mod state;

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use kuberic_runtime::RuntimeError;
use serde::Serialize;

pub use batch::{BatchRequest, Mutation};
pub use service::RocksService;
pub use state::RocksState;

#[derive(Debug, Serialize)]
pub struct WriteReceipt {
    pub lsn: i64,
}

pub fn rocks_router(application: Arc<RocksService>) -> Router {
    Router::new()
        .route("/keys/{key}", get(get_key).put(put_key).delete(delete_key))
        .route("/batch", post(write_batch))
        .layer(DefaultBodyLimit::max(batch::MAX_HTTP_BYTES))
        .with_state(application)
}

async fn get_key(
    State(application): State<Arc<RocksService>>,
    Path(key): Path<String>,
) -> Result<Bytes, (StatusCode, String)> {
    application
        .get(key.into_bytes())
        .await
        .map_err(http_error)?
        .map(Bytes::from)
        .ok_or((StatusCode::NOT_FOUND, "key does not exist".into()))
}

async fn put_key(
    State(application): State<Arc<RocksService>>,
    Path(key): Path<String>,
    value: Bytes,
) -> Result<Json<WriteReceipt>, (StatusCode, String)> {
    submit(
        &application,
        BatchRequest {
            column_family: "default".into(),
            operations: vec![Mutation::Put {
                key: key.into_bytes(),
                value: value.to_vec(),
            }],
        },
    )
    .await
}

async fn delete_key(
    State(application): State<Arc<RocksService>>,
    Path(key): Path<String>,
) -> Result<Json<WriteReceipt>, (StatusCode, String)> {
    submit(
        &application,
        BatchRequest {
            column_family: "default".into(),
            operations: vec![Mutation::Delete {
                key: key.into_bytes(),
            }],
        },
    )
    .await
}

async fn write_batch(
    State(application): State<Arc<RocksService>>,
    Json(request): Json<BatchRequest>,
) -> Result<Json<WriteReceipt>, (StatusCode, String)> {
    submit(&application, request).await
}

async fn submit(
    application: &RocksService,
    request: BatchRequest,
) -> Result<Json<WriteReceipt>, (StatusCode, String)> {
    let bytes = batch::Envelope::encode(request)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    application
        .submit(bytes.into())
        .await
        .map(|lsn| Json(WriteReceipt { lsn }))
        .map_err(http_error)
}

fn http_error(error: RuntimeError) -> (StatusCode, String) {
    let status = match error {
        RuntimeError::NotPrimary
        | RuntimeError::NotOpen
        | RuntimeError::Closed
        | RuntimeError::WriteClosed(_)
        | RuntimeError::ReadClosed(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    tracing::warn!(%error, %status, "RocksDB client request failed");
    (status, error.to_string())
}
