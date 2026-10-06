mod persistence;
mod service;
mod state;

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{post, put};
use serde::Deserialize;

pub use persistence::{
    COPY_CHUNK_SIZE, MAX_COPY, MAX_ENVELOPE, MAX_MUTATIONS, Mutation, RocksState,
};
pub use service::RocksService;
pub use state::RocksStateProvider;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum BatchMutation {
    Put { key: String, value: String },
    Delete { key: String },
    Merge { key: String, value: String },
}

pub fn rocksdb_router(application: Arc<RocksService>) -> Router {
    Router::new()
        .route("/keys/{key}", put(put_key).get(get_key).delete(delete_key))
        .route("/batch", post(post_batch))
        .with_state(application)
}

async fn put_key(
    State(application): State<Arc<RocksService>>,
    Path(key): Path<String>,
    value: Bytes,
) -> std::result::Result<String, (StatusCode, String)> {
    application
        .write(vec![Mutation::Put {
            key: key.into_bytes(),
            value: value.to_vec(),
        }])
        .await
        .map(|lsn| lsn.to_string())
        .map_err(runtime_http_error)
}

async fn delete_key(
    State(application): State<Arc<RocksService>>,
    Path(key): Path<String>,
) -> std::result::Result<String, (StatusCode, String)> {
    application
        .write(vec![Mutation::Delete {
            key: key.into_bytes(),
        }])
        .await
        .map(|lsn| lsn.to_string())
        .map_err(runtime_http_error)
}

async fn get_key(
    State(application): State<Arc<RocksService>>,
    Path(key): Path<String>,
) -> std::result::Result<Bytes, (StatusCode, String)> {
    application
        .get(key.as_bytes())
        .await
        .map_err(runtime_http_error)?
        .ok_or((StatusCode::NOT_FOUND, "key was not found".into()))
}

async fn post_batch(
    State(application): State<Arc<RocksService>>,
    Json(batch): Json<Vec<BatchMutation>>,
) -> std::result::Result<String, (StatusCode, String)> {
    let mutations = batch
        .into_iter()
        .map(|mutation| match mutation {
            BatchMutation::Put { key, value } => Mutation::Put {
                key: key.into_bytes(),
                value: value.into_bytes(),
            },
            BatchMutation::Delete { key } => Mutation::Delete {
                key: key.into_bytes(),
            },
            BatchMutation::Merge { key, value } => Mutation::Merge {
                key: key.into_bytes(),
                value: value.into_bytes(),
            },
        })
        .collect();
    application
        .write(mutations)
        .await
        .map(|lsn| lsn.to_string())
        .map_err(runtime_http_error)
}

fn runtime_http_error(error: kuberic_runtime::RuntimeError) -> (StatusCode, String) {
    let status = match error {
        kuberic_runtime::RuntimeError::NotPrimary
        | kuberic_runtime::RuntimeError::NotOpen
        | kuberic_runtime::RuntimeError::WriteClosed(_)
        | kuberic_runtime::RuntimeError::ReadClosed(_) => StatusCode::SERVICE_UNAVAILABLE,
        kuberic_runtime::RuntimeError::Application(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string())
}
