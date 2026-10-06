mod service;
mod state;

use std::sync::Arc;

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::get;

pub use service::PageService;

pub fn page_router(application: Arc<PageService>) -> Router {
    Router::new()
        .route("/page", get(get_page).put(put_page))
        .with_state(application)
}

async fn put_page(
    State(application): State<Arc<PageService>>,
    page: Bytes,
) -> Result<String, (StatusCode, String)> {
    application
        .replace_page(page)
        .await
        .map(|lsn| lsn.to_string())
        .map_err(runtime_http_error)
}

async fn get_page(
    State(application): State<Arc<PageService>>,
) -> Result<Bytes, (StatusCode, String)> {
    application
        .page()
        .await
        .map_err(runtime_http_error)?
        .ok_or((StatusCode::NOT_FOUND, "page has not been written".into()))
}

fn runtime_http_error(error: kuberic_runtime::RuntimeError) -> (StatusCode, String) {
    let status = match error {
        kuberic_runtime::RuntimeError::NotPrimary
        | kuberic_runtime::RuntimeError::NotOpen
        | kuberic_runtime::RuntimeError::WriteClosed(_)
        | kuberic_runtime::RuntimeError::ReadClosed(_) => StatusCode::SERVICE_UNAVAILABLE,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, error.to_string())
}
