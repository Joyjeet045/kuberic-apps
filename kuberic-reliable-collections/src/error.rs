use kuberic_runtime::{RuntimeError, protocol::types::AccessStatus};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("transaction conflict: {0}")]
    Conflict(String),
    #[error("invalid transaction: {0}")]
    Invalid(String),
    #[error("transaction expired")]
    Expired,
    #[error("transaction belongs to a stale replica generation")]
    StaleEpoch,
    #[error("runtime is not open")]
    NotOpen,
    #[error("primary write access is not granted")]
    NotPrimary,
    #[error("reads are closed with status {0:?}")]
    ReadClosed(AccessStatus),
    #[error("writes are closed with status {0:?}")]
    WriteClosed(AccessStatus),
    #[error("replica requires recovery")]
    RecoveryRequired,
    #[error("transaction resource limit exceeded")]
    ResourceExhausted,
    #[error("request identity was reused with different transaction data")]
    DuplicateRequest,
    #[error("transaction result requires quorum confirmation; retry the original transaction")]
    UnconfirmedCommit,
    #[error("storage: {0}")]
    Storage(#[from] std::io::Error),
    #[error("encoding: {0}")]
    Encoding(#[from] postcard::Error),
    #[error("runtime: {0}")]
    Runtime(#[from] RuntimeError),
}

impl Error {
    pub(crate) fn runtime(self) -> RuntimeError {
        match self {
            Self::Runtime(error) => error,
            error => RuntimeError::Application(error.to_string()),
        }
    }
}

pub(crate) fn conflict(message: &str) -> Error {
    Error::Conflict(message.to_owned())
}
