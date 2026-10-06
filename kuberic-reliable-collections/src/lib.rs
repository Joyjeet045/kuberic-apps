#![doc = include_str!("../README.md")]

mod dictionary;
mod error;
pub mod log;
mod service;
mod state;

pub use dictionary::{
    ReliableDictionary, ReliableValue, StateManager, Transaction, TransactionId, TransactionOptions,
};
pub use error::{Error, Result};
pub use service::ReliableCollectionsService;
pub use state::{CommitVersion, ReliableCollectionsProvider, ReliableCollectionsState};
