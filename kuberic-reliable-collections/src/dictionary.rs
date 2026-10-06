use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::AccessStatus;
use kuberic_runtime::replicator::{StateReplicator, StatefulServicePartition};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::sync::{OwnedSemaphorePermit, RwLock, Semaphore};

use crate::error::{Error, Result, conflict};
use crate::state::{
    CommitVersion, MAX_TRANSACTION_BYTES, PreparedCommit, ReliableCollectionsState,
};

pub trait ReliableValue: Serialize + DeserializeOwned {
    const TYPE_ID: &'static str;
    const VERSION: u32 = 1;
}

impl ReliableValue for String {
    const TYPE_ID: &'static str = "string/utf8";
}

impl ReliableValue for Vec<u8> {
    const TYPE_ID: &'static str = "bytes";
}

impl ReliableValue for i64 {
    const TYPE_ID: &'static str = "i64";
}

impl ReliableValue for u64 {
    const TYPE_ID: &'static str = "u64";
}

impl ReliableValue for bool {
    const TYPE_ID: &'static str = "bool";
}

#[derive(Clone, Copy, Debug)]
pub enum IsolationLevel {
    OptimisticSerializable,
}

#[derive(Clone, Copy, Debug)]
pub struct TransactionOptions {
    pub timeout: Duration,
    pub isolation: IsolationLevel,
}

impl Default for TransactionOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            isolation: IsolationLevel::OptimisticSerializable,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionId {
    pub transaction: u128,
    pub request: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Value {
    version: i64,
    bytes: Option<Vec<u8>>,
}

#[derive(Clone, Serialize, Deserialize)]
struct Provider {
    id: u128,
    key_type: String,
    key_version: u32,
    value_type: String,
    value_version: u32,
    revision: i64,
    entries: BTreeMap<Vec<u8>, Value>,
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub(crate) struct Registry {
    revision: i64,
    providers: BTreeMap<String, Provider>,
}

impl Registry {
    pub(crate) const FORMAT_ID: &'static str = "kuberic-reliable-dictionary-registry/2";

    fn key_version(&self, provider: &str, key: &[u8]) -> i64 {
        self.providers
            .get(provider)
            .and_then(|provider| provider.entries.get(key))
            .map_or(0, |entry| entry.version)
    }

    fn provider_mut(&mut self, name: &str, id: u128) -> Result<&mut Provider> {
        self.providers
            .get_mut(name)
            .filter(|provider| provider.id == id)
            .ok_or_else(|| conflict("provider removed or recreated"))
    }

    pub(crate) fn validate(&self, observed: &Observations) -> Result<()> {
        if observed
            .registry
            .is_some_and(|revision| revision != self.revision)
        {
            return Err(conflict("provider enumeration changed"));
        }
        for (name, identity) in &observed.providers {
            if self.providers.get(name).map(|provider| provider.id) != *identity {
                return Err(conflict("provider registry changed"));
            }
        }
        for ((name, key), version) in &observed.keys {
            if self.key_version(name, key) != *version {
                return Err(conflict("observed key changed"));
            }
        }
        for (name, revision) in &observed.scans {
            if self
                .providers
                .get(name)
                .map_or(0, |provider| provider.revision)
                != *revision
            {
                return Err(conflict("enumerated dictionary changed"));
            }
        }
        Ok(())
    }

    pub(crate) fn validate_snapshot(&self, version: CommitVersion) -> Result<()> {
        if self.providers.len() > 1024 || self.revision < 0 || self.revision > version.0 {
            return Err(Error::Invalid("invalid registry snapshot".into()));
        }
        for (name, provider) in &self.providers {
            if name.is_empty()
                || name.len() > 128
                || provider.key_type.is_empty()
                || provider.key_type.len() > 128
                || provider.value_type.is_empty()
                || provider.value_type.len() > 128
                || provider.revision < 0
                || provider.revision > version.0
                || provider.entries.iter().any(|(key, value)| {
                    key.len() > 64 * 1024 || value.version < 0 || value.version > provider.revision
                })
            {
                return Err(Error::Invalid("invalid dictionary snapshot".into()));
            }
        }
        Ok(())
    }

    pub(crate) fn apply(&mut self, command: &[Change], version: CommitVersion) -> Result<()> {
        for change in command {
            match change {
                Change::Create {
                    name,
                    id,
                    key_type,
                    key_version,
                    value_type,
                    value_version,
                } => {
                    if self.providers.contains_key(name) {
                        return Err(conflict("duplicate provider name"));
                    }
                    if self.providers.len() >= 1024 {
                        return Err(Error::ResourceExhausted);
                    }
                    self.providers.insert(
                        name.clone(),
                        Provider {
                            id: *id,
                            key_type: key_type.clone(),
                            key_version: *key_version,
                            value_type: value_type.clone(),
                            value_version: *value_version,
                            revision: version.0,
                            entries: BTreeMap::new(),
                        },
                    );
                    self.revision = version.0;
                }
                Change::RemoveProvider { name, id } => {
                    self.provider_mut(name, *id)?;
                    self.providers.remove(name);
                    self.revision = version.0;
                }
                Change::Set {
                    name,
                    id,
                    key,
                    value,
                } => {
                    let provider = self.provider_mut(name, *id)?;
                    provider.entries.insert(
                        key.clone(),
                        Value {
                            bytes: value.clone(),
                            version: version.0,
                        },
                    );
                    provider.revision = version.0;
                }
                Change::Clear { name, id } => {
                    let provider = self.provider_mut(name, *id)?;
                    for entry in provider.entries.values_mut() {
                        entry.bytes = None;
                        entry.version = version.0;
                    }
                    provider.revision = version.0;
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) enum Change {
    Create {
        name: String,
        id: u128,
        key_type: String,
        key_version: u32,
        value_type: String,
        value_version: u32,
    },
    RemoveProvider {
        name: String,
        id: u128,
    },
    Set {
        name: String,
        id: u128,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    },
    Clear {
        name: String,
        id: u128,
    },
}

#[derive(Clone, Default)]
pub(crate) struct Observations {
    providers: BTreeMap<String, Option<u128>>,
    keys: BTreeMap<(String, Vec<u8>), i64>,
    scans: BTreeMap<String, i64>,
    registry: Option<i64>,
}

#[derive(Default)]
struct RuntimeBindings {
    state_replicator: Option<Arc<dyn StateReplicator>>,
    partition: Option<StatefulServicePartition>,
}

struct StateManagerInner {
    state: Arc<ReliableCollectionsState>,
    manager_id: u128,
    commit_gate: tokio::sync::Mutex<()>,
    admission: Arc<Semaphore>,
    bindings: RwLock<RuntimeBindings>,
    standalone_writes: bool,
}

#[derive(Clone)]
pub struct StateManager {
    inner: Arc<StateManagerInner>,
}

impl StateManager {
    pub async fn open(path: PathBuf) -> Result<Self> {
        Self::from_state(Arc::new(ReliableCollectionsState::open(path)?), true)
    }

    pub fn standalone(state: Arc<ReliableCollectionsState>) -> Result<Self> {
        Self::from_state(state, true)
    }

    pub(crate) fn from_state(
        state: Arc<ReliableCollectionsState>,
        standalone_writes: bool,
    ) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(StateManagerInner {
                state,
                manager_id: new_id(),
                commit_gate: tokio::sync::Mutex::new(()),
                admission: Arc::new(Semaphore::new(16)),
                bindings: RwLock::new(RuntimeBindings::default()),
                standalone_writes,
            }),
        })
    }

    pub(crate) async fn attach_runtime(
        &self,
        partition: StatefulServicePartition,
        state_replicator: Arc<dyn StateReplicator>,
    ) {
        *self.inner.bindings.write().await = RuntimeBindings {
            state_replicator: Some(state_replicator),
            partition: Some(partition),
        };
    }

    pub(crate) async fn detach_runtime(&self) {
        *self.inner.bindings.write().await = RuntimeBindings::default();
    }

    pub(crate) fn try_detach_runtime(&self) {
        if let Ok(mut bindings) = self.inner.bindings.try_write() {
            *bindings = RuntimeBindings::default();
        }
    }

    pub async fn create_transaction(&self) -> Result<Transaction> {
        self.transaction_with_options(TransactionOptions::default())
            .await
    }

    pub async fn transaction_with_options(
        &self,
        options: TransactionOptions,
    ) -> Result<Transaction> {
        if options.timeout.is_zero() || options.timeout > Duration::from_secs(60) {
            return Err(Error::Invalid(
                "timeout must be between zero and 60 seconds".into(),
            ));
        }
        self.ensure_readable().await?;
        let permit = self
            .inner
            .admission
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::ResourceExhausted)?;
        let (generation, state) = self.inner.state.begin_snapshot()?;
        let transaction = new_id();
        Ok(Transaction {
            identity: TransactionId {
                transaction,
                request: format!("{transaction:032x}"),
            },
            manager: self.inner.manager_id,
            owner: self.clone(),
            context: TransactionContext {
                generation,
                deadline: Instant::now() + options.timeout,
                owner: self.inner.admission.clone(),
                _permit: permit,
            },
            baseline: state.clone(),
            state,
            observed: Observations::default(),
            changes: Vec::new(),
            bytes: 0,
        })
    }

    pub async fn get_or_add_dictionary<Key: ReliableValue, Item: ReliableValue>(
        &self,
        name: &str,
    ) -> Result<ReliableDictionary<Key, Item>> {
        let mut transaction = self.create_transaction().await?;
        let existed = transaction.state.providers.contains_key(name);
        let dictionary = transaction.get_or_add_dictionary(name)?;
        if !existed {
            transaction.commit().await?;
        } else {
            transaction.abort();
        }
        Ok(dictionary)
    }

    pub async fn committed_result(&self, identity: TransactionId) -> Result<Option<CommitVersion>> {
        self.ensure_readable().await?;
        self.inner.state.committed_result(identity)
    }

    pub async fn checkpoint(&self) -> Result<()> {
        self.inner.state.checkpoint().await
    }

    pub async fn backup(&self, destination: PathBuf) -> Result<()> {
        self.inner.state.backup(destination).await
    }

    pub async fn restore_backup(&self, source: PathBuf) -> Result<()> {
        if self.inner.bindings.read().await.state_replicator.is_some() {
            return Err(Error::Invalid(
                "restore requires a manager that is not attached to a runtime".into(),
            ));
        }
        self.inner.state.restore_backup(source).await
    }

    pub async fn applied_lsn(&self) -> Result<i64> {
        self.inner.state.applied_lsn().await
    }

    async fn ensure_readable(&self) -> Result<()> {
        let partition = self.inner.bindings.read().await.partition.clone();
        if let Some(partition) = partition {
            let status = partition.get_read_status().await?;
            if status != AccessStatus::Granted {
                return Err(Error::ReadClosed(status));
            }
        }
        Ok(())
    }

    async fn commit_transaction(
        &self,
        context: TransactionContext,
        identity: TransactionId,
        observations: Observations,
        command: Vec<Change>,
    ) -> Result<CommitVersion> {
        context.ensure_active()?;
        if !Arc::ptr_eq(&context.owner, &self.inner.admission) {
            return Err(Error::Invalid(
                "transaction belongs to another state manager".into(),
            ));
        }
        if identity.request.is_empty() || identity.request.len() > 128 {
            return Err(Error::Invalid("request ID must be 1-128 bytes".into()));
        }
        let remaining = context.deadline.saturating_duration_since(Instant::now());
        let _gate = tokio::time::timeout(remaining, self.inner.commit_gate.lock())
            .await
            .map_err(|_| Error::Expired)?;
        context.ensure_active()?;
        let prepared =
            self.inner
                .state
                .prepare_commit(context.generation, identity, observations, command)?;
        if let Some(version) = prepared.retained {
            let progress = self.inner.state.durable_progress().await?;
            if version.0 <= progress.committed_lsn {
                return Ok(version);
            }
        }
        let lsn = self.replicate(prepared).await?;
        Ok(CommitVersion(lsn))
    }

    async fn replicate(&self, prepared: PreparedCommit) -> Result<i64> {
        let bindings = self.inner.bindings.read().await;
        if let Some(partition) = bindings.partition.clone() {
            let status = partition.get_write_status().await?;
            if status != AccessStatus::Granted {
                return Err(Error::WriteClosed(status));
            }
        }
        if let Some(replicator) = bindings.state_replicator.clone() {
            drop(bindings);
            let lsn = replicator.replicate(Bytes::from(prepared.payload)).await?;
            return Ok(prepared.retained.unwrap_or(CommitVersion(lsn)).0);
        }
        drop(bindings);
        if !self.inner.standalone_writes {
            return Err(Error::NotOpen);
        }
        let version = self.inner.state.apply_standalone(prepared.payload)?;
        Ok(prepared.retained.unwrap_or(version).0)
    }
}

pub(crate) struct TransactionContext {
    generation: u64,
    deadline: Instant,
    owner: Arc<Semaphore>,
    _permit: OwnedSemaphorePermit,
}

impl TransactionContext {
    fn ensure_active(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            Err(Error::Expired)
        } else {
            Ok(())
        }
    }
}

pub struct Transaction {
    identity: TransactionId,
    manager: u128,
    owner: StateManager,
    context: TransactionContext,
    baseline: Registry,
    state: Registry,
    observed: Observations,
    changes: Vec<Change>,
    bytes: usize,
}

impl Transaction {
    pub fn id(&self) -> &TransactionId {
        &self.identity
    }

    pub fn with_identity(mut self, identity: TransactionId) -> Result<Self> {
        if identity.request.is_empty() || identity.request.len() > 128 {
            return Err(Error::Invalid("request ID must be 1-128 bytes".into()));
        }
        self.identity = identity;
        Ok(self)
    }

    pub fn get_or_add_dictionary<Key: ReliableValue, Item: ReliableValue>(
        &mut self,
        name: &str,
    ) -> Result<ReliableDictionary<Key, Item>> {
        if let Some(dictionary) = self.get_dictionary(name)? {
            return Ok(dictionary);
        }
        if Key::TYPE_ID.is_empty()
            || Item::TYPE_ID.is_empty()
            || Key::TYPE_ID.len() > 128
            || Item::TYPE_ID.len() > 128
        {
            return Err(Error::Invalid("stable type IDs must be 1-128 bytes".into()));
        }
        let id = new_id();
        self.stage(Change::Create {
            name: name.to_owned(),
            id,
            key_type: Key::TYPE_ID.to_owned(),
            key_version: Key::VERSION,
            value_type: Item::TYPE_ID.to_owned(),
            value_version: Item::VERSION,
        })?;
        Ok(ReliableDictionary {
            manager: self.manager,
            name: name.to_owned(),
            id,
            marker: PhantomData,
        })
    }

    pub fn get_dictionary<Key: ReliableValue, Item: ReliableValue>(
        &mut self,
        name: &str,
    ) -> Result<Option<ReliableDictionary<Key, Item>>> {
        self.observe_provider(name)?;
        let Some(provider) = self.state.providers.get(name) else {
            return Ok(None);
        };
        if provider.key_type != Key::TYPE_ID
            || provider.key_version != Key::VERSION
            || provider.value_type != Item::TYPE_ID
            || provider.value_version != Item::VERSION
        {
            return Err(Error::Invalid(
                "incompatible provider types or versions".into(),
            ));
        }
        Ok(Some(ReliableDictionary {
            manager: self.manager,
            name: name.to_owned(),
            id: provider.id,
            marker: PhantomData,
        }))
    }

    pub fn remove_provider(&mut self, name: &str) -> Result<bool> {
        self.scan(name)?;
        let Some(provider) = self.state.providers.get(name) else {
            return Ok(false);
        };
        self.stage(Change::RemoveProvider {
            name: name.to_owned(),
            id: provider.id,
        })?;
        Ok(true)
    }

    pub fn provider_names(&mut self) -> Result<Vec<String>> {
        self.context.ensure_active()?;
        self.observed.registry = Some(self.baseline.revision);
        Ok(self.state.providers.keys().cloned().collect())
    }

    pub async fn commit(self) -> Result<CommitVersion> {
        let owner = self.owner;
        owner
            .commit_transaction(self.context, self.identity, self.observed, self.changes)
            .await
    }

    pub fn abort(self) {}

    fn observe_provider(&mut self, name: &str) -> Result<()> {
        self.context.ensure_active()?;
        if name.is_empty() || name.len() > 128 {
            return Err(Error::Invalid("provider name must be 1-128 bytes".into()));
        }
        if self.observed.providers.len() >= 1024 && !self.observed.providers.contains_key(name) {
            return Err(Error::ResourceExhausted);
        }
        self.observed
            .providers
            .entry(name.to_owned())
            .or_insert_with(|| {
                self.baseline
                    .providers
                    .get(name)
                    .map(|provider| provider.id)
            });
        Ok(())
    }

    fn observe_key(&mut self, name: &str, key: &[u8]) -> Result<()> {
        self.observe_provider(name)?;
        if key.len() > 64 * 1024 {
            return Err(Error::ResourceExhausted);
        }
        if self.observed.keys.len() >= 1024
            && !self
                .observed
                .keys
                .contains_key(&(name.to_owned(), key.to_vec()))
        {
            return Err(Error::ResourceExhausted);
        }
        self.observed
            .keys
            .entry((name.to_owned(), key.to_vec()))
            .or_insert_with(|| self.baseline.key_version(name, key));
        Ok(())
    }

    fn scan(&mut self, name: &str) -> Result<()> {
        self.observe_provider(name)?;
        self.observed
            .scans
            .entry(name.to_owned())
            .or_insert_with(|| {
                self.baseline
                    .providers
                    .get(name)
                    .map_or(0, |provider| provider.revision)
            });
        Ok(())
    }

    fn stage(&mut self, change: Change) -> Result<()> {
        self.context.ensure_active()?;
        let size = postcard::to_allocvec(&change)?.len();
        if self.changes.len() >= 1024
            || size > (MAX_TRANSACTION_BYTES / 2).saturating_sub(self.bytes)
        {
            return Err(Error::ResourceExhausted);
        }
        let command = vec![change.clone()];
        self.state.apply(&command, CommitVersion(0))?;
        self.bytes += size;
        self.changes.push(change);
        Ok(())
    }
}

pub struct ReliableDictionary<Key, Item> {
    manager: u128,
    name: String,
    id: u128,
    marker: PhantomData<fn(Key) -> Item>,
}

impl<Key, Item> Clone for ReliableDictionary<Key, Item> {
    fn clone(&self) -> Self {
        Self {
            manager: self.manager,
            name: self.name.clone(),
            id: self.id,
            marker: PhantomData,
        }
    }
}

impl<Key: ReliableValue, Item: ReliableValue> ReliableDictionary<Key, Item> {
    fn check(&self, transaction: &mut Transaction) -> Result<()> {
        transaction.context.ensure_active()?;
        if self.manager != transaction.manager {
            return Err(Error::Invalid(
                "dictionary belongs to another state manager".into(),
            ));
        }
        transaction.observe_provider(&self.name)?;
        transaction.state.provider_mut(&self.name, self.id)?;
        Ok(())
    }

    pub fn get(&self, transaction: &mut Transaction, key: &Key) -> Result<Option<Item>> {
        self.check(transaction)?;
        let key = postcard::to_allocvec(key)?;
        transaction.observe_key(&self.name, &key)?;
        transaction.state.providers[&self.name]
            .entries
            .get(&key)
            .and_then(|entry| entry.bytes.as_ref())
            .map(|bytes| postcard::from_bytes(bytes).map_err(Error::from))
            .transpose()
    }

    pub fn contains_key(&self, transaction: &mut Transaction, key: &Key) -> Result<bool> {
        Ok(self.get(transaction, key)?.is_some())
    }

    pub fn set(&self, transaction: &mut Transaction, key: &Key, value: &Item) -> Result<()> {
        self.check(transaction)?;
        let key = postcard::to_allocvec(key)?;
        transaction.observe_key(&self.name, &key)?;
        transaction.stage(Change::Set {
            name: self.name.clone(),
            id: self.id,
            key,
            value: Some(postcard::to_allocvec(value)?),
        })
    }

    pub fn insert(&self, transaction: &mut Transaction, key: &Key, value: &Item) -> Result<bool> {
        if self.contains_key(transaction, key)? {
            return Ok(false);
        }
        self.set(transaction, key, value)?;
        Ok(true)
    }

    pub fn update(
        &self,
        transaction: &mut Transaction,
        key: &Key,
        expected: &Item,
        replacement: &Item,
    ) -> Result<bool> {
        let Some(current) = self.get(transaction, key)? else {
            return Ok(false);
        };
        if postcard::to_allocvec(&current)? != postcard::to_allocvec(expected)? {
            return Ok(false);
        }
        self.set(transaction, key, replacement)?;
        Ok(true)
    }

    pub fn remove(&self, transaction: &mut Transaction, key: &Key) -> Result<Option<Item>> {
        let value = self.get(transaction, key)?;
        transaction.stage(Change::Set {
            name: self.name.clone(),
            id: self.id,
            key: postcard::to_allocvec(key)?,
            value: None,
        })?;
        Ok(value)
    }

    pub fn clear(&self, transaction: &mut Transaction) -> Result<()> {
        self.check(transaction)?;
        transaction.scan(&self.name)?;
        transaction.stage(Change::Clear {
            name: self.name.clone(),
            id: self.id,
        })
    }

    pub fn get_or_add(
        &self,
        transaction: &mut Transaction,
        key: &Key,
        value: Item,
    ) -> Result<Item> {
        if let Some(existing) = self.get(transaction, key)? {
            return Ok(existing);
        }
        self.set(transaction, key, &value)?;
        Ok(value)
    }

    pub fn add_or_update(
        &self,
        transaction: &mut Transaction,
        key: &Key,
        update: impl FnOnce(Option<Item>) -> Item,
    ) -> Result<Item> {
        let value = update(self.get(transaction, key)?);
        self.set(transaction, key, &value)?;
        Ok(value)
    }

    pub fn entries(&self, transaction: &mut Transaction) -> Result<Vec<(Key, Item)>> {
        self.check(transaction)?;
        transaction.scan(&self.name)?;
        transaction.state.providers[&self.name]
            .entries
            .iter()
            .filter_map(|(key, entry)| entry.bytes.as_ref().map(|value| (key, value)))
            .map(|(key, value)| Ok((postcard::from_bytes(key)?, postcard::from_bytes(value)?)))
            .collect()
    }
}

fn new_id() -> u128 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    (now << 32) ^ u128::from(NEXT.fetch_add(1, Ordering::Relaxed))
}
