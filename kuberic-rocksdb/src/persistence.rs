use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use fs2::FileExt;
use futures::stream;
use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, Operation,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::protocol::types::{Epoch, OperationId};
use kuberic_runtime::{Result, RuntimeError};
use rocksdb::{DB, DBCompressionType, Direction, IteratorMode, Options, WriteBatch, WriteOptions};
use serde::{Deserialize, Serialize};

pub const MAX_MUTATIONS: usize = 1024;
pub const MAX_ENVELOPE: usize = 1024 * 1024;
pub const MAX_COPY: usize = 256 * 1024 * 1024;
pub const COPY_CHUNK_SIZE: usize = 256 * 1024;

const USER_PREFIX: u8 = 1;
const META_PREFIX: u8 = 0;
const ENVELOPE_VERSION: u16 = 1;
const PROFILE: &str = "kuberic-rocksdb/2;rocksdb=10.4.2;cf=default;comparator=bytewise;merge=append-v1;compression=none";

const PROFILE_KEY: &[u8] = b"\0profile";
const LSN_KEY: &[u8] = b"\0lsn";
const COMMITTED_LSN_KEY: &[u8] = b"\0committed-lsn";
const BASE_LSN_KEY: &[u8] = b"\0base-lsn";
const EPOCH_KEY: &[u8] = b"\0epoch";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Merge { key: Vec<u8>, value: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize)]
struct Envelope {
    version: u16,
    profile: String,
    mutations: Vec<Mutation>,
    batch: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OperationRecord {
    committed_lsn: i64,
    data: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
struct History {
    record: Vec<u8>,
    committed_lsn: i64,
    previous: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CopyState {
    version: u16,
    profile: String,
    lsn: i64,
    committed_lsn: i64,
    values: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct EpochRecord {
    epoch: Epoch,
}

pub struct RocksState {
    root: PathBuf,
    store: Arc<Mutex<Store>>,
    _lock: File,
}

struct Store {
    database: DB,
    path: PathBuf,
    applied_lsn: i64,
    committed_lsn: i64,
    base_lsn: i64,
    epoch: Epoch,
}

impl RocksState {
    pub fn is_fresh_empty(root: impl AsRef<Path>) -> Result<bool> {
        let root = root.as_ref();
        if !root.exists() {
            return Ok(true);
        }
        if !root.is_dir() {
            return Err(application_error(
                "application data root is not a directory",
            ));
        }
        Ok(std::fs::read_dir(root)
            .map_err(runtime_io_error)?
            .next()
            .is_none())
    }

    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        let root = root.as_ref().to_path_buf();
        std::fs::create_dir_all(&root).map_err(runtime_io_error)?;
        let root = std::fs::canonicalize(&root).map_err(runtime_io_error)?;
        let lock = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(root.join("replica.lock"))
            .map_err(runtime_io_error)?;
        lock.try_lock_exclusive()
            .map_err(|error| RuntimeError::Application(format!("RocksDB replica lock: {error}")))?;
        let active = active_generation(&root)?;
        let store = Store::open(root.join(active))?;
        publish_database(&root, store.generation_name()?)?;
        Ok(Self {
            root,
            store: Arc::new(Mutex::new(store)),
            _lock: lock,
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let key = key.to_vec();
        self.with_store_blocking(move |store| {
            store.get_user(&key).map(|value| value.map(Bytes::from))
        })
        .await
    }

    pub async fn applied_lsn(&self) -> Result<i64> {
        self.with_store_blocking(|store| Ok(store.applied_lsn))
            .await
    }

    pub async fn committed_lsn(&self) -> Result<i64> {
        self.with_store_blocking(|store| Ok(store.committed_lsn))
            .await
    }

    pub async fn base_lsn(&self) -> Result<i64> {
        self.with_store_blocking(|store| Ok(store.base_lsn)).await
    }

    pub fn encode_mutations(mutations: Vec<Mutation>) -> Result<Bytes> {
        record(mutations).map(Bytes::from)
    }

    pub async fn snapshot_bytes(&self, up_to_lsn: i64) -> Result<Bytes> {
        self.with_store_blocking(move |store| store.copy_at(up_to_lsn).map(Bytes::from))
            .await
    }

    pub async fn copy_chunks(&self, up_to_lsn: i64) -> Result<Vec<Bytes>> {
        let snapshot = self.snapshot_bytes(up_to_lsn).await?;
        Ok(snapshot
            .chunks(COPY_CHUNK_SIZE)
            .map(Bytes::copy_from_slice)
            .collect())
    }

    #[cfg(test)]
    pub async fn raw_metadata_for_test(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = key.to_vec();
        self.with_store_blocking(move |store| store.database.get(key).map_err(runtime_rocks_error))
            .await
    }

    #[cfg(test)]
    pub async fn raw_user_value_for_test(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let key = key.to_vec();
        self.with_store_blocking(move |store| {
            store
                .database
                .get(user_key(&key))
                .map_err(runtime_rocks_error)
        })
        .await
    }

    async fn with_store_blocking<T: Send + 'static>(
        &self,
        action: impl FnOnce(&mut Store) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let mut store = store
                .lock()
                .map_err(|_| application_error("RocksDB state mutex poisoned"))?;
            action(&mut store)
        })
        .await
        .map_err(|error| RuntimeError::Application(error.to_string()))?
    }

    pub async fn update_epoch(&self, epoch: Epoch, previous_epoch_last_lsn: i64) -> Result<()> {
        self.with_store_blocking(move |store| store.update_epoch(epoch, previous_epoch_last_lsn))
            .await
    }

    async fn replace_from_copy(
        &self,
        copy: CopyState,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> Result<DurableApplicationProgress> {
        if copy.version != ENVELOPE_VERSION || copy.profile != PROFILE || copy.lsn != up_to_lsn {
            return Err(application_error(
                "copy profile, version, or LSN boundary mismatch",
            ));
        }
        if copy.committed_lsn != committed_lsn {
            return Err(application_error("copy committed boundary mismatch"));
        }
        let root = self.root.clone();
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = store
                .lock()
                .map_err(|_| application_error("RocksDB state mutex poisoned"))?;
            let replacement =
                Store::install_snapshot(&root, copy.values, up_to_lsn, committed_lsn)?;
            *guard = replacement;
            Ok(DurableApplicationProgress {
                applied_lsn: up_to_lsn,
                committed_lsn,
            })
        })
        .await
        .map_err(|error| RuntimeError::Application(error.to_string()))?
    }
}

#[async_trait]
impl DurableState for RocksState {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> Result<RetainedOperationStream> {
        if from_lsn > to_lsn {
            return Ok(Box::pin(stream::empty()));
        }
        let operations = self
            .with_store_blocking(move |store| store.retained_operations(from_lsn, to_lsn))
            .await?;
        Ok(Box::pin(stream::iter(operations.into_iter().map(Ok))))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> Result<()> {
        if sequence == 0 {
            return Err(application_error("copy chunk sequence must be one-based"));
        }
        let root = self.root.clone();
        let build = build_dir_name(build_id);
        tokio::task::spawn_blocking(move || stage_copy_chunk(&root, &build, sequence, &chunk.data))
            .await
            .map_err(|error| RuntimeError::Application(error.to_string()))?
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> Result<bool> {
        let root = self.root.clone();
        let build = build_dir_name(build_id);
        let expected = chunk.data.clone();
        tokio::task::spawn_blocking(move || {
            let path = copy_chunk_path(&root, &build, sequence);
            match std::fs::read(path) {
                Ok(bytes) => Ok(bytes == expected),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(runtime_io_error(error)),
            }
        })
        .await
        .map_err(|error| RuntimeError::Application(error.to_string()))?
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> Result<DurableApplicationProgress> {
        if up_to_lsn != committed_lsn {
            return Err(application_error(
                "copy completion requires applied and committed progress at the same frozen boundary",
            ));
        }
        let root = self.root.clone();
        let build = build_dir_name(build_id);
        let bytes = tokio::task::spawn_blocking(move || read_copy_chunks(&root, &build))
            .await
            .map_err(|error| RuntimeError::Application(error.to_string()))??;
        let copy = decode_record::<CopyState>(&bytes, MAX_COPY)?;
        self.replace_from_copy(copy, up_to_lsn, committed_lsn).await
    }

    async fn apply(&self, operation: Operation) -> Result<DurableApplicationAck> {
        self.with_store_blocking(move |store| store.apply(operation))
            .await
    }

    async fn durable_progress(&self) -> Result<DurableApplicationProgress> {
        self.with_store_blocking(|store| Ok(store.progress())).await
    }

    async fn verify_applied(&self, operation: &Operation) -> Result<bool> {
        let operation = operation.clone();
        self.with_store_blocking(move |store| store.verify(&operation))
            .await
    }

    async fn commit(&self, committed_lsn: i64) -> Result<DurableApplicationProgress> {
        self.with_store_blocking(move |store| store.commit(committed_lsn))
            .await
    }
}

impl Store {
    fn open(path: PathBuf) -> Result<Self> {
        std::fs::create_dir_all(&path).map_err(runtime_io_error)?;
        let database = DB::open(&rocks_options(), &path).map_err(runtime_rocks_error)?;
        initialize_profile(&database)?;
        let applied_lsn = read_lsn(&database, LSN_KEY)?.unwrap_or(0);
        let committed_lsn = read_lsn(&database, COMMITTED_LSN_KEY)?.unwrap_or(0);
        let base_lsn = read_lsn(&database, BASE_LSN_KEY)?.unwrap_or(0);
        if base_lsn < 0 || committed_lsn < 0 || applied_lsn < 0 {
            return Err(application_error("negative durable progress"));
        }
        if base_lsn > applied_lsn || committed_lsn > applied_lsn {
            return Err(application_error(
                "durable progress is internally inconsistent",
            ));
        }
        let epoch = match database.get(EPOCH_KEY).map_err(runtime_rocks_error)? {
            Some(bytes) => decode_record::<EpochRecord>(&bytes, MAX_ENVELOPE)?.epoch,
            None => Epoch::default(),
        };
        Ok(Self {
            database,
            path,
            applied_lsn,
            committed_lsn,
            base_lsn,
            epoch,
        })
    }

    fn install_snapshot(
        root: &Path,
        values: BTreeMap<Vec<u8>, Vec<u8>>,
        lsn: i64,
        committed_lsn: i64,
    ) -> Result<Self> {
        if lsn < 0 || committed_lsn < 0 || committed_lsn > lsn {
            return Err(application_error("invalid copy progress"));
        }
        let generation = unique_generation_name();
        let temporary_name = format!("{generation}.tmp");
        let temporary = root.join(&temporary_name);
        let final_path = root.join(&generation);
        if temporary.exists() {
            std::fs::remove_dir_all(&temporary).map_err(runtime_io_error)?;
        }
        std::fs::create_dir_all(&temporary).map_err(runtime_io_error)?;
        {
            let mut store = Self::open(temporary.clone())?;
            store.replace_contents(values, lsn, committed_lsn)?;
            store.database.flush().map_err(runtime_rocks_error)?;
        }
        sync_directory(&temporary)?;
        std::fs::rename(&temporary, &final_path).map_err(runtime_io_error)?;
        sync_directory(root)?;
        publish_database(root, &generation)?;
        Self::open(final_path)
    }

    fn generation_name(&self) -> Result<&str> {
        self.path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| application_error("active RocksDB generation has no valid name"))
    }

    fn progress(&self) -> DurableApplicationProgress {
        DurableApplicationProgress {
            applied_lsn: self.applied_lsn,
            committed_lsn: self.committed_lsn,
        }
    }

    fn get_user(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.database
            .get(user_key(key))
            .map_err(runtime_rocks_error)
    }

    fn apply(&mut self, operation: Operation) -> Result<DurableApplicationAck> {
        if operation.lsn < 0
            || operation.committed_lsn < 0
            || operation.committed_lsn > operation.lsn
        {
            return Err(application_error("operation progress is out of bounds"));
        }
        let envelope = decode_operation(&operation.data)?;
        if operation.lsn <= self.applied_lsn {
            return self.verify_duplicate(&operation).map(|()| self.progress());
        }
        if operation.lsn != self.applied_lsn + 1 {
            return Err(application_error(format!(
                "LSN gap: expected {}, got {}",
                self.applied_lsn + 1,
                operation.lsn
            )));
        }
        let mut previous = BTreeMap::new();
        for mutation in &envelope.mutations {
            let key = mutation_user_key(mutation);
            if !previous.contains_key(&key) {
                previous.insert(
                    key.clone(),
                    self.database.get(&key).map_err(runtime_rocks_error)?,
                );
            }
        }
        let mut batch = WriteBatch::from_data(&envelope.batch);
        let next_committed = self.committed_lsn.max(operation.committed_lsn);
        batch.put(LSN_KEY, operation.lsn.to_le_bytes());
        batch.put(COMMITTED_LSN_KEY, next_committed.to_le_bytes());
        batch.put(
            operation_key(operation.lsn),
            encode_record(
                &OperationRecord {
                    committed_lsn: operation.committed_lsn,
                    data: operation.data.to_vec(),
                },
                MAX_COPY,
            )?,
        );
        batch.put(
            history_key(operation.lsn),
            encode_record(
                &History {
                    record: operation.data.to_vec(),
                    committed_lsn: operation.committed_lsn,
                    previous,
                },
                MAX_COPY,
            )?,
        );
        sync_write(&self.database, batch)?;
        self.applied_lsn = operation.lsn;
        self.committed_lsn = next_committed;
        Ok(self.progress())
    }

    fn verify_duplicate(&self, operation: &Operation) -> Result<()> {
        let record = self
            .database
            .get(operation_key(operation.lsn))
            .map_err(runtime_rocks_error)?
            .ok_or_else(|| {
                RuntimeError::AuthorityMismatch(
                    "duplicate LSN predates retained operation identity".into(),
                )
            })?;
        let record = decode_record::<OperationRecord>(&record, MAX_COPY)?;
        if record.data == operation.data && record.committed_lsn == operation.committed_lsn {
            Ok(())
        } else {
            Err(RuntimeError::AuthorityMismatch(
                "LSN was reused with different RocksDB bytes".into(),
            ))
        }
    }

    fn verify(&self, operation: &Operation) -> Result<bool> {
        let Some(record) = self
            .database
            .get(operation_key(operation.lsn))
            .map_err(runtime_rocks_error)?
        else {
            return Ok(false);
        };
        let record = decode_record::<OperationRecord>(&record, MAX_COPY)?;
        Ok(record.data == operation.data && record.committed_lsn == operation.committed_lsn)
    }

    fn commit(&mut self, committed_lsn: i64) -> Result<DurableApplicationProgress> {
        if committed_lsn < 0 || committed_lsn > self.applied_lsn {
            return Err(application_error(
                "cannot commit beyond applied RocksDB progress",
            ));
        }
        if committed_lsn <= self.committed_lsn {
            return Ok(self.progress());
        }
        let mut batch = WriteBatch::default();
        batch.put(COMMITTED_LSN_KEY, committed_lsn.to_le_bytes());
        sync_write(&self.database, batch)?;
        self.committed_lsn = committed_lsn;
        Ok(self.progress())
    }

    fn retained_operations(&self, from_lsn: i64, to_lsn: i64) -> Result<Vec<Operation>> {
        if from_lsn <= self.base_lsn {
            return Err(application_error(format!(
                "retained operation request starts at {from_lsn}, but base LSN is {}",
                self.base_lsn
            )));
        }
        if to_lsn > self.applied_lsn {
            return Err(application_error(format!(
                "retained operation request ends at {to_lsn}, but applied LSN is {}",
                self.applied_lsn
            )));
        }
        let mut operations = Vec::new();
        for lsn in from_lsn..=to_lsn {
            let bytes = self
                .database
                .get(operation_key(lsn))
                .map_err(runtime_rocks_error)?
                .ok_or_else(|| application_error(format!("retained operation {lsn} is missing")))?;
            let record = decode_record::<OperationRecord>(&bytes, MAX_COPY)?;
            operations.push(Operation {
                lsn,
                committed_lsn: record.committed_lsn,
                data: Bytes::from(record.data),
            });
        }
        Ok(operations)
    }

    fn copy_at(&self, target_lsn: i64) -> Result<Vec<u8>> {
        if target_lsn > self.committed_lsn {
            return Err(application_error(format!(
                "copy boundary {target_lsn} exceeds committed LSN {}",
                self.committed_lsn
            )));
        }
        let values = self.values_at(target_lsn)?;
        encode_record(
            &CopyState {
                version: ENVELOPE_VERSION,
                profile: PROFILE.into(),
                lsn: target_lsn,
                committed_lsn: target_lsn,
                values,
            },
            MAX_COPY,
        )
    }

    fn values_at(&self, target_lsn: i64) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        if target_lsn < self.base_lsn || target_lsn > self.applied_lsn {
            return Err(application_error(format!(
                "copy boundary {target_lsn} is outside retained range {}..={}",
                self.base_lsn, self.applied_lsn
            )));
        }
        let mut values = self.current_encoded_values()?;
        for lsn in (target_lsn + 1..=self.applied_lsn).rev() {
            let bytes = self
                .database
                .get(history_key(lsn))
                .map_err(runtime_rocks_error)?
                .ok_or_else(|| application_error(format!("undo history {lsn} is missing")))?;
            let history = decode_record::<History>(&bytes, MAX_COPY)?;
            for (key, previous) in history.previous {
                match previous {
                    Some(value) => {
                        values.insert(key, value);
                    }
                    None => {
                        values.remove(&key);
                    }
                }
            }
        }
        let mut decoded = BTreeMap::new();
        for (key, value) in values {
            let user = key
                .strip_prefix(&[USER_PREFIX])
                .ok_or_else(|| application_error("snapshot encountered non-user key"))?;
            decoded.insert(user.to_vec(), value);
        }
        Ok(decoded)
    }

    fn current_encoded_values(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut values = BTreeMap::new();
        for item in self
            .database
            .iterator(IteratorMode::From(&[USER_PREFIX], Direction::Forward))
        {
            let (key, value) = item.map_err(runtime_rocks_error)?;
            if key.first() != Some(&USER_PREFIX) {
                break;
            }
            values.insert(key.to_vec(), value.to_vec());
        }
        Ok(values)
    }

    fn replace_contents(
        &mut self,
        values: BTreeMap<Vec<u8>, Vec<u8>>,
        lsn: i64,
        committed_lsn: i64,
    ) -> Result<()> {
        let mut batch = WriteBatch::default();
        for (key, value) in values {
            batch.put(user_key(&key), value);
        }
        batch.put(PROFILE_KEY, PROFILE.as_bytes());
        batch.put(LSN_KEY, lsn.to_le_bytes());
        batch.put(COMMITTED_LSN_KEY, committed_lsn.to_le_bytes());
        batch.put(BASE_LSN_KEY, lsn.to_le_bytes());
        batch.put(
            EPOCH_KEY,
            encode_record(&EpochRecord { epoch: self.epoch }, MAX_ENVELOPE)?,
        );
        sync_write(&self.database, batch)?;
        self.applied_lsn = lsn;
        self.committed_lsn = committed_lsn;
        self.base_lsn = lsn;
        Ok(())
    }

    fn update_epoch(&mut self, epoch: Epoch, previous_epoch_last_lsn: i64) -> Result<()> {
        if previous_epoch_last_lsn < 0 {
            return Err(application_error("negative previous epoch boundary"));
        }
        if epoch < self.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "application epoch regressed".into(),
            ));
        }
        if epoch == self.epoch {
            return Ok(());
        }
        let mut batch = WriteBatch::default();
        batch.put(
            EPOCH_KEY,
            encode_record(&EpochRecord { epoch }, MAX_ENVELOPE)?,
        );
        sync_write(&self.database, batch)?;
        self.epoch = epoch;
        Ok(())
    }
}

fn rocks_options() -> Options {
    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_compression_type(DBCompressionType::None);
    options.set_merge_operator_associative("append-v1", |_key, existing, operands| {
        let mut value = existing.unwrap_or_default().to_vec();
        for operand in operands {
            value.extend_from_slice(operand);
        }
        Some(value)
    });
    options
}

fn initialize_profile(database: &DB) -> Result<()> {
    match database.get(PROFILE_KEY).map_err(runtime_rocks_error)? {
        Some(profile) if profile == PROFILE.as_bytes() => Ok(()),
        Some(_) => Err(application_error("database configuration mismatch")),
        None => {
            if database
                .iterator(IteratorMode::Start)
                .next()
                .transpose()
                .map_err(runtime_rocks_error)?
                .is_some()
            {
                return Err(application_error(
                    "database was not created by this adapter",
                ));
            }
            let mut batch = WriteBatch::default();
            batch.put(PROFILE_KEY, PROFILE.as_bytes());
            batch.put(LSN_KEY, 0i64.to_le_bytes());
            batch.put(COMMITTED_LSN_KEY, 0i64.to_le_bytes());
            batch.put(BASE_LSN_KEY, 0i64.to_le_bytes());
            batch.put(
                EPOCH_KEY,
                encode_record(
                    &EpochRecord {
                        epoch: Epoch::default(),
                    },
                    MAX_ENVELOPE,
                )?,
            );
            sync_write(database, batch)
        }
    }
}

fn decode_operation(data: &[u8]) -> Result<Envelope> {
    let envelope = decode_record::<Envelope>(data, MAX_ENVELOPE)?;
    if envelope.version != ENVELOPE_VERSION || envelope.profile != PROFILE {
        return Err(application_error("unsupported batch envelope"));
    }
    if envelope.mutations.len() > MAX_MUTATIONS {
        return Err(application_error("batch exceeds 1024 mutations"));
    }
    let expected = make_batch(&envelope.mutations)?;
    if expected.data() != envelope.batch {
        return Err(application_error(
            "batch bytes do not match typed mutations",
        ));
    }
    Ok(envelope)
}

fn record(mutations: Vec<Mutation>) -> Result<Vec<u8>> {
    let batch = make_batch(&mutations)?.data().to_vec();
    encode_record(
        &Envelope {
            version: ENVELOPE_VERSION,
            profile: PROFILE.into(),
            mutations,
            batch,
        },
        MAX_ENVELOPE,
    )
}

fn make_batch(mutations: &[Mutation]) -> Result<WriteBatch> {
    if mutations.len() > MAX_MUTATIONS {
        return Err(application_error("batch exceeds 1024 mutations"));
    }
    let mut batch = WriteBatch::default();
    for mutation in mutations {
        match mutation {
            Mutation::Put { key, value } => batch.put(user_key(key), value),
            Mutation::Delete { key } => batch.delete(user_key(key)),
            Mutation::Merge { key, value } => batch.merge(user_key(key), value),
        }
    }
    if batch.data().len() > MAX_ENVELOPE {
        return Err(application_error("batch exceeds payload limit"));
    }
    Ok(batch)
}

fn encode_record<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(value)
        .map_err(|error| RuntimeError::Application(format!("serialization: {error}")))?;
    if payload.len() > limit {
        return Err(application_error("record exceeds size limit"));
    }
    let mut record = crc32fast::hash(&payload).to_le_bytes().to_vec();
    record.extend_from_slice(&payload);
    Ok(record)
}

fn decode_record<T: serde::de::DeserializeOwned>(record: &[u8], limit: usize) -> Result<T> {
    if record.len() < 4 || record.len() > limit + 4 {
        return Err(application_error("invalid record length"));
    }
    let checksum = u32::from_le_bytes(record[..4].try_into().expect("checksum length checked"));
    if crc32fast::hash(&record[4..]) != checksum {
        return Err(application_error("record checksum mismatch"));
    }
    postcard::from_bytes(&record[4..])
        .map_err(|error| RuntimeError::Application(format!("serialization: {error}")))
}

fn user_key(key: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(key.len() + 1);
    encoded.push(USER_PREFIX);
    encoded.extend_from_slice(key);
    encoded
}

fn mutation_user_key(mutation: &Mutation) -> Vec<u8> {
    match mutation {
        Mutation::Put { key, .. } | Mutation::Delete { key } | Mutation::Merge { key, .. } => {
            user_key(key)
        }
    }
}

fn operation_key(lsn: i64) -> Vec<u8> {
    record_key(b"\0operation", lsn)
}

fn history_key(lsn: i64) -> Vec<u8> {
    record_key(b"\0history", lsn)
}

fn record_key(prefix: &[u8], lsn: i64) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + 8);
    key.extend_from_slice(prefix);
    key.extend_from_slice(&lsn.to_be_bytes());
    key
}

fn read_lsn(database: &DB, key: &[u8]) -> Result<Option<i64>> {
    match database.get(key).map_err(runtime_rocks_error)? {
        Some(bytes) => {
            let bytes: [u8; 8] = bytes
                .as_slice()
                .try_into()
                .map_err(|_| application_error("invalid persisted LSN"))?;
            Ok(Some(i64::from_le_bytes(bytes)))
        }
        None => Ok(None),
    }
}

fn active_generation(root: &Path) -> Result<String> {
    match std::fs::read_to_string(root.join("active")) {
        Ok(name) => {
            let name = name.trim();
            validate_generation_name(name)?;
            if !root.join(name).join("CURRENT").is_file() {
                return Err(application_error("active RocksDB generation is missing"));
            }
            Ok(name.into())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            let name = "generation-00000000000000000000".to_string();
            std::fs::create_dir_all(root.join(&name)).map_err(runtime_io_error)?;
            publish_database(root, &name)?;
            Ok(name)
        }
        Err(error) => Err(runtime_io_error(error)),
    }
}

fn publish_database(root: &Path, name: &str) -> Result<()> {
    validate_generation_name(name)?;
    let temporary = root.join("active.tmp");
    {
        let mut file = File::create(&temporary).map_err(runtime_io_error)?;
        file.write_all(name.as_bytes()).map_err(runtime_io_error)?;
        file.sync_all().map_err(runtime_io_error)?;
    }
    std::fs::rename(&temporary, root.join("active")).map_err(runtime_io_error)?;
    sync_directory(root)
}

fn validate_generation_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name == "."
        || name == ".."
        || name.as_bytes().contains(&META_PREFIX)
        || name.contains(['/', '\\', ':'])
    {
        return Err(application_error("unsafe RocksDB generation name"));
    }
    Ok(())
}

fn unique_generation_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    format!("generation-{nanos:032}")
}

fn stage_copy_chunk(root: &Path, build: &str, sequence: u64, data: &[u8]) -> Result<()> {
    let path = copy_chunk_path(root, build, sequence);
    let staging = copy_chunk_staging_path(root, build, sequence);
    let parent = path
        .parent()
        .ok_or_else(|| application_error("copy chunk path has no parent"))?;
    std::fs::create_dir_all(parent).map_err(runtime_io_error)?;
    if path.is_file() {
        let existing = std::fs::read(&path).map_err(runtime_io_error)?;
        if existing != data {
            return Err(RuntimeError::AuthorityMismatch(
                "copy sequence was reused with different bytes".into(),
            ));
        }
        File::open(&path)
            .and_then(|file| file.sync_all())
            .map_err(runtime_io_error)?;
    } else {
        match std::fs::remove_file(&staging) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(runtime_io_error(error)),
        }
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&staging)
                .map_err(runtime_io_error)?;
            file.write_all(data).map_err(runtime_io_error)?;
            file.sync_all().map_err(runtime_io_error)?;
        }
        std::fs::rename(&staging, &path).map_err(runtime_io_error)?;
    }
    sync_ancestors(parent, root)
}

fn read_copy_chunks(root: &Path, build: &str) -> Result<Vec<u8>> {
    let directory = root.join("copy").join(build);
    let mut entries = std::fs::read_dir(&directory)
        .map_err(runtime_io_error)?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(runtime_io_error)?;
    entries.retain(|entry| {
        entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "chunk")
    });
    entries.sort_by_key(|entry| entry.file_name());
    if entries.is_empty() {
        return Err(application_error("copy contained no chunks"));
    }
    let mut bytes = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let expected = format!("{:020}.chunk", index as u64 + 1);
        if entry.file_name() != expected.as_str() {
            return Err(application_error("copy chunk sequence has a gap"));
        }
        let chunk = std::fs::read(entry.path()).map_err(runtime_io_error)?;
        bytes
            .len()
            .checked_add(chunk.len())
            .filter(|total| *total <= MAX_COPY + 4)
            .ok_or_else(|| application_error("copy exceeds 256 MiB limit"))?;
        bytes.extend(chunk);
    }
    Ok(bytes)
}

fn copy_chunk_path(root: &Path, build: &str, sequence: u64) -> PathBuf {
    root.join("copy")
        .join(build)
        .join(format!("{sequence:020}.chunk"))
}

fn copy_chunk_staging_path(root: &Path, build: &str, sequence: u64) -> PathBuf {
    root.join("copy")
        .join(build)
        .join(format!("{sequence:020}.chunk.tmp"))
}

fn build_dir_name(build_id: &OperationId) -> String {
    let mut output = String::with_capacity(build_id.as_str().len() * 2);
    for byte in build_id.as_str().as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn sync_write(database: &DB, batch: WriteBatch) -> Result<()> {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    database
        .write_opt(batch, &options)
        .map_err(runtime_rocks_error)
}

fn sync_ancestors(mut path: &Path, root: &Path) -> Result<()> {
    loop {
        sync_directory(path)?;
        if path == root {
            return Ok(());
        }
        path = path
            .parent()
            .ok_or_else(|| application_error("copy path escaped application root"))?;
    }
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|directory| directory.sync_all())
            .map_err(runtime_io_error)?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn runtime_rocks_error(error: rocksdb::Error) -> RuntimeError {
    RuntimeError::Application(format!("RocksDB: {error}"))
}

fn runtime_io_error(error: io::Error) -> RuntimeError {
    RuntimeError::Application(format!("I/O: {error}"))
}

fn application_error(message: impl Into<String>) -> RuntimeError {
    RuntimeError::Application(message.into())
}

#[cfg(test)]
mod tests {
    use kuberic_runtime::engine::DurableState;

    use super::*;

    fn operation(lsn: i64, committed_lsn: i64, mutations: Vec<Mutation>) -> Operation {
        Operation {
            lsn,
            committed_lsn,
            data: RocksState::encode_mutations(mutations).unwrap(),
        }
    }

    #[tokio::test]
    async fn append_merge_operator_is_deterministic() {
        let directory = tempfile::tempdir().unwrap();
        let state = RocksState::open(directory.path()).unwrap();
        state
            .apply(operation(
                1,
                0,
                vec![Mutation::Put {
                    key: b"key".to_vec(),
                    value: b"base".to_vec(),
                }],
            ))
            .await
            .unwrap();
        state
            .apply(operation(
                2,
                1,
                vec![Mutation::Merge {
                    key: b"key".to_vec(),
                    value: b"+tail".to_vec(),
                }],
            ))
            .await
            .unwrap();

        assert_eq!(
            state.get(b"key").await.unwrap(),
            Some(Bytes::from_static(b"base+tail"))
        );
    }

    #[tokio::test]
    async fn user_keys_cannot_touch_reserved_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let state = RocksState::open(directory.path()).unwrap();
        let before = state.raw_metadata_for_test(LSN_KEY).await.unwrap();
        state
            .apply(operation(
                1,
                0,
                vec![Mutation::Put {
                    key: LSN_KEY.to_vec(),
                    value: b"user".to_vec(),
                }],
            ))
            .await
            .unwrap();

        assert_eq!(
            state.raw_metadata_for_test(LSN_KEY).await.unwrap(),
            Some(1i64.to_le_bytes().to_vec())
        );
        assert_eq!(
            state.raw_user_value_for_test(LSN_KEY).await.unwrap(),
            Some(b"user".to_vec())
        );
        assert_ne!(before, state.raw_metadata_for_test(LSN_KEY).await.unwrap());
    }
}
