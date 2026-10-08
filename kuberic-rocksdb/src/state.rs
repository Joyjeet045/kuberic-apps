use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use kuberic_runtime::RuntimeError;
use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, Operation, OperationDataStream,
    StateProvider,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::protocol::types::{Epoch, OperationId};
use rocksdb::{
    ColumnFamilyDescriptor, DB, DBCompressionType, IteratorMode, MergeOperands, Options,
    WriteBatch, WriteOptions, checkpoint::Checkpoint,
};
use serde::{Deserialize, Serialize};

use crate::batch::{Envelope, FORMAT_VERSION, STORAGE_PROFILE};
use crate::checkpoint::{self, COPY_CHUNK_BYTES, MAX_COPY_BYTES};

const META: &str = "__kuberic_metadata";
const ACCEPTED: &str = "__kuberic_accepted";
const PROGRESS: &[u8] = b"progress";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Progress {
    version: u32,
    profile: String,
    base: i64,
    applied: i64,
    committed: i64,
}

impl Progress {
    fn at(lsn: i64) -> Self {
        Self {
            version: FORMAT_VERSION,
            profile: STORAGE_PROFILE.into(),
            base: lsn,
            applied: lsn,
            committed: lsn,
        }
    }

    fn validate(&self) -> Result<()> {
        ensure!(self.version == FORMAT_VERSION, "unsupported storage format");
        ensure!(
            self.profile == STORAGE_PROFILE,
            "incompatible RocksDB storage profile"
        );
        ensure!(
            0 <= self.base && self.base <= self.committed && self.committed <= self.applied,
            "invalid durable replication progress"
        );
        Ok(())
    }

    fn public(&self) -> DurableApplicationProgress {
        DurableApplicationProgress {
            applied_lsn: self.applied,
            committed_lsn: self.committed,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    lsn: i64,
    committed_lsn: i64,
    data: Vec<u8>,
    undo: Vec<(Vec<u8>, Option<Vec<u8>>)>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CopyState {
    count: u64,
    bytes: usize,
    installed: Option<(i64, u32)>,
}

struct Store {
    root: PathBuf,
    catalog: DB,
    db: DB,
    progress: Progress,
    fenced: bool,
    #[cfg(test)]
    fail_after_data_write: bool,
}

#[derive(Clone)]
pub struct RocksState {
    root: PathBuf,
    store: Arc<Mutex<Option<Store>>>,
}

impl RocksState {
    /// Does not create or open storage before ReplicaHost authorizes the process.
    pub fn deferred(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            store: Arc::new(Mutex::new(None)),
        }
    }

    pub fn is_fresh_empty(root: &Path) -> std::io::Result<bool> {
        match fs::read_dir(root) {
            Ok(mut entries) => Ok(entries.next().transpose()?.is_none()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
            Err(error) => Err(error),
        }
    }

    pub async fn initialize(&self) -> kuberic_runtime::Result<()> {
        let state = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = state
                .store
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            ensure!(guard.is_none(), "RocksDB storage is already open");
            *guard = Some(Store::open(&state.root)?);
            Ok(())
        })
        .await
        .map_err(runtime_error)?
        .map_err(runtime_error)
    }

    pub async fn close(&self) -> kuberic_runtime::Result<()> {
        let state = self.clone();
        tokio::task::spawn_blocking(move || {
            state
                .store
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?
                .take();
            Ok::<_, anyhow::Error>(())
        })
        .await
        .map_err(runtime_error)?
        .map_err(runtime_error)
    }

    async fn run<T, F>(&self, action: F) -> kuberic_runtime::Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&mut Store) -> Result<T> + Send + 'static,
    {
        let state = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = state
                .store
                .lock()
                .map_err(|_| anyhow::anyhow!("storage lock poisoned"))?;
            let store = guard.as_mut().context("RocksDB storage is not open")?;
            ensure!(
                !store.fenced,
                "storage write outcome is uncertain; reopen required"
            );
            action(store)
        })
        .await
        .map_err(runtime_error)?
        .map_err(runtime_error)
    }

    pub async fn get(&self, key: Vec<u8>) -> kuberic_runtime::Result<Option<Vec<u8>>> {
        self.run(move |store| Ok(store.db.get(key)?)).await
    }

    pub async fn checkpoint(&self, up_to_lsn: i64) -> kuberic_runtime::Result<Bytes> {
        self.run(move |store| store.snapshot(up_to_lsn).map(Bytes::from))
            .await
    }
}

fn runtime_error(error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Application(error.to_string())
}

fn options(create: bool) -> Options {
    let mut options = Options::default();
    options.create_if_missing(create);
    options.create_missing_column_families(create);
    options.set_compression_type(DBCompressionType::None);
    options.set_atomic_flush(true);
    options.set_write_buffer_size(4 * 1024 * 1024);
    options.set_max_open_files(64);
    options.set_max_background_jobs(2);
    options.set_merge_operator_associative("kuberic.append-v1", append_merge);
    options
}

fn append_merge(_key: &[u8], value: Option<&[u8]>, operands: &MergeOperands) -> Option<Vec<u8>> {
    let mut result = value.unwrap_or_default().to_vec();
    for operand in operands {
        result.extend_from_slice(operand);
    }
    Some(result)
}

fn open_data(path: &Path, create: bool) -> Result<DB> {
    if !create {
        let actual = DB::list_cf(&options(false), path)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let expected = ["default", META, ACCEPTED]
            .map(str::to_owned)
            .into_iter()
            .collect();
        ensure!(actual == expected, "unsupported column-family layout");
    }
    let descriptors = ["default", META, ACCEPTED]
        .into_iter()
        .map(|name| ColumnFamilyDescriptor::new(name, options(create)));
    Ok(DB::open_cf_descriptors(
        &options(create),
        path,
        descriptors,
    )?)
}

fn sync_options() -> WriteOptions {
    let mut options = WriteOptions::default();
    options.set_sync(true);
    options.disable_wal(false);
    options
}

fn log_key(lsn: i64) -> Vec<u8> {
    let mut key = b"operation/".to_vec();
    key.extend_from_slice(&lsn.to_be_bytes());
    key
}

fn copy_key(build: &OperationId) -> Result<String> {
    ensure!(build.as_str().len() <= 1024, "copy identity is too long");
    let encoded = build
        .as_str()
        .bytes()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!("copy/{encoded}"))
}

impl Store {
    fn open(root: &Path) -> Result<Self> {
        let fresh = RocksState::is_fresh_empty(root)?;
        fs::create_dir_all(root)?;
        let catalog_path = root.join("catalog");
        ensure!(
            fresh || catalog_path.join("CURRENT").is_file(),
            "missing catalog in established storage"
        );
        let catalog = DB::open(&options(fresh), catalog_path)?;
        let db;
        if fresh {
            db = open_data(&root.join("generation-1"), true)?;
            let mut batch = WriteBatch::default();
            batch.put_cf(
                db.cf_handle(META).context("metadata family missing")?,
                PROGRESS,
                serde_json::to_vec(&Progress::at(0))?,
            );
            db.write_opt(batch, &sync_options())?;
            checkpoint::sync_directory(root)?;
            let mut batch = WriteBatch::default();
            batch.put(b"profile", STORAGE_PROFILE);
            batch.put(b"active", 1_u64.to_le_bytes());
            batch.put(b"next-generation", 2_u64.to_le_bytes());
            catalog.write_opt(batch, &sync_options())?;
        } else {
            ensure!(
                catalog.get(b"profile")?.as_deref() == Some(STORAGE_PROFILE.as_bytes()),
                "incompatible or missing storage profile"
            );
            let active = read_u64(&catalog, b"active")?;
            db = open_data(&root.join(format!("generation-{active}")), false)?;
        }
        let metadata = db.cf_handle(META).context("metadata family missing")?;
        let progress: Progress =
            serde_json::from_slice(&db.get_cf(metadata, PROGRESS)?.context("missing progress")?)?;
        progress.validate()?;
        let store = Self {
            root: root.to_owned(),
            catalog,
            db,
            progress,
            fenced: false,
            #[cfg(test)]
            fail_after_data_write: false,
        };
        for lsn in store.progress.base + 1..=store.progress.applied {
            let record = store.record(lsn)?;
            ensure!(record.lsn == lsn, "retained operation identity mismatch");
            ensure!(
                record.committed_lsn >= 0 && record.committed_lsn <= lsn,
                "invalid retained watermark"
            );
            Envelope::decode(&record.data)?;
        }
        Ok(store)
    }

    fn record(&self, lsn: i64) -> Result<Record> {
        let metadata = self.db.cf_handle(META).context("metadata family missing")?;
        let bytes = self
            .db
            .get_cf(metadata, log_key(lsn))?
            .context("required operation is not retained; rebuild from checkpoint")?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn write_data(&mut self, batch: WriteBatch) -> Result<()> {
        self.fenced = true;
        self.db.write_opt(batch, &sync_options())?;
        #[cfg(test)]
        if std::mem::take(&mut self.fail_after_data_write) {
            anyhow::bail!("injected error after durable data write");
        }
        self.fenced = false;
        Ok(())
    }

    fn write_catalog(&mut self, batch: WriteBatch) -> Result<()> {
        self.fenced = true;
        self.catalog.write_opt(batch, &sync_options())?;
        self.fenced = false;
        Ok(())
    }

    fn commit(&mut self, target: i64) -> Result<DurableApplicationProgress> {
        ensure!(
            target >= 0 && target <= self.progress.applied,
            "commit exceeds durable applied progress"
        );
        for lsn in self.progress.committed + 1..=target {
            let record = self.record(lsn)?;
            let mut batch = Envelope::decode(&record.data)?.write_batch();
            let mut progress = self.progress.clone();
            progress.committed = lsn;
            batch.put_cf(
                self.db.cf_handle(META).context("metadata family missing")?,
                PROGRESS,
                serde_json::to_vec(&progress)?,
            );
            self.write_data(batch)?;
            self.progress = progress;
        }
        Ok(self.progress.public())
    }

    fn apply(&mut self, operation: Operation) -> Result<DurableApplicationAck> {
        ensure!(
            operation.lsn > self.progress.base,
            "operation predates retained base"
        );
        ensure!(
            operation.committed_lsn >= 0 && operation.committed_lsn <= operation.lsn,
            "invalid operation watermark"
        );
        let envelope = Envelope::decode(&operation.data)?;
        if operation.lsn <= self.progress.applied {
            let original = self.record(operation.lsn)?;
            ensure!(
                original.data == operation.data
                    && original.committed_lsn == operation.committed_lsn,
                "LSN reused with different operation bytes or watermark"
            );
            return self.commit(self.progress.committed.max(operation.committed_lsn));
        }
        ensure!(
            self.progress.applied.checked_add(1) == Some(operation.lsn),
            "replication LSN gap"
        );
        self.commit(operation.committed_lsn.min(self.progress.applied))?;
        let accepted = self
            .db
            .cf_handle(ACCEPTED)
            .context("accepted family missing")?;
        let metadata = self.db.cf_handle(META).context("metadata family missing")?;
        let mut undo = BTreeMap::new();
        let mut batch = WriteBatch::default();
        for mutation in &envelope.operations {
            if !undo.contains_key(mutation.key()) {
                undo.insert(
                    mutation.key().to_vec(),
                    self.db.get_cf(accepted, mutation.key())?,
                );
            }
            mutation.append_cf(&mut batch, accepted);
        }
        let record = Record {
            lsn: operation.lsn,
            committed_lsn: operation.committed_lsn,
            data: operation.data.to_vec(),
            undo: undo.into_iter().collect(),
        };
        let mut progress = self.progress.clone();
        progress.applied = operation.lsn;
        batch.put_cf(
            metadata,
            log_key(operation.lsn),
            serde_json::to_vec(&record)?,
        );
        batch.put_cf(metadata, PROGRESS, serde_json::to_vec(&progress)?);
        // Accepted data, exact retry identity, undo, and progress share one synced WAL write.
        self.write_data(batch)?;
        self.progress = progress;
        self.commit(self.progress.committed.max(operation.committed_lsn))
    }

    fn snapshot(&mut self, lsn: i64) -> Result<Vec<u8>> {
        ensure!(
            lsn >= self.progress.base && lsn <= self.progress.committed,
            "copy boundary is outside retained committed history"
        );
        let cache = format!("checkpoint/{lsn}");
        if let Some(bytes) = self.catalog.get(&cache)? {
            return Ok(bytes);
        }
        let scratch = tempfile::tempdir_in(&self.root)?;
        let prepared_path = scratch.path().join("prepared");
        Checkpoint::new(&self.db)?.create_checkpoint(&prepared_path)?;
        let prepared = open_data(&prepared_path, false)?;
        let mut batch = WriteBatch::default();
        for (family, high) in [
            ("default", self.progress.committed),
            (ACCEPTED, self.progress.applied),
        ] {
            let cf = prepared
                .cf_handle(family)
                .context("checkpoint column family missing")?;
            for position in (lsn + 1..=high).rev() {
                for (key, value) in self.record(position)?.undo {
                    match value {
                        Some(value) => batch.put_cf(cf, key, value),
                        None => batch.delete_cf(cf, key),
                    }
                }
            }
        }
        let metadata = prepared
            .cf_handle(META)
            .context("checkpoint metadata missing")?;
        for entry in prepared.iterator_cf(metadata, IteratorMode::Start) {
            let (key, _) = entry?;
            batch.delete_cf(metadata, key);
        }
        batch.put_cf(metadata, PROGRESS, serde_json::to_vec(&Progress::at(lsn))?);
        prepared.write_opt(batch, &sync_options())?;
        let final_path = scratch.path().join("checkpoint");
        Checkpoint::new(&prepared)?.create_checkpoint(&final_path)?;
        let bytes = checkpoint::pack(&final_path, lsn)?;
        // Persist the exact physical bytes before exposing them to an authorized build.
        self.catalog.put_opt(cache, &bytes, &sync_options())?;
        drop(prepared);
        scratch.close()?;
        Ok(bytes)
    }

    fn copy_state(&self, key: &str) -> Result<CopyState> {
        self.catalog
            .get(key)?
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .map(|state| state.unwrap_or_default())
            .map_err(Into::into)
    }

    fn apply_chunk(&mut self, build: &OperationId, sequence: u64, bytes: &[u8]) -> Result<()> {
        ensure!(
            sequence > 0 && !bytes.is_empty() && bytes.len() <= COPY_CHUNK_BYTES,
            "invalid copy chunk"
        );
        let key = copy_key(build)?;
        let chunk_key = format!("{key}/chunk/{sequence:020}");
        if let Some(existing) = self.catalog.get(&chunk_key)? {
            ensure!(
                existing == bytes,
                "copy sequence reused with different bytes"
            );
            return Ok(());
        }
        let mut state = self.copy_state(&key)?;
        ensure!(state.installed.is_none(), "copy is already installed");
        ensure!(
            state.count.checked_add(1) == Some(sequence),
            "copy sequence gap"
        );
        state.bytes = state
            .bytes
            .checked_add(bytes.len())
            .context("copy size overflow")?;
        ensure!(state.bytes <= MAX_COPY_BYTES, "checkpoint exceeds 128 MiB");
        state.count = sequence;
        let mut batch = WriteBatch::default();
        batch.put(chunk_key, bytes);
        batch.put(&key, serde_json::to_vec(&state)?);
        self.write_catalog(batch)?;
        Ok(())
    }

    fn finish_copy(
        &mut self,
        build: &OperationId,
        lsn: i64,
        committed: i64,
    ) -> Result<DurableApplicationProgress> {
        ensure!(
            lsn == committed && lsn >= 0,
            "copy must install a committed boundary"
        );
        let key = copy_key(build)?;
        let mut state = self.copy_state(&key)?;
        ensure!(state.count > 0, "copy contains no checkpoint");
        if let Some((installed, _)) = state.installed {
            ensure!(installed == lsn, "copy completion boundary changed");
            return Ok(self.progress.public());
        }
        ensure!(
            lsn >= self.progress.committed,
            "copy would regress committed state"
        );
        let mut bytes = Vec::with_capacity(state.bytes);
        for sequence in 1..=state.count {
            bytes.extend_from_slice(
                &self
                    .catalog
                    .get(format!("{key}/chunk/{sequence:020}"))?
                    .context("missing durable copy chunk")?,
            );
        }
        ensure!(bytes.len() == state.bytes, "copy length mismatch");
        let generation = read_u64(&self.catalog, b"next-generation")?;
        self.catalog.put_opt(
            b"next-generation",
            generation
                .checked_add(1)
                .context("generation exhausted")?
                .to_le_bytes(),
            &sync_options(),
        )?;
        let destination = self.root.join(format!("generation-{generation}"));
        checkpoint::unpack(&bytes, &destination, lsn)?;
        let restored = open_data(&destination, false)?;
        let metadata = restored
            .cf_handle(META)
            .context("restored metadata missing")?;
        let progress: Progress = serde_json::from_slice(
            &restored
                .get_cf(metadata, PROGRESS)?
                .context("restored progress missing")?,
        )?;
        progress.validate()?;
        ensure!(
            progress.base == lsn && progress.applied == lsn && progress.committed == lsn,
            "checkpoint progress mismatch"
        );
        let accepted = restored
            .cf_handle(ACCEPTED)
            .context("restored accepted family missing")?;
        let visible = restored
            .iterator(IteratorMode::Start)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let staged = restored
            .iterator_cf(accepted, IteratorMode::Start)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(visible == staged, "checkpoint contains uncommitted data");
        checkpoint::sync_directory(&destination)?;
        checkpoint::sync_directory(&self.root)?;
        state.installed = Some((lsn, crc32fast::hash(&bytes)));
        let mut batch = WriteBatch::default();
        batch.put(b"active", generation.to_le_bytes());
        batch.put(&key, serde_json::to_vec(&state)?);
        for entry in self.catalog.prefix_iterator(b"checkpoint/") {
            let (cache_key, _) = entry?;
            if !cache_key.starts_with(b"checkpoint/") {
                break;
            }
            batch.delete(cache_key);
        }
        self.write_catalog(batch)?;
        self.db = restored;
        self.progress = progress;
        Ok(self.progress.public())
    }
}

fn read_u64(db: &DB, key: &[u8]) -> Result<u64> {
    Ok(u64::from_le_bytes(
        db.get(key)?
            .context("missing catalog entry")?
            .as_slice()
            .try_into()?,
    ))
}

#[async_trait]
impl DurableState for RocksState {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> kuberic_runtime::Result<RetainedOperationStream> {
        let operations = self
            .run(move |store| {
                if from_lsn > to_lsn {
                    return Ok(Vec::new());
                }
                ensure!(
                    from_lsn > store.progress.base && to_lsn <= store.progress.applied,
                    "requested history is not retained; checkpoint rebuild required"
                );
                (from_lsn..=to_lsn)
                    .map(|lsn| {
                        let record = store.record(lsn)?;
                        Ok(Operation {
                            lsn,
                            committed_lsn: record.committed_lsn,
                            data: record.data.into(),
                        })
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .await?;
        Ok(Box::pin(stream::iter(operations.into_iter().map(Ok))))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> kuberic_runtime::Result<()> {
        let build = build_id.clone();
        self.run(move |store| store.apply_chunk(&build, sequence, &chunk.data))
            .await
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> kuberic_runtime::Result<bool> {
        let build = build_id.clone();
        let bytes = chunk.data.clone();
        self.run(move |store| {
            Ok(store
                .catalog
                .get(format!("{}/chunk/{sequence:020}", copy_key(&build)?))?
                .as_deref()
                == Some(bytes.as_ref()))
        })
        .await
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> kuberic_runtime::Result<DurableApplicationProgress> {
        let build = build_id.clone();
        self.run(move |store| store.finish_copy(&build, up_to_lsn, committed_lsn))
            .await
    }

    async fn apply(&self, operation: Operation) -> kuberic_runtime::Result<DurableApplicationAck> {
        self.run(move |store| store.apply(operation)).await
    }

    async fn durable_progress(&self) -> kuberic_runtime::Result<DurableApplicationProgress> {
        self.run(|store| Ok(store.progress.public())).await
    }

    async fn verify_applied(&self, operation: &Operation) -> kuberic_runtime::Result<bool> {
        let operation = operation.clone();
        self.run(move |store| {
            if operation.lsn <= store.progress.base || operation.lsn > store.progress.applied {
                return Ok(false);
            }
            let record = store.record(operation.lsn)?;
            Ok(record.data == operation.data && record.committed_lsn == operation.committed_lsn)
        })
        .await
    }

    async fn commit(
        &self,
        committed_lsn: i64,
    ) -> kuberic_runtime::Result<DurableApplicationProgress> {
        self.run(move |store| store.commit(committed_lsn)).await
    }
}

#[async_trait]
impl StateProvider for RocksState {
    async fn update_epoch(
        &self,
        epoch: Epoch,
        previous_epoch_last_lsn: i64,
    ) -> kuberic_runtime::Result<()> {
        self.run(move |store| {
            ensure!(previous_epoch_last_lsn >= 0, "negative epoch boundary");
            if let Some(bytes) = store.catalog.get(b"epoch")? {
                let previous: Epoch = serde_json::from_slice(&bytes)?;
                ensure!(epoch >= previous, "application epoch regressed");
            }
            // V2 authority recovery owns prefix selection; accepted suffixes remain invisible.
            store
                .catalog
                .put_opt(b"epoch", serde_json::to_vec(&epoch)?, &sync_options())?;
            Ok(())
        })
        .await
    }

    async fn last_committed_lsn(&self) -> kuberic_runtime::Result<i64> {
        Ok(self.durable_progress().await?.committed_lsn)
    }

    async fn get_copy_context(&self) -> kuberic_runtime::Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut copy_context: OperationDataStream,
    ) -> kuberic_runtime::Result<OperationDataStream> {
        if copy_context.next().await.is_some() {
            return Err(runtime_error("RocksDB does not accept custom copy context"));
        }
        let bytes = self.checkpoint(up_to_lsn).await?;
        let chunks = (0..bytes.len())
            .step_by(COPY_CHUNK_BYTES)
            .map(|start| Ok(bytes.slice(start..(start + COPY_CHUNK_BYTES).min(bytes.len()))))
            .collect::<Vec<_>>();
        Ok(Box::pin(stream::iter(chunks)))
    }

    async fn on_data_loss(&self) -> kuberic_runtime::Result<bool> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{BatchRequest, Mutation};

    #[tokio::test]
    async fn uncertain_synced_write_fences_until_reopen_without_reapplying_merge() {
        let root = tempfile::tempdir().unwrap();
        let state = RocksState::deferred(root.path());
        state.initialize().await.unwrap();
        let operation = Operation {
            lsn: 1,
            committed_lsn: 0,
            data: BatchRequest {
                column_family: "default".into(),
                operations: vec![Mutation::Merge {
                    key: b"k".to_vec(),
                    value: b"A".to_vec(),
                }],
            }
            .encode()
            .unwrap(),
        };
        state
            .run(|store| {
                store.fail_after_data_write = true;
                Ok(())
            })
            .await
            .unwrap();
        assert!(state.apply(operation.clone()).await.is_err());
        assert!(state.apply(operation.clone()).await.is_err());
        assert!(state.get(b"k".to_vec()).await.is_err());
        state.close().await.unwrap();
        state.initialize().await.unwrap();
        state.apply(operation).await.unwrap();
        state
            .run(|store| {
                store.fail_after_data_write = true;
                Ok(())
            })
            .await
            .unwrap();
        assert!(state.commit(1).await.is_err());
        assert!(state.get(b"k".to_vec()).await.is_err());
        state.close().await.unwrap();
        state.initialize().await.unwrap();
        state.commit(1).await.unwrap();
        assert_eq!(state.get(b"k".to_vec()).await.unwrap().unwrap(), b"A");
        state.close().await.unwrap();
    }
}
