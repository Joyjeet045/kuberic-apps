use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, Operation, OperationDataStream,
    StateProvider,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::protocol::types::{Epoch, OperationId};
use kuberic_runtime::{RuntimeError, application::Lsn};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::dictionary::{Change, Observations, Registry, TransactionId};
use crate::error::{Error, Result};
use crate::log::{MAX_RETAINED_LOG, Record, TransactionLog, atomic_write};

pub const FORMAT: u32 = 2;
pub const MAX_TRANSACTION_BYTES: usize = 1024 * 1024;
pub const MAX_SNAPSHOT_BYTES: usize = 8 * 1024 * 1024;
pub const RETAINED_RESULTS: usize = 1024;
const COMMITTED_PROGRESS_FILE: &str = "committed-progress";
const MAX_COMMITTED_PROGRESS_BYTES: usize = 1024;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct CommitVersion(pub i64);

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Outcome {
    pub identity: TransactionId,
    pub digest: [u8; 32],
    pub version: CommitVersion,
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Snapshot {
    format: u32,
    provider_format: String,
    pub state: Registry,
    pub results: BTreeMap<String, Outcome>,
    pub applied_lsn: i64,
    pub committed_lsn: i64,
}

impl Default for Snapshot {
    fn default() -> Self {
        Self {
            format: FORMAT,
            provider_format: Registry::FORMAT_ID.to_owned(),
            state: Registry::default(),
            results: BTreeMap::new(),
            applied_lsn: 0,
            committed_lsn: 0,
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Envelope {
    format: u32,
    provider_format: String,
    confirmed_lsn: i64,
    identity: TransactionId,
    command: Vec<Change>,
}

#[derive(Serialize, Deserialize)]
struct CommittedProgress {
    format: u32,
    committed_lsn: i64,
}

#[derive(Clone)]
pub(crate) struct PreparedCommit {
    pub payload: Vec<u8>,
    pub retained: Option<CommitVersion>,
}

struct Inner {
    log: TransactionLog,
    snapshot: Snapshot,
    generation: u64,
    copy_root: PathBuf,
}

pub struct ReliableCollectionsState {
    inner: Mutex<Inner>,
}

impl ReliableCollectionsState {
    pub fn open(path: PathBuf) -> Result<Self> {
        let log = TransactionLog::open(path)?;
        let copy_root = log.root().join("copy");
        std::fs::create_dir_all(&copy_root)?;
        let mut snapshot = recover(&log, log.last_lsn())?;
        if let Some(committed_lsn) = read_committed_lsn(log.root(), snapshot.applied_lsn)? {
            snapshot.committed_lsn = snapshot.committed_lsn.max(committed_lsn);
        }
        Ok(Self {
            inner: Mutex::new(Inner {
                log,
                snapshot,
                generation: 0,
                copy_root,
            }),
        })
    }

    pub(crate) fn begin_snapshot(&self) -> Result<(u64, Registry)> {
        let inner = self.lock()?;
        Ok((inner.generation, inner.snapshot.state.clone()))
    }

    pub(crate) fn prepare_commit(
        &self,
        generation: u64,
        identity: TransactionId,
        observations: Observations,
        command: Vec<Change>,
    ) -> Result<PreparedCommit> {
        let mut inner = self.lock()?;
        if generation != inner.generation {
            return Err(Error::StaleEpoch);
        }
        let digest: [u8; 32] = Sha256::digest(postcard::to_allocvec(&command)?).into();
        let retained = retained_result(&inner.snapshot, &identity, digest)?;
        if retained.is_none() {
            inner.snapshot.state.validate(&observations)?;
        }
        let payload = encode_checked(
            &Envelope {
                format: FORMAT,
                provider_format: Registry::FORMAT_ID.to_owned(),
                confirmed_lsn: inner.snapshot.committed_lsn,
                identity,
                command,
            },
            MAX_TRANSACTION_BYTES,
        )?;
        if retained.is_none() {
            let _ = next_snapshot(&inner.snapshot, inner.snapshot.applied_lsn + 1, &payload)?;
        }
        checkpoint_if_needed(&mut inner, payload.len())?;
        inner.log.check_capacity(payload.len())?;
        Ok(PreparedCommit { payload, retained })
    }

    pub(crate) fn apply_standalone(&self, payload: Vec<u8>) -> Result<CommitVersion> {
        let mut inner = self.lock()?;
        let lsn = inner.snapshot.applied_lsn + 1;
        let snapshot = next_snapshot(&inner.snapshot, lsn, &payload)?;
        inner.log.append(Record { lsn, payload })?;
        inner.snapshot = snapshot;
        drop(inner);
        self.commit_prefix(lsn)?;
        Ok(CommitVersion(lsn))
    }

    pub(crate) fn committed_result(
        &self,
        identity: TransactionId,
    ) -> Result<Option<CommitVersion>> {
        let inner = self.lock()?;
        match inner.snapshot.results.get(&identity.request) {
            Some(outcome) if outcome.identity == identity => {
                if outcome.version.0 > inner.snapshot.committed_lsn {
                    return Err(Error::UnconfirmedCommit);
                }
                Ok(Some(outcome.version))
            }
            Some(_) => Err(Error::DuplicateRequest),
            None => Ok(None),
        }
    }

    pub async fn checkpoint(&self) -> Result<()> {
        let mut inner = self.lock()?;
        let target = inner.snapshot.committed_lsn;
        checkpoint_to(&mut inner, target)
    }

    pub async fn backup(&self, destination: PathBuf) -> Result<()> {
        let inner = self.lock()?;
        if inner.snapshot.applied_lsn != inner.snapshot.committed_lsn {
            return Err(Error::RecoveryRequired);
        }
        atomic_write(
            &destination,
            &encode_checked(&inner.snapshot, MAX_SNAPSHOT_BYTES)?,
        )?;
        Ok(())
    }

    pub async fn restore_backup(&self, source: PathBuf) -> Result<()> {
        if std::fs::metadata(&source)?.len() > (MAX_SNAPSHOT_BYTES + 32) as u64 {
            return Err(Error::ResourceExhausted);
        }
        let bytes = std::fs::read(source)?;
        let snapshot = checked_snapshot(&bytes)?;
        let mut inner = self.lock()?;
        if inner.snapshot.applied_lsn != 0 {
            return Err(Error::Invalid(
                "restore requires an unopened empty collections state".into(),
            ));
        }
        inner.log.install_checkpoint(Record {
            lsn: snapshot.applied_lsn,
            payload: bytes,
        })?;
        write_committed_lsn(inner.log.root(), snapshot.committed_lsn)?;
        inner.snapshot = snapshot;
        inner.generation += 1;
        Ok(())
    }

    pub async fn applied_lsn(&self) -> Result<i64> {
        Ok(self.lock()?.snapshot.applied_lsn)
    }

    pub(crate) fn snapshot_bytes(&self, up_to_lsn: i64) -> Result<Vec<u8>> {
        let inner = self.lock()?;
        if up_to_lsn > inner.snapshot.committed_lsn {
            return Err(Error::Invalid(
                "copy boundary must be within committed progress".into(),
            ));
        }
        let mut snapshot = recover(&inner.log, up_to_lsn)?;
        snapshot.committed_lsn = up_to_lsn;
        encode_checked(&snapshot, MAX_SNAPSHOT_BYTES)
    }

    pub(crate) fn update_epoch(&self, previous_epoch_last_lsn: i64) -> Result<()> {
        if previous_epoch_last_lsn < 0 {
            return Err(Error::Invalid("negative previous epoch boundary".into()));
        }
        self.lock()?.generation += 1;
        Ok(())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| Error::Invalid("collections state mutex poisoned".into()))
    }

    fn commit_prefix(&self, committed_lsn: i64) -> Result<DurableApplicationProgress> {
        let mut inner = self.lock()?;
        if committed_lsn < 0 || committed_lsn > inner.snapshot.applied_lsn {
            return Err(Error::Invalid("invalid committed progress".into()));
        }
        if committed_lsn > inner.snapshot.committed_lsn {
            update_committed_lsn(&mut inner, committed_lsn)?;
        }
        Ok(DurableApplicationProgress {
            applied_lsn: inner.snapshot.applied_lsn,
            committed_lsn: inner.snapshot.committed_lsn,
        })
    }
}

pub struct ReliableCollectionsProvider {
    state: std::sync::Arc<ReliableCollectionsState>,
}

impl ReliableCollectionsProvider {
    pub fn new(state: std::sync::Arc<ReliableCollectionsState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl StateProvider for ReliableCollectionsProvider {
    async fn update_epoch(
        &self,
        _epoch: Epoch,
        previous_epoch_last_lsn: Lsn,
    ) -> kuberic_runtime::Result<()> {
        self.state
            .update_epoch(previous_epoch_last_lsn)
            .map_err(Error::runtime)
    }

    async fn last_committed_lsn(&self) -> kuberic_runtime::Result<Lsn> {
        Ok(self.state.durable_progress().await?.committed_lsn)
    }

    async fn get_copy_context(&self) -> kuberic_runtime::Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: Lsn,
        mut copy_context: OperationDataStream,
    ) -> kuberic_runtime::Result<OperationDataStream> {
        if copy_context.next().await.is_some() {
            return Err(RuntimeError::Application(
                "Reliable Collections does not use copy context".into(),
            ));
        }
        let bytes = self
            .state
            .snapshot_bytes(up_to_lsn)
            .map_err(Error::runtime)?;
        Ok(Box::pin(stream::once(
            async move { Ok(Bytes::from(bytes)) },
        )))
    }

    async fn on_data_loss(&self) -> kuberic_runtime::Result<bool> {
        Ok(false)
    }
}

#[async_trait]
impl DurableState for ReliableCollectionsState {
    async fn get_replication_operations(
        &self,
        from_lsn: Lsn,
        to_lsn: Lsn,
    ) -> kuberic_runtime::Result<RetainedOperationStream> {
        let operations = {
            let inner = self.lock().map_err(Error::runtime)?;
            if from_lsn > to_lsn {
                Vec::new()
            } else {
                retained_operations(&inner, from_lsn, to_lsn).map_err(Error::runtime)?
            }
        };
        Ok(Box::pin(stream::iter(operations.into_iter().map(Ok))))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> kuberic_runtime::Result<()> {
        self.stage_copy_chunk(build_id.as_str(), sequence, &chunk.data)
            .map_err(Error::runtime)
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> kuberic_runtime::Result<bool> {
        self.verify_copy_chunk_bytes(build_id.as_str(), sequence, &chunk.data)
            .map_err(Error::runtime)
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: Lsn,
        committed_lsn: Lsn,
    ) -> kuberic_runtime::Result<DurableApplicationProgress> {
        self.finish_copy_inner(build_id.as_str(), up_to_lsn, committed_lsn)
            .map_err(Error::runtime)
    }

    async fn apply(&self, operation: Operation) -> kuberic_runtime::Result<DurableApplicationAck> {
        self.apply_operation(operation).map_err(Error::runtime)
    }

    async fn durable_progress(&self) -> kuberic_runtime::Result<DurableApplicationProgress> {
        let inner = self.lock().map_err(Error::runtime)?;
        Ok(DurableApplicationProgress {
            applied_lsn: inner.snapshot.applied_lsn,
            committed_lsn: inner.snapshot.committed_lsn,
        })
    }

    async fn verify_applied(&self, operation: &Operation) -> kuberic_runtime::Result<bool> {
        self.verify_applied_inner(operation).map_err(Error::runtime)
    }

    async fn commit(
        &self,
        committed_lsn: Lsn,
    ) -> kuberic_runtime::Result<DurableApplicationProgress> {
        self.commit_prefix(committed_lsn).map_err(Error::runtime)
    }
}

impl ReliableCollectionsState {
    fn apply_operation(&self, operation: Operation) -> Result<DurableApplicationProgress> {
        let mut inner = self.lock()?;
        if operation.lsn <= inner.snapshot.applied_lsn {
            if verify_operation(&inner, &operation)? {
                return Ok(DurableApplicationProgress {
                    applied_lsn: inner.snapshot.applied_lsn,
                    committed_lsn: inner.snapshot.committed_lsn,
                });
            }
            return Err(Error::DuplicateRequest);
        }
        if operation.lsn != inner.snapshot.applied_lsn + 1 {
            return Err(Error::Invalid("transaction LSN gap".into()));
        }
        let payload = operation.data.to_vec();
        let previous_committed_lsn = inner.snapshot.committed_lsn;
        let mut snapshot = next_snapshot(&inner.snapshot, operation.lsn, &payload)?;
        let committed_lsn = operation.committed_lsn.min(operation.lsn);
        snapshot.committed_lsn = snapshot.committed_lsn.max(committed_lsn);
        checkpoint_if_needed(&mut inner, payload.len())?;
        inner.log.append(Record {
            lsn: operation.lsn,
            payload,
        })?;
        inner.snapshot = snapshot;
        if committed_lsn > previous_committed_lsn {
            write_committed_lsn(inner.log.root(), committed_lsn)?;
        }
        Ok(DurableApplicationProgress {
            applied_lsn: inner.snapshot.applied_lsn,
            committed_lsn: inner.snapshot.committed_lsn,
        })
    }

    fn verify_applied_inner(&self, operation: &Operation) -> Result<bool> {
        let inner = self.lock()?;
        verify_operation(&inner, operation)
    }

    fn copy_build_dir(root: &Path, build: &str) -> PathBuf {
        let name = build
            .bytes()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        root.join(name)
    }

    fn stage_copy_chunk(&self, build: &str, sequence: u64, bytes: &[u8]) -> Result<()> {
        if build.is_empty() || sequence == 0 {
            return Err(Error::Invalid("invalid copy sequence".into()));
        }
        let copy_root = self.lock()?.copy_root.clone();
        let build_dir = Self::copy_build_dir(&copy_root, build);
        std::fs::create_dir_all(&build_dir)?;
        let path = build_dir.join(format!("{sequence:020}.chunk"));
        match std::fs::read(&path) {
            Ok(existing) => {
                if existing == bytes {
                    return Ok(());
                }
                return Err(Error::Invalid("copy sequence was reused".into()));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        if sequence > 1 {
            let previous = build_dir.join(format!("{:020}.chunk", sequence - 1));
            if !previous.exists() {
                return Err(Error::Invalid("copy sequence gap".into()));
            }
        }
        atomic_write(&path, bytes)?;
        Ok(())
    }

    fn verify_copy_chunk_bytes(&self, build: &str, sequence: u64, bytes: &[u8]) -> Result<bool> {
        let copy_root = self.lock()?.copy_root.clone();
        let path = Self::copy_build_dir(&copy_root, build).join(format!("{sequence:020}.chunk"));
        match std::fs::read(path) {
            Ok(existing) => Ok(existing == bytes),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn finish_copy_inner(
        &self,
        build: &str,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> Result<DurableApplicationProgress> {
        if up_to_lsn != committed_lsn {
            return Err(Error::Invalid(
                "copy completion must equal the committed boundary".into(),
            ));
        }
        let mut inner = self.lock()?;
        let build_dir = Self::copy_build_dir(&inner.copy_root, build);
        let mut bytes = Vec::new();
        let mut expected = 1u64;
        let mut chunks = BTreeMap::new();
        for entry in std::fs::read_dir(&build_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.ends_with(".chunk") {
                continue;
            }
            let sequence = name
                .trim_end_matches(".chunk")
                .parse::<u64>()
                .map_err(|_| Error::Invalid("invalid copy chunk file".into()))?;
            chunks.insert(sequence, entry.path());
        }
        for (sequence, path) in chunks {
            if sequence != expected {
                return Err(Error::Invalid("copy sequence gap".into()));
            }
            bytes.extend_from_slice(&std::fs::read(path)?);
            expected += 1;
        }
        if expected == 1 {
            return Err(Error::Invalid("copy has no staged snapshot".into()));
        }
        let mut snapshot = checked_snapshot(&bytes)?;
        if snapshot.applied_lsn != up_to_lsn || snapshot.committed_lsn != committed_lsn {
            return Err(Error::Invalid("copy boundary mismatch".into()));
        }
        inner.log.install_checkpoint(Record {
            lsn: up_to_lsn,
            payload: bytes,
        })?;
        write_committed_lsn(inner.log.root(), committed_lsn)?;
        snapshot.committed_lsn = committed_lsn;
        inner.snapshot = snapshot;
        inner.generation += 1;
        Ok(DurableApplicationProgress {
            applied_lsn: up_to_lsn,
            committed_lsn,
        })
    }
}

pub(crate) fn encode_checked<T: Serialize>(value: &T, limit: usize) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(value)?;
    if payload.len() > limit {
        return Err(Error::ResourceExhausted);
    }
    let mut bytes = Sha256::digest(&payload).to_vec();
    bytes.extend_from_slice(&payload);
    Ok(bytes)
}

pub(crate) fn decode_checked<T: DeserializeOwned>(bytes: &[u8], limit: usize) -> Result<T> {
    if bytes.len() < 32 || bytes.len() > limit + 32 {
        return Err(Error::Invalid("invalid record length".into()));
    }
    let digest = Sha256::digest(&bytes[32..]);
    if digest[..] != bytes[..32] {
        return Err(Error::Invalid("invalid record checksum".into()));
    }
    let (value, remaining) = postcard::take_from_bytes(&bytes[32..])?;
    if !remaining.is_empty() {
        return Err(Error::Invalid("trailing record bytes".into()));
    }
    Ok(value)
}

fn retained_result(
    snapshot: &Snapshot,
    identity: &TransactionId,
    digest: [u8; 32],
) -> Result<Option<CommitVersion>> {
    if let Some(outcome) = snapshot.results.get(&identity.request) {
        return if &outcome.identity == identity && outcome.digest == digest {
            Ok(Some(outcome.version))
        } else {
            Err(Error::DuplicateRequest)
        };
    }
    if snapshot
        .results
        .values()
        .any(|outcome| outcome.identity.transaction == identity.transaction)
    {
        return Err(Error::DuplicateRequest);
    }
    Ok(None)
}

fn next_snapshot(previous: &Snapshot, lsn: i64, payload: &[u8]) -> Result<Snapshot> {
    if lsn != previous.applied_lsn + 1 {
        return Err(Error::Invalid("transaction LSN gap".into()));
    }
    let envelope: Envelope = decode_checked(payload, MAX_TRANSACTION_BYTES)?;
    if envelope.format != FORMAT
        || envelope.provider_format != Registry::FORMAT_ID
        || envelope.confirmed_lsn < 0
        || envelope.confirmed_lsn >= lsn
        || envelope.identity.request.is_empty()
        || envelope.identity.request.len() > 128
    {
        return Err(Error::Invalid(
            "transaction format or identity mismatch".into(),
        ));
    }
    let digest = Sha256::digest(postcard::to_allocvec(&envelope.command)?).into();
    let mut snapshot = previous.clone();
    snapshot.committed_lsn = snapshot.committed_lsn.max(envelope.confirmed_lsn);
    if retained_result(&snapshot, &envelope.identity, digest)?.is_none() {
        snapshot
            .state
            .apply(&envelope.command, CommitVersion(lsn))?;
        snapshot.results.insert(
            envelope.identity.request.clone(),
            Outcome {
                identity: envelope.identity,
                digest,
                version: CommitVersion(lsn),
            },
        );
        if snapshot.results.len() > RETAINED_RESULTS {
            let oldest = snapshot
                .results
                .iter()
                .min_by_key(|(_, outcome)| outcome.version.0)
                .map(|(key, _)| key.clone())
                .unwrap();
            snapshot.results.remove(&oldest);
        }
    }
    snapshot.applied_lsn = lsn;
    snapshot.state.validate_snapshot(CommitVersion(lsn))?;
    let _ = encode_checked(&snapshot, MAX_SNAPSHOT_BYTES)?;
    Ok(snapshot)
}

fn checked_snapshot(payload: &[u8]) -> Result<Snapshot> {
    let snapshot: Snapshot = decode_checked(payload, MAX_SNAPSHOT_BYTES)?;
    if snapshot.format != FORMAT
        || snapshot.provider_format != Registry::FORMAT_ID
        || snapshot.applied_lsn < 0
        || snapshot.committed_lsn < 0
        || snapshot.committed_lsn > snapshot.applied_lsn
        || snapshot.results.len() > RETAINED_RESULTS
    {
        return Err(Error::Invalid("invalid checkpoint format".into()));
    }
    let mut identities = BTreeSet::new();
    let mut versions = BTreeSet::new();
    for (request, outcome) in &snapshot.results {
        if request != &outcome.identity.request
            || request.is_empty()
            || request.len() > 128
            || outcome.version.0 <= 0
            || outcome.version.0 > snapshot.applied_lsn
            || !identities.insert(outcome.identity.transaction)
            || !versions.insert(outcome.version.0)
        {
            return Err(Error::Invalid("invalid retained transaction result".into()));
        }
    }
    snapshot
        .state
        .validate_snapshot(CommitVersion(snapshot.applied_lsn))?;
    Ok(snapshot)
}

fn recover(log: &TransactionLog, target: i64) -> Result<Snapshot> {
    let mut snapshot = match log.checkpoint_record() {
        Some(record) => {
            let snapshot = checked_snapshot(&record.payload)?;
            if snapshot.applied_lsn != record.lsn {
                return Err(Error::Invalid("checkpoint LSN mismatch".into()));
            }
            snapshot
        }
        None => Snapshot::default(),
    };
    if target < snapshot.applied_lsn || target > log.last_lsn() {
        return Err(Error::Invalid(
            "history unavailable; full copy required".into(),
        ));
    }
    for record in log.records().iter().filter(|record| record.lsn <= target) {
        snapshot = next_snapshot(&snapshot, record.lsn, &record.payload)?;
    }
    Ok(snapshot)
}

fn committed_progress_path(root: &Path) -> PathBuf {
    root.join(COMMITTED_PROGRESS_FILE)
}

fn read_committed_lsn(root: &Path, applied_lsn: i64) -> Result<Option<i64>> {
    let path = committed_progress_path(root);
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let progress: CommittedProgress = decode_checked(&bytes, MAX_COMMITTED_PROGRESS_BYTES)?;
    if progress.format != FORMAT
        || progress.committed_lsn < 0
        || progress.committed_lsn > applied_lsn
    {
        return Err(Error::Invalid("invalid committed-progress record".into()));
    }
    Ok(Some(progress.committed_lsn))
}

fn write_committed_lsn(root: &Path, committed_lsn: i64) -> Result<()> {
    atomic_write(
        &committed_progress_path(root),
        &encode_checked(
            &CommittedProgress {
                format: FORMAT,
                committed_lsn,
            },
            MAX_COMMITTED_PROGRESS_BYTES,
        )?,
    )?;
    Ok(())
}

fn update_committed_lsn(inner: &mut Inner, committed_lsn: i64) -> Result<()> {
    write_committed_lsn(inner.log.root(), committed_lsn)?;
    inner.snapshot.committed_lsn = committed_lsn;
    Ok(())
}

fn retained_operations(inner: &Inner, from_lsn: i64, to_lsn: i64) -> Result<Vec<Operation>> {
    let checkpoint_lsn = inner.log.checkpoint_record().map_or(0, |record| record.lsn);
    if from_lsn <= checkpoint_lsn {
        return Err(Error::Invalid(format!(
            "replication range {from_lsn}..={to_lsn} predates retained checkpoint boundary {checkpoint_lsn}"
        )));
    }
    if to_lsn > inner.snapshot.applied_lsn {
        return Err(Error::Invalid(format!(
            "replication range {from_lsn}..={to_lsn} exceeds applied progress {}",
            inner.snapshot.applied_lsn
        )));
    }
    let mut operations = Vec::new();
    for expected in from_lsn..=to_lsn {
        let record = inner
            .log
            .records()
            .iter()
            .find(|record| record.lsn == expected)
            .ok_or_else(|| {
                Error::Invalid(format!(
                    "retained replication history has a gap at LSN {expected}"
                ))
            })?;
        let envelope: Envelope = decode_checked(&record.payload, MAX_TRANSACTION_BYTES)?;
        operations.push(Operation {
            lsn: record.lsn,
            committed_lsn: envelope.confirmed_lsn,
            data: Bytes::copy_from_slice(&record.payload),
        });
    }
    Ok(operations)
}

fn checkpoint_if_needed(inner: &mut Inner, additional: usize) -> Result<()> {
    if inner
        .log
        .retained_bytes()?
        .saturating_add(additional as u64 + 24)
        > MAX_RETAINED_LOG / 2
        && inner.snapshot.committed_lsn
            > inner.log.checkpoint_record().map_or(0, |record| record.lsn)
    {
        checkpoint_to(inner, inner.snapshot.committed_lsn)?;
    }
    Ok(())
}

fn checkpoint_to(inner: &mut Inner, target: i64) -> Result<()> {
    let mut snapshot = recover(&inner.log, target)?;
    snapshot.committed_lsn = target;
    let record = Record {
        lsn: target,
        payload: encode_checked(&snapshot, MAX_SNAPSHOT_BYTES)?,
    };
    inner.log.checkpoint(record).map_err(Error::from)
}

fn verify_operation(inner: &Inner, operation: &Operation) -> Result<bool> {
    if operation.lsn > inner.snapshot.applied_lsn {
        return Ok(false);
    }
    Ok(inner
        .log
        .records()
        .iter()
        .find(|record| record.lsn == operation.lsn)
        .is_some_and(|record| record.payload == operation.data.as_ref()))
}
