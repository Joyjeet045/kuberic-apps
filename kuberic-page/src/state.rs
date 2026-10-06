use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use bytes::{BufMut, Bytes, BytesMut};
use futures::{StreamExt, stream};
use kuberic_runtime::application::{
    CopyChunk, DurableApplicationAck, DurableApplicationProgress, Operation, OperationDataStream,
    StateProvider,
};
use kuberic_runtime::engine::{DurableState, RetainedOperationStream};
use kuberic_runtime::protocol::types::{Epoch, OperationId};
use kuberic_runtime::{Result, RuntimeError};

#[derive(Clone)]
struct OperationRecord {
    committed_lsn: i64,
    data: Bytes,
}

#[derive(Default)]
struct PageData {
    page: Option<Bytes>,
    base_page: Option<Bytes>,
    base_lsn: i64,
    operations: BTreeMap<i64, OperationRecord>,
    applied_lsn: i64,
    committed_lsn: i64,
    epoch: Epoch,
    copy_builds: BTreeMap<OperationId, BTreeMap<u64, Bytes>>,
}

#[derive(Default)]
pub struct PageState {
    data: Mutex<PageData>,
}

impl PageState {
    pub fn page(&self) -> Option<Bytes> {
        self.data.lock().unwrap().page.clone()
    }

    fn page_at(&self, up_to_lsn: i64) -> Result<Option<Bytes>> {
        let data = self.data.lock().unwrap();
        if up_to_lsn < data.base_lsn {
            return Err(RuntimeError::Application(format!(
                "copy boundary {up_to_lsn} predates retained base {}",
                data.base_lsn
            )));
        }
        let mut page = data.base_page.clone();
        for record in data
            .operations
            .range((data.base_lsn + 1)..=up_to_lsn)
            .map(|(_, record)| record)
        {
            page = Some(record.data.clone());
        }
        Ok(page)
    }

    fn update_epoch(&self, epoch: Epoch) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        if epoch < data.epoch {
            return Err(RuntimeError::AuthorityMismatch(
                "application epoch regressed".into(),
            ));
        }
        data.epoch = epoch;
        Ok(())
    }
}

#[async_trait]
impl DurableState for PageState {
    async fn get_replication_operations(
        &self,
        from_lsn: i64,
        to_lsn: i64,
    ) -> Result<RetainedOperationStream> {
        if from_lsn > to_lsn {
            return Ok(Box::pin(stream::empty()));
        }
        let operations = self
            .data
            .lock()
            .unwrap()
            .operations
            .range(from_lsn..=to_lsn)
            .map(|(lsn, record)| {
                Ok(Operation {
                    lsn: *lsn,
                    committed_lsn: record.committed_lsn,
                    data: record.data.clone(),
                })
            })
            .collect::<Vec<_>>();
        Ok(Box::pin(stream::iter(operations)))
    }

    async fn apply_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: CopyChunk,
    ) -> Result<()> {
        let mut data = self.data.lock().unwrap();
        let chunks = data.copy_builds.entry(build_id.clone()).or_default();
        if let Some(existing) = chunks.get(&sequence) {
            if existing != &chunk.data {
                return Err(RuntimeError::AuthorityMismatch(
                    "copy sequence was reused with different bytes".into(),
                ));
            }
            return Ok(());
        }
        chunks.insert(sequence, chunk.data);
        Ok(())
    }

    async fn verify_copy_chunk(
        &self,
        build_id: &OperationId,
        sequence: u64,
        chunk: &CopyChunk,
    ) -> Result<bool> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .copy_builds
            .get(build_id)
            .and_then(|chunks| chunks.get(&sequence))
            == Some(&chunk.data))
    }

    async fn finish_copy(
        &self,
        build_id: &OperationId,
        up_to_lsn: i64,
        committed_lsn: i64,
    ) -> Result<DurableApplicationProgress> {
        let mut data = self.data.lock().unwrap();
        let chunks = data
            .copy_builds
            .get(build_id)
            .ok_or_else(|| RuntimeError::Application("copy contained no page snapshot".into()))?;
        let mut snapshot = Vec::new();
        for (expected, (sequence, chunk)) in chunks.iter().enumerate() {
            if *sequence != expected as u64 + 1 {
                return Err(RuntimeError::Application(
                    "copy page snapshot had a sequence gap".into(),
                ));
            }
            snapshot.extend_from_slice(chunk);
        }
        let page = decode_snapshot(&snapshot)?;
        data.page = page.clone();
        data.base_page = page;
        data.base_lsn = up_to_lsn;
        data.operations.clear();
        data.applied_lsn = up_to_lsn;
        data.committed_lsn = committed_lsn;
        Ok(DurableApplicationProgress {
            applied_lsn: up_to_lsn,
            committed_lsn,
        })
    }

    async fn apply(&self, operation: Operation) -> Result<DurableApplicationAck> {
        let mut data = self.data.lock().unwrap();
        if let Some(existing) = data.operations.get(&operation.lsn) {
            if existing.data == operation.data && existing.committed_lsn == operation.committed_lsn
            {
                return Ok(DurableApplicationProgress {
                    applied_lsn: data.applied_lsn,
                    committed_lsn: data.committed_lsn,
                });
            }
            return Err(RuntimeError::AuthorityMismatch(
                "LSN was reused with different page data".into(),
            ));
        }
        data.page = Some(operation.data.clone());
        data.operations.insert(
            operation.lsn,
            OperationRecord {
                committed_lsn: operation.committed_lsn,
                data: operation.data,
            },
        );
        data.applied_lsn = data.applied_lsn.max(operation.lsn);
        data.committed_lsn = data.committed_lsn.max(operation.committed_lsn);
        Ok(DurableApplicationProgress {
            applied_lsn: data.applied_lsn,
            committed_lsn: data.committed_lsn,
        })
    }

    async fn durable_progress(&self) -> Result<DurableApplicationProgress> {
        let data = self.data.lock().unwrap();
        Ok(DurableApplicationProgress {
            applied_lsn: data.applied_lsn,
            committed_lsn: data.committed_lsn,
        })
    }

    async fn verify_applied(&self, operation: &Operation) -> Result<bool> {
        Ok(self
            .data
            .lock()
            .unwrap()
            .operations
            .get(&operation.lsn)
            .is_some_and(|record| {
                record.data == operation.data && record.committed_lsn == operation.committed_lsn
            }))
    }

    async fn commit(&self, committed_lsn: i64) -> Result<DurableApplicationProgress> {
        let mut data = self.data.lock().unwrap();
        if committed_lsn > data.applied_lsn {
            return Err(RuntimeError::Application(
                "cannot commit beyond applied page progress".into(),
            ));
        }
        data.committed_lsn = data.committed_lsn.max(committed_lsn);
        Ok(DurableApplicationProgress {
            applied_lsn: data.applied_lsn,
            committed_lsn: data.committed_lsn,
        })
    }
}

pub struct PageStateProvider {
    state: std::sync::Arc<PageState>,
}

impl PageStateProvider {
    pub fn new(state: std::sync::Arc<PageState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl StateProvider for PageStateProvider {
    async fn update_epoch(&self, epoch: Epoch, _previous_epoch_last_lsn: i64) -> Result<()> {
        self.state.update_epoch(epoch)
    }

    async fn last_committed_lsn(&self) -> Result<i64> {
        Ok(self.state.durable_progress().await?.committed_lsn)
    }

    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut copy_context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        if copy_context.next().await.is_some() {
            return Err(RuntimeError::Application(
                "kuberic-page does not use a copy context".into(),
            ));
        }
        let snapshot = encode_snapshot(self.state.page_at(up_to_lsn)?);
        Ok(Box::pin(stream::once(async move { Ok(snapshot) })))
    }

    async fn on_data_loss(&self) -> Result<bool> {
        Ok(false)
    }
}

fn encode_snapshot(page: Option<Bytes>) -> Bytes {
    let mut snapshot = BytesMut::with_capacity(1 + page.as_ref().map_or(0, bytes::Bytes::len));
    match page {
        Some(page) => {
            snapshot.put_u8(1);
            snapshot.extend_from_slice(&page);
        }
        None => snapshot.put_u8(0),
    }
    snapshot.freeze()
}

fn decode_snapshot(snapshot: &[u8]) -> Result<Option<Bytes>> {
    match snapshot.split_first() {
        Some((0, [])) => Ok(None),
        Some((1, page)) => Ok(Some(Bytes::copy_from_slice(page))),
        _ => Err(RuntimeError::Application(
            "copy page snapshot has an invalid encoding".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use kuberic_runtime::application::CopyChunk;
    use kuberic_runtime::engine::DurableState;

    use super::*;

    #[tokio::test]
    async fn copy_uses_one_based_sequences_and_completion_is_retryable() {
        let state = PageState::default();
        let build = OperationId::new("copy");
        state
            .apply_copy_chunk(
                &build,
                1,
                CopyChunk {
                    data: Bytes::from_static(&[0]),
                },
            )
            .await
            .unwrap();

        let first = state.finish_copy(&build, 4, 3).await.unwrap();
        let retry = state.finish_copy(&build, 4, 3).await.unwrap();

        assert_eq!(first, retry);
        assert_eq!(state.page(), None);
    }

    #[tokio::test]
    async fn copy_distinguishes_absent_page_from_present_empty_page() {
        let state = PageState::default();
        let build = OperationId::new("empty-page-copy");
        state
            .apply_copy_chunk(
                &build,
                1,
                CopyChunk {
                    data: Bytes::from_static(&[1]),
                },
            )
            .await
            .unwrap();
        state.finish_copy(&build, 1, 1).await.unwrap();

        assert_eq!(state.page(), Some(Bytes::new()));
    }
}
