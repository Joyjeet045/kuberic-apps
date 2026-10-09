use anyhow::{Result, ensure};
use rocksdb::{AsColumnFamilyRef, WriteBatch};
use serde::{Deserialize, Serialize};

pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_HTTP_BYTES: usize = 32 * 1024 * 1024;
pub const STORAGE_PROFILE: &str =
    "rust-rocksdb-0.25.0/sys-0.19.0/bytewise/no-compression/append-v1/cfs-v1";
pub const FORMAT_VERSION: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Merge { key: Vec<u8>, value: Vec<u8> },
}

impl Mutation {
    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } | Self::Merge { key, .. } => key,
        }
    }

    pub(crate) fn append(&self, batch: &mut WriteBatch) {
        match self {
            Self::Put { key, value } => batch.put(key, value),
            Self::Delete { key } => batch.delete(key),
            Self::Merge { key, value } => batch.merge(key, value),
        }
    }

    pub(crate) fn append_cf(&self, batch: &mut WriteBatch, cf: &impl AsColumnFamilyRef) {
        match self {
            Self::Put { key, value } => batch.put_cf(cf, key, value),
            Self::Delete { key } => batch.delete_cf(cf, key),
            Self::Merge { key, value } => batch.merge_cf(cf, key, value),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchRequest {
    #[serde(default = "default_cf")]
    pub column_family: String,
    pub operations: Vec<Mutation>,
}

impl BatchRequest {
    pub fn encode(self) -> Result<bytes::Bytes> {
        Envelope::encode(self).map(bytes::Bytes::from)
    }
}

fn default_cf() -> String {
    "default".into()
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Envelope {
    version: u32,
    profile: String,
    pub operations: Vec<Mutation>,
    batch: Vec<u8>,
    checksum: u32,
}

fn canonical(operations: &[Mutation]) -> Result<WriteBatch> {
    ensure!(
        !operations.is_empty() && operations.len() <= 1024,
        "a batch must contain between 1 and 1024 mutations"
    );
    let mut batch = WriteBatch::default();
    for operation in operations {
        ensure!(operation.key().len() <= 64 * 1024, "key exceeds 64 KiB");
        if let Mutation::Put { value, .. } | Mutation::Merge { value, .. } = operation {
            ensure!(value.len() <= MAX_BATCH_BYTES, "value exceeds batch limit");
        }
        operation.append(&mut batch);
        ensure!(batch.data().len() <= MAX_BATCH_BYTES, "batch exceeds 4 MiB");
    }
    Ok(batch)
}

impl Envelope {
    pub fn encode(request: BatchRequest) -> Result<Vec<u8>> {
        ensure!(
            request.column_family == "default",
            "only the default user column family is supported"
        );
        let batch = canonical(&request.operations)?.data().to_vec();
        let value = Self {
            version: FORMAT_VERSION,
            profile: STORAGE_PROFILE.into(),
            checksum: crc32fast::hash(&batch),
            batch,
            operations: request.operations,
        };
        let encoded = serde_json::to_vec(&value)?;
        ensure!(
            encoded.len() <= MAX_HTTP_BYTES,
            "encoded batch exceeds limit"
        );
        Ok(encoded)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        ensure!(bytes.len() <= MAX_HTTP_BYTES, "encoded batch exceeds limit");
        let value: Self = serde_json::from_slice(bytes)?;
        ensure!(value.version == FORMAT_VERSION, "unsupported batch format");
        ensure!(
            value.profile == STORAGE_PROFILE,
            "incompatible RocksDB profile"
        );
        ensure!(
            value.checksum == crc32fast::hash(&value.batch),
            "batch checksum mismatch"
        );
        ensure!(
            canonical(&value.operations)?.data() == value.batch,
            "noncanonical, corrupted, or unsupported WriteBatch"
        );
        Ok(value)
    }

    pub fn write_batch(&self) -> WriteBatch {
        // Reconstruct only after validating against the supported canonical operations.
        WriteBatch::from_data(&self.batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_native_batch_size_and_mutation_limits_are_enforced() {
        let mut probe = WriteBatch::default();
        probe.put(b"k", vec![0; MAX_BATCH_BYTES]);
        let overhead = probe.data().len() - MAX_BATCH_BYTES;
        let request = |size| BatchRequest {
            column_family: "default".into(),
            operations: vec![Mutation::Put {
                key: b"k".to_vec(),
                value: vec![0; size],
            }],
        };
        assert!(request(MAX_BATCH_BYTES - overhead).encode().is_ok());
        assert!(request(MAX_BATCH_BYTES - overhead + 1).encode().is_err());
        for (count, accepted) in [(1024, true), (1025, false)] {
            let request = BatchRequest {
                column_family: "default".into(),
                operations: vec![Mutation::Delete { key: b"k".to_vec() }; count],
            };
            assert_eq!(request.encode().is_ok(), accepted);
        }
        assert!(
            BatchRequest {
                column_family: "default".into(),
                operations: vec![Mutation::Delete {
                    key: vec![0; 64 * 1024 + 1]
                }],
            }
            .encode()
            .is_err()
        );
    }
}
