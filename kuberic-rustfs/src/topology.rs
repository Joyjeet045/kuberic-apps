use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, ensure};
use reqwest::Url;
use serde::{Deserialize, Serialize};

const MAX_ENDPOINTS: usize = 1024;
const MAX_RECORD_BYTES: usize = 64 * 1024;
const RECORD: &str = "topology.json";
const LOCK: &str = "owner.lock";
const ACTIVE: &str = "process-active";

/// Ordered RustFS pool arguments. Changing an established layout is not a restart.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Topology {
    pub pools: Vec<String>,
    /// Direct HTTP origin identifying this node in distributed pool arguments.
    /// `None` selects local-directory pool arguments.
    pub local_node: Option<String>,
    pub erasure_set_drive_count: Option<u16>,
}

impl Topology {
    /// Expand the supported numeric ellipses and derive this node's owned paths.
    /// RustFS still validates its native layout, parity, and quorum.
    pub fn local_volumes(&self) -> Result<Vec<PathBuf>> {
        ensure!(
            !self.pools.is_empty(),
            "RustFS topology needs at least one pool"
        );
        if let Some(width) = self.erasure_set_drive_count {
            ensure!(
                (2..=16).contains(&width),
                "erasure set width must be between 2 and 16"
            );
        }
        let node = self.local_node.as_deref().map(http_endpoint).transpose()?;
        if let Some(node) = &node {
            ensure!(node.path() == "/", "local node must be an HTTP origin");
        }
        let mut endpoints = BTreeSet::new();
        let mut local = Vec::new();
        for pool in &self.pools {
            ensure!(
                self.pools.len() == 1 || pool.contains('{'),
                "each argument in a multi-pool layout must contain a numeric ellipsis"
            );
            let expanded = expand(pool)?;
            ensure!(
                expanded.len() >= 2 || (self.pools.len() == 1 && node.is_none()),
                "distributed and multi-pool layouts need at least two endpoints per pool"
            );
            if let Some(width) = self.erasure_set_drive_count {
                ensure!(
                    expanded.len() % usize::from(width) == 0,
                    "pool endpoint count must be divisible by the erasure set width"
                );
            }
            for endpoint in expanded {
                let path = if let Some(node) = &node {
                    let url = http_endpoint(&endpoint)?;
                    ensure!(
                        url.path() != "/",
                        "distributed endpoint needs a volume path"
                    );
                    ensure!(
                        endpoints.insert(url.to_string()),
                        "duplicate RustFS endpoint"
                    );
                    (url.origin() == node.origin()).then(|| PathBuf::from(url.path()))
                } else {
                    let path = PathBuf::from(&endpoint);
                    ensure!(path.is_absolute(), "local volume paths must be absolute");
                    ensure!(
                        !path
                            .components()
                            .any(|part| matches!(part, Component::ParentDir)),
                        "local volume paths must not contain parent traversal"
                    );
                    ensure!(endpoints.insert(endpoint), "duplicate RustFS endpoint");
                    Some(path)
                };
                ensure!(
                    endpoints.len() <= MAX_ENDPOINTS,
                    "topology exceeds 1024 endpoints"
                );
                if let Some(path) = path {
                    local.push(path);
                }
            }
        }
        ensure!(
            !local.is_empty(),
            "local node has no volumes in the configured pools"
        );
        Ok(local)
    }
}

fn http_endpoint(value: &str) -> Result<Url> {
    ensure!(
        !value.bytes().any(|b| b.is_ascii_control()) && !value.contains('%'),
        "HTTP endpoints must not contain control characters or escaped paths"
    );
    let url = Url::parse(value).context("invalid RustFS endpoint")?;
    ensure!(
        url.scheme() == "http"
            && url.host_str().is_some()
            && url.port() != Some(0)
            && url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none(),
        "RustFS process topology requires HTTP endpoints without credentials, query, or fragment"
    );
    ensure!(
        !value.split('/').any(|part| matches!(part, "." | "..")),
        "RustFS endpoint paths must not contain traversal"
    );
    Ok(url)
}

fn expand(expression: &str) -> Result<Vec<String>> {
    ensure!(
        !expression.is_empty()
            && expression.len() <= 8192
            && expression.trim() == expression
            && !expression.bytes().any(|b| b.is_ascii_control()),
        "invalid RustFS pool expression"
    );
    let mut expanded = vec![expression.to_owned()];
    while let Some(open) = expanded[0].find('{') {
        let close = expanded[0][open..]
            .find('}')
            .context("unclosed numeric ellipsis")?
            + open;
        let (start, end) = expanded[0][open + 1..close]
            .split_once("...")
            .context("expected a numeric {start...end} ellipsis")?;
        for bound in [start, end] {
            ensure!(
                !bound.is_empty()
                    && bound.bytes().all(|b| b.is_ascii_digit())
                    && (bound == "0" || !bound.starts_with('0')),
                "ellipsis bounds must be non-padded unsigned integers"
            );
        }
        let start: u32 = start.parse().context("invalid ellipsis start")?;
        let end: u32 = end.parse().context("invalid ellipsis end")?;
        ensure!(start < end, "ellipsis range must be increasing");
        let count = u64::from(end) - u64::from(start) + 1;
        ensure!(
            count * expanded.len() as u64 <= MAX_ENDPOINTS as u64,
            "pool expression exceeds 1024 endpoints"
        );
        expanded = expanded
            .into_iter()
            .flat_map(|value| {
                // Earlier substitutions can change offsets, so locate each range again.
                let open = value.find('{').expect("same ellipsis in each expansion");
                let close = value[open..].find('}').expect("validated ellipsis") + open;
                (start..=end)
                    .map(move |number| format!("{}{number}{}", &value[..open], &value[close + 1..]))
            })
            .collect();
    }
    ensure!(
        expanded.iter().all(|value| !value.contains('}')),
        "unexpected closing brace in pool expression"
    );
    Ok(expanded)
}

#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Record {
    schema_version: u32,
    topology: Topology,
    local_volumes: Vec<PathBuf>,
}

pub(crate) struct TopologyLease {
    directory: PathBuf,
    _lock: File,
}

impl TopologyLease {
    pub(crate) fn acquire(directory: &Path, topology: &Topology) -> Result<Self> {
        let directory = existing_directory(directory)?;
        let mut volumes: Vec<PathBuf> = Vec::new();
        for path in topology.local_volumes()? {
            let path = existing_directory(&path)?;
            ensure!(
                !overlaps(&directory, &path) && !volumes.iter().any(|other| overlaps(other, &path)),
                "RustFS state and volume directories must be disjoint"
            );
            volumes.push(path);
        }
        let lock_path = directory.join(LOCK);
        if lock_path.try_exists()? {
            ensure!(
                fs::symlink_metadata(&lock_path)?.is_file(),
                "invalid ownership lock file"
            );
        }
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        fs2::FileExt::try_lock_exclusive(&lock)
            .context("RustFS state directory is already owned")?;
        let lease = Self {
            directory,
            _lock: lock,
        };
        ensure!(
            !lease.directory.join(ACTIVE).try_exists()?,
            "unclean RustFS process ownership: resolve the previous process before reopening"
        );
        let expected = Record {
            schema_version: 1,
            topology: topology.clone(),
            local_volumes: volumes,
        };
        let record_path = lease.directory.join(RECORD);
        if record_path.try_exists()? {
            let metadata = fs::symlink_metadata(&record_path)?;
            ensure!(
                metadata.is_file() && metadata.len() <= MAX_RECORD_BYTES as u64,
                "invalid RustFS topology record"
            );
            let stored: Record = serde_json::from_slice(&fs::read(&record_path)?)
                .context("corrupt or unsupported RustFS topology record")?;
            ensure!(
                stored == expected,
                "RustFS topology does not match established storage"
            );
        } else {
            for entry in fs::read_dir(&lease.directory)? {
                ensure!(
                    entry?.file_name() == LOCK,
                    "unrecognized RustFS state directory"
                );
            }
            for volume in &expected.local_volumes {
                ensure!(
                    fs::read_dir(volume)?.next().transpose()?.is_none(),
                    "cannot adopt nonempty RustFS volumes without a topology record"
                );
            }
            let bytes = serde_json::to_vec_pretty(&expected)?;
            ensure!(
                bytes.len() <= MAX_RECORD_BYTES,
                "RustFS topology record exceeds 64 KiB"
            );
            let mut temporary = tempfile::NamedTempFile::new_in(&lease.directory)?;
            temporary.write_all(&bytes)?;
            temporary.as_file().sync_all()?;
            temporary
                .persist_noclobber(&record_path)
                .context("persisting RustFS topology")?;
            sync_directory(&lease.directory)?;
        }
        Ok(lease)
    }

    pub(crate) fn begin_process(&self) -> Result<()> {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.directory.join(ACTIVE))?;
        file.write_all(
            b"Process launch or ownership may be unresolved. Do not automatically clear.\n",
        )?;
        file.sync_all()?;
        sync_directory(&self.directory)
    }

    /// Only call after spawn failed or the owned child was conclusively reaped.
    pub(crate) fn finish_process(&self) -> Result<()> {
        fs::remove_file(self.directory.join(ACTIVE))
            .context("clearing confirmed RustFS process ownership")?;
        sync_directory(&self.directory)
    }
}

fn existing_directory(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "RustFS directory path must be absolute");
    ensure!(
        fs::symlink_metadata(path)?.is_dir(),
        "RustFS directory must exist and not be a symlink"
    );
    fs::canonicalize(path).context("resolving RustFS directory")
}

fn overlaps(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup(root: &Path) -> Topology {
        for directory in ["state", "data1", "data2"] {
            fs::create_dir(root.join(directory)).unwrap();
        }
        Topology {
            pools: vec![format!("{}{{1...2}}", root.join("data").display())],
            local_node: None,
            erasure_set_drive_count: Some(2),
        }
    }

    #[test]
    fn persists_identity_locks_ownership_and_preserves_established_data() {
        let root = tempfile::tempdir().unwrap();
        let topology = setup(root.path());
        let directory = root.path().join("state");
        let lease = TopologyLease::acquire(&directory, &topology).unwrap();
        let record = fs::read(directory.join(RECORD)).unwrap();
        assert!(TopologyLease::acquire(&directory, &topology).is_err());
        fs::write(root.path().join("data1").join("sentinel"), "untouched").unwrap();
        drop(lease);
        let lease = TopologyLease::acquire(&directory, &topology).unwrap();
        assert_eq!(fs::read(directory.join(RECORD)).unwrap(), record);
        assert_eq!(
            fs::read(root.path().join("data1").join("sentinel")).unwrap(),
            b"untouched"
        );
        drop(lease);
        let mut changed = topology.clone();
        changed.erasure_set_drive_count = None;
        assert!(TopologyLease::acquire(&directory, &changed).is_err());
        assert_eq!(fs::read(directory.join(RECORD)).unwrap(), record);
    }

    #[test]
    fn rejects_unrecognized_storage_and_unclean_ownership_without_mutation() {
        let root = tempfile::tempdir().unwrap();
        let topology = setup(root.path());
        let directory = root.path().join("state");
        let sentinel = root.path().join("data1").join("sentinel");
        fs::write(&sentinel, "existing").unwrap();
        assert!(TopologyLease::acquire(&directory, &topology).is_err());
        assert!(!directory.join(RECORD).exists());
        assert_eq!(fs::read(&sentinel).unwrap(), b"existing");
        fs::remove_file(sentinel).unwrap();
        let lease = TopologyLease::acquire(&directory, &topology).unwrap();
        lease.begin_process().unwrap();
        drop(lease);
        assert!(TopologyLease::acquire(&directory, &topology).is_err());
        assert!(directory.join(ACTIVE).exists());
    }

    #[test]
    fn rejects_corrupt_unknown_and_reordered_records() {
        let root = tempfile::tempdir().unwrap();
        let topology = setup(root.path());
        let directory = root.path().join("state");
        drop(TopologyLease::acquire(&directory, &topology).unwrap());
        let original = fs::read(directory.join(RECORD)).unwrap();
        for bytes in [
            b"invalid-json".to_vec(),
            {
                let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
                value["schema_version"] = 2.into();
                serde_json::to_vec(&value).unwrap()
            },
            {
                let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
                value["local_volumes"].as_array_mut().unwrap().reverse();
                serde_json::to_vec(&value).unwrap()
            },
        ] {
            fs::write(directory.join(RECORD), &bytes).unwrap();
            assert!(TopologyLease::acquire(&directory, &topology).is_err());
            assert_eq!(fs::read(directory.join(RECORD)).unwrap(), bytes);
        }
    }

    #[test]
    fn rejects_overlapping_storage_and_missing_directories() {
        let root = tempfile::tempdir().unwrap();
        let mut topology = setup(root.path());
        let directory = root.path().join("state");
        topology.erasure_set_drive_count = None;
        topology.pools = vec![directory.to_str().unwrap().into()];
        assert!(TopologyLease::acquire(&directory, &topology).is_err());
        topology.pools = vec![root.path().join("missing").to_str().unwrap().into()];
        topology.erasure_set_drive_count = None;
        assert!(TopologyLease::acquire(&directory, &topology).is_err());
        assert!(!root.path().join("missing").exists());
    }
}
