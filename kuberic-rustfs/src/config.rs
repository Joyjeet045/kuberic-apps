use std::ffi::OsStr;
use std::fs::{self, File};
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};

use crate::Topology;

#[derive(Clone, Debug)]
pub struct BinaryPin {
    path: PathBuf,
    sha256: String,
}

impl BinaryPin {
    /// The digest is for the executable, not its release archive.
    pub fn new(path: PathBuf, sha256: &str) -> Result<Self> {
        ensure!(path.is_absolute(), "RustFS binary path must be absolute");
        ensure!(
            sha256.len() == 64 && sha256.bytes().all(|b| b.is_ascii_hexdigit()),
            "RustFS binary SHA-256 must contain exactly 64 hexadecimal digits"
        );
        Ok(Self {
            path,
            sha256: sha256.to_ascii_lowercase(),
        })
    }

    pub(crate) fn verify(&self) -> Result<PathBuf> {
        let path = regular_file(&self.path)?;
        let mut file = File::open(&path).context("opening pinned RustFS executable")?;
        let mut hash = Sha256::new();
        let mut buffer = [0; 64 * 1024];
        loop {
            let read = file
                .read(&mut buffer)
                .context("hashing RustFS executable")?;
            if read == 0 {
                break;
            }
            hash.update(&buffer[..read]);
        }
        ensure!(
            hex_digest(&hash.finalize()) == self.sha256,
            "RustFS executable SHA-256 does not match the configured pin"
        );
        Ok(path)
    }
}

pub(crate) fn hex_digest(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialFiles {
    pub access_key: PathBuf,
    pub secret_key: PathBuf,
}

#[derive(Clone, Debug)]
pub struct LaunchConfig {
    pub binary: BinaryPin,
    pub credentials: CredentialFiles,
    pub state_directory: PathBuf,
    pub topology: Topology,
    pub address: SocketAddr,
    pub shutdown_grace: Duration,
}

impl LaunchConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(
            self.address.port() != 0,
            "RustFS listen port must be nonzero"
        );
        ensure!(
            !self.shutdown_grace.is_zero()
                && self
                    .shutdown_grace
                    .checked_add(Duration::from_secs(6))
                    .and_then(|duration| Instant::now().checked_add(duration))
                    .is_some(),
            "RustFS shutdown grace must be positive and fit the supported deadline range"
        );
        self.topology.local_volumes()?;
        if let Some(node) = &self.topology.local_node {
            ensure!(
                reqwest::Url::parse(node)?.port_or_known_default() == Some(self.address.port()),
                "RustFS local-node and listen ports must match"
            );
        }
        regular_file(&self.credentials.access_key).context("invalid access-key file")?;
        regular_file(&self.credentials.secret_key).context("invalid secret-key file")?;
        Ok(())
    }

    pub(crate) fn command(&self, binary: &Path) -> Command {
        let mut command = Command::new(binary);
        command
            .env_clear()
            .current_dir(&self.state_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .args(["server", "--address", &self.address.to_string(), "--"])
            .args(&self.topology.pools);
        // Retain only platform essentials, never inherited RustFS/MinIO options or credentials.
        for name in [
            "PATH",
            "SystemRoot",
            "WINDIR",
            "TEMP",
            "TMP",
            "TMPDIR",
            "LANG",
            "LC_ALL",
        ] {
            if let Some(value) = std::env::var_os(name) {
                command.env(OsStr::new(name), value);
            }
        }
        command
            .env("RUSTFS_ACCESS_KEY_FILE", &self.credentials.access_key)
            .env("RUSTFS_SECRET_KEY_FILE", &self.credentials.secret_key)
            .env("RUSTFS_CONSOLE_ENABLE", "false")
            .env("RUSTFS_CHECK_UPDATE", "false")
            .env("RUSTFS_HEALTH_ENDPOINT_ENABLE", "true");
        if let Some(width) = self.topology.erasure_set_drive_count {
            command.env("RUSTFS_ERASURE_SET_DRIVE_COUNT", width.to_string());
        }
        command
    }
}

fn regular_file(path: &Path) -> Result<PathBuf> {
    ensure!(path.is_absolute(), "file path must be absolute");
    ensure!(
        fs::metadata(path)?.is_file(),
        "path must name a regular file"
    );
    fs::canonicalize(path).context("resolving file path")
}
