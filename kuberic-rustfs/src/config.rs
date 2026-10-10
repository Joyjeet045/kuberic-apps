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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlTlsConfig {
    pub certificate_file: PathBuf,
    pub private_key_file: PathBuf,
}

impl ControlTlsConfig {
    pub(crate) async fn load(&self) -> Result<axum_server::tls_rustls::RustlsConfig> {
        let certificates = tokio::fs::read(&self.certificate_file).await?;
        let key = tokio::fs::read(&self.private_key_file).await?;
        let mut config = server_tls_config(&certificates, &key)?;
        config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(axum_server::tls_rustls::RustlsConfig::from_config(
            std::sync::Arc::new(config),
        ))
    }
}

fn server_tls_config(certificates: &[u8], key: &[u8]) -> Result<rustls::ServerConfig> {
    use rustls::pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject};

    rustls::ServerConfig::builder_with_provider(rustls::crypto::ring::default_provider().into())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(
            CertificateDer::pem_slice_iter(certificates).collect::<Result<Vec<_>, _>>()?,
            PrivateKeyDer::from_pem_slice(key)?,
        )
        .context("invalid TLS certificate or private key")
}

pub(crate) fn trust_ca(
    client: reqwest::ClientBuilder,
    pem: &[u8],
) -> Result<reqwest::ClientBuilder> {
    let certificates = reqwest::Certificate::from_pem_bundle(pem)?;
    ensure!(
        !certificates.is_empty(),
        "TLS CA bundle contains no certificates"
    );
    let mut client = client.https_only(true).tls_built_in_root_certs(false);
    for certificate in certificates {
        client = client.add_root_certificate(certificate);
    }
    Ok(client)
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeTlsConfig {
    /// RustFS 1.0.1 layout: rustfs_cert.pem, rustfs_key.pem, and ca.crt.
    pub directory: PathBuf,
}

impl NativeTlsConfig {
    pub(crate) fn ca_bundle(&self) -> Result<Vec<u8>> {
        fs::read(regular_file(&self.directory.join("ca.crt"))?).context("reading native TLS CA")
    }

    fn validate(&self) -> Result<()> {
        let certificates = fs::read(regular_file(&self.directory.join("rustfs_cert.pem"))?)?;
        let key = fs::read(regular_file(&self.directory.join("rustfs_key.pem"))?)?;
        server_tls_config(&certificates, &key)?;
        let _ = trust_ca(reqwest::Client::builder(), &self.ca_bundle()?)?.build()?;
        Ok(())
    }
}

pub(crate) async fn control_token(path: &Path) -> Result<String> {
    use tokio::io::AsyncReadExt;

    let mut bytes = Vec::new();
    tokio::fs::File::open(path)
        .await?
        .take(1025)
        .read_to_end(&mut bytes)
        .await?;
    ensure!(bytes.len() <= 1024, "control token file exceeds 1 KiB");
    let token = std::str::from_utf8(&bytes)?.trim();
    ensure!(
        (32..=256).contains(&token.len()) && token.bytes().all(|byte| byte.is_ascii_graphic()),
        "control token must contain 32-256 non-whitespace ASCII characters"
    );
    Ok(token.to_owned())
}

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
    pub native_tls: Option<NativeTlsConfig>,
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
        if let Some(tls) = &self.native_tls {
            tls.validate()?;
        } else {
            ensure!(
                self.address.ip().is_loopback() && self.topology.local_node.is_none(),
                "native TLS is required except for loopback-only local storage"
            );
        }
        if let Some(node) = &self.topology.local_node {
            let node = reqwest::Url::parse(node)?;
            ensure!(
                node.port_or_known_default() == Some(self.address.port()),
                "RustFS local-node and listen ports must match"
            );
            ensure!(
                node.scheme() == "https",
                "distributed native topology must use HTTPS"
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
        if let Some(tls) = &self.native_tls {
            command.env("RUSTFS_TLS_PATH", &tls.directory);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn control_tokens_are_bounded_and_validated_consistently() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("token");
        assert!(control_token(&path).await.is_err());
        for invalid in [
            String::new(),
            "short".into(),
            "x".repeat(257),
            format!("{} has spaces", "x".repeat(32)),
            "x".repeat(1025),
        ] {
            fs::write(&path, invalid).unwrap();
            assert!(control_token(&path).await.is_err());
        }
        for length in [32, 256] {
            let token = "x".repeat(length);
            fs::write(&path, format!("{token}\n")).unwrap();
            assert_eq!(control_token(&path).await.unwrap(), token);
        }
    }
}
