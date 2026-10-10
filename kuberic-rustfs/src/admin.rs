use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, ensure};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use reqwest::{Client, Method, Url};
use serde::Deserialize;

use crate::{CredentialFiles, HealthClient};

const MAX_RESPONSE: usize = 1024 * 1024;

pub(crate) struct AdminClient {
    endpoint: Url,
    credentials: CredentialFiles,
    client: Client,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Pool {
    pub id: usize,
    pub cmdline: String,
    pub status: String,
}

impl AdminClient {
    pub(crate) fn new(
        endpoint: &str,
        credentials: CredentialFiles,
        ca: Option<&[u8]>,
    ) -> Result<Self> {
        HealthClient::new(endpoint, Duration::from_secs(5))?;
        let mut client = Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy();
        if let Some(ca) = ca {
            client = crate::config::trust_ca(client, ca)?;
        }
        Ok(Self {
            endpoint: Url::parse(endpoint)?,
            credentials,
            client: client.build()?,
        })
    }

    async fn request(&self, method: Method, path: &str) -> Result<Vec<u8>> {
        let url = self.endpoint.join(path)?;
        let access = tokio::fs::read_to_string(&self.credentials.access_key).await?;
        let secret = tokio::fs::read_to_string(&self.credentials.secret_key).await?;
        ensure!(
            !access.trim().is_empty() && !secret.trim().is_empty(),
            "native admin credential files cannot be empty"
        );
        let identity =
            Credentials::new(access.trim(), secret.trim(), None, None, "rustfs-files").into();
        let params = v4::SigningParams::builder()
            .identity(&identity)
            .region("us-east-1")
            .name("s3")
            .time(SystemTime::now())
            .settings(SigningSettings::default())
            .build()?
            .into();
        let payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
        let signable = SignableRequest::new(
            method.as_str(),
            url.as_str(),
            [("x-amz-content-sha256", payload_hash)].into_iter(),
            SignableBody::Bytes(&[]),
        )?;
        let (instructions, _) = sign(signable, &params)?.into_parts();
        let mut request = self
            .client
            .request(method, url)
            .header("x-amz-content-sha256", payload_hash);
        for (name, value) in instructions.headers() {
            request = request.header(name, value);
        }
        let mut response = request
            .send()
            .await
            .context("native admin request failed")?;
        let status = response.status();
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= MAX_RESPONSE,
                "native admin response exceeds 1 MiB"
            );
            bytes.extend_from_slice(&chunk);
        }
        ensure!(
            status.is_success(),
            "native admin {path} returned HTTP {status}: {}",
            String::from_utf8_lossy(&bytes)
        );
        Ok(bytes)
    }

    pub(crate) async fn pools(&self) -> Result<Vec<Pool>> {
        serde_json::from_slice(
            &self
                .request(Method::GET, "/rustfs/admin/v3/pools/list")
                .await?,
        )
        .context("invalid native pool list")
    }
}
