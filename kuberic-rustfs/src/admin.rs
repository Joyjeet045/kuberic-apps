use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, ensure};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{SignableBody, SignableRequest, SigningSettings, sign};
use aws_sigv4::sign::v4;
use reqwest::{Client, Method, Url};
use serde::Deserialize;
use serde_json::Value;

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
    pub(crate) fn new(endpoint: &str, credentials: CredentialFiles) -> Result<Self> {
        HealthClient::new(endpoint, Duration::from_secs(5))?;
        Ok(Self {
            endpoint: Url::parse(endpoint)?,
            credentials,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .no_proxy()
                .build()?,
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

    pub(crate) async fn decommission(&self, pool: usize) -> Result<Option<Value>> {
        let status: Value = serde_json::from_slice(
            &self
                .request(Method::GET, "/rustfs/admin/v3/decommission/status")
                .await?,
        )?;
        let entry = status
            .get("pools")
            .and_then(Value::as_array)
            .context("native decommission response omitted pools")?
            .iter()
            .find(|entry| entry.get("id").and_then(Value::as_u64) == Some(pool as u64))
            .context("native decommission response omitted requested pool")?;
        match decommission_state(entry)? {
            DecommissionState::Complete => Ok(Some(entry.clone())),
            DecommissionState::Running => Ok(None),
            DecommissionState::Inactive => {
                self.request(
                    Method::POST,
                    &format!("/rustfs/admin/v3/pools/decommission?pool={pool}&by-id=true"),
                )
                .await?;
                Ok(None)
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
enum DecommissionState {
    Inactive,
    Running,
    Complete,
}

fn decommission_state(entry: &Value) -> Result<DecommissionState> {
    let status = entry
        .get("status")
        .and_then(Value::as_str)
        .context("native pool status is missing")?;
    ensure!(
        !matches!(status, "failed" | "canceled"),
        "native decommission has failed or was canceled"
    );
    let Some(info) = entry.get("decommissionInfo").filter(|info| !info.is_null()) else {
        ensure!(
            status == "none" && entry.get("poolStatus").and_then(Value::as_str) == Some("active"),
            "unrecognized native pool status: {status}"
        );
        return Ok(DecommissionState::Inactive);
    };
    for field in ["failed", "canceled"] {
        ensure!(
            info.get(field).and_then(Value::as_bool) == Some(false),
            "native decommission {field} is true or missing"
        );
    }
    for field in ["objectsDecommissionedFailed", "bytesDecommissionedFailed"] {
        ensure!(
            info.get(field).and_then(Value::as_u64) == Some(0),
            "native decommission {field} is nonzero or missing"
        );
    }
    if let Some(entries) = info.get("unresolvedEntries") {
        ensure!(
            entries.as_array().is_some_and(Vec::is_empty),
            "native decommission has unresolved or malformed entries"
        );
    }
    match info.get("complete").and_then(Value::as_bool) {
        Some(true) => {
            ensure!(
                status == "complete"
                    && entry.get("poolStatus").and_then(Value::as_str) == Some("decommissioned"),
                "native completion flag does not match terminal pool status"
            );
            Ok(DecommissionState::Complete)
        }
        Some(false) => {
            ensure!(
                matches!(status, "queued" | "running")
                    && entry.get("poolStatus").and_then(Value::as_str) == Some("decommissioning"),
                "unrecognized native decommission state: {status}"
            );
            Ok(DecommissionState::Running)
        }
        None => anyhow::bail!("native decommission completion is missing"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn completion_requires_native_terminal_success_without_unresolved_entries() {
        let complete = json!({
            "status": "complete",
            "poolStatus": "decommissioned",
            "decommissionInfo": {
                "complete": true, "failed": false, "canceled": false,
                "objectsDecommissionedFailed": 0, "bytesDecommissionedFailed": 0,
                "unresolvedEntries": []
            }
        });
        assert_eq!(
            decommission_state(&complete).unwrap(),
            DecommissionState::Complete
        );
        for field in [
            "complete",
            "failed",
            "canceled",
            "objectsDecommissionedFailed",
            "bytesDecommissionedFailed",
        ] {
            let mut invalid = complete.clone();
            invalid["decommissionInfo"]
                .as_object_mut()
                .unwrap()
                .remove(field);
            assert!(decommission_state(&invalid).is_err(), "{field}");
        }
        let mut failed = complete.clone();
        failed["decommissionInfo"]["objectsDecommissionedFailed"] = json!(1);
        assert!(decommission_state(&failed).is_err());
        failed = complete.clone();
        failed["decommissionInfo"]["unresolvedEntries"] = json!(["object"]);
        assert!(decommission_state(&failed).is_err());
        let mut omitted = complete.clone();
        omitted["decommissionInfo"]
            .as_object_mut()
            .unwrap()
            .remove("unresolvedEntries");
        assert_eq!(
            decommission_state(&omitted).unwrap(),
            DecommissionState::Complete
        );
        let active = json!({"status": "none", "poolStatus": "active"});
        assert_eq!(
            decommission_state(&active).unwrap(),
            DecommissionState::Inactive
        );
    }
}
