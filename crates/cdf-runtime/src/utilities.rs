use crate::prelude::*;

use serde::Serialize;
use sha2::{Digest, Sha256};

pub fn artifact_hash(value: &impl Serialize) -> Result<String> {
    let bytes = serde_json::to_vec(value).map_err(|error| CdfError::internal(error.to_string()))?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

pub fn commit_request(
    delta: &StateDelta,
    target: TargetName,
    disposition: WriteDisposition,
) -> Result<DestinationCommitRequest> {
    Ok(DestinationCommitRequest {
        package_hash: delta.package_hash.clone(),
        target,
        disposition,
        segments: delta.segments.clone(),
        idempotency_token: cdf_kernel::IdempotencyToken::new(delta.package_hash.as_str())?,
    })
}

pub fn destination_uri_scheme(uri: &str) -> Result<&str> {
    let (scheme, _) = uri.split_once(':').ok_or_else(|| {
        CdfError::contract(format!("destination URI `{uri}` is missing a scheme"))
    })?;
    validate_destination_scheme(scheme)?;
    Ok(scheme)
}

pub fn validate_destination_scheme(scheme: &str) -> Result<()> {
    if scheme.is_empty() {
        return Err(CdfError::contract("destination URI scheme cannot be empty"));
    }
    if scheme
        .bytes()
        .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'+' | b'.' | b'-')))
    {
        return Err(CdfError::contract(format!(
            "destination URI scheme `{scheme}` contains invalid characters"
        )));
    }
    Ok(())
}

pub fn local_uri_path<'a>(uri: &'a str, scheme: &str) -> Result<&'a str> {
    let prefix = format!("{scheme}://");
    let raw = uri.strip_prefix(&prefix).ok_or_else(|| {
        CdfError::contract(format!(
            "destination URI `{uri}` is unsupported; expected {scheme}://path"
        ))
    })?;
    if raw.trim().is_empty() || raw.contains("://") {
        return Err(CdfError::contract(format!(
            "destination URI `{uri}` is malformed or non-local; expected {scheme}://path"
        )));
    }
    Ok(raw)
}

pub fn absolute_under_root(root: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        root.join(path)
    }
}
