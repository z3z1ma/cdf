use cdf_kernel::{CdfError, Result};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Write;

use crate::model::{ManifestIdentity, PackageManifest};

pub fn canonical_json_bytes<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>> {
    let value = serde_json::to_value(value).map_err(json_error)?;
    let mut output = Vec::new();
    write_canonical_value(&value, &mut output)?;
    Ok(output)
}

pub fn manifest_identity_hash(identity: &ManifestIdentity) -> Result<String> {
    let mut writer = DigestWriter(Sha256::new());
    write_manifest_identity_canonical(identity, &mut writer)?;
    Ok(format!("sha256:{}", hex::encode(writer.0.finalize())))
}

pub(crate) fn write_package_manifest_canonical<W: Write>(
    manifest: &PackageManifest,
    writer: &mut W,
) -> Result<()> {
    write_bytes(writer, b"{")?;
    let mut wrote = false;
    if let Some(archives) = &manifest.archives {
        write_bytes(writer, b"\"archives\":")?;
        write_value(writer, archives)?;
        wrote = true;
    }
    write_field_prefix(writer, "identity", &mut wrote)?;
    write_manifest_identity_canonical(&manifest.identity, writer)?;
    write_field_prefix(writer, "lifecycle", &mut wrote)?;
    write_value(writer, &manifest.lifecycle)?;
    write_field_prefix(writer, "manifest_version", &mut wrote)?;
    write_value(writer, &manifest.manifest_version)?;
    write_field_prefix(writer, "package_hash", &mut wrote)?;
    write_value(writer, &manifest.package_hash)?;
    write_field_prefix(writer, "signature", &mut wrote)?;
    write_value(writer, &manifest.signature)?;
    write_bytes(writer, b"}")
}

fn write_manifest_identity_canonical<W: Write>(
    identity: &ManifestIdentity,
    writer: &mut W,
) -> Result<()> {
    write_bytes(writer, b"{\"files\":[")?;
    write_values(writer, &identity.files)?;
    write_bytes(writer, b"],\"layout\":[")?;
    write_values(writer, &identity.layout)?;
    write_bytes(writer, b"],\"manifest_version\":")?;
    write_value(writer, &identity.manifest_version)?;
    write_bytes(writer, b",\"package_id\":")?;
    write_value(writer, &identity.package_id)?;
    write_bytes(writer, b",\"segments\":[")?;
    write_values(writer, &identity.segments)?;
    write_bytes(writer, b"]}")
}

fn write_values<W: Write, T: Serialize>(writer: &mut W, values: &[T]) -> Result<()> {
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            write_bytes(writer, b",")?;
        }
        write_value(writer, value)?;
    }
    Ok(())
}

fn write_field_prefix<W: Write>(writer: &mut W, field: &str, wrote: &mut bool) -> Result<()> {
    if *wrote {
        write_bytes(writer, b",")?;
    }
    write_value(writer, field)?;
    write_bytes(writer, b":")?;
    *wrote = true;
    Ok(())
}

fn write_value<W: Write, T: Serialize + ?Sized>(writer: &mut W, value: &T) -> Result<()> {
    write_bytes(writer, &canonical_json_bytes(value)?)
}

fn write_bytes<W: Write>(writer: &mut W, bytes: &[u8]) -> Result<()> {
    writer
        .write_all(bytes)
        .map_err(|error| CdfError::internal(format!("write canonical JSON: {error}")))
}

struct DigestWriter(Sha256);

impl Write for DigestWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn write_canonical_value(value: &Value, output: &mut Vec<u8>) -> Result<()> {
    match value {
        Value::Null => output.extend_from_slice(b"null"),
        Value::Bool(value) => output.extend_from_slice(if *value { b"true" } else { b"false" }),
        Value::Number(number) => output.extend_from_slice(number.to_string().as_bytes()),
        Value::String(value) => write_canonical_string(value, output)?,
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_value(value, output)?;
            }
            output.push(b']');
        }
        Value::Object(map) => {
            output.push(b'{');
            let mut entries = map.iter().collect::<Vec<_>>();
            entries.sort_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                write_canonical_string(key, output)?;
                output.push(b':');
                write_canonical_value(value, output)?;
            }
            output.push(b'}');
        }
    }
    Ok(())
}

fn write_canonical_string(value: &str, output: &mut Vec<u8>) -> Result<()> {
    let escaped = serde_json::to_string(value).map_err(json_error)?;
    output.extend_from_slice(escaped.as_bytes());
    Ok(())
}

pub(crate) fn json_error(error: serde_json::Error) -> CdfError {
    CdfError::data(error.to_string())
}
