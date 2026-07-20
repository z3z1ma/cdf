use cdf_kernel::{CdfError, Result};
use cdf_package_contract::{
    FileEntry, LifecycleState, MANIFEST_VERSION, ManifestIdentity, PackageManifest, PackageStatus,
    SegmentEntry, SignatureSlot,
};
use serde::Serialize;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Write;

type FileEntrySink<'a> = dyn FnMut(FileEntry) -> Result<()> + 'a;
type SegmentEntrySink<'a> = dyn FnMut(SegmentEntry) -> Result<()> + 'a;
type FileEntrySource<'a> = dyn FnMut(&mut FileEntrySink<'_>) -> Result<()> + 'a;
type SegmentEntrySource<'a> = dyn FnMut(&mut SegmentEntrySink<'_>) -> Result<()> + 'a;

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

pub(crate) fn manifest_identity_hash_streaming(
    package_id: &str,
    layout: &[String],
    visit_files: &mut FileEntrySource<'_>,
    visit_segments: &mut SegmentEntrySource<'_>,
) -> Result<String> {
    let mut writer = DigestWriter(Sha256::new());
    write_manifest_identity_canonical_streaming(
        package_id,
        layout,
        visit_files,
        visit_segments,
        &mut writer,
    )?;
    Ok(format!("sha256:{}", hex::encode(writer.0.finalize())))
}

pub(crate) fn write_package_manifest_canonical_streaming<W: Write>(
    package_id: &str,
    layout: &[String],
    package_hash: &str,
    status: PackageStatus,
    visit_files: &mut FileEntrySource<'_>,
    visit_segments: &mut SegmentEntrySource<'_>,
    writer: &mut W,
) -> Result<()> {
    write_bytes(writer, b"{\"identity\":")?;
    write_manifest_identity_canonical_streaming(
        package_id,
        layout,
        visit_files,
        visit_segments,
        writer,
    )?;
    write_bytes(writer, b",\"lifecycle\":")?;
    write_value(writer, &LifecycleState { status })?;
    write_bytes(writer, b",\"manifest_version\":")?;
    write_display(writer, MANIFEST_VERSION)?;
    write_bytes(writer, b",\"package_hash\":")?;
    write_json_string(writer, package_hash)?;
    write_bytes(writer, b",\"signature\":")?;
    write_value(
        writer,
        &SignatureSlot {
            signing_input: package_hash.to_owned(),
            value: None,
        },
    )?;
    write_bytes(writer, b"}")
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
    for (index, entry) in identity.files.iter().enumerate() {
        if index > 0 {
            write_bytes(writer, b",")?;
        }
        write_file_entry(writer, entry)?;
    }
    write_bytes(writer, b"],\"layout\":[")?;
    for (index, value) in identity.layout.iter().enumerate() {
        if index > 0 {
            write_bytes(writer, b",")?;
        }
        write_json_string(writer, value)?;
    }
    write_bytes(writer, b"],\"manifest_version\":")?;
    write_display(writer, identity.manifest_version)?;
    write_bytes(writer, b",\"package_id\":")?;
    write_json_string(writer, &identity.package_id)?;
    write_bytes(writer, b",\"segments\":[")?;
    for (index, entry) in identity.segments.iter().enumerate() {
        if index > 0 {
            write_bytes(writer, b",")?;
        }
        write_segment_entry(writer, entry)?;
    }
    write_bytes(writer, b"]}")
}

fn write_manifest_identity_canonical_streaming<W: Write>(
    package_id: &str,
    layout: &[String],
    visit_files: &mut FileEntrySource<'_>,
    visit_segments: &mut SegmentEntrySource<'_>,
    writer: &mut W,
) -> Result<()> {
    write_bytes(writer, b"{\"files\":[")?;
    let mut first = true;
    visit_files(&mut |entry| {
        if !first {
            write_bytes(writer, b",")?;
        }
        first = false;
        write_file_entry(writer, &entry)
    })?;
    write_bytes(writer, b"],\"layout\":[")?;
    for (index, value) in layout.iter().enumerate() {
        if index > 0 {
            write_bytes(writer, b",")?;
        }
        write_json_string(writer, value)?;
    }
    write_bytes(writer, b"],\"manifest_version\":")?;
    write_display(writer, MANIFEST_VERSION)?;
    write_bytes(writer, b",\"package_id\":")?;
    write_json_string(writer, package_id)?;
    write_bytes(writer, b",\"segments\":[")?;
    first = true;
    visit_segments(&mut |entry| {
        if !first {
            write_bytes(writer, b",")?;
        }
        first = false;
        write_segment_entry(writer, &entry)
    })?;
    write_bytes(writer, b"]}")
}

fn write_file_entry<W: Write>(writer: &mut W, entry: &FileEntry) -> Result<()> {
    write_bytes(writer, b"{\"byte_count\":")?;
    write_display(writer, entry.byte_count)?;
    write_bytes(writer, b",\"path\":")?;
    write_json_string(writer, &entry.path)?;
    write_bytes(writer, b",\"sha256\":")?;
    write_json_string(writer, &entry.sha256)?;
    write_bytes(writer, b"}")
}

fn write_segment_entry<W: Write>(writer: &mut W, entry: &SegmentEntry) -> Result<()> {
    write_bytes(writer, b"{\"byte_count\":")?;
    write_display(writer, entry.byte_count)?;
    write_bytes(writer, b",\"package_row_ord_start\":")?;
    write_display(writer, entry.package_row_ord_start)?;
    write_bytes(writer, b",\"path\":")?;
    write_json_string(writer, &entry.path)?;
    write_bytes(writer, b",\"row_count\":")?;
    write_display(writer, entry.row_count)?;
    write_bytes(writer, b",\"segment_id\":")?;
    write_json_string(writer, entry.segment_id.as_str())?;
    write_bytes(writer, b",\"sha256\":")?;
    write_json_string(writer, &entry.sha256)?;
    write_bytes(writer, b"}")
}

fn write_json_string<W: Write>(writer: &mut W, value: &str) -> Result<()> {
    serde_json::to_writer(writer, value).map_err(json_error)
}

fn write_display<W: Write>(writer: &mut W, value: impl std::fmt::Display) -> Result<()> {
    write!(writer, "{value}")
        .map_err(|error| CdfError::internal(format!("write canonical JSON: {error}")))
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

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_package_contract::{MANIFEST_VERSION, ManifestIdentity};
    use std::time::Instant;

    #[test]
    #[ignore = "performance evidence; run explicitly in release mode"]
    fn million_entry_manifest_identity_streams_without_a_dom() {
        const ENTRIES: usize = 1_000_000;
        let files = (0..ENTRIES)
            .map(|index| FileEntry {
                path: format!("data/segment-{index:07}.arrow"),
                byte_count: 65_536,
                sha256: format!("{index:064x}"),
            })
            .collect();
        let identity = ManifestIdentity {
            manifest_version: MANIFEST_VERSION,
            package_id: "million-entry".to_owned(),
            layout: vec!["data/".to_owned()],
            files,
            segments: Vec::new(),
        };
        let started = Instant::now();
        let hash = manifest_identity_hash(&identity).unwrap();
        let elapsed = started.elapsed();
        assert!(hash.starts_with("sha256:"));
        eprintln!(
            "manifest_entries={ENTRIES} elapsed_ns={} entries_per_second={:.0}",
            elapsed.as_nanos(),
            ENTRIES as f64 / elapsed.as_secs_f64()
        );
    }
}
