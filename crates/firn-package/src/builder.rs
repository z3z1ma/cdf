use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use arrow_array::RecordBatch;
use firn_kernel::{FirnError, Result, SegmentId};
use serde::Serialize;

use crate::{
    json::canonical_json_bytes,
    model::{FileEntry, MANIFEST_FILE, PackageManifest, PackageStatus, SegmentEntry, TRACE_FILE},
    ops::update_package_status,
    storage::{
        atomic_write, build_manifest, collect_identity_file_entries, create_layout,
        file_entry_for_path, io_error, nested_artifact_path, normalize_artifact_path, package_path,
        segment_relative_path, sync_directory, write_arrow_ipc_file, write_manifest_atomic,
    },
};

#[derive(Debug)]
pub struct PackageBuilder {
    package_dir: PathBuf,
    package_id: String,
    segments: Vec<SegmentDraft>,
}

#[derive(Clone, Debug)]
struct SegmentDraft {
    segment_id: SegmentId,
    path: String,
    row_count: u64,
}
impl PackageBuilder {
    pub fn create(package_dir: impl AsRef<Path>, package_id: impl Into<String>) -> Result<Self> {
        let package_dir = package_dir.as_ref().to_path_buf();
        let package_id = package_id.into();
        if package_id.trim().is_empty() {
            return Err(FirnError::contract("package id cannot be empty"));
        }
        if package_dir.join(MANIFEST_FILE).exists() {
            return Err(FirnError::data(format!(
                "package manifest already exists at {}",
                package_dir.join(MANIFEST_FILE).display()
            )));
        }

        create_layout(&package_dir)?;
        let manifest = build_manifest(
            package_id.clone(),
            collect_identity_file_entries(&package_dir)?,
            Vec::new(),
            PackageStatus::Planned,
        )?;
        write_manifest_atomic(&package_dir, &manifest)?;

        Ok(Self {
            package_dir,
            package_id,
            segments: Vec::new(),
        })
    }

    pub fn package_dir(&self) -> &Path {
        &self.package_dir
    }

    pub fn update_status(&self, status: PackageStatus) -> Result<PackageManifest> {
        update_package_status(&self.package_dir, status)
    }

    pub fn write_json_artifact<T: Serialize>(
        &self,
        relative_path: impl AsRef<Path>,
        value: &T,
    ) -> Result<FileEntry> {
        let bytes = canonical_json_bytes(value)?;
        self.write_identity_artifact(relative_path, &bytes)
    }

    pub fn write_identity_artifact(
        &self,
        relative_path: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        let relative_path = normalize_artifact_path(relative_path.as_ref())?;
        let path = package_path(&self.package_dir, &relative_path);
        atomic_write(&path, bytes)?;
        file_entry_for_path(&self.package_dir, &relative_path)
    }

    pub fn write_stats_artifact(
        &self,
        file_name: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        self.write_identity_artifact(nested_artifact_path("stats", file_name.as_ref())?, bytes)
    }

    pub fn write_quarantine_artifact(
        &self,
        file_name: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        self.write_identity_artifact(
            nested_artifact_path("quarantine", file_name.as_ref())?,
            bytes,
        )
    }

    pub fn write_lineage_artifact(
        &self,
        file_name: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        self.write_identity_artifact(nested_artifact_path("lineage", file_name.as_ref())?, bytes)
    }

    pub fn append_trace_event<T: Serialize>(&self, event: &T) -> Result<()> {
        let mut bytes = canonical_json_bytes(event)?;
        bytes.push(b'\n');
        let path = self.package_dir.join(TRACE_FILE);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| io_error(format!("open {}", path.display()), error))?;
        file.write_all(&bytes)
            .map_err(|error| io_error(format!("write {}", path.display()), error))?;
        file.sync_all()
            .map_err(|error| io_error(format!("sync {}", path.display()), error))?;
        sync_directory(&self.package_dir)
    }

    pub fn write_segment(
        &mut self,
        segment_id: SegmentId,
        batches: &[RecordBatch],
    ) -> Result<SegmentEntry> {
        if batches.is_empty() {
            return Err(FirnError::data("segment must contain at least one batch"));
        }

        let schema = batches[0].schema();
        let mut row_count = 0_u64;
        for batch in batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(FirnError::data(
                    "all record batches in a package segment must share one schema",
                ));
            }
            row_count += batch.num_rows() as u64;
        }

        let relative_path = segment_relative_path(&segment_id)?;
        let path = package_path(&self.package_dir, &relative_path);
        write_arrow_ipc_file(&path, schema.as_ref(), batches)?;

        let file_entry = file_entry_for_path(&self.package_dir, &relative_path)?;
        let segment = SegmentEntry {
            segment_id: segment_id.clone(),
            path: relative_path.clone(),
            row_count,
            byte_count: file_entry.byte_count,
            sha256: file_entry.sha256,
        };
        self.segments.push(SegmentDraft {
            segment_id,
            path: relative_path,
            row_count,
        });
        Ok(segment)
    }

    pub fn finish(&self) -> Result<PackageManifest> {
        self.finish_with_status(PackageStatus::Packaged)
    }

    pub fn finish_with_status(&self, status: PackageStatus) -> Result<PackageManifest> {
        let files = collect_identity_file_entries(&self.package_dir)?;
        let entries_by_path: BTreeMap<&str, &FileEntry> = files
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        let mut segments = Vec::with_capacity(self.segments.len());

        for draft in &self.segments {
            let entry = entries_by_path.get(draft.path.as_str()).ok_or_else(|| {
                FirnError::data(format!(
                    "segment file {} missing before package finalization",
                    draft.path
                ))
            })?;
            segments.push(SegmentEntry {
                segment_id: draft.segment_id.clone(),
                path: draft.path.clone(),
                row_count: draft.row_count,
                byte_count: entry.byte_count,
                sha256: entry.sha256.clone(),
            });
        }

        let manifest = build_manifest(self.package_id.clone(), files, segments, status)?;
        write_manifest_atomic(&self.package_dir, &manifest)?;
        Ok(manifest)
    }
}
