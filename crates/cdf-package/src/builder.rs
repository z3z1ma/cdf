use std::{
    collections::BTreeMap,
    fs::OpenOptions,
    io::Write,
    path::{Path, PathBuf},
};

use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, Checkpoint, Result, SegmentId};
use serde::Serialize;

use crate::{
    artifacts::{
        DEDUP_SUMMARY_FILE, DESTINATION_COMMIT_PLAN_FILE, DestinationCommitPlanPreimage,
        STATE_INPUT_CHECKPOINT_FILE, STATE_PROPOSED_DELTA_FILE, StateDeltaPreimage,
    },
    json::canonical_json_bytes,
    model::{FileEntry, MANIFEST_FILE, PackageManifest, PackageStatus, SegmentEntry, TRACE_FILE},
    ops::update_package_status,
    quarantine::{QuarantineRecord, quarantine_records_to_parquet_bytes},
    storage::{
        ArtifactDurability, atomic_write, build_manifest, collect_identity_file_entries,
        collect_identity_file_paths, create_layout, file_entry_for_path, io_error,
        nested_artifact_path, normalize_artifact_path, package_path, segment_relative_path,
        sync_directory, write_arrow_ipc_file, write_manifest_atomic,
    },
};

#[derive(Debug)]
pub struct PackageBuilder {
    package_dir: PathBuf,
    package_id: String,
    segments: Vec<SegmentDraft>,
    written_artifacts: Mutex<BTreeMap<String, FileEntry>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentWriteMetrics {
    pub segment: SegmentEntry,
    pub encode_duration_ns: u64,
    pub persist_hash_duration_ns: u64,
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
            return Err(CdfError::contract("package id cannot be empty"));
        }
        if package_dir.join(MANIFEST_FILE).exists() {
            return Err(CdfError::data(format!(
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
            written_artifacts: Mutex::new(BTreeMap::new()),
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

    pub fn write_runtime_arrow_schema(&self, schema: &arrow_schema::Schema) -> Result<FileEntry> {
        self.write_identity_artifact(
            crate::RUNTIME_ARROW_SCHEMA_FILE,
            &crate::runtime_schema_bytes(schema)?,
        )
    }

    pub fn write_input_checkpoint_artifact(
        &self,
        checkpoint: &Option<Checkpoint>,
    ) -> Result<FileEntry> {
        self.write_json_artifact(STATE_INPUT_CHECKPOINT_FILE, checkpoint)
    }

    pub fn write_state_delta_preimage_artifact(
        &self,
        preimage: &StateDeltaPreimage,
    ) -> Result<FileEntry> {
        self.write_json_artifact(STATE_PROPOSED_DELTA_FILE, preimage)
    }

    pub fn write_commit_plan_preimage_artifact(
        &self,
        preimage: &DestinationCommitPlanPreimage,
    ) -> Result<FileEntry> {
        self.write_json_artifact(DESTINATION_COMMIT_PLAN_FILE, preimage)
    }

    pub fn write_identity_artifact(
        &self,
        relative_path: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<FileEntry> {
        let relative_path = normalize_artifact_path(relative_path.as_ref())?;
        let path = package_path(&self.package_dir, &relative_path);
        let mut written_artifacts = self
            .written_artifacts
            .lock()
            .map_err(|_| CdfError::internal("package artifact receipt index lock is poisoned"))?;
        let receipt = atomic_write(&path, bytes)?;
        if receipt.path != path || receipt.durability != ArtifactDurability::PhaseMetadata {
            return Err(CdfError::internal(format!(
                "artifact writer returned an invalid receipt for {relative_path}"
            )));
        }
        let entry = FileEntry {
            path: relative_path,
            byte_count: receipt.byte_count,
            sha256: receipt.sha256,
        };
        written_artifacts.insert(entry.path.clone(), entry.clone());
        Ok(entry)
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

    pub fn write_quarantine_records(
        &self,
        file_name: impl AsRef<Path>,
        records: &[QuarantineRecord],
    ) -> Result<FileEntry> {
        let bytes = quarantine_records_to_parquet_bytes(records)?;
        self.write_quarantine_artifact(file_name, &bytes)
    }

    pub fn write_dedup_summary<T: Serialize>(&self, summary: &T) -> Result<FileEntry> {
        self.write_json_artifact(DEDUP_SUMMARY_FILE, summary)
    }

    pub fn write_dedup_provenance_shard(
        &self,
        file_name: &str,
        rows: &[(u64, u64)],
    ) -> Result<FileEntry> {
        if rows.is_empty() {
            return Err(CdfError::contract(
                "dedup provenance shard requires at least one row",
            ));
        }
        if rows.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
            return Err(CdfError::contract(
                "dedup provenance rows must be strictly ordered by dropped ordinal",
            ));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("package_row_ordinal", DataType::UInt64, false),
            Field::new("kept_package_row_ordinal", DataType::UInt64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.0))) as ArrayRef,
                Arc::new(UInt64Array::from_iter_values(rows.iter().map(|row| row.1))) as ArrayRef,
            ],
        )
        .map_err(CdfError::from)?;
        let bytes = crate::transcode_record_batches_to_parquet_bytes(&[batch])?;
        self.write_identity_artifact(format!("stats/dedup-dropped/{file_name}"), &bytes)
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
        Ok(self
            .write_segment_inner(segment_id, batches, false)?
            .segment)
    }

    pub fn write_segment_with_metrics(
        &mut self,
        segment_id: SegmentId,
        batches: &[RecordBatch],
    ) -> Result<SegmentWriteMetrics> {
        self.write_segment_inner(segment_id, batches, true)
    }

    fn write_segment_inner(
        &mut self,
        segment_id: SegmentId,
        batches: &[RecordBatch],
        measure: bool,
    ) -> Result<SegmentWriteMetrics> {
        if batches.is_empty() {
            return Err(CdfError::data("segment must contain at least one batch"));
        }

        let schema = batches[0].schema();
        let mut row_count = 0_u64;
        for batch in batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(CdfError::data(
                    "all record batches in a package segment must share one schema",
                ));
            }
            row_count += batch.num_rows() as u64;
        }

        let relative_path = segment_relative_path(&segment_id)?;
        if self
            .segments
            .iter()
            .any(|draft| draft.segment_id == segment_id || draft.path == relative_path)
        {
            return Err(CdfError::data(format!(
                "package segment is already registered: {}",
                segment_id.as_str()
            )));
        }
        let path = package_path(&self.package_dir, &relative_path);
        let receipt = write_arrow_ipc_file(&path, schema.as_ref(), batches)?;
        if receipt.artifact.path != path
            || receipt.artifact.durability != ArtifactDurability::SegmentPublish
        {
            return Err(CdfError::internal(format!(
                "segment writer returned an invalid receipt for {relative_path}"
            )));
        }
        let file_entry = FileEntry {
            path: relative_path.clone(),
            byte_count: receipt.artifact.byte_count,
            sha256: receipt.artifact.sha256.clone(),
        };
        self.written_artifacts
            .lock()
            .map_err(|_| CdfError::internal("package artifact receipt index lock is poisoned"))?
            .insert(relative_path.clone(), file_entry);
        let segment = SegmentEntry {
            segment_id: segment_id.clone(),
            path: relative_path.clone(),
            row_count,
            byte_count: receipt.artifact.byte_count,
            sha256: receipt.artifact.sha256,
        };
        self.segments.push(SegmentDraft {
            segment_id,
            path: relative_path,
            row_count,
        });
        Ok(SegmentWriteMetrics {
            segment,
            encode_duration_ns: if measure {
                receipt.encode_hash_duration_ns
            } else {
                0
            },
            persist_hash_duration_ns: if measure {
                receipt.publish_duration_ns
            } else {
                0
            },
        })
    }

    pub fn finish(&self) -> Result<PackageManifest> {
        self.finish_with_status(PackageStatus::Packaged)
    }

    pub fn finish_with_status(&self, status: PackageStatus) -> Result<PackageManifest> {
        let written_artifacts = self
            .written_artifacts
            .lock()
            .map_err(|_| CdfError::internal("package artifact receipt index lock is poisoned"))?;
        let mut files = Vec::new();
        for relative_path in collect_identity_file_paths(&self.package_dir)? {
            let path = package_path(&self.package_dir, &relative_path);
            let byte_count = std::fs::metadata(&path)
                .map_err(|error| io_error(format!("stat {}", path.display()), error))?
                .len();
            match written_artifacts.get(&relative_path) {
                Some(entry) if entry.byte_count == byte_count => files.push(entry.clone()),
                Some(entry) => {
                    return Err(CdfError::data(format!(
                        "identity artifact {relative_path} changed after its writer receipt: expected {} bytes, found {byte_count}",
                        entry.byte_count
                    )));
                }
                None => files.push(file_entry_for_path(&self.package_dir, &relative_path)?),
            }
        }
        let entries_by_path: BTreeMap<&str, &FileEntry> = files
            .iter()
            .map(|entry| (entry.path.as_str(), entry))
            .collect();
        let mut segments = Vec::with_capacity(self.segments.len());

        for draft in &self.segments {
            let entry = entries_by_path.get(draft.path.as_str()).ok_or_else(|| {
                CdfError::data(format!(
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
