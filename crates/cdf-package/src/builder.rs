use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{CdfError, Checkpoint, Result, SegmentId};
use serde::{Serialize, de::DeserializeOwned};

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
        ArtifactDurability, HashingWriter, atomic_write, build_manifest,
        collect_identity_file_entries, create_layout, file_entry_for_path, io_error,
        nested_artifact_path, normalize_artifact_path, package_path, segment_relative_path,
        sync_directory, visit_identity_file_paths, write_arrow_ipc_file, write_manifest_atomic,
    },
};

#[derive(Debug)]
pub struct PackageBuilder {
    package_dir: PathBuf,
    package_id: String,
    segment_drafts: Mutex<File>,
    artifact_receipts: Mutex<File>,
    trace: Mutex<HashingWriter<std::fs::File>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SegmentWriteMetrics {
    pub segment: SegmentEntry,
    pub encode_duration_ns: u64,
    pub persist_hash_duration_ns: u64,
}

#[derive(Clone, Debug, Serialize, serde::Deserialize)]
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
        let trace_path = package_dir.join(TRACE_FILE);
        let trace = OpenOptions::new()
            .append(true)
            .open(&trace_path)
            .map_err(|error| io_error(format!("open {}", trace_path.display()), error))?;

        Ok(Self {
            package_dir,
            package_id,
            segment_drafts: Mutex::new(
                tempfile::tempfile()
                    .map_err(|error| io_error("create package segment draft journal", error))?,
            ),
            artifact_receipts: Mutex::new(
                tempfile::tempfile()
                    .map_err(|error| io_error("create package artifact receipt journal", error))?,
            ),
            trace: Mutex::new(HashingWriter::new(trace)),
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
        append_journal(
            &self.artifact_receipts,
            &entry,
            "package artifact receipt journal",
        )?;
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
        let mut file = self
            .trace
            .lock()
            .map_err(|_| CdfError::internal("package trace sink lock is poisoned"))?;
        file.write_all(&bytes)
            .map_err(|error| io_error(format!("write {TRACE_FILE}"), error))
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
        if package_path(&self.package_dir, &relative_path).exists() {
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
        append_journal(
            &self.artifact_receipts,
            &file_entry,
            "package artifact receipt journal",
        )?;
        let segment = SegmentEntry {
            segment_id: segment_id.clone(),
            path: relative_path.clone(),
            row_count,
            byte_count: receipt.artifact.byte_count,
            sha256: receipt.artifact.sha256,
        };
        append_journal(
            &self.segment_drafts,
            &SegmentDraft {
                segment_id,
                path: relative_path,
                row_count,
            },
            "package segment draft journal",
        )?;
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
        let trace_entry = {
            let mut trace = self
                .trace
                .lock()
                .map_err(|_| CdfError::internal("package trace sink lock is poisoned"))?;
            trace.sync_all()?;
            trace.file_entry(TRACE_FILE)
        };
        append_journal(
            &self.artifact_receipts,
            &trace_entry,
            "package artifact receipt journal",
        )?;
        sync_directory(&self.package_dir)?;
        let mut pending_artifacts =
            read_journal::<FileEntry>(&self.artifact_receipts, "package artifact receipt journal")?
                .into_iter()
                .map(|entry| (entry.path.clone(), entry))
                .collect::<BTreeMap<_, _>>();
        let mut files = Vec::new();
        visit_identity_file_paths(&self.package_dir, |relative_path| {
            let path = package_path(&self.package_dir, &relative_path);
            let byte_count = std::fs::metadata(&path)
                .map_err(|error| io_error(format!("stat {}", path.display()), error))?
                .len();
            match pending_artifacts.remove(&relative_path) {
                Some(entry) if entry.byte_count == byte_count => files.push(entry),
                Some(entry) => {
                    return Err(CdfError::data(format!(
                        "identity artifact {relative_path} changed after its writer receipt: expected {} bytes, found {byte_count}",
                        entry.byte_count
                    )));
                }
                None => files.push(file_entry_for_path(&self.package_dir, &relative_path)?),
            }
            Ok(())
        })?;
        if let Some((path, _)) = pending_artifacts.first_key_value() {
            return Err(CdfError::data(format!(
                "identity artifact {path} is missing before package finalization"
            )));
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        let mut segments = Vec::new();
        visit_journal::<SegmentDraft>(
            &self.segment_drafts,
            "package segment draft journal",
            |draft| {
                let index = files
                    .binary_search_by(|entry| entry.path.as_str().cmp(draft.path.as_str()))
                    .map_err(|_| {
                        CdfError::data(format!(
                            "segment file {} missing before package finalization",
                            draft.path
                        ))
                    })?;
                let entry = &files[index];
                segments.push(SegmentEntry {
                    segment_id: draft.segment_id,
                    path: draft.path,
                    row_count: draft.row_count,
                    byte_count: entry.byte_count,
                    sha256: entry.sha256.clone(),
                });
                Ok(())
            },
        )?;

        let manifest = build_manifest(self.package_id.clone(), files, segments, status)?;
        write_manifest_atomic(&self.package_dir, &manifest)?;
        Ok(manifest)
    }
}

fn append_journal<T: Serialize>(journal: &Mutex<File>, value: &T, label: &str) -> Result<()> {
    let mut bytes = canonical_json_bytes(value)?;
    bytes.push(b'\n');
    journal
        .lock()
        .map_err(|_| CdfError::internal(format!("{label} lock is poisoned")))?
        .write_all(&bytes)
        .map_err(|error| io_error(format!("write {label}"), error))
}

fn read_journal<T: DeserializeOwned>(journal: &Mutex<File>, label: &str) -> Result<Vec<T>> {
    let mut values = Vec::new();
    visit_journal(journal, label, |value| {
        values.push(value);
        Ok(())
    })?;
    Ok(values)
}

fn visit_journal<T: DeserializeOwned>(
    journal: &Mutex<File>,
    label: &str,
    mut visit: impl FnMut(T) -> Result<()>,
) -> Result<()> {
    let mut file = journal
        .lock()
        .map_err(|_| CdfError::internal(format!("{label} lock is poisoned")))?;
    file.flush()
        .map_err(|error| io_error(format!("flush {label}"), error))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| io_error(format!("rewind {label}"), error))?;
    for line in BufReader::new(&mut *file).lines() {
        let line = line.map_err(|error| io_error(format!("read {label}"), error))?;
        visit(serde_json::from_str(&line).map_err(crate::json::json_error)?)?;
    }
    Ok(())
}
