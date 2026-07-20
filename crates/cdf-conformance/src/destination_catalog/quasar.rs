use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use cdf_kernel::{
    CapabilitySupport, CdfError, CommitCounts, CommitPlan, CommitSegmentIterator, CommitSession,
    ConcurrencyLimit, DeliveryGuarantee, DestinationCommitRequest, DestinationId,
    DestinationProtocol, DestinationProtocolCapabilities, DestinationSheet, IdempotencySupport,
    IdentifierRules, Receipt, ReceiptId, ReceiptVerification, SchemaHash, SegmentAck,
    TransactionSupport, TypeMapping, TypeMappingFidelity, VerifyClause, WriteDisposition,
};
use cdf_runtime::{
    BulkFallbackMode, BulkOrdering, BulkPathDescriptor, BulkSizeRange, DestinationDescription,
    DestinationDriver, DestinationIngress, DestinationIngressMode, DestinationInspection,
    DestinationPlanningContext, DestinationReceiptReportingPolicy, DestinationResolutionContext,
    DestinationRuntime, DestinationRuntimeCapabilities, DestinationWriterModel,
    FinalizedPackageIngress, PreparedDestinationCommit,
};
use serde::{Deserialize, Serialize};

use super::LogicalRow;

const SCHEMES: &[&str] = &["quasar"];
const RECEIPT_FILE_NAME: &str = "receipt.json";
const PAYLOAD_FILE_NAME: &str = "payload.ndjson";
static NEXT_STAGING_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct QuasarDriver;

pub(super) fn verify_receipt(receipt: &Receipt) -> cdf_kernel::Result<ReceiptVerification> {
    QuasarProtocol::new()?.verify(receipt)
}

pub(super) fn payload(root: &Path) -> cdf_kernel::Result<Vec<LogicalRow>> {
    let commits = root.join("commits");
    if !commits.exists() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(&commits)
        .map_err(|error| CdfError::destination(format!("read {}: {error}", commits.display())))?
        .map(|entry| entry.map(|entry| entry.path()))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| {
            CdfError::destination(format!("read entry in {}: {error}", commits.display()))
        })?;
    paths.sort();
    let mut payload = Vec::new();
    for path in paths {
        if !path.is_dir() {
            continue;
        }
        let payload_path = path.join(PAYLOAD_FILE_NAME);
        let file = fs::File::open(&payload_path).map_err(|error| {
            CdfError::destination(format!("open {}: {error}", payload_path.display()))
        })?;
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|error| {
                CdfError::destination(format!("read {}: {error}", payload_path.display()))
            })?;
            payload.push(serde_json::from_str(&line).map_err(|error| {
                CdfError::destination(format!("decode {}: {error}", payload_path.display()))
            })?);
        }
    }
    Ok(payload)
}

impl DestinationDriver for QuasarDriver {
    fn schemes(&self) -> &'static [&'static str] {
        SCHEMES
    }

    fn inspect(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> cdf_kernel::Result<DestinationInspection> {
        parse_root(uri)?;
        let protocol = QuasarProtocol::new()?;
        let sheet_artifact = protocol.sheet_artifact()?;
        Ok(DestinationInspection {
            description: description()?,
            sheet_artifact_hash: cdf_runtime::artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: runtime_capabilities(),
            health_probes: Vec::new(),
        })
    }

    fn resolve(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> cdf_kernel::Result<Box<dyn DestinationRuntime>> {
        Ok(Box::new(QuasarRuntime {
            root: parse_root(uri)?,
            protocol: QuasarProtocol::new()?,
        }))
    }
}

struct QuasarRuntime {
    root: PathBuf,
    protocol: QuasarProtocol,
}

impl DestinationRuntime for QuasarRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.protocol
    }

    fn ingress(&mut self) -> DestinationIngress<'_> {
        DestinationIngress::FinalizedPackage(self)
    }

    fn describe(&self) -> DestinationDescription {
        description().expect("static quasar destination description is valid")
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        runtime_capabilities()
    }
}

impl FinalizedPackageIngress for QuasarRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &cdf_package_contract::PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> cdf_kernel::Result<PreparedDestinationCommit> {
        runtime_capabilities().validate_prepared_bulk_path(context.bulk_path)?;
        let duplicate = read_record(
            &self.root,
            inputs.destination_commit.idempotency_token.as_str(),
        )?
        .is_some();
        let plan = self.protocol.plan_commit(&inputs.destination_commit)?;
        PreparedDestinationCommit::from_verified_inputs(
            inputs,
            plan,
            context.bulk_path.clone(),
            DestinationReceiptReportingPolicy::DestinationCommit { duplicate },
        )
    }

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut PreparedDestinationCommit,
    ) -> cdf_kernel::Result<Box<dyn CommitSession + '_>> {
        if prepared.has_pending_context() {
            return Err(CdfError::internal(
                "quasar conformance destination received unexpected pending context",
            ));
        }
        let duplicate = read_record(&self.root, prepared.commit().idempotency_token.as_str())?;
        let payload = if duplicate.is_none() {
            Some(QuasarPayloadWriter::create(
                &self.root,
                prepared.commit().idempotency_token.as_str(),
            )?)
        } else {
            None
        };
        Ok(Box::new(QuasarCommitSession {
            root: self.root.clone(),
            request: prepared.commit().clone(),
            plan: prepared.plan().clone(),
            schema_hash: prepared.schema_hash().clone(),
            duplicate,
            acknowledgements: Vec::new(),
            payload,
        }))
    }
}

struct QuasarProtocol {
    sheet: DestinationSheet,
}

impl QuasarProtocol {
    fn new() -> cdf_kernel::Result<Self> {
        Ok(Self {
            sheet: DestinationSheet {
                destination: DestinationId::new("quasar")?,
                supported_dispositions: vec![WriteDisposition::Append],
                transactions: TransactionSupport::AtomicPackage,
                idempotency: IdempotencySupport::PackageToken,
                type_mappings: vec![
                    TypeMapping {
                        arrow_type: "int64".to_owned(),
                        destination_type: "INT64".to_owned(),
                        fidelity: TypeMappingFidelity::Lossless,
                    },
                    TypeMapping {
                        arrow_type: "utf8".to_owned(),
                        destination_type: "UTF8".to_owned(),
                        fidelity: TypeMappingFidelity::Lossless,
                    },
                ],
                identifier_rules: IdentifierRules {
                    normalizer: "namecase-v1".to_owned(),
                    max_length: Some(128),
                    allowed_pattern: None,
                },
                migration_support: CapabilitySupport::Unsupported,
                quarantine_tables: CapabilitySupport::Unsupported,
                concurrency: ConcurrencyLimit {
                    max_writers: Some(1),
                },
            },
        })
    }
}

impl DestinationProtocol for QuasarProtocol {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn protocol_capabilities(&self) -> DestinationProtocolCapabilities {
        DestinationProtocolCapabilities::default()
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> cdf_kernel::Result<CommitPlan> {
        if !self
            .sheet
            .supported_dispositions
            .contains(&request.disposition)
        {
            return Err(CdfError::contract(format!(
                "quasar destination does not support {:?}",
                request.disposition
            )));
        }
        Ok(CommitPlan {
            plan_id: cdf_kernel::PlanId::new(format!(
                "quasar:{}:{}",
                request.target, request.idempotency_token
            ))?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        })
    }

    fn verify(&self, receipt: &Receipt) -> cdf_kernel::Result<ReceiptVerification> {
        let verified =
            read_record_from_receipt(receipt)?.is_some_and(|record| record.receipt == *receipt);
        Ok(ReceiptVerification {
            verified,
            receipt_id: receipt.receipt_id.clone(),
            reason: (!verified).then(|| "quasar destination receipt is not durable".to_owned()),
        })
    }
}

struct QuasarCommitSession {
    root: PathBuf,
    request: DestinationCommitRequest,
    plan: CommitPlan,
    schema_hash: SchemaHash,
    duplicate: Option<QuasarCommitRecord>,
    acknowledgements: Vec<SegmentAck>,
    payload: Option<QuasarPayloadWriter>,
}

impl CommitSession for QuasarCommitSession {
    fn apply_migrations(&mut self) -> cdf_kernel::Result<()> {
        Ok(())
    }

    fn write_segments(
        &mut self,
        segments: CommitSegmentIterator,
    ) -> cdf_kernel::Result<Vec<SegmentAck>> {
        if let Some(record) = &self.duplicate {
            return Ok(record.receipt.segment_acks.clone());
        }
        let payload = self
            .payload
            .as_mut()
            .expect("quasar payload writer was initialized");
        let mut acknowledgements = Vec::new();
        for segment in segments {
            let segment = segment?;
            let expected = self
                .request
                .segments
                .iter()
                .find(|expected| expected.segment_id == segment.state.segment_id)
                .ok_or_else(|| CdfError::data("quasar destination received undeclared segment"))?;
            if expected != &segment.state {
                return Err(CdfError::data(
                    "quasar destination segment identity differs from commit request",
                ));
            }
            for batch in segment.into_batches()? {
                payload.write_batch(&batch.batch)?;
            }
            let acknowledgement = SegmentAck {
                segment_id: expected.segment_id.clone(),
                row_count: expected.row_count,
                byte_count: expected.byte_count,
            };
            self.acknowledgements.push(acknowledgement.clone());
            acknowledgements.push(acknowledgement);
        }
        Ok(acknowledgements)
    }

    fn finalize(mut self: Box<Self>) -> cdf_kernel::Result<Receipt> {
        if let Some(record) = self.duplicate.take() {
            return Ok(record.receipt);
        }
        if self.acknowledgements.len() != self.request.segments.len() {
            return Err(CdfError::destination(
                "quasar destination did not acknowledge every declared segment",
            ));
        }
        let rows_written = self
            .acknowledgements
            .iter()
            .map(|acknowledgement| acknowledgement.row_count)
            .sum();
        let root = self.root.clone();
        let receipt = Receipt {
            receipt_id: ReceiptId::new(format!(
                "quasar:{}:{}",
                self.request.target, self.request.idempotency_token
            ))?,
            destination: DestinationId::new("quasar")?,
            target: self.request.target.clone(),
            package_hash: self.request.package_hash.clone(),
            segment_acks: self.acknowledgements,
            disposition: self.request.disposition.clone(),
            idempotency_token: self.request.idempotency_token.clone(),
            transaction: None,
            counts: CommitCounts {
                rows_written,
                rows_inserted: Some(rows_written),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: self.schema_hash,
            migrations: self.plan.migrations,
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "quasar_file".to_owned(),
                statement: root.display().to_string(),
                parameters: BTreeMap::new(),
            },
        };
        let payload = self.payload.take().ok_or_else(|| {
            CdfError::destination("quasar destination received no payload writer")
        })?;
        payload.publish(&QuasarCommitRecord {
            receipt: receipt.clone(),
        })?;
        Ok(receipt)
    }

    fn abort(mut self: Box<Self>) -> cdf_kernel::Result<()> {
        self.payload.take();
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct QuasarCommitRecord {
    receipt: Receipt,
}

struct QuasarPayloadWriter {
    writer: Option<BufWriter<fs::File>>,
    staging_root: Option<PathBuf>,
    final_root: PathBuf,
}

impl QuasarPayloadWriter {
    fn create(root: &Path, token: &str) -> cdf_kernel::Result<Self> {
        let commits = root.join("commits");
        fs::create_dir_all(&commits).map_err(|error| {
            CdfError::destination(format!("create {}: {error}", commits.display()))
        })?;
        let encoded = encoded_token(token);
        let staging_id = NEXT_STAGING_ID.fetch_add(1, Ordering::Relaxed);
        let staging_root = commits.join(format!(
            ".{encoded}.tmp-{}-{staging_id}",
            std::process::id()
        ));
        fs::create_dir(&staging_root).map_err(|error| {
            CdfError::destination(format!("create {}: {error}", staging_root.display()))
        })?;
        let payload_path = staging_root.join(PAYLOAD_FILE_NAME);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&payload_path)
            .map_err(|error| {
                CdfError::destination(format!("create {}: {error}", payload_path.display()))
            })?;
        Ok(Self {
            writer: Some(BufWriter::new(file)),
            staging_root: Some(staging_root),
            final_root: commits.join(encoded),
        })
    }

    fn write_batch(&mut self, batch: &RecordBatch) -> cdf_kernel::Result<()> {
        let id_index = batch
            .schema()
            .index_of("id")
            .map_err(|error| CdfError::data(format!("quasar payload misses id: {error}")))?;
        let name_index = batch
            .schema()
            .index_of("name")
            .map_err(|error| CdfError::data(format!("quasar payload misses name: {error}")))?;
        let ids = batch
            .column(id_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| CdfError::data("quasar payload id is not int64"))?;
        let names = batch
            .column(name_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| CdfError::data("quasar payload name is not utf8"))?;
        for index in 0..batch.num_rows() {
            let row = QuasarPayloadRow {
                id: ids.value(index),
                name: (!names.is_null(index)).then(|| names.value(index)),
            };
            let writer = self
                .writer
                .as_mut()
                .expect("quasar payload writer exists until publish");
            writer
                .write_all(&cdf_package::canonical_json_bytes(&row)?)
                .map_err(|error| CdfError::destination(format!("write quasar payload: {error}")))?;
            writer
                .write_all(b"\n")
                .map_err(|error| CdfError::destination(format!("write quasar payload: {error}")))?;
        }
        Ok(())
    }

    fn publish(mut self, record: &QuasarCommitRecord) -> cdf_kernel::Result<()> {
        let mut writer = self
            .writer
            .take()
            .expect("quasar payload writer exists until publish");
        writer
            .flush()
            .and_then(|()| writer.get_ref().sync_all())
            .map_err(|error| CdfError::destination(format!("sync quasar payload: {error}")))?;
        drop(writer);
        let staging_root = self
            .staging_root
            .as_ref()
            .expect("quasar staging root exists until publish");
        write_record(&staging_root.join(RECEIPT_FILE_NAME), record)?;
        fs::rename(staging_root, &self.final_root).map_err(|error| {
            CdfError::destination(format!(
                "publish {} to {}: {error}",
                staging_root.display(),
                self.final_root.display()
            ))
        })?;
        self.staging_root = None;
        Ok(())
    }
}

#[derive(Serialize)]
struct QuasarPayloadRow<'a> {
    id: i64,
    name: Option<&'a str>,
}

impl Drop for QuasarPayloadWriter {
    fn drop(&mut self) {
        if let Some(staging_root) = self.staging_root.take() {
            let _ = fs::remove_dir_all(staging_root);
        }
    }
}

fn description() -> cdf_kernel::Result<DestinationDescription> {
    Ok(DestinationDescription::new(
        DestinationId::new("quasar")?,
        SCHEMES,
        "quasar conformance destination",
    ))
}

fn runtime_capabilities() -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        bulk_paths: vec![BulkPathDescriptor {
            path_id: "quasar_native".to_owned(),
            version: 1,
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            writer_model: DestinationWriterModel::SingleWriter,
            ordering: BulkOrdering::ManifestOrder,
            rows: BulkSizeRange {
                minimum: 1,
                preferred: 64 * 1024,
                maximum: 1024 * 1024,
            },
            bytes: BulkSizeRange {
                minimum: 1,
                preferred: 16 * 1024 * 1024,
                maximum: 64 * 1024 * 1024,
            },
            max_useful_writers: 1,
            blocking_lane: None,
            native_internal_parallelism: 1,
            external_staging: false,
            fallback: BulkFallbackMode::Forbidden,
            schema_preflight_version: "quasar-schema@1".to_owned(),
            measured_evidence_version: Some("quasar-conformance-v1".to_owned()),
        }],
        bulk_path: Some("quasar_native".to_owned()),
        bulk_evidence_version: Some("quasar-conformance-v1".to_owned()),
        ..Default::default()
    }
}

fn parse_root(uri: &str) -> cdf_kernel::Result<PathBuf> {
    let path = uri
        .strip_prefix("quasar://")
        .ok_or_else(|| CdfError::contract("quasar driver received a non-quasar URI"))?;
    if path.is_empty() {
        return Err(CdfError::contract("quasar destination path is empty"));
    }
    Ok(PathBuf::from(path))
}

fn record_path(root: &Path, token: &str) -> PathBuf {
    root.join("commits")
        .join(encoded_token(token))
        .join(RECEIPT_FILE_NAME)
}

fn encoded_token(token: &str) -> String {
    token
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_record(root: &Path, token: &str) -> cdf_kernel::Result<Option<QuasarCommitRecord>> {
    let path = record_path(root, token);
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path)
        .map_err(|error| CdfError::destination(format!("read {}: {error}", path.display())))?;
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| CdfError::destination(format!("decode {}: {error}", path.display())))
}

fn read_record_from_receipt(receipt: &Receipt) -> cdf_kernel::Result<Option<QuasarCommitRecord>> {
    read_record(
        Path::new(&receipt.verify.statement),
        receipt.idempotency_token.as_str(),
    )
}

fn write_record(path: &Path, record: &QuasarCommitRecord) -> cdf_kernel::Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| CdfError::destination("quasar commit path has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| CdfError::destination(format!("create {}: {error}", parent.display())))?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .map_err(|error| CdfError::destination(format!("create {}: {error}", path.display())))?;
    file.write_all(&cdf_package::canonical_json_bytes(record)?)
        .and_then(|()| file.sync_all())
        .map_err(|error| CdfError::destination(format!("write {}: {error}", path.display())))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use cdf_kernel::{
        CommitCounts, DestinationId, Receipt, ReceiptId, SchemaHash, VerifyClause, WriteDisposition,
    };

    use super::*;

    #[test]
    fn quasar_payload_is_streamed_to_an_atomic_external_commit() {
        let temp = tempfile::tempdir().unwrap();
        let token = "sha256:quasar-streaming-payload";
        let mut writer = QuasarPayloadWriter::create(temp.path(), token).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        writer
            .write_batch(
                &RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![1, 2])),
                        Arc::new(StringArray::from(vec![Some("ada"), None])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        let request = crate::destination::representative_commit_request(WriteDisposition::Append);
        let receipt = Receipt {
            receipt_id: ReceiptId::new("quasar-streaming-receipt").unwrap(),
            destination: DestinationId::new("quasar").unwrap(),
            target: request.target,
            package_hash: request.package_hash,
            segment_acks: Vec::new(),
            disposition: request.disposition,
            idempotency_token: cdf_kernel::IdempotencyToken::new(token).unwrap(),
            transaction: None,
            counts: CommitCounts {
                rows_written: 2,
                rows_inserted: Some(2),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new("schema-quasar-streaming").unwrap(),
            migrations: Vec::new(),
            committed_at_ms: 1_700_000_000_000,
            verify: VerifyClause {
                kind: "quasar_file".to_owned(),
                statement: temp.path().display().to_string(),
                parameters: BTreeMap::new(),
            },
        };
        writer
            .publish(&QuasarCommitRecord {
                receipt: receipt.clone(),
            })
            .unwrap();

        assert_eq!(
            read_record(temp.path(), token).unwrap().unwrap().receipt,
            receipt
        );
        assert_eq!(
            payload(temp.path()).unwrap(),
            vec![
                LogicalRow {
                    id: 1,
                    name: Some("ada".to_owned()),
                },
                LogicalRow { id: 2, name: None },
            ]
        );
        let record_bytes = fs::read(record_path(temp.path(), token)).unwrap();
        let record_json: serde_json::Value = serde_json::from_slice(&record_bytes).unwrap();
        assert!(!record_json.as_object().unwrap().contains_key("payload"));
        assert!(
            fs::read_dir(temp.path().join("commits"))
                .unwrap()
                .all(|entry| !entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with('.'))
        );
    }

    #[test]
    fn quasar_aborted_payload_removes_external_staging() {
        let temp = tempfile::tempdir().unwrap();
        drop(QuasarPayloadWriter::create(temp.path(), "aborted").unwrap());
        assert_eq!(
            fs::read_dir(temp.path().join("commits")).unwrap().count(),
            0
        );
    }
}
