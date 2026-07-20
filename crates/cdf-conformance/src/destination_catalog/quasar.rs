use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

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

use super::{LogicalRow, logical_rows_from_batch};

const SCHEMES: &[&str] = &["quasar"];

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
        let bytes = fs::read(&path)
            .map_err(|error| CdfError::destination(format!("read {}: {error}", path.display())))?;
        let record: QuasarCommitRecord = serde_json::from_slice(&bytes).map_err(|error| {
            CdfError::destination(format!("decode {}: {error}", path.display()))
        })?;
        payload.extend(record.payload);
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
        Ok(Box::new(QuasarCommitSession {
            root: self.root.clone(),
            request: prepared.commit().clone(),
            plan: prepared.plan().clone(),
            schema_hash: prepared.schema_hash().clone(),
            duplicate,
            acknowledgements: Vec::new(),
            payload: Vec::new(),
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
    payload: Vec<LogicalRow>,
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
                self.payload.extend(logical_rows_from_batch(&batch.batch)?);
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

    fn finalize(self: Box<Self>) -> cdf_kernel::Result<Receipt> {
        if let Some(record) = self.duplicate {
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
        write_record(
            &root,
            receipt.idempotency_token.as_str(),
            &QuasarCommitRecord {
                receipt: receipt.clone(),
                payload: self.payload,
            },
        )?;
        Ok(receipt)
    }

    fn abort(self: Box<Self>) -> cdf_kernel::Result<()> {
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct QuasarCommitRecord {
    receipt: Receipt,
    payload: Vec<LogicalRow>,
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
    let encoded = token
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    root.join("commits").join(format!("{encoded}.json"))
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

fn write_record(root: &Path, token: &str, record: &QuasarCommitRecord) -> cdf_kernel::Result<()> {
    let path = record_path(root, token);
    let parent = path
        .parent()
        .ok_or_else(|| CdfError::destination("quasar commit path has no parent"))?;
    fs::create_dir_all(parent)
        .map_err(|error| CdfError::destination(format!("create {}: {error}", parent.display())))?;
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(|error| {
            CdfError::destination(format!("create {}: {error}", temporary.display()))
        })?;
    file.write_all(&cdf_package::canonical_json_bytes(record)?)
        .and_then(|()| file.sync_all())
        .map_err(|error| {
            CdfError::destination(format!("write {}: {error}", temporary.display()))
        })?;
    fs::rename(&temporary, &path).map_err(|error| {
        CdfError::destination(format!(
            "publish {} to {}: {error}",
            temporary.display(),
            path.display()
        ))
    })?;
    Ok(())
}
