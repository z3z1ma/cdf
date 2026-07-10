use super::*;
use std::{collections::BTreeMap, sync::Arc};

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

fn sample_state_delta_and_receipt() -> (StateDelta, Receipt) {
    let scope = ScopeKey::Partition {
        partition_id: PartitionId::new("p0").unwrap(),
    };
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: 7,
        field: "updated_at".to_owned(),
        value: CursorValue::TimestampMicros {
            micros: 1_700_000_000_000_000,
            timezone: Some("America/Phoenix".to_owned()),
        },
    });
    let segment = StateSegment {
        segment_id: SegmentId::new("segment-1").unwrap(),
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: 3,
        byte_count: 24,
    };
    let delta = StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-1").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        scope,
        state_version: 1,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        package_hash: PackageHash::new("package-sha256").unwrap(),
        schema_hash: SchemaHash::new("schema-sha256").unwrap(),
        segments: vec![segment],
    };
    let receipt = Receipt {
        receipt_id: ReceiptId::new("receipt-1").unwrap(),
        destination: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("orders").unwrap(),
        package_hash: PackageHash::new("package-sha256").unwrap(),
        segment_acks: vec![SegmentAck {
            segment_id: SegmentId::new("segment-1").unwrap(),
            row_count: 3,
            byte_count: 24,
        }],
        disposition: WriteDisposition::Merge,
        idempotency_token: IdempotencyToken::new("package-sha256").unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: 3,
            rows_inserted: Some(3),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: SchemaHash::new("schema-sha256").unwrap(),
        migrations: Vec::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "sql".to_owned(),
            statement: "select count(*) from orders where _cdf_package = ?".to_owned(),
            parameters: BTreeMap::new(),
        },
    };

    (delta, receipt)
}

fn sample_destination_sheet() -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new("fake-session").unwrap(),
        supported_dispositions: vec![WriteDisposition::Merge],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: vec![TypeMapping {
            arrow_type: "Int64".to_owned(),
            destination_type: "BIGINT".to_owned(),
            fidelity: TypeMappingFidelity::Lossless,
        }],
        identifier_rules: IdentifierRules {
            normalizer: "lowercase".to_owned(),
            max_length: Some(63),
            allowed_pattern: Some("^[a-z_][a-z0-9_]*$".to_owned()),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Unsupported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    }
}

fn sample_destination_commit_request(delta: &StateDelta) -> DestinationCommitRequest {
    DestinationCommitRequest {
        package_hash: delta.package_hash.clone(),
        target: TargetName::new("orders").unwrap(),
        disposition: WriteDisposition::Merge,
        segments: delta.segments.clone(),
        idempotency_token: IdempotencyToken::new(delta.package_hash.as_str()).unwrap(),
    }
}

struct FakeSessionDestination {
    sheet: DestinationSheet,
}

impl DestinationProtocol for FakeSessionDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        Ok(sample_commit_plan(request))
    }

    fn begin(
        &self,
        request: DestinationCommitRequest,
        plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        if plan.target != request.target || plan.disposition != request.disposition {
            return Err(CdfError::destination(
                "commit plan does not match destination request",
            ));
        }
        Ok(Box::new(FakeCommitSession {
            destination: self.sheet.destination.clone(),
            request,
            plan,
            migrations_applied: false,
            accepted_segments: Vec::new(),
        }))
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        let verified = receipt.destination == self.sheet.destination;
        Ok(ReceiptVerification {
            verified,
            receipt_id: receipt.receipt_id.clone(),
            reason: if verified {
                None
            } else {
                Some("receipt destination does not match verifier".to_owned())
            },
        })
    }
}

fn sample_commit_plan(request: &DestinationCommitRequest) -> CommitPlan {
    CommitPlan {
        plan_id: PlanId::new(format!(
            "fake-plan:{}:{}",
            request.target.as_str(),
            request.idempotency_token.as_str()
        ))
        .unwrap(),
        target: request.target.clone(),
        disposition: request.disposition.clone(),
        idempotency: IdempotencySupport::PackageToken,
        migrations: vec![MigrationRecord {
            migration_id: "migration-1".to_owned(),
            description: "create target table".to_owned(),
        }],
        delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerKey,
    }
}

struct FakeCommitSession {
    destination: DestinationId,
    request: DestinationCommitRequest,
    plan: CommitPlan,
    migrations_applied: bool,
    accepted_segments: Vec<SegmentAck>,
}

impl CommitSession for FakeCommitSession {
    fn apply_migrations(&mut self) -> Result<()> {
        self.migrations_applied = true;
        Ok(())
    }

    fn write_segment(&mut self, segment: CommitSegment) -> Result<SegmentAck> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "migrations must be applied before writing",
            ));
        }
        if self
            .accepted_segments
            .iter()
            .any(|ack| ack.segment_id == segment.state.segment_id)
        {
            return Err(CdfError::destination(format!(
                "segment {} was already written",
                segment.state.segment_id
            )));
        }
        let requested = self
            .request
            .segments
            .iter()
            .find(|requested| requested.segment_id == segment.state.segment_id)
            .ok_or_else(|| {
                CdfError::destination(format!(
                    "segment {} is not part of the destination request",
                    segment.state.segment_id
                ))
            })?;
        if requested != &segment.state {
            return Err(CdfError::destination(format!(
                "segment {} state does not match destination request",
                segment.state.segment_id
            )));
        }
        let batch_rows = segment
            .batches
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum::<u64>();
        if batch_rows != segment.state.row_count {
            return Err(CdfError::destination(format!(
                "segment {} has {} batch rows but request expects {}",
                segment.state.segment_id, batch_rows, segment.state.row_count
            )));
        }
        let ack = SegmentAck {
            segment_id: segment.state.segment_id,
            row_count: segment.state.row_count,
            byte_count: segment.state.byte_count,
        };
        self.accepted_segments.push(ack.clone());
        Ok(ack)
    }

    fn finalize(self: Box<Self>) -> Result<Receipt> {
        if self.accepted_segments.len() != self.request.segments.len() {
            return Err(CdfError::destination(
                "cannot finalize before all package segments are written",
            ));
        }
        let mut parameters = BTreeMap::new();
        parameters.insert("plan_id".to_owned(), self.plan.plan_id.as_str().to_owned());
        let rows_written = self
            .request
            .segments
            .iter()
            .map(|segment| segment.row_count)
            .sum();
        Ok(Receipt {
            receipt_id: ReceiptId::new(format!(
                "receipt-{}",
                self.request.idempotency_token.as_str()
            ))?,
            destination: self.destination,
            target: self.request.target,
            package_hash: self.request.package_hash,
            segment_acks: self.accepted_segments,
            disposition: self.plan.disposition,
            idempotency_token: self.request.idempotency_token,
            transaction: None,
            counts: CommitCounts {
                rows_written,
                rows_inserted: Some(rows_written),
                rows_updated: Some(0),
                rows_deleted: Some(0),
            },
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            migrations: self.plan.migrations,
            committed_at_ms: 1_700_000_000_100,
            verify: VerifyClause {
                kind: "fake".to_owned(),
                statement: "verify fake durable receipt".to_owned(),
                parameters,
            },
        })
    }

    fn abort(self: Box<Self>) -> Result<()> {
        Ok(())
    }
}

#[test]
fn commit_session_api_writes_segments_and_finalizes_to_durable_receipt() {
    let destination = FakeSessionDestination {
        sheet: sample_destination_sheet(),
    };
    let (delta, _) = sample_state_delta_and_receipt();
    let request = sample_destination_commit_request(&delta);
    let plan = destination.plan_commit(&request).unwrap();

    let mut session = destination.begin(request, plan).unwrap();
    session.apply_migrations().unwrap();
    let segment = delta.segments[0].clone();
    let ack = session
        .write_segment(sample_commit_segment(segment.clone()))
        .unwrap();
    assert_eq!(
        ack,
        SegmentAck {
            segment_id: segment.segment_id,
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        }
    );
    let receipt = session.finalize().unwrap();

    assert_eq!(receipt.destination, destination.sheet().destination);
    assert!(receipt.covers_state_delta(&delta));
    assert_eq!(receipt.segment_acks.len(), delta.segments.len());
    assert_eq!(receipt.counts.rows_written, 3);
    assert_eq!(receipt.migrations.len(), 1);
    assert_eq!(receipt.verify.kind, "fake");

    let protocol: &dyn DestinationProtocol = &destination;
    let verification = protocol.verify(&receipt).unwrap();
    assert!(verification.verified);
    assert_eq!(verification.receipt_id, receipt.receipt_id);
    assert_eq!(verification.reason, None);

    let request = sample_destination_commit_request(&delta);
    let plan = destination.plan_commit(&request).unwrap();
    let mut session = destination.begin(request, plan).unwrap();
    session.apply_migrations().unwrap();
    session.abort().unwrap();
}

fn sample_commit_segment(state: StateSegment) -> CommitSegment {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let values = (0..state.row_count as i64).collect::<Vec<_>>();
    let column: ArrayRef = Arc::new(Int64Array::from(values));
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
    CommitSegment {
        state,
        package_byte_count: 96,
        batches: vec![batch],
    }
}

#[test]
fn metadata_helpers_round_trip_cdf_annotations() {
    let field = Field::new("normalized_name", DataType::Utf8, true);
    let field = with_cdf_metadata(
        field,
        Some("Original Name"),
        Some("pii:email"),
        Some("source_absent"),
    );

    assert_eq!(source_name(&field), Some("Original Name"));
    assert_eq!(semantic(&field), Some("pii:email"));
    let field = with_physical_type(field, "Int32");

    assert_eq!(null_origin(&field), Some("source_absent"));
    assert_eq!(physical_type(&field), Some("Int32"));
    assert_eq!(
        field.metadata().get(SOURCE_NAME_METADATA_KEY),
        Some(&"Original Name".to_owned())
    );
}

#[test]
fn destination_correction_vocabulary_is_backward_compatible_and_semver_stable() {
    let legacy_sheet = sample_destination_sheet();
    let legacy_json = serde_json::to_string(&legacy_sheet).unwrap();
    assert!(!legacy_json.contains("corrections"));
    let decoded: DestinationSheetArtifact = serde_json::from_str(&legacy_json).unwrap();
    assert_eq!(
        decoded.protocol_capabilities,
        DestinationProtocolCapabilities::default()
    );
    assert_eq!(decoded.sheet, legacy_sheet);
    assert_eq!(serde_json::to_string(&decoded).unwrap(), legacy_json);

    assert_eq!(
        serde_json::to_string(&[
            CorrectionStrategy::InPlaceUpdate,
            CorrectionStrategy::CorrectionSidecar,
            CorrectionStrategy::VersionedRematerialization,
        ])
        .unwrap(),
        r#"["in_place_update","correction_sidecar","versioned_rematerialization"]"#
    );
    assert!(serde_json::from_str::<CorrectionStrategy>(r#""unsafe_update""#).is_err());

    let sidecar = DestinationCorrectionCapabilities {
        strategies: vec![CorrectionStrategyCapability {
            strategy: CorrectionStrategy::CorrectionSidecar,
            transaction_guarantee: TransactionSupport::AtomicPackage,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        }],
        ..DestinationCorrectionCapabilities::default()
    };
    sidecar
        .validate(
            &TransactionSupport::AtomicPackage,
            &IdempotencySupport::PackageToken,
        )
        .unwrap();
    let rematerialization = DestinationCorrectionCapabilities {
        strategies: vec![CorrectionStrategyCapability {
            strategy: CorrectionStrategy::VersionedRematerialization,
            transaction_guarantee: TransactionSupport::AtomicTarget,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        }],
        ..DestinationCorrectionCapabilities::default()
    };
    rematerialization
        .validate(
            &TransactionSupport::AtomicTarget,
            &IdempotencySupport::PackageToken,
        )
        .unwrap();

    let mut unsupported_version = DestinationCorrectionCapabilities::default();
    unsupported_version.version += 1;
    assert!(
        unsupported_version
            .validate(
                &TransactionSupport::AtomicPackage,
                &IdempotencySupport::PackageToken,
            )
            .unwrap_err()
            .to_string()
            .contains("unsupported destination correction capabilities version")
    );
}

#[test]
fn row_provenance_and_correction_plan_round_trip_without_destination_types() {
    let original_row = RowProvenanceAddress::new(
        PackageHash::new("sha256:original-package").unwrap(),
        SegmentId::new("seg-000001").unwrap(),
        0,
    );
    let request = DestinationCorrectionRequest {
        promotion_id: PromotionId::new("promotion-001").unwrap(),
        original_row: original_row.clone(),
        old_schema_hash: SchemaHash::new("sha256:old-schema").unwrap(),
        new_schema_hash: SchemaHash::new("sha256:new-schema").unwrap(),
        promoted_path: "/payload/customer_id".to_owned(),
        promoted_value_json: r#"{"arrow_type":"int64","value":"42"}"#.to_owned(),
        residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
        selected_strategy: CorrectionStrategy::InPlaceUpdate,
    };
    let plan = DestinationCorrectionPlan {
        request,
        transaction_guarantee: TransactionSupport::AtomicPackage,
        idempotency_guarantee: IdempotencySupport::PackageToken,
    };
    let capabilities = DestinationCorrectionCapabilities {
        version: DESTINATION_CORRECTION_CAPABILITIES_VERSION,
        row_provenance: RowProvenanceCapabilities {
            persistence: CapabilitySupport::Supported,
            targetability: CapabilitySupport::Supported,
        },
        residual_readback: CapabilitySupport::Supported,
        strategies: vec![CorrectionStrategyCapability {
            strategy: CorrectionStrategy::InPlaceUpdate,
            transaction_guarantee: TransactionSupport::AtomicPackage,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        }],
    };

    plan.validate_for(
        &capabilities,
        &TransactionSupport::AtomicPackage,
        &IdempotencySupport::PackageToken,
    )
    .unwrap();
    let encoded = serde_json::to_string(&plan).unwrap();
    assert!(encoded.contains(r#""original_row_ordinal":0"#));
    assert!(encoded.contains(r#""selected_strategy":"in_place_update""#));
    assert!(encoded.contains(r#""residual_operation":"remove_promoted_path""#));
    assert!(!encoded.contains("merge_key"));
    let decoded: DestinationCorrectionPlan = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, plan);
    assert_eq!(decoded.request.original_row, original_row);
}

#[test]
fn correction_capability_validation_rejects_impossible_claims_and_plans() {
    let mut targetable_without_persistence = DestinationCorrectionCapabilities::default();
    targetable_without_persistence.row_provenance.targetability = CapabilitySupport::Supported;
    assert!(
        targetable_without_persistence
            .validate(
                &TransactionSupport::AtomicPackage,
                &IdempotencySupport::PackageToken,
            )
            .is_err()
    );

    let mut in_place_without_targetability = DestinationCorrectionCapabilities::default();
    in_place_without_targetability.row_provenance.persistence = CapabilitySupport::Supported;
    in_place_without_targetability.strategies = vec![CorrectionStrategyCapability {
        strategy: CorrectionStrategy::InPlaceUpdate,
        transaction_guarantee: TransactionSupport::AtomicPackage,
        idempotency_guarantee: IdempotencySupport::PackageToken,
    }];
    assert!(
        in_place_without_targetability
            .validate(
                &TransactionSupport::AtomicPackage,
                &IdempotencySupport::PackageToken,
            )
            .unwrap_err()
            .to_string()
            .contains("targetable persisted row provenance")
    );

    let mut duplicate_strategy = DestinationCorrectionCapabilities {
        row_provenance: RowProvenanceCapabilities {
            persistence: CapabilitySupport::Supported,
            targetability: CapabilitySupport::Supported,
        },
        ..DestinationCorrectionCapabilities::default()
    };
    let strategy = CorrectionStrategyCapability {
        strategy: CorrectionStrategy::InPlaceUpdate,
        transaction_guarantee: TransactionSupport::AtomicPackage,
        idempotency_guarantee: IdempotencySupport::PackageToken,
    };
    duplicate_strategy.strategies = vec![strategy.clone(), strategy];
    assert!(
        duplicate_strategy
            .validate(
                &TransactionSupport::AtomicPackage,
                &IdempotencySupport::PackageToken,
            )
            .is_err()
    );

    let unsupported_plan = DestinationCorrectionPlan {
        request: DestinationCorrectionRequest {
            promotion_id: PromotionId::new("promotion-unsupported").unwrap(),
            original_row: RowProvenanceAddress::new(
                PackageHash::new("sha256:original-package").unwrap(),
                SegmentId::new("seg-000001").unwrap(),
                7,
            ),
            old_schema_hash: SchemaHash::new("sha256:old-schema").unwrap(),
            new_schema_hash: SchemaHash::new("sha256:new-schema").unwrap(),
            promoted_path: "/payload/customer_id".to_owned(),
            promoted_value_json: "42".to_owned(),
            residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
            selected_strategy: CorrectionStrategy::CorrectionSidecar,
        },
        transaction_guarantee: TransactionSupport::AtomicPackage,
        idempotency_guarantee: IdempotencySupport::PackageToken,
    };
    assert!(
        unsupported_plan
            .validate_for(
                &DestinationCorrectionCapabilities::default(),
                &TransactionSupport::AtomicPackage,
                &IdempotencySupport::PackageToken,
            )
            .unwrap_err()
            .to_string()
            .contains("does not support correction strategy")
    );
}

#[test]
fn batch_wraps_arrow_record_batch_and_reports_counts() {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let column: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let record_batch = RecordBatch::try_new(schema, vec![column]).unwrap();

    let batch = Batch::from_record_batch(
        BatchId::new("batch-1").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("p0").unwrap(),
        SchemaHash::new("schema-sha256").unwrap(),
        record_batch,
    )
    .unwrap();

    assert_eq!(batch.header.row_count, 3);
    assert!(batch.header.byte_count > 0);
    assert!(batch.header.pre_contract_quarantine.is_empty());
    assert!(batch.record_batch().is_some());
}

#[test]
fn batch_header_serde_defaults_missing_optional_evidence_fields() {
    let header = BatchHeader {
        batch_id: BatchId::new("batch-legacy").unwrap(),
        resource_id: ResourceId::new("orders").unwrap(),
        partition_id: PartitionId::new("p0").unwrap(),
        observed_schema_hash: SchemaHash::new("schema-sha256").unwrap(),
        row_count: 1,
        byte_count: 8,
        source_position: None,
        pre_contract_quarantine: Vec::new(),
        schema_coercion_plan: None,
        watermarks: Vec::new(),
        stats: BatchStats::default(),
        cdc: None,
    };

    let mut json = serde_json::to_value(&header).unwrap();
    assert!(json.get("pre_contract_quarantine").is_none());
    assert!(json.get("schema_coercion_plan").is_none());
    json.as_object_mut()
        .unwrap()
        .remove("pre_contract_quarantine");

    let decoded: BatchHeader = serde_json::from_value(json).unwrap();

    assert!(decoded.pre_contract_quarantine.is_empty());
    assert!(decoded.schema_coercion_plan.is_none());
}

#[test]
fn artifact_values_serde_round_trip() {
    let descriptor = ResourceDescriptor {
        resource_id: ResourceId::new("orders").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            source: "contract/orders.v1".to_owned(),
        },
        primary_key: vec!["id".to_owned()],
        merge_key: vec!["id".to_owned()],
        cursor: Some(CursorSpec {
            field: "updated_at".to_owned(),
            ordering: CursorOrderingClaim::Inexact,
            lag_tolerance_ms: 60_000,
        }),
        write_disposition: WriteDisposition::Merge,
        contract: Some(ContractRef::new("orders-contract").unwrap()),
        state_scope: ScopeKey::Partition {
            partition_id: PartitionId::new("p0").unwrap(),
        },
        freshness: Some(FreshnessSpec {
            max_age_ms: 300_000,
        }),
        trust_level: TrustLevel::Governed,
    };

    let json = serde_json::to_string(&descriptor).unwrap();
    assert_eq!(
        descriptor,
        serde_json::from_str::<ResourceDescriptor>(&json).unwrap()
    );

    let output_position = SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "updated_at".to_owned(),
        value: CursorValue::TimestampMicros {
            micros: 1_700_000_000_000_000,
            timezone: Some("America/Phoenix".to_owned()),
        },
    });
    let segment = StateSegment {
        segment_id: SegmentId::new("segment-1").unwrap(),
        scope: descriptor.state_scope.clone(),
        output_position: output_position.clone(),
        row_count: 3,
        byte_count: 24,
    };
    let delta = StateDelta {
        checkpoint_id: CheckpointId::new("checkpoint-1").unwrap(),
        pipeline_id: PipelineId::new("pipeline-1").unwrap(),
        resource_id: descriptor.resource_id.clone(),
        scope: descriptor.state_scope.clone(),
        state_version: 1,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        package_hash: PackageHash::new("package-sha256").unwrap(),
        schema_hash: SchemaHash::new("schema-sha256").unwrap(),
        segments: vec![segment],
    };
    let receipt = Receipt {
        receipt_id: ReceiptId::new("receipt-1").unwrap(),
        destination: DestinationId::new("local-test").unwrap(),
        target: TargetName::new("orders").unwrap(),
        package_hash: PackageHash::new("package-sha256").unwrap(),
        segment_acks: vec![SegmentAck {
            segment_id: SegmentId::new("segment-1").unwrap(),
            row_count: 3,
            byte_count: 24,
        }],
        disposition: WriteDisposition::Merge,
        idempotency_token: IdempotencyToken::new("package-sha256").unwrap(),
        transaction: None,
        counts: CommitCounts {
            rows_written: 3,
            rows_inserted: Some(3),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: SchemaHash::new("schema-sha256").unwrap(),
        migrations: Vec::new(),
        committed_at_ms: 1_700_000_000_000,
        verify: VerifyClause {
            kind: "sql".to_owned(),
            statement: "select count(*) from orders where _cdf_package = ?".to_owned(),
            parameters: BTreeMap::new(),
        },
    };

    assert!(receipt.covers_state_delta(&delta));
    let delta_json = serde_json::to_string(&delta).unwrap();
    assert_eq!(
        delta,
        serde_json::from_str::<StateDelta>(&delta_json).unwrap()
    );
    let receipt_json = serde_json::to_string(&receipt).unwrap();
    assert_eq!(
        receipt,
        serde_json::from_str::<Receipt>(&receipt_json).unwrap()
    );
}

#[test]
fn schema_source_modes_serde_round_trip() {
    let snapshot = SchemaSnapshotReference {
        schema_hash: SchemaHash::new("sha256:snapshot").unwrap(),
        path: ".cdf/schemas/orders@sha256:snapshot.json".to_owned(),
        metadata: BTreeMap::from([("probe".to_owned(), "parquet-footer".to_owned())]),
    };
    assert!(snapshot.discovery_manifest().unwrap().is_none());
    let manifest = DiscoveryManifestReference {
        manifest_hash: DiscoveryManifestHash::new("sha256:manifest").unwrap(),
        path: ".cdf/schemas/orders@sha256:manifest.discovery.json".to_owned(),
    };
    let linked = snapshot.clone().with_discovery_manifest(&manifest).unwrap();
    assert_eq!(linked.discovery_manifest().unwrap(), Some(manifest));
    assert!(
        serde_json::to_value(&linked)
            .unwrap()
            .get("discovery_manifest")
            .is_none()
    );
    let mut partial = snapshot.clone();
    partial.metadata.insert(
        DISCOVERY_MANIFEST_HASH_METADATA_KEY.to_owned(),
        "sha256:partial".to_owned(),
    );
    assert!(partial.discovery_manifest().is_err());
    let sources = vec![
        SchemaSource::Declared {
            schema_hash: SchemaHash::new("sha256:declared").unwrap(),
            source: "declarative:orders".to_owned(),
        },
        SchemaSource::Discover,
        SchemaSource::Discovered {
            snapshot: snapshot.clone(),
        },
        SchemaSource::Hints {
            source: "declarative:orders".to_owned(),
            hints_hash: Some(SchemaHash::new("sha256:hints").unwrap()),
            snapshot: Some(snapshot.clone()),
        },
    ];

    for source in sources {
        let json = serde_json::to_string(&source).unwrap();
        assert_eq!(source, serde_json::from_str::<SchemaSource>(&json).unwrap());
    }
    assert_eq!(
        SchemaSource::Discovered {
            snapshot: snapshot.clone(),
        }
        .pinned_snapshot(),
        Some(&snapshot)
    );
    assert_eq!(SchemaSource::Discover.pinned_snapshot(), None);
}

#[test]
fn checkpoint_contract_values_serde_round_trip() {
    let (delta, receipt) = sample_state_delta_and_receipt();
    assert_eq!(CHECKPOINT_STATE_VERSION, 1);
    assert_eq!(CheckpointStatus::Committed.as_str(), "committed");
    assert_eq!(
        CheckpointStatus::parse("rewound").unwrap(),
        CheckpointStatus::Rewound
    );

    let checkpoint = Checkpoint {
        delta: delta.clone(),
        status: CheckpointStatus::Committed,
        receipt: Some(receipt.clone()),
        is_head: true,
        created_at_ms: 1_700_000_000_000,
        committed_at_ms: Some(receipt.committed_at_ms),
        rewind_target_checkpoint_id: None,
    };
    let checkpoint_json = serde_json::to_string(&checkpoint).unwrap();
    assert_eq!(
        checkpoint,
        serde_json::from_str::<Checkpoint>(&checkpoint_json).unwrap()
    );

    let rewind_request = RewindRequest {
        marker_checkpoint_id: CheckpointId::new("rewind-marker-1").unwrap(),
        pipeline_id: delta.pipeline_id.clone(),
        resource_id: delta.resource_id.clone(),
        scope: delta.scope.clone(),
        target_checkpoint_id: delta.checkpoint_id.clone(),
    };
    let request_json = serde_json::to_string(&rewind_request).unwrap();
    assert_eq!(
        rewind_request,
        serde_json::from_str::<RewindRequest>(&request_json).unwrap()
    );

    let rewind_report = RewindReport {
        marker: Checkpoint {
            delta,
            status: CheckpointStatus::Rewound,
            receipt: None,
            is_head: false,
            created_at_ms: 1_700_000_000_001,
            committed_at_ms: None,
            rewind_target_checkpoint_id: Some(checkpoint.delta.checkpoint_id.clone()),
        },
        head: checkpoint,
        packages_ahead: vec![PackageHash::new("package-sha256").unwrap()],
    };
    let report_json = serde_json::to_string(&rewind_report).unwrap();
    assert_eq!(
        rewind_report,
        serde_json::from_str::<RewindReport>(&report_json).unwrap()
    );
}

#[test]
fn error_taxonomy_contains_required_categories() {
    let kinds = [
        ErrorKind::Transient,
        ErrorKind::RateLimited,
        ErrorKind::Auth,
        ErrorKind::Contract,
        ErrorKind::Data,
        ErrorKind::Destination,
        ErrorKind::Internal,
    ];

    assert_eq!(kinds.len(), 7);
    assert_eq!(
        CdfError::rate_limited("slow down", Some(100)).kind,
        ErrorKind::RateLimited
    );
}

#[test]
fn cdf_error_display_includes_retry_context_when_present() {
    assert_eq!(
        CdfError::contract("schema drift").to_string(),
        "Contract: schema drift"
    );
    assert_eq!(
        CdfError::rate_limited("slow down", Some(250)).to_string(),
        "RateLimited: slow down (retry after 250 ms)"
    );
}

#[test]
fn source_position_version_returns_embedded_variant_version() {
    let mut composite_parts = BTreeMap::new();
    composite_parts.insert(
        "cursor".to_owned(),
        SourcePosition::Cursor(CursorPosition {
            version: 2,
            field: "updated_at".to_owned(),
            value: CursorValue::I64(10),
        }),
    );

    let positions = [
        (
            SourcePosition::Cursor(CursorPosition {
                version: 2,
                field: "updated_at".to_owned(),
                value: CursorValue::I64(10),
            }),
            2,
        ),
        (
            SourcePosition::Log(LogPosition {
                version: 3,
                log: "orders".to_owned(),
                offset: 42,
                sequence: Some("abc".to_owned()),
            }),
            3,
        ),
        (
            SourcePosition::FileManifest(FileManifest {
                version: 4,
                files: vec![FilePosition {
                    path: "orders.jsonl".to_owned(),
                    size_bytes: 1024,
                    etag: Some("etag-1".to_owned()),
                    sha256: Some("file-sha256".to_owned()),
                }],
            }),
            4,
        ),
        (
            SourcePosition::PageToken(PageToken {
                version: 5,
                token: "next-page".to_owned(),
            }),
            5,
        ),
        (
            SourcePosition::Composite(CompositePosition {
                version: 6,
                positions: composite_parts,
            }),
            6,
        ),
        (
            SourcePosition::ForeignState(ForeignState {
                version: 7,
                protocol: "singer".to_owned(),
                opaque_blob: b"state".to_vec(),
                blob_sha256: "state-sha256".to_owned(),
            }),
            7,
        ),
    ];

    for (position, expected_version) in positions {
        assert_eq!(position.version(), expected_version);
    }
}

#[test]
fn receipt_rejects_state_delta_when_identity_or_segments_do_not_match() {
    let (delta, receipt) = sample_state_delta_and_receipt();
    assert!(receipt.covers_state_delta(&delta));

    let mut wrong_package = receipt.clone();
    wrong_package.package_hash = PackageHash::new("other-package-sha256").unwrap();
    assert!(!wrong_package.covers_state_delta(&delta));

    let mut wrong_schema = receipt.clone();
    wrong_schema.schema_hash = SchemaHash::new("other-schema-sha256").unwrap();
    assert!(!wrong_schema.covers_state_delta(&delta));

    let mut missing_segment = receipt;
    missing_segment.segment_acks.clear();
    assert!(!missing_segment.covers_state_delta(&delta));
}
