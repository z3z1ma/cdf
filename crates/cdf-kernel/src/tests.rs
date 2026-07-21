use super::*;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_core::Stream;

use arrow_array::{ArrayRef, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

fn planned_task_set_reference() -> PlannedTaskSetReference {
    PlannedTaskSetReference {
        version: PLANNED_TASK_SET_REFERENCE_VERSION,
        task_type: "iceberg-scan-v1".to_owned(),
        task_count: 1_000_000,
        store_namespace: ContentStoreNamespace::new("planner-artifacts").unwrap(),
        object_key: ContentObjectKey::new(
            "task-sets/sha256/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.cdftasks",
        )
        .unwrap(),
        byte_count: 64,
        content_sha256:
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
        provider_generation: ContentProviderGeneration::new(
            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        .unwrap(),
    }
}

#[test]
fn compiled_source_identity_categories_validate_during_construction_and_deserialization() {
    let valid = format!("sha256:{}", "a".repeat(64));
    assert_eq!(SourceDiscoveryBinding::new(&valid).unwrap().as_str(), valid);
    assert_eq!(CompiledSourcePlanHash::new(&valid).unwrap().as_str(), valid);
    assert_eq!(PhysicalSourcePlanHash::new(&valid).unwrap().as_str(), valid);
    assert_eq!(SourceSemanticsHash::new(&valid).unwrap().as_str(), valid);
    assert_eq!(
        SchemaObservationBinding::new(&valid).unwrap().as_str(),
        valid
    );

    for malformed in [
        serde_json::json!("not-a-hash"),
        serde_json::json!(format!("sha256:{}", "A".repeat(64))),
        serde_json::json!(format!("sha256:{}", "a".repeat(63))),
    ] {
        assert!(serde_json::from_value::<SourceDiscoveryBinding>(malformed.clone()).is_err());
        assert!(serde_json::from_value::<CompiledSourcePlanHash>(malformed.clone()).is_err());
        assert!(serde_json::from_value::<PhysicalSourcePlanHash>(malformed.clone()).is_err());
        assert!(serde_json::from_value::<SourceSemanticsHash>(malformed.clone()).is_err());
        assert!(serde_json::from_value::<SchemaObservationBinding>(malformed).is_err());
    }
}

#[test]
fn external_task_authority_is_a_closed_alternative_to_inline_partitions() {
    let scan = ScanPlan::new(
        PlanId::new("external-task-plan").unwrap(),
        ScanRequest {
            resource_id: ResourceId::new("lake.events").unwrap(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        },
        PartitionAuthority::External(planned_task_set_reference()),
        Vec::new(),
        Vec::new(),
        None,
        None,
        DeliveryGuarantee::AtLeastOnceDuplicateRisk,
    );
    assert_eq!(scan.partition_count().unwrap(), 1_000_000);
    assert!(scan.inline_partitions().is_none());
    assert_eq!(
        scan.external_task_set(),
        Some(&planned_task_set_reference())
    );
}

#[test]
fn scan_plan_serialization_has_one_partition_authority_and_rejects_the_retired_shape() {
    let scan = ScanPlan::new(
        PlanId::new("external-task-plan").unwrap(),
        ScanRequest {
            resource_id: ResourceId::new("lake.events").unwrap(),
            projection: None,
            filters: Vec::new(),
            limit: None,
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        },
        PartitionAuthority::External(planned_task_set_reference()),
        Vec::new(),
        Vec::new(),
        None,
        None,
        DeliveryGuarantee::AtLeastOnceDuplicateRisk,
    );
    let encoded = serde_json::to_value(&scan).unwrap();
    assert!(encoded.get("partition_authority").is_some());
    assert!(encoded.get("partitions").is_none());
    assert!(encoded.get("planned_task_set").is_none());

    let mut retired = encoded;
    let object = retired.as_object_mut().unwrap();
    object.remove("partition_authority");
    object.insert("partitions".to_owned(), serde_json::json!([]));
    object.insert("planned_task_set".to_owned(), serde_json::Value::Null);
    assert!(serde_json::from_value::<ScanPlan>(retired).is_err());
}

#[test]
fn external_task_authority_rejects_tampered_identity_and_paths() {
    let mut reference = planned_task_set_reference();
    reference.content_sha256 = "sha256:NOT-CANONICAL".to_owned();
    assert!(reference.validate().is_err());
    let mut reference = planned_task_set_reference();
    reference.object_key = ContentObjectKey::new("../escape.cdftasks").unwrap();
    assert!(reference.validate().is_err());
}

#[test]
fn schema_observation_identity_is_partition_scoped_for_file_partitions() {
    let partition = PartitionPlan {
        partition_id: PartitionId::new("file-row-group-7").unwrap(),
        scope: ScopeKey::File {
            path: "s3://bucket/object.parquet".to_owned(),
        },
        planned_position: None,
        start_position: None,
        scan_intent: CompiledScanIntent::full_scan(),
        retry_safety: PartitionRetrySafety::Forbidden,
        metadata: BTreeMap::new(),
    };

    assert_eq!(
        partition_schema_observation_id(&partition),
        "file-row-group-7"
    );

    let mut explicit = partition;
    explicit.metadata.insert(
        PLAN_SCHEMA_OBSERVATION_ID_KEY.to_owned(),
        "planned-observation-7".to_owned(),
    );
    assert_eq!(
        partition_schema_observation_id(&explicit),
        "planned-observation-7"
    );
}

#[test]
fn planned_file_position_is_required_typed_authority() {
    let file = FilePosition {
        path: "s3://bucket/object.parquet".to_owned(),
        size_bytes: 42,
        source_generation: None,
        etag: Some("etag-1".to_owned()),
        object_version: None,
        sha256: None,
    };
    let partition = PartitionPlan {
        partition_id: PartitionId::new("file-object").unwrap(),
        scope: ScopeKey::File {
            path: file.path.clone(),
        },
        planned_position: Some(SourcePosition::FileManifest(FileManifest {
            version: 1,
            files: vec![file.clone()],
        })),
        start_position: None,
        scan_intent: CompiledScanIntent::full_scan(),
        retry_safety: PartitionRetrySafety::ImmutableContent,
        metadata: BTreeMap::new(),
    };

    assert_eq!(partition.planned_file().unwrap(), Some(&file));
    let mut missing = serde_json::to_value(&partition).unwrap();
    missing.as_object_mut().unwrap().remove("planned_position");
    assert!(serde_json::from_value::<PartitionPlan>(missing).is_err());

    let mut multiple = partition.clone();
    let Some(SourcePosition::FileManifest(manifest)) = &mut multiple.planned_position else {
        unreachable!();
    };
    manifest.files.push(file.clone());
    assert!(multiple.planned_file().is_err());

    let mut wrong_scope = partition;
    wrong_scope.scope = ScopeKey::File {
        path: "s3://bucket/other.parquet".to_owned(),
    };
    assert!(wrong_scope.planned_file().is_err());
}

#[test]
fn compiled_scan_intent_is_versioned_and_rejects_ambiguous_work() {
    let intent = CompiledScanIntent {
        version: COMPILED_SCAN_INTENT_VERSION,
        projection: Some(vec!["id".to_owned()]),
        predicates: vec![PushedPredicate {
            predicate: ScanPredicate::new(PredicateId::new("p1").unwrap(), "id >= 7").unwrap(),
            fidelity: PushdownFidelity::Exact,
        }],
        limit: Some(10),
        order_by: vec![OrderBy {
            field: "id".to_owned(),
            direction: SortDirection::Asc,
        }],
    };
    intent.validate().unwrap();
    let encoded = serde_json::to_value(&intent).unwrap();
    assert_eq!(
        serde_json::from_value::<CompiledScanIntent>(encoded).unwrap(),
        intent
    );

    let mut wrong_version = serde_json::to_value(&intent).unwrap();
    wrong_version["version"] = serde_json::json!(99);
    assert!(serde_json::from_value::<CompiledScanIntent>(wrong_version).is_err());

    let mut duplicate_projection = intent.clone();
    duplicate_projection.projection = Some(vec!["id".to_owned(), "id".to_owned()]);
    assert!(duplicate_projection.validate().is_err());

    let mut unsupported = intent;
    unsupported.predicates[0].fidelity = PushdownFidelity::Unsupported;
    assert!(unsupported.validate().is_err());
}

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
    let output_watermark = WatermarkClaim {
        version: WATERMARK_CLAIM_VERSION,
        policy_version: STREAM_EPOCH_POLICY_VERSION,
        event_time_field: "updated_at".into(),
        domain: EventTimeDomain::Timestamp {
            unit: CanonicalArrowTimeUnit::Microsecond,
            timezone: Some("America/Phoenix".into()),
        },
        value: WatermarkValue::Timestamp(1_700_000_000_000_000),
        partition_id: PartitionId::new("p0").unwrap(),
        source_position: output_position.clone(),
        authority: WatermarkAuthority::Source,
        observation_context: WatermarkObservationContext::EpochBarrier,
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
        output_watermark: Some(output_watermark),
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
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

impl FakeSessionDestination {
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

    fn write_segments(&mut self, segments: CommitSegmentIterator) -> Result<Vec<SegmentAck>> {
        if !self.migrations_applied {
            return Err(CdfError::destination(
                "migrations must be applied before writing",
            ));
        }
        let mut acknowledgements = Vec::new();
        for segment in segments {
            let segment = segment?;
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
            acknowledgements.push(ack);
        }
        Ok(acknowledgements)
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
        .write_segments(Box::new(std::iter::once(Ok(sample_commit_segment(
            segment.clone(),
        )))))
        .unwrap()
        .remove(0);
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

#[test]
fn commit_segment_yields_ordered_batches_without_losing_authority() {
    let (delta, _) = sample_state_delta_and_receipt();
    let state = delta.segments[0].clone();
    let segment = sample_commit_segment(state.clone());
    let batches = segment.into_batches().unwrap().collect::<Vec<_>>();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].state, state);
    assert_eq!(batches[0].package_byte_count, 96);
    assert_eq!(batches[0].batch_ordinal, 0);
    assert_eq!(batches[0].batch_count, 1);
    assert_eq!(batches[0].batch.num_rows(), 3);
}

fn sample_commit_segment(state: StateSegment) -> CommitSegment {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let values = (0..state.row_count as i64).collect::<Vec<_>>();
    let column: ArrayRef = Arc::new(Int64Array::from(values));
    let batch = RecordBatch::try_new(schema, vec![column]).unwrap();
    CommitSegment::new(state, 96, vec![batch])
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
fn destination_sheet_artifact_requires_current_typed_protocol_capabilities() {
    let sheet = sample_destination_sheet();
    let bare_sheet_json = serde_json::to_string(&sheet).unwrap();
    assert!(serde_json::from_str::<DestinationSheetArtifact>(&bare_sheet_json).is_err());
    let object_capabilities = DestinationProtocolCapabilities::default()
        .with_object_key_rules(ObjectKeyRules::component_v1());
    let encoded = serde_json::to_value(
        DestinationSheetArtifact::new(sheet.clone(), object_capabilities.clone()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        encoded["protocol_capabilities"]["object_key_rules"],
        serde_json::json!({
            "version": 1,
            "policy": "object-key-component-v1"
        })
    );
    assert_eq!(
        serde_json::from_value::<DestinationSheetArtifact>(encoded).unwrap(),
        DestinationSheetArtifact::new(sheet.clone(), object_capabilities).unwrap()
    );

    let invalid =
        DestinationProtocolCapabilities::default().with_object_key_rules(ObjectKeyRules {
            version: OBJECT_KEY_RULES_VERSION + 1,
            policy: ObjectKeyPolicy::ComponentV1,
        });
    assert!(
        DestinationSheetArtifact::new(sheet, invalid)
            .unwrap_err()
            .message
            .contains("unsupported object-key rules version")
    );

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

fn correction_operation_fixture(
    path: &str,
    output_name: &str,
    display_json: &str,
    exact_authority: &[u8],
) -> DestinationCorrectionOperation {
    DestinationCorrectionOperation {
        correction: DestinationCorrectionPlan {
            request: DestinationCorrectionRequest {
                promotion_id: PromotionId::new("promotion-digest").unwrap(),
                original_row: RowProvenanceAddress::new(
                    PackageHash::new("sha256:original-package").unwrap(),
                    SegmentId::new("seg-000001").unwrap(),
                    0,
                ),
                old_schema_hash: SchemaHash::new("sha256:old-schema").unwrap(),
                new_schema_hash: SchemaHash::new("sha256:new-schema").unwrap(),
                promoted_path: path.to_owned(),
                promoted_value_json: display_json.to_owned(),
                residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
                selected_strategy: CorrectionStrategy::InPlaceUpdate,
            },
            transaction_guarantee: TransactionSupport::AtomicPackage,
            idempotency_guarantee: IdempotencySupport::PackageToken,
        },
        output_field: CanonicalArrowField {
            name: output_name.to_owned(),
            data_type: CanonicalArrowType::Int {
                signed: true,
                bits: 64,
            },
            nullable: true,
            metadata: BTreeMap::new(),
        },
        promoted_value_residual_json_v1: exact_authority.to_vec(),
    }
}

#[test]
fn correction_digest_excludes_display_json_and_binds_exact_authority() {
    let base = correction_operation_fixture("/age", "age", "42", br#"{"exact":42}"#);
    let display_changed =
        correction_operation_fixture("/age", "age", r#"{"pretty":42}"#, br#"{"exact":42}"#);
    assert_eq!(
        correction_operations_digest(std::slice::from_ref(&base)).unwrap(),
        correction_operations_digest(&[display_changed]).unwrap()
    );

    let exact_changed = correction_operation_fixture("/age", "age", "42", br#"{"exact":43}"#);
    assert_ne!(
        correction_operations_digest(std::slice::from_ref(&base)).unwrap(),
        correction_operations_digest(&[exact_changed]).unwrap()
    );

    let type_changed = DestinationCorrectionOperation {
        output_field: CanonicalArrowField {
            data_type: CanonicalArrowType::Int {
                signed: true,
                bits: 32,
            },
            ..base.output_field.clone()
        },
        ..base.clone()
    };
    assert_ne!(
        correction_operations_digest(&[base]).unwrap(),
        correction_operations_digest(&[type_changed]).unwrap()
    );
}

#[test]
fn correction_request_rejects_two_paths_for_one_output_field() {
    let first = correction_operation_fixture("/age", "promoted", "42", b"first");
    let second = correction_operation_fixture("/years", "promoted", "42", b"second");
    let error = DestinationCorrectionCommitRequest::new(
        PackageHash::new("sha256:correction-package").unwrap(),
        IdempotencyToken::new("sha256:correction-package").unwrap(),
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        vec![StateSegment {
            segment_id: SegmentId::new("seg-correction").unwrap(),
            scope: ScopeKey::Resource,
            output_position: SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "correction".to_owned(),
                value: CursorValue::U64(2),
            }),
            row_count: 2,
            byte_count: 2,
        }],
        vec![first, second],
    )
    .unwrap_err();
    assert!(error.to_string().contains("conflicting promoted paths"));
}

#[test]
fn correction_sidecar_receipt_uses_insert_counts_and_closed_manifest_evidence() {
    let mut operation = correction_operation_fixture("/age", "age", "42", b"exact");
    operation.correction.request.selected_strategy = CorrectionStrategy::CorrectionSidecar;
    operation.correction.transaction_guarantee = TransactionSupport::AtomicTarget;
    let request = DestinationCorrectionCommitRequest::new(
        PackageHash::new("sha256:correction-package").unwrap(),
        IdempotencyToken::new("sha256:correction-package").unwrap(),
        TargetName::new("orders").unwrap(),
        WriteDisposition::Append,
        vec![StateSegment {
            segment_id: SegmentId::new("seg-correction").unwrap(),
            scope: ScopeKey::Resource,
            output_position: SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "correction".to_owned(),
                value: CursorValue::U64(1),
            }),
            row_count: 1,
            byte_count: 1,
        }],
        vec![operation],
    )
    .unwrap();
    let plan = DestinationCorrectionCommitPlan {
        kernel: CommitPlan {
            plan_id: PlanId::new("sidecar-plan").unwrap(),
            target: request.target.clone(),
            disposition: request.resource_disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        },
        correction_package_hash: request.correction_package_hash.clone(),
        promotion_id: request.promotion_id().clone(),
        old_schema_hash: request.old_schema_hash().clone(),
        new_schema_hash: request.new_schema_hash().clone(),
        strategy: CorrectionStrategy::CorrectionSidecar,
        operations_digest: request.operations_digest.clone(),
        correction_count: 1,
    };
    let sidecar = DestinationCorrectionSidecarReceiptEvidence {
        version: DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_VERSION,
        manifest_key: "targets/orders/corrections/manifest.json".to_owned(),
        manifest_sha256: format!("sha256:{}", "a".repeat(64)),
        operation_count: 1,
        atomic_manifest_publication: true,
        base_target_unchanged: true,
        objects: vec![DestinationCorrectionSidecarObjectEvidence {
            key: "targets/orders/corrections/object.json".to_owned(),
            sha256: format!("sha256:{}", "b".repeat(64)),
            byte_count: 42,
            operation_count: 1,
        }],
    };
    let receipt = Receipt {
        receipt_id: ReceiptId::new("sidecar-receipt").unwrap(),
        destination: DestinationId::new("parquet").unwrap(),
        target: request.target.clone(),
        package_hash: request.correction_package_hash.clone(),
        segment_acks: request.segment_acks(),
        disposition: request.resource_disposition.clone(),
        idempotency_token: request.idempotency_token.clone(),
        transaction: Some(TransactionMetadata {
            system: "object_store_correction_sidecar".to_owned(),
            values: BTreeMap::from([
                (
                    DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY.to_owned(),
                    DestinationCorrectionReceiptEvidence::for_request(&request)
                        .to_json()
                        .unwrap(),
                ),
                (
                    DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_KEY.to_owned(),
                    sidecar.to_json().unwrap(),
                ),
            ]),
        }),
        counts: CommitCounts {
            rows_written: 1,
            rows_inserted: Some(1),
            rows_updated: Some(0),
            rows_deleted: Some(0),
        },
        schema_hash: request.new_schema_hash().clone(),
        migrations: Vec::new(),
        committed_at_ms: 1,
        verify: VerifyClause {
            kind: "sidecar".to_owned(),
            statement: "verify sidecar".to_owned(),
            parameters: BTreeMap::new(),
        },
    };

    plan.validate_receipt(&request, &receipt).unwrap();
    let mut false_update = receipt.clone();
    false_update.counts.rows_inserted = Some(0);
    false_update.counts.rows_updated = Some(1);
    assert!(
        plan.validate_receipt(&request, &false_update)
            .unwrap_err()
            .to_string()
            .contains("sidecar operations")
    );
    let mut false_base = sidecar;
    false_base.base_target_unchanged = false;
    assert!(false_base.to_json().is_err());
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
    let header = BatchHeader::new(
        BatchId::new("batch-legacy").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("p0").unwrap(),
        SchemaHash::new("schema-sha256").unwrap(),
        1,
        8,
    );

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
fn materialized_batch_header_rejects_typed_schema_hash_split() {
    let physical = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let mut header = BatchHeader::new(
        BatchId::new("batch-materialized").unwrap(),
        ResourceId::new("orders").unwrap(),
        PartitionId::new("p0").unwrap(),
        SchemaHash::new("unbound-before-materialization").unwrap(),
        1,
        8,
    );
    header.mark_materialized_output(&physical).unwrap();
    assert_eq!(header.materialized_physical_schema().unwrap(), physical);

    header.physical_observation_schema = Some(
        CanonicalArrowSchema::from_arrow(&Schema::new(vec![Field::new(
            "id",
            DataType::Int32,
            false,
        )]))
        .unwrap(),
    );
    let error = header.materialized_physical_schema().unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not match batch observation hash"),
        "{error}"
    );
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
        deduplication: None,
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
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
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
fn closed_cursor_aggregation_validates_resource_schema_without_reapplying_lag() {
    let descriptor = ResourceDescriptor {
        resource_id: ResourceId::new("orders").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: SchemaHash::new("schema-sha256").unwrap(),
            source: "fixture".to_owned(),
        },
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor: Some(CursorSpec {
            field: "updated_at".to_owned(),
            ordering: CursorOrderingClaim::Exact,
            lag_tolerance_ms: 5,
        }),
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    };
    let schema = Schema::new(vec![Field::new("updated_at", DataType::Int64, false)]);
    let prior = SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(90),
    });
    let observed = SourcePosition::Cursor(CursorPosition {
        version: 1,
        field: "updated_at".to_owned(),
        value: CursorValue::I64(100),
    });

    let output = aggregate_resource_closed_output_position(
        &descriptor,
        &schema,
        Some(&prior),
        std::slice::from_ref(&observed),
    )
    .unwrap();
    assert_eq!(output, observed);

    let missing_cursor_schema = Schema::new(vec![Field::new("other", DataType::Int64, false)]);
    assert!(
        aggregate_resource_closed_output_position(
            &descriptor,
            &missing_cursor_schema,
            Some(&prior),
            &[observed],
        )
        .unwrap_err()
        .message
        .contains("missing from the declared schema")
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
    let hints = SchemaSource::Hints {
        source: "declarative:orders".to_owned(),
        hints_hash: Some(SchemaHash::new("sha256:hints").unwrap()),
        snapshot: None,
    };
    let pinned_hints = hints
        .with_pinned_snapshot(snapshot.clone())
        .expect("hints support pinning");
    assert!(matches!(
        &pinned_hints,
        SchemaSource::Hints {
            source,
            hints_hash: Some(hash),
            snapshot: Some(reference),
        } if source == "declarative:orders"
            && hash.as_str() == "sha256:hints"
            && reference == &snapshot
    ));
    assert_eq!(pinned_hints.without_pinned_snapshot(), Some(hints));
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
                    source_generation: None,
                    etag: Some("etag-1".to_owned()),
                    object_version: Some("version-1".to_owned()),
                    sha256: Some("file-sha256".to_owned()),
                }],
            }),
            4,
        ),
        (
            SourcePosition::TableSnapshot(Box::new(TableSnapshotPosition {
                version: 8,
                protocol: "iceberg".to_owned(),
                catalog: "rest:https://catalog.example.test".to_owned(),
                namespace: vec!["analytics".to_owned()],
                table: "orders".to_owned(),
                selector: TableSnapshotSelector::Current,
                snapshot_id: 42,
                sequence_number: 7,
                parent_snapshot_id: Some(41),
                metadata_location: "s3://warehouse/analytics/orders/metadata/v7.json".to_owned(),
                metadata_generation: "etag:v7".to_owned(),
            })),
            8,
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
fn table_snapshot_position_is_canonical_exact_and_batch_slice_invariant() {
    assert!(
        std::mem::size_of::<SourcePosition>() <= 96,
        "large source variants must remain indirect so ordinary positions stay compact"
    );
    let position = SourcePosition::TableSnapshot(Box::new(TableSnapshotPosition {
        version: SOURCE_POSITION_VERSION,
        protocol: "iceberg".to_owned(),
        catalog: "glue:us-east-1:123456789012".to_owned(),
        namespace: vec!["analytics".to_owned(), "curated".to_owned()],
        table: "orders".to_owned(),
        selector: TableSnapshotSelector::Branch {
            name: "main".to_owned(),
        },
        snapshot_id: 42,
        sequence_number: 7,
        parent_snapshot_id: Some(41),
        metadata_location: "s3://warehouse/analytics/orders/metadata/v7.metadata.json".to_owned(),
        metadata_generation: "version-id:metadata-v7".to_owned(),
    }));

    position.validate().unwrap();
    assert_eq!(position.kind(), SourcePositionKind::TableSnapshot);
    assert!(position.is_batch_slice_invariant());
    assert_eq!(
        serde_json::from_value::<SourcePosition>(serde_json::to_value(&position).unwrap()).unwrap(),
        position
    );

    let SourcePosition::TableSnapshot(valid) = &position else {
        unreachable!();
    };
    let mut invalid = valid.clone();
    invalid.protocol = "Iceberg".to_owned();
    assert!(invalid.validate().is_err());
    let mut invalid = valid.clone();
    invalid.namespace.clear();
    assert!(invalid.validate().is_err());
    let mut invalid = valid.clone();
    invalid.snapshot_id = 0;
    assert!(invalid.validate().is_err());
    let mut invalid = valid.clone();
    invalid.sequence_number = -1;
    assert!(invalid.validate().is_err());
    let mut invalid = valid.clone();
    invalid.parent_snapshot_id = Some(valid.snapshot_id);
    assert!(invalid.validate().is_err());
    let mut invalid = valid.clone();
    invalid.selector = TableSnapshotSelector::Snapshot { snapshot_id: 99 };
    assert!(invalid.validate().is_err());
    let mut invalid = valid.clone();
    invalid.selector = TableSnapshotSelector::Timestamp { timestamp_ms: -1 };
    assert!(invalid.validate().is_err());
}

#[test]
fn segment_and_processed_observation_paths_share_file_manifest_aggregation_authority() {
    let descriptor = ResourceDescriptor {
        resource_id: ResourceId::new("files.events").unwrap(),
        schema_source: SchemaSource::Declared {
            schema_hash: SchemaHash::new("schema-v1").unwrap(),
            source: "fixture".to_owned(),
        },
        primary_key: Vec::new(),
        merge_key: Vec::new(),
        cursor: None,
        write_disposition: WriteDisposition::Append,
        deduplication: None,
        contract: None,
        state_scope: ScopeKey::Resource,
        freshness: None,
        trust_level: TrustLevel::Governed,
    };
    let input = SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "old.parquet".to_owned(),
            size_bytes: 10,
            source_generation: None,
            etag: Some("old".to_owned()),
            object_version: None,
            sha256: None,
        }],
    });
    let current = SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "new.parquet".to_owned(),
            size_bytes: 20,
            source_generation: None,
            etag: Some("new".to_owned()),
            object_version: None,
            sha256: None,
        }],
    });
    let observation = ProcessedObservationPosition::new(
        "new.parquet",
        ProcessedObservationOutcome::Quarantined,
        current.clone(),
    )
    .unwrap();

    let segment_path = aggregate_resource_output_position(
        &descriptor,
        &Schema::empty(),
        Some(&input),
        std::slice::from_ref(&current),
    )
    .unwrap();
    let processed_path = aggregate_processed_observation_positions(
        Some(&input),
        &[observation],
        &WriteDisposition::Append,
    )
    .unwrap();

    assert_eq!(segment_path, processed_path);
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

#[test]
fn two_axis_discovery_coverage_evidence_is_total_and_round_trips() {
    let evidence = DiscoveryCoverageEvidence::new(DiscoveryCoverageEvidenceInput {
        file_coverage: "sampled_files".to_owned(),
        within_file_coverage: "bounded_content".to_owned(),
        selector: Some("stratified-hash-v1".to_owned()),
        sample_files: Some(2),
        matched_files: 5,
        selected_files: 2,
        observed_bytes: 4096,
        observed_records: 1000,
    })
    .unwrap();
    assert_eq!(evidence.unobserved_files, 3);
    let encoded = serde_json::to_vec(&evidence).unwrap();
    let decoded: DiscoveryCoverageEvidence = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, evidence);
    decoded.validate().unwrap();

    let mut invalid = evidence;
    invalid.unobserved_files = 2;
    assert!(invalid.validate().is_err());
}

#[test]
fn run_phase_metric_preserves_the_current_scalar_event_value_shape() {
    let scalar_json = r#"{"attributes":{"rows":{"type":"u64","value":7}}}"#;
    let scalar: RunEventDetails = serde_json::from_str(scalar_json).unwrap();
    assert_eq!(serde_json::to_string(&scalar).unwrap(), scalar_json);

    let details = RunEventDetails::new([(
        "metric",
        RunEventValue::PhaseMetric(RunPhaseMetric {
            phase: RunPhase::Decode,
            context: None,
            status: RunPhaseStatus::Completed,
            duration_ns: 42,
            input_bytes: 100,
            output_bytes: 80,
            operations: 2,
        }),
    )]);
    details.validate().unwrap();
    let encoded = serde_json::to_vec(&details).unwrap();
    let decoded: RunEventDetails = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(decoded, details);

    let invalid = RunEventDetails::new([(
        "metric",
        RunEventValue::PhaseMetric(RunPhaseMetric {
            phase: RunPhase::Decode,
            context: Some(RunPhaseContext::SourceRead {
                mode: SourceReadMode::ExactRanges,
            }),
            status: RunPhaseStatus::Completed,
            duration_ns: 1,
            input_bytes: 1,
            output_bytes: 1,
            operations: 1,
        }),
    )]);
    assert!(invalid.validate().is_err());
}

#[test]
fn partition_completion_evidence_is_eof_bound_and_single_use() {
    struct EmptyBatchStream;
    impl Stream for EmptyBatchStream {
        type Item = Result<Batch>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(None)
        }
    }
    let mut context = Context::from_waker(std::task::Waker::noop());
    let stream: BatchStream = Box::pin(EmptyBatchStream);
    let termination_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_termination = Arc::clone(&termination_polled);
    let payload = PartitionStreamPayload::new(
        stream,
        Box::pin(async { Ok(PartitionCompletion::default()) }),
    );
    let mut attempt = PartitionOpenAttempt::with_termination(
        Box::pin(async move { Ok(payload) }),
        InvocationTermination::new(
            || {},
            Box::pin(async move {
                observed_termination.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }),
        ),
    );
    let mut opened = match Pin::new(&mut attempt).poll(&mut context) {
        Poll::Ready(Ok(opened)) => opened,
        Poll::Ready(Err(error)) => panic!("partition opening failed: {error}"),
        Poll::Pending => panic!("materialized partition opening unexpectedly blocked"),
    };

    let before_eof = {
        let mut completion = Box::pin(opened.completion());
        match completion.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("pre-EOF completion check unexpectedly blocked"),
        }
    };
    assert!(before_eof.is_err());
    assert!(matches!(
        Pin::new(&mut opened).poll_next(&mut context),
        Poll::Ready(None)
    ));

    let after_eof = {
        let mut completion = Box::pin(opened.completion());
        match completion.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("terminal completion evidence unexpectedly blocked"),
        }
    };
    assert_eq!(after_eof.unwrap(), PartitionCompletion::default());
    assert!(termination_polled.load(std::sync::atomic::Ordering::SeqCst));
    let second = {
        let mut completion = Box::pin(opened.completion());
        match completion.as_mut().poll(&mut context) {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("duplicate completion check unexpectedly blocked"),
        }
    };
    assert!(second.is_err());
}

#[test]
fn failed_partition_attempt_requires_its_bound_termination_barrier() {
    struct ErrorBatchStream;
    impl Stream for ErrorBatchStream {
        type Item = Result<Batch>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(Some(Err(CdfError::transient("fixture producer failed"))))
        }
    }

    let barrier_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_barrier = Arc::clone(&barrier_polled);
    let stream: BatchStream = Box::pin(ErrorBatchStream);
    let payload = PartitionStreamPayload::new(
        stream,
        Box::pin(async { Ok(PartitionCompletion::default()) }),
    );
    let mut attempt = PartitionOpenAttempt::with_termination(
        Box::pin(async move { Ok(payload) }),
        InvocationTermination::new(
            || {},
            Box::pin(async move {
                observed_barrier.store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }),
        ),
    );
    let mut context = Context::from_waker(std::task::Waker::noop());
    let mut opened = match Pin::new(&mut attempt).poll(&mut context) {
        Poll::Ready(Ok(opened)) => opened,
        Poll::Ready(Err(error)) => panic!("partition opening failed: {error}"),
        Poll::Pending => panic!("materialized partition opening unexpectedly blocked"),
    };
    assert!(matches!(
        Pin::new(&mut opened).poll_next(&mut context),
        Poll::Ready(Some(Err(_)))
    ));
    let mut joined = Box::pin(opened.join_failed_attempt());
    assert!(matches!(
        joined.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert!(barrier_polled.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn failed_partition_attempt_does_not_treat_repeated_producer_error_as_cleanup_failure() {
    struct ErrorBatchStream(CdfError);
    impl Stream for ErrorBatchStream {
        type Item = Result<Batch>;

        fn poll_next(self: Pin<&mut Self>, _context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(Some(Err(self.0.clone())))
        }
    }

    let producer_error = CdfError::transient("fixture producer failed");
    let stream: BatchStream = Box::pin(ErrorBatchStream(producer_error.clone()));
    let payload = PartitionStreamPayload::batches(stream);
    let mut attempt = PartitionOpenAttempt::with_termination(
        Box::pin(async move { Ok(payload) }),
        InvocationTermination::new(|| {}, Box::pin(async move { Err(producer_error) })),
    );
    let mut context = Context::from_waker(std::task::Waker::noop());
    let mut opened = match Pin::new(&mut attempt).poll(&mut context) {
        Poll::Ready(Ok(opened)) => opened,
        Poll::Ready(Err(error)) => panic!("partition opening failed: {error}"),
        Poll::Pending => panic!("materialized partition opening unexpectedly blocked"),
    };
    assert!(matches!(
        Pin::new(&mut opened).poll_next(&mut context),
        Poll::Ready(Some(Err(_)))
    ));
    let mut joined = Box::pin(opened.join_failed_attempt());
    assert!(matches!(
        joined.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    drop(joined);
    let mut repeated = Box::pin(opened.terminate_and_join());
    assert!(matches!(
        repeated.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
}

#[test]
fn probed_batch_can_return_to_its_invocation_bound_stream() {
    fn batch(id: &str, value: i64) -> Batch {
        let record_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
            vec![Arc::new(Int64Array::from(vec![value]))],
        )
        .unwrap();
        Batch::from_record_batch(
            BatchId::new(id).unwrap(),
            ResourceId::new("test.events").unwrap(),
            PartitionId::new("p0").unwrap(),
            SchemaHash::new("sha256:test").unwrap(),
            record_batch,
        )
        .unwrap()
    }

    let stream: BatchStream = Box::pin(futures_util::stream::iter([
        Ok(batch("first", 1)),
        Ok(batch("second", 2)),
    ]));
    let mut attempt = PartitionOpenAttempt::materialized(Box::pin(async move {
        Ok(PartitionStreamPayload::batches(stream))
    }));
    let mut context = Context::from_waker(std::task::Waker::noop());
    let mut opened = match Pin::new(&mut attempt).poll(&mut context) {
        Poll::Ready(Ok(opened)) => opened,
        Poll::Ready(Err(error)) => panic!("partition opening failed: {error}"),
        Poll::Pending => panic!("materialized partition opening unexpectedly blocked"),
    };
    let first = match Pin::new(&mut opened).poll_next(&mut context) {
        Poll::Ready(Some(Ok(batch))) => batch,
        result => panic!("first batch was not ready: {result:?}"),
    };
    opened.prepend_batch(first).unwrap();
    let replayed = match Pin::new(&mut opened).poll_next(&mut context) {
        Poll::Ready(Some(Ok(batch))) => batch,
        result => panic!("replayed batch was not ready: {result:?}"),
    };
    assert_eq!(replayed.header.batch_id.as_str(), "first");
    let second = match Pin::new(&mut opened).poll_next(&mut context) {
        Poll::Ready(Some(Ok(batch))) => batch,
        result => panic!("second batch was not ready: {result:?}"),
    };
    assert_eq!(second.header.batch_id.as_str(), "second");
    assert!(matches!(
        Pin::new(&mut opened).poll_next(&mut context),
        Poll::Ready(None)
    ));
    let mut completion = Box::pin(opened.completion());
    assert!(matches!(
        completion.as_mut().poll(&mut context),
        Poll::Ready(Ok(_))
    ));
}

#[test]
fn cancelled_partition_open_retains_awaitable_termination_authority() {
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_cancel = Arc::clone(&cancelled);
    let barrier_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_barrier = Arc::clone(&barrier_polled);
    let termination = InvocationTermination::new(
        move || observed_cancel.store(true, std::sync::atomic::Ordering::SeqCst),
        Box::pin(async move {
            observed_barrier.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
    );
    let opening = Box::pin(std::future::pending::<Result<PartitionStreamPayload>>());
    let mut attempt = PartitionOpenAttempt::with_termination(opening, termination);
    let mut context = Context::from_waker(std::task::Waker::noop());

    assert!(matches!(
        Pin::new(&mut attempt).poll(&mut context),
        Poll::Pending
    ));
    let mut terminated = Box::pin(attempt.terminate_and_join());
    assert!(matches!(
        terminated.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
    assert!(barrier_polled.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn cancelled_partition_attestation_retains_awaitable_termination_authority() {
    let cancelled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_cancel = Arc::clone(&cancelled);
    let barrier_polled = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed_barrier = Arc::clone(&barrier_polled);
    let termination = InvocationTermination::new(
        move || observed_cancel.store(true, std::sync::atomic::Ordering::SeqCst),
        Box::pin(async move {
            observed_barrier.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }),
    );
    let attesting = Box::pin(std::future::pending::<Result<Option<PartitionAttestation>>>());
    let mut attempt = PartitionAttestationAttempt::with_termination(attesting, termination);
    let mut context = Context::from_waker(std::task::Waker::noop());

    assert!(matches!(
        Pin::new(&mut attempt).poll(&mut context),
        Poll::Pending
    ));
    let mut terminated = Box::pin(attempt.terminate_and_join());
    assert!(matches!(
        terminated.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    assert!(cancelled.load(std::sync::atomic::Ordering::SeqCst));
    assert!(barrier_polled.load(std::sync::atomic::Ordering::SeqCst));
}

#[test]
fn partition_open_error_is_visible_before_its_explicit_termination_barrier() {
    let opening =
        Box::pin(async { Err::<PartitionStreamPayload, _>(CdfError::transient("opening failed")) });
    let termination = InvocationTermination::new(
        || {},
        Box::pin(async { Err(CdfError::internal("join failed")) }),
    );
    let mut attempt = PartitionOpenAttempt::with_termination(opening, termination);
    let mut context = Context::from_waker(std::task::Waker::noop());

    let result = match Pin::new(&mut attempt).poll(&mut context) {
        Poll::Ready(result) => result,
        Poll::Pending => panic!("immediate opening and termination unexpectedly blocked"),
    };
    let error = match result {
        Ok(_) => panic!("opening unexpectedly succeeded"),
        Err(error) => error,
    };
    assert_eq!(error.kind, ErrorKind::Transient);
    assert!(error.message.contains("opening failed"));
    assert!(!error.message.contains("join failed"));
    let cleanup = {
        let mut cleanup = Box::pin(attempt.terminate_and_join());
        match cleanup.as_mut().poll(&mut context) {
            Poll::Ready(result) => result.unwrap_err(),
            Poll::Pending => panic!("immediate termination unexpectedly blocked"),
        }
    };
    assert!(cleanup.message.contains("join failed"));
}

#[test]
fn terminal_file_attestation_may_only_add_a_content_hash() {
    let earlier_position = SourcePosition::FileManifest(FileManifest {
        version: 1,
        files: vec![FilePosition {
            path: "https://example.invalid/data.parquet".to_owned(),
            size_bytes: 42,
            source_generation: Some("generation-1".to_owned()),
            etag: None,
            object_version: None,
            sha256: None,
        }],
    });
    let earlier = PartitionAttestation::new(earlier_position.clone(), None);
    let mut completed_position = earlier_position.clone();
    let SourcePosition::FileManifest(manifest) = &mut completed_position else {
        unreachable!("fixture is a file manifest")
    };
    manifest.files[0].sha256 = Some(format!("sha256:{}", "a".repeat(64)));
    let completed = PartitionAttestation::new(completed_position.clone(), None);
    assert!(completed.is_monotonic_refinement_of(&earlier));

    let mut changed_position = completed_position;
    let SourcePosition::FileManifest(manifest) = &mut changed_position else {
        unreachable!("fixture is a file manifest")
    };
    manifest.files[0].source_generation = Some("generation-2".to_owned());
    let changed = PartitionAttestation::new(changed_position, None);
    assert!(!changed.is_monotonic_refinement_of(&earlier));
    assert!(!earlier.is_monotonic_refinement_of(&completed));
}
