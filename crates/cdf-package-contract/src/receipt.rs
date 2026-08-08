use std::collections::BTreeSet;

use cdf_kernel::{
    CdfError, CommitCounts, CommitPlan, DestinationCommitRequest, DestinationCorrectionCommitPlan,
    DestinationCorrectionCommitRequest, DestinationId, Receipt, ReceiptId, Result, SchemaHash,
    SegmentAck, TransactionMetadata, VerifyClause,
};

/// Destination-physical evidence supplied to the common receipt finalizer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReceiptEvidence {
    pub transaction: Option<TransactionMetadata>,
    pub counts: CommitCounts,
    pub committed_at_ms: i64,
    pub verify: VerifyClause,
}

/// A typed receipt awaiting common completeness and consistency validation.
#[derive(Clone, Debug)]
pub struct ReceiptDraft {
    receipt: Receipt,
    expected_segments: Vec<ExpectedReceiptSegment>,
}

#[derive(Clone, Debug)]
struct ExpectedReceiptSegment {
    kind: cdf_kernel::PackageSegmentKind,
    segment_id: cdf_kernel::SegmentId,
    row_count: u64,
    byte_count: u64,
}

impl ReceiptDraft {
    /// Maps an ordinary destination commit request and its physical evidence into one receipt.
    pub fn ordinary(
        receipt_id: ReceiptId,
        destination: DestinationId,
        request: &DestinationCommitRequest,
        plan: &CommitPlan,
        segment_acks: Vec<SegmentAck>,
        schema_hash: SchemaHash,
        evidence: ReceiptEvidence,
    ) -> Result<Self> {
        validate_ordinary_plan(request, plan)?;
        let expected_segments = request
            .segments
            .iter()
            .map(|segment| ExpectedReceiptSegment {
                kind: segment.kind,
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect();
        Ok(Self {
            receipt: Receipt {
                receipt_id,
                destination,
                target: request.target.clone(),
                package_hash: request.package_hash.clone(),
                content: request.content.clone(),
                segment_acks,
                disposition: request.disposition.clone(),
                idempotency_token: request.idempotency_token.clone(),
                transaction: evidence.transaction,
                counts: evidence.counts,
                schema_hash,
                migrations: plan.migrations.clone(),
                committed_at_ms: evidence.committed_at_ms,
                verify: evidence.verify,
            },
            expected_segments,
        })
    }

    /// Maps a correction request and its physical evidence into one receipt.
    pub fn correction(
        receipt_id: ReceiptId,
        destination: DestinationId,
        request: &DestinationCorrectionCommitRequest,
        plan: &DestinationCorrectionCommitPlan,
        evidence: ReceiptEvidence,
    ) -> Result<Self> {
        validate_correction_plan(request, plan)?;
        let segment_acks = request.segment_acks();
        let expected_segments = segment_acks
            .iter()
            .map(|segment| ExpectedReceiptSegment {
                kind: segment.kind,
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect();
        Ok(Self {
            receipt: Receipt {
                receipt_id,
                destination,
                target: request.target.clone(),
                package_hash: request.correction_package_hash.clone(),
                content: cdf_kernel::PackageContentAuthority::rows(
                    request.new_schema_hash().clone(),
                ),
                segment_acks,
                disposition: request.resource_disposition.clone(),
                idempotency_token: request.idempotency_token.clone(),
                transaction: evidence.transaction,
                counts: evidence.counts,
                schema_hash: request.new_schema_hash().clone(),
                migrations: plan.kernel.migrations.clone(),
                committed_at_ms: evidence.committed_at_ms,
                verify: evidence.verify,
            },
            expected_segments,
        })
    }

    /// Validates common receipt invariants and returns the canonical kernel receipt.
    pub fn finalize(self) -> Result<Receipt> {
        validate_receipt(&self.receipt, &self.expected_segments)?;
        Ok(self.receipt)
    }
}

fn validate_ordinary_plan(request: &DestinationCommitRequest, plan: &CommitPlan) -> Result<()> {
    request.content.validate_segment_rows(
        request
            .segments
            .iter()
            .map(|segment| (&segment.kind, segment.row_count)),
    )?;
    if plan.target != request.target || plan.disposition != request.disposition {
        return Err(CdfError::contract(
            "ordinary receipt request does not match its typed commit plan",
        ));
    }
    Ok(())
}

fn validate_correction_plan(
    request: &DestinationCorrectionCommitRequest,
    plan: &DestinationCorrectionCommitPlan,
) -> Result<()> {
    let correction_count = u64::try_from(request.corrections.len())
        .map_err(|_| CdfError::contract("correction count exceeds u64"))?;
    if plan.kernel.target != request.target
        || plan.kernel.disposition != request.resource_disposition
        || plan.correction_package_hash != request.correction_package_hash
        || plan.promotion_id != *request.promotion_id()
        || plan.old_schema_hash != *request.old_schema_hash()
        || plan.new_schema_hash != *request.new_schema_hash()
        || plan.strategy != request.strategy()
        || plan.operations_digest != request.operations_digest
        || plan.correction_count != correction_count
    {
        return Err(CdfError::contract(
            "correction receipt request does not match its typed correction commit plan",
        ));
    }
    Ok(())
}

fn validate_receipt(receipt: &Receipt, expected_segments: &[ExpectedReceiptSegment]) -> Result<()> {
    receipt.content.validate()?;
    receipt.content.validate_segment_rows(
        receipt
            .segment_acks
            .iter()
            .map(|ack| (&ack.kind, ack.row_count)),
    )?;
    validate_commit_counts(&receipt.content, &receipt.counts)?;
    if receipt.committed_at_ms < 0 {
        return Err(CdfError::contract(
            "receipt committed_at_ms must be a nonnegative Unix timestamp",
        ));
    }
    if receipt.verify.kind.trim().is_empty() || receipt.verify.statement.trim().is_empty() {
        return Err(CdfError::contract(
            "receipt verify clause requires a nonempty kind and statement",
        ));
    }
    if let Some(transaction) = &receipt.transaction
        && transaction.system.trim().is_empty()
    {
        return Err(CdfError::contract(
            "receipt transaction metadata requires a nonempty system",
        ));
    }

    let mut migration_ids = BTreeSet::new();
    for migration in &receipt.migrations {
        if migration.migration_id.trim().is_empty()
            || migration.description.trim().is_empty()
            || !migration_ids.insert(migration.migration_id.as_str())
        {
            return Err(CdfError::contract(
                "receipt migrations require unique nonempty ids and nonempty descriptions",
            ));
        }
    }

    if receipt.segment_acks.len() != expected_segments.len() {
        return Err(CdfError::contract(format!(
            "receipt acknowledges {} segments but its typed request requires {}",
            receipt.segment_acks.len(),
            expected_segments.len()
        )));
    }
    let mut segment_ids = BTreeSet::new();
    for (ack, expected) in receipt.segment_acks.iter().zip(expected_segments) {
        if !segment_ids.insert(&ack.segment_id)
            || ack.kind != expected.kind
            || ack.segment_id != expected.segment_id
            || ack.row_count != expected.row_count
            || ack.byte_count != expected.byte_count
        {
            return Err(CdfError::contract(
                "receipt segment acknowledgements must uniquely preserve typed request order, effect kind, identity, row count, and byte count",
            ));
        }
    }

    validate_verify_parameter(receipt, "target", receipt.target.as_str())?;
    validate_verify_parameter(receipt, "package_hash", receipt.package_hash.as_str())?;
    validate_verify_parameter(
        receipt,
        "idempotency_token",
        receipt.idempotency_token.as_str(),
    )?;
    validate_verify_parameter(receipt, "schema_hash", receipt.schema_hash.as_str())?;
    Ok(())
}

fn validate_commit_counts(
    content: &cdf_kernel::PackageContentAuthority,
    counts: &CommitCounts,
) -> Result<()> {
    match (content, counts) {
        (cdf_kernel::PackageContentAuthority::Rows { .. }, CommitCounts::Rows { .. }) => Ok(()),
        (
            cdf_kernel::PackageContentAuthority::KeyedChanges {
                reduction,
                delete_application,
                ..
            },
            CommitCounts::KeyedChanges {
                intent,
                hard_deletes,
                soft_deletes,
                missing_delete_keys,
                ignored_deletes,
                ..
            },
        ) => {
            if intent != &reduction.surviving {
                return Err(CdfError::contract(
                    "keyed-change receipt intent does not match the verified package effects",
                ));
            }
            match delete_application {
                cdf_kernel::DeleteApplicationAuthority::NotApplicable => {
                    if intent.deletes != 0
                        || hard_deletes.is_some()
                        || soft_deletes.is_some()
                        || missing_delete_keys.is_some()
                        || ignored_deletes.is_some()
                    {
                        return Err(CdfError::contract(
                            "delete-inapplicable receipt contains delete intent or outcomes",
                        ));
                    }
                }
                cdf_kernel::DeleteApplicationAuthority::Apply {
                    policy: cdf_kernel::DeleteApplicationPolicy::Ignore,
                } => {
                    if *ignored_deletes != Some(intent.deletes)
                        || hard_deletes.is_some()
                        || soft_deletes.is_some()
                        || missing_delete_keys.is_some()
                    {
                        return Err(CdfError::contract(
                            "ignored-delete receipt must report the exact ignored intent and no mutation outcomes",
                        ));
                    }
                }
                cdf_kernel::DeleteApplicationAuthority::Apply {
                    policy: cdf_kernel::DeleteApplicationPolicy::Hard,
                } => {
                    validate_delete_outcome_partition(
                        intent.deletes,
                        *hard_deletes,
                        *missing_delete_keys,
                        *soft_deletes,
                        *ignored_deletes,
                        "hard",
                    )?;
                }
                cdf_kernel::DeleteApplicationAuthority::Apply {
                    policy: cdf_kernel::DeleteApplicationPolicy::Soft { .. },
                } => {
                    validate_delete_outcome_partition(
                        intent.deletes,
                        *soft_deletes,
                        *missing_delete_keys,
                        *hard_deletes,
                        *ignored_deletes,
                        "soft",
                    )?;
                }
            }
            Ok(())
        }
        _ => Err(CdfError::contract(
            "receipt count kind does not match its package content authority",
        )),
    }
}

fn validate_delete_outcome_partition(
    intent: u64,
    applied: Option<u64>,
    missing: Option<u64>,
    incompatible: Option<u64>,
    ignored: Option<u64>,
    policy: &str,
) -> Result<()> {
    if incompatible.is_some() || ignored.is_some() || applied.is_some() != missing.is_some() {
        return Err(CdfError::contract(format!(
            "{policy}-delete receipt outcomes are incomplete or contain another delete policy"
        )));
    }
    if let (Some(applied), Some(missing)) = (applied, missing)
        && applied.checked_add(missing) != Some(intent)
    {
        return Err(CdfError::contract(format!(
            "{policy}-delete receipt outcomes do not partition the exact delete intent"
        )));
    }
    Ok(())
}

fn validate_verify_parameter(receipt: &Receipt, name: &str, expected: &str) -> Result<()> {
    if let Some(actual) = receipt.verify.parameters.get(name)
        && actual != expected
    {
        return Err(CdfError::contract(format!(
            "receipt verify parameter {name}={actual:?} contradicts typed value {expected:?}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use arrow_schema::{DataType, Field};
    use cdf_kernel::{
        CorrectionStrategy, DeliveryGuarantee, DestinationCorrectionOperation,
        DestinationCorrectionPlan, DestinationCorrectionRequest, IdempotencySupport,
        IdempotencyToken, MigrationRecord, PackageHash, PlanId, PromotionId,
        ResidualCorrectionOperation, RowProvenanceAddress, SchemaHash, SegmentId, StateSegment,
        TargetName, TransactionSupport, WriteDisposition,
    };

    use super::*;

    fn segment() -> StateSegment {
        StateSegment {
            kind: cdf_kernel::PackageSegmentKind::Row,
            segment_id: SegmentId::new("segment-1").unwrap(),
            scope: cdf_kernel::ScopeKey::Resource,
            output_position: cdf_kernel::SourcePosition::Cursor(cdf_kernel::CursorPosition {
                version: cdf_kernel::SOURCE_POSITION_VERSION,
                field: "id".to_owned(),
                value: cdf_kernel::CursorValue::U64(7),
            }),
            row_count: 7,
            byte_count: 70,
        }
    }

    fn content() -> cdf_kernel::PackageContentAuthority {
        cdf_kernel::PackageContentAuthority::rows(SchemaHash::new("schema").unwrap())
    }

    fn evidence(parameters: BTreeMap<String, String>) -> ReceiptEvidence {
        ReceiptEvidence {
            transaction: Some(TransactionMetadata {
                system: "test".to_owned(),
                values: BTreeMap::new(),
            }),
            counts: CommitCounts::rows(7, Some(7), Some(0), Some(0)),
            committed_at_ms: 1_234,
            verify: VerifyClause {
                kind: "test_v1".to_owned(),
                statement: "verify typed receipt".to_owned(),
                parameters,
            },
        }
    }

    fn ordinary_plan(request: &DestinationCommitRequest) -> CommitPlan {
        CommitPlan {
            plan_id: PlanId::new("plan").unwrap(),
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        }
    }

    fn correction_plan(
        request: &DestinationCorrectionCommitRequest,
    ) -> DestinationCorrectionCommitPlan {
        DestinationCorrectionCommitPlan {
            kernel: CommitPlan {
                plan_id: PlanId::new("correction-plan").unwrap(),
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
            strategy: request.strategy(),
            operations_digest: request.operations_digest.clone(),
            correction_count: request.corrections.len() as u64,
        }
    }

    #[test]
    fn ordinary_draft_maps_typed_request_fields() {
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("package").unwrap(),
            content: content(),
            target: TargetName::new("main.events").unwrap(),
            disposition: WriteDisposition::Append,
            segments: vec![segment()],
            idempotency_token: IdempotencyToken::new("token").unwrap(),
        };
        let receipt = ReceiptDraft::ordinary(
            ReceiptId::new("receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &ordinary_plan(&request),
            vec![SegmentAck {
                kind: cdf_kernel::PackageSegmentKind::Row,
                segment_id: SegmentId::new("segment-1").unwrap(),
                row_count: 7,
                byte_count: 70,
            }],
            SchemaHash::new("schema").unwrap(),
            evidence(BTreeMap::from([
                ("target".to_owned(), "main.events".to_owned()),
                ("package_hash".to_owned(), "package".to_owned()),
                ("idempotency_token".to_owned(), "token".to_owned()),
                ("schema_hash".to_owned(), "schema".to_owned()),
            ])),
        )
        .unwrap()
        .finalize()
        .unwrap();

        assert_eq!(receipt.target, request.target);
        assert_eq!(receipt.package_hash, request.package_hash);
        assert_eq!(receipt.committed_at_ms, 1_234);
        assert_eq!(
            serde_json::to_value(&receipt).unwrap(),
            serde_json::json!({
                "receipt_id": "receipt",
                "destination": "test",
                "target": "main.events",
                "package_hash": "package",
                "content": {
                    "kind": "rows",
                    "logical_schema_hash": "schema"
                },
                "segment_acks": [{
                    "kind": "row",
                    "segment_id": "segment-1",
                    "row_count": 7,
                    "byte_count": 70
                }],
                "disposition": "append",
                "idempotency_token": "token",
                "transaction": {
                    "system": "test",
                    "values": {}
                },
                // `CommitCounts` is a tagged enum since package-native keyed effects landed, so the
                // serialized form carries its `kind` discriminant alongside the row counts.
                "counts": {
                    "kind": "rows",
                    "rows_written": 7,
                    "rows_inserted": 7,
                    "rows_updated": 0,
                    "rows_deleted": 0
                },
                "schema_hash": "schema",
                "migrations": [],
                "committed_at_ms": 1_234,
                "verify": {
                    "kind": "test_v1",
                    "statement": "verify typed receipt",
                    "parameters": {
                        "idempotency_token": "token",
                        "package_hash": "package",
                        "schema_hash": "schema",
                        "target": "main.events"
                    }
                }
            })
        );
    }

    #[test]
    fn correction_draft_derives_correction_identity_and_segments() {
        let mut correction_segment = segment();
        correction_segment.row_count = 1;
        let request = DestinationCorrectionCommitRequest::new(
            PackageHash::new("correction-package").unwrap(),
            IdempotencyToken::new("correction-token").unwrap(),
            TargetName::new("main.events").unwrap(),
            WriteDisposition::Append,
            vec![correction_segment],
            vec![DestinationCorrectionOperation {
                correction: DestinationCorrectionPlan {
                    request: DestinationCorrectionRequest {
                        promotion_id: PromotionId::new("promotion").unwrap(),
                        original_row: RowProvenanceAddress::new(
                            PackageHash::new("original").unwrap(),
                            SegmentId::new("segment-1").unwrap(),
                            0,
                        ),
                        old_schema_hash: SchemaHash::new("old").unwrap(),
                        new_schema_hash: SchemaHash::new("new").unwrap(),
                        promoted_path: "/field".to_owned(),
                        promoted_value_json: "\"value\"".to_owned(),
                        residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
                        selected_strategy: CorrectionStrategy::InPlaceUpdate,
                    },
                    transaction_guarantee: TransactionSupport::AtomicPackage,
                    idempotency_guarantee: IdempotencySupport::PackageToken,
                },
                output_field: cdf_kernel::CanonicalArrowField::from_arrow(&Field::new(
                    "field",
                    DataType::Utf8,
                    true,
                ))
                .unwrap(),
                promoted_value_residual_json_v1: vec![b'{'],
            }],
        )
        .unwrap();
        let receipt = ReceiptDraft::correction(
            ReceiptId::new("correction-receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &correction_plan(&request),
            evidence(BTreeMap::from([
                ("target".to_owned(), "main.events".to_owned()),
                ("package_hash".to_owned(), "correction-package".to_owned()),
            ])),
        )
        .unwrap()
        .finalize()
        .unwrap();

        assert_eq!(receipt.package_hash, request.correction_package_hash);
        assert_eq!(receipt.schema_hash, *request.new_schema_hash());
        assert_eq!(receipt.segment_acks, request.segment_acks());
    }

    #[test]
    fn finalizer_rejects_verify_parameter_drift() {
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("package").unwrap(),
            content: content(),
            target: TargetName::new("main.events").unwrap(),
            disposition: WriteDisposition::Append,
            segments: vec![segment()],
            idempotency_token: IdempotencyToken::new("token").unwrap(),
        };
        let error = ReceiptDraft::ordinary(
            ReceiptId::new("receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &ordinary_plan(&request),
            vec![SegmentAck {
                kind: cdf_kernel::PackageSegmentKind::Row,
                segment_id: SegmentId::new("segment-1").unwrap(),
                row_count: 7,
                byte_count: 70,
            }],
            SchemaHash::new("schema").unwrap(),
            evidence(BTreeMap::from([(
                "package_hash".to_owned(),
                "different".to_owned(),
            )])),
        )
        .unwrap()
        .finalize()
        .unwrap_err();

        assert!(error.to_string().contains("contradicts typed value"));
    }

    #[test]
    fn ordinary_draft_uses_exact_typed_plan_migrations() {
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("package").unwrap(),
            content: content(),
            target: TargetName::new("main.events").unwrap(),
            disposition: WriteDisposition::Append,
            segments: vec![segment()],
            idempotency_token: IdempotencyToken::new("token").unwrap(),
        };
        let mut plan = ordinary_plan(&request);
        plan.migrations = vec![MigrationRecord {
            migration_id: "migration-1".to_owned(),
            description: "ALTER TABLE events ADD COLUMN field TEXT".to_owned(),
        }];
        let receipt = ReceiptDraft::ordinary(
            ReceiptId::new("receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &plan,
            vec![SegmentAck {
                kind: cdf_kernel::PackageSegmentKind::Row,
                segment_id: SegmentId::new("segment-1").unwrap(),
                row_count: 7,
                byte_count: 70,
            }],
            SchemaHash::new("schema").unwrap(),
            evidence(BTreeMap::new()),
        )
        .unwrap()
        .finalize()
        .unwrap();

        assert_eq!(receipt.migrations, plan.migrations);
    }

    #[test]
    fn draft_rejects_plan_drift_and_incomplete_migration_authority() {
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("package").unwrap(),
            content: content(),
            target: TargetName::new("main.events").unwrap(),
            disposition: WriteDisposition::Append,
            segments: vec![segment()],
            idempotency_token: IdempotencyToken::new("token").unwrap(),
        };
        let mut drifted_plan = ordinary_plan(&request);
        drifted_plan.target = TargetName::new("main.other").unwrap();
        let error = ReceiptDraft::ordinary(
            ReceiptId::new("receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &drifted_plan,
            Vec::new(),
            SchemaHash::new("schema").unwrap(),
            evidence(BTreeMap::new()),
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not match"));

        let mut incomplete_plan = ordinary_plan(&request);
        incomplete_plan.migrations = vec![MigrationRecord {
            migration_id: "migration-1".to_owned(),
            description: " ".to_owned(),
        }];
        let error = ReceiptDraft::ordinary(
            ReceiptId::new("receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &incomplete_plan,
            vec![SegmentAck {
                kind: cdf_kernel::PackageSegmentKind::Row,
                segment_id: SegmentId::new("segment-1").unwrap(),
                row_count: 7,
                byte_count: 70,
            }],
            SchemaHash::new("schema").unwrap(),
            evidence(BTreeMap::new()),
        )
        .unwrap()
        .finalize()
        .unwrap_err();
        assert!(error.to_string().contains("nonempty descriptions"));
    }

    #[test]
    fn finalizer_rejects_segment_byte_count_drift() {
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("package").unwrap(),
            content: content(),
            target: TargetName::new("main.events").unwrap(),
            disposition: WriteDisposition::Append,
            segments: vec![segment()],
            idempotency_token: IdempotencyToken::new("token").unwrap(),
        };
        let error = ReceiptDraft::ordinary(
            ReceiptId::new("receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &ordinary_plan(&request),
            vec![SegmentAck {
                kind: cdf_kernel::PackageSegmentKind::Row,
                segment_id: SegmentId::new("segment-1").unwrap(),
                row_count: 7,
                byte_count: 69,
            }],
            SchemaHash::new("schema").unwrap(),
            evidence(BTreeMap::new()),
        )
        .unwrap()
        .finalize()
        .unwrap_err();

        assert!(error.to_string().contains("byte count"));
    }

    #[test]
    fn keyed_receipt_binds_exact_intent_and_delete_policy_outcomes() {
        let logical = SchemaHash::new("schema").unwrap();
        let key_schema = SchemaHash::new("key-schema").unwrap();
        let intent = cdf_kernel::KeyedEffectCounts {
            upserts: 2,
            deletes: 1,
        };
        let content = cdf_kernel::PackageContentAuthority::KeyedChanges {
            logical_schema_hash: logical.clone(),
            upsert_schema_hash: logical.clone(),
            delete_schema_hash: key_schema.clone(),
            key: cdf_kernel::KeyAuthority {
                version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
                fields: vec!["id".to_owned()],
                encoding: cdf_kernel::DEDUP_KEY_ENCODING_VERSION.to_owned(),
                schema_hash: key_schema,
            },
            reduction: Box::new(cdf_kernel::KeyedEffectReductionAuthority {
                version: cdf_kernel::KEYED_EFFECT_AUTHORITY_VERSION,
                winner: cdf_kernel::KeyedEffectWinnerPolicy::Last,
                input_order: cdf_kernel::KeyedEffectInputOrder::SourceProtocol {
                    protocol: "postgresql".to_owned(),
                    version: 1,
                    scope_sha256: format!("sha256:{}", "a".repeat(64)),
                },
                input: cdf_kernel::KeyedEffectCounts {
                    upserts: 3,
                    deletes: 2,
                },
                duplicate_key_count: 2,
                surviving: intent,
                provenance_format: "parquet".to_owned(),
                provenance_version: 1,
            }),
            deletion_capture: cdf_kernel::DeletionCaptureAuthority {
                support: cdf_kernel::DeletionCaptureSupport::Inherent,
                enabled: true,
                semantics_sha256: format!("sha256:{}", "b".repeat(64)),
            },
            delete_application: cdf_kernel::DeleteApplicationAuthority::Apply {
                policy: cdf_kernel::DeleteApplicationPolicy::Ignore,
            },
        };
        let mut upsert = segment();
        upsert.kind = cdf_kernel::PackageSegmentKind::Upsert;
        upsert.segment_id = SegmentId::new("effect-0-upsert").unwrap();
        upsert.row_count = 2;
        upsert.byte_count = 20;
        let mut delete = segment();
        delete.kind = cdf_kernel::PackageSegmentKind::Delete;
        delete.segment_id = SegmentId::new("effect-1-delete").unwrap();
        delete.row_count = 1;
        delete.byte_count = 10;
        let request = DestinationCommitRequest {
            package_hash: PackageHash::new("package").unwrap(),
            content,
            target: TargetName::new("main.events").unwrap(),
            disposition: WriteDisposition::CdcApply,
            segments: vec![upsert.clone(), delete.clone()],
            idempotency_token: IdempotencyToken::new("token").unwrap(),
        };
        let acks = [&upsert, &delete]
            .into_iter()
            .map(|segment| SegmentAck {
                kind: segment.kind,
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect();
        let evidence = ReceiptEvidence {
            transaction: None,
            counts: CommitCounts::keyed_changes(intent, None, None, None, None, None, Some(1)),
            committed_at_ms: 1,
            verify: VerifyClause {
                kind: "test".to_owned(),
                statement: "verify keyed receipt".to_owned(),
                parameters: BTreeMap::new(),
            },
        };

        ReceiptDraft::ordinary(
            ReceiptId::new("receipt").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &ordinary_plan(&request),
            acks,
            logical,
            evidence.clone(),
        )
        .unwrap()
        .finalize()
        .unwrap();

        let mut wrong = evidence;
        wrong.counts = CommitCounts::keyed_changes(intent, None, None, None, None, None, Some(0));
        let error = ReceiptDraft::ordinary(
            ReceiptId::new("receipt-wrong").unwrap(),
            DestinationId::new("test").unwrap(),
            &request,
            &ordinary_plan(&request),
            request
                .segments
                .iter()
                .map(|segment| SegmentAck {
                    kind: segment.kind,
                    segment_id: segment.segment_id.clone(),
                    row_count: segment.row_count,
                    byte_count: segment.byte_count,
                })
                .collect(),
            SchemaHash::new("schema").unwrap(),
            wrong,
        )
        .unwrap()
        .finalize()
        .unwrap_err();
        assert!(error.message.contains("exact ignored intent"));
    }
}
