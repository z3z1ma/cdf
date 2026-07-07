use cdf_kernel::{
    CapabilitySupport, CommitPlan, CursorPosition, CursorValue, DeliveryGuarantee,
    DestinationCommitRequest, DestinationProtocol, DestinationSheet, IdempotencySupport,
    IdempotencyToken, MigrationRecord, PackageHash, PartitionId, ScopeKey, SegmentId,
    SourcePosition, StateSegment, TargetName, TypeMappingFidelity, WriteDisposition,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DestinationConformanceCase {
    pub request: DestinationCommitRequest,
    pub expected_migrations: Vec<MigrationRecord>,
}

impl DestinationConformanceCase {
    pub fn new(request: DestinationCommitRequest) -> Self {
        Self {
            request,
            expected_migrations: Vec::new(),
        }
    }

    pub fn with_expected_migrations(mut self, expected_migrations: Vec<MigrationRecord>) -> Self {
        self.expected_migrations = expected_migrations;
        self
    }
}

pub fn assert_destination_conformance<D, I>(destination: &D, cases: I)
where
    D: DestinationProtocol,
    I: IntoIterator<Item = DestinationConformanceCase>,
{
    let cases = cases.into_iter().collect::<Vec<_>>();
    assert!(!cases.is_empty(), "destination conformance needs cases");

    let sheet = destination.sheet();
    assert_sheet_has_required_evidence(sheet);

    let mut cases_by_disposition = Vec::new();
    for case in cases {
        let duplicate = cases_by_disposition
            .iter()
            .any(|stored: &DestinationConformanceCase| {
                stored.request.disposition == case.request.disposition
            });
        assert!(
            !duplicate,
            "destination conformance needs one case per disposition"
        );
        cases_by_disposition.push(case);
    }

    assert!(
        !sheet.supported_dispositions.is_empty(),
        "destination sheet must declare at least one supported disposition"
    );

    for disposition in &sheet.supported_dispositions {
        let case = case_for_disposition(&cases_by_disposition, disposition)
            .unwrap_or_else(|| panic!("missing conformance case for {disposition:?}"));
        let plan = destination
            .plan_commit(&case.request)
            .unwrap_or_else(|error| panic!("declared {disposition:?} failed to plan: {error}"));
        assert_plan_matches_case(sheet, &case.request, &case.expected_migrations, &plan);
    }

    for disposition in mvp_dispositions() {
        if sheet.supported_dispositions.contains(&disposition) {
            continue;
        }
        let request = unsupported_request(&cases_by_disposition, disposition.clone());
        assert!(
            destination.plan_commit(&request).is_err(),
            "unsupported MVP disposition {disposition:?} planned successfully"
        );
    }
}

pub fn representative_commit_request(disposition: WriteDisposition) -> DestinationCommitRequest {
    DestinationCommitRequest {
        package_hash: PackageHash::new("sha256:destination-conformance").unwrap(),
        target: TargetName::new("orders").unwrap(),
        disposition,
        segments: vec![StateSegment {
            segment_id: SegmentId::new("seg-000001").unwrap(),
            scope: ScopeKey::Partition {
                partition_id: PartitionId::new("p0").unwrap(),
            },
            output_position: SourcePosition::Cursor(CursorPosition {
                version: 1,
                field: "id".to_owned(),
                value: CursorValue::I64(42),
            }),
            row_count: 3,
            byte_count: 48,
        }],
        idempotency_token: IdempotencyToken::new("sha256:destination-conformance").unwrap(),
    }
}

fn assert_sheet_has_required_evidence(sheet: &DestinationSheet) {
    assert!(
        !sheet.destination.as_str().trim().is_empty(),
        "destination sheet must name the destination"
    );
    assert!(
        !sheet.identifier_rules.normalizer.trim().is_empty(),
        "destination sheet must declare identifier normalizer"
    );
    if let Some(max_length) = sheet.identifier_rules.max_length {
        assert!(max_length > 0, "identifier max length must be positive");
    }
    if let Some(pattern) = &sheet.identifier_rules.allowed_pattern {
        assert!(
            !pattern.trim().is_empty(),
            "identifier allowed pattern must not be empty when present"
        );
    }
    assert!(
        sheet
            .concurrency
            .max_writers
            .is_some_and(|max_writers| max_writers > 0),
        "destination sheet must declare a positive writer concurrency limit"
    );
    assert!(
        !sheet.type_mappings.is_empty(),
        "destination sheet must declare type mappings"
    );
    assert!(
        sheet
            .type_mappings
            .iter()
            .any(|mapping| mapping.fidelity == TypeMappingFidelity::Lossless),
        "destination sheet must declare at least one lossless type mapping"
    );
    for mapping in &sheet.type_mappings {
        assert!(
            !mapping.arrow_type.trim().is_empty(),
            "type mapping must name the Arrow type"
        );
        assert!(
            !mapping.destination_type.trim().is_empty(),
            "type mapping must name the destination type"
        );
    }
}

fn assert_plan_matches_case(
    sheet: &DestinationSheet,
    request: &DestinationCommitRequest,
    expected_migrations: &[MigrationRecord],
    plan: &CommitPlan,
) {
    assert_eq!(plan.target, request.target, "plan must echo request target");
    assert_eq!(
        plan.disposition, request.disposition,
        "plan must echo request disposition"
    );
    assert_eq!(
        plan.idempotency, sheet.idempotency,
        "plan must echo sheet idempotency support"
    );
    if sheet.migration_support == CapabilitySupport::Unsupported {
        assert!(
            expected_migrations.is_empty(),
            "unsupported migration sheet cannot expect migrations"
        );
        assert!(
            plan.migrations.is_empty(),
            "unsupported migration sheet cannot plan migrations"
        );
    }
    assert_eq!(
        plan.migrations, expected_migrations,
        "plan migrations must match the conformance case"
    );
    assert_eq!(
        plan.delivery_guarantee,
        expected_delivery_guarantee(sheet, &request.disposition),
        "plan delivery guarantee must be mechanically derived"
    );
}

fn expected_delivery_guarantee(
    sheet: &DestinationSheet,
    disposition: &WriteDisposition,
) -> DeliveryGuarantee {
    match disposition {
        WriteDisposition::Append => match sheet.idempotency {
            IdempotencySupport::PackageToken => DeliveryGuarantee::EffectivelyOncePerPackage,
            IdempotencySupport::None | IdempotencySupport::SegmentToken => {
                DeliveryGuarantee::AtLeastOnceDuplicateRisk
            }
        },
        WriteDisposition::Replace => DeliveryGuarantee::EffectivelyOncePerTarget,
        WriteDisposition::Merge => DeliveryGuarantee::EffectivelyOncePerKey,
        WriteDisposition::CdcApply => match sheet.idempotency {
            IdempotencySupport::PackageToken => DeliveryGuarantee::EffectivelyOncePerPosition,
            IdempotencySupport::None | IdempotencySupport::SegmentToken => {
                DeliveryGuarantee::AtLeastOnceDuplicateRisk
            }
        },
    }
}

fn unsupported_request(
    cases_by_disposition: &[DestinationConformanceCase],
    disposition: WriteDisposition,
) -> DestinationCommitRequest {
    let mut request = cases_by_disposition
        .first()
        .expect("destination conformance needs cases")
        .request
        .clone();
    request.disposition = disposition;
    request
}

fn mvp_dispositions() -> [WriteDisposition; 4] {
    [
        WriteDisposition::Append,
        WriteDisposition::Replace,
        WriteDisposition::Merge,
        WriteDisposition::CdcApply,
    ]
}

fn case_for_disposition<'a>(
    cases: &'a [DestinationConformanceCase],
    disposition: &WriteDisposition,
) -> Option<&'a DestinationConformanceCase> {
    cases
        .iter()
        .find(|case| case.request.disposition == *disposition)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind};

    use cdf_kernel::{
        CdfError, ConcurrencyLimit, DestinationId, IdentifierRules, PlanId, Result,
        TransactionSupport, TypeMapping,
    };

    use super::*;

    #[test]
    fn sound_destination_passes_conformance() {
        let destination = FaultyDestination::sound();
        assert_destination_conformance(
            &destination,
            [
                case(WriteDisposition::Append),
                case(WriteDisposition::Replace),
                case(WriteDisposition::Merge),
            ],
        );
    }

    #[test]
    fn negative_self_tests_prove_harness_catches_contract_violations() {
        for fault in [
            Fault::FalseDispositionClaim,
            Fault::WrongIdempotency,
            Fault::WrongTargetEcho,
            Fault::WrongDispositionEcho,
            Fault::WrongDeliveryGuarantee,
            Fault::WrongMigrations,
            Fault::MissingTypeMappings,
            Fault::UnsupportedDispositionAccepted,
            Fault::UnsupportedMigrationPlanned,
        ] {
            assert_harness_panics(FaultyDestination::with_fault(fault));
        }
    }

    fn assert_harness_panics(destination: FaultyDestination) {
        let result = catch_unwind(AssertUnwindSafe(|| {
            assert_destination_conformance(&destination, cases_for_fault(destination.fault));
        }));
        assert!(result.is_err(), "fault {:?} passed", destination.fault);
    }

    fn cases_for_fault(fault: Option<Fault>) -> Vec<DestinationConformanceCase> {
        let append = if matches!(fault, Some(Fault::UnsupportedMigrationPlanned)) {
            case(WriteDisposition::Append).with_expected_migrations(vec![sample_migration()])
        } else {
            case(WriteDisposition::Append)
        };
        vec![
            append,
            case(WriteDisposition::Replace),
            case(WriteDisposition::Merge),
        ]
    }

    fn case(disposition: WriteDisposition) -> DestinationConformanceCase {
        DestinationConformanceCase::new(representative_commit_request(disposition))
    }

    fn sample_migration() -> MigrationRecord {
        MigrationRecord {
            migration_id: "unexpected".to_owned(),
            description: "unexpected DDL".to_owned(),
        }
    }

    #[derive(Clone, Debug)]
    struct FaultyDestination {
        sheet: DestinationSheet,
        fault: Option<Fault>,
    }

    #[derive(Clone, Copy, Debug)]
    enum Fault {
        FalseDispositionClaim,
        WrongIdempotency,
        WrongTargetEcho,
        WrongDispositionEcho,
        WrongDeliveryGuarantee,
        WrongMigrations,
        MissingTypeMappings,
        UnsupportedDispositionAccepted,
        UnsupportedMigrationPlanned,
    }

    impl FaultyDestination {
        fn sound() -> Self {
            Self {
                sheet: sound_sheet(),
                fault: None,
            }
        }

        fn with_fault(fault: Fault) -> Self {
            let mut sheet = sound_sheet();
            if matches!(fault, Fault::MissingTypeMappings) {
                sheet.type_mappings.clear();
            }
            if matches!(fault, Fault::UnsupportedMigrationPlanned) {
                sheet.migration_support = CapabilitySupport::Unsupported;
            }
            Self {
                sheet,
                fault: Some(fault),
            }
        }
    }

    impl DestinationProtocol for FaultyDestination {
        fn sheet(&self) -> &DestinationSheet {
            &self.sheet
        }

        fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
            if !self
                .sheet
                .supported_dispositions
                .contains(&request.disposition)
                && !matches!(
                    (self.fault, &request.disposition),
                    (
                        Some(Fault::UnsupportedDispositionAccepted),
                        WriteDisposition::CdcApply
                    )
                )
            {
                return Err(CdfError::contract(format!(
                    "unsupported disposition {:?}",
                    request.disposition
                )));
            }
            if matches!(self.fault, Some(Fault::FalseDispositionClaim))
                && request.disposition == WriteDisposition::Merge
            {
                return Err(CdfError::contract("merge not actually supported"));
            }

            let target = if matches!(self.fault, Some(Fault::WrongTargetEcho)) {
                TargetName::new("other_orders").unwrap()
            } else {
                request.target.clone()
            };
            let disposition = if matches!(self.fault, Some(Fault::WrongDispositionEcho)) {
                WriteDisposition::Append
            } else {
                request.disposition.clone()
            };
            let idempotency = if matches!(self.fault, Some(Fault::WrongIdempotency)) {
                IdempotencySupport::None
            } else {
                self.sheet.idempotency.clone()
            };
            let should_plan_migration = matches!(self.fault, Some(Fault::WrongMigrations))
                || (matches!(self.fault, Some(Fault::UnsupportedMigrationPlanned))
                    && request.disposition == WriteDisposition::Append);
            let migrations = if should_plan_migration {
                vec![sample_migration()]
            } else {
                Vec::new()
            };
            let delivery_guarantee = if matches!(self.fault, Some(Fault::WrongDeliveryGuarantee)) {
                DeliveryGuarantee::AtLeastOnceDuplicateRisk
            } else {
                expected_delivery_guarantee(&self.sheet, &request.disposition)
            };

            Ok(CommitPlan {
                plan_id: PlanId::new(format!(
                    "faulty:{}:{:?}",
                    request.target.as_str(),
                    request.disposition
                ))
                .unwrap(),
                target,
                disposition,
                idempotency,
                migrations,
                delivery_guarantee,
            })
        }
    }

    fn sound_sheet() -> DestinationSheet {
        DestinationSheet {
            destination: DestinationId::new("faulty").unwrap(),
            supported_dispositions: vec![
                WriteDisposition::Append,
                WriteDisposition::Replace,
                WriteDisposition::Merge,
            ],
            transactions: TransactionSupport::AtomicPackage,
            idempotency: IdempotencySupport::PackageToken,
            type_mappings: vec![TypeMapping {
                arrow_type: "Int64".to_owned(),
                destination_type: "BIGINT".to_owned(),
                fidelity: TypeMappingFidelity::Lossless,
            }],
            identifier_rules: IdentifierRules {
                normalizer: "namecase-v1".to_owned(),
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
}
