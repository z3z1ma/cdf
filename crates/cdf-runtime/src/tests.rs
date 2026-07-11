use crate::prelude::*;
use cdf_kernel::{
    CommitSession, ConcurrencyLimit, DeliveryGuarantee, DestinationId, IdempotencySupport,
    IdentifierRules, MigrationRecord, PlanId, TransactionSupport, TypeMapping,
};

use super::*;

struct MockProtocol {
    sheet: DestinationSheet,
}

impl DestinationProtocol for MockProtocol {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        Ok(CommitPlan {
            plan_id: PlanId::new(format!("plan-{}", self.sheet.destination))?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: Vec::<MigrationRecord>::new(),
            delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerPackage,
        })
    }

    fn begin(
        &self,
        _request: DestinationCommitRequest,
        _plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        Err(CdfError::destination("mock commit session is not used"))
    }

    fn verify(&self, receipt: &Receipt) -> Result<ReceiptVerification> {
        Ok(ReceiptVerification {
            verified: true,
            receipt_id: receipt.receipt_id.clone(),
            reason: None,
        })
    }
}

struct MockRuntime {
    protocol: MockProtocol,
    description: DestinationDescription,
}

impl DestinationRuntime for MockRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.protocol
    }

    fn describe(&self) -> DestinationDescription {
        self.description.clone()
    }

    fn prepare_package_commit(
        &mut self,
        _package_dir: &Path,
        _reader: &PackageReader,
        _inputs: &PackageReplayInputs,
        _context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        Err(CdfError::destination(
            "mock package preparation is not used",
        ))
    }

    fn bind_prepared_commit(&mut self, _prepared: &mut PreparedDestinationCommit) -> Result<()> {
        Ok(())
    }
}

struct MockDriver {
    schemes: &'static [&'static str],
    destination: &'static str,
}

impl DestinationDriver for MockDriver {
    fn schemes(&self) -> &'static [&'static str] {
        self.schemes
    }

    fn inspect(
        &self,
        _uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        let sheet = mock_sheet(self.destination);
        Ok(DestinationInspection {
            description: DestinationDescription::new(
                sheet.destination.clone(),
                self.schemes,
                self.destination,
            ),
            sheet,
            sheet_artifact_hash: format!("sha256:{}", self.destination),
            runtime: DestinationRuntimeCapabilities {
                ingress_mode: DestinationIngressMode::StagedDurableSegments,
                writer_model: DestinationWriterModel::ConcurrentSegments,
                max_in_flight_segments: Some(4),
                max_in_flight_bytes: Some(64 * 1024 * 1024),
                bulk_path: Some("mock_arrow".to_owned()),
                bulk_evidence_version: Some("v1".to_owned()),
            },
            health_probes: vec![DestinationHealthProbe {
                probe_id: "reachable".to_owned(),
                description: "mock reachability".to_owned(),
                requires_credentials: false,
                mutates_destination: false,
            }],
        })
    }

    fn resolve(
        &self,
        _uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let sheet = mock_sheet(self.destination);
        Ok(Box::new(MockRuntime {
            description: DestinationDescription::new(
                sheet.destination.clone(),
                self.schemes,
                self.destination,
            ),
            protocol: MockProtocol { sheet },
        }))
    }
}

fn mock_sheet(destination: &str) -> DestinationSheet {
    DestinationSheet {
        destination: DestinationId::new(destination).unwrap(),
        supported_dispositions: vec![WriteDisposition::Append],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: Vec::<TypeMapping>::new(),
        identifier_rules: IdentifierRules {
            normalizer: "namecase-v1".to_owned(),
            max_length: None,
            allowed_pattern: None,
        },
        migration_support: CapabilitySupport::Unsupported,
        quarantine_tables: CapabilitySupport::Unsupported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    }
}

#[test]
fn registry_resolves_and_inspects_without_order_authority() {
    static ALPHA: &[&str] = &["alpha"];
    static BETA: &[&str] = &["beta"];
    let mut forward = DestinationRegistry::new();
    forward
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha_destination",
        })
        .unwrap();
    forward
        .register(MockDriver {
            schemes: BETA,
            destination: "beta_destination",
        })
        .unwrap();
    let mut reverse = DestinationRegistry::new();
    reverse
        .register(MockDriver {
            schemes: BETA,
            destination: "beta_destination",
        })
        .unwrap();
    reverse
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha_destination",
        })
        .unwrap();

    assert_eq!(forward.registered_schemes(), reverse.registered_schemes());
    let context = DestinationResolutionContext::new();
    let inspected = forward.inspect("alpha://target", &context).unwrap();
    assert_eq!(
        inspected.description.destination_id.as_str(),
        "alpha_destination"
    );
    assert_eq!(
        inspected.runtime.ingress_mode,
        DestinationIngressMode::StagedDurableSegments
    );
    assert!(!inspected.health_probes[0].mutates_destination);
    let resolved = reverse.resolve("ALPHA://target", &context).unwrap();
    assert_eq!(
        resolved.describe().destination_id.as_str(),
        "alpha_destination"
    );
}

#[test]
fn registry_rejects_empty_malformed_and_duplicate_schemes() {
    static EMPTY: &[&str] = &[];
    static MALFORMED: &[&str] = &["bad_scheme"];
    static ALPHA: &[&str] = &["alpha"];
    let mut registry = DestinationRegistry::new();
    assert!(
        registry
            .register(MockDriver {
                schemes: EMPTY,
                destination: "empty",
            })
            .is_err()
    );
    assert!(
        registry
            .register(MockDriver {
                schemes: MALFORMED,
                destination: "malformed",
            })
            .is_err()
    );
    registry
        .register(MockDriver {
            schemes: ALPHA,
            destination: "alpha",
        })
        .unwrap();
    assert!(
        registry
            .register(MockDriver {
                schemes: ALPHA,
                destination: "duplicate",
            })
            .is_err()
    );
    assert!(
        registry
            .resolve("unknown://target", &DestinationResolutionContext::new())
            .is_err()
    );
}

#[test]
fn runtime_capabilities_are_serializable_plan_evidence() {
    let capabilities = DestinationRuntimeCapabilities {
        ingress_mode: DestinationIngressMode::StagedDurableSegments,
        writer_model: DestinationWriterModel::ConcurrentSegments,
        max_in_flight_segments: Some(8),
        max_in_flight_bytes: Some(128 * 1024 * 1024),
        bulk_path: Some("arrow".to_owned()),
        bulk_evidence_version: Some("2026-07".to_owned()),
    };
    let json = serde_json::to_string(&capabilities).unwrap();
    assert_eq!(
        serde_json::from_str::<DestinationRuntimeCapabilities>(&json).unwrap(),
        capabilities
    );
}

#[test]
fn manifest_has_no_upward_or_concrete_dependencies() {
    let manifest = include_str!("../Cargo.toml");
    for forbidden in [
        "cdf-project",
        "cdf-engine",
        "cdf-dest-",
        "datafusion",
        "duckdb",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "cdf-runtime manifest contains forbidden dependency `{forbidden}`"
        );
    }
}
