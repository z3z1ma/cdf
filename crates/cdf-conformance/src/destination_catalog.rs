use std::path::Path;
#[cfg(test)]
use std::sync::{Arc, Mutex};

use cdf_dest_duckdb::DuckDbRuntimeDriver;
use cdf_dest_parquet::ParquetRuntimeDriver;
use cdf_dest_postgres::PostgresRuntimeDriver;
#[cfg(test)]
use cdf_kernel::{DestinationId, DestinationProtocol, IdempotencySupport, WriteDisposition};
use cdf_kernel::{Result, TargetName};
use cdf_project::ResolvedProjectDestination;
#[cfg(test)]
use cdf_project::{
    PackageArtifactRecoveryRequest, PackageArtifactReplayRequest, recover_package_from_artifacts,
    replay_package_from_artifacts,
};
#[cfg(test)]
use cdf_runtime::{
    BulkFallbackMode, BulkOrdering, BulkPathDescriptor, BulkSizeRange, DestinationDescription,
    DestinationDriver, DestinationIngressMode, DestinationInspection, DestinationRuntime,
    DestinationRuntimeCapabilities, DestinationWriterModel,
};
use cdf_runtime::{DestinationPolicyProvider, DestinationRegistry, DestinationResolutionContext};

#[cfg(test)]
use crate::{
    destination::{
        DestinationConformanceCase, MockDestination, assert_destination_conformance,
        representative_commit_request,
    },
    package_replay::{PackageReader, PreparedPackageFixtureSpec, build_prepared_package_fixture},
};

struct ConformanceDestinationPolicy;

impl DestinationPolicyProvider for ConformanceDestinationPolicy {
    fn value(&self, destination: &str, key: &str) -> Option<&str> {
        match (destination, key) {
            ("postgres", "merge_dedup") => Some("fail"),
            _ => None,
        }
    }
}

static POLICY: ConformanceDestinationPolicy = ConformanceDestinationPolicy;

struct DestinationCatalogEntry {
    install: fn(&mut DestinationRegistry) -> Result<()>,
    #[cfg(test)]
    inspection_uri: fn(&Path) -> String,
}

const DESTINATIONS: &[DestinationCatalogEntry] = &[
    DestinationCatalogEntry {
        install: |registry| registry.register(DuckDbRuntimeDriver),
        #[cfg(test)]
        inspection_uri: |root| local_uri("duckdb", &root.join("conformance.duckdb")),
    },
    DestinationCatalogEntry {
        install: |registry| registry.register(ParquetRuntimeDriver),
        #[cfg(test)]
        inspection_uri: |root| local_uri("parquet", &root.join("conformance-lake")),
    },
    DestinationCatalogEntry {
        install: |registry| registry.register(PostgresRuntimeDriver),
        #[cfg(test)]
        inspection_uri: |_| "postgres://localhost/conformance".to_owned(),
    },
];

pub(crate) fn registry() -> Result<DestinationRegistry> {
    let mut registry = DestinationRegistry::new();
    for entry in DESTINATIONS {
        (entry.install)(&mut registry)?;
    }
    Ok(registry)
}

pub(crate) fn resolve(
    uri: &str,
    project_root: &Path,
    target: TargetName,
) -> Result<ResolvedProjectDestination> {
    let execution = crate::test_execution_services();
    let context = DestinationResolutionContext::for_project_run(project_root, &target)
        .with_environment_name("conformance")
        .with_destination_policy(&POLICY)
        .with_execution_services(&execution);
    let runtime = registry()?.resolve(uri, &context)?;
    Ok(ResolvedProjectDestination::new(runtime, target))
}

pub(crate) fn local_uri(scheme: &str, path: &Path) -> String {
    format!("{scheme}://{}", path.display())
}

#[test]
fn catalog_is_the_single_first_party_destination_enrollment_point() {
    assert_eq!(
        registry().unwrap().registered_schemes(),
        ["duckdb", "parquet", "postgres"]
    );
}

#[test]
fn every_catalog_destination_publishes_measured_bulk_and_provenance_capabilities() {
    let temp = tempfile::tempdir().unwrap();
    let registry = registry().unwrap();
    let context = DestinationResolutionContext::for_project_inspection(temp.path());
    for entry in DESTINATIONS {
        let inspection = registry
            .inspect(&(entry.inspection_uri)(temp.path()), &context)
            .unwrap();
        assert_bulk_matrix_contract(&inspection);
        assert_eq!(
            inspection
                .sheet_artifact
                .protocol_capabilities
                .corrections
                .row_provenance
                .persistence,
            cdf_kernel::CapabilitySupport::Supported
        );
        assert_eq!(
            inspection
                .sheet_artifact
                .protocol_capabilities
                .corrections
                .row_provenance
                .targetability,
            cdf_kernel::CapabilitySupport::Supported
        );
    }
}

#[test]
fn first_party_bulk_preflight_accepts_eligible_and_rejects_ineligible_schema_fixtures() {
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use cdf_dest_parquet::FilesystemParquetRuntime;
    use cdf_dest_postgres::{
        MergeDedupPolicy, PostgresDestination, PostgresRuntime, PostgresTarget,
    };

    let temp = tempfile::tempdir().unwrap();
    let eligible = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let mut duckdb =
        cdf_dest_duckdb::DuckDbDestination::new(temp.path().join("schema.duckdb")).unwrap();
    let postgres_destination = PostgresDestination::new();
    let mut postgres = PostgresRuntime::for_replay(
        &postgres_destination,
        PostgresTarget::parse("public.orders").unwrap(),
        MergeDedupPolicy::Fail,
        None,
    );
    let mut parquet = FilesystemParquetRuntime::with_execution_services(
        temp.path().join("lake"),
        crate::test_execution_services(),
    );

    for runtime in [
        &mut duckdb as &mut dyn DestinationRuntime,
        &mut postgres,
        &mut parquet,
    ] {
        let prepared = runtime
            .prepare_selected_bulk_path(&cdf_runtime::BulkPathPreparationInput::new(&eligible))
            .unwrap();
        runtime
            .runtime_capabilities()
            .validate_prepared_bulk_path(&prepared)
            .unwrap();
    }

    let duckdb_ineligible = Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal256(76, 9),
        false,
    )]);
    let postgres_ineligible = Schema::new(vec![Field::new(
        "clock",
        DataType::Time32(TimeUnit::Second),
        false,
    )]);
    let parquet_ineligible = Schema::new(vec![Field::new(
        "amount",
        DataType::Decimal128(38, 9),
        false,
    )]);
    for (runtime, schema, expected) in [
        (
            &mut duckdb as &mut dyn DestinationRuntime,
            &duckdb_ineligible,
            "Decimal256",
        ),
        (&mut postgres, &postgres_ineligible, "Time32"),
        (&mut parquet, &parquet_ineligible, "Decimal128"),
    ] {
        let error = runtime
            .prepare_selected_bulk_path(&cdf_runtime::BulkPathPreparationInput::new(schema))
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
}

#[cfg(test)]
fn assert_bulk_matrix_contract(inspection: &DestinationInspection) {
    let runtime = &inspection.runtime;
    assert!(
        !runtime.bulk_paths.is_empty(),
        "{} publishes no bulk path",
        inspection.description.destination_id
    );
    assert!(runtime.bulk_evidence_version.is_some());
    assert!(
        runtime
            .bulk_paths
            .iter()
            .all(|path| path.measured_evidence_version.is_some())
    );
    let selected = runtime
        .bulk_path
        .as_deref()
        .expect("measured destination must select a bulk path");
    assert!(runtime.bulk_paths.iter().any(|path| {
        path.path_id == selected
            && path.ingress_mode == runtime.ingress_mode
            && path.writer_model == runtime.writer_model
    }));
    assert!(
        !inspection
            .sheet_artifact
            .sheet
            .supported_dispositions
            .is_empty()
    );
    assert_ne!(
        inspection.sheet_artifact.sheet.idempotency,
        IdempotencySupport::None,
        "bulk destination must retain idempotent package/segment authority"
    );
}

#[test]
fn fourth_registered_destination_inherits_the_generic_bulk_matrix_contract() {
    let destination = MockDestination::new("fourth", vec![WriteDisposition::Append]);
    let mut registry = registry().unwrap();
    registry
        .register(FourthDriver::new(destination.clone()))
        .unwrap();
    let root = tempfile::tempdir().unwrap();
    let target = TargetName::new("orders").unwrap();
    let inspection = registry
        .inspect(
            "fourth://local/matrix",
            &DestinationResolutionContext::for_project_inspection(root.path()),
        )
        .unwrap();

    assert_bulk_matrix_contract(&inspection);
    assert_destination_conformance(
        &destination,
        [DestinationConformanceCase::new(
            representative_commit_request(WriteDisposition::Append),
        )],
    );
    assert_eq!(
        registry.registered_schemes(),
        ["duckdb", "fourth", "parquet", "postgres", "postgresql"]
    );

    let package_dir = root.path().join("package");
    build_prepared_package_fixture(
        PreparedPackageFixtureSpec::new(&package_dir, "fourth-runtime-package").unwrap(),
    )
    .unwrap();
    let inputs = PackageReader::open(&package_dir)
        .unwrap()
        .replay_inputs()
        .unwrap();
    let services = crate::test_execution_services();
    let resolution = DestinationResolutionContext::for_project_run(root.path(), &target)
        .with_execution_services(&services);
    let mut runtime = registry
        .resolve("fourth://local/matrix", &resolution)
        .unwrap();
    assert_eq!(
        runtime.protocol().sheet_artifact().unwrap(),
        inspection.sheet_artifact
    );
    let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "id",
        arrow_schema::DataType::Int64,
        false,
    )]);
    let preparation = runtime
        .prepare_bulk_paths(&cdf_runtime::BulkPathPreparationInput::new(&schema))
        .unwrap();
    assert_eq!(preparation.eligible.len(), 2);
    assert_eq!(preparation.eligible[0].descriptor.path_id, "fourth_native");
    assert_eq!(
        preparation.eligible[0].descriptor.fallback,
        BulkFallbackMode::PreflightOnly
    );
    preparation.eligible[0].validate().unwrap();
    assert_eq!(preparation.eligible[1].descriptor.path_id, "fourth_compat");
    let forced_fallback_schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "name",
        arrow_schema::DataType::Utf8,
        false,
    )]);
    let forced_fallback = runtime
        .prepare_bulk_paths(&cdf_runtime::BulkPathPreparationInput::new(
            &forced_fallback_schema,
        ))
        .unwrap();
    assert_eq!(forced_fallback.selected_path_id, "fourth_compat");
    assert_eq!(forced_fallback.eligible.len(), 1);
    assert_eq!(forced_fallback.rejected.len(), 1);
    assert_eq!(forced_fallback.rejected[0].path_id, "fourth_native");
    let unsupported_schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
        "unsupported",
        arrow_schema::DataType::Decimal256(76, 9),
        false,
    )]);
    let rejection = runtime
        .prepare_bulk_paths(&cdf_runtime::BulkPathPreparationInput::new(
            &unsupported_schema,
        ))
        .unwrap_err();
    assert!(rejection.message.contains("unsupported"));
    let store =
        cdf_state_sqlite::SqliteCheckpointStore::open(root.path().join("state.sqlite")).unwrap();
    let report = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::new(runtime, target.clone()),
        checkpoint_store: &store,
        after_receipt_verified: None,
    })
    .unwrap();
    assert!(destination.verify(&report.receipt).unwrap().verified);
    assert_eq!(
        PackageReader::open(&package_dir)
            .unwrap()
            .receipts()
            .unwrap(),
        vec![report.receipt.clone()]
    );
    assert_eq!(destination.committed_paths(), ["fourth_compat"]);
    assert!(
        destination.preparation_contexts().contains(&(true, false)),
        "package replay must provide semantic commit authority to destination preflight"
    );
    assert_eq!(
        report.receipt.segment_acks,
        inputs
            .destination_commit
            .segments
            .iter()
            .map(|segment| cdf_kernel::SegmentAck {
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect::<Vec<_>>()
    );
    let committed_once = destination.committed_segments();

    let duplicate_store =
        cdf_state_sqlite::SqliteCheckpointStore::open(root.path().join("duplicate-state.sqlite"))
            .unwrap();
    let duplicate_runtime = registry
        .resolve("fourth://local/matrix", &resolution)
        .unwrap();
    let duplicate = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: package_dir.clone(),
        destination: ResolvedProjectDestination::new(duplicate_runtime, target.clone()),
        checkpoint_store: &duplicate_store,
        after_receipt_verified: None,
    })
    .unwrap();
    assert_eq!(duplicate.receipt, report.receipt);
    assert_eq!(destination.committed_segments(), committed_once);
    assert_eq!(
        PackageReader::open(&package_dir)
            .unwrap()
            .receipts()
            .unwrap(),
        vec![report.receipt.clone()]
    );

    let crash_package_dir = root.path().join("crash-package");
    build_prepared_package_fixture(
        PreparedPackageFixtureSpec::new(&crash_package_dir, "fourth-crash-package").unwrap(),
    )
    .unwrap();
    let crashed_receipt = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&crashed_receipt);
    let fail_after_receipt = move |receipt: &cdf_kernel::Receipt| {
        *captured.lock().unwrap() = Some(receipt.clone());
        Err(cdf_kernel::CdfError::internal(
            "injected crash after durable destination receipt",
        ))
    };
    let crash_store =
        cdf_state_sqlite::SqliteCheckpointStore::open(root.path().join("crash-state.sqlite"))
            .unwrap();
    let crash_runtime = registry
        .resolve("fourth://local/matrix", &resolution)
        .unwrap();
    assert!(
        replay_package_from_artifacts(PackageArtifactReplayRequest {
            package_dir: crash_package_dir.clone(),
            destination: ResolvedProjectDestination::new(crash_runtime, target.clone()),
            checkpoint_store: &crash_store,
            after_receipt_verified: Some(&fail_after_receipt),
        })
        .is_err()
    );
    let durable_receipt = crashed_receipt.lock().unwrap().clone().unwrap();
    assert_eq!(
        PackageReader::open(&crash_package_dir)
            .unwrap()
            .receipts()
            .unwrap(),
        vec![durable_receipt.clone()]
    );
    let recovery_runtime = registry
        .resolve("fourth://local/matrix", &resolution)
        .unwrap();
    let recovered = recover_package_from_artifacts(PackageArtifactRecoveryRequest {
        package_dir: crash_package_dir.clone(),
        checkpoint_store: &crash_store,
        destination: ResolvedProjectDestination::new(recovery_runtime, target),
        receipt: durable_receipt.clone(),
        after_receipt_verified: None,
    })
    .unwrap();
    assert_eq!(recovered.receipt, durable_receipt);
    assert_eq!(
        PackageReader::open(&crash_package_dir)
            .unwrap()
            .receipts()
            .unwrap(),
        vec![durable_receipt]
    );
}

#[cfg(test)]
struct FourthDriver {
    destination: MockDestination,
}

#[cfg(test)]
impl FourthDriver {
    fn new(destination: MockDestination) -> Self {
        Self { destination }
    }
}

#[cfg(test)]
impl DestinationDriver for FourthDriver {
    fn schemes(&self) -> &'static [&'static str] {
        &["fourth"]
    }

    fn inspect(
        &self,
        _uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        let sheet_artifact = self.destination.sheet_artifact()?;
        Ok(DestinationInspection {
            description: DestinationDescription::new(
                DestinationId::new("fourth")?,
                &["fourth"],
                "fourth matrix fixture",
            ),
            sheet_artifact_hash: cdf_runtime::artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: fourth_runtime_capabilities(),
            health_probes: Vec::new(),
        })
    }

    fn resolve(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        if !uri.starts_with("fourth:") {
            return Err(cdf_kernel::CdfError::contract(
                "fourth driver received a non-fourth URI",
            ));
        }
        Ok(Box::new(FourthRuntime {
            destination: self.destination.clone(),
        }))
    }
}

#[cfg(test)]
struct FourthRuntime {
    destination: MockDestination,
}

#[cfg(test)]
impl DestinationRuntime for FourthRuntime {
    fn protocol(&self) -> &dyn cdf_kernel::DestinationProtocol {
        &self.destination
    }

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::FinalizedPackage(self)
    }

    fn describe(&self) -> DestinationDescription {
        DestinationDescription::new(
            DestinationId::new("fourth").unwrap(),
            &["fourth"],
            "fourth matrix fixture",
        )
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        fourth_runtime_capabilities()
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        self.destination
            .record_preparation_context(input.commit.is_some(), input.execution.is_some());
        if let Some(field) = input
            .output_schema
            .fields()
            .iter()
            .find(|field| matches!(field.data_type(), arrow_schema::DataType::Decimal256(_, _)))
        {
            return Err(cdf_kernel::CdfError::contract(format!(
                "field {} type {} is unsupported by fourth_native; use a non-decimal256 mapping or select another destination",
                field.name(),
                field.data_type()
            )));
        }
        let capabilities = fourth_runtime_capabilities();
        let contains_utf8 = input.output_schema.fields().iter().any(|field| {
            matches!(
                field.data_type(),
                arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8
            )
        });
        let mut paths = capabilities.bulk_paths.clone();
        let selected_path_id = if contains_utf8 {
            "fourth_compat"
        } else {
            "fourth_native"
        }
        .to_owned();
        let rejected = if contains_utf8 {
            paths.retain(|path| path.path_id == "fourth_compat");
            vec![cdf_runtime::BulkPathRejection {
                path_id: "fourth_native".to_owned(),
                field: input
                    .output_schema
                    .fields()
                    .iter()
                    .find(|field| {
                        matches!(
                            field.data_type(),
                            arrow_schema::DataType::Utf8 | arrow_schema::DataType::LargeUtf8
                        )
                    })
                    .map(|field| field.name().clone()),
                reason: "native numeric path does not encode strings".to_owned(),
                fixes: vec!["use the exact preflight compatibility path".to_owned()],
            }]
        } else {
            Vec::new()
        };
        Ok(cdf_runtime::BulkPathPreparation {
            selected_path_id,
            eligible: paths
                .into_iter()
                .map(|descriptor| cdf_runtime::PreparedBulkPath {
                    rows_per_batch: descriptor.rows.preferred,
                    bytes_per_batch: descriptor.bytes.preferred,
                    writers: 1,
                    descriptor,
                })
                .collect(),
            rejected,
        })
    }
}

#[cfg(test)]
impl cdf_runtime::FinalizedPackageIngress for FourthRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &cdf_package_contract::PackageReplayInputs,
        context: &cdf_runtime::DestinationPlanningContext<'_>,
    ) -> Result<cdf_runtime::PreparedDestinationCommit> {
        fourth_runtime_capabilities().validate_prepared_bulk_path(context.bulk_path)?;
        let plan = self.destination.plan_commit(&inputs.destination_commit)?;
        cdf_runtime::PreparedDestinationCommit::from_verified_inputs(
            inputs,
            plan,
            context.bulk_path.clone(),
            cdf_runtime::DestinationReceiptReportingPolicy::DestinationCommitReceiptOnly,
        )
    }

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut cdf_runtime::PreparedDestinationCommit,
    ) -> Result<Box<dyn cdf_kernel::CommitSession + '_>> {
        if prepared.has_pending_context() {
            return Err(cdf_kernel::CdfError::internal(
                "fourth fixture received unexpected pending context",
            ));
        }
        self.destination.record_prepared_path(
            prepared.plan().plan_id.clone(),
            prepared.bulk_path().descriptor.path_id.clone(),
        );
        self.destination
            .begin(prepared.commit().clone(), prepared.plan().clone())
    }
}

#[cfg(test)]
fn fourth_runtime_capabilities() -> DestinationRuntimeCapabilities {
    let descriptor =
        |path_id: &str, fallback: BulkFallbackMode, evidence: &str| -> BulkPathDescriptor {
            BulkPathDescriptor {
                path_id: path_id.to_owned(),
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
                fallback,
                schema_preflight_version: "fourth-schema@1".to_owned(),
                measured_evidence_version: Some(evidence.to_owned()),
            }
        };
    DestinationRuntimeCapabilities {
        bulk_paths: vec![
            descriptor(
                "fourth_native",
                BulkFallbackMode::PreflightOnly,
                "fourth-native-v1",
            ),
            descriptor(
                "fourth_compat",
                BulkFallbackMode::Forbidden,
                "fourth-compat-v1",
            ),
        ],
        bulk_path: Some("fourth_native".to_owned()),
        bulk_evidence_version: Some("fourth-native-v1".to_owned()),
        ..Default::default()
    }
}

#[test]
fn generic_project_and_cli_runtime_sources_do_not_import_destination_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_no_concrete_destination_imports(
        &root.join("crates/cdf-project/src/runtime"),
        &["destinations.rs"],
    );
    assert_no_concrete_destination_imports(
        &root.join("crates/cdf-cli/src"),
        &["destination_registry.rs", "doctor_drift.rs", "tests.rs"],
    );
}

#[cfg(test)]
fn assert_no_concrete_destination_imports(root: &Path, allowed_files: &[&str]) {
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
                let source = std::fs::read_to_string(&path).unwrap();
                let allowed = path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|name| allowed_files.contains(&name));
                assert!(
                    allowed
                        || (!source.contains("cdf_dest_duckdb")
                            && !source.contains("cdf_dest_parquet")
                            && !source.contains("cdf_dest_postgres")),
                    "generic runtime source imports a concrete destination: {}",
                    path.display()
                );
            }
        }
    }
}
