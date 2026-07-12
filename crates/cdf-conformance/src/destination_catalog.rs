use std::path::Path;

use cdf_dest_duckdb::DuckDbRuntimeDriver;
use cdf_dest_parquet::ParquetRuntimeDriver;
use cdf_dest_postgres::PostgresRuntimeDriver;
use cdf_kernel::{Result, TargetName};
use cdf_project::ResolvedProjectDestination;
use cdf_runtime::{DestinationPolicyProvider, DestinationRegistry, DestinationResolutionContext};

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
        assert!(!inspection.runtime.bulk_paths.is_empty());
        assert!(inspection.runtime.bulk_evidence_version.is_some());
        assert!(
            inspection
                .runtime
                .bulk_paths
                .iter()
                .all(|path| path.measured_evidence_version.is_some())
        );
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
