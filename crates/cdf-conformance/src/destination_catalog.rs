use std::path::Path;
#[cfg(test)]
use std::{fs, path::PathBuf};

#[cfg(test)]
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};

use cdf_dest_duckdb::DuckDbRuntimeDriver;
use cdf_dest_parquet::ParquetRuntimeDriver;
use cdf_dest_postgres::PostgresRuntimeDriver;
#[cfg(test)]
use cdf_kernel::{CdfError, DestinationProtocol, IdempotencySupport, Receipt, WriteDisposition};
use cdf_kernel::{Result, TargetName};
#[cfg(test)]
use cdf_project::ProjectReceiptSource;
use cdf_project::ResolvedProjectDestination;
#[cfg(test)]
use cdf_runtime::{DestinationInspection, DestinationRuntime};
use cdf_runtime::{DestinationPolicyProvider, DestinationRegistry, DestinationResolutionContext};

use crate::run_matrix::MatrixDestination;
#[cfg(test)]
use crate::run_matrix::local_postgres::{LivePostgres, qualified_name, reset_postgres_schema};
#[cfg(test)]
use cdf_dest_duckdb::{DuckDbDestination, DuckDbMirrorSnapshot};
#[cfg(test)]
use cdf_dest_parquet::ParquetDestination;
#[cfg(test)]
use cdf_dest_postgres::{PostgresDestination, PostgresTarget};
#[cfg(test)]
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
#[cfg(test)]
use postgres::{Client, NoTls};

#[cfg(test)]
mod quasar;

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
    id: &'static str,
    #[cfg(test)]
    runtime_destination_id: &'static str,
    #[cfg(test)]
    expects_row_provenance: bool,
    install: fn(&mut DestinationRegistry) -> Result<()>,
    #[cfg(test)]
    inspection_uri: fn(&Path) -> String,
    #[cfg(test)]
    fixture: fn(&Path, &str, &ConformanceEnvironment) -> Result<DestinationFixture>,
}

#[cfg(test)]
pub(crate) struct ConformanceEnvironment {
    postgres: Option<LivePostgres>,
}

#[cfg(test)]
impl ConformanceEnvironment {
    pub(crate) fn start() -> Result<Self> {
        Ok(Self {
            postgres: Some(LivePostgres::start()?),
        })
    }

    pub(crate) fn local_only() -> Self {
        Self { postgres: None }
    }

    pub(crate) fn postgres(&self) -> Result<&LivePostgres> {
        self.postgres
            .as_ref()
            .ok_or_else(|| CdfError::contract("conformance environment does not provide Postgres"))
    }

    pub(crate) fn assert_redacted(&self, text: &str) {
        if let Some(postgres) = &self.postgres {
            assert!(!text.contains(postgres.url()));
        }
    }
}

const DESTINATIONS: &[DestinationCatalogEntry] = &[
    DestinationCatalogEntry {
        id: "duckdb",
        #[cfg(test)]
        runtime_destination_id: "duckdb",
        #[cfg(test)]
        expects_row_provenance: true,
        install: |registry| registry.register(DuckDbRuntimeDriver),
        #[cfg(test)]
        inspection_uri: |root| local_uri("duckdb", &root.join("conformance.duckdb")),
        #[cfg(test)]
        fixture: duckdb_fixture,
    },
    DestinationCatalogEntry {
        id: "parquet_filesystem",
        #[cfg(test)]
        runtime_destination_id: "parquet_object_store",
        #[cfg(test)]
        expects_row_provenance: true,
        install: |registry| registry.register(ParquetRuntimeDriver),
        #[cfg(test)]
        inspection_uri: |root| local_uri("parquet", &root.join("conformance-lake")),
        #[cfg(test)]
        fixture: parquet_fixture,
    },
    DestinationCatalogEntry {
        id: "postgres",
        #[cfg(test)]
        runtime_destination_id: "postgres",
        #[cfg(test)]
        expects_row_provenance: true,
        install: |registry| registry.register(PostgresRuntimeDriver),
        #[cfg(test)]
        inspection_uri: |_| "postgres://localhost/conformance".to_owned(),
        #[cfg(test)]
        fixture: postgres_fixture,
    },
    #[cfg(test)]
    DestinationCatalogEntry {
        id: "quasar",
        runtime_destination_id: "quasar",
        expects_row_provenance: false,
        install: |registry| registry.register(quasar::QuasarDriver),
        inspection_uri: |root| local_uri("quasar", &root.join("conformance-quasar")),
        fixture: quasar_fixture,
    },
];

pub(crate) fn conformance_destinations() -> Vec<MatrixDestination> {
    DESTINATIONS
        .iter()
        .map(|entry| MatrixDestination::new(entry.id).expect("catalog id is valid"))
        .collect()
}

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
    Ok(ResolvedProjectDestination::new(runtime, target).with_execution_services(execution))
}

pub(crate) fn local_uri(scheme: &str, path: &Path) -> String {
    format!("{scheme}://{}", path.display())
}

#[test]
fn catalog_is_the_single_first_party_destination_enrollment_point() {
    assert_eq!(
        registry().unwrap().registered_schemes(),
        ["duckdb", "parquet", "postgres", "postgresql", "quasar"]
    );
    assert_eq!(
        conformance_destinations()
            .into_iter()
            .map(|destination| destination.as_str().to_owned())
            .collect::<Vec<_>>(),
        ["duckdb", "parquet_filesystem", "postgres", "quasar"]
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
        assert_eq!(
            inspection.description.destination_id.as_str(),
            entry.runtime_destination_id
        );
        assert_eq!(
            inspection.sheet_artifact.sheet.destination.as_str(),
            entry.runtime_destination_id
        );
        assert_bulk_matrix_contract(&inspection);
        let row_provenance = &inspection
            .sheet_artifact
            .protocol_capabilities
            .corrections
            .row_provenance;
        assert_eq!(
            row_provenance.persistence == cdf_kernel::CapabilitySupport::Supported,
            entry.expects_row_provenance
        );
        assert_eq!(
            row_provenance.targetability == cdf_kernel::CapabilitySupport::Supported,
            entry.expects_row_provenance
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
        DataType::Time32(TimeUnit::Microsecond),
        false,
    )]);
    let parquet_ineligible = Schema::new(vec![Field::new(
        "interval",
        DataType::Interval(arrow_schema::IntervalUnit::MonthDayNano),
        false,
    )]);
    for (runtime, schema, expected) in [
        (
            &mut duckdb as &mut dyn DestinationRuntime,
            &duckdb_ineligible,
            "Decimal256",
        ),
        (&mut postgres, &postgres_ineligible, "Time32"),
        (&mut parquet, &parquet_ineligible, "month-day-nanosecond"),
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

#[cfg(test)]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub(crate) struct DestinationExecutionSpec {
    uri: String,
    project_root: PathBuf,
    target: TargetName,
}

#[cfg(test)]
impl DestinationExecutionSpec {
    pub(crate) fn resolved(&self) -> Result<ResolvedProjectDestination> {
        resolve(&self.uri, &self.project_root, self.target.clone())
    }
}

#[cfg(test)]
#[derive(Clone, Debug)]
pub(crate) struct DestinationFixture {
    destination: MatrixDestination,
    runtime_destination_id: &'static str,
    execution: DestinationExecutionSpec,
    state: DestinationFixtureState,
}

#[cfg(test)]
#[derive(Clone, Debug)]
enum DestinationFixtureState {
    DuckDb {
        database_path: PathBuf,
    },
    Parquet {
        root: PathBuf,
        scheme: &'static str,
    },
    Postgres {
        database_url: String,
        schema: String,
        table: String,
    },
    Quasar {
        root: PathBuf,
    },
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum DestinationFootprint {
    DuckDb {
        mirror: DuckDbMirrorSnapshot,
        payload_rows: Vec<LogicalRow>,
    },
    Parquet {
        files: Vec<FileFootprint>,
    },
    Postgres {
        payload_rows: Vec<LogicalRow>,
        loads_rows: i64,
        state_rows: i64,
    },
    Quasar {
        files: Vec<FileFootprint>,
    },
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FileFootprint {
    path: String,
    bytes: Vec<u8>,
}

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DestinationPayload(Vec<LogicalRow>);

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(super) struct LogicalRow {
    id: i64,
    name: Option<String>,
}

#[cfg(test)]
impl DestinationPayload {
    pub(crate) fn prepared_orders() -> Self {
        Self(vec![
            LogicalRow {
                id: 1,
                name: Some("ada".to_owned()),
            },
            LogicalRow {
                id: 2,
                name: Some("grace".to_owned()),
            },
            LogicalRow { id: 3, name: None },
        ])
    }
}

#[cfg(test)]
impl DestinationFixture {
    pub(crate) fn execution_spec(&self) -> DestinationExecutionSpec {
        self.execution.clone()
    }

    pub(crate) fn target_name(&self) -> TargetName {
        self.execution.target.clone()
    }

    pub(crate) fn resolved(&self) -> Result<ResolvedProjectDestination> {
        self.execution.resolved()
    }

    pub(crate) fn assert_receipt_identity(&self, receipt: &Receipt) -> Result<()> {
        if receipt.destination.as_str() != self.runtime_destination_id {
            return Err(CdfError::destination(format!(
                "destination {} emitted receipt identity {}; expected {}",
                self.destination.as_str(),
                receipt.destination,
                self.runtime_destination_id
            )));
        }
        let resolved = self.resolved()?;
        if resolved.describe().destination_id.as_str() != self.runtime_destination_id {
            return Err(CdfError::destination(format!(
                "destination {} resolved runtime identity {}; expected {}",
                self.destination.as_str(),
                resolved.describe().destination_id,
                self.runtime_destination_id
            )));
        }
        Ok(())
    }

    pub(crate) fn supported_dispositions(&self) -> Result<Vec<WriteDisposition>> {
        let inspection = registry()?.inspect(
            &self.execution.uri,
            &DestinationResolutionContext::for_project_inspection(&self.execution.project_root),
        )?;
        Ok(inspection
            .sheet_artifact
            .sheet
            .supported_dispositions
            .clone())
    }

    pub(crate) fn idempotency(&self) -> Result<IdempotencySupport> {
        let inspection = registry()?.inspect(
            &self.execution.uri,
            &DestinationResolutionContext::for_project_inspection(&self.execution.project_root),
        )?;
        Ok(inspection.sheet_artifact.sheet.idempotency)
    }

    pub(crate) fn verify_trait_receipt(&self, receipt: &Receipt) -> Result<()> {
        let verification = match &self.state {
            DestinationFixtureState::DuckDb { database_path } => {
                DuckDbDestination::new(database_path)?.verify(receipt)?
            }
            DestinationFixtureState::Parquet { root, .. } => {
                ParquetDestination::new_filesystem(root, crate::test_execution_services())?
                    .verify(receipt)?
            }
            DestinationFixtureState::Postgres { database_url, .. } => {
                PostgresDestination::connect(database_url.clone())?.verify(receipt)?
            }
            DestinationFixtureState::Quasar { .. } => quasar::verify_receipt(receipt)?,
        };
        if !verification.verified {
            return Err(CdfError::destination(format!(
                "conformance receipt {} did not verify through DestinationProtocol::verify: {}",
                verification.receipt_id,
                verification
                    .reason
                    .unwrap_or_else(|| "verification returned false".to_owned())
            )));
        }
        Ok(())
    }

    pub(crate) fn footprint(&self) -> Result<DestinationFootprint> {
        match &self.state {
            DestinationFixtureState::DuckDb { database_path } => {
                if !database_path.exists() {
                    return Ok(DestinationFootprint::DuckDb {
                        mirror: DuckDbMirrorSnapshot::default(),
                        payload_rows: Vec::new(),
                    });
                }
                Ok(DestinationFootprint::DuckDb {
                    mirror: DuckDbDestination::new(database_path)?
                        .read_mirror_snapshot_read_only()?,
                    payload_rows: duckdb_payload(database_path, self.execution.target.as_str())?,
                })
            }
            DestinationFixtureState::Parquet { root, .. } => Ok(DestinationFootprint::Parquet {
                files: list_relative_files(root)?,
            }),
            DestinationFixtureState::Postgres {
                database_url,
                schema,
                table,
            } => postgres_footprint(database_url, schema, table, self.execution.target.as_str()),
            DestinationFixtureState::Quasar { root } => Ok(DestinationFootprint::Quasar {
                files: list_relative_files(root)?,
            }),
        }
    }

    pub(crate) fn payload_snapshot(&self) -> Result<DestinationPayload> {
        match &self.state {
            DestinationFixtureState::DuckDb { database_path } => {
                Ok(DestinationPayload(if database_path.exists() {
                    duckdb_payload(database_path, self.execution.target.as_str())?
                } else {
                    Vec::new()
                }))
            }
            DestinationFixtureState::Parquet { root, .. } => {
                Ok(DestinationPayload(parquet_payload(root)?))
            }
            DestinationFixtureState::Postgres {
                database_url,
                schema,
                table,
            } => Ok(DestinationPayload(postgres_payload(
                database_url,
                schema,
                table,
            )?)),
            DestinationFixtureState::Quasar { root } => {
                Ok(DestinationPayload(quasar::payload(root)?))
            }
        }
    }

    pub(crate) fn fresh_artifact_replay_destination(&self, root: &Path) -> Result<Self> {
        match &self.state {
            DestinationFixtureState::DuckDb { .. } => {
                let database_path = root.join(".cdf/replay.duckdb");
                Ok(Self {
                    destination: self.destination.clone(),
                    runtime_destination_id: self.runtime_destination_id,
                    execution: DestinationExecutionSpec {
                        uri: local_uri("duckdb", &database_path),
                        project_root: root.to_path_buf(),
                        target: self.execution.target.clone(),
                    },
                    state: DestinationFixtureState::DuckDb { database_path },
                })
            }
            DestinationFixtureState::Parquet { scheme, .. } => {
                let replay_root = root.join(format!(".cdf/replay-{}", self.destination.as_str()));
                Ok(Self {
                    destination: self.destination.clone(),
                    runtime_destination_id: self.runtime_destination_id,
                    execution: DestinationExecutionSpec {
                        uri: local_uri(scheme, &replay_root),
                        project_root: root.to_path_buf(),
                        target: self.execution.target.clone(),
                    },
                    state: DestinationFixtureState::Parquet {
                        root: replay_root,
                        scheme,
                    },
                })
            }
            DestinationFixtureState::Postgres {
                database_url,
                schema,
                table,
            } => {
                reset_postgres_schema(database_url, schema)?;
                Ok(Self {
                    destination: self.destination.clone(),
                    runtime_destination_id: self.runtime_destination_id,
                    execution: self.execution.clone(),
                    state: DestinationFixtureState::Postgres {
                        database_url: database_url.clone(),
                        schema: schema.clone(),
                        table: table.clone(),
                    },
                })
            }
            DestinationFixtureState::Quasar { .. } => {
                let replay_root = root.join(".cdf/replay-quasar");
                Ok(Self {
                    destination: self.destination.clone(),
                    runtime_destination_id: self.runtime_destination_id,
                    execution: DestinationExecutionSpec {
                        uri: local_uri("quasar", &replay_root),
                        project_root: root.to_path_buf(),
                        target: self.execution.target.clone(),
                    },
                    state: DestinationFixtureState::Quasar { root: replay_root },
                })
            }
        }
    }

    pub(crate) fn duplicate_retry_behavior(&self, source: ProjectReceiptSource) -> String {
        match source {
            ProjectReceiptSource::DestinationCommit {
                duplicate: true,
                package_receipt_recorded: false,
            } => "no-op duplicate: package-token destination reported duplicate=true and destination footprint was unchanged".to_owned(),
            ProjectReceiptSource::DestinationCommitReceiptOnly {
                package_receipt_recorded: false,
            } => "no-op duplicate: receipt-only destination returned the stable receipt and destination footprint was unchanged".to_owned(),
            other => panic!("unexpected duplicate retry receipt source: {other:?}"),
        }
    }
}

#[cfg(test)]
impl DestinationFootprint {
    pub(crate) fn has_destination_write(&self) -> bool {
        match self {
            Self::DuckDb {
                mirror,
                payload_rows,
            } => {
                mirror.loads_table_present
                    || mirror.state_table_present
                    || !mirror.loads.is_empty()
                    || !mirror.state.is_empty()
                    || !payload_rows.is_empty()
            }
            Self::Parquet { files } => !files.is_empty(),
            Self::Postgres {
                payload_rows,
                loads_rows,
                ..
            } => !payload_rows.is_empty() || *loads_rows > 0,
            Self::Quasar { files } => !files.is_empty(),
        }
    }
}

#[cfg(test)]
pub(crate) fn fixture(
    destination: &MatrixDestination,
    root: &Path,
    table: &str,
    environment: &ConformanceEnvironment,
) -> Result<DestinationFixture> {
    let entry = DESTINATIONS
        .iter()
        .find(|entry| entry.id == destination.as_str())
        .ok_or_else(|| {
            CdfError::contract(format!(
                "destination {} is not enrolled in conformance",
                destination.as_str()
            ))
        })?;
    (entry.fixture)(root, table, environment)
}

#[cfg(test)]
fn duckdb_fixture(
    root: &Path,
    table: &str,
    _environment: &ConformanceEnvironment,
) -> Result<DestinationFixture> {
    let database_path = root.join(".cdf/run-matrix.duckdb");
    Ok(DestinationFixture {
        destination: MatrixDestination::new("duckdb")?,
        runtime_destination_id: "duckdb",
        execution: DestinationExecutionSpec {
            uri: local_uri("duckdb", &database_path),
            project_root: root.to_path_buf(),
            target: TargetName::new(table)?,
        },
        state: DestinationFixtureState::DuckDb { database_path },
    })
}

#[cfg(test)]
fn parquet_fixture(
    root: &Path,
    table: &str,
    _environment: &ConformanceEnvironment,
) -> Result<DestinationFixture> {
    let lake_root = root.join(".cdf/lake");
    Ok(DestinationFixture {
        destination: MatrixDestination::new("parquet_filesystem")?,
        runtime_destination_id: "parquet_object_store",
        execution: DestinationExecutionSpec {
            uri: local_uri("parquet", &lake_root),
            project_root: root.to_path_buf(),
            target: TargetName::new(table)?,
        },
        state: DestinationFixtureState::Parquet {
            root: lake_root,
            scheme: "parquet",
        },
    })
}

#[cfg(test)]
fn postgres_fixture(
    root: &Path,
    table: &str,
    environment: &ConformanceEnvironment,
) -> Result<DestinationFixture> {
    let postgres = environment.postgres()?;
    let target = PostgresTarget::new(Some(postgres.schema()), table)?;
    Ok(DestinationFixture {
        destination: MatrixDestination::new("postgres")?,
        runtime_destination_id: "postgres",
        execution: DestinationExecutionSpec {
            uri: postgres.url().to_owned(),
            project_root: root.to_path_buf(),
            target: TargetName::new(target.display_name())?,
        },
        state: DestinationFixtureState::Postgres {
            database_url: postgres.url().to_owned(),
            schema: postgres.schema().to_owned(),
            table: table.to_owned(),
        },
    })
}

#[cfg(test)]
fn quasar_fixture(
    root: &Path,
    table: &str,
    _environment: &ConformanceEnvironment,
) -> Result<DestinationFixture> {
    let quasar_root = root.join(".cdf/quasar");
    Ok(DestinationFixture {
        destination: MatrixDestination::new("quasar")?,
        runtime_destination_id: "quasar",
        execution: DestinationExecutionSpec {
            uri: local_uri("quasar", &quasar_root),
            project_root: root.to_path_buf(),
            target: TargetName::new(table)?,
        },
        state: DestinationFixtureState::Quasar { root: quasar_root },
    })
}

#[cfg(test)]
fn list_relative_files(root: &Path) -> Result<Vec<FileFootprint>> {
    if !root.exists() {
        return Ok(Vec::new());
    }
    let mut files = Vec::new();
    collect_relative_files(root, root, &mut files)?;
    files.sort();
    Ok(files)
}

#[cfg(test)]
fn collect_relative_files(
    root: &Path,
    current: &Path,
    files: &mut Vec<FileFootprint>,
) -> Result<()> {
    for entry in fs::read_dir(current)
        .map_err(|error| CdfError::data(format!("read {}: {error}", current.display())))?
    {
        let entry = entry.map_err(|error| {
            CdfError::data(format!("read entry in {}: {error}", current.display()))
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            CdfError::data(format!("read file type for {}: {error}", path.display()))
        })?;
        if file_type.is_dir() {
            collect_relative_files(root, &path, files)?;
        } else if file_type.is_file() {
            let relative = path.strip_prefix(root).map_err(|error| {
                CdfError::data(format!("relativize {}: {error}", path.display()))
            })?;
            files.push(FileFootprint {
                path: relative.display().to_string(),
                bytes: fs::read(&path)
                    .map_err(|error| CdfError::data(format!("read {}: {error}", path.display())))?,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
fn duckdb_payload(database_path: &Path, target: &str) -> Result<Vec<LogicalRow>> {
    let connection = duckdb::Connection::open(database_path).map_err(|error| {
        CdfError::destination(format!("open DuckDB {}: {error}", database_path.display()))
    })?;
    let table = target.rsplit('.').next().unwrap_or(target);
    let exists: bool = connection
        .query_row(
            "SELECT COUNT(*) > 0 FROM information_schema.tables WHERE table_name = ?",
            [table],
            |row| row.get(0),
        )
        .map_err(|error| {
            CdfError::destination(format!("inspect DuckDB target {target}: {error}"))
        })?;
    if !exists {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT \"id\", \"name\" FROM {} ORDER BY \"_cdf_row_key\"",
        quote_qualified_identifier(target)
    );
    let mut statement = connection.prepare(&sql).map_err(|error| {
        CdfError::destination(format!(
            "prepare DuckDB payload query for {target}: {error}"
        ))
    })?;
    let rows = statement
        .query_map([], |row| {
            Ok(LogicalRow {
                id: row.get(0)?,
                name: row.get(1)?,
            })
        })
        .map_err(|error| {
            CdfError::destination(format!("query DuckDB payload for {target}: {error}"))
        })?;
    rows.map(|row| {
        row.map_err(|error| {
            CdfError::destination(format!("read DuckDB payload row for {target}: {error}"))
        })
    })
    .collect()
}

#[cfg(test)]
fn parquet_payload(root: &Path) -> Result<Vec<LogicalRow>> {
    let mut paths = Vec::new();
    collect_files_with_extension(root, root, "parquet", &mut paths)?;
    paths.sort();
    let mut rows = Vec::new();
    for path in paths {
        let file = fs::File::open(&path)
            .map_err(|error| CdfError::data(format!("open {}: {error}", path.display())))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|error| CdfError::data(format!("open Parquet {}: {error}", path.display())))?
            .build()
            .map_err(|error| CdfError::data(format!("read Parquet {}: {error}", path.display())))?;
        for batch in reader {
            let batch = batch.map_err(|error| {
                CdfError::data(format!("decode Parquet {}: {error}", path.display()))
            })?;
            rows.extend(logical_rows_from_batch(&batch)?);
        }
    }
    Ok(rows)
}

#[cfg(test)]
fn collect_files_with_extension(
    root: &Path,
    current: &Path,
    extension: &str,
    paths: &mut Vec<PathBuf>,
) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(current)
        .map_err(|error| CdfError::data(format!("read {}: {error}", current.display())))?
    {
        let entry = entry.map_err(|error| {
            CdfError::data(format!("read entry in {}: {error}", current.display()))
        })?;
        let path = entry.path();
        let file_type = entry.file_type().map_err(|error| {
            CdfError::data(format!("read file type for {}: {error}", path.display()))
        })?;
        if file_type.is_dir() {
            collect_files_with_extension(root, &path, extension, paths)?;
        } else if file_type.is_file()
            && path.extension().and_then(|value| value.to_str()) == Some(extension)
        {
            paths.push(path);
        }
    }
    Ok(())
}

#[cfg(test)]
pub(super) fn logical_rows_from_batch(batch: &RecordBatch) -> Result<Vec<LogicalRow>> {
    let id_index = batch
        .schema()
        .index_of("id")
        .map_err(|error| CdfError::data(format!("conformance payload misses id: {error}")))?;
    let name_index = batch
        .schema()
        .index_of("name")
        .map_err(|error| CdfError::data(format!("conformance payload misses name: {error}")))?;
    let ids = batch
        .column(id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| CdfError::data("conformance payload id is not int64"))?;
    let names = batch
        .column(name_index)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| CdfError::data("conformance payload name is not utf8"))?;
    Ok((0..batch.num_rows())
        .map(|index| LogicalRow {
            id: ids.value(index),
            name: (!names.is_null(index)).then(|| names.value(index).to_owned()),
        })
        .collect())
}

#[cfg(test)]
fn quote_qualified_identifier(value: &str) -> String {
    value
        .split('.')
        .map(|part| format!("\"{}\"", part.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

#[cfg(test)]
fn postgres_footprint(
    database_url: &str,
    schema: &str,
    table: &str,
    target_name: &str,
) -> Result<DestinationFootprint> {
    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?;
    Ok(DestinationFootprint::Postgres {
        payload_rows: query_postgres_payload(&mut client, schema, table)?,
        loads_rows: query_load_count_if_exists(&mut client, schema, target_name)?,
        state_rows: query_count_if_exists(&mut client, schema, "_cdf_state")?,
    })
}

#[cfg(test)]
fn postgres_payload(database_url: &str, schema: &str, table: &str) -> Result<Vec<LogicalRow>> {
    let mut client = Client::connect(database_url, NoTls)
        .map_err(|error| CdfError::destination(format!("connect to Postgres: {error}")))?;
    query_postgres_payload(&mut client, schema, table)
}

#[cfg(test)]
fn query_postgres_payload(
    client: &mut Client,
    schema: &str,
    table: &str,
) -> Result<Vec<LogicalRow>> {
    if !table_exists(client, schema, table)? {
        return Ok(Vec::new());
    }
    let rows = client
        .query(
            &format!(
                "SELECT \"id\", \"name\" FROM {} ORDER BY \"_cdf_row_key\"",
                qualified_name(schema, table)
            ),
            &[],
        )
        .map_err(|error| {
            CdfError::destination(format!(
                "query Postgres payload for {schema}.{table}: {error}"
            ))
        })?;
    Ok(rows
        .into_iter()
        .map(|row| LogicalRow {
            id: row.get(0),
            name: row.get(1),
        })
        .collect())
}

#[cfg(test)]
fn query_count_if_exists(client: &mut Client, schema: &str, table: &str) -> Result<i64> {
    if !table_exists(client, schema, table)? {
        return Ok(0);
    }
    client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {}",
                qualified_name(schema, table)
            ),
            &[],
        )
        .map(|row| row.get(0))
        .map_err(|error| {
            CdfError::destination(format!(
                "query Postgres row count for {schema}.{table}: {error}"
            ))
        })
}

#[cfg(test)]
fn query_load_count_if_exists(client: &mut Client, schema: &str, target_name: &str) -> Result<i64> {
    if !table_exists(client, schema, "_cdf_loads")? {
        return Ok(0);
    }
    client
        .query_one(
            &format!(
                "SELECT COUNT(*)::bigint FROM {} WHERE \"target\" = $1",
                qualified_name(schema, "_cdf_loads")
            ),
            &[&target_name],
        )
        .map(|row| row.get(0))
        .map_err(|error| {
            CdfError::destination(format!(
                "query Postgres _cdf_loads row count for target {target_name}: {error}"
            ))
        })
}

#[cfg(test)]
fn table_exists(client: &mut Client, schema: &str, table: &str) -> Result<bool> {
    client
        .query_one(
            "SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
            &[&schema, &table],
        )
        .map(|row| row.get(0))
        .map_err(|error| {
            CdfError::destination(format!(
                "inspect Postgres table existence for {schema}.{table}: {error}"
            ))
        })
}

#[test]
fn generic_project_and_cli_runtime_sources_do_not_import_destination_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    assert_no_concrete_destination_imports(
        &root.join("crates/cdf-project/src"),
        &["runtime_tests.rs", "test_destinations.rs", "tests.rs"],
    );
    assert_no_concrete_destination_imports(
        &root.join("crates/cdf-cli/src"),
        &["destination_registry.rs", "doctor_drift.rs", "tests.rs"],
    );
}

#[test]
fn generic_conformance_engines_do_not_branch_on_destination_identity() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut identities = DESTINATIONS
        .iter()
        .flat_map(|entry| [entry.id, entry.runtime_destination_id])
        .collect::<Vec<_>>();
    identities.sort_unstable();
    identities.dedup();
    for relative in [
        "run_matrix/assertions.rs",
        "run_matrix/core.rs",
        "run_matrix/destinations.rs",
        "run_matrix/mod.rs",
        "run_matrix/tests.rs",
        "runtime_chaos/destinations.rs",
        "runtime_chaos/fixture.rs",
        "runtime_chaos/helper.rs",
        "runtime_chaos/mod.rs",
        "runtime_chaos/tests.rs",
    ] {
        let path = root.join(relative);
        let source = fs::read_to_string(&path).unwrap();
        assert!(
            !source.contains("cdf_dest_"),
            "generic conformance engine imports a concrete destination: {}",
            path.display()
        );
        for identity in &identities {
            assert!(
                !source.contains(&format!("\"{identity}\"")),
                "generic conformance engine branches on destination `{identity}`: {}",
                path.display()
            );
        }
    }
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
                    allowed || !source.contains("cdf_dest_"),
                    "generic runtime source imports a concrete destination: {}",
                    path.display()
                );
            }
        }
    }
}
