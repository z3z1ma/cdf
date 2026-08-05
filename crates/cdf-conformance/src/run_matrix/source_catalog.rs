use std::path::Path;

use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_project::{ProjectRunReport, ProjectRunSource};

use super::{
    MatrixDisposition, RunMatrixCell, SourceArchetype, clickhouse_fixture, file_fixture,
    mongodb_fixture, nebula_task_fixture, plan_json, postgres_fixture, python_fixture,
    rest_fixture, sqlite_fixture,
};
use crate::destination_catalog::ConformanceEnvironment;

type PrepareSource =
    fn(&RunMatrixCell, &Path, &ConformanceEnvironment) -> Result<PreparedMatrixSource>;

struct SourceFixture {
    archetype: &'static str,
    prepare: PrepareSource,
}

pub(crate) struct PreparedMatrixSource {
    resource: crate::source_fixture::ResolvedSourceFixture,
    after_run: Box<dyn Fn(&ProjectRunReport)>,
}

impl PreparedMatrixSource {
    fn new<F>(resource: crate::source_fixture::ResolvedSourceFixture, after_run: F) -> Self
    where
        F: Fn(&ProjectRunReport) + 'static,
    {
        Self {
            resource,
            after_run: Box::new(after_run),
        }
    }

    pub(crate) fn queryable(&self) -> &dyn QueryableResource {
        self.resource.queryable()
    }

    pub(crate) fn engine_plan(
        &self,
        package_id: &str,
        disposition: MatrixDisposition,
        identifier_policy: Option<&cdf_contract::IdentifierPolicy>,
    ) -> Result<cdf_engine::EnginePlan> {
        if self.queryable().descriptor().write_disposition != disposition.to_write_disposition() {
            return Err(CdfError::contract(
                "run-matrix disposition does not match compiled resource",
            ));
        }
        self.resource.bind_plan(plan_json::planned_engine_plan(
            self.queryable(),
            package_id,
            identifier_policy,
        )?)
    }

    pub(crate) fn project_run_source(&self) -> ProjectRunSource<'_> {
        ProjectRunSource::new(self.queryable())
    }

    pub(crate) fn execution(&self) -> &cdf_runtime::ExecutionServices {
        self.resource.execution()
    }

    pub(crate) fn assert_after_run(&self, report: &ProjectRunReport) {
        (self.after_run)(report);
    }
}

const FIXTURES: &[SourceFixture] = &[
    SourceFixture {
        archetype: "file",
        prepare: prepare_file,
    },
    SourceFixture {
        archetype: "python",
        prepare: prepare_python,
    },
    SourceFixture {
        archetype: "rest",
        prepare: prepare_rest,
    },
    SourceFixture {
        archetype: "postgres",
        prepare: prepare_postgres,
    },
    SourceFixture {
        archetype: "clickhouse",
        prepare: prepare_clickhouse,
    },
    SourceFixture {
        archetype: "mongodb",
        prepare: prepare_mongodb,
    },
    SourceFixture {
        archetype: "sqlite",
        prepare: prepare_sqlite,
    },
    SourceFixture {
        archetype: "nebula",
        prepare: prepare_nebula,
    },
];

pub(crate) fn archetypes() -> Vec<SourceArchetype> {
    FIXTURES
        .iter()
        .map(|fixture| {
            SourceArchetype::new(fixture.archetype)
                .expect("registered source fixture archetype is valid")
        })
        .collect()
}

fn fixture(source: &SourceArchetype) -> Option<&'static SourceFixture> {
    FIXTURES
        .iter()
        .find(|fixture| fixture.archetype == source.as_str())
}

pub(crate) fn prepare(
    cell: &RunMatrixCell,
    project_root: &Path,
    environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    let fixture = fixture(&cell.source_archetype).ok_or_else(|| {
        CdfError::contract(format!(
            "source archetype `{}` is absent from the conformance fixture catalog",
            cell.source_archetype
        ))
    })?;
    (fixture.prepare)(cell, project_root, environment)
}

fn prepare_file(
    cell: &RunMatrixCell,
    project_root: &Path,
    _environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    let compiled = file_fixture::resource(project_root, cell.disposition)?;
    let resource = crate::source_fixture::resolve_local_file(&compiled, project_root)?;
    Ok(PreparedMatrixSource::new(
        resource,
        file_fixture::assert_source_position,
    ))
}

fn prepare_python(
    cell: &RunMatrixCell,
    project_root: &Path,
    _environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        python_fixture::resource(project_root, cell.disposition)?,
        python_fixture::assert_source_position,
    ))
}

fn prepare_rest(
    cell: &RunMatrixCell,
    _project_root: &Path,
    _environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    let (resource, transport) = rest_fixture::resource(cell.disposition)?;
    Ok(PreparedMatrixSource::new(resource, move |report| {
        rest_fixture::assert_runtime_observed(&transport);
        rest_fixture::assert_source_position(report);
    }))
}

fn prepare_postgres(
    cell: &RunMatrixCell,
    project_root: &Path,
    environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        postgres_fixture::resource(cell.clone(), project_root, environment.postgres()?)?,
        postgres_fixture::assert_source_position,
    ))
}

fn prepare_clickhouse(
    cell: &RunMatrixCell,
    _project_root: &Path,
    _environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        clickhouse_fixture::resource(cell)?,
        clickhouse_fixture::assert_source_position,
    ))
}

fn prepare_mongodb(
    cell: &RunMatrixCell,
    project_root: &Path,
    _environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        mongodb_fixture::resource(cell, project_root)?,
        mongodb_fixture::assert_source_position,
    ))
}

fn prepare_sqlite(
    cell: &RunMatrixCell,
    project_root: &Path,
    _environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        sqlite_fixture::resource(cell, project_root)?,
        sqlite_fixture::assert_source_position,
    ))
}

fn prepare_nebula(
    cell: &RunMatrixCell,
    project_root: &Path,
    _environment: &ConformanceEnvironment,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        nebula_task_fixture::resource(project_root, cell.disposition)?,
        nebula_task_fixture::assert_source_position,
    ))
}
