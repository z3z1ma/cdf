use std::path::Path;

use cdf_kernel::{CdfError, QueryableResource, Result};
use cdf_project::{ProjectRunReport, ProjectRunSource};

use super::{
    MatrixDisposition, RunMatrixCell, SourceArchetype, external_mock_fixture, file_fixture,
    local_postgres::LivePostgres, plan_json, python_fixture, rest_fixture, sql_fixture,
};

type PrepareSource =
    fn(&RunMatrixCell, &Path, Option<&LivePostgres>) -> Result<PreparedMatrixSource>;

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
        archetype: "sql",
        prepare: prepare_sql,
    },
    SourceFixture {
        archetype: "external_mock",
        prepare: prepare_external_mock,
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
    postgres: Option<&LivePostgres>,
) -> Result<PreparedMatrixSource> {
    let fixture = fixture(&cell.source_archetype).ok_or_else(|| {
        CdfError::contract(format!(
            "source archetype `{}` is absent from the conformance fixture catalog",
            cell.source_archetype
        ))
    })?;
    (fixture.prepare)(cell, project_root, postgres)
}

fn prepare_file(
    cell: &RunMatrixCell,
    project_root: &Path,
    _postgres: Option<&LivePostgres>,
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
    _postgres: Option<&LivePostgres>,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        python_fixture::resource(project_root, cell.disposition)?,
        python_fixture::assert_source_position,
    ))
}

fn prepare_rest(
    cell: &RunMatrixCell,
    _project_root: &Path,
    _postgres: Option<&LivePostgres>,
) -> Result<PreparedMatrixSource> {
    let (resource, transport) = rest_fixture::resource(cell.disposition)?;
    Ok(PreparedMatrixSource::new(resource, move |report| {
        rest_fixture::assert_runtime_observed(&transport);
        rest_fixture::assert_source_position(report);
    }))
}

fn prepare_sql(
    cell: &RunMatrixCell,
    _project_root: &Path,
    postgres: Option<&LivePostgres>,
) -> Result<PreparedMatrixSource> {
    let postgres = postgres.ok_or_else(|| {
        CdfError::contract("SQL source conformance requires a live Postgres fixture")
    })?;
    Ok(PreparedMatrixSource::new(
        sql_fixture::resource(cell.clone(), postgres)?,
        sql_fixture::assert_source_position,
    ))
}

fn prepare_external_mock(
    cell: &RunMatrixCell,
    project_root: &Path,
    _postgres: Option<&LivePostgres>,
) -> Result<PreparedMatrixSource> {
    Ok(PreparedMatrixSource::new(
        external_mock_fixture::resource(project_root, cell.disposition)?,
        external_mock_fixture::assert_source_position,
    ))
}
