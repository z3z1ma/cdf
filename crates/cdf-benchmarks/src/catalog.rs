use serde::{Deserialize, Serialize};

use crate::{BenchResult, bench_error};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureCatalog {
    pub schema_version: u16,
    pub fixtures: Vec<FixtureSpec>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureSpec {
    pub name: String,
    pub rows: usize,
    pub batch_size: usize,
    pub wide_columns: usize,
}

pub fn fixture_catalog() -> BenchResult<FixtureCatalog> {
    serde_json::from_str(include_str!("../fixtures/baseline-fixtures.json")).map_err(Into::into)
}

pub fn fixture_spec(name: &str) -> BenchResult<FixtureSpec> {
    fixture_catalog()?
        .fixtures
        .into_iter()
        .find(|fixture| fixture.name == name)
        .ok_or_else(|| bench_error(format!("fixture spec `{name}` is not declared")))
}

pub(crate) fn validate_spec(spec: &FixtureSpec) -> BenchResult<()> {
    if spec.rows == 0 {
        return Err(bench_error(format!(
            "fixture `{}` must have rows",
            spec.name
        )));
    }
    if spec.batch_size == 0 {
        return Err(bench_error(format!(
            "fixture `{}` must have a positive batch_size",
            spec.name
        )));
    }
    Ok(())
}
