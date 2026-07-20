use std::path::Path;

use cdf_kernel::Result;

use super::RunMatrixCell;

pub(crate) use crate::destination_catalog::{
    ConformanceEnvironment, DestinationFixture as MatrixDestinationHandle,
};

pub(crate) fn destination_for_cell(
    cell: &RunMatrixCell,
    root: &Path,
    environment: &ConformanceEnvironment,
) -> Result<MatrixDestinationHandle> {
    crate::destination_catalog::fixture(
        &cell.destination,
        root,
        &target_table_for_cell(cell),
        environment,
    )
}

fn target_table_for_cell(cell: &RunMatrixCell) -> String {
    format!(
        "{}_events_{}",
        cell.source_archetype.as_str(),
        cell.disposition.as_str()
    )
}
