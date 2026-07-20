use std::path::Path;

use cdf_kernel::Result;

use super::{ChaosCrashWindow, ChaosDestination};

pub(crate) use crate::destination_catalog::{
    ConformanceEnvironment, DestinationFixture as ChaosDestinationHandle, DestinationFootprint,
    DestinationPayload,
};

pub(crate) fn destination_for_case(
    destination: &ChaosDestination,
    window: ChaosCrashWindow,
    root: &Path,
    environment: &ConformanceEnvironment,
) -> Result<ChaosDestinationHandle> {
    crate::destination_catalog::fixture(
        &crate::run_matrix::MatrixDestination::new(destination.as_str())?,
        root,
        &target_table(destination, window),
        environment,
    )
}

fn target_table(destination: &ChaosDestination, window: ChaosCrashWindow) -> String {
    format!("chaos_{}_{}", destination.as_str(), window.fixture_suffix())
}
