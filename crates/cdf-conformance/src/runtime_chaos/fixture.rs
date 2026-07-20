use std::path::{Path, PathBuf};

use cdf_kernel::{CdfError, Receipt, Result, TargetName, WriteDisposition};
use cdf_package_contract::{PackageReplayInputs, PackageStatus};
use cdf_project::{
    PackageArtifactRecoveryRequest, PackageArtifactReplayRequest, PackageReplayReport,
    recover_package_from_artifacts, replay_package_from_artifacts,
};
use cdf_state_sqlite::SqliteCheckpointStore;

use crate::package_replay::{
    PackageReader, PreparedPackageFixtureSpec, build_prepared_package_fixture,
};

use super::{
    ChaosCrashWindow, ChaosDestination, ExecutedChaosCase, destinations::ChaosDestinationHandle,
};

pub(crate) struct ChaosPackageFixture {
    pub(crate) package_dir: PathBuf,
    pub(crate) package_id: String,
    pub(crate) inputs: PackageReplayInputs,
}

impl ChaosPackageFixture {
    pub(crate) fn build(
        root: &Path,
        destination: ChaosDestination,
        window: ChaosCrashWindow,
        target: TargetName,
    ) -> Result<Self> {
        let package_id = format!("runtime-chaos-{}-{}", destination.as_str(), window.as_str());
        let package_dir = root.join(".cdf/packages").join(&package_id);
        let mut spec = PreparedPackageFixtureSpec::new(&package_dir, package_id.clone())?;
        spec.target = target;
        spec.disposition = WriteDisposition::Append;
        build_prepared_package_fixture(spec)?;
        let inputs = PackageReader::open(&package_dir)?.replay_inputs()?;
        Ok(Self {
            package_dir,
            package_id,
            inputs,
        })
    }
}

pub(crate) fn recover_after_crash(
    destination: &ChaosDestinationHandle,
    store: &SqliteCheckpointStore,
    fixture: &ChaosPackageFixture,
    window: ChaosCrashWindow,
    receipt: Option<Receipt>,
) -> Result<(PackageReplayReport, String)> {
    match receipt {
        Some(receipt) => Ok((
            recover_package_from_artifacts(PackageArtifactRecoveryRequest {
                package_dir: fixture.package_dir.clone(),
                destination: destination.resolved()?,
                checkpoint_store: store,
                receipt,
                after_receipt_verified: None,
            })?,
            "recover_package_from_artifacts_with_supplied_durable_receipt".to_owned(),
        )),
        None => Ok((
            replay_package_from_artifacts(PackageArtifactReplayRequest {
                package_dir: fixture.package_dir.clone(),
                destination: destination.resolved()?,
                checkpoint_store: store,
                after_receipt_verified: None,
            })?,
            match window {
                ChaosCrashWindow::CheckpointProposedBeforeDestinationWrite => {
                    "replay_package_from_artifacts_reusing_pinned_proposal".to_owned()
                }
                _ => "replay_package_from_artifacts_without_source_contact".to_owned(),
            },
        )),
    }
}

pub(crate) fn durable_receipt(package_dir: &Path) -> Result<Option<Receipt>> {
    Ok(PackageReader::open(package_dir)?
        .receipts()?
        .into_iter()
        .next())
}

pub(crate) fn package_status(package_dir: &Path) -> Result<PackageStatus> {
    Ok(PackageReader::open(package_dir)?
        .manifest()
        .lifecycle
        .status
        .clone())
}

pub(crate) fn assert_checkpoint_not_ahead_of_durable_data(
    destination: &ChaosDestinationHandle,
    report: &PackageReplayReport,
) -> Result<()> {
    destination.assert_receipt_identity(&report.receipt)?;
    destination.verify_trait_receipt(&report.receipt)?;
    if report.checkpoint.receipt.as_ref() != Some(&report.receipt) {
        return Err(CdfError::contract(
            "runtime chaos recovery checkpoint does not carry the durable receipt",
        ));
    }
    if report.checkpoint.delta.package_hash != report.receipt.package_hash {
        return Err(CdfError::contract(
            "runtime chaos recovery checkpoint package hash is ahead of durable receipt",
        ));
    }
    Ok(())
}

pub(crate) fn assert_duplicate_retry_no_second_write(
    destination: &ChaosDestinationHandle,
    fixture: &ChaosPackageFixture,
    report: &PackageReplayReport,
    root: &Path,
) -> Result<(bool, String)> {
    let before = destination.footprint()?;
    let duplicate_store = SqliteCheckpointStore::open(root.join(".cdf/duplicate-retry.sqlite"))?;
    let duplicate = replay_package_from_artifacts(PackageArtifactReplayRequest {
        package_dir: fixture.package_dir.clone(),
        destination: destination.resolved()?,
        checkpoint_store: &duplicate_store,
        after_receipt_verified: None,
    })?;
    let after = destination.footprint()?;
    if before != after {
        return Err(CdfError::destination(
            "runtime chaos duplicate retry mutated the destination footprint",
        ));
    }
    if duplicate.receipt != report.receipt {
        return Err(CdfError::destination(
            "runtime chaos duplicate retry did not return the stable durable receipt",
        ));
    }
    destination.assert_receipt_identity(&duplicate.receipt)?;
    destination.verify_trait_receipt(&duplicate.receipt)?;
    Ok((
        true,
        destination.duplicate_retry_behavior(duplicate.receipt_source),
    ))
}

pub(crate) struct ExecutedCaseParts<'a> {
    pub(crate) destination: ChaosDestination,
    pub(crate) window: ChaosCrashWindow,
    pub(crate) fixture: &'a ChaosPackageFixture,
    pub(crate) report: &'a PackageReplayReport,
    pub(crate) recovery_path: String,
    pub(crate) crash_left_durable_receipt: bool,
    pub(crate) crash_left_checkpoint_head: bool,
    pub(crate) crash_left_destination_write: bool,
    pub(crate) receipt_recovery_avoided_second_destination_write: bool,
    pub(crate) duplicate_retry_no_second_destination_write: bool,
    pub(crate) duplicate_retry_behavior: String,
}

pub(crate) fn executed_case(parts: ExecutedCaseParts<'_>) -> ExecutedChaosCase {
    let ExecutedCaseParts {
        destination,
        window,
        fixture,
        report,
        recovery_path,
        crash_left_durable_receipt,
        crash_left_checkpoint_head,
        crash_left_destination_write,
        receipt_recovery_avoided_second_destination_write,
        duplicate_retry_no_second_destination_write,
        duplicate_retry_behavior,
    } = parts;
    ExecutedChaosCase {
        destination,
        crash_window: window,
        package_id: fixture.package_id.clone(),
        crashed_checkpoint_id: fixture.inputs.state_delta.checkpoint_id.as_str().to_owned(),
        recovery_checkpoint_id: report.checkpoint.delta.checkpoint_id.as_str().to_owned(),
        recovery_receipt_id: report.receipt.receipt_id.as_str().to_owned(),
        crash_left_durable_receipt,
        crash_left_checkpoint_head,
        crash_left_destination_write,
        recovery_path,
        recovery_without_source_contact: true,
        checkpoint_not_ahead_of_durable_data: true,
        receipt_recovery_avoided_second_destination_write,
        duplicate_retry_no_second_destination_write,
        duplicate_retry_behavior,
    }
}
