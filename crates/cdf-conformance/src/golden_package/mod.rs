use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::CdfError;
use cdf_kernel::{
    CHECKPOINT_STATE_VERSION, CheckpointId, CursorPosition, CursorValue, PartitionId, PipelineId,
    ResourceId, Result, SchemaHash, ScopeKey, SegmentId, SourcePosition, StateSegment, TargetName,
    WriteDisposition,
};
use cdf_package::{PackageBuilder, PackageReader};
use cdf_package_contract::{
    DestinationCommitPlanPreimage, PackageManifest, PackageStatus, SegmentEntry, StateDeltaPreimage,
};
use serde::{Deserialize, Serialize};

pub const PREPARED_ORDERS_V1_EXPECTED_JSON: &str =
    include_str!("../../golden/prepared-orders-v1/expected.json");
pub const PREPARED_ORDERS_V1_PACKAGE_ID: &str = "prepared-orders-v1";

#[derive(Clone, Debug)]
pub struct GoldenPackageFixtureSpec {
    pub package_dir: PathBuf,
    pub package_id: String,
    pub status: PackageStatus,
}

impl GoldenPackageFixtureSpec {
    pub fn prepared_orders_v1(package_dir: impl AsRef<Path>) -> Self {
        Self {
            package_dir: package_dir.as_ref().to_path_buf(),
            package_id: PREPARED_ORDERS_V1_PACKAGE_ID.to_owned(),
            status: PackageStatus::Packaged,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GoldenPackageFixture {
    pub package_dir: PathBuf,
    pub evidence: GoldenPackageEvidence,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoldenPackageEvidence {
    pub manifest_version: u16,
    pub package_hash: String,
    pub package_status: String,
    pub signature_signing_input: String,
    pub signature_value: Option<String>,
    pub identity_manifest_version: u16,
    pub identity_layout: Vec<String>,
    pub identity_files: Vec<GoldenIdentityFileEvidence>,
    pub segments: Vec<GoldenSegmentEvidence>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoldenIdentityFileEvidence {
    pub path: String,
    pub byte_count: u64,
    pub sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GoldenSegmentEvidence {
    pub segment_id: String,
    pub path: String,
    pub row_count: u64,
    pub byte_count: u64,
    pub sha256: String,
}

pub fn build_prepared_orders_golden_package(
    spec: GoldenPackageFixtureSpec,
) -> Result<GoldenPackageFixture> {
    let builder = PackageBuilder::create(
        &spec.package_dir,
        spec.package_id,
        cdf_package::PackageBuilderResources::standalone(8 * 1024 * 1024, 64 * 1024 * 1024)?,
    )?;
    builder.update_status(PackageStatus::Extracting)?;
    builder.write_json_artifact(
        "plan/resource_plan.json",
        &BTreeMap::from([("resource", "orders"), ("partition", "p0")]),
    )?;
    builder.write_identity_artifact(
        "plan/execution_plan.txt",
        b"PackageSinkExec: deterministic prepared-orders-v1 fixture\n",
    )?;
    builder.write_json_artifact(
        "plan/validation_program.json",
        &BTreeMap::from([("program", "accept-all")]),
    )?;
    builder.write_json_artifact(
        "schema/observed.arrow.json",
        &BTreeMap::from([("schema_hash", "schema-prepared-orders-v1")]),
    )?;
    builder.write_json_artifact(
        "schema/output.arrow.json",
        &BTreeMap::from([("schema_hash", "schema-prepared-orders-v1")]),
    )?;
    builder.write_json_artifact("schema/diff.json", &BTreeMap::<String, String>::new())?;
    builder.write_stats_artifact("profile.parquet", b"stats-prepared-orders-v1")?;
    builder.write_stats_artifact("quality.parquet", b"quality-prepared-orders-v1")?;
    builder.write_quarantine_artifact("part-000001.parquet", b"quarantine-prepared-orders-v1")?;
    builder.write_lineage_artifact("batches.parquet", b"lineage-prepared-orders-v1")?;
    builder.append_trace_event(&BTreeMap::from([("event", "prepared-orders-v1")]))?;
    let batch = cdf_package_contract::append_package_row_ord(vec![prepared_orders_batch()?], 0)?;
    let segment = builder.write_segment(SegmentId::new("seg-000001")?, 0, &batch)?;
    write_prepared_orders_state_commit_artifacts(&builder, segment)?;
    builder.finish_with_status(spec.status)?;

    let evidence = read_verified_golden_package_evidence(&spec.package_dir)?;
    Ok(GoldenPackageFixture {
        package_dir: spec.package_dir,
        evidence,
    })
}

pub fn prepared_orders_v1_expected_evidence() -> Result<GoldenPackageEvidence> {
    serde_json::from_str(PREPARED_ORDERS_V1_EXPECTED_JSON).map_err(|error| {
        CdfError::data(format!(
            "read prepared-orders-v1 expected evidence: {error}"
        ))
    })
}

pub fn read_verified_golden_package_evidence(
    package_dir: impl AsRef<Path>,
) -> Result<GoldenPackageEvidence> {
    let package_dir = package_dir.as_ref();
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    Ok(evidence_from_manifest(&cdf_package::read_manifest(
        package_dir,
    )?))
}

pub fn assert_verified_package_matches_golden(
    package_dir: impl AsRef<Path>,
    expected: &GoldenPackageEvidence,
) -> Result<GoldenPackageEvidence> {
    let actual = read_verified_golden_package_evidence(package_dir)?;
    let mismatches = compare_golden_package_evidence(expected, &actual);
    if !mismatches.is_empty() {
        return Err(CdfError::data(format!(
            "golden package evidence mismatch:\n{}",
            mismatches.join("\n")
        )));
    }
    Ok(actual)
}

pub fn assert_golden_package_evidence_matches(
    expected: &GoldenPackageEvidence,
    actual: &GoldenPackageEvidence,
) {
    let mismatches = compare_golden_package_evidence(expected, actual);
    assert!(
        mismatches.is_empty(),
        "golden package evidence mismatch:\n{}",
        mismatches.join("\n")
    );
}

pub fn compare_golden_package_evidence(
    expected: &GoldenPackageEvidence,
    actual: &GoldenPackageEvidence,
) -> Vec<String> {
    let mut mismatches = Vec::new();
    compare_value(
        &mut mismatches,
        "manifest version",
        expected.manifest_version,
        actual.manifest_version,
    );
    compare_value(
        &mut mismatches,
        "package hash",
        expected.package_hash.as_str(),
        actual.package_hash.as_str(),
    );
    compare_value(
        &mut mismatches,
        "package lifecycle status",
        expected.package_status.as_str(),
        actual.package_status.as_str(),
    );
    compare_value(
        &mut mismatches,
        "signature signing input",
        expected.signature_signing_input.as_str(),
        actual.signature_signing_input.as_str(),
    );
    compare_value(
        &mut mismatches,
        "signature value",
        expected.signature_value.as_ref(),
        actual.signature_value.as_ref(),
    );
    compare_value(
        &mut mismatches,
        "identity manifest version",
        expected.identity_manifest_version,
        actual.identity_manifest_version,
    );
    compare_vec(
        &mut mismatches,
        "identity layout",
        &expected.identity_layout,
        &actual.identity_layout,
    );
    compare_identity_files(&mut mismatches, expected, actual);
    compare_segments(&mut mismatches, expected, actual);
    mismatches
}

fn evidence_from_manifest(manifest: &PackageManifest) -> GoldenPackageEvidence {
    GoldenPackageEvidence {
        manifest_version: manifest.manifest_version,
        package_hash: manifest.package_hash.clone(),
        package_status: manifest.lifecycle.status.as_str().to_owned(),
        signature_signing_input: manifest.signature.signing_input.clone(),
        signature_value: manifest.signature.value.clone(),
        identity_manifest_version: manifest.identity.manifest_version,
        identity_layout: manifest.identity.layout.clone(),
        identity_files: manifest
            .identity
            .files
            .iter()
            .map(|file| GoldenIdentityFileEvidence {
                path: file.path.clone(),
                byte_count: file.byte_count,
                sha256: file.sha256.clone(),
            })
            .collect(),
        segments: manifest
            .identity
            .segments
            .iter()
            .map(|segment| GoldenSegmentEvidence {
                segment_id: segment.segment_id.as_str().to_owned(),
                path: segment.path.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
                sha256: segment.sha256.clone(),
            })
            .collect(),
    }
}

fn compare_identity_files(
    mismatches: &mut Vec<String>,
    expected: &GoldenPackageEvidence,
    actual: &GoldenPackageEvidence,
) {
    let expected_by_path = expected
        .identity_files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    let actual_by_path = actual
        .identity_files
        .iter()
        .map(|file| (file.path.as_str(), file))
        .collect::<BTreeMap<_, _>>();
    let expected_paths = expected_by_path.keys().copied().collect::<BTreeSet<_>>();
    let actual_paths = actual_by_path.keys().copied().collect::<BTreeSet<_>>();

    for path in expected_paths.difference(&actual_paths) {
        mismatches.push(format!("missing identity file {path}"));
    }
    for path in actual_paths.difference(&expected_paths) {
        mismatches.push(format!("extra identity file {path}"));
    }
    for path in expected_paths.intersection(&actual_paths) {
        let expected = expected_by_path[path];
        let actual = actual_by_path[path];
        compare_value(
            mismatches,
            &format!("identity file {path} byte count"),
            expected.byte_count,
            actual.byte_count,
        );
        compare_value(
            mismatches,
            &format!("identity file {path} sha256"),
            expected.sha256.as_str(),
            actual.sha256.as_str(),
        );
    }
}

fn compare_segments(
    mismatches: &mut Vec<String>,
    expected: &GoldenPackageEvidence,
    actual: &GoldenPackageEvidence,
) {
    let expected_by_id = expected
        .segments
        .iter()
        .map(|segment| (segment.segment_id.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    let actual_by_id = actual
        .segments
        .iter()
        .map(|segment| (segment.segment_id.as_str(), segment))
        .collect::<BTreeMap<_, _>>();
    let expected_ids = expected_by_id.keys().copied().collect::<BTreeSet<_>>();
    let actual_ids = actual_by_id.keys().copied().collect::<BTreeSet<_>>();

    for segment_id in expected_ids.difference(&actual_ids) {
        mismatches.push(format!("missing segment {segment_id}"));
    }
    for segment_id in actual_ids.difference(&expected_ids) {
        mismatches.push(format!("extra segment {segment_id}"));
    }
    for segment_id in expected_ids.intersection(&actual_ids) {
        let expected = expected_by_id[segment_id];
        let actual = actual_by_id[segment_id];
        compare_value(
            mismatches,
            &format!("segment {segment_id} path"),
            expected.path.as_str(),
            actual.path.as_str(),
        );
        compare_value(
            mismatches,
            &format!("segment {segment_id} row count"),
            expected.row_count,
            actual.row_count,
        );
        compare_value(
            mismatches,
            &format!("segment {segment_id} byte count"),
            expected.byte_count,
            actual.byte_count,
        );
        compare_value(
            mismatches,
            &format!("segment {segment_id} sha256"),
            expected.sha256.as_str(),
            actual.sha256.as_str(),
        );
    }
}

fn compare_value<T>(mismatches: &mut Vec<String>, label: &str, expected: T, actual: T)
where
    T: PartialEq + std::fmt::Debug,
{
    if expected != actual {
        mismatches.push(format!(
            "{label} mismatch: expected {expected:?}, actual {actual:?}"
        ));
    }
}

fn compare_vec<T>(mismatches: &mut Vec<String>, label: &str, expected: &[T], actual: &[T])
where
    T: PartialEq + std::fmt::Debug,
{
    if expected != actual {
        mismatches.push(format!(
            "{label} mismatch: expected {expected:?}, actual {actual:?}"
        ));
    }
}

fn prepared_orders_batch() -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
    let name: ArrayRef = Arc::new(StringArray::from(vec![Some("ada"), Some("grace"), None]));
    RecordBatch::try_new(schema, vec![id, name]).map_err(|error| CdfError::data(error.to_string()))
}

fn write_prepared_orders_state_commit_artifacts(
    builder: &PackageBuilder,
    segment: SegmentEntry,
) -> Result<()> {
    let schema_hash = SchemaHash::new("schema-prepared-orders-v1")?;
    let scope = ScopeKey::Partition {
        partition_id: PartitionId::new("p0")?,
    };
    let output_position = SourcePosition::Cursor(CursorPosition {
        version: CHECKPOINT_STATE_VERSION,
        field: "id".to_owned(),
        value: CursorValue::I64(3),
    });
    let segments = vec![StateSegment {
        segment_id: segment.segment_id,
        scope: scope.clone(),
        output_position: output_position.clone(),
        row_count: segment.row_count,
        byte_count: segment.byte_count,
    }];
    let state_delta = StateDeltaPreimage {
        checkpoint_id: CheckpointId::new("checkpoint-prepared-orders-v1")?,
        pipeline_id: PipelineId::new("pipeline-prepared-orders-v1")?,
        resource_id: ResourceId::new("orders")?,
        scope,
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: None,
        input_position: None,
        output_position,
        output_watermark: None,
        partition_watermarks: Vec::new(),
        late_data_carryover: Vec::new(),
        source_continuation: None,
        schema_hash: schema_hash.clone(),
        segments,
    };
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        TargetName::new("orders")?,
        WriteDisposition::Append,
        Vec::new(),
        schema_hash,
    );
    builder.write_input_checkpoint_artifact(&None)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    Ok(())
}

#[cfg(test)]
mod tests;
