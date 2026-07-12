use std::{collections::BTreeMap, path::Path};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use cdf_kernel::{CursorPosition, CursorValue, ScopeKey, SourcePosition};
use cdf_package::{PackageReader, SegmentEntry};

use crate::{rows::*, validate::plan_segment_acks, *};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresPackageData {
    pub(crate) segments: Vec<PostgresLoadedSegment>,
    pub(crate) rows: Vec<PostgresStageRow>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresLoadedSegment {
    pub(crate) entry: SegmentEntry,
    pub(crate) row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresSessionSegments {
    pub(crate) expected: BTreeMap<SegmentId, PostgresExpectedSegment>,
    pub(crate) order: Vec<SegmentId>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresExpectedSegment {
    pub(crate) state: StateSegment,
    pub(crate) package_byte_count: u64,
}

pub(crate) fn read_commit_segments_for_plan(
    package_dir: &Path,
    plan: &PostgresLoadPlan,
) -> Result<Vec<CommitSegment>> {
    let session_segments = expected_segments_for_session(package_dir, plan, None)?;
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    let mut payloads = BTreeMap::new();
    for (entry, batches) in reader.read_all_segments()? {
        if payloads.insert(entry.segment_id.clone(), batches).is_some() {
            return Err(CdfError::data(format!(
                "package manifest contains duplicate segment {}",
                entry.segment_id
            )));
        }
    }

    let mut segments = Vec::with_capacity(session_segments.order.len());
    for segment_id in &session_segments.order {
        let expected = session_segments.expected.get(segment_id).ok_or_else(|| {
            CdfError::internal(format!(
                "Postgres expected segment {} is missing from session map",
                segment_id.as_str()
            ))
        })?;
        let batches = payloads.remove(segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "package segment {} is missing its payload",
                segment_id.as_str()
            ))
        })?;
        segments.push(CommitSegment::new(
            expected.state.clone(),
            expected.package_byte_count,
            batches,
        ));
    }

    Ok(segments)
}

pub(crate) fn expected_segments_for_session(
    package_dir: &Path,
    plan: &PostgresLoadPlan,
    request: Option<&DestinationCommitRequest>,
) -> Result<PostgresSessionSegments> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    let replay = reader.replay_view()?;
    let plan_hash = plan_package_hash(plan)?;
    if replay.package_hash != plan_hash {
        return Err(CdfError::data(format!(
            "Postgres plan package hash {} does not match package {}",
            plan_hash, replay.package_hash
        )));
    }
    if let Some(request) = request
        && request.package_hash != replay.package_hash
    {
        return Err(CdfError::data(format!(
            "Postgres commit request package hash {} does not match package {}",
            request.package_hash, replay.package_hash
        )));
    }

    let manifest_segments = &reader.manifest().identity.segments;
    let plan_by_id = plan_segment_map(plan)?;
    let mut manifest_by_id = BTreeMap::new();
    let mut order = Vec::with_capacity(manifest_segments.len());
    for segment in manifest_segments {
        if manifest_by_id
            .insert(segment.segment_id.clone(), segment)
            .is_some()
        {
            return Err(CdfError::data(format!(
                "package manifest contains duplicate segment {}",
                segment.segment_id
            )));
        }
        order.push(segment.segment_id.clone());
    }

    let states = state_segments_for_session(plan, request);
    let mut state_by_id = BTreeMap::new();
    for state in states {
        if state_by_id
            .insert(state.segment_id.clone(), state)
            .is_some()
        {
            return Err(CdfError::data(
                "destination commit request contains duplicate segment",
            ));
        }
    }

    for segment_id in plan_by_id.keys() {
        if !manifest_by_id.contains_key(segment_id) {
            return Err(CdfError::data(format!(
                "Postgres plan segment {} is not present in the package manifest",
                segment_id.as_str()
            )));
        }
    }
    for segment_id in state_by_id.keys() {
        if !manifest_by_id.contains_key(segment_id) {
            return Err(CdfError::data(format!(
                "destination commit request segment {} is not present in the package manifest",
                segment_id.as_str()
            )));
        }
    }

    let mut expected = BTreeMap::new();
    for segment_id in &order {
        let manifest_segment = manifest_by_id.get(segment_id).ok_or_else(|| {
            CdfError::internal(format!(
                "Postgres manifest segment {} is missing from manifest map",
                segment_id.as_str()
            ))
        })?;
        let ack = plan_by_id.get(segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "Postgres plan does not cover package segment {}",
                segment_id.as_str()
            ))
        })?;
        let state = state_by_id.get(segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "package manifest segment {} is missing from destination commit request",
                segment_id.as_str()
            ))
        })?;
        if ack.row_count != state.row_count || ack.byte_count != state.byte_count {
            return Err(CdfError::data(format!(
                "Postgres plan segment {} has {} rows/{} bytes but commit request has {} rows/{} bytes",
                segment_id.as_str(),
                ack.row_count,
                ack.byte_count,
                state.row_count,
                state.byte_count
            )));
        }
        if state.row_count != manifest_segment.row_count {
            return Err(CdfError::data(format!(
                "destination commit request segment {} has {} rows but package manifest has {} rows",
                segment_id.as_str(),
                state.row_count,
                manifest_segment.row_count
            )));
        }
        expected.insert(
            segment_id.clone(),
            PostgresExpectedSegment {
                state: state.clone(),
                package_byte_count: manifest_segment.byte_count,
            },
        );
    }

    Ok(PostgresSessionSegments { expected, order })
}

pub(crate) fn package_data_from_commit_segments(
    segments: Vec<CommitSegment>,
    plan: &PostgresLoadPlan,
) -> Result<PostgresPackageData> {
    let segments = segments
        .into_iter()
        .map(|segment| {
            (
                SegmentEntry {
                    segment_id: segment.state.segment_id,
                    path: String::new(),
                    row_count: segment.state.row_count,
                    byte_count: segment.package_byte_count,
                    sha256: String::new(),
                },
                segment.batches,
            )
        })
        .collect::<Vec<_>>();
    package_data_from_segments(segments, plan)
}

fn package_data_from_segments(
    segments: Vec<(SegmentEntry, Vec<RecordBatch>)>,
    plan: &PostgresLoadPlan,
) -> Result<PostgresPackageData> {
    if segments.is_empty() {
        return Ok(PostgresPackageData {
            segments: Vec::new(),
            rows: Vec::new(),
        });
    }

    let schema = first_schema(&segments)?;
    validate_schema_matches_plan(schema.as_ref(), &plan.columns)?;

    let mut loaded_segments = Vec::with_capacity(segments.len());
    let mut rows = Vec::new();
    for (entry, batches) in segments {
        let mut row_count = 0_u64;
        for batch in batches {
            if batch.schema().as_ref() != schema.as_ref() {
                return Err(CdfError::data(
                    "Postgres destination requires all package segments to share one schema",
                ));
            }
            for row in 0..batch.num_rows() {
                rows.push(PostgresStageRow {
                    values: batch_row_values(&batch, row)?,
                    segment_id: entry.segment_id.as_str().to_owned(),
                    row_index: row_count,
                });
                row_count += 1;
            }
        }
        if row_count != entry.row_count {
            return Err(CdfError::data(format!(
                "package segment {} manifest row count {} differs from package data {}",
                entry.segment_id.as_str(),
                entry.row_count,
                row_count
            )));
        }
        loaded_segments.push(PostgresLoadedSegment { entry, row_count });
    }

    Ok(PostgresPackageData {
        segments: loaded_segments,
        rows,
    })
}

pub(crate) fn record_package_receipt_once(package_dir: &Path, receipt: &Receipt) -> Result<bool> {
    let reader = PackageReader::open(package_dir)?;
    let receipts = reader.receipts()?;
    if receipts
        .iter()
        .any(|existing| existing.receipt_id == receipt.receipt_id)
    {
        return Ok(false);
    }
    reader.append_receipt(receipt.clone())?;
    Ok(true)
}

fn plan_segment_map(plan: &PostgresLoadPlan) -> Result<BTreeMap<SegmentId, SegmentAck>> {
    let mut by_id = BTreeMap::new();
    for ack in plan_segment_acks(plan) {
        if by_id.insert(ack.segment_id.clone(), ack).is_some() {
            return Err(CdfError::data(
                "Postgres plan contains duplicate segment expectations",
            ));
        }
    }
    Ok(by_id)
}

fn state_segments_for_session(
    plan: &PostgresLoadPlan,
    request: Option<&DestinationCommitRequest>,
) -> Vec<StateSegment> {
    if let Some(request) = request {
        return request.segments.clone();
    }
    if let Some(delta) = &plan.state_delta {
        return delta.segments.clone();
    }
    plan_segment_acks(plan)
        .into_iter()
        .map(synthetic_state_segment)
        .collect()
}

fn synthetic_state_segment(ack: SegmentAck) -> StateSegment {
    let position_value = ack.segment_id.as_str().to_owned();
    StateSegment {
        segment_id: ack.segment_id,
        scope: ScopeKey::Resource,
        output_position: SourcePosition::Cursor(CursorPosition {
            version: 1,
            field: "_cdf_segment".to_owned(),
            value: CursorValue::String(position_value),
        }),
        row_count: ack.row_count,
        byte_count: ack.byte_count,
    }
}

fn first_schema(segments: &[(SegmentEntry, Vec<RecordBatch>)]) -> Result<SchemaRef> {
    segments
        .iter()
        .flat_map(|(_, batches)| batches.iter())
        .next()
        .map(RecordBatch::schema)
        .ok_or_else(|| CdfError::data("Postgres destination found no record batches in package"))
}

fn plan_package_hash(plan: &PostgresLoadPlan) -> Result<PackageHash> {
    PackageHash::new(
        plan.verify
            .parameters
            .get("package_hash")
            .ok_or_else(|| CdfError::internal("verify clause missing package_hash"))?
            .clone(),
    )
}
