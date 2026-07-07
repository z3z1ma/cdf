use std::{collections::BTreeMap, path::Path};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
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

pub(crate) fn load_package_for_plan(
    package_dir: &Path,
    plan: &PostgresLoadPlan,
) -> Result<PostgresPackageData> {
    let reader = PackageReader::open(package_dir)?;
    reader.verify()?;
    let replay = reader.replay_view()?;
    if replay.package_hash != plan_package_hash(plan)? {
        return Err(CdfError::data(format!(
            "Postgres plan package hash {} does not match package {}",
            plan_package_hash(plan)?,
            replay.package_hash
        )));
    }

    let segments = reader.read_all_segments()?;
    if segments.is_empty() {
        return Err(CdfError::data(
            "Postgres destination requires at least one package segment",
        ));
    }
    validate_segment_coverage(plan, &segments)?;

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

fn validate_segment_coverage(
    plan: &PostgresLoadPlan,
    segments: &[(SegmentEntry, Vec<RecordBatch>)],
) -> Result<()> {
    let requested = plan_segment_acks(plan);
    if requested.len() != segments.len() {
        return Err(CdfError::data(format!(
            "Postgres plan covers {} segment(s) but package contains {} segment(s)",
            requested.len(),
            segments.len()
        )));
    }

    let requested_by_id = requested
        .iter()
        .map(|ack| (ack.segment_id.as_str(), ack))
        .collect::<BTreeMap<_, _>>();
    for (entry, _) in segments {
        let Some(ack) = requested_by_id.get(entry.segment_id.as_str()) else {
            return Err(CdfError::data(format!(
                "Postgres plan does not cover package segment {}",
                entry.segment_id.as_str()
            )));
        };
        if ack.row_count != entry.row_count || ack.byte_count != entry.byte_count {
            return Err(CdfError::data(format!(
                "Postgres plan segment {} has {} rows/{} bytes but package manifest has {} rows/{} bytes",
                entry.segment_id.as_str(),
                ack.row_count,
                ack.byte_count,
                entry.row_count,
                entry.byte_count
            )));
        }
    }
    Ok(())
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
