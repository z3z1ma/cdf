use std::collections::BTreeMap;

use cdf_kernel::{CdfError, DestinationCommitRequest, Result, SegmentId};
use cdf_package_contract::VerifiedPackageAccess;

use crate::models::{SqliteExpectedSegment, SqliteLoadPlan, SqliteSessionSegments};

pub(crate) fn expected_segments_for_session(
    package: &dyn VerifiedPackageAccess,
    plan: &SqliteLoadPlan,
    request: &DestinationCommitRequest,
) -> Result<SqliteSessionSegments> {
    if package.package_hash() != plan.package_hash.as_str()
        || request.package_hash.as_str() != package.package_hash()
    {
        return Err(CdfError::data(
            "SQLite destination package, plan, and commit hashes differ",
        ));
    }
    let plan_by_id = plan
        .segments
        .iter()
        .map(|segment| (segment.segment_id.clone(), segment))
        .collect::<BTreeMap<_, _>>();
    if plan_by_id.len() != plan.segments.len() {
        return Err(CdfError::data(
            "SQLite destination plan contains duplicate segment identifiers",
        ));
    }
    let request_by_id = request
        .segments
        .iter()
        .map(|segment| (segment.segment_id.clone(), segment))
        .collect::<BTreeMap<_, _>>();
    if request_by_id.len() != request.segments.len() || request_by_id != plan_by_id {
        return Err(CdfError::data(
            "SQLite destination plan segments differ from the commit request",
        ));
    }

    let mut manifest = BTreeMap::new();
    let mut order = Vec::<SegmentId>::new();
    package.for_each_identity_segment(&mut |segment| {
        if manifest
            .insert(segment.segment_id.clone(), segment.clone())
            .is_some()
        {
            return Err(CdfError::data(
                "SQLite destination package manifest contains duplicate segments",
            ));
        }
        order.push(segment.segment_id.clone());
        Ok(())
    })?;
    if manifest.len() != plan_by_id.len() {
        return Err(CdfError::data(
            "SQLite destination package manifest and plan segment counts differ",
        ));
    }
    let mut expected = BTreeMap::new();
    for segment_id in order {
        let manifest_segment = manifest
            .get(&segment_id)
            .ok_or_else(|| CdfError::internal("SQLite package manifest ordering lost a segment"))?;
        let state = plan_by_id.get(&segment_id).ok_or_else(|| {
            CdfError::data(format!(
                "SQLite destination plan omits package segment {segment_id}"
            ))
        })?;
        if state.row_count != manifest_segment.row_count {
            return Err(CdfError::data(format!(
                "SQLite destination segment {segment_id} row count differs from package manifest"
            )));
        }
        expected.insert(
            segment_id,
            SqliteExpectedSegment {
                state: (*state).clone(),
                package_byte_count: manifest_segment.byte_count,
                package_row_ord_start: manifest_segment.package_row_ord_start,
            },
        );
    }
    Ok(SqliteSessionSegments { expected })
}
