use std::collections::BTreeMap;

use cdf_package_contract::VerifiedPackageAccess;

use crate::{validate::plan_segment_acks, *};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresSessionSegments {
    pub(crate) expected: BTreeMap<SegmentId, PostgresExpectedSegment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct PostgresExpectedSegment {
    pub(crate) state: StateSegment,
    pub(crate) package_byte_count: u64,
}

pub(crate) fn expected_segments_for_session(
    package: &dyn VerifiedPackageAccess,
    plan: &PostgresLoadPlan,
    request: &DestinationCommitRequest,
) -> Result<PostgresSessionSegments> {
    let manifest_segments = package.identity_segments();
    let plan_hash = plan_package_hash(plan)?;
    if package.package_hash() != plan_hash.as_str() {
        return Err(CdfError::data(format!(
            "Postgres plan package hash {} does not match package {}",
            plan_hash,
            package.package_hash()
        )));
    }
    if request.package_hash.as_str() != package.package_hash() {
        return Err(CdfError::data(format!(
            "Postgres commit request package hash {} does not match package {}",
            request.package_hash,
            package.package_hash()
        )));
    }

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

    let states = request.segments.clone();
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

    Ok(PostgresSessionSegments { expected })
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

fn plan_package_hash(plan: &PostgresLoadPlan) -> Result<PackageHash> {
    PackageHash::new(
        plan.verify
            .parameters
            .get("package_hash")
            .ok_or_else(|| CdfError::internal("verify clause missing package_hash"))?
            .clone(),
    )
}
