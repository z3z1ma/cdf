use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{CdfError, ResourceId, Result};

pub const STRATIFIED_HASH_SELECTOR_V1: &str = "stratified-hash-v1";

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct StratifiedHashBoundedIdentity {
    pub size_bytes: Option<u64>,
    pub modified_at_ms: Option<i64>,
    pub value: Option<String>,
    pub strength: StratifiedHashIdentityStrength,
}

impl StratifiedHashBoundedIdentity {
    pub fn canonical_bytes(&self) -> Result<Vec<u8>> {
        let mut value =
            serde_json::to_value(self).map_err(|error| CdfError::internal(error.to_string()))?;
        value.sort_all_objects();
        serde_json::to_vec(&value).map_err(|error| CdfError::internal(error.to_string()))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StratifiedHashIdentityStrength {
    StrongChecksum,
    StableEtag,
    WeakEtag,
    MultipartEtag,
    BoundedObservation,
    Unavailable,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StratifiedHashCandidate {
    canonical_location: String,
    bounded_identity: Vec<u8>,
}

impl StratifiedHashCandidate {
    pub fn new(canonical_location: impl Into<String>, bounded_identity: Vec<u8>) -> Result<Self> {
        let canonical_location = canonical_location.into();
        if canonical_location.is_empty() {
            return Err(CdfError::contract(
                "stratified selector candidate location must not be empty",
            ));
        }
        Ok(Self {
            canonical_location,
            bounded_identity,
        })
    }

    pub fn canonical_location(&self) -> &str {
        &self.canonical_location
    }

    pub fn bounded_identity(&self) -> &[u8] {
        &self.bounded_identity
    }

    pub fn from_bounded_identity(
        canonical_location: impl Into<String>,
        bounded_identity: &StratifiedHashBoundedIdentity,
    ) -> Result<Self> {
        Self::new(canonical_location, bounded_identity.canonical_bytes()?)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StratifiedHashSelection {
    pub canonical_location: String,
    pub score_sha256: String,
    pub bounded_identity_sha256: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StratifiedHashStratum {
    pub start_index_inclusive: u64,
    pub end_index_exclusive: u64,
    pub selected_location: String,
}

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StratifiedHashPlan {
    pub selector: String,
    pub candidate_count: u64,
    pub membership_limit: u64,
    pub selected: Vec<StratifiedHashSelection>,
    pub interior_strata: Vec<StratifiedHashStratum>,
}

impl StratifiedHashPlan {
    pub fn selects(&self, canonical_location: &str) -> bool {
        self.selected
            .iter()
            .any(|item| item.canonical_location == canonical_location)
    }
}

pub fn plan_stratified_hash_v1(
    resource_id: &ResourceId,
    membership_limit: u64,
    candidates: &[StratifiedHashCandidate],
) -> Result<StratifiedHashPlan> {
    if membership_limit == 0 {
        return Err(CdfError::contract(
            "stratified selector membership limit must be positive",
        ));
    }
    if candidates.is_empty() {
        return Err(CdfError::data("stratified selector received no candidates"));
    }
    let mut candidates = candidates.to_vec();
    candidates.sort_by(|left, right| {
        left.canonical_location
            .cmp(&right.canonical_location)
            .then_with(|| left.bounded_identity.cmp(&right.bounded_identity))
    });
    if candidates
        .windows(2)
        .any(|pair| pair[0].canonical_location == pair[1].canonical_location)
    {
        return Err(CdfError::contract(
            "stratified selector candidates require unique canonical locations",
        ));
    }
    let candidate_count = u64::try_from(candidates.len())
        .map_err(|_| CdfError::contract("stratified selector candidate count exceeds u64"))?;
    let selected_count = usize::try_from(candidate_count.min(membership_limit))
        .map_err(|_| CdfError::contract("stratified selector membership exceeds address space"))?;
    let mut selected_indexes = Vec::with_capacity(selected_count);
    let mut interior_strata = Vec::new();
    match selected_count {
        1 => selected_indexes.push(lowest_score_index(
            resource_id,
            &candidates,
            0,
            candidates.len(),
        )?),
        2 => {
            selected_indexes.push(0);
            selected_indexes.push(candidates.len() - 1);
        }
        _ if selected_count == candidates.len() => {
            selected_indexes.extend(0..candidates.len());
        }
        _ => {
            selected_indexes.push(0);
            let stratum_count = selected_count - 2;
            let interior_count = candidates.len() - 2;
            let base_size = interior_count / stratum_count;
            let remainder = interior_count % stratum_count;
            let mut start = 1_usize;
            for stratum_index in 0..stratum_count {
                let size = base_size + usize::from(stratum_index < remainder);
                let end = start + size;
                let selected = lowest_score_index(resource_id, &candidates, start, end)?;
                interior_strata.push(StratifiedHashStratum {
                    start_index_inclusive: u64::try_from(start)
                        .map_err(|_| CdfError::contract("selector stratum index exceeds u64"))?,
                    end_index_exclusive: u64::try_from(end)
                        .map_err(|_| CdfError::contract("selector stratum index exceeds u64"))?,
                    selected_location: candidates[selected].canonical_location.clone(),
                });
                selected_indexes.push(selected);
                start = end;
            }
            selected_indexes.push(candidates.len() - 1);
        }
    }
    selected_indexes.sort_unstable();
    selected_indexes.dedup();
    if selected_indexes.len() != selected_count {
        return Err(CdfError::internal(
            "stratified selector produced duplicate membership",
        ));
    }
    let selected = selected_indexes
        .into_iter()
        .map(|index| {
            let candidate = &candidates[index];
            Ok(StratifiedHashSelection {
                canonical_location: candidate.canonical_location.clone(),
                score_sha256: stratified_hash_v1_score(resource_id, candidate)?,
                bounded_identity_sha256: format!(
                    "sha256:{}",
                    hex::encode(Sha256::digest(&candidate.bounded_identity))
                ),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let unique = selected
        .iter()
        .map(|item| item.canonical_location.as_str())
        .collect::<BTreeSet<_>>();
    if unique.len() != selected.len() {
        return Err(CdfError::internal(
            "stratified selector emitted duplicate locations",
        ));
    }
    Ok(StratifiedHashPlan {
        selector: STRATIFIED_HASH_SELECTOR_V1.to_owned(),
        candidate_count,
        membership_limit,
        selected,
        interior_strata,
    })
}

pub fn stratified_hash_v1_score(
    resource_id: &ResourceId,
    candidate: &StratifiedHashCandidate,
) -> Result<String> {
    let mut hasher = Sha256::new();
    for component in [
        b"cdf-sample:stratified-hash-v1".as_slice(),
        resource_id.as_str().as_bytes(),
        candidate.canonical_location.as_bytes(),
        candidate.bounded_identity.as_slice(),
    ] {
        let length = u64::try_from(component.len())
            .map_err(|_| CdfError::contract("selector score component exceeds u64"))?;
        hasher.update(length.to_be_bytes());
        hasher.update(component);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn lowest_score_index(
    resource_id: &ResourceId,
    candidates: &[StratifiedHashCandidate],
    start: usize,
    end: usize,
) -> Result<usize> {
    let mut scored = (start..end)
        .map(|index| {
            Ok((
                stratified_hash_v1_score(resource_id, &candidates[index])?,
                candidates[index].canonical_location.as_str(),
                candidates[index].bounded_identity.as_slice(),
                index,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    scored.sort();
    scored
        .first()
        .map(|(_, _, _, index)| *index)
        .ok_or_else(|| CdfError::internal("stratified selector received an empty stratum"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidates(count: usize) -> Vec<StratifiedHashCandidate> {
        (0..count)
            .map(|index| {
                StratifiedHashCandidate::new(
                    format!("file://bucket/{index:04}.parquet"),
                    format!("identity-{index}").into_bytes(),
                )
                .unwrap()
            })
            .collect()
    }

    #[test]
    fn selector_is_permutation_independent_and_preserves_edge_policy() {
        let resource_id = ResourceId::new("events.raw").unwrap();
        let source = candidates(19);
        let expected = plan_stratified_hash_v1(&resource_id, 7, &source).unwrap();

        for offset in 0..source.len() {
            let mut permuted = source.clone();
            permuted.rotate_left(offset);
            if offset % 2 == 1 {
                permuted.reverse();
            }
            assert_eq!(
                plan_stratified_hash_v1(&resource_id, 7, &permuted).unwrap(),
                expected
            );
        }
        assert_eq!(expected.selected.len(), 7);
        assert_eq!(expected.interior_strata.len(), 5);
        assert_eq!(
            expected.selected.first().unwrap().canonical_location,
            "file://bucket/0000.parquet"
        );
        assert_eq!(
            expected.selected.last().unwrap().canonical_location,
            "file://bucket/0018.parquet"
        );
    }

    #[test]
    fn selector_bounds_large_membership_before_payload_io() {
        let resource_id = ResourceId::new("events.large").unwrap();
        let source = candidates(10_000);

        let plan = plan_stratified_hash_v1(&resource_id, 64, &source).unwrap();

        assert_eq!(plan.candidate_count, 10_000);
        assert_eq!(plan.membership_limit, 64);
        assert_eq!(plan.selected.len(), 64);
        assert_eq!(plan.interior_strata.len(), 62);
    }

    #[test]
    fn one_candidate_membership_uses_the_lowest_canonical_score() {
        let resource_id = ResourceId::new("events.raw").unwrap();
        let source = candidates(10);
        let expected = source
            .iter()
            .min_by_key(|candidate| stratified_hash_v1_score(&resource_id, candidate).unwrap())
            .unwrap();

        let plan = plan_stratified_hash_v1(&resource_id, 1, &source).unwrap();

        assert_eq!(plan.selected.len(), 1);
        assert_eq!(
            plan.selected[0].canonical_location,
            expected.canonical_location()
        );
    }
}
