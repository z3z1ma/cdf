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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StratifiedHashSelectionChange {
    pub retained: bool,
    pub evicted_location: Option<String>,
}

#[derive(Clone, Debug)]
struct OrderedSelection {
    candidate: StratifiedHashCandidate,
    score: String,
}

/// Bounded form of the v1 selector for already-canonical candidate streams.
///
/// External task stores know their exact cardinality but must not materialize every task merely
/// to choose a preview sample. This accumulator preserves the exact v1 edge/stratum policy while
/// retaining at most `membership_limit` candidates. Callers must supply strictly increasing
/// canonical locations; the ordinary slice API sorts before delegating here.
#[derive(Clone, Debug)]
pub struct OrderedStratifiedHashV1 {
    resource_id: ResourceId,
    membership_limit: u64,
    candidate_count: u64,
    next_index: u64,
    previous_location: Option<String>,
    strata: Vec<(u64, u64)>,
    selected: Vec<Option<OrderedSelection>>,
}

impl OrderedStratifiedHashV1 {
    pub fn new(
        resource_id: ResourceId,
        membership_limit: u64,
        candidate_count: u64,
    ) -> Result<Self> {
        if membership_limit == 0 {
            return Err(CdfError::contract(
                "stratified selector membership limit must be positive",
            ));
        }
        if candidate_count == 0 {
            return Err(CdfError::data("stratified selector received no candidates"));
        }
        let selected_count = candidate_count.min(membership_limit);
        let selected_len = usize::try_from(selected_count).map_err(|_| {
            CdfError::contract("stratified selector membership exceeds address space")
        })?;
        let mut strata = Vec::new();
        if selected_count > 2 && selected_count < candidate_count {
            let stratum_count = selected_count - 2;
            let interior_count = candidate_count - 2;
            let base_size = interior_count / stratum_count;
            let remainder = interior_count % stratum_count;
            let mut start = 1_u64;
            for stratum_index in 0..stratum_count {
                let size = base_size + u64::from(stratum_index < remainder);
                let end = start
                    .checked_add(size)
                    .ok_or_else(|| CdfError::contract("selector stratum index exceeds u64"))?;
                strata.push((start, end));
                start = end;
            }
        }
        Ok(Self {
            resource_id,
            membership_limit,
            candidate_count,
            next_index: 0,
            previous_location: None,
            strata,
            selected: vec![None; selected_len],
        })
    }

    pub fn push(
        &mut self,
        candidate: StratifiedHashCandidate,
    ) -> Result<StratifiedHashSelectionChange> {
        if self.next_index >= self.candidate_count {
            return Err(CdfError::contract(
                "ordered stratified selector received more candidates than declared",
            ));
        }
        if self
            .previous_location
            .as_deref()
            .is_some_and(|previous| previous >= candidate.canonical_location())
        {
            return Err(CdfError::contract(
                "ordered stratified selector candidates require strictly increasing canonical locations",
            ));
        }
        self.previous_location = Some(candidate.canonical_location().to_owned());
        let slot = self.selection_slot(self.next_index);
        self.next_index += 1;
        let Some(slot) = slot else {
            return Ok(StratifiedHashSelectionChange {
                retained: false,
                evicted_location: None,
            });
        };
        let score = stratified_hash_v1_score(&self.resource_id, &candidate)?;
        let replace = self.selected[slot].as_ref().is_none_or(|current| {
            (
                score.as_str(),
                candidate.canonical_location(),
                candidate.bounded_identity(),
            ) < (
                current.score.as_str(),
                current.candidate.canonical_location(),
                current.candidate.bounded_identity(),
            )
        });
        if !replace {
            return Ok(StratifiedHashSelectionChange {
                retained: false,
                evicted_location: None,
            });
        }
        let evicted_location = self.selected[slot]
            .replace(OrderedSelection { candidate, score })
            .map(|selection| selection.candidate.canonical_location);
        Ok(StratifiedHashSelectionChange {
            retained: true,
            evicted_location,
        })
    }

    pub fn finish(self) -> Result<StratifiedHashPlan> {
        if self.next_index != self.candidate_count || self.selected.iter().any(Option::is_none) {
            return Err(CdfError::data(
                "ordered stratified selector ended before its declared candidate count",
            ));
        }
        let selected = self
            .selected
            .into_iter()
            .map(|selection| {
                let selection = selection.expect("validated ordered selection is complete");
                StratifiedHashSelection {
                    canonical_location: selection.candidate.canonical_location,
                    score_sha256: selection.score,
                    bounded_identity_sha256: format!(
                        "sha256:{}",
                        hex::encode(Sha256::digest(&selection.candidate.bounded_identity))
                    ),
                }
            })
            .collect::<Vec<_>>();
        let interior_strata = self
            .strata
            .into_iter()
            .zip(selected.iter().skip(1))
            .map(
                |((start_index_inclusive, end_index_exclusive), selection)| StratifiedHashStratum {
                    start_index_inclusive,
                    end_index_exclusive,
                    selected_location: selection.canonical_location.clone(),
                },
            )
            .collect();
        Ok(StratifiedHashPlan {
            selector: STRATIFIED_HASH_SELECTOR_V1.to_owned(),
            candidate_count: self.candidate_count,
            membership_limit: self.membership_limit,
            selected,
            interior_strata,
        })
    }

    fn selection_slot(&self, index: u64) -> Option<usize> {
        let selected_count = u64::try_from(self.selected.len()).expect("selection fits u64");
        if selected_count == 1 {
            return Some(0);
        }
        if selected_count == self.candidate_count {
            return usize::try_from(index).ok();
        }
        if index == 0 {
            return Some(0);
        }
        if index + 1 == self.candidate_count {
            return self.selected.len().checked_sub(1);
        }
        self.strata
            .iter()
            .position(|(start, end)| index >= *start && index < *end)
            .map(|stratum| stratum + 1)
    }
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
    let mut ordered =
        OrderedStratifiedHashV1::new(resource_id.clone(), membership_limit, candidate_count)?;
    for candidate in candidates {
        ordered.push(candidate)?;
    }
    ordered.finish()
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

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
    fn ordered_selector_exposes_exact_bounded_retention_changes() {
        let resource_id = ResourceId::new("events.external").unwrap();
        let source = candidates(10_000);
        let expected = plan_stratified_hash_v1(&resource_id, 64, &source).unwrap();
        let mut selector =
            OrderedStratifiedHashV1::new(resource_id, 64, u64::try_from(source.len()).unwrap())
                .unwrap();
        let mut retained = BTreeSet::new();
        for candidate in source {
            let location = candidate.canonical_location().to_owned();
            let change = selector.push(candidate).unwrap();
            if let Some(evicted) = change.evicted_location {
                assert!(retained.remove(&evicted));
            }
            if change.retained {
                retained.insert(location);
            }
            assert!(retained.len() <= 64);
        }
        let actual = selector.finish().unwrap();
        assert_eq!(actual, expected);
        assert_eq!(
            retained,
            actual
                .selected
                .iter()
                .map(|selected| selected.canonical_location.clone())
                .collect()
        );
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
