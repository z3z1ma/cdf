use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    CdfError, CommittedContentRootId, ContentClaimAttemptId, ContentDigestAlgorithm,
    ContentDigestValue, ContentObjectKey, ContentProviderGeneration, ContentPublicationClaimId,
    ContentReclamationCandidateSource, ContentReclamationReservationId, ContentRootShardRef,
    ContentStoreNamespace, DestinationId, FencingToken, LeaseAuthorityDomainId, Result, ScopeLease,
    TargetName,
};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ContentDigest {
    pub algorithm: ContentDigestAlgorithm,
    pub value: ContentDigestValue,
}

impl ContentDigest {
    pub fn new(algorithm: ContentDigestAlgorithm, value: ContentDigestValue) -> Result<Self> {
        let digest = Self { algorithm, value };
        digest.validate()?;
        Ok(digest)
    }

    pub fn validate(&self) -> Result<()> {
        if self.algorithm.as_str().trim().is_empty() || self.value.as_str().trim().is_empty() {
            return Err(CdfError::contract(
                "content digest algorithm and value must be non-empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ImmutableContentIdentity {
    pub store_namespace: ContentStoreNamespace,
    pub object_key: ContentObjectKey,
    pub byte_count: u64,
    pub digest: ContentDigest,
    pub provider_generation: Option<ContentProviderGeneration>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub grouping: BTreeMap<String, String>,
}

impl ImmutableContentIdentity {
    pub fn new(
        store_namespace: ContentStoreNamespace,
        object_key: ContentObjectKey,
        byte_count: u64,
        digest: ContentDigest,
        provider_generation: Option<ContentProviderGeneration>,
    ) -> Result<Self> {
        let identity = Self {
            store_namespace,
            object_key,
            byte_count,
            digest,
            provider_generation,
            grouping: BTreeMap::new(),
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn validate(&self) -> Result<()> {
        self.digest.validate()?;
        for (key, value) in &self.grouping {
            if key.trim().is_empty() || value.trim().is_empty() {
                return Err(CdfError::contract(
                    "immutable content grouping metadata cannot contain empty keys or values",
                ));
            }
        }
        Ok(())
    }

    /// Whether two observations name the same immutable content address and bytes.
    /// Provider generation is deliberately excluded because it is learned only after publication.
    pub fn same_content_object(&self, other: &Self) -> bool {
        self.store_namespace == other.store_namespace
            && self.object_key == other.object_key
            && self.byte_count == other.byte_count
            && self.digest == other.digest
            && self.grouping == other.grouping
    }

    pub fn with_provider_generation(
        mut self,
        provider_generation: ContentProviderGeneration,
    ) -> Self {
        self.provider_generation = Some(provider_generation);
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentPublicationClaimState {
    Planned,
    Published,
    Settled,
    Released,
}

impl ContentPublicationClaimState {
    pub fn is_live(self) -> bool {
        matches!(self, Self::Planned | Self::Published)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentPublicationClaim {
    pub destination_id: DestinationId,
    pub target: TargetName,
    pub attempt_id: ContentClaimAttemptId,
    pub lease_authority_domain_id: LeaseAuthorityDomainId,
    pub lease: ScopeLease,
    pub content: ImmutableContentIdentity,
    pub claim_id: ContentPublicationClaimId,
    pub claim_generation: u64,
    pub state: ContentPublicationClaimState,
}

impl ContentPublicationClaim {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        destination_id: DestinationId,
        target: TargetName,
        attempt_id: ContentClaimAttemptId,
        lease_authority_domain_id: LeaseAuthorityDomainId,
        lease: ScopeLease,
        content: ImmutableContentIdentity,
        claim_id: ContentPublicationClaimId,
        claim_generation: u64,
        state: ContentPublicationClaimState,
    ) -> Result<Self> {
        let claim = Self {
            destination_id,
            target,
            attempt_id,
            lease_authority_domain_id,
            lease,
            content,
            claim_id,
            claim_generation,
            state,
        };
        claim.validate()?;
        Ok(claim)
    }

    pub fn validate(&self) -> Result<()> {
        if self.claim_generation == 0 {
            return Err(CdfError::contract(
                "content publication claim generation must be positive",
            ));
        }
        if self.lease.expires_at_ms <= self.lease.acquired_at_ms {
            return Err(CdfError::contract(
                "content publication claim lease expiry must follow acquisition",
            ));
        }
        self.content.validate()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CommittedContentMembership {
    Inline {
        identities: Vec<ImmutableContentIdentity>,
    },
    Shard {
        shard_reference: ContentRootShardRef,
        member_count: u64,
    },
}

impl CommittedContentMembership {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Inline { identities } => {
                let mut seen = BTreeSet::new();
                for identity in identities {
                    identity.validate()?;
                    if !seen.insert(identity) {
                        return Err(CdfError::contract(
                            "committed content root contains duplicate inline content identity",
                        ));
                    }
                }
            }
            Self::Shard { member_count, .. } if *member_count == 0 => {
                return Err(CdfError::contract(
                    "committed content root shard must declare at least one member",
                ));
            }
            Self::Shard { .. } => {}
        }
        Ok(())
    }

    pub fn inline_references(&self, identity: &ImmutableContentIdentity) -> bool {
        match self {
            Self::Inline { identities } => identities.iter().any(|candidate| candidate == identity),
            Self::Shard { .. } => false,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedContentRoot {
    pub destination_id: DestinationId,
    pub target: TargetName,
    pub root_id: CommittedContentRootId,
    pub root_generation: u64,
    pub retained_until_ms: Option<i64>,
    pub membership: CommittedContentMembership,
}

impl CommittedContentRoot {
    pub fn validate(&self) -> Result<()> {
        if self.root_generation == 0 {
            return Err(CdfError::contract(
                "committed content root generation must be positive",
            ));
        }
        self.membership.validate()
    }

    pub fn inline_references(&self, identity: &ImmutableContentIdentity) -> bool {
        self.membership.inline_references(identity)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentRootState {
    Prepared,
    Committed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentRootIntent {
    pub root: CommittedContentRoot,
    pub claim_ids: Vec<ContentPublicationClaimId>,
    pub state: ContentRootState,
}

impl ContentRootIntent {
    pub fn validate(&self) -> Result<()> {
        self.root.validate()?;
        reject_duplicates(
            self.claim_ids.iter(),
            "content root intent publication claim ids",
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentReclamationCandidate {
    pub content: ImmutableContentIdentity,
    pub observed_provider_generation: ContentProviderGeneration,
    pub candidate_source: ContentReclamationCandidateSource,
    pub consulted_claims: Vec<ContentPublicationClaimId>,
    pub consulted_roots: Vec<CommittedContentRootId>,
}

impl ContentReclamationCandidate {
    pub fn validate(&self) -> Result<()> {
        self.content.validate()?;
        if self.content.provider_generation.as_ref() != Some(&self.observed_provider_generation) {
            return Err(CdfError::contract(
                "content reclamation candidate must bind the exact provider generation it observed",
            ));
        }
        reject_duplicates(
            self.consulted_claims.iter(),
            "content reclamation candidate consulted claim ids",
        )?;
        reject_duplicates(
            self.consulted_roots.iter(),
            "content reclamation candidate consulted root ids",
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpiredContentPublicationClaim {
    pub claim_id: ContentPublicationClaimId,
    pub claim_generation: u64,
    pub lease_authority_domain_id: LeaseAuthorityDomainId,
    pub lease: ScopeLease,
    pub cleanup_fencing_token: FencingToken,
    pub proven_at_ms: i64,
}

impl ExpiredContentPublicationClaim {
    pub fn matches_claim(&self, claim: &ContentPublicationClaim) -> bool {
        self.claim_id == claim.claim_id
            && self.claim_generation == claim.claim_generation
            && self.lease_authority_domain_id == claim.lease_authority_domain_id
            && self.lease == claim.lease
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommittedContentRootCheck {
    pub root_id: CommittedContentRootId,
    pub root_generation: u64,
    pub references_candidate: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentReclamationProof {
    pub candidate: ContentReclamationCandidate,
    pub expired_claims: Vec<ExpiredContentPublicationClaim>,
    pub checked_roots: Vec<CommittedContentRootCheck>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentReclamationSnapshot {
    pub candidate: ContentReclamationCandidate,
    pub same_content_claims: Vec<ContentPublicationClaim>,
    pub checked_roots: Vec<CommittedContentRootCheck>,
}

impl ContentReclamationSnapshot {
    pub fn validate(&self) -> Result<()> {
        self.candidate.validate()?;
        let claims = self
            .same_content_claims
            .iter()
            .map(|claim| &claim.claim_id)
            .collect::<BTreeSet<_>>();
        let consulted_claims = self
            .candidate
            .consulted_claims
            .iter()
            .collect::<BTreeSet<_>>();
        if claims != consulted_claims {
            return Err(CdfError::contract(
                "content reclamation snapshot claims differ from its consulted index positions",
            ));
        }
        let roots = self
            .checked_roots
            .iter()
            .map(|root| &root.root_id)
            .collect::<BTreeSet<_>>();
        let consulted_roots = self
            .candidate
            .consulted_roots
            .iter()
            .collect::<BTreeSet<_>>();
        if roots != consulted_roots {
            return Err(CdfError::contract(
                "content reclamation snapshot roots differ from its consulted index positions",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentReclamationReservation {
    pub reservation_id: ContentReclamationReservationId,
    pub reservation_generation: u64,
    pub proof: ContentReclamationProof,
}

impl ContentReclamationReservation {
    pub fn validate(&self) -> Result<()> {
        if self.reservation_generation == 0 {
            return Err(CdfError::contract(
                "content reclamation reservation generation must be positive",
            ));
        }
        self.proof.candidate.validate()
    }
}

/// Durable, bounded index for destination-neutral immutable-content lifetime authority.
/// Implementations must make each method atomic with respect to other methods on the same store.
pub trait ContentReachabilityStore: Send + Sync {
    fn install_claim(&self, claim: ContentPublicationClaim) -> Result<ContentPublicationClaim>;

    fn publish_claim(
        &self,
        claim_id: &ContentPublicationClaimId,
        expected_generation: u64,
        content: ImmutableContentIdentity,
    ) -> Result<ContentPublicationClaim>;

    fn release_claim(
        &self,
        claim_id: &ContentPublicationClaimId,
        expected_generation: u64,
    ) -> Result<()>;

    fn prepare_root(&self, intent: ContentRootIntent) -> Result<ContentRootIntent>;

    fn commit_root(
        &self,
        root_id: &CommittedContentRootId,
        expected_generation: u64,
    ) -> Result<CommittedContentRoot>;

    fn abort_root(&self, root_id: &CommittedContentRootId, expected_generation: u64) -> Result<()>;

    fn release_root(
        &self,
        root_id: &CommittedContentRootId,
        expected_generation: u64,
    ) -> Result<()>;

    fn reclamation_candidates(&self, limit: u32) -> Result<Vec<ContentReclamationSnapshot>>;

    fn reserve_reclamation(
        &self,
        proof: ContentReclamationProof,
        reservation_id: ContentReclamationReservationId,
    ) -> Result<Option<ContentReclamationReservation>>;

    fn complete_reclamation(&self, reservation: &ContentReclamationReservation) -> Result<()>;

    fn release_reclamation(&self, reservation: &ContentReclamationReservation) -> Result<()>;
}

impl ContentReclamationProof {
    pub fn prove(
        candidate: ContentReclamationCandidate,
        same_identity_claims: Vec<ContentPublicationClaim>,
        expired_claims: Vec<ExpiredContentPublicationClaim>,
        checked_roots: Vec<CommittedContentRootCheck>,
    ) -> Result<Self> {
        candidate.validate()?;
        let consulted_claims: BTreeSet<_> = candidate.consulted_claims.iter().collect();
        let observed_claims: BTreeSet<_> = same_identity_claims
            .iter()
            .map(|claim| &claim.claim_id)
            .collect();
        if consulted_claims != observed_claims {
            return Err(CdfError::contract(
                "content reclamation proof must include exactly the same claim ids named by the candidate index consultation",
            ));
        }

        let consulted_roots: BTreeSet<_> = candidate.consulted_roots.iter().collect();
        let observed_roots: BTreeSet<_> = checked_roots.iter().map(|root| &root.root_id).collect();
        if consulted_roots != observed_roots {
            return Err(CdfError::contract(
                "content reclamation proof must include exactly the same root ids named by the candidate index consultation",
            ));
        }

        reject_duplicates(
            expired_claims.iter().map(|claim| &claim.claim_id),
            "expired content claim proofs",
        )?;

        for claim in &same_identity_claims {
            claim.validate()?;
            if !claim.content.same_content_object(&candidate.content) {
                return Err(CdfError::contract(
                    "content reclamation proof received a claim for a different content identity",
                ));
            }
            if claim.state.is_live()
                && !expired_claims
                    .iter()
                    .any(|expired| expired.matches_claim(claim))
            {
                return Err(CdfError::contract(format!(
                    "content publication claim {} is still live or lacks exact expired-lease proof",
                    claim.claim_id
                )));
            }
        }

        for expired in &expired_claims {
            if !same_identity_claims
                .iter()
                .any(|claim| expired.matches_claim(claim))
            {
                return Err(CdfError::contract(
                    "expired content claim proof does not match a consulted claim generation",
                ));
            }
        }

        for root in &checked_roots {
            if root.root_generation == 0 {
                return Err(CdfError::contract(
                    "committed content root check generation must be positive",
                ));
            }
            if root.references_candidate {
                return Err(CdfError::contract(format!(
                    "committed content root {} still references the reclamation candidate",
                    root.root_id
                )));
            }
        }

        Ok(Self {
            candidate,
            expired_claims,
            checked_roots,
        })
    }

    pub fn deletion_generation(&self) -> &ContentProviderGeneration {
        &self.candidate.observed_provider_generation
    }
}

fn reject_duplicates<'a, T>(values: impl IntoIterator<Item = &'a T>, label: &str) -> Result<()>
where
    T: Ord + 'a,
{
    let mut seen = BTreeSet::new();
    for value in values {
        if !seen.insert(value) {
            return Err(CdfError::contract(format!(
                "{label} must not contain duplicates"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id() -> ImmutableContentIdentity {
        ImmutableContentIdentity::new(
            ContentStoreNamespace::new("store").unwrap(),
            ContentObjectKey::new("objects/sha256-a.parquet").unwrap(),
            42,
            ContentDigest::new(
                ContentDigestAlgorithm::new("sha256").unwrap(),
                ContentDigestValue::new("a".repeat(64)).unwrap(),
            )
            .unwrap(),
            Some(ContentProviderGeneration::new("etag-a").unwrap()),
        )
        .unwrap()
    }

    fn claim(name: &str, state: ContentPublicationClaimState) -> ContentPublicationClaim {
        ContentPublicationClaim::new(
            DestinationId::new("parquet").unwrap(),
            TargetName::new("target").unwrap(),
            ContentClaimAttemptId::new(format!("attempt-{name}")).unwrap(),
            LeaseAuthorityDomainId::new("lease-domain").unwrap(),
            ScopeLease {
                scope: crate::ScopeKey::Stream {
                    name: format!("claim-{name}"),
                },
                owner: crate::LeaseOwnerId::new(format!("owner-{name}")).unwrap(),
                fencing_token: FencingToken::new(7).unwrap(),
                acquired_at_ms: 1,
                expires_at_ms: 9,
            },
            id(),
            ContentPublicationClaimId::new(format!("claim-{name}")).unwrap(),
            1,
            state,
        )
        .unwrap()
    }

    fn candidate(claims: &[&ContentPublicationClaim]) -> ContentReclamationCandidate {
        ContentReclamationCandidate {
            content: id(),
            observed_provider_generation: ContentProviderGeneration::new("etag-a").unwrap(),
            candidate_source: ContentReclamationCandidateSource::new("test-index").unwrap(),
            consulted_claims: claims.iter().map(|claim| claim.claim_id.clone()).collect(),
            consulted_roots: vec![CommittedContentRootId::new("root-a").unwrap()],
        }
    }

    fn expired(claim: &ContentPublicationClaim) -> ExpiredContentPublicationClaim {
        ExpiredContentPublicationClaim {
            claim_id: claim.claim_id.clone(),
            claim_generation: claim.claim_generation,
            lease_authority_domain_id: claim.lease_authority_domain_id.clone(),
            lease: claim.lease.clone(),
            cleanup_fencing_token: FencingToken::new(8).unwrap(),
            proven_at_ms: 10,
        }
    }

    fn root(references_candidate: bool) -> CommittedContentRootCheck {
        CommittedContentRootCheck {
            root_id: CommittedContentRootId::new("root-a").unwrap(),
            root_generation: 1,
            references_candidate,
        }
    }

    #[test]
    fn content_reclamation_requires_every_live_claim_to_expire() {
        let live = claim("live", ContentPublicationClaimState::Published);
        let proof = ContentReclamationProof::prove(
            candidate(&[&live]),
            vec![live.clone()],
            Vec::new(),
            vec![root(false)],
        )
        .unwrap_err();
        assert!(proof.message.contains("still live"));

        ContentReclamationProof::prove(
            candidate(&[&live]),
            vec![live.clone()],
            vec![expired(&live)],
            vec![root(false)],
        )
        .unwrap();
    }

    #[test]
    fn content_reclamation_retains_when_another_same_content_claim_is_live() {
        let expired_claim = claim("expired", ContentPublicationClaimState::Published);
        let other_live = claim("other", ContentPublicationClaimState::Planned);
        let error = ContentReclamationProof::prove(
            candidate(&[&expired_claim, &other_live]),
            vec![expired_claim.clone(), other_live],
            vec![expired(&expired_claim)],
            vec![root(false)],
        )
        .unwrap_err();
        assert!(error.message.contains("claim-other"));
    }

    #[test]
    fn content_reclamation_retains_committed_root_references() {
        let live = claim("expired", ContentPublicationClaimState::Published);
        let error = ContentReclamationProof::prove(
            candidate(&[&live]),
            vec![live.clone()],
            vec![expired(&live)],
            vec![root(true)],
        )
        .unwrap_err();
        assert!(error.message.contains("root-a"));
    }

    #[test]
    fn content_reclamation_requires_exact_provider_generation() {
        let live = claim("expired", ContentPublicationClaimState::Published);
        let mut candidate = candidate(&[&live]);
        candidate.observed_provider_generation = ContentProviderGeneration::new("etag-b").unwrap();
        let error = ContentReclamationProof::prove(
            candidate,
            vec![live.clone()],
            vec![expired(&live)],
            vec![root(false)],
        )
        .unwrap_err();
        assert!(error.message.contains("exact provider generation"));
    }
}
