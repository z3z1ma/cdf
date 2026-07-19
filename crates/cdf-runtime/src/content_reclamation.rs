use cdf_kernel::{
    CdfError, ContentPublicationClaim, ContentReclamationProof, ContentReclamationReservation,
    ContentReclamationReservationId, ExpiredContentPublicationClaim, FencingToken,
    ImmutableContentIdentity, Result,
};
use std::collections::BTreeSet;

use crate::{
    ExecutionServices, LoadAttemptId, ManagedExpiredStagingLeaseProof, StagingLease,
    StagingLeaseIdentity,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConditionalContentDeleteOutcome {
    Deleted,
    AlreadyAbsent,
    GenerationMismatch,
    Unsupported,
}

/// Provider-owned exact-generation deletion capability.
/// Implementations must never emulate this with an unfenced HEAD-then-delete sequence.
pub trait ConditionalContentDeleter: Send + Sync {
    fn store_namespace(&self) -> &cdf_kernel::ContentStoreNamespace;

    fn delete_if_generation(
        &self,
        content: &ImmutableContentIdentity,
    ) -> Result<ConditionalContentDeleteOutcome>;
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContentReclamationReport {
    pub candidates_considered: u32,
    pub objects_deleted: u32,
    pub already_absent: u32,
    pub retained_live: u32,
    pub generation_conflicts: u32,
    pub unsupported: u32,
    pub recovered_reservations: u32,
}

impl ExecutionServices {
    /// Reclaims at most `limit` immutable objects using only indexed candidates and exact provider
    /// generations. The durable reservation is installed before provider mutation and blocks new
    /// claims/root intents until completion or an explicit inconclusive release.
    pub fn reclaim_unreachable_content(
        &self,
        limit: u32,
        deleter: &dyn ConditionalContentDeleter,
    ) -> Result<ContentReclamationReport> {
        if limit == 0 {
            return Err(CdfError::contract(
                "content reclamation limit must be positive",
            ));
        }
        let store = self.content_reachability_store()?;
        let mut report = ContentReclamationReport::default();
        let mut remaining = limit;
        let mut recovered = BTreeSet::new();

        for reservation in store.reclamation_reservations(deleter.store_namespace(), limit)? {
            recovered.insert((
                reservation.proof.candidate.content.store_namespace.clone(),
                reservation.proof.candidate.content.object_key.clone(),
            ));
            resolve_reserved_delete(store.as_ref(), deleter, &reservation, &mut report)?;
            report.recovered_reservations = report.recovered_reservations.saturating_add(1);
            remaining = remaining.saturating_sub(1);
        }

        if remaining == 0 {
            return Ok(report);
        }
        for snapshot in store.reclamation_candidates(deleter.store_namespace(), remaining)? {
            if recovered.contains(&(
                snapshot.candidate.content.store_namespace.clone(),
                snapshot.candidate.content.object_key.clone(),
            )) {
                continue;
            }
            report.candidates_considered = report.candidates_considered.saturating_add(1);
            if snapshot
                .checked_roots
                .iter()
                .any(|root| root.references_candidate)
            {
                report.retained_live = report.retained_live.saturating_add(1);
                continue;
            }

            let mut managed = Vec::<(StagingLease, ManagedExpiredStagingLeaseProof)>::new();
            let mut retained = false;
            for claim in snapshot
                .same_content_claims
                .iter()
                .filter(|claim| claim.state.is_live())
            {
                let lease = staging_lease_for_claim(claim)?;
                if managed
                    .iter()
                    .any(|(existing, _)| existing.same_generation(&lease))
                {
                    continue;
                }
                match self.prove_expired_staging_lease(&lease)? {
                    Some(proof) => managed.push((lease, proof)),
                    None => {
                        retained = true;
                        break;
                    }
                }
            }
            if retained {
                finish_expiry_proofs(managed)?;
                report.retained_live = report.retained_live.saturating_add(1);
                continue;
            }

            let expired_claims = snapshot
                .same_content_claims
                .iter()
                .filter(|claim| claim.state.is_live())
                .map(|claim| expired_claim_for(claim, &managed))
                .collect::<Result<Vec<_>>>()?;
            let proof = ContentReclamationProof::prove(
                snapshot.candidate,
                snapshot.same_content_claims,
                expired_claims,
                snapshot.checked_roots,
            )?;
            let reservation_id = ContentReclamationReservationId::new(format!(
                "cdf-reclaim-{}-{:016x}",
                std::process::id(),
                self.entropy_u64()
            ))?;
            let Some(reservation) = store.reserve_reclamation(proof, reservation_id)? else {
                finish_expiry_proofs(managed)?;
                report.retained_live = report.retained_live.saturating_add(1);
                continue;
            };
            for (_, proof) in &managed {
                proof.mutation_guard()?.assert_current()?;
            }
            let deleted =
                resolve_reserved_delete(store.as_ref(), deleter, &reservation, &mut report);
            let released = finish_expiry_proofs(managed);
            match (deleted, released) {
                (Ok(()), Ok(())) => {}
                (Err(error), Ok(())) | (Ok(()), Err(error)) => return Err(error),
                (Err(mut error), Err(release)) => {
                    error.message.push_str(&format!(
                        "; expired content cleanup lease release also failed: {release}"
                    ));
                    return Err(error);
                }
            }
        }
        Ok(report)
    }
}

fn staging_lease_for_claim(claim: &ContentPublicationClaim) -> Result<StagingLease> {
    Ok(StagingLease {
        authority_domain_id: claim.lease_authority_domain_id.clone(),
        identity: StagingLeaseIdentity::new(
            claim.destination_id.clone(),
            claim.target.clone(),
            LoadAttemptId::new(claim.attempt_id.as_str())?,
        ),
        scope_lease: claim.lease.clone(),
    })
}

fn expired_claim_for(
    claim: &ContentPublicationClaim,
    proofs: &[(StagingLease, ManagedExpiredStagingLeaseProof)],
) -> Result<ExpiredContentPublicationClaim> {
    let (_, proof) = proofs
        .iter()
        .find(|(lease, _)| {
            lease.authority_domain_id == claim.lease_authority_domain_id
                && lease.scope_lease == claim.lease
        })
        .ok_or_else(|| CdfError::internal("expired content claim lost its managed lease proof"))?;
    Ok(ExpiredContentPublicationClaim {
        claim_id: claim.claim_id.clone(),
        claim_generation: claim.claim_generation,
        lease_authority_domain_id: claim.lease_authority_domain_id.clone(),
        lease: claim.lease.clone(),
        cleanup_fencing_token: FencingToken::new(proof.proof().cleanup_fencing_token())?,
        proven_at_ms: proof.proof().proven_at_ms(),
    })
}

fn finish_expiry_proofs(
    proofs: Vec<(StagingLease, ManagedExpiredStagingLeaseProof)>,
) -> Result<()> {
    let mut failure = None;
    for (_, proof) in proofs {
        if let Err(error) = proof.finish()
            && failure.is_none()
        {
            failure = Some(error);
        }
    }
    failure.map_or(Ok(()), Err)
}

fn resolve_reserved_delete(
    store: &dyn cdf_kernel::ContentReachabilityStore,
    deleter: &dyn ConditionalContentDeleter,
    reservation: &ContentReclamationReservation,
    report: &mut ContentReclamationReport,
) -> Result<()> {
    match deleter.delete_if_generation(&reservation.proof.candidate.content)? {
        ConditionalContentDeleteOutcome::Deleted => {
            store.complete_reclamation(reservation)?;
            report.objects_deleted = report.objects_deleted.saturating_add(1);
        }
        ConditionalContentDeleteOutcome::AlreadyAbsent => {
            store.complete_reclamation(reservation)?;
            report.already_absent = report.already_absent.saturating_add(1);
        }
        ConditionalContentDeleteOutcome::GenerationMismatch => {
            store.release_reclamation(reservation)?;
            report.generation_conflicts = report.generation_conflicts.saturating_add(1);
        }
        ConditionalContentDeleteOutcome::Unsupported => {
            store.release_reclamation(reservation)?;
            report.unsupported = report.unsupported.saturating_add(1);
        }
    }
    Ok(())
}
