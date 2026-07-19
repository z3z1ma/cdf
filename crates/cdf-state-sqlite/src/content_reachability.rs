use std::{
    path::Path,
    sync::{Mutex, MutexGuard},
};

use cdf_kernel::{
    CdfError, CommittedContentMembership, CommittedContentRoot, CommittedContentRootCheck,
    CommittedContentRootId, ContentPublicationClaim, ContentPublicationClaimId,
    ContentPublicationClaimState, ContentReachabilityStore, ContentReclamationCandidate,
    ContentReclamationCandidateSource, ContentReclamationProof, ContentReclamationReservation,
    ContentReclamationReservationId, ContentReclamationSnapshot, ContentRootIntent,
    ContentRootState, ImmutableContentIdentity, Result,
};
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

use crate::support::{
    decode_json, encode_json, ensure_schema_version_table, lock_error,
    read_component_schema_version, require_sqlite_tables, sqlite_error, sqlite_table_exists,
    write_component_schema_version,
};

const COMPONENT: &str = "content_reachability_store";
const SCHEMA_VERSION: i64 = 1;

pub struct SqliteContentReachabilityStore {
    conn: Mutex<Connection>,
}

impl SqliteContentReachabilityStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let conn = Connection::open(path).map_err(sqlite_error)?;
        initialize_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    pub fn open_in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(sqlite_error)?;
        initialize_schema(&conn)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, Connection>> {
        self.conn.lock().map_err(lock_error)
    }
}

impl ContentReachabilityStore for SqliteContentReachabilityStore {
    fn install_claim(&self, claim: ContentPublicationClaim) -> Result<ContentPublicationClaim> {
        claim.validate()?;
        if claim.claim_generation != 1
            || claim.state != ContentPublicationClaimState::Planned
            || claim.content.provider_generation.is_some()
        {
            return Err(CdfError::contract(
                "new content publication claims must be generation-one planned observations without provider generation",
            ));
        }
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        ensure_unreserved(&tx, &claim.content)?;
        ensure_consistent_content_address(&tx, &claim.content)?;
        if let Some(existing) = claim_by_id(&tx, &claim.claim_id)? {
            if existing != claim {
                return Err(CdfError::contract(format!(
                    "content publication claim {} already exists with different authority",
                    claim.claim_id
                )));
            }
            tx.commit().map_err(sqlite_error)?;
            return Ok(existing);
        }
        tx.execute(
            "INSERT INTO cdf_content_claims \
             (claim_id, store_namespace, object_key, claim_generation, state, claim_json) \
             VALUES (?, ?, ?, ?, ?, ?)",
            params![
                claim.claim_id.as_str(),
                claim.content.store_namespace.as_str(),
                claim.content.object_key.as_str(),
                generation_i64(claim.claim_generation)?,
                claim_state(claim.state),
                encode_json(&claim)?,
            ],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(claim)
    }

    fn publish_claim(
        &self,
        claim_id: &ContentPublicationClaimId,
        expected_generation: u64,
        content: ImmutableContentIdentity,
    ) -> Result<ContentPublicationClaim> {
        content.validate()?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        ensure_unreserved(&tx, &content)?;
        let mut claim = claim_by_id(&tx, claim_id)?.ok_or_else(|| {
            CdfError::contract(format!(
                "content publication claim {claim_id} does not exist"
            ))
        })?;
        if claim.state == ContentPublicationClaimState::Published && claim.content == content {
            tx.commit().map_err(sqlite_error)?;
            return Ok(claim);
        }
        if claim.claim_generation != expected_generation
            || claim.state != ContentPublicationClaimState::Planned
            || !claim.content.same_content_object(&content)
        {
            return Err(stale_claim(claim_id));
        }
        claim.claim_generation = next_generation(claim.claim_generation)?;
        claim.state = ContentPublicationClaimState::Published;
        claim.content = content;
        update_claim(&tx, &claim)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(claim)
    }

    fn release_claim(
        &self,
        claim_id: &ContentPublicationClaimId,
        expected_generation: u64,
    ) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let mut claim = claim_by_id(&tx, claim_id)?.ok_or_else(|| stale_claim(claim_id))?;
        if claim.state == ContentPublicationClaimState::Released {
            tx.commit().map_err(sqlite_error)?;
            return Ok(());
        }
        if claim.claim_generation != expected_generation
            || claim.state == ContentPublicationClaimState::Settled
        {
            return Err(stale_claim(claim_id));
        }
        claim.claim_generation = next_generation(claim.claim_generation)?;
        claim.state = ContentPublicationClaimState::Released;
        update_claim(&tx, &claim)?;
        tx.commit().map_err(sqlite_error)
    }

    fn prepare_root(&self, intent: ContentRootIntent) -> Result<ContentRootIntent> {
        intent.validate()?;
        if intent.state != ContentRootState::Prepared {
            return Err(CdfError::contract(
                "new content root intent must begin in prepared state",
            ));
        }
        let identities = inline_members(&intent.root)?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        for identity in identities {
            ensure_unreserved(&tx, identity)?;
        }
        let mut claimed_identities = std::collections::BTreeSet::new();
        for claim_id in &intent.claim_ids {
            let claim = claim_by_id(&tx, claim_id)?.ok_or_else(|| stale_claim(claim_id))?;
            if !matches!(
                claim.state,
                ContentPublicationClaimState::Published | ContentPublicationClaimState::Settled
            ) || !identities.contains(&claim.content)
            {
                return Err(CdfError::contract(format!(
                    "content root {} does not match published claim {claim_id}",
                    intent.root.root_id
                )));
            }
            claimed_identities.insert(claim.content.clone());
        }
        if claimed_identities.len() != identities.len() {
            return Err(CdfError::contract(
                "content root intent must cover every inline identity with a published claim",
            ));
        }
        if let Some(mut existing) = root_by_id(&tx, &intent.root.root_id)? {
            if existing.root != intent.root {
                return Err(CdfError::contract(format!(
                    "content root {} already exists with different authority",
                    intent.root.root_id
                )));
            }
            match existing.state {
                ContentRootState::Prepared => {
                    existing.claim_ids.extend(intent.claim_ids);
                    existing.claim_ids.sort();
                    existing.claim_ids.dedup();
                    tx.execute(
                        "UPDATE cdf_content_roots SET root_intent_json = ? WHERE root_id = ?",
                        params![encode_json(&existing)?, existing.root.root_id.as_str()],
                    )
                    .map_err(sqlite_error)?;
                }
                ContentRootState::Committed => {
                    for claim_id in &intent.claim_ids {
                        let mut claim =
                            claim_by_id(&tx, claim_id)?.ok_or_else(|| stale_claim(claim_id))?;
                        if claim.state == ContentPublicationClaimState::Published {
                            claim.claim_generation = next_generation(claim.claim_generation)?;
                            claim.state = ContentPublicationClaimState::Settled;
                            update_claim(&tx, &claim)?;
                        }
                    }
                }
            }
            tx.commit().map_err(sqlite_error)?;
            return Ok(existing);
        }
        tx.execute(
            "INSERT INTO cdf_content_roots \
             (root_id, root_generation, state, root_intent_json) VALUES (?, ?, 'prepared', ?)",
            params![
                intent.root.root_id.as_str(),
                generation_i64(intent.root.root_generation)?,
                encode_json(&intent)?,
            ],
        )
        .map_err(sqlite_error)?;
        for identity in identities {
            tx.execute(
                "INSERT INTO cdf_content_root_members \
                 (root_id, store_namespace, object_key) VALUES (?, ?, ?)",
                params![
                    intent.root.root_id.as_str(),
                    identity.store_namespace.as_str(),
                    identity.object_key.as_str(),
                ],
            )
            .map_err(sqlite_error)?;
        }
        tx.commit().map_err(sqlite_error)?;
        Ok(intent)
    }

    fn commit_root(
        &self,
        root_id: &CommittedContentRootId,
        expected_generation: u64,
    ) -> Result<CommittedContentRoot> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let mut intent = root_by_id(&tx, root_id)?.ok_or_else(|| stale_root(root_id))?;
        if intent.root.root_generation != expected_generation {
            return Err(stale_root(root_id));
        }
        if intent.state == ContentRootState::Committed {
            tx.commit().map_err(sqlite_error)?;
            return Ok(intent.root);
        }
        intent.state = ContentRootState::Committed;
        tx.execute(
            "UPDATE cdf_content_roots SET state = 'committed', root_intent_json = ? \
             WHERE root_id = ? AND root_generation = ? AND state = 'prepared'",
            params![
                encode_json(&intent)?,
                root_id.as_str(),
                generation_i64(expected_generation)?
            ],
        )
        .map_err(sqlite_error)?;
        for claim_id in &intent.claim_ids {
            let mut claim = claim_by_id(&tx, claim_id)?.ok_or_else(|| stale_claim(claim_id))?;
            if claim.state == ContentPublicationClaimState::Settled {
                continue;
            }
            if claim.state != ContentPublicationClaimState::Published {
                return Err(stale_claim(claim_id));
            }
            claim.claim_generation = next_generation(claim.claim_generation)?;
            claim.state = ContentPublicationClaimState::Settled;
            update_claim(&tx, &claim)?;
        }
        tx.commit().map_err(sqlite_error)?;
        Ok(intent.root)
    }

    fn root_intent(&self, root_id: &CommittedContentRootId) -> Result<Option<ContentRootIntent>> {
        let conn = self.lock()?;
        root_by_id(&conn, root_id)
    }

    fn abort_root(&self, root_id: &CommittedContentRootId, expected_generation: u64) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let intent = root_by_id(&tx, root_id)?.ok_or_else(|| stale_root(root_id))?;
        if intent.root.root_generation != expected_generation
            || intent.state != ContentRootState::Prepared
        {
            return Err(stale_root(root_id));
        }
        for claim_id in &intent.claim_ids {
            let mut claim = claim_by_id(&tx, claim_id)?.ok_or_else(|| stale_claim(claim_id))?;
            if claim.state == ContentPublicationClaimState::Published {
                claim.claim_generation = next_generation(claim.claim_generation)?;
                claim.state = ContentPublicationClaimState::Released;
                update_claim(&tx, &claim)?;
            }
        }
        delete_root(&tx, root_id)?;
        tx.commit().map_err(sqlite_error)
    }

    fn release_root(
        &self,
        root_id: &CommittedContentRootId,
        expected_generation: u64,
    ) -> Result<()> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let intent = root_by_id(&tx, root_id)?.ok_or_else(|| stale_root(root_id))?;
        if intent.root.root_generation != expected_generation
            || intent.state != ContentRootState::Committed
        {
            return Err(stale_root(root_id));
        }
        delete_root(&tx, root_id)?;
        tx.commit().map_err(sqlite_error)
    }

    fn reclamation_candidates(
        &self,
        store_namespace: &cdf_kernel::ContentStoreNamespace,
        limit: u32,
    ) -> Result<Vec<ContentReclamationSnapshot>> {
        if limit == 0 {
            return Err(CdfError::contract(
                "content reclamation candidate limit must be positive",
            ));
        }
        let conn = self.lock()?;
        candidate_snapshots(&conn, store_namespace, limit)
    }

    fn reserve_reclamation(
        &self,
        proof: ContentReclamationProof,
        reservation_id: ContentReclamationReservationId,
    ) -> Result<Option<ContentReclamationReservation>> {
        proof.candidate.validate()?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let current = snapshot_for_identity(&tx, &proof.candidate.content)?;
        let Some(current) = current else {
            tx.commit().map_err(sqlite_error)?;
            return Ok(None);
        };
        if current.candidate != proof.candidate
            || current.same_content_claims.iter().any(|claim| {
                !proof
                    .expired_claims
                    .iter()
                    .any(|expired| expired.matches_claim(claim))
                    && claim.state.is_live()
            })
            || current
                .checked_roots
                .iter()
                .any(|root| root.references_candidate)
        {
            tx.commit().map_err(sqlite_error)?;
            return Ok(None);
        }
        let existing = reservation_for_identity(&tx, &proof.candidate.content)?;
        if existing.is_some() {
            tx.commit().map_err(sqlite_error)?;
            return Ok(None);
        }
        let reservation = ContentReclamationReservation {
            reservation_id,
            reservation_generation: 1,
            proof,
        };
        reservation.validate()?;
        tx.execute(
            "INSERT INTO cdf_content_reclamation_reservations \
             (store_namespace, object_key, reservation_id, reservation_generation, reservation_json) \
             VALUES (?, ?, ?, 1, ?)",
            params![
                reservation.proof.candidate.content.store_namespace.as_str(),
                reservation.proof.candidate.content.object_key.as_str(),
                reservation.reservation_id.as_str(),
                encode_json(&reservation)?,
            ],
        )
        .map_err(sqlite_error)?;
        tx.commit().map_err(sqlite_error)?;
        Ok(Some(reservation))
    }

    fn complete_reclamation(&self, reservation: &ContentReclamationReservation) -> Result<()> {
        reservation.validate()?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        ensure_exact_reservation(&tx, reservation)?;
        let content = &reservation.proof.candidate.content;
        tx.execute(
            "DELETE FROM cdf_content_claims WHERE store_namespace = ? AND object_key = ?",
            params![
                content.store_namespace.as_str(),
                content.object_key.as_str()
            ],
        )
        .map_err(sqlite_error)?;
        delete_reservation(&tx, reservation)?;
        tx.commit().map_err(sqlite_error)
    }

    fn reclamation_reservations(
        &self,
        store_namespace: &cdf_kernel::ContentStoreNamespace,
        limit: u32,
    ) -> Result<Vec<ContentReclamationReservation>> {
        if limit == 0 {
            return Err(CdfError::contract(
                "content reclamation reservation limit must be positive",
            ));
        }
        let conn = self.lock()?;
        let mut statement = conn
            .prepare(
                "SELECT reservation_json FROM cdf_content_reclamation_reservations \
                 WHERE store_namespace = ? ORDER BY object_key LIMIT ?",
            )
            .map_err(sqlite_error)?;
        let rows = statement
            .query_map(params![store_namespace.as_str(), i64::from(limit)], |row| {
                row.get::<_, String>(0)
            })
            .map_err(sqlite_error)?;
        rows.map(|row| decode_json(&row.map_err(sqlite_error)?, 1))
            .collect()
    }

    fn release_reclamation(&self, reservation: &ContentReclamationReservation) -> Result<()> {
        reservation.validate()?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        ensure_exact_reservation(&tx, reservation)?;
        delete_reservation(&tx, reservation)?;
        tx.commit().map_err(sqlite_error)
    }
}

fn initialize_schema(conn: &Connection) -> Result<()> {
    validate_schema_version(conn)?;
    ensure_schema_version_table(conn)?;
    conn.execute_batch(
        "
        CREATE TABLE IF NOT EXISTS cdf_content_claims (
            claim_id TEXT PRIMARY KEY,
            store_namespace TEXT NOT NULL,
            object_key TEXT NOT NULL,
            claim_generation INTEGER NOT NULL CHECK (claim_generation > 0),
            state TEXT NOT NULL,
            claim_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS cdf_content_claims_by_object
            ON cdf_content_claims (store_namespace, object_key, claim_id);
        CREATE TABLE IF NOT EXISTS cdf_content_roots (
            root_id TEXT PRIMARY KEY,
            root_generation INTEGER NOT NULL CHECK (root_generation > 0),
            state TEXT NOT NULL CHECK (state IN ('prepared', 'committed')),
            root_intent_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS cdf_content_root_members (
            root_id TEXT NOT NULL REFERENCES cdf_content_roots(root_id) ON DELETE CASCADE,
            store_namespace TEXT NOT NULL,
            object_key TEXT NOT NULL,
            PRIMARY KEY (root_id, store_namespace, object_key)
        );
        CREATE INDEX IF NOT EXISTS cdf_content_roots_by_object
            ON cdf_content_root_members (store_namespace, object_key, root_id);
        CREATE TABLE IF NOT EXISTS cdf_content_reclamation_reservations (
            store_namespace TEXT NOT NULL,
            object_key TEXT NOT NULL,
            reservation_id TEXT NOT NULL UNIQUE,
            reservation_generation INTEGER NOT NULL CHECK (reservation_generation > 0),
            reservation_json TEXT NOT NULL,
            PRIMARY KEY (store_namespace, object_key)
        );
        ",
    )
    .map_err(sqlite_error)?;
    write_component_schema_version(conn, COMPONENT, SCHEMA_VERSION)
}

fn validate_schema_version(conn: &Connection) -> Result<()> {
    match read_component_schema_version(conn, COMPONENT)? {
        Some(SCHEMA_VERSION) => require_sqlite_tables(
            conn,
            "content reachability store",
            &[
                "cdf_content_claims",
                "cdf_content_roots",
                "cdf_content_root_members",
                "cdf_content_reclamation_reservations",
            ],
        ),
        Some(version) => Err(CdfError::internal(format!(
            "unsupported content reachability SQLite schema version {version}"
        ))),
        None if sqlite_table_exists(conn, "cdf_content_claims")? => {
            Err(CdfError::internal(format!(
                "content reachability SQLite schema is unversioned; expected current version {SCHEMA_VERSION}"
            )))
        }
        None => Ok(()),
    }
}

fn claim_by_id(
    conn: &Connection,
    id: &ContentPublicationClaimId,
) -> Result<Option<ContentPublicationClaim>> {
    conn.query_row(
        "SELECT claim_json FROM cdf_content_claims WHERE claim_id = ?",
        params![id.as_str()],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(sqlite_error)?
    .map(|json| decode_json(&json, 1))
    .transpose()
}

fn root_by_id(conn: &Connection, id: &CommittedContentRootId) -> Result<Option<ContentRootIntent>> {
    conn.query_row(
        "SELECT root_intent_json FROM cdf_content_roots WHERE root_id = ?",
        params![id.as_str()],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(sqlite_error)?
    .map(|json| decode_json(&json, 1))
    .transpose()
}

fn update_claim(conn: &Connection, claim: &ContentPublicationClaim) -> Result<()> {
    let changed = conn
        .execute(
            "UPDATE cdf_content_claims SET claim_generation = ?, state = ?, claim_json = ? \
         WHERE claim_id = ?",
            params![
                generation_i64(claim.claim_generation)?,
                claim_state(claim.state),
                encode_json(claim)?,
                claim.claim_id.as_str(),
            ],
        )
        .map_err(sqlite_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(stale_claim(&claim.claim_id))
    }
}

fn ensure_consistent_content_address(
    conn: &Connection,
    content: &ImmutableContentIdentity,
) -> Result<()> {
    for claim in claims_for_identity(conn, content)? {
        if !claim.content.same_content_object(content) {
            return Err(CdfError::contract(format!(
                "immutable content address {}/{} is already bound to different bytes",
                content.store_namespace, content.object_key
            )));
        }
    }
    Ok(())
}

fn ensure_unreserved(conn: &Connection, content: &ImmutableContentIdentity) -> Result<()> {
    if reservation_for_identity(conn, content)?.is_some() {
        return Err(CdfError::contract(format!(
            "immutable content address {}/{} is fenced for reclamation",
            content.store_namespace, content.object_key
        )));
    }
    Ok(())
}

fn claims_for_identity(
    conn: &Connection,
    content: &ImmutableContentIdentity,
) -> Result<Vec<ContentPublicationClaim>> {
    let mut statement = conn
        .prepare(
            "SELECT claim_json FROM cdf_content_claims \
             WHERE store_namespace = ? AND object_key = ? ORDER BY claim_id",
        )
        .map_err(sqlite_error)?;
    let rows = statement
        .query_map(
            params![
                content.store_namespace.as_str(),
                content.object_key.as_str()
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(sqlite_error)?;
    rows.map(|row| decode_json(&row.map_err(sqlite_error)?, 1))
        .collect()
}

fn root_checks_for_identity(
    conn: &Connection,
    content: &ImmutableContentIdentity,
) -> Result<Vec<CommittedContentRootCheck>> {
    let mut statement = conn
        .prepare(
            "SELECT r.root_intent_json FROM cdf_content_roots r \
             JOIN cdf_content_root_members m ON m.root_id = r.root_id \
             WHERE m.store_namespace = ? AND m.object_key = ? ORDER BY r.root_id",
        )
        .map_err(sqlite_error)?;
    let rows = statement
        .query_map(
            params![
                content.store_namespace.as_str(),
                content.object_key.as_str()
            ],
            |row| row.get::<_, String>(0),
        )
        .map_err(sqlite_error)?;
    let mut checks = Vec::new();
    for row in rows {
        let intent: ContentRootIntent = decode_json(&row.map_err(sqlite_error)?, 1)?;
        checks.push(CommittedContentRootCheck {
            root_id: intent.root.root_id,
            root_generation: intent.root.root_generation,
            references_candidate: true,
        });
    }
    Ok(checks)
}

fn snapshot_for_identity(
    conn: &Connection,
    content: &ImmutableContentIdentity,
) -> Result<Option<ContentReclamationSnapshot>> {
    let claims = claims_for_identity(conn, content)?;
    let exact = claims.iter().find_map(|claim| {
        claim
            .content
            .provider_generation
            .as_ref()
            .map(|generation| (claim.content.clone(), generation.clone()))
    });
    let Some((content, observed_provider_generation)) = exact else {
        return Ok(None);
    };
    for claim in &claims {
        if !claim.content.same_content_object(&content) {
            return Err(CdfError::data(
                "content reachability index contains conflicting bytes for one immutable address",
            ));
        }
        if let Some(generation) = &claim.content.provider_generation
            && generation != &observed_provider_generation
        {
            return Err(CdfError::data(
                "content reachability index contains conflicting provider generations for one immutable address",
            ));
        }
    }
    let roots = root_checks_for_identity(conn, &content)?;
    let candidate = ContentReclamationCandidate {
        content,
        observed_provider_generation,
        candidate_source: ContentReclamationCandidateSource::new("sqlite-content-index-v1")?,
        consulted_claims: claims.iter().map(|claim| claim.claim_id.clone()).collect(),
        consulted_roots: roots.iter().map(|root| root.root_id.clone()).collect(),
    };
    let snapshot = ContentReclamationSnapshot {
        candidate,
        same_content_claims: claims,
        checked_roots: roots,
    };
    snapshot.validate()?;
    Ok(Some(snapshot))
}

fn candidate_snapshots(
    conn: &Connection,
    store_namespace: &cdf_kernel::ContentStoreNamespace,
    limit: u32,
) -> Result<Vec<ContentReclamationSnapshot>> {
    let mut statement = conn
        .prepare(
            "SELECT DISTINCT c.store_namespace, c.object_key \
             FROM cdf_content_claims c \
             LEFT JOIN cdf_content_root_members m \
               ON m.store_namespace = c.store_namespace AND m.object_key = c.object_key \
             LEFT JOIN cdf_content_reclamation_reservations q \
               ON q.store_namespace = c.store_namespace AND q.object_key = c.object_key \
             WHERE c.store_namespace = ? AND m.root_id IS NULL AND q.reservation_id IS NULL \
             ORDER BY c.object_key LIMIT ?",
        )
        .map_err(sqlite_error)?;
    let rows = statement
        .query_map(params![store_namespace.as_str(), i64::from(limit)], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(sqlite_error)?;
    let mut snapshots = Vec::new();
    for row in rows {
        let (namespace, object_key) = row.map_err(sqlite_error)?;
        let claims = conn
            .query_row(
                "SELECT claim_json FROM cdf_content_claims \
                 WHERE store_namespace = ? AND object_key = ? ORDER BY claim_id LIMIT 1",
                params![namespace, object_key],
                |row| row.get::<_, String>(0),
            )
            .map_err(sqlite_error)?;
        let claim: ContentPublicationClaim = decode_json(&claims, 1)?;
        if let Some(snapshot) = snapshot_for_identity(conn, &claim.content)? {
            snapshots.push(snapshot);
        }
    }
    Ok(snapshots)
}

fn reservation_for_identity(
    conn: &Connection,
    content: &ImmutableContentIdentity,
) -> Result<Option<ContentReclamationReservation>> {
    conn.query_row(
        "SELECT reservation_json FROM cdf_content_reclamation_reservations \
         WHERE store_namespace = ? AND object_key = ?",
        params![
            content.store_namespace.as_str(),
            content.object_key.as_str()
        ],
        |row| row.get::<_, String>(0),
    )
    .optional()
    .map_err(sqlite_error)?
    .map(|json| decode_json(&json, 1))
    .transpose()
}

fn ensure_exact_reservation(
    conn: &Connection,
    expected: &ContentReclamationReservation,
) -> Result<()> {
    let observed = reservation_for_identity(conn, &expected.proof.candidate.content)?
        .ok_or_else(|| CdfError::contract("content reclamation reservation is absent"))?;
    if &observed == expected {
        Ok(())
    } else {
        Err(CdfError::contract(
            "content reclamation reservation is stale or superseded",
        ))
    }
}

fn delete_reservation(
    conn: &Connection,
    reservation: &ContentReclamationReservation,
) -> Result<()> {
    let changed = conn.execute(
        "DELETE FROM cdf_content_reclamation_reservations \
         WHERE store_namespace = ? AND object_key = ? AND reservation_id = ? AND reservation_generation = ?",
        params![
            reservation.proof.candidate.content.store_namespace.as_str(),
            reservation.proof.candidate.content.object_key.as_str(),
            reservation.reservation_id.as_str(),
            generation_i64(reservation.reservation_generation)?,
        ],
    )
    .map_err(sqlite_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(CdfError::contract(
            "content reclamation reservation is stale",
        ))
    }
}

fn delete_root(conn: &Connection, root_id: &CommittedContentRootId) -> Result<()> {
    conn.execute(
        "DELETE FROM cdf_content_root_members WHERE root_id = ?",
        params![root_id.as_str()],
    )
    .map_err(sqlite_error)?;
    let changed = conn
        .execute(
            "DELETE FROM cdf_content_roots WHERE root_id = ?",
            params![root_id.as_str()],
        )
        .map_err(sqlite_error)?;
    if changed == 1 {
        Ok(())
    } else {
        Err(stale_root(root_id))
    }
}

fn inline_members(root: &CommittedContentRoot) -> Result<&[ImmutableContentIdentity]> {
    match &root.membership {
        CommittedContentMembership::Inline { identities } => Ok(identities),
        CommittedContentMembership::Shard { .. } => Err(CdfError::contract(
            "content reachability store v1 requires inline root membership",
        )),
    }
}

fn claim_state(state: ContentPublicationClaimState) -> &'static str {
    match state {
        ContentPublicationClaimState::Planned => "planned",
        ContentPublicationClaimState::Published => "published",
        ContentPublicationClaimState::Settled => "settled",
        ContentPublicationClaimState::Released => "released",
    }
}

fn next_generation(value: u64) -> Result<u64> {
    value
        .checked_add(1)
        .ok_or_else(|| CdfError::internal("content authority generation overflow"))
}

fn generation_i64(value: u64) -> Result<i64> {
    i64::try_from(value).map_err(|_| {
        CdfError::internal("content authority generation exceeds SQLite integer range")
    })
}

fn stale_claim(id: &ContentPublicationClaimId) -> CdfError {
    CdfError::contract(format!(
        "content publication claim {id} is stale or superseded"
    ))
}

fn stale_root(id: &CommittedContentRootId) -> CdfError {
    CdfError::contract(format!("content root {id} is stale or superseded"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cdf_kernel::{
        ContentClaimAttemptId, ContentDigest, ContentDigestAlgorithm, ContentDigestValue,
        ContentObjectKey, ContentStoreNamespace, DestinationId, FencingToken,
        LeaseAuthorityDomainId, LeaseOwnerId, ScopeKey, ScopeLease, TargetName,
    };

    fn identity(generation: Option<&str>) -> ImmutableContentIdentity {
        ImmutableContentIdentity::new(
            ContentStoreNamespace::new("store").unwrap(),
            ContentObjectKey::new("objects/a").unwrap(),
            42,
            ContentDigest::new(
                ContentDigestAlgorithm::new("sha256").unwrap(),
                ContentDigestValue::new("a".repeat(64)).unwrap(),
            )
            .unwrap(),
            generation.map(|value| cdf_kernel::ContentProviderGeneration::new(value).unwrap()),
        )
        .unwrap()
    }

    fn claim(name: &str) -> ContentPublicationClaim {
        ContentPublicationClaim::new(
            DestinationId::new("parquet").unwrap(),
            TargetName::new("events").unwrap(),
            ContentClaimAttemptId::new(format!("attempt-{name}")).unwrap(),
            LeaseAuthorityDomainId::new("domain").unwrap(),
            ScopeLease {
                scope: ScopeKey::Stream {
                    name: format!("scope-{name}"),
                },
                owner: LeaseOwnerId::new(format!("owner-{name}")).unwrap(),
                fencing_token: FencingToken::new(1).unwrap(),
                acquired_at_ms: 1,
                expires_at_ms: 10,
            },
            identity(None),
            ContentPublicationClaimId::new(format!("claim-{name}")).unwrap(),
            1,
            ContentPublicationClaimState::Planned,
        )
        .unwrap()
    }

    fn published(store: &SqliteContentReachabilityStore, name: &str) -> ContentPublicationClaim {
        let claim = store.install_claim(claim(name)).unwrap();
        store
            .publish_claim(
                &claim.claim_id,
                claim.claim_generation,
                identity(Some("etag-a")),
            )
            .unwrap()
    }

    #[test]
    fn prepared_and_committed_roots_protect_content_monotonically() {
        let store = SqliteContentReachabilityStore::open_in_memory().unwrap();
        let claim = published(&store, "a");
        let root = CommittedContentRoot {
            destination_id: DestinationId::new("parquet").unwrap(),
            target: TargetName::new("events").unwrap(),
            root_id: CommittedContentRootId::new("root-a").unwrap(),
            root_generation: 1,
            retained_until_ms: None,
            membership: CommittedContentMembership::Inline {
                identities: vec![claim.content.clone()],
            },
        };
        store
            .prepare_root(ContentRootIntent {
                root: root.clone(),
                claim_ids: vec![claim.claim_id.clone()],
                state: ContentRootState::Prepared,
            })
            .unwrap();
        let namespace = ContentStoreNamespace::new("store").unwrap();
        assert!(
            store
                .reclamation_candidates(&namespace, 8)
                .unwrap()
                .is_empty()
        );
        assert_eq!(store.commit_root(&root.root_id, 1).unwrap(), root);
        assert!(
            store
                .reclamation_candidates(&namespace, 8)
                .unwrap()
                .is_empty()
        );
        store.release_root(&root.root_id, 1).unwrap();
        assert_eq!(
            store.reclamation_candidates(&namespace, 8).unwrap().len(),
            1
        );
    }

    #[test]
    fn one_root_settles_concurrent_same_content_publishers() {
        let store = SqliteContentReachabilityStore::open_in_memory().unwrap();
        let first = published(&store, "first");
        let second = published(&store, "second");
        let root = CommittedContentRoot {
            destination_id: DestinationId::new("parquet").unwrap(),
            target: TargetName::new("events").unwrap(),
            root_id: CommittedContentRootId::new("root-shared").unwrap(),
            root_generation: 1,
            retained_until_ms: None,
            membership: CommittedContentMembership::Inline {
                identities: vec![first.content.clone()],
            },
        };
        store
            .prepare_root(ContentRootIntent {
                root: root.clone(),
                claim_ids: vec![first.claim_id],
                state: ContentRootState::Prepared,
            })
            .unwrap();
        let merged = store
            .prepare_root(ContentRootIntent {
                root: root.clone(),
                claim_ids: vec![second.claim_id],
                state: ContentRootState::Prepared,
            })
            .unwrap();
        assert_eq!(merged.claim_ids.len(), 2);
        assert_eq!(store.commit_root(&root.root_id, 1).unwrap(), root);
        assert!(
            store
                .reclamation_candidates(&ContentStoreNamespace::new("store").unwrap(), 8)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn reclamation_reservation_blocks_racing_claim_until_released() {
        let store = SqliteContentReachabilityStore::open_in_memory().unwrap();
        let published_claim = published(&store, "a");
        store
            .release_claim(&published_claim.claim_id, published_claim.claim_generation)
            .unwrap();
        let snapshot = store
            .reclamation_candidates(&ContentStoreNamespace::new("store").unwrap(), 8)
            .unwrap()
            .pop()
            .unwrap();
        let expired = cdf_kernel::ExpiredContentPublicationClaim {
            claim_id: snapshot.same_content_claims[0].claim_id.clone(),
            claim_generation: snapshot.same_content_claims[0].claim_generation,
            lease_authority_domain_id: snapshot.same_content_claims[0]
                .lease_authority_domain_id
                .clone(),
            lease: snapshot.same_content_claims[0].lease.clone(),
            cleanup_fencing_token: FencingToken::new(2).unwrap(),
            proven_at_ms: 11,
        };
        let proof = ContentReclamationProof::prove(
            snapshot.candidate,
            snapshot.same_content_claims,
            vec![expired],
            snapshot.checked_roots,
        )
        .unwrap();
        let reservation = store
            .reserve_reclamation(
                proof,
                ContentReclamationReservationId::new("reservation-a").unwrap(),
            )
            .unwrap()
            .unwrap();
        assert!(
            store
                .install_claim(claim("racing"))
                .unwrap_err()
                .message
                .contains("fenced")
        );
        store.release_reclamation(&reservation).unwrap();
        store.install_claim(claim("racing")).unwrap();
    }
}
