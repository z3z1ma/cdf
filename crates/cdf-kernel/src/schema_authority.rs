use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CanonicalArrowSchema, CdfError, Checkpoint, CheckpointId, ContractRef, DestinationId,
    EnvironmentName, FencingToken, ImmutableContentIdentity, LeaseAuthorityDomainId, LeaseOwnerId,
    OutputBindingId, PackageHash, ProjectId, PromotionId, Receipt, ReceiptId, ResourceId, Result,
    RunId, SchemaHash, ScopeKey, ScopeLease, TargetName, canonical_arrow_schema_hash,
};

pub const MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT: u32 = 10_000;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaAuthorityKey {
    pub authority_domain_id: LeaseAuthorityDomainId,
    pub project_id: ProjectId,
    pub environment: EnvironmentName,
    pub resource_id: ResourceId,
    pub output_binding: OutputBindingId,
}

impl SchemaAuthorityKey {
    pub fn new(
        authority_domain_id: LeaseAuthorityDomainId,
        project_id: ProjectId,
        environment: EnvironmentName,
        resource_id: ResourceId,
        output_binding: OutputBindingId,
    ) -> Result<Self> {
        let key = Self {
            authority_domain_id,
            project_id,
            environment,
            resource_id,
            output_binding,
        };
        key.validate()?;
        Ok(key)
    }

    pub fn validate(&self) -> Result<()> {
        LeaseAuthorityDomainId::new(self.authority_domain_id.as_str()).map(drop)?;
        ProjectId::new(self.project_id.as_str()).map(drop)?;
        EnvironmentName::new(self.environment.as_str()).map(drop)?;
        ResourceId::new(self.resource_id.as_str()).map(drop)?;
        OutputBindingId::new(self.output_binding.as_str()).map(drop)
    }

    pub fn promotion_scope(&self) -> Result<ScopeKey> {
        self.validate()?;
        let mut encoded = String::from("schema-authority");
        for part in [
            self.authority_domain_id.as_str(),
            self.project_id.as_str(),
            self.environment.as_str(),
            self.resource_id.as_str(),
            self.output_binding.as_str(),
        ] {
            use std::fmt::Write as _;
            write!(&mut encoded, ":{}:{part}", part.len()).map_err(|error| {
                CdfError::internal(format!("encode schema authority lease scope: {error}"))
            })?;
        }
        Ok(ScopeKey::SchemaContract {
            contract: ContractRef::new(encoded)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaVersionProvenance {
    FirstUse,
    Promotion { promotion_id: PromotionId },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaVersion {
    pub schema_hash: SchemaHash,
    pub canonical_schema: CanonicalArrowSchema,
    pub predecessor: Option<SchemaHash>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_evidence: Option<ImmutableContentIdentity>,
    pub created_at_ms: i64,
    pub provenance: SchemaVersionProvenance,
}

impl SchemaVersion {
    pub fn new(
        canonical_schema: CanonicalArrowSchema,
        predecessor: Option<SchemaHash>,
        discovery_evidence: Option<ImmutableContentIdentity>,
        created_at_ms: i64,
        provenance: SchemaVersionProvenance,
    ) -> Result<Self> {
        let schema_hash = canonical_arrow_schema_hash(&canonical_schema.to_arrow()?)?;
        let version = Self {
            schema_hash,
            canonical_schema,
            predecessor,
            discovery_evidence,
            created_at_ms,
            provenance,
        };
        version.validate()?;
        Ok(version)
    }

    pub fn validate(&self) -> Result<()> {
        if self.created_at_ms < 0 {
            return Err(CdfError::contract(
                "schema version creation time must be a non-negative epoch millisecond",
            ));
        }
        let observed_hash = canonical_arrow_schema_hash(&self.canonical_schema.to_arrow()?)?;
        if observed_hash != self.schema_hash {
            return Err(CdfError::contract(format!(
                "schema version hash {} does not match canonical schema {}",
                self.schema_hash, observed_hash
            )));
        }
        if self.predecessor.as_ref() == Some(&self.schema_hash) {
            return Err(CdfError::contract(
                "schema version predecessor must differ from its schema hash",
            ));
        }
        if let Some(predecessor) = &self.predecessor {
            SchemaHash::new(predecessor.as_str()).map(drop)?;
        }
        match &self.provenance {
            SchemaVersionProvenance::FirstUse if self.predecessor.is_some() => Err(
                CdfError::contract("first-use schema version cannot declare a predecessor"),
            ),
            SchemaVersionProvenance::Promotion { .. } if self.predecessor.is_none() => Err(
                CdfError::contract("promoted schema version must declare its predecessor"),
            ),
            SchemaVersionProvenance::Promotion { promotion_id } => {
                PromotionId::new(promotion_id.as_str()).map(drop)?;
                if let Some(evidence) = &self.discovery_evidence {
                    evidence.validate()?;
                }
                Ok(())
            }
            SchemaVersionProvenance::FirstUse => {
                if let Some(evidence) = &self.discovery_evidence {
                    evidence.validate()?;
                }
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaHeadStatus {
    Active,
    Promoting {
        promotion_id: PromotionId,
        from_schema_hash: SchemaHash,
        to_schema_hash: SchemaHash,
        lease_owner: LeaseOwnerId,
        fencing_token: FencingToken,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaHead {
    pub key: SchemaAuthorityKey,
    pub generation: u64,
    pub schema_hash: SchemaHash,
    pub status: SchemaHeadStatus,
}

impl SchemaHead {
    pub fn active(
        key: SchemaAuthorityKey,
        generation: u64,
        schema_hash: SchemaHash,
    ) -> Result<Self> {
        let head = Self {
            key,
            generation,
            schema_hash,
            status: SchemaHeadStatus::Active,
        };
        head.validate()?;
        Ok(head)
    }

    pub fn validate(&self) -> Result<()> {
        self.key.validate()?;
        if self.generation == 0 {
            return Err(CdfError::contract(
                "schema authority head generation must be positive",
            ));
        }
        if let SchemaHeadStatus::Promoting {
            promotion_id,
            from_schema_hash,
            to_schema_hash,
            lease_owner,
            fencing_token,
        } = &self.status
        {
            PromotionId::new(promotion_id.as_str()).map(drop)?;
            LeaseOwnerId::new(lease_owner.as_str()).map(drop)?;
            FencingToken::new(fencing_token.get()).map(drop)?;
            if from_schema_hash != &self.schema_hash {
                return Err(CdfError::contract(
                    "promoting schema head must retain its source schema hash",
                ));
            }
            if from_schema_hash == to_schema_hash {
                return Err(CdfError::contract(
                    "schema promotion target must differ from its source",
                ));
            }
        }
        Ok(())
    }

    pub fn exact_precondition(&self) -> SchemaAuthorityPrecondition {
        SchemaAuthorityPrecondition::Exact {
            generation: self.generation,
            schema_hash: self.schema_hash.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaAuthorityPrecondition {
    Absent,
    Exact {
        generation: u64,
        schema_hash: SchemaHash,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaAuthorityCheck {
    pub key: SchemaAuthorityKey,
    pub precondition: SchemaAuthorityPrecondition,
}

impl SchemaAuthorityCheck {
    pub fn new(key: SchemaAuthorityKey, precondition: SchemaAuthorityPrecondition) -> Result<Self> {
        key.validate()?;
        precondition.validate()?;
        Ok(Self { key, precondition })
    }

    pub fn validate(&self) -> Result<()> {
        Self::new(self.key.clone(), self.precondition.clone()).map(drop)
    }
}

impl SchemaAuthorityPrecondition {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Absent => Ok(()),
            Self::Exact {
                generation,
                schema_hash,
            } => {
                if *generation == 0 {
                    return Err(CdfError::contract(
                        "schema authority precondition generation must be positive",
                    ));
                }
                SchemaHash::new(schema_hash.as_str()).map(drop)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaAuthorityEstablishment {
    pub key: SchemaAuthorityKey,
    pub version: SchemaVersion,
}

impl SchemaAuthorityEstablishment {
    pub fn new(key: SchemaAuthorityKey, version: SchemaVersion) -> Result<Self> {
        key.validate()?;
        version.validate()?;
        if !matches!(version.provenance, SchemaVersionProvenance::FirstUse) {
            return Err(CdfError::contract(
                "schema authority establishment requires first-use provenance",
            ));
        }
        Ok(Self { key, version })
    }

    pub fn validate(&self) -> Result<()> {
        Self::new(self.key.clone(), self.version.clone()).map(drop)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionFence {
    pub authority_domain_id: LeaseAuthorityDomainId,
    pub promotion_id: PromotionId,
    pub lease: ScopeLease,
}

/// A renewable, generation-bound capability to cross one destination settlement boundary.
///
/// The state store, rather than the caller's clock, owns validity. A permit is deliberately
/// resource/run scoped: packaging does not require it, and it is acquired only when a verified
/// package is ready to mutate its destination.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaSettlementPermit {
    pub key: SchemaAuthorityKey,
    pub run_id: RunId,
    pub generation: u64,
    pub schema_hash: SchemaHash,
    pub acquired_at_ms: i64,
    pub expires_at_ms: i64,
}

impl SchemaSettlementPermit {
    pub fn validate(&self) -> Result<()> {
        self.key.validate()?;
        RunId::new(self.run_id.as_str()).map(drop)?;
        SchemaHash::new(self.schema_hash.as_str()).map(drop)?;
        if self.generation == 0
            || self.acquired_at_ms < 0
            || self.expires_at_ms <= self.acquired_at_ms
        {
            return Err(CdfError::contract(
                "schema settlement permit generation and lifetime must be valid",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionTarget {
    pub destination_id: DestinationId,
    pub target: TargetName,
}

impl SchemaPromotionTarget {
    pub fn validate(&self) -> Result<()> {
        DestinationId::new(self.destination_id.as_str()).map(drop)?;
        TargetName::new(self.target.as_str()).map(drop)
    }
}

/// Canonical, credential-free dry-plan authority persisted before promotion work begins.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionPlanState {
    pub promotion_id: PromotionId,
    pub plan_sha256: String,
    pub canonical_plan_json: String,
    pub required_targets: Vec<SchemaPromotionTarget>,
    pub residual_summary_sha256s: Vec<String>,
    pub created_at_ms: i64,
}

impl SchemaPromotionPlanState {
    pub fn new(
        promotion_id: PromotionId,
        canonical_plan_json: String,
        required_targets: Vec<SchemaPromotionTarget>,
        residual_summary_sha256s: Vec<String>,
        created_at_ms: i64,
    ) -> Result<Self> {
        let plan_sha256 = sha256(canonical_plan_json.as_bytes());
        let state = Self {
            promotion_id,
            plan_sha256,
            canonical_plan_json,
            required_targets,
            residual_summary_sha256s,
            created_at_ms,
        };
        state.validate()?;
        Ok(state)
    }

    pub fn validate(&self) -> Result<()> {
        PromotionId::new(self.promotion_id.as_str()).map(drop)?;
        if self.created_at_ms < 0 || self.required_targets.is_empty() {
            return Err(CdfError::contract(
                "schema promotion plan requires a creation time and at least one target",
            ));
        }
        let parsed: serde_json::Value = serde_json::from_str(&self.canonical_plan_json)
            .map_err(|error| CdfError::contract(format!("parse schema promotion plan: {error}")))?;
        let canonical = serde_json::to_string(&parsed)
            .map_err(|error| CdfError::internal(format!("canonicalize promotion plan: {error}")))?;
        if canonical != self.canonical_plan_json || self.plan_sha256 != sha256(canonical.as_bytes())
        {
            return Err(CdfError::contract(
                "schema promotion plan bytes are not canonical or do not match their SHA-256",
            ));
        }
        for target in &self.required_targets {
            target.validate()?;
        }
        if self
            .required_targets
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(CdfError::contract(
                "schema promotion targets must be unique and sorted",
            ));
        }
        for summary in &self.residual_summary_sha256s {
            validate_sha256("residual summary", summary)?;
        }
        if self
            .residual_summary_sha256s
            .windows(2)
            .any(|pair| pair[0] >= pair[1])
        {
            return Err(CdfError::contract(
                "schema promotion residual summaries must be unique and sorted",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionCutoffCheckpoint {
    pub checkpoint_id: CheckpointId,
    pub package_hash: PackageHash,
    pub run_id: RunId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionCutoff {
    pub generation: u64,
    pub schema_hash: SchemaHash,
    pub established_at_ms: i64,
    pub checkpoints: Vec<SchemaPromotionCutoffCheckpoint>,
}

impl SchemaPromotionCutoff {
    pub fn validate(&self) -> Result<()> {
        if self.generation == 0 || self.established_at_ms < 0 {
            return Err(CdfError::contract(
                "schema promotion cutoff generation and time must be valid",
            ));
        }
        SchemaHash::new(self.schema_hash.as_str()).map(drop)?;
        for checkpoint in &self.checkpoints {
            CheckpointId::new(checkpoint.checkpoint_id.as_str()).map(drop)?;
            PackageHash::new(checkpoint.package_hash.as_str()).map(drop)?;
            RunId::new(checkpoint.run_id.as_str()).map(drop)?;
        }
        if self.checkpoints.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err(CdfError::contract(
                "schema promotion cutoff checkpoints must be unique and sorted",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionTargetSettlement {
    pub target: SchemaPromotionTarget,
    pub correction_package_hash: PackageHash,
    pub receipt_id: ReceiptId,
    pub checkpoint_id: CheckpointId,
    pub settled_at_ms: i64,
}

impl SchemaPromotionTargetSettlement {
    pub fn validate(&self) -> Result<()> {
        self.target.validate()?;
        PackageHash::new(self.correction_package_hash.as_str()).map(drop)?;
        ReceiptId::new(self.receipt_id.as_str()).map(drop)?;
        CheckpointId::new(self.checkpoint_id.as_str()).map(drop)?;
        if self.settled_at_ms < 0 {
            return Err(CdfError::contract(
                "schema promotion target settlement time must be non-negative",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchemaPromotionLifecyclePhase {
    Fenced,
    CutoffEstablished,
    Published,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaPromotionState {
    pub key: SchemaAuthorityKey,
    pub plan: SchemaPromotionPlanState,
    pub from_generation: u64,
    pub from_schema_hash: SchemaHash,
    pub to_schema_hash: SchemaHash,
    pub phase: SchemaPromotionLifecyclePhase,
    pub cutoff: Option<SchemaPromotionCutoff>,
    pub target_settlements: Vec<SchemaPromotionTargetSettlement>,
    pub published_generation: Option<u64>,
    pub updated_at_ms: i64,
}

impl SchemaPromotionState {
    pub fn validate(&self) -> Result<()> {
        self.key.validate()?;
        self.plan.validate()?;
        if self.from_generation == 0
            || self.from_schema_hash == self.to_schema_hash
            || self.updated_at_ms < self.plan.created_at_ms
        {
            return Err(CdfError::contract(
                "schema promotion state carries an invalid generation, schema transition, or time",
            ));
        }
        for settlement in &self.target_settlements {
            settlement.validate()?;
        }
        if self
            .target_settlements
            .windows(2)
            .any(|pair| pair[0].target >= pair[1].target)
        {
            return Err(CdfError::contract(
                "schema promotion settlements must be unique and target-sorted",
            ));
        }
        match self.phase {
            SchemaPromotionLifecyclePhase::Fenced => {
                if self.cutoff.is_none()
                    && self.target_settlements.is_empty()
                    && self.published_generation.is_none()
                {
                    Ok(())
                } else {
                    Err(invalid_promotion_lifecycle())
                }
            }
            SchemaPromotionLifecyclePhase::CutoffEstablished => {
                if self.published_generation.is_some() {
                    return Err(invalid_promotion_lifecycle());
                }
                self.cutoff
                    .as_ref()
                    .ok_or_else(invalid_promotion_lifecycle)?
                    .validate()
            }
            SchemaPromotionLifecyclePhase::Published => {
                if self.published_generation != self.from_generation.checked_add(1) {
                    return Err(invalid_promotion_lifecycle());
                }
                self.cutoff
                    .as_ref()
                    .ok_or_else(invalid_promotion_lifecycle)?
                    .validate()
            }
        }
    }
}

fn invalid_promotion_lifecycle() -> CdfError {
    CdfError::contract(
        "schema promotion lifecycle phase does not match its cutoff/publication state",
    )
}

fn sha256(bytes: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

fn validate_sha256(name: &str, value: &str) -> Result<()> {
    let digest = value
        .strip_prefix("sha256:")
        .ok_or_else(|| CdfError::contract(format!("{name} must use a sha256: content identity")))?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CdfError::contract(format!(
            "{name} must contain a 64-digit SHA-256"
        )));
    }
    Ok(())
}

impl SchemaPromotionFence {
    pub fn new(
        authority_domain_id: LeaseAuthorityDomainId,
        promotion_id: PromotionId,
        lease: ScopeLease,
    ) -> Result<Self> {
        let fence = Self {
            authority_domain_id,
            promotion_id,
            lease,
        };
        fence.validate()?;
        Ok(fence)
    }

    pub fn validate(&self) -> Result<()> {
        LeaseAuthorityDomainId::new(self.authority_domain_id.as_str()).map(drop)?;
        PromotionId::new(self.promotion_id.as_str()).map(drop)?;
        LeaseOwnerId::new(self.lease.owner.as_str()).map(drop)?;
        FencingToken::new(self.lease.fencing_token.get()).map(drop)?;
        if self.lease.acquired_at_ms < 0 || self.lease.expires_at_ms <= self.lease.acquired_at_ms {
            return Err(CdfError::contract(
                "schema promotion fence carries an invalid lease lifetime",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SchemaAuthorityEventKind {
    Established,
    PromotionBegun {
        promotion_id: PromotionId,
        from_schema_hash: SchemaHash,
        to_schema_hash: SchemaHash,
        lease_owner: LeaseOwnerId,
        fencing_token: FencingToken,
    },
    PromotionResumed {
        promotion_id: PromotionId,
        from_schema_hash: SchemaHash,
        to_schema_hash: SchemaHash,
        lease_owner: LeaseOwnerId,
        fencing_token: FencingToken,
    },
    PromotionCutoffEstablished {
        promotion_id: PromotionId,
        checkpoint_count: u64,
    },
    PromotionTargetSettled {
        promotion_id: PromotionId,
        destination_id: DestinationId,
        target: TargetName,
        checkpoint_id: CheckpointId,
        receipt_id: ReceiptId,
    },
    PromotionPublished {
        promotion_id: PromotionId,
        from_schema_hash: SchemaHash,
        to_schema_hash: SchemaHash,
        lease_owner: LeaseOwnerId,
        fencing_token: FencingToken,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaAuthorityEvent {
    pub key: SchemaAuthorityKey,
    pub ordinal: u64,
    pub generation: u64,
    pub schema_hash: SchemaHash,
    pub recorded_at_ms: i64,
    pub kind: SchemaAuthorityEventKind,
}

impl SchemaAuthorityEvent {
    pub fn validate(&self) -> Result<()> {
        self.key.validate()?;
        if self.ordinal == 0 || self.generation == 0 || self.recorded_at_ms < 0 {
            return Err(CdfError::contract(
                "schema authority event ordinal, generation, and time must be valid",
            ));
        }
        match &self.kind {
            SchemaAuthorityEventKind::Established => Ok(()),
            SchemaAuthorityEventKind::PromotionCutoffEstablished {
                promotion_id,
                checkpoint_count: _,
            } => PromotionId::new(promotion_id.as_str()).map(drop),
            SchemaAuthorityEventKind::PromotionTargetSettled {
                promotion_id,
                destination_id,
                target,
                checkpoint_id,
                receipt_id,
            } => {
                PromotionId::new(promotion_id.as_str()).map(drop)?;
                DestinationId::new(destination_id.as_str()).map(drop)?;
                TargetName::new(target.as_str()).map(drop)?;
                CheckpointId::new(checkpoint_id.as_str()).map(drop)?;
                ReceiptId::new(receipt_id.as_str()).map(drop)
            }
            SchemaAuthorityEventKind::PromotionBegun {
                promotion_id,
                from_schema_hash,
                to_schema_hash,
                lease_owner,
                fencing_token,
            }
            | SchemaAuthorityEventKind::PromotionResumed {
                promotion_id,
                from_schema_hash,
                to_schema_hash,
                lease_owner,
                fencing_token,
            }
            | SchemaAuthorityEventKind::PromotionPublished {
                promotion_id,
                from_schema_hash,
                to_schema_hash,
                lease_owner,
                fencing_token,
            } => {
                PromotionId::new(promotion_id.as_str()).map(drop)?;
                LeaseOwnerId::new(lease_owner.as_str()).map(drop)?;
                FencingToken::new(fencing_token.get()).map(drop)?;
                if from_schema_hash == to_schema_hash {
                    return Err(CdfError::contract(
                        "schema authority promotion event must change schema hash",
                    ));
                }
                match &self.kind {
                    SchemaAuthorityEventKind::PromotionBegun { .. }
                    | SchemaAuthorityEventKind::PromotionResumed { .. }
                        if &self.schema_hash != from_schema_hash =>
                    {
                        Err(CdfError::contract(
                            "promotion begin/resume event must retain the source schema hash",
                        ))
                    }
                    SchemaAuthorityEventKind::PromotionPublished { .. }
                        if &self.schema_hash != to_schema_hash =>
                    {
                        Err(CdfError::contract(
                            "promotion-published event must carry the target schema hash",
                        ))
                    }
                    _ => Ok(()),
                }
            }
        }
    }
}

pub trait SchemaAuthorityStore: Send + Sync {
    fn authority_domain_id(&self) -> LeaseAuthorityDomainId;

    fn head(&self, key: &SchemaAuthorityKey) -> Result<Option<SchemaHead>>;

    fn version(
        &self,
        key: &SchemaAuthorityKey,
        schema_hash: &SchemaHash,
    ) -> Result<Option<SchemaVersion>>;

    fn establish_if_absent(
        &self,
        establishment: SchemaAuthorityEstablishment,
    ) -> Result<SchemaHead> {
        self.establish_batch_if_absent(vec![establishment])?
            .into_iter()
            .next()
            .ok_or_else(|| CdfError::internal("single schema establishment returned no head"))
    }

    fn establish_batch_if_absent(
        &self,
        establishments: Vec<SchemaAuthorityEstablishment>,
    ) -> Result<Vec<SchemaHead>>;

    fn establish_batch_checked(
        &self,
        checks: Vec<SchemaAuthorityCheck>,
        establishments: Vec<SchemaAuthorityEstablishment>,
    ) -> Result<Vec<SchemaHead>>;

    fn begin_promotion(
        &self,
        expected: &SchemaHead,
        proposed: SchemaVersion,
        plan: SchemaPromotionPlanState,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaPromotionState>;

    fn resume_promotion(
        &self,
        expected_source: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead>;

    fn promotion_state(
        &self,
        key: &SchemaAuthorityKey,
        promotion_id: &PromotionId,
    ) -> Result<Option<SchemaPromotionState>>;

    fn establish_promotion_cutoff(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaPromotionState>;

    fn commit_promotion_target(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
        target: &SchemaPromotionTarget,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<SchemaPromotionState>;

    fn publish_promotion(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead>;

    fn history(&self, key: &SchemaAuthorityKey, limit: u32) -> Result<Vec<SchemaAuthorityEvent>>;
}

/// State-atomic ordinary-run settlement fencing for one schema authority domain.
///
/// Implementations MUST serialize permit acquisition with promotion begin, and MUST validate the
/// exact permit, head generation, schema hash, receipt, and checkpoint commit in one transaction.
pub trait SchemaSettlementStore: Send + Sync {
    fn acquire_run_permit(
        &self,
        expected_active: &SchemaHead,
        run_id: RunId,
        permit_duration_ms: u64,
    ) -> Result<SchemaSettlementPermit>;

    fn renew_run_permit(
        &self,
        permit: &SchemaSettlementPermit,
        permit_duration_ms: u64,
    ) -> Result<SchemaSettlementPermit>;

    fn assert_run_permit(&self, permit: &SchemaSettlementPermit) -> Result<()>;

    fn release_run_permit(&self, permit: &SchemaSettlementPermit) -> Result<()>;

    fn commit_run_checkpoint(
        &self,
        permit: &SchemaSettlementPermit,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<Checkpoint>;
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn canonical_schema(field_name: &str) -> CanonicalArrowSchema {
        CanonicalArrowSchema::from_arrow(&Schema::new(vec![Field::new(
            field_name,
            DataType::Int64,
            true,
        )]))
        .unwrap()
    }

    fn key(project: &str, environment: &str) -> SchemaAuthorityKey {
        SchemaAuthorityKey::new(
            LeaseAuthorityDomainId::new("state-domain").unwrap(),
            ProjectId::new(project).unwrap(),
            EnvironmentName::new(environment).unwrap(),
            ResourceId::new("orders").unwrap(),
            OutputBindingId::new(crate::PRIMARY_OUTPUT_BINDING).unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn schema_version_validation_recomputes_canonical_hash() {
        let mut version = SchemaVersion::new(
            canonical_schema("order_id"),
            None,
            None,
            1,
            SchemaVersionProvenance::FirstUse,
        )
        .unwrap();
        version.schema_hash = SchemaHash::new("sha256:tampered").unwrap();

        assert!(version.validate().is_err());
    }

    #[test]
    fn schema_authority_lease_scope_is_delimiter_safe() {
        let first = key("a:b", "c").promotion_scope().unwrap();
        let second = key("a", "b:c").promotion_scope().unwrap();
        let mut routed = key("a:b", "c");
        routed.output_binding = OutputBindingId::new("route_east").unwrap();

        assert_ne!(first, second);
        assert_ne!(first, routed.promotion_scope().unwrap());
    }

    #[test]
    fn exact_precondition_requires_positive_generation() {
        let precondition = SchemaAuthorityPrecondition::Exact {
            generation: 0,
            schema_hash: SchemaHash::new("sha256:schema").unwrap(),
        };

        assert!(precondition.validate().is_err());
    }
}
