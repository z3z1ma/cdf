use serde::{Deserialize, Serialize};

use crate::{
    CanonicalArrowSchema, CdfError, ContractRef, EnvironmentName, FencingToken,
    ImmutableContentIdentity, LeaseAuthorityDomainId, LeaseOwnerId, ProjectId, PromotionId,
    ResourceId, Result, SchemaHash, ScopeKey, ScopeLease, canonical_arrow_schema_hash,
};

pub const MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT: u32 = 10_000;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaAuthorityKey {
    pub authority_domain_id: LeaseAuthorityDomainId,
    pub project_id: ProjectId,
    pub environment: EnvironmentName,
    pub resource_id: ResourceId,
}

impl SchemaAuthorityKey {
    pub fn new(
        authority_domain_id: LeaseAuthorityDomainId,
        project_id: ProjectId,
        environment: EnvironmentName,
        resource_id: ResourceId,
    ) -> Result<Self> {
        let key = Self {
            authority_domain_id,
            project_id,
            environment,
            resource_id,
        };
        key.validate()?;
        Ok(key)
    }

    pub fn validate(&self) -> Result<()> {
        LeaseAuthorityDomainId::new(self.authority_domain_id.as_str()).map(drop)?;
        ProjectId::new(self.project_id.as_str()).map(drop)?;
        EnvironmentName::new(self.environment.as_str()).map(drop)?;
        ResourceId::new(self.resource_id.as_str()).map(drop)
    }

    pub fn promotion_scope(&self) -> Result<ScopeKey> {
        self.validate()?;
        let mut encoded = String::from("schema-authority");
        for part in [
            self.authority_domain_id.as_str(),
            self.project_id.as_str(),
            self.environment.as_str(),
            self.resource_id.as_str(),
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
            SchemaAuthorityEventKind::PromotionBegun {
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
                        if &self.schema_hash != from_schema_hash =>
                    {
                        Err(CdfError::contract(
                            "promotion-begun event must retain the source schema hash",
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

    fn begin_promotion(
        &self,
        expected: &SchemaHead,
        proposed: SchemaVersion,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead>;

    fn publish_promotion(
        &self,
        expected_promoting: &SchemaHead,
        fence: &SchemaPromotionFence,
    ) -> Result<SchemaHead>;

    fn history(&self, key: &SchemaAuthorityKey, limit: u32) -> Result<Vec<SchemaAuthorityEvent>>;
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

        assert_ne!(first, second);
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
