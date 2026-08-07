use std::{
    cell::RefCell,
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use cdf_kernel::{
    CanonicalArrowSchema, CdfError, EnvironmentName, LeaseAuthorityDomainId, ResourceId,
    SchemaAuthorityCheck, SchemaAuthorityEstablishment, SchemaAuthorityKey,
    SchemaAuthorityPrecondition, SchemaAuthorityStore, SchemaHead, SchemaHeadStatus, SchemaVersion,
    SchemaVersionProvenance,
};
use cdf_project::CompiledSchemaAuthority;
use cdf_state_sqlite::{SqliteSchemaAuthorityState, SqliteSchemaAuthorityStore};

use crate::{context::ProjectContext, output::CliError};

#[derive(Clone, Debug)]
pub(crate) enum PreparedSchemaAuthority {
    Active {
        head: SchemaHead,
    },
    Proposed {
        establishment: SchemaAuthorityEstablishment,
    },
}

impl PreparedSchemaAuthority {
    pub(crate) fn key(&self) -> &SchemaAuthorityKey {
        match self {
            Self::Active { head } => &head.key,
            Self::Proposed { establishment } => &establishment.key,
        }
    }

    pub(crate) fn precondition(&self) -> SchemaAuthorityPrecondition {
        match self {
            Self::Active { head } => head.exact_precondition(),
            Self::Proposed { .. } => SchemaAuthorityPrecondition::Absent,
        }
    }

    pub(crate) fn compiled_authority(&self) -> Result<CompiledSchemaAuthority, CliError> {
        match self {
            Self::Active { head } => CompiledSchemaAuthority::from_head(head).map_err(Into::into),
            Self::Proposed { establishment } => Ok(CompiledSchemaAuthority {
                key: establishment.key.clone(),
                generation: 1,
                schema_hash: establishment.version.schema_hash.clone(),
            }),
        }
    }

    pub(crate) fn proposal(&self) -> Option<&SchemaAuthorityEstablishment> {
        match self {
            Self::Active { .. } => None,
            Self::Proposed { establishment } => Some(establishment),
        }
    }

    pub(crate) fn status_name(&self) -> &'static str {
        match self {
            Self::Active { .. } => "active",
            Self::Proposed { .. } => "proposed_first_use",
        }
    }
}

pub(crate) fn prepare(
    context: &ProjectContext,
    resource: &cdf_declarative::CompiledResource,
) -> Result<PreparedSchemaAuthority, CliError> {
    let state_path = context.state_store_path()?;
    let ownership = context.state_store_path_ownership();
    let state = SqliteSchemaAuthorityStore::inspect_state(&state_path, ownership)?;
    let (authority_domain_id, ready) = match state {
        SqliteSchemaAuthorityState::Missing => (proposed_domain_id(), false),
        SqliteSchemaAuthorityState::Uninitialized {
            authority_domain_id,
        } => (
            authority_domain_id.unwrap_or_else(proposed_domain_id),
            false,
        ),
        SqliteSchemaAuthorityState::Ready {
            authority_domain_id,
        } => (authority_domain_id, true),
    };
    let key = SchemaAuthorityKey::new(
        authority_domain_id,
        context.config.project.id.clone(),
        EnvironmentName::new(context.environment.name.clone())?,
        ResourceId::new(resource.descriptor().resource_id.to_string())?,
    )?;
    let canonical_schema = CanonicalArrowSchema::from_arrow(resource.schema().as_ref())?;
    let mut proposed = SchemaVersion::new(
        canonical_schema,
        None,
        None,
        now_ms()?,
        SchemaVersionProvenance::FirstUse,
    )?;
    if let Some(seed) = PROPOSAL_SEED.with_borrow(|seed| {
        seed.versions
            .get(resource.descriptor().resource_id.as_str())
            .filter(|seeded| seeded.schema_hash == proposed.schema_hash)
            .cloned()
    }) {
        proposed = seed;
    }
    if !ready {
        return Ok(PreparedSchemaAuthority::Proposed {
            establishment: SchemaAuthorityEstablishment::new(key, proposed)?,
        });
    }
    let store =
        SqliteSchemaAuthorityStore::open_read_only_with_path_ownership(&state_path, ownership)?;
    let Some(head) = store.head(&key)? else {
        return Ok(PreparedSchemaAuthority::Proposed {
            establishment: SchemaAuthorityEstablishment::new(key, proposed)?,
        });
    };
    if !matches!(head.status, SchemaHeadStatus::Active) {
        return Err(CdfError::contract(format!(
            "schema authority for `{}` is being promoted; retry after promotion settles",
            resource.descriptor().resource_id
        ))
        .into());
    }
    let stored = store
        .version(&key, &head.schema_hash)?
        .ok_or_else(|| CdfError::internal("active schema authority has no immutable version"))?;
    if stored.schema_hash != proposed.schema_hash {
        return Err(CdfError::contract(format!(
            "resource `{}` discovered schema {} but state authority is generation {} schema {}; run `cdf schema promote {}` to review the drift",
            resource.descriptor().resource_id,
            proposed.schema_hash,
            head.generation,
            head.schema_hash,
            resource.descriptor().resource_id,
        ))
        .into());
    }
    Ok(PreparedSchemaAuthority::Active { head })
}

pub(crate) fn commit_one_idempotent(
    context: &ProjectContext,
    prepared: &PreparedSchemaAuthority,
) -> Result<(), CliError> {
    let domain = &prepared.key().authority_domain_id;
    let store = SqliteSchemaAuthorityStore::open_with_authority_domain_and_path_ownership(
        context.state_store_path()?,
        domain,
        context.state_store_path_ownership(),
    )?;
    match prepared {
        PreparedSchemaAuthority::Active { .. } => {
            store.establish_batch_checked(
                vec![SchemaAuthorityCheck::new(
                    prepared.key().clone(),
                    prepared.precondition(),
                )?],
                Vec::new(),
            )?;
        }
        PreparedSchemaAuthority::Proposed { establishment } => {
            store.establish_if_absent(establishment.clone())?;
        }
    }
    Ok(())
}

pub(crate) fn commit_at(
    state_path: std::path::PathBuf,
    ownership: cdf_state_sqlite::StateStorePathOwnership,
    prepared: &[PreparedSchemaAuthority],
) -> Result<(), CliError> {
    let Some(first) = prepared.first() else {
        return Ok(());
    };
    let domain = first.key().authority_domain_id.clone();
    if prepared
        .iter()
        .any(|authority| authority.key().authority_domain_id != domain)
    {
        return Err(CdfError::internal(
            "selected schema preparations resolved different state authority domains",
        )
        .into());
    }
    let store = SqliteSchemaAuthorityStore::open_with_authority_domain_and_path_ownership(
        state_path, &domain, ownership,
    )?;
    let checks = prepared
        .iter()
        .map(|authority| {
            SchemaAuthorityCheck::new(authority.key().clone(), authority.precondition())
        })
        .collect::<cdf_kernel::Result<Vec<_>>>()?;
    let proposals = prepared
        .iter()
        .filter_map(PreparedSchemaAuthority::proposal)
        .cloned()
        .collect();
    store.establish_batch_checked(checks, proposals)?;
    Ok(())
}

fn proposed_domain_id() -> LeaseAuthorityDomainId {
    PROPOSAL_SEED.with_borrow_mut(|seed| {
        seed.authority_domain_id
            .get_or_insert_with(|| {
                LeaseAuthorityDomainId::new(format!("lease-{}", uuid::Uuid::new_v4()))
                    .expect("generated UUID domain is non-empty")
            })
            .clone()
    })
}

#[derive(Default)]
struct ProposalSeed {
    authority_domain_id: Option<LeaseAuthorityDomainId>,
    versions: BTreeMap<String, SchemaVersion>,
}

thread_local! {
    static PROPOSAL_SEED: RefCell<ProposalSeed> = RefCell::new(ProposalSeed::default());
}

pub(crate) fn seed_portable_proposals(artifact: &cdf_project::PortablePlanArtifact) {
    let Some(first) = artifact.resources.first() else {
        return;
    };
    PROPOSAL_SEED.with_borrow_mut(|seed| {
        seed.authority_domain_id = Some(first.schema_authority.key().authority_domain_id.clone());
        seed.versions = artifact
            .resources
            .iter()
            .filter_map(|resource| {
                resource
                    .schema_authority
                    .proposed_version()
                    .map(|version| (resource.resource_id.clone(), version.clone()))
            })
            .collect();
    });
}

pub(crate) fn reset_proposal_seed() {
    PROPOSAL_SEED.with_borrow_mut(|seed| {
        *seed = ProposalSeed::default();
    });
}

fn now_ms() -> cdf_kernel::Result<i64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|error| {
        CdfError::environment(format!(
            "read the host clock for schema preparation: {error}; correct the system clock and retry"
        ))
    })?;
    i64::try_from(elapsed.as_millis()).map_err(|error| {
        CdfError::environment(format!(
            "represent schema preparation time: {error}; correct the system clock and retry"
        ))
    })
}
