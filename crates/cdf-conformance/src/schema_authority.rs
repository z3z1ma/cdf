use arrow_schema::{DataType, Field, Schema};
use cdf_kernel::{
    CanonicalArrowSchema, DestinationId, EnvironmentName, LeaseAuthorityDomainId, LeaseOwnerId,
    ProjectId, PromotionId, ResourceId, SchemaAuthorityCheck, SchemaAuthorityEstablishment,
    SchemaAuthorityEventKind, SchemaAuthorityKey, SchemaAuthorityPrecondition,
    SchemaAuthorityStore, SchemaHeadStatus, SchemaPromotionFence, SchemaPromotionPlanState,
    SchemaPromotionTarget, SchemaVersion, SchemaVersionProvenance, ScopeLeaseStore, TargetName,
};

pub fn assert_schema_authority_store_send_sync<S: SchemaAuthorityStore + Send + Sync>() {}

pub fn assert_schema_authority_store_conformance<S, L, F>(mut fresh_store: F)
where
    S: SchemaAuthorityStore,
    L: ScopeLeaseStore,
    F: FnMut() -> (S, L),
{
    assert_schema_authority_store_send_sync::<S>();
    assert_first_use_is_exact_and_idempotent(&fresh_store().0);
    assert_batch_is_all_or_none(&fresh_store().0);
    assert_checked_batch_fences_all_selected_resources(&fresh_store().0);
    assert_key_isolation_and_bounded_history(&fresh_store().0);
    let (first, _) = fresh_store();
    let (second, _) = fresh_store();
    assert_foreign_domain_is_rejected(&first, &second);
    let (store, leases) = fresh_store();
    assert_fenced_promotion(&store, &leases);
}

pub fn schema_authority_key<S: SchemaAuthorityStore>(
    store: &S,
    environment: &str,
    resource: &str,
) -> SchemaAuthorityKey {
    SchemaAuthorityKey::new(
        store.authority_domain_id(),
        ProjectId::new("project-01").unwrap(),
        EnvironmentName::new(environment).unwrap(),
        ResourceId::new(resource).unwrap(),
        cdf_kernel::OutputBindingId::new(cdf_kernel::PRIMARY_OUTPUT_BINDING).unwrap(),
    )
    .unwrap()
}

pub fn schema_authority_version(field: &str, predecessor: Option<&SchemaVersion>) -> SchemaVersion {
    let schema = CanonicalArrowSchema::from_arrow(&Schema::new(vec![Field::new(
        field,
        DataType::Int64,
        true,
    )]))
    .unwrap();
    match predecessor {
        Some(predecessor) => SchemaVersion::new(
            schema,
            Some(predecessor.schema_hash.clone()),
            None,
            2_000,
            SchemaVersionProvenance::Promotion {
                promotion_id: PromotionId::new("promotion-01").unwrap(),
            },
        )
        .unwrap(),
        None => SchemaVersion::new(schema, None, None, 1_000, SchemaVersionProvenance::FirstUse)
            .unwrap(),
    }
}

pub fn first_use_schema_authority_establishment<S: SchemaAuthorityStore>(
    store: &S,
    environment: &str,
    resource: &str,
    field: &str,
) -> SchemaAuthorityEstablishment {
    SchemaAuthorityEstablishment::new(
        schema_authority_key(store, environment, resource),
        schema_authority_version(field, None),
    )
    .unwrap()
}

fn assert_first_use_is_exact_and_idempotent<S: SchemaAuthorityStore>(store: &S) {
    let proposed = first_use_schema_authority_establishment(store, "dev", "orders", "order_id");
    assert!(store.head(&proposed.key).unwrap().is_none());

    let first = store.establish_if_absent(proposed.clone()).unwrap();
    assert_eq!(first.generation, 1);
    assert!(matches!(first.status, SchemaHeadStatus::Active));
    assert_eq!(store.establish_if_absent(proposed.clone()).unwrap(), first);
    assert_eq!(
        store
            .version(&proposed.key, &proposed.version.schema_hash)
            .unwrap(),
        Some(proposed.version.clone())
    );

    let conflict =
        first_use_schema_authority_establishment(store, "dev", "orders", "different_order_id");
    assert!(store.establish_if_absent(conflict).is_err());
    assert_eq!(store.head(&proposed.key).unwrap(), Some(first));
    assert_eq!(store.history(&proposed.key, 10).unwrap().len(), 1);
}

fn assert_batch_is_all_or_none<S: SchemaAuthorityStore>(store: &S) {
    let existing = first_use_schema_authority_establishment(store, "dev", "existing", "id");
    store.establish_if_absent(existing.clone()).unwrap();
    let absent = first_use_schema_authority_establishment(store, "dev", "absent", "id");
    let conflicting =
        first_use_schema_authority_establishment(store, "dev", "existing", "other_id");

    assert!(
        store
            .establish_batch_if_absent(vec![absent.clone(), conflicting])
            .is_err()
    );
    assert!(store.head(&absent.key).unwrap().is_none());

    let second = first_use_schema_authority_establishment(store, "dev", "second", "id");
    let heads = store
        .establish_batch_if_absent(vec![absent.clone(), second.clone()])
        .unwrap();
    assert_eq!(heads.len(), 2);
    assert!(store.head(&absent.key).unwrap().is_some());
    assert!(store.head(&second.key).unwrap().is_some());
}

fn assert_checked_batch_fences_all_selected_resources<S: SchemaAuthorityStore>(store: &S) {
    let active = first_use_schema_authority_establishment(store, "dev", "active", "id");
    let active_head = store.establish_if_absent(active).unwrap();
    let proposed = first_use_schema_authority_establishment(store, "dev", "proposed", "id");
    store
        .establish_batch_checked(
            vec![
                SchemaAuthorityCheck::new(
                    active_head.key.clone(),
                    active_head.exact_precondition(),
                )
                .unwrap(),
                SchemaAuthorityCheck::new(
                    proposed.key.clone(),
                    SchemaAuthorityPrecondition::Absent,
                )
                .unwrap(),
            ],
            vec![proposed.clone()],
        )
        .unwrap();

    let blocked = first_use_schema_authority_establishment(store, "dev", "blocked", "id");
    let stale = SchemaAuthorityPrecondition::Exact {
        generation: active_head.generation + 1,
        schema_hash: active_head.schema_hash.clone(),
    };
    assert!(
        store
            .establish_batch_checked(
                vec![
                    SchemaAuthorityCheck::new(active_head.key.clone(), stale).unwrap(),
                    SchemaAuthorityCheck::new(
                        blocked.key.clone(),
                        SchemaAuthorityPrecondition::Absent,
                    )
                    .unwrap(),
                ],
                vec![blocked.clone()],
            )
            .is_err()
    );
    assert!(store.head(&blocked.key).unwrap().is_none());
}

fn assert_key_isolation_and_bounded_history<S: SchemaAuthorityStore>(store: &S) {
    let dev_a = first_use_schema_authority_establishment(store, "dev", "a", "id");
    let prod_a = first_use_schema_authority_establishment(store, "prod", "a", "id");
    let dev_b = first_use_schema_authority_establishment(store, "dev", "b", "id");
    store
        .establish_batch_if_absent(vec![dev_a.clone(), prod_a.clone(), dev_b.clone()])
        .unwrap();

    assert_ne!(dev_a.key, prod_a.key);
    assert_ne!(dev_a.key, dev_b.key);
    assert_eq!(store.history(&dev_a.key, 1).unwrap().len(), 1);
    assert!(store.history(&dev_a.key, 0).is_err());
    assert!(
        store
            .history(
                &dev_a.key,
                cdf_kernel::MAX_SCHEMA_AUTHORITY_HISTORY_LIMIT + 1
            )
            .is_err()
    );
}

fn assert_foreign_domain_is_rejected<S: SchemaAuthorityStore>(first: &S, second: &S) {
    assert_ne!(first.authority_domain_id(), second.authority_domain_id());
    let foreign = first_use_schema_authority_establishment(first, "dev", "orders", "id");
    assert!(second.head(&foreign.key).is_err());
    assert!(second.establish_if_absent(foreign).is_err());
}

fn assert_fenced_promotion<S: SchemaAuthorityStore, L: ScopeLeaseStore>(store: &S, leases: &L) {
    assert_eq!(store.authority_domain_id(), leases.authority_domain_id());
    let establishment = first_use_schema_authority_establishment(store, "dev", "promoted", "id");
    let first_version = establishment.version.clone();
    let active = store.establish_if_absent(establishment).unwrap();
    let proposed = schema_authority_version("promoted_value", Some(&first_version));
    let lease = leases
        .acquire(
            active.key.promotion_scope().unwrap(),
            LeaseOwnerId::new("promoter").unwrap(),
            60_000,
        )
        .unwrap();
    let fence = SchemaPromotionFence::new(
        store.authority_domain_id(),
        PromotionId::new("promotion-01").unwrap(),
        lease,
    )
    .unwrap();
    let foreign_fence = SchemaPromotionFence::new(
        LeaseAuthorityDomainId::new("foreign-state-domain").unwrap(),
        fence.promotion_id.clone(),
        fence.lease.clone(),
    )
    .unwrap();
    assert!(
        store
            .begin_promotion(
                &active,
                proposed.clone(),
                promotion_plan(&foreign_fence.promotion_id),
                &foreign_fence
            )
            .is_err()
    );

    let state = store
        .begin_promotion(
            &active,
            proposed.clone(),
            promotion_plan(&fence.promotion_id),
            &fence,
        )
        .unwrap();
    assert_eq!(state.from_generation, active.generation);
    let promoting = store.head(&active.key).unwrap().unwrap();
    assert!(matches!(
        promoting.status,
        SchemaHeadStatus::Promoting { .. }
    ));
    assert_eq!(promoting.generation, active.generation);
    assert_eq!(store.head(&active.key).unwrap(), Some(promoting.clone()));

    let cutoff = store
        .establish_promotion_cutoff(&promoting, &fence)
        .unwrap();
    assert!(cutoff.cutoff.is_some());
    assert!(store.publish_promotion(&promoting, &fence).is_err());
    let history = store.history(&active.key, 10).unwrap();
    assert_eq!(history.len(), 3);
    assert!(matches!(
        history[0].kind,
        SchemaAuthorityEventKind::Established
    ));
    assert!(matches!(
        history[1].kind,
        SchemaAuthorityEventKind::PromotionBegun { .. }
    ));
    assert!(matches!(
        history[2].kind,
        SchemaAuthorityEventKind::PromotionCutoffEstablished { .. }
    ));
    assert_eq!(
        history
            .iter()
            .map(|event| event.ordinal)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        store
            .history(&active.key, 2)
            .unwrap()
            .iter()
            .map(|event| event.ordinal)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
}

fn promotion_plan(promotion_id: &PromotionId) -> SchemaPromotionPlanState {
    SchemaPromotionPlanState::new(
        promotion_id.clone(),
        "{}".to_owned(),
        vec![SchemaPromotionTarget {
            destination_id: DestinationId::new("duckdb").unwrap(),
            target: TargetName::new("promoted").unwrap(),
        }],
        Vec::new(),
        1_500,
    )
    .unwrap()
}
