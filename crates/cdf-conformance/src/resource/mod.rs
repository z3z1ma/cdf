use std::collections::{BTreeMap, BTreeSet};

use cdf_kernel::{
    IncrementalShape, PartitionPlan, PredicateId, PushdownFidelity, QueryableResource,
    ReplaySupport, ResourceCapabilities, ResourceDescriptor, ResourceId, ResourceStream, ScanPlan,
    ScanPredicate, ScanRequest, SchemaSource, ScopeKey, ScopeKind,
};

mod execution;

pub use execution::{
    ResourceExecutionConformanceCase, SourcePositionRequirement,
    assert_resource_stream_execution_conformance,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResourceConformanceCase {
    pub request: ScanRequest,
    pub expected_predicates: Vec<PredicateExpectation>,
}

impl ResourceConformanceCase {
    pub fn new(request: ScanRequest) -> Self {
        Self {
            request,
            expected_predicates: Vec::new(),
        }
    }

    pub fn with_expected_predicates<I>(mut self, expected_predicates: I) -> Self
    where
        I: IntoIterator<Item = PredicateExpectation>,
    {
        self.expected_predicates = expected_predicates.into_iter().collect();
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PredicateExpectation {
    pub predicate_id: PredicateId,
    pub classification: PredicateClassification,
}

impl PredicateExpectation {
    pub fn exact(predicate_id: PredicateId) -> Self {
        Self::pushed(predicate_id, PushdownFidelity::Exact)
    }

    pub fn inexact(predicate_id: PredicateId) -> Self {
        Self::pushed(predicate_id, PushdownFidelity::Inexact)
    }

    pub fn pushed(predicate_id: PredicateId, fidelity: PushdownFidelity) -> Self {
        assert_ne!(
            fidelity,
            PushdownFidelity::Unsupported,
            "pushed predicate expectations must use Exact or Inexact fidelity"
        );
        Self {
            predicate_id,
            classification: PredicateClassification::Pushed(fidelity),
        }
    }

    pub fn unsupported(predicate_id: PredicateId) -> Self {
        Self {
            predicate_id,
            classification: PredicateClassification::Unsupported,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PredicateClassification {
    Pushed(PushdownFidelity),
    Unsupported,
}

pub fn assert_resource_stream_conformance<R, I>(resource: &R, requests: I)
where
    R: ResourceStream + ?Sized,
    I: IntoIterator<Item = ScanRequest>,
{
    let requests = requests.into_iter().collect::<Vec<_>>();
    assert!(
        !requests.is_empty(),
        "resource conformance requires representative scan requests"
    );

    assert_descriptor_schema_coherence(resource);

    for request in &requests {
        assert_request_targets_resource(resource.descriptor(), request);
        let partitions = resource
            .plan_partitions(request)
            .unwrap_or_else(|error| panic!("resource partition planning failed: {error}"));
        assert_partition_plans(resource.descriptor(), None, request, &partitions);
    }
}

pub fn assert_queryable_resource_conformance<R, I>(resource: &R, cases: I)
where
    R: QueryableResource + ?Sized,
    I: IntoIterator<Item = ResourceConformanceCase>,
{
    let cases = cases.into_iter().collect::<Vec<_>>();
    assert!(
        !cases.is_empty(),
        "queryable resource conformance requires representative scan cases"
    );

    assert_resource_stream_conformance(resource, cases.iter().map(|case| case.request.clone()));
    assert_capability_preconditions(resource.descriptor(), resource.capabilities());

    for case in &cases {
        assert_predicate_expectations_cover_request(case);
        assert_mismatched_resource_rejected(resource, &case.request);

        let plan = resource
            .negotiate(&case.request)
            .unwrap_or_else(|error| panic!("resource negotiation failed: {error}"));
        assert_negotiate_plan(resource, case, &plan);
    }
}

fn assert_descriptor_schema_coherence<R>(resource: &R)
where
    R: ResourceStream + ?Sized,
{
    let descriptor = resource.descriptor();
    let schema = resource.schema();
    let mut field_names = BTreeSet::new();
    for field in schema.fields() {
        assert!(
            field_names.insert(field.name().to_owned()),
            "schema field `{}` is declared more than once",
            field.name()
        );
    }
    let schema_has_fields = !field_names.is_empty();

    assert_schema_source_evidence(&descriptor.schema_source, schema_has_fields);
    assert_named_fields(
        "primary key",
        &descriptor.primary_key,
        &field_names,
        schema_has_fields,
    );
    assert_named_fields(
        "merge key",
        &descriptor.merge_key,
        &field_names,
        schema_has_fields,
    );
    if let Some(cursor) = &descriptor.cursor {
        assert!(
            !cursor.field.trim().is_empty(),
            "cursor field must not be empty"
        );
        if schema_has_fields {
            assert!(
                field_names.contains(&cursor.field),
                "cursor field `{}` must exist in Arrow schema",
                cursor.field
            );
        }
    }
}

fn assert_schema_source_evidence(schema_source: &SchemaSource, schema_has_fields: bool) {
    match schema_source {
        SchemaSource::Declared {
            schema_hash,
            source,
        } => {
            assert!(
                !schema_hash.as_str().trim().is_empty(),
                "declared schema source must carry a schema hash"
            );
            assert!(
                !source.trim().is_empty(),
                "declared schema source must name its source evidence"
            );
        }
        SchemaSource::Discover => {
            assert!(
                !schema_has_fields,
                "discovered Arrow schemas with fields must carry pinned schema snapshot evidence"
            );
        }
        SchemaSource::Discovered { snapshot } => {
            assert!(
                !snapshot.schema_hash.as_str().trim().is_empty(),
                "pinned discovered schemas must carry schema hash evidence"
            );
            assert!(
                !snapshot.path.trim().is_empty(),
                "pinned discovered schemas must carry snapshot path evidence"
            );
        }
        SchemaSource::Hints { snapshot, .. } => {
            if schema_has_fields {
                assert!(
                    snapshot.is_some(),
                    "hinted Arrow schemas with fields must carry pinned schema snapshot evidence"
                );
            }
        }
        SchemaSource::Contract {
            contract,
            schema_hash,
        } => {
            assert!(
                !contract.as_str().trim().is_empty(),
                "contract schema source must name its contract"
            );
            if schema_has_fields {
                assert!(
                    schema_hash.is_some(),
                    "contract Arrow schemas with fields must carry schema hash evidence"
                );
            }
        }
    }
}

fn assert_named_fields(
    field_kind: &str,
    required_fields: &[String],
    schema_fields: &BTreeSet<String>,
    schema_has_fields: bool,
) {
    for field in required_fields {
        assert!(
            !field.trim().is_empty(),
            "{field_kind} field names must not be empty"
        );
        if schema_has_fields {
            assert!(
                schema_fields.contains(field),
                "{field_kind} field `{field}` must exist in Arrow schema"
            );
        }
    }
}

fn assert_request_targets_resource(descriptor: &ResourceDescriptor, request: &ScanRequest) {
    assert_eq!(
        request.resource_id, descriptor.resource_id,
        "scan request must target the candidate resource"
    );
}

fn assert_partition_plans(
    descriptor: &ResourceDescriptor,
    capabilities: Option<&ResourceCapabilities>,
    request: &ScanRequest,
    partitions: &[PartitionPlan],
) {
    assert!(
        !partitions.is_empty(),
        "resource partition planning must return at least one partition"
    );
    if let Some(capabilities) = capabilities {
        assert!(
            capabilities.partitioning.parallel_partitions || partitions.len() == 1,
            "multiple partitions require declared parallel partition support"
        );
    }

    let mut partition_ids = BTreeSet::new();
    for partition in partitions {
        assert!(
            partition_ids.insert(partition.partition_id.clone()),
            "partition id `{}` is returned more than once",
            partition.partition_id
        );
        assert_checkpoint_scope(&partition.scope);
        assert_partition_scope_echoes_identity(partition);
        assert_descriptor_scope_compatible(descriptor, &partition.scope);
        if let Some(capabilities) = capabilities {
            assert_partition_scope_supported(capabilities, &partition.scope);
        }
        if let Some(resource_id) = partition.metadata.get("resource_id") {
            assert_eq!(
                resource_id,
                request.resource_id.as_str(),
                "partition metadata resource_id must match requested resource"
            );
        }
    }
}

fn assert_checkpoint_scope(scope: &ScopeKey) {
    match scope {
        ScopeKey::Resource => {}
        ScopeKey::Partition { partition_id } => {
            assert!(
                !partition_id.as_str().trim().is_empty(),
                "partition scope must carry a partition id"
            );
        }
        ScopeKey::Window { start, end } => {
            assert!(
                !start.trim().is_empty() && !end.trim().is_empty(),
                "window scope must carry non-empty start and end values"
            );
        }
        ScopeKey::File { path } => {
            assert!(
                !path.trim().is_empty(),
                "file scope must carry a non-empty path"
            );
        }
        ScopeKey::Stream { name } => {
            assert!(
                !name.trim().is_empty(),
                "stream scope must carry a non-empty name"
            );
        }
        ScopeKey::SchemaContract { contract } => {
            assert!(
                !contract.as_str().trim().is_empty(),
                "schema-contract scope must carry a contract id"
            );
        }
        ScopeKey::DestinationLoad {
            destination,
            target,
        } => {
            assert!(
                !destination.as_str().trim().is_empty() && !target.as_str().trim().is_empty(),
                "destination-load scope must carry destination and target ids"
            );
        }
        ScopeKey::Composite { parts } => {
            assert!(
                !parts.is_empty(),
                "composite scope must carry at least one part"
            );
            for part in parts {
                assert_checkpoint_scope(part);
            }
        }
    }
}

fn assert_partition_scope_echoes_identity(partition: &PartitionPlan) {
    if let ScopeKey::Partition { partition_id } = &partition.scope {
        assert_eq!(
            partition_id, &partition.partition_id,
            "partition scope id must match partition plan id"
        );
    }
}

fn assert_descriptor_scope_compatible(descriptor: &ResourceDescriptor, scope: &ScopeKey) {
    let descriptor_kind = descriptor.state_scope.kind();
    if descriptor_kind == ScopeKind::Resource {
        return;
    }
    assert_eq!(
        scope.kind(),
        descriptor_kind,
        "partition scope kind must match descriptor state scope kind"
    );
}

fn assert_partition_scope_supported(capabilities: &ResourceCapabilities, scope: &ScopeKey) {
    let supported_scopes = &capabilities.partitioning.supported_scopes;
    if supported_scopes.is_empty() {
        assert_eq!(
            scope.kind(),
            ScopeKind::Resource,
            "resources without declared partition scopes may only plan resource-scoped partitions"
        );
    } else {
        assert!(
            supported_scopes.contains(&scope.kind()),
            "partition scope kind {:?} is not declared as supported",
            scope.kind()
        );
    }
}

fn assert_capability_preconditions(
    descriptor: &ResourceDescriptor,
    capabilities: &ResourceCapabilities,
) {
    for operator in &capabilities.filters.supported_operators {
        assert!(
            !operator.trim().is_empty(),
            "filter capability operators must not be empty"
        );
    }
    if capabilities.partitioning.supported_scopes.is_empty() {
        assert!(
            !capabilities.partitioning.parallel_partitions,
            "parallel partitioning requires at least one supported partition scope"
        );
    }

    match capabilities.incremental {
        IncrementalShape::Full => {}
        IncrementalShape::Cursor => {
            assert!(
                descriptor.cursor.is_some(),
                "cursor incremental support requires a descriptor cursor"
            );
        }
        IncrementalShape::File => {
            assert!(
                capabilities
                    .partitioning
                    .supported_scopes
                    .contains(&ScopeKind::File)
                    || descriptor.state_scope.kind() == ScopeKind::File,
                "file incremental support requires file-like partition scope support"
            );
        }
        IncrementalShape::Log | IncrementalShape::PageToken | IncrementalShape::Cdc => {}
    }

    if capabilities.replay == ReplaySupport::FromPosition {
        assert!(
            matches!(
                capabilities.incremental,
                IncrementalShape::Cursor
                    | IncrementalShape::Log
                    | IncrementalShape::File
                    | IncrementalShape::PageToken
                    | IncrementalShape::Cdc
            ),
            "replay-from-position support requires a position-bearing incremental shape"
        );
    }
}

fn assert_predicate_expectations_cover_request(case: &ResourceConformanceCase) {
    let request_predicates = case
        .request
        .filters
        .iter()
        .map(|predicate| predicate.predicate_id.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        request_predicates.len(),
        case.request.filters.len(),
        "scan request predicate ids must be unique"
    );
    assert_eq!(
        case.expected_predicates.len(),
        case.request.filters.len(),
        "each requested predicate must have an expected conformance classification"
    );

    let mut expected = BTreeSet::new();
    for expectation in &case.expected_predicates {
        assert!(
            expected.insert(expectation.predicate_id.clone()),
            "predicate expectation `{}` is declared more than once",
            expectation.predicate_id
        );
        assert!(
            request_predicates.contains(&expectation.predicate_id),
            "predicate expectation `{}` is not present in the request",
            expectation.predicate_id
        );
        if let PredicateClassification::Pushed(PushdownFidelity::Unsupported) =
            &expectation.classification
        {
            panic!("Unsupported predicate fidelity must be classified as unsupported");
        }
    }
}

fn assert_mismatched_resource_rejected<R>(resource: &R, request: &ScanRequest)
where
    R: QueryableResource + ?Sized,
{
    let mut mismatched = request.clone();
    mismatched.resource_id = ResourceId::new(format!(
        "{}.__conformance_mismatch__",
        resource.descriptor().resource_id.as_str()
    ))
    .expect("generated mismatch id is non-empty");

    assert!(
        resource.negotiate(&mismatched).is_err(),
        "negotiate must reject scan requests for other resource ids"
    );
}

fn assert_negotiate_plan<R>(resource: &R, case: &ResourceConformanceCase, plan: &ScanPlan)
where
    R: QueryableResource + ?Sized,
{
    assert_eq!(
        plan.request, case.request,
        "negotiated scan plan must preserve request identity"
    );
    let stream_partitions = resource
        .plan_partitions(&case.request)
        .unwrap_or_else(|error| panic!("resource partition planning failed: {error}"));
    assert!(
        stream_partitions.iter().all(|partition| {
            partition.scan_intent == cdf_kernel::CompiledScanIntent::full_scan()
        }),
        "ResourceStream partition planning is Tier-A and must not compile source pushdown"
    );
    let mut negotiated_topology = plan.partitions.clone();
    for partition in &mut negotiated_topology {
        partition.scan_intent = cdf_kernel::CompiledScanIntent::full_scan();
    }
    assert_eq!(
        negotiated_topology, stream_partitions,
        "negotiated partitions must preserve Tier-A partition topology and metadata"
    );
    assert_partition_plans(
        resource.descriptor(),
        Some(resource.capabilities()),
        &case.request,
        &plan.partitions,
    );
    assert_predicate_classification(resource.capabilities(), case, plan);
}

fn assert_predicate_classification(
    capabilities: &ResourceCapabilities,
    case: &ResourceConformanceCase,
    plan: &ScanPlan,
) {
    let requested = case
        .request
        .filters
        .iter()
        .map(|predicate| (predicate.predicate_id.clone(), predicate.clone()))
        .collect::<BTreeMap<_, _>>();
    let mut actual = BTreeMap::new();

    for pushed in &plan.pushed_predicates {
        assert_requested_predicate(&requested, &pushed.predicate);
        assert_ne!(
            pushed.fidelity,
            PushdownFidelity::Unsupported,
            "unsupported predicate fidelity must not appear in pushed predicates"
        );
        assert_ne!(
            capabilities.filters.default_fidelity,
            PushdownFidelity::Unsupported,
            "resources that declare unsupported filter pushdown must not push predicates"
        );
        assert!(
            predicate_matches_supported_operator(capabilities, &pushed.predicate),
            "pushed predicate `{}` does not match any declared supported operator",
            pushed.predicate.predicate_id
        );
        assert!(
            actual
                .insert(
                    pushed.predicate.predicate_id.clone(),
                    PredicateClassification::Pushed(pushed.fidelity.clone()),
                )
                .is_none(),
            "predicate `{}` appears more than once in negotiated classifications",
            pushed.predicate.predicate_id
        );
    }

    for unsupported in &plan.unsupported_predicates {
        assert_requested_predicate(&requested, unsupported);
        assert!(
            actual
                .insert(
                    unsupported.predicate_id.clone(),
                    PredicateClassification::Unsupported,
                )
                .is_none(),
            "predicate `{}` appears more than once in negotiated classifications",
            unsupported.predicate_id
        );
    }

    assert_eq!(
        actual.len(),
        requested.len(),
        "negotiation must classify every requested predicate exactly once"
    );

    let expected = case
        .expected_predicates
        .iter()
        .map(|expectation| {
            (
                expectation.predicate_id.clone(),
                expectation.classification.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        actual, expected,
        "negotiated predicate classifications must match the conformance case"
    );
}

fn assert_requested_predicate(
    requested: &BTreeMap<PredicateId, ScanPredicate>,
    predicate: &ScanPredicate,
) {
    assert_eq!(
        requested.get(&predicate.predicate_id),
        Some(predicate),
        "negotiated predicate `{}` must be copied from the request",
        predicate.predicate_id
    );
}

fn predicate_matches_supported_operator(
    capabilities: &ResourceCapabilities,
    predicate: &ScanPredicate,
) -> bool {
    let cdf_kernel::ExpressionNode::Call { function, .. } = &predicate.canonical_expression.root
    else {
        return false;
    };
    let operator = match function.name.as_str() {
        "eq" => "=",
        "neq" => "!=",
        "gt" => ">",
        "gte" => ">=",
        "lt" => "<",
        "lte" => "<=",
        _ => return false,
    };
    capabilities
        .filters
        .supported_operators
        .iter()
        .any(|supported| supported == operator)
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeMap,
        panic::{AssertUnwindSafe, catch_unwind},
        sync::Arc,
    };

    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use cdf_kernel::{
        BackpressureSupport, CapabilitySupport, CdfError, ContractRef, CursorOrderingClaim,
        CursorSpec, DeliveryGuarantee, EstimateSupport, FilterCapabilities, PartitionId,
        PartitioningCapabilities, PlanId, Result, SchemaHash, SourcePosition, TrustLevel,
        WriteDisposition,
    };

    use super::*;

    #[test]
    fn sound_resource_stream_passes_conformance() {
        let resource = FaultyResource::sound();
        assert_resource_stream_conformance(&resource, [case().request]);
    }

    #[test]
    fn sound_queryable_resource_passes_conformance() {
        let resource = FaultyResource::sound();
        assert_queryable_resource_conformance(&resource, [case()]);
    }

    #[test]
    fn negative_self_tests_prove_harness_catches_contract_violations() {
        for fault in [
            Fault::MissingSchemaKey,
            Fault::MissingSchemaEvidence,
            Fault::DuplicatePartitionId,
            Fault::InvalidCheckpointScope,
            Fault::PartitionScopeMismatch,
            Fault::DescriptorScopeMismatch,
            Fault::UnsupportedScopeClaim,
            Fault::AcceptsMismatchedResource,
            Fault::MismatchedNegotiatedRequest,
            Fault::DishonestPushdownClassification,
            Fault::MutatedNegotiatedPredicate,
            Fault::CursorIncrementalWithoutCursor,
            Fault::FileIncrementalWithoutFileScope,
            Fault::ReplayFromPositionWithoutStateShape,
        ] {
            assert_harness_panics(FaultyResource::with_fault(fault));
        }
    }

    #[test]
    fn stream_negative_self_tests_prove_harness_catches_contract_violations() {
        assert_stream_harness_panics(FaultyResource::with_fault(Fault::WrongRequestResource));
    }

    #[test]
    fn case_negative_self_tests_prove_harness_catches_contract_violations() {
        let mut duplicate_expectation = case();
        let duplicate = duplicate_expectation.expected_predicates[0].clone();
        duplicate_expectation.expected_predicates.push(duplicate);
        assert_harness_panics_with_case(FaultyResource::sound(), duplicate_expectation);

        let mut unsupported_operator_case = case();
        unsupported_operator_case.expected_predicates[2] =
            PredicateExpectation::inexact(PredicateId::new("p-unsupported").unwrap());
        assert_harness_panics_with_case(
            FaultyResource::with_fault(Fault::PushesUnsupportedOperator),
            unsupported_operator_case,
        );
    }

    fn assert_harness_panics(resource: FaultyResource) {
        assert_harness_panics_with_case(resource, case());
    }

    fn assert_harness_panics_with_case(
        resource: FaultyResource,
        conformance_case: ResourceConformanceCase,
    ) {
        let fault = resource
            .fault
            .map(|fault| format!("{fault:?}"))
            .unwrap_or_else(|| "case fault".to_owned());
        let result = catch_unwind(AssertUnwindSafe(|| {
            assert_queryable_resource_conformance(&resource, [conformance_case]);
        }));
        assert!(result.is_err(), "fault {fault:?} passed conformance");
    }

    fn assert_stream_harness_panics(resource: FaultyResource) {
        let fault = resource.fault.expect("faulty resource must carry a fault");
        let mut request = case().request;
        if matches!(fault, Fault::WrongRequestResource) {
            request.resource_id = ResourceId::new("other.orders").unwrap();
        }
        let result = catch_unwind(AssertUnwindSafe(|| {
            assert_resource_stream_conformance(&resource, [request]);
        }));
        assert!(result.is_err(), "fault {fault:?} passed conformance");
    }

    fn case() -> ResourceConformanceCase {
        let exact = PredicateId::new("p-exact").unwrap();
        let inexact = PredicateId::new("p-inexact").unwrap();
        let unsupported = PredicateId::new("p-unsupported").unwrap();
        ResourceConformanceCase::new(ScanRequest {
            resource_id: ResourceId::new("orders").unwrap(),
            projection: Some(vec!["id".to_owned(), "updated_at".to_owned()]),
            filters: vec![
                ScanPredicate::new(exact.clone(), "id = 1").unwrap(),
                ScanPredicate::new(inexact.clone(), "updated_at >= 1").unwrap(),
                ScanPredicate::new(unsupported.clone(), "notes != 'x'").unwrap(),
            ],
            limit: Some(100),
            order_by: Vec::new(),
            scope: ScopeKey::Resource,
        })
        .with_expected_predicates([
            PredicateExpectation::exact(exact),
            PredicateExpectation::inexact(inexact),
            PredicateExpectation::unsupported(unsupported),
        ])
    }

    #[derive(Clone, Debug)]
    struct FaultyResource {
        descriptor: ResourceDescriptor,
        schema: SchemaRef,
        capabilities: ResourceCapabilities,
        fault: Option<Fault>,
    }

    #[derive(Clone, Copy, Debug)]
    enum Fault {
        MissingSchemaKey,
        MissingSchemaEvidence,
        DuplicatePartitionId,
        InvalidCheckpointScope,
        PartitionScopeMismatch,
        DescriptorScopeMismatch,
        UnsupportedScopeClaim,
        WrongRequestResource,
        AcceptsMismatchedResource,
        MismatchedNegotiatedRequest,
        DishonestPushdownClassification,
        MutatedNegotiatedPredicate,
        PushesUnsupportedOperator,
        CursorIncrementalWithoutCursor,
        FileIncrementalWithoutFileScope,
        ReplayFromPositionWithoutStateShape,
    }

    impl FaultyResource {
        fn sound() -> Self {
            Self {
                descriptor: descriptor(Some(CursorSpec {
                    field: "updated_at".to_owned(),
                    ordering: CursorOrderingClaim::Exact,
                    lag_tolerance_ms: 0,
                })),
                schema: schema(&["id", "updated_at", "notes"]),
                capabilities: capabilities(),
                fault: None,
            }
        }

        fn with_fault(fault: Fault) -> Self {
            let mut resource = Self::sound();
            resource.fault = Some(fault);
            match fault {
                Fault::MissingSchemaKey => {
                    resource.descriptor.primary_key = vec!["missing_id".to_owned()];
                }
                Fault::MissingSchemaEvidence => {
                    resource.descriptor.schema_source = SchemaSource::Discover;
                }
                Fault::InvalidCheckpointScope => {
                    resource.descriptor.state_scope = ScopeKey::Resource;
                    resource.capabilities.partitioning.supported_scopes = vec![ScopeKind::Window];
                }
                Fault::DescriptorScopeMismatch => {
                    resource.descriptor.state_scope = ScopeKey::Window {
                        start: "cursor".to_owned(),
                        end: "cursor+1h".to_owned(),
                    };
                }
                Fault::UnsupportedScopeClaim => {
                    resource.capabilities.partitioning.supported_scopes = vec![ScopeKind::File];
                }
                Fault::DishonestPushdownClassification => {
                    resource.capabilities.filters.default_fidelity = PushdownFidelity::Unsupported;
                }
                Fault::CursorIncrementalWithoutCursor => {
                    resource.descriptor.cursor = None;
                }
                Fault::FileIncrementalWithoutFileScope => {
                    resource.capabilities.incremental = IncrementalShape::File;
                    resource.capabilities.partitioning.supported_scopes =
                        vec![ScopeKind::Partition];
                }
                Fault::ReplayFromPositionWithoutStateShape => {
                    resource.capabilities.incremental = IncrementalShape::Full;
                    resource.capabilities.replay = ReplaySupport::FromPosition;
                }
                Fault::DuplicatePartitionId
                | Fault::PartitionScopeMismatch
                | Fault::WrongRequestResource
                | Fault::AcceptsMismatchedResource
                | Fault::MismatchedNegotiatedRequest
                | Fault::MutatedNegotiatedPredicate
                | Fault::PushesUnsupportedOperator => {}
            }
            resource
        }
    }

    impl ResourceStream for FaultyResource {
        fn descriptor(&self) -> &ResourceDescriptor {
            &self.descriptor
        }

        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }

        fn plan_partitions(&self, _request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
            if matches!(self.fault, Some(Fault::DuplicatePartitionId)) {
                return Ok(vec![partition("p0"), partition("p0")]);
            }
            if matches!(self.fault, Some(Fault::WrongRequestResource)) {
                return Ok(vec![partition_without_metadata("p0")]);
            }
            if matches!(self.fault, Some(Fault::InvalidCheckpointScope)) {
                return Ok(vec![PartitionPlan {
                    partition_id: PartitionId::new("window").unwrap(),
                    scope: ScopeKey::Window {
                        start: String::new(),
                        end: String::new(),
                    },
                    planned_position: None,
                    start_position: None::<SourcePosition>,
                    scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
                    retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
                    metadata: BTreeMap::from([("resource_id".to_owned(), "orders".to_owned())]),
                }]);
            }
            if matches!(self.fault, Some(Fault::PartitionScopeMismatch)) {
                return Ok(vec![PartitionPlan {
                    partition_id: PartitionId::new("p0").unwrap(),
                    scope: ScopeKey::Partition {
                        partition_id: PartitionId::new("p1").unwrap(),
                    },
                    planned_position: None,
                    start_position: None::<SourcePosition>,
                    scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
                    retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
                    metadata: BTreeMap::from([("resource_id".to_owned(), "orders".to_owned())]),
                }]);
            }
            Ok(vec![partition("p0")])
        }

        fn open(&self, _partition: PartitionPlan) -> cdf_kernel::PartitionOpenAttempt<'_> {
            cdf_kernel::PartitionOpenAttempt::materialized(Box::pin(async {
                Err(CdfError::internal(
                    "resource conformance self-tests must not call open",
                ))
            }))
        }
    }

    impl QueryableResource for FaultyResource {
        fn capabilities(&self) -> &ResourceCapabilities {
            &self.capabilities
        }

        fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
            if request.resource_id != self.descriptor.resource_id
                && !matches!(self.fault, Some(Fault::AcceptsMismatchedResource))
            {
                return Err(CdfError::contract("resource id mismatch"));
            }

            let plan_request = if matches!(self.fault, Some(Fault::MismatchedNegotiatedRequest)) {
                let mut mismatched = request.clone();
                mismatched.resource_id = ResourceId::new("other.orders").unwrap();
                mismatched
            } else {
                request.clone()
            };

            let mut pushed_predicates = Vec::new();
            let mut unsupported_predicates = Vec::new();
            for predicate in &request.filters {
                match predicate.predicate_id.as_str() {
                    "p-exact" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                        predicate: if matches!(self.fault, Some(Fault::MutatedNegotiatedPredicate))
                        {
                            let mut mutated = predicate.clone();
                            mutated.expression = "id = 2".to_owned();
                            mutated
                        } else {
                            predicate.clone()
                        },
                        fidelity: PushdownFidelity::Exact,
                    }),
                    "p-inexact" | "p-unsupported"
                        if matches!(self.fault, Some(Fault::DishonestPushdownClassification)) =>
                    {
                        pushed_predicates.push(cdf_kernel::PushedPredicate {
                            predicate: predicate.clone(),
                            fidelity: PushdownFidelity::Exact,
                        });
                    }
                    "p-inexact" => pushed_predicates.push(cdf_kernel::PushedPredicate {
                        predicate: predicate.clone(),
                        fidelity: PushdownFidelity::Inexact,
                    }),
                    "p-unsupported"
                        if matches!(self.fault, Some(Fault::PushesUnsupportedOperator)) =>
                    {
                        pushed_predicates.push(cdf_kernel::PushedPredicate {
                            predicate: predicate.clone(),
                            fidelity: PushdownFidelity::Inexact,
                        });
                    }
                    _ => unsupported_predicates.push(predicate.clone()),
                }
            }

            Ok(ScanPlan {
                plan_id: PlanId::new("plan-orders").unwrap(),
                request: plan_request,
                partitions: self.plan_partitions(request)?,
                planned_task_set: None,
                pushed_predicates,
                unsupported_predicates,
                estimated_rows: None,
                estimated_bytes: None,
                delivery_guarantee: DeliveryGuarantee::EffectivelyOncePerKey,
            })
        }
    }

    fn descriptor(cursor: Option<CursorSpec>) -> ResourceDescriptor {
        ResourceDescriptor {
            resource_id: ResourceId::new("orders").unwrap(),
            schema_source: SchemaSource::Declared {
                schema_hash: SchemaHash::new("sha256:resource-conformance").unwrap(),
                source: "fixture:resource-conformance".to_owned(),
            },
            primary_key: vec!["id".to_owned()],
            merge_key: vec!["id".to_owned()],
            cursor,
            write_disposition: WriteDisposition::Merge,
            deduplication: None,
            contract: Some(ContractRef::new("orders-contract").unwrap()),
            state_scope: ScopeKey::Partition {
                partition_id: PartitionId::new("p0").unwrap(),
            },
            freshness: None,
            trust_level: TrustLevel::Governed,
        }
    }

    fn schema(fields: &[&str]) -> SchemaRef {
        Arc::new(Schema::new(
            fields
                .iter()
                .map(|field| Field::new(*field, DataType::Utf8, true))
                .collect::<Vec<_>>(),
        ))
    }

    fn capabilities() -> ResourceCapabilities {
        ResourceCapabilities {
            projection: CapabilitySupport::Supported,
            filters: FilterCapabilities {
                default_fidelity: PushdownFidelity::Inexact,
                supported_operators: vec!["=".to_owned(), ">=".to_owned()],
            },
            limits: CapabilitySupport::Supported,
            ordering: CapabilitySupport::Unsupported,
            partitioning: PartitioningCapabilities {
                parallel_partitions: true,
                supported_scopes: vec![ScopeKind::Partition],
            },
            incremental: IncrementalShape::Cursor,
            replay: ReplaySupport::FromPosition,
            idempotent_reads: true,
            backpressure: BackpressureSupport::Pausable,
            estimates: EstimateSupport::None,
        }
    }

    fn partition(id: &str) -> PartitionPlan {
        PartitionPlan {
            partition_id: PartitionId::new(id).unwrap(),
            scope: ScopeKey::Partition {
                partition_id: PartitionId::new(id).unwrap(),
            },
            planned_position: None,
            start_position: None::<SourcePosition>,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::from([("resource_id".to_owned(), "orders".to_owned())]),
        }
    }

    fn partition_without_metadata(id: &str) -> PartitionPlan {
        PartitionPlan {
            partition_id: PartitionId::new(id).unwrap(),
            scope: ScopeKey::Partition {
                partition_id: PartitionId::new(id).unwrap(),
            },
            planned_position: None,
            start_position: None::<SourcePosition>,
            scan_intent: cdf_kernel::CompiledScanIntent::full_scan(),
            retry_safety: cdf_kernel::PartitionRetrySafety::Forbidden,
            metadata: BTreeMap::new(),
        }
    }
}
