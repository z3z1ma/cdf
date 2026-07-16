use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use cdf_kernel::{
    CdfError, EffectiveSchemaCatalogEntry, EffectiveSchemaRuntime, PartitionAttestationAttempt,
    PartitionOpenAttempt, PartitionPlan, QueryableResource, ResourceCapabilities,
    ResourceDescriptor, ResourceStream, Result, ScanPlan, ScanRequest, TypePolicyAllowances,
};

use crate::{
    CompiledSourcePlan, PlannedSourceAdd, SourceAddRequest, SourceCompileRequest,
    SourceDiscoveryCandidate, SourceDiscoveryKind, SourceDiscoveryRequest, SourceDiscoverySession,
    SourceDriver, SourceDriverDescriptor, SourceDriverId, SourceHealthRequest, SourceHealthResult,
    SourceReferenceCompileRequest, SourceResolutionContext, SourceSchemaObservation, artifact_hash,
};

#[derive(Default)]
pub struct SourceRegistry {
    drivers: BTreeMap<SourceDriverId, Arc<dyn SourceDriver>>,
    kinds: BTreeMap<String, SourceDriverId>,
    schemes: BTreeMap<String, SourceDriverId>,
}

impl SourceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<D>(&mut self, driver: D) -> Result<()>
    where
        D: SourceDriver + 'static,
    {
        self.register_shared(Arc::new(driver))
    }

    pub fn register_shared(&mut self, driver: Arc<dyn SourceDriver>) -> Result<()> {
        let descriptor = driver.descriptor();
        descriptor.validate()?;
        validate_option_schema(driver.option_schema())?;
        if artifact_hash(driver.option_schema())? != descriptor.option_schema_hash {
            return Err(CdfError::contract(format!(
                "source driver `{}` option schema does not match its declared hash",
                descriptor.driver_id.as_str()
            )));
        }
        if self.drivers.contains_key(&descriptor.driver_id) {
            return Err(CdfError::contract(format!(
                "source driver `{}` is already registered",
                descriptor.driver_id.as_str()
            )));
        }
        for kind in &descriptor.kinds {
            if self.kinds.contains_key(kind) {
                return Err(CdfError::contract(format!(
                    "source kind `{kind}` is already registered"
                )));
            }
        }
        for scheme in &descriptor.schemes {
            if self.schemes.contains_key(scheme) {
                return Err(CdfError::contract(format!(
                    "source scheme `{scheme}` is already registered"
                )));
            }
        }
        for kind in &descriptor.kinds {
            self.kinds
                .insert(kind.clone(), descriptor.driver_id.clone());
        }
        for scheme in &descriptor.schemes {
            self.schemes
                .insert(scheme.clone(), descriptor.driver_id.clone());
        }
        self.drivers.insert(descriptor.driver_id.clone(), driver);
        Ok(())
    }

    pub fn compile(&self, request: SourceCompileRequest) -> Result<CompiledSourcePlan> {
        request.context.validate()?;
        let driver = self.driver_for_kind(&request.source_kind)?;
        validate_driver_options(
            driver.as_ref(),
            &request.source_options,
            &request.resource_options,
        )?;
        let expected_descriptor = request.descriptor.clone();
        let expected_schema = request.schema.clone();
        let expected_type_policy_allowances = request.type_policy_allowances;
        let expected_effective_schema_runtime = request.effective_schema_runtime.clone();
        let expected_baseline_observation_schema_catalog =
            request.baseline_observation_schema_catalog.clone();
        let plan = driver.compile(request)?;
        self.verify_plan_driver(&plan, driver.descriptor())?;
        plan.validate()?;
        if plan.descriptor != expected_descriptor
            || plan.schema != expected_schema
            || plan.type_policy_allowances != expected_type_policy_allowances
            || plan.effective_schema_runtime != expected_effective_schema_runtime
            || plan.baseline_observation_schema_catalog
                != expected_baseline_observation_schema_catalog
        {
            return Err(CdfError::contract(format!(
                "source driver `{}` changed compiler-owned schema or resource authority",
                driver.descriptor().driver_id.as_str()
            )));
        }
        Ok(plan)
    }

    pub fn compile_reference(
        &self,
        request: SourceReferenceCompileRequest,
    ) -> Result<CompiledSourcePlan> {
        request.validate()?;
        let expected_resource_id = request.resource_id.clone();
        let expected_trust = request.trust_level.clone();
        let expected_freshness = request.freshness.clone();
        let driver = self.driver_for_uri(&request.uri)?;
        let compiler = driver.reference_compiler().ok_or_else(|| {
            CdfError::contract(format!(
                "source driver `{}` does not support direct project references",
                driver.descriptor().driver_id.as_str()
            ))
        })?;
        let plan = compiler.compile_reference(request)?;
        self.verify_plan_driver(&plan, driver.descriptor())?;
        plan.validate()?;
        if plan.descriptor.resource_id != expected_resource_id
            || plan.descriptor.trust_level != expected_trust
            || plan.descriptor.freshness != expected_freshness
        {
            return Err(CdfError::contract(format!(
                "source driver `{}` changed framework-owned project reference authority",
                driver.descriptor().driver_id.as_str()
            )));
        }
        Ok(plan)
    }

    pub fn resolve(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Arc<dyn QueryableResource>> {
        let driver = self.driver_for_plan(plan)?;
        if let Some(lane) = &plan.execution_capabilities.blocking_lane {
            context
                .execution()
                .ensure_blocking_lanes(std::slice::from_ref(lane))?;
        }
        let inner = driver.resolve(plan, context)?;
        verify_resolved_resource(plan, inner.as_ref())?;
        Ok(Arc::new(RegistryBoundResource {
            inner,
            baseline_observation_schema_catalog: plan.baseline_observation_schema_catalog.clone(),
        }))
    }

    pub fn discovery_session(
        &self,
        plan: &CompiledSourcePlan,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Box<dyn SourceDiscoverySession>> {
        let driver = self.driver_for_plan(plan)?;
        if let Some(lane) = &plan.execution_capabilities.blocking_lane {
            context
                .execution()
                .ensure_blocking_lanes(std::slice::from_ref(lane))?;
        }
        Ok(Box::new(VerifiedSourceDiscoverySession {
            inner: driver.discovery_session(plan, context)?,
        }))
    }

    pub fn driver_for_uri(&self, uri: &str) -> Result<&Arc<dyn SourceDriver>> {
        let scheme = uri
            .split_once("://")
            .map(|(scheme, _)| scheme)
            .ok_or_else(|| CdfError::contract("source URI must contain an explicit scheme"))?;
        let driver_id = self.schemes.get(scheme).ok_or_else(|| {
            CdfError::contract(format!("no source driver registered for scheme `{scheme}`"))
        })?;
        Ok(self
            .drivers
            .get(driver_id)
            .expect("source scheme index references registered driver"))
    }

    pub fn descriptors(&self) -> Vec<SourceDriverDescriptor> {
        self.drivers
            .values()
            .map(|driver| driver.descriptor().clone())
            .collect()
    }

    pub fn option_schemas(&self) -> BTreeMap<String, serde_json::Value> {
        self.drivers
            .iter()
            .map(|(driver_id, driver)| {
                (
                    driver_id.as_str().to_owned(),
                    driver.option_schema().clone(),
                )
            })
            .collect()
    }

    pub fn validate_project_options(
        &self,
        options: &BTreeMap<String, serde_json::Value>,
    ) -> Result<()> {
        for (driver_id, value) in options {
            let driver_id = SourceDriverId::new(driver_id.clone())?;
            let driver = self.drivers.get(&driver_id).ok_or_else(|| {
                CdfError::contract(format!(
                    "project config declares options for unregistered source driver `{}`",
                    driver_id.as_str()
                ))
            })?;
            driver.validate_project_options(value)?;
        }
        Ok(())
    }

    pub fn validate_reference_project_options(
        &self,
        uri: &str,
        options: &BTreeMap<String, serde_json::Value>,
    ) -> Result<()> {
        let driver = self.driver_for_uri(uri)?;
        let value = options
            .get(driver.descriptor().driver_id.as_str())
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        driver.validate_project_options(&value)
    }

    pub fn plan_add(
        &self,
        request: SourceAddRequest,
        driver_options: &BTreeMap<String, serde_json::Value>,
    ) -> Result<PlannedSourceAdd> {
        request.validate()?;
        let mut candidates = Vec::new();
        for (driver_id, driver) in &self.drivers {
            let Some(planner) = driver.add_planner() else {
                continue;
            };
            let mut driver_request = request.clone();
            driver_request.project_options = driver_options.get(driver_id.as_str()).cloned();
            if let Some(proposal) = planner.propose_add(&driver_request)? {
                proposal.validate()?;
                validate_driver_options(
                    driver.as_ref(),
                    &proposal.source_options,
                    &proposal.resource_options,
                )?;
                if !driver.descriptor().kinds.contains(&proposal.source_kind) {
                    return Err(CdfError::contract(format!(
                        "source driver `{}` proposed unowned source kind `{}`",
                        driver_id.as_str(),
                        proposal.source_kind
                    )));
                }
                candidates.push(PlannedSourceAdd {
                    driver: driver.descriptor().clone(),
                    proposal,
                });
            }
        }
        match candidates.len() {
            0 => Err(CdfError::contract(
                "no registered source driver can add the supplied location and options",
            )),
            1 => Ok(candidates.remove(0)),
            _ => Err(CdfError::contract(format!(
                "source add request is ambiguous across registered drivers: {}",
                candidates
                    .iter()
                    .map(|candidate| candidate.driver.driver_id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ))),
        }
    }

    pub fn health_checks(
        &self,
        context: &SourceResolutionContext<'_>,
        compiled_plans: &[CompiledSourcePlan],
    ) -> Result<Vec<SourceHealthResult>> {
        let mut plans = BTreeMap::<SourceDriverId, Vec<CompiledSourcePlan>>::new();
        for plan in compiled_plans {
            let driver = self.driver_for_plan(plan)?;
            plans
                .entry(driver.descriptor().driver_id.clone())
                .or_default()
                .push(plan.clone());
        }
        let mut results = Vec::new();
        let mut probe_ids = BTreeSet::new();
        for (driver_id, driver) in &self.drivers {
            let project_options = context.driver_options(driver_id).cloned();
            if let Some(options) = &project_options {
                driver.validate_project_options(options)?;
            }
            let mut driver_plans = plans.remove(driver_id).unwrap_or_default();
            driver_plans.sort_by(|left, right| {
                left.descriptor
                    .resource_id
                    .as_str()
                    .cmp(right.descriptor.resource_id.as_str())
            });
            let driver_results = driver.health(
                SourceHealthRequest {
                    compiled_plans: driver_plans,
                },
                context,
            )?;
            for result in driver_results {
                let result = verify_health_result(driver_id, result)?;
                if !probe_ids.insert(result.probe_id.clone()) {
                    return Err(CdfError::contract(format!(
                        "source health probe id `{}` was emitted more than once",
                        result.probe_id
                    )));
                }
                results.push(result);
            }
        }
        Ok(results)
    }

    fn driver_for_kind(&self, kind: &str) -> Result<&Arc<dyn SourceDriver>> {
        let driver_id = self.kinds.get(kind).ok_or_else(|| {
            CdfError::contract(format!("no source driver registered for kind `{kind}`"))
        })?;
        Ok(self
            .drivers
            .get(driver_id)
            .expect("source kind index references registered driver"))
    }

    fn driver_for_plan(&self, plan: &CompiledSourcePlan) -> Result<&Arc<dyn SourceDriver>> {
        plan.validate()?;
        let driver = self.drivers.get(&plan.driver.driver_id).ok_or_else(|| {
            CdfError::contract(format!(
                "compiled source plan requires unregistered driver `{}`",
                plan.driver.driver_id.as_str()
            ))
        })?;
        self.verify_plan_driver(plan, driver.descriptor())?;
        Ok(driver)
    }

    fn verify_plan_driver(
        &self,
        plan: &CompiledSourcePlan,
        registered: &SourceDriverDescriptor,
    ) -> Result<()> {
        if &plan.driver != registered {
            return Err(CdfError::contract(format!(
                "compiled source plan driver authority for `{}` does not match the registered version/schema",
                plan.driver.driver_id.as_str()
            )));
        }
        Ok(())
    }
}

struct RegistryBoundResource {
    inner: Arc<dyn QueryableResource>,
    baseline_observation_schema_catalog: Vec<EffectiveSchemaCatalogEntry>,
}

impl ResourceStream for RegistryBoundResource {
    fn descriptor(&self) -> &ResourceDescriptor {
        self.inner.descriptor()
    }

    fn schema(&self) -> arrow_schema::SchemaRef {
        self.inner.schema()
    }

    fn compiled_source_plan_hash(&self) -> Option<&str> {
        self.inner.compiled_source_plan_hash()
    }

    fn validate_runtime_dependencies(&self) -> Result<()> {
        self.inner.validate_runtime_dependencies()
    }

    fn plan_partitions(&self, request: &ScanRequest) -> Result<Vec<PartitionPlan>> {
        self.inner.plan_partitions(request)
    }

    fn open(&self, partition: PartitionPlan) -> PartitionOpenAttempt<'_> {
        self.inner.open(partition)
    }

    fn attest_partition(&self, partition: PartitionPlan) -> PartitionAttestationAttempt<'_> {
        self.inner.attest_partition(partition)
    }

    fn effective_schema_runtime(&self) -> Option<&EffectiveSchemaRuntime> {
        self.inner.effective_schema_runtime()
    }

    fn baseline_observation_schema_catalog(&self) -> &[EffectiveSchemaCatalogEntry] {
        &self.baseline_observation_schema_catalog
    }

    fn type_policy_allowances(&self) -> TypePolicyAllowances {
        self.inner.type_policy_allowances()
    }
}

fn verify_resolved_resource(
    plan: &CompiledSourcePlan,
    resource: &dyn QueryableResource,
) -> Result<()> {
    let expected_plan_hash = artifact_hash(plan)?;
    let mut mismatches = Vec::new();
    if resource.descriptor() != &plan.descriptor {
        mismatches.push("descriptor");
    }
    if resource.schema().as_ref() != &plan.schema {
        mismatches.push("Arrow schema");
    }
    if resource.capabilities() != &plan.resource_capabilities {
        mismatches.push("resource capabilities");
    }
    if resource.type_policy_allowances() != plan.type_policy_allowances {
        mismatches.push("type-policy allowances");
    }
    if resource.effective_schema_runtime() != plan.effective_schema_runtime.as_ref() {
        mismatches.push("effective-schema runtime");
    }
    if resource.compiled_source_plan_hash() != Some(expected_plan_hash.as_str()) {
        mismatches.push("compiled plan hash");
    }
    if !mismatches.is_empty() {
        return Err(CdfError::contract(format!(
            "source driver `{}` resolved executable authority that differs from its compiled plan: {}",
            plan.driver.driver_id.as_str(),
            mismatches.join(", ")
        )));
    }
    Ok(())
}

impl QueryableResource for RegistryBoundResource {
    fn capabilities(&self) -> &ResourceCapabilities {
        self.inner.capabilities()
    }

    fn negotiate(&self, request: &ScanRequest) -> Result<ScanPlan> {
        self.inner.negotiate(request)
    }
}

struct VerifiedSourceDiscoverySession {
    inner: Box<dyn SourceDiscoverySession>,
}

impl SourceDiscoverySession for VerifiedSourceDiscoverySession {
    fn kind(&self) -> SourceDiscoveryKind {
        self.inner.kind()
    }

    fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
        let mut candidates = self.inner.candidates()?;
        for candidate in &candidates {
            candidate.validate()?;
        }
        candidates.sort_by(|left, right| {
            left.evidence_location
                .cmp(&right.evidence_location)
                .then_with(|| left.identity.cmp(&right.identity))
        });
        if let Some(duplicates) = candidates
            .windows(2)
            .find(|pair| pair[0].evidence_location == pair[1].evidence_location)
        {
            return Err(CdfError::contract(format!(
                "source discovery returned duplicate canonical candidate `{}`",
                duplicates[0].evidence_location.as_str()
            )));
        }
        Ok(candidates)
    }

    fn observe(
        &self,
        candidate: &SourceDiscoveryCandidate,
        request: &SourceDiscoveryRequest,
    ) -> Result<SourceSchemaObservation> {
        candidate.validate()?;
        request.validate()?;
        let observation = self.inner.observe(candidate, request)?;
        observation.validate()?;
        if observation.evidence_location != candidate.evidence_location {
            return Err(CdfError::contract(format!(
                "source discovery observation location `{}` does not match candidate `{}`",
                observation.evidence_location.as_str(),
                candidate.evidence_location.as_str()
            )));
        }
        if observation.candidate_binding() != candidate.discovery_binding()? {
            return Err(CdfError::contract(format!(
                "source discovery observation generation for `{}` does not match the inventoried candidate",
                candidate.evidence_location.as_str()
            )));
        }
        if observation.bytes_read > request.maximum_bytes
            || observation.records_read > request.maximum_records
        {
            return Err(CdfError::data(format!(
                "source discovery observation for `{}` exceeded its compiler budget: read {} of {} bytes and {} of {} records",
                candidate.canonical_location,
                observation.bytes_read,
                request.maximum_bytes,
                observation.records_read,
                request.maximum_records
            )));
        }
        Ok(observation)
    }
}

fn validate_option_schema(schema: &serde_json::Value) -> Result<()> {
    let object = schema
        .as_object()
        .ok_or_else(|| CdfError::contract("source driver option schema must be a JSON object"))?;
    if let Some(keyword) = object
        .keys()
        .find(|keyword| !matches!(keyword.as_str(), "$schema" | "source" | "resource"))
    {
        return Err(CdfError::contract(format!(
            "source driver option schema root uses unsupported member `{keyword}`"
        )));
    }
    if object.get("$schema").and_then(serde_json::Value::as_str)
        != Some("https://json-schema.org/draft/2020-12/schema")
    {
        return Err(CdfError::contract(
            "source driver option schema must declare JSON Schema draft 2020-12",
        ));
    }
    for section in ["source", "resource"] {
        let section_schema = object
            .get(section)
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "source driver option schema must declare an object `{section}` section"
                ))
            })?;
        if section_schema
            .get("type")
            .and_then(serde_json::Value::as_str)
            != Some("object")
            || section_schema
                .get("additionalProperties")
                .and_then(serde_json::Value::as_bool)
                != Some(false)
            || !section_schema
                .get("properties")
                .is_some_and(serde_json::Value::is_object)
        {
            return Err(CdfError::contract(format!(
                "source driver option schema `{section}` must be a closed object with properties"
            )));
        }
        validate_option_schema_node(
            object
                .get(section)
                .expect("validated source option schema section"),
            &format!("$.{section}"),
        )?;
    }
    Ok(())
}

fn validate_driver_options(
    driver: &dyn SourceDriver,
    source: &BTreeMap<String, serde_json::Value>,
    resource: &BTreeMap<String, serde_json::Value>,
) -> Result<()> {
    let schema = driver.option_schema();
    validate_option_instance(
        schema
            .get("source")
            .expect("registered source driver has a source option schema"),
        &serde_json::to_value(source)
            .map_err(|error| CdfError::internal(format!("serialize source options: {error}")))?,
        "$.source",
    )?;
    validate_option_instance(
        schema
            .get("resource")
            .expect("registered source driver has a resource option schema"),
        &serde_json::to_value(resource)
            .map_err(|error| CdfError::internal(format!("serialize resource options: {error}")))?,
        "$.resource",
    )
}

fn validate_option_schema_node(schema: &serde_json::Value, path: &str) -> Result<()> {
    const SUPPORTED: &[&str] = &[
        "type",
        "additionalProperties",
        "required",
        "properties",
        "oneOf",
        "const",
        "default",
        "pattern",
        "minLength",
        "format",
        "items",
        "uniqueItems",
        "minimum",
        "enum",
    ];
    const TYPES: &[&str] = &[
        "object", "array", "string", "number", "integer", "boolean", "null",
    ];

    let object = schema.as_object().ok_or_else(|| {
        CdfError::contract(format!("source option schema `{path}` must be an object"))
    })?;
    if let Some(keyword) = object
        .keys()
        .find(|keyword| !SUPPORTED.contains(&keyword.as_str()))
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}` uses unsupported keyword `{keyword}`"
        )));
    }
    if let Some(types) = object.get("type") {
        let declared = match types {
            serde_json::Value::String(value) => vec![value.as_str()],
            serde_json::Value::Array(values) if !values.is_empty() => values
                .iter()
                .map(|value| {
                    value.as_str().ok_or_else(|| {
                        CdfError::contract(format!(
                            "source option schema `{path}.type` entries must be strings"
                        ))
                    })
                })
                .collect::<Result<Vec<_>>>()?,
            _ => {
                return Err(CdfError::contract(format!(
                    "source option schema `{path}.type` must be a type name or nonempty array"
                )));
            }
        };
        if declared.iter().any(|value| !TYPES.contains(value)) {
            return Err(CdfError::contract(format!(
                "source option schema `{path}.type` contains an unsupported JSON type"
            )));
        }
    }
    if let Some(properties) = object.get("properties") {
        let properties = properties.as_object().ok_or_else(|| {
            CdfError::contract(format!(
                "source option schema `{path}.properties` must be an object"
            ))
        })?;
        for (name, child) in properties {
            validate_option_schema_node(child, &format!("{path}.properties.{name}"))?;
        }
    }
    if let Some(additional) = object.get("additionalProperties")
        && !additional.is_boolean()
    {
        validate_option_schema_node(additional, &format!("{path}.additionalProperties"))?;
    }
    if let Some(required) = object.get("required") {
        let required = required.as_array().ok_or_else(|| {
            CdfError::contract(format!(
                "source option schema `{path}.required` must be an array"
            ))
        })?;
        let mut names = std::collections::BTreeSet::new();
        for value in required {
            let name = value
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    CdfError::contract(format!(
                        "source option schema `{path}.required` entries must be nonempty strings"
                    ))
                })?;
            if !names.insert(name) {
                return Err(CdfError::contract(format!(
                    "source option schema `{path}.required` contains duplicate `{name}`"
                )));
            }
            if object
                .get("properties")
                .and_then(serde_json::Value::as_object)
                .is_none_or(|properties| !properties.contains_key(name))
            {
                return Err(CdfError::contract(format!(
                    "source option schema `{path}.required` names undeclared property `{name}`"
                )));
            }
        }
    }
    if let Some(branches) = object.get("oneOf") {
        let branches = branches
            .as_array()
            .filter(|branches| !branches.is_empty())
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "source option schema `{path}.oneOf` must be a nonempty array"
                ))
            })?;
        for (index, branch) in branches.iter().enumerate() {
            validate_option_schema_node(branch, &format!("{path}.oneOf[{index}]"))?;
        }
    }
    if let Some(values) = object.get("enum") {
        let values = values
            .as_array()
            .filter(|values| !values.is_empty())
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "source option schema `{path}.enum` must be a nonempty array"
                ))
            })?;
        let unique = values
            .iter()
            .map(serde_json::to_string)
            .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()
            .map_err(|error| CdfError::internal(format!("serialize option enum value: {error}")))?;
        if unique.len() != values.len() {
            return Err(CdfError::contract(format!(
                "source option schema `{path}.enum` contains duplicate values"
            )));
        }
    }
    if let Some(pattern) = object.get("pattern") {
        let pattern = pattern.as_str().ok_or_else(|| {
            CdfError::contract(format!(
                "source option schema `{path}.pattern` must be a string"
            ))
        })?;
        if !pattern.starts_with('^')
            || pattern.len() == 1
            || pattern[1..].chars().any(|character| {
                matches!(
                    character,
                    '^' | '$' | '*' | '+' | '?' | '[' | ']' | '(' | ')' | '{' | '}' | '\\' | '|'
                )
            })
        {
            return Err(CdfError::contract(format!(
                "source option schema `{path}.pattern` must be a literal prefix pattern"
            )));
        }
    }
    if object
        .get("format")
        .is_some_and(|format| format.as_str() != Some("uri"))
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}.format` supports only `uri`"
        )));
    }
    if object.get("minLength").is_some_and(|value| !value.is_u64()) {
        return Err(CdfError::contract(format!(
            "source option schema `{path}.minLength` must be a nonnegative integer"
        )));
    }
    if object
        .get("minimum")
        .is_some_and(|value| !value.is_u64() && !value.is_i64() && !value.is_f64())
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}.minimum` must be numeric"
        )));
    }
    if let Some(items) = object.get("items") {
        validate_option_schema_node(items, &format!("{path}.items"))?;
    }
    if object
        .get("uniqueItems")
        .is_some_and(|value| !value.is_boolean())
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}.uniqueItems` must be boolean"
        )));
    }
    if ["properties", "required", "additionalProperties"]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
        && !schema_declares_type(object, "object")
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}` uses object keywords without declaring object type"
        )));
    }
    if ["items", "uniqueItems"]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
        && !schema_declares_type(object, "array")
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}` uses array keywords without declaring array type"
        )));
    }
    if ["pattern", "minLength", "format"]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
        && !schema_declares_type(object, "string")
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}` uses string keywords without declaring string type"
        )));
    }
    if object.contains_key("minimum")
        && !schema_declares_type(object, "integer")
        && !schema_declares_type(object, "number")
    {
        return Err(CdfError::contract(format!(
            "source option schema `{path}` uses `minimum` without declaring numeric type"
        )));
    }
    Ok(())
}

fn schema_declares_type(
    schema: &serde_json::Map<String, serde_json::Value>,
    expected: &str,
) -> bool {
    match schema.get("type") {
        Some(serde_json::Value::String(value)) => value == expected,
        Some(serde_json::Value::Array(values)) => values.iter().any(|value| value == expected),
        _ => false,
    }
}

fn validate_option_instance(
    schema: &serde_json::Value,
    instance: &serde_json::Value,
    path: &str,
) -> Result<()> {
    let schema = schema
        .as_object()
        .expect("registered option schema nodes are objects");
    if let Some(branches) = schema.get("oneOf").and_then(serde_json::Value::as_array) {
        let matching = branches
            .iter()
            .filter(|branch| validate_option_instance(branch, instance, path).is_ok())
            .count();
        if matching != 1 {
            return Err(CdfError::contract(format!(
                "source option `{path}` must match exactly one declared alternative (matched {matching})"
            )));
        }
    }
    if let Some(types) = schema.get("type") {
        let matches = match types {
            serde_json::Value::String(value) => instance_matches_type(instance, value),
            serde_json::Value::Array(values) => values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .any(|value| instance_matches_type(instance, value)),
            _ => false,
        };
        if !matches {
            return Err(CdfError::contract(format!(
                "source option `{path}` does not match its declared JSON type"
            )));
        }
    }
    if let Some(expected) = schema.get("const")
        && instance != expected
    {
        return Err(CdfError::contract(format!(
            "source option `{path}` does not match its required constant"
        )));
    }
    if let Some(values) = schema.get("enum").and_then(serde_json::Value::as_array)
        && !values.contains(instance)
    {
        return Err(CdfError::contract(format!(
            "source option `{path}` is not one of its allowed values"
        )));
    }
    if let Some(object) = instance.as_object() {
        let properties = schema
            .get("properties")
            .and_then(serde_json::Value::as_object);
        if let Some(required) = schema.get("required").and_then(serde_json::Value::as_array) {
            for required in required.iter().filter_map(serde_json::Value::as_str) {
                if !object.contains_key(required) {
                    return Err(CdfError::contract(format!(
                        "source option `{path}` requires field `{required}`"
                    )));
                }
            }
        }
        for (name, value) in object {
            if let Some(child) = properties.and_then(|properties| properties.get(name)) {
                validate_option_instance(child, value, &format!("{path}.{name}"))?;
                continue;
            }
            match schema.get("additionalProperties") {
                Some(serde_json::Value::Bool(false)) => {
                    return Err(CdfError::contract(format!(
                        "source option `{path}` does not allow field `{name}`"
                    )));
                }
                Some(additional) if additional.is_object() => {
                    validate_option_instance(additional, value, &format!("{path}.{name}"))?;
                }
                _ => {}
            }
        }
    }
    if let Some(array) = instance.as_array() {
        if let Some(items) = schema.get("items") {
            for (index, value) in array.iter().enumerate() {
                validate_option_instance(items, value, &format!("{path}[{index}]"))?;
            }
        }
        if schema
            .get("uniqueItems")
            .and_then(serde_json::Value::as_bool)
            == Some(true)
        {
            let unique = array
                .iter()
                .map(serde_json::to_string)
                .collect::<std::result::Result<std::collections::BTreeSet<_>, _>>()
                .map_err(|error| CdfError::internal(format!("serialize source option: {error}")))?;
            if unique.len() != array.len() {
                return Err(CdfError::contract(format!(
                    "source option `{path}` requires unique array values"
                )));
            }
        }
    }
    if let Some(value) = instance.as_str() {
        if schema
            .get("minLength")
            .and_then(serde_json::Value::as_u64)
            .is_some_and(|minimum| value.chars().count() < minimum as usize)
        {
            return Err(CdfError::contract(format!(
                "source option `{path}` is shorter than its declared minimum"
            )));
        }
        if let Some(prefix) = schema.get("pattern").and_then(serde_json::Value::as_str) {
            let prefix = &prefix[1..];
            if !value.starts_with(prefix) {
                return Err(CdfError::contract(format!(
                    "source option `{path}` does not match required prefix `{prefix}`"
                )));
            }
        }
        if schema.get("format").and_then(serde_json::Value::as_str) == Some("uri")
            && (value.chars().any(char::is_control)
                || value
                    .split_once("://")
                    .is_none_or(|(scheme, rest)| scheme.is_empty() || rest.is_empty()))
        {
            return Err(CdfError::contract(format!(
                "source option `{path}` must be an absolute URI"
            )));
        }
    }
    if let Some(minimum) = schema.get("minimum").and_then(serde_json::Value::as_f64)
        && instance.as_f64().is_some_and(|value| value < minimum)
    {
        return Err(CdfError::contract(format!(
            "source option `{path}` is below its declared minimum"
        )));
    }
    Ok(())
}

fn instance_matches_type(instance: &serde_json::Value, expected: &str) -> bool {
    match expected {
        "object" => instance.is_object(),
        "array" => instance.is_array(),
        "string" => instance.is_string(),
        "number" => instance.is_number(),
        "integer" => instance.as_i64().is_some() || instance.as_u64().is_some(),
        "boolean" => instance.is_boolean(),
        "null" => instance.is_null(),
        _ => false,
    }
}

fn verify_health_result(
    driver_id: &SourceDriverId,
    mut result: SourceHealthResult,
) -> Result<SourceHealthResult> {
    result.probe_id = format!("source.{}.{}", driver_id.as_str(), result.probe_id);
    sanitize_health_details(&mut result.details, 0)?;
    result.validate()?;
    Ok(result)
}

fn sanitize_health_details(value: &mut serde_json::Value, depth: usize) -> Result<()> {
    const MAX_DEPTH: usize = 16;
    if depth > MAX_DEPTH {
        return Err(CdfError::contract(format!(
            "source health details exceed the {MAX_DEPTH}-level nesting boundary"
        )));
    }
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                if key.is_empty() || key.len() > 128 || key.chars().any(char::is_control) {
                    return Err(CdfError::contract(
                        "source health detail keys must be nonempty, bounded, and control-free",
                    ));
                }
                if is_sensitive_health_key(key) {
                    *value = serde_json::Value::String("<redacted>".to_owned());
                } else {
                    sanitize_health_details(value, depth + 1)?;
                }
            }
        }
        serde_json::Value::Array(values) => {
            for value in values {
                sanitize_health_details(value, depth + 1)?;
            }
        }
        serde_json::Value::String(text) => {
            if text.chars().any(char::is_control) {
                return Err(CdfError::contract(
                    "source health detail strings must be control-free",
                ));
            }
            if text.contains("://") {
                if text.split_whitespace().count() != 1 || url::Url::parse(text).is_err() {
                    return Err(CdfError::contract(
                        "source health detail URI must be the complete string; mixed operational text is forbidden",
                    ));
                }
                *text = crate::SourceEvidenceLocation::from_operational(text)?
                    .as_str()
                    .to_owned();
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_sensitive_health_key(key: &str) -> bool {
    let normalized = key.to_ascii_lowercase().replace(['-', '.'], "_");
    [
        "authorization",
        "credential",
        "password",
        "secret",
        "token",
        "api_key",
        "cookie",
        "connection",
        "dsn",
        "private_key",
        "access_key",
        "session_key",
    ]
    .iter()
    .any(|sensitive| normalized.contains(sensitive))
}

#[cfg(test)]
mod tests {
    use arrow_schema::Schema;

    use super::*;
    use crate::SourceEvidenceLocation;

    struct BoundaryProbeSession {
        candidates: Vec<SourceDiscoveryCandidate>,
        bytes_read: u64,
        records_read: u64,
        replace_location: Option<String>,
        replace_binding: Option<String>,
    }

    impl SourceDiscoverySession for BoundaryProbeSession {
        fn kind(&self) -> SourceDiscoveryKind {
            SourceDiscoveryKind::BoundedContent
        }

        fn candidates(&self) -> Result<Vec<SourceDiscoveryCandidate>> {
            Ok(self.candidates.clone())
        }

        fn observe(
            &self,
            candidate: &SourceDiscoveryCandidate,
            _request: &SourceDiscoveryRequest,
        ) -> Result<SourceSchemaObservation> {
            let mut observation = SourceSchemaObservation::new(
                candidate,
                Schema::empty(),
                BTreeMap::new(),
                self.bytes_read,
                self.records_read,
            )?;
            if let Some(location) = &self.replace_location {
                observation.evidence_location = SourceEvidenceLocation::from_operational(location)?;
            }
            if let Some(binding) = &self.replace_binding {
                observation.candidate_binding.clone_from(binding);
            }
            Ok(observation)
        }
    }

    fn candidate(location: &str) -> SourceDiscoveryCandidate {
        SourceDiscoveryCandidate::new(location, Some(1), None, BTreeMap::new()).unwrap()
    }

    #[test]
    fn discovery_boundary_orders_candidates_and_rejects_duplicate_locations() {
        let verified = VerifiedSourceDiscoverySession {
            inner: Box::new(BoundaryProbeSession {
                candidates: vec![candidate("mock://z"), candidate("mock://a")],
                bytes_read: 1,
                records_read: 1,
                replace_location: None,
                replace_binding: None,
            }),
        };
        assert_eq!(
            verified
                .candidates()
                .unwrap()
                .into_iter()
                .map(|candidate| candidate.canonical_location)
                .collect::<Vec<_>>(),
            vec!["mock://a", "mock://z"]
        );

        let duplicate = VerifiedSourceDiscoverySession {
            inner: Box::new(BoundaryProbeSession {
                candidates: vec![candidate("mock://same"), candidate("mock://same")],
                bytes_read: 1,
                records_read: 1,
                replace_location: None,
                replace_binding: None,
            }),
        };
        assert!(
            duplicate
                .candidates()
                .unwrap_err()
                .message
                .contains("duplicate canonical candidate")
        );
    }

    #[test]
    fn discovery_boundary_redacts_secret_bearing_locations_before_evidence() {
        let secret_candidate = candidate(
            "https://alice:secret@example.test/events.parquet?X-Amz-Signature=secret#fragment",
        );
        assert_eq!(
            secret_candidate.evidence_location.as_str(),
            "https://example.test/events.parquet?<redacted>"
        );
        let mut forged = secret_candidate.clone();
        forged.evidence_location =
            SourceEvidenceLocation::from_operational("https://safe.example/events.parquet")
                .unwrap();
        assert!(
            forged
                .validate()
                .unwrap_err()
                .message
                .contains("does not match its canonical redaction")
        );

        let duplicate = VerifiedSourceDiscoverySession {
            inner: Box::new(BoundaryProbeSession {
                candidates: vec![
                    secret_candidate.clone(),
                    candidate(
                        "https://bob:other@example.test/events.parquet?X-Amz-Signature=other",
                    ),
                ],
                bytes_read: 1,
                records_read: 1,
                replace_location: None,
                replace_binding: None,
            }),
        };
        assert!(
            duplicate
                .candidates()
                .unwrap_err()
                .message
                .contains("duplicate canonical candidate")
        );

        let error = SourceSchemaObservation::new(
            &secret_candidate,
            Schema::empty(),
            BTreeMap::from([(
                "unsafe_location".to_owned(),
                "https://alice:secret@example.test/events?token=secret".to_owned(),
            )]),
            1,
            1,
        )
        .unwrap_err();
        assert!(error.message.contains("invalid canonical identity"));
    }

    #[test]
    fn discovery_boundary_rejects_budget_and_candidate_identity_drift() {
        let candidate = candidate("mock://events");
        let over_budget = VerifiedSourceDiscoverySession {
            inner: Box::new(BoundaryProbeSession {
                candidates: vec![candidate.clone()],
                bytes_read: 11,
                records_read: 3,
                replace_location: None,
                replace_binding: None,
            }),
        };
        let request = SourceDiscoveryRequest::new(10, 2).unwrap();
        assert!(
            over_budget
                .observe(&candidate, &request)
                .unwrap_err()
                .message
                .contains("exceeded its compiler budget")
        );

        let drifted = VerifiedSourceDiscoverySession {
            inner: Box::new(BoundaryProbeSession {
                candidates: vec![candidate.clone()],
                bytes_read: 1,
                records_read: 1,
                replace_location: Some("mock://other".to_owned()),
                replace_binding: None,
            }),
        };
        assert!(
            drifted
                .observe(&candidate, &request)
                .unwrap_err()
                .message
                .contains("does not match candidate")
        );

        let wrong_generation = VerifiedSourceDiscoverySession {
            inner: Box::new(BoundaryProbeSession {
                candidates: vec![candidate.clone()],
                bytes_read: 1,
                records_read: 1,
                replace_location: None,
                replace_binding: Some(
                    artifact_hash(&serde_json::json!({"wrong_generation": true})).unwrap(),
                ),
            }),
        };
        assert!(
            wrong_generation
                .observe(&candidate, &request)
                .unwrap_err()
                .message
                .contains("does not match the inventoried candidate")
        );
    }

    #[test]
    fn option_schema_dialect_validates_nested_instances_before_drivers() {
        let schema = serde_json::json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "source": {
                "type": "object",
                "additionalProperties": false,
                "required": ["endpoint", "auth"],
                "properties": {
                    "endpoint": {"type": "string", "format": "uri"},
                    "auth": {
                        "oneOf": [
                            {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["kind", "token"],
                                "properties": {
                                    "kind": {"const": "bearer"},
                                    "token": {"type": "string", "pattern": "^secret://"}
                                }
                            },
                            {
                                "type": "object",
                                "additionalProperties": false,
                                "required": ["kind"],
                                "properties": {"kind": {"const": "anonymous"}}
                            }
                        ]
                    }
                }
            },
            "resource": {
                "type": "object",
                "additionalProperties": false,
                "required": ["path", "limit"],
                "properties": {
                    "path": {"type": "string", "minLength": 1},
                    "limit": {"type": "integer", "minimum": 1},
                    "fields": {"type": "array", "items": {"type": "string"}, "uniqueItems": true}
                }
            }
        });
        validate_option_schema(&schema).unwrap();
        validate_option_instance(
            &schema["source"],
            &serde_json::json!({
                "endpoint": "https://example.test",
                "auth": {"kind": "bearer", "token": "secret://env/API_TOKEN"}
            }),
            "$.source",
        )
        .unwrap();
        let error = validate_option_instance(
            &schema["resource"],
            &serde_json::json!({"path": "", "limit": 1, "fields": ["id"]}),
            "$.resource",
        )
        .unwrap_err();
        assert!(
            error.message.contains("$.resource.path"),
            "{}",
            error.message
        );
        let error = validate_option_instance(
            &schema["resource"],
            &serde_json::json!({"path": "events", "limit": 1, "fields": ["id", "id"]}),
            "$.resource",
        )
        .unwrap_err();
        assert!(error.message.contains("$.resource.fields"));
        let error = validate_option_instance(
            &schema["source"],
            &serde_json::json!({
                "endpoint": "not-a-uri",
                "auth": {"kind": "anonymous"}
            }),
            "$.source",
        )
        .unwrap_err();
        assert!(error.message.contains("$.source.endpoint"));
        let error = validate_option_instance(
            &schema["source"],
            &serde_json::json!({
                "endpoint": "https://example.test",
                "auth": {"kind": "bearer", "token": "plain-text"}
            }),
            "$.source",
        )
        .unwrap_err();
        assert!(error.message.contains("exactly one declared alternative"));

        let mut unsupported = schema;
        unsupported["resource"]["properties"]["path"]["if"] = serde_json::json!({});
        let error = validate_option_schema(&unsupported).unwrap_err();
        assert!(error.message.contains("unsupported keyword `if`"));
    }

    #[test]
    fn health_boundary_namespaces_bounds_and_redacts_driver_output() {
        let driver = SourceDriverId::new("mock").unwrap();
        let verified = verify_health_result(
            &driver,
            SourceHealthResult {
                probe_id: "reachable".to_owned(),
                status: crate::SourceHealthStatus::Passed,
                message: "endpoint responded successfully".to_owned(),
                details: serde_json::json!({
                    "endpoint": "https://alice:secret@example.test/data?token=secret",
                    "credentials": {"value": "do-not-render"},
                    "nested": [{"session_key": "do-not-render"}],
                }),
            },
        )
        .unwrap();
        assert_eq!(verified.probe_id, "source.mock.reachable");
        assert_eq!(
            verified.details["endpoint"],
            "https://example.test/data?<redacted>"
        );
        assert_eq!(verified.details["credentials"], "<redacted>");
        assert_eq!(verified.details["nested"][0]["session_key"], "<redacted>");

        let error = verify_health_result(
            &driver,
            SourceHealthResult {
                probe_id: "unsafe".to_owned(),
                status: crate::SourceHealthStatus::Failed,
                message: "failed at https://alice:secret@example.test".to_owned(),
                details: serde_json::json!({}),
            },
        )
        .unwrap_err();
        assert!(error.message.contains("contain no URI"));

        let error = verify_health_result(
            &driver,
            SourceHealthResult {
                probe_id: "mixed-uri".to_owned(),
                status: crate::SourceHealthStatus::Failed,
                message: "endpoint probe failed".to_owned(),
                details: serde_json::json!({
                    "context": "primary https://alice:secret@example.test and fallback https://bob:secret@example.test"
                }),
            },
        )
        .unwrap_err();
        assert!(error.message.contains("complete string"));
    }
}
