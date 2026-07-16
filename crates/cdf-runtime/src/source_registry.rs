use std::{collections::BTreeMap, path::Path, sync::Arc};

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
        project_root: &Path,
        driver_options: &BTreeMap<String, serde_json::Value>,
        referenced_uris: &[String],
    ) -> Result<Vec<SourceHealthResult>> {
        let mut references = BTreeMap::<SourceDriverId, Vec<String>>::new();
        for uri in referenced_uris {
            let driver = self.driver_for_uri(uri)?;
            references
                .entry(driver.descriptor().driver_id.clone())
                .or_default()
                .push(uri.clone());
        }
        let mut results = Vec::new();
        for (driver_id, driver) in &self.drivers {
            let Some(probe) = driver.health_probe() else {
                continue;
            };
            let mut uris = references.remove(driver_id).unwrap_or_default();
            uris.sort();
            uris.dedup();
            results.extend(probe.health(SourceHealthRequest {
                project_root: project_root.to_path_buf(),
                project_options: driver_options.get(driver_id.as_str()).cloned(),
                referenced_uris: uris,
            })?);
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
    }
    Ok(())
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
            }),
        };
        assert!(
            drifted
                .observe(&candidate, &request)
                .unwrap_err()
                .message
                .contains("does not match candidate")
        );
    }
}
