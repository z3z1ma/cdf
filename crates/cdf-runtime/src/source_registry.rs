use std::{collections::BTreeMap, sync::Arc};

use cdf_kernel::{CdfError, QueryableResource, Result};

use crate::{
    CompiledSourcePlan, SourceCompileRequest, SourceDiscoveryCandidate, SourceDiscoveryKind,
    SourceDiscoveryRequest, SourceDiscoverySession, SourceDriver, SourceDriverDescriptor,
    SourceDriverId, SourceResolutionContext, SourceSchemaObservation, artifact_hash,
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
        let plan = driver.compile(request)?;
        self.verify_plan_driver(&plan, driver.descriptor())?;
        plan.validate()?;
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
        driver.resolve(plan, context)
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
            left.canonical_location
                .cmp(&right.canonical_location)
                .then_with(|| left.identity.cmp(&right.identity))
        });
        if let Some(duplicates) = candidates
            .windows(2)
            .find(|pair| pair[0].canonical_location == pair[1].canonical_location)
        {
            return Err(CdfError::contract(format!(
                "source discovery returned duplicate canonical candidate `{}`",
                duplicates[0].canonical_location
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
        if observation.canonical_location != candidate.canonical_location {
            return Err(CdfError::contract(format!(
                "source discovery observation location `{}` does not match candidate `{}`",
                observation.canonical_location, candidate.canonical_location
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
                observation.canonical_location = location.clone();
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
