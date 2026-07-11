use std::{collections::BTreeMap, sync::Arc};

use cdf_kernel::{CdfError, QueryableResource, Result};

use crate::{
    CompiledSourcePlan, SourceCompileRequest, SourceDriver, SourceDriverDescriptor, SourceDriverId,
    SourceResolutionContext,
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
        plan.validate()?;
        let driver = self.drivers.get(&plan.driver.driver_id).ok_or_else(|| {
            CdfError::contract(format!(
                "compiled source plan requires unregistered driver `{}`",
                plan.driver.driver_id.as_str()
            ))
        })?;
        self.verify_plan_driver(plan, driver.descriptor())?;
        driver.resolve(plan, context)
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

    fn driver_for_kind(&self, kind: &str) -> Result<&Arc<dyn SourceDriver>> {
        let driver_id = self.kinds.get(kind).ok_or_else(|| {
            CdfError::contract(format!("no source driver registered for kind `{kind}`"))
        })?;
        Ok(self
            .drivers
            .get(driver_id)
            .expect("source kind index references registered driver"))
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
