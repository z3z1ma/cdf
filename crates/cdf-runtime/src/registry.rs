use crate::prelude::*;

pub trait DestinationDriver: Send + Sync {
    fn schemes(&self) -> &'static [&'static str];

    fn inspect(
        &self,
        _uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        Err(CdfError::contract(
            "destination driver does not expose no-mutation inspection",
        ))
    }

    fn resolve(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>>;

    fn health(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        Ok(self
            .inspect(uri, context)?
            .health_probes
            .into_iter()
            .map(|probe| DestinationHealthResult {
                probe_id: probe.probe_id,
                status: DestinationHealthStatus::Unsupported,
                message: format!("{} is not implemented by this driver", probe.description),
                details: Default::default(),
            })
            .collect())
    }

    fn replay_target(&self, target: &str) -> Result<TargetName> {
        TargetName::new(target)
    }
}

#[derive(Default)]
pub struct DestinationRegistry {
    drivers: Vec<Box<dyn DestinationDriver>>,
}

impl DestinationRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<D>(&mut self, driver: D) -> Result<()>
    where
        D: DestinationDriver + 'static,
    {
        self.register_boxed(Box::new(driver))
    }

    pub fn register_boxed(&mut self, driver: Box<dyn DestinationDriver>) -> Result<()> {
        let schemes = driver.schemes();
        if schemes.is_empty() {
            return Err(CdfError::contract(
                "destination driver must register at least one URI scheme",
            ));
        }
        for scheme in schemes {
            validate_destination_scheme(scheme)?;
            if self.driver_for_scheme(scheme).is_some() {
                return Err(CdfError::contract(format!(
                    "destination driver scheme `{scheme}` is already registered"
                )));
            }
        }
        self.drivers.push(driver);
        Ok(())
    }

    pub fn inspect(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        let inspection = self.driver_for_uri(uri)?.inspect(uri, context)?;
        inspection.runtime.validate()?;
        Ok(inspection)
    }

    pub fn resolve(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let runtime = self.driver_for_uri(uri)?.resolve(uri, context)?;
        let capabilities = runtime.runtime_capabilities();
        capabilities.validate()?;
        if let Some(execution) = context.execution_services() {
            execution.ensure_blocking_lanes(&capabilities.blocking_lanes)?;
        }
        Ok(runtime)
    }

    pub fn health(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        self.driver_for_uri(uri)?.health(uri, context)
    }

    pub fn replay_target(&self, uri: &str, target: &str) -> Result<TargetName> {
        self.driver_for_uri(uri)?.replay_target(target)
    }

    pub fn registered_schemes(&self) -> Vec<&'static str> {
        let mut schemes = self
            .drivers
            .iter()
            .flat_map(|driver| driver.schemes().iter().copied())
            .collect::<Vec<_>>();
        schemes.sort_unstable_by_key(|scheme| scheme.to_ascii_lowercase());
        schemes
    }

    fn driver_for_uri(&self, uri: &str) -> Result<&dyn DestinationDriver> {
        let scheme = destination_uri_scheme(uri)?;
        self.driver_for_scheme(scheme).ok_or_else(|| {
            CdfError::contract(format!(
                "no destination driver registered for URI scheme `{scheme}`"
            ))
        })
    }

    fn driver_for_scheme(&self, scheme: &str) -> Option<&dyn DestinationDriver> {
        self.drivers.iter().map(Box::as_ref).find(|driver| {
            driver
                .schemes()
                .iter()
                .any(|registered| registered.eq_ignore_ascii_case(scheme))
        })
    }
}
