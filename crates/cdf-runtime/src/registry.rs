use crate::ExecutionServices;
use crate::prelude::*;

/// Binds one runtime to an execution authority, then installs its post-bind lane capabilities.
///
/// Binding may derive native resource demand from the host, so capability validation and lane
/// installation must observe the bound runtime rather than its conservative construction state.
pub fn bind_destination_runtime(
    runtime: &mut dyn DestinationRuntime,
    execution: &ExecutionServices,
) -> Result<()> {
    runtime.bind_execution_services(execution)?;
    let capabilities = runtime.runtime_capabilities();
    capabilities.validate()?;
    execution.ensure_blocking_lanes(&capabilities.blocking_lanes)
}

/// Thread-safe destination descriptor and runtime factory.
///
/// A driver may be called through a shared registry from multiple threads, so it is stateless or
/// internally synchronized. Resolution returns a runtime exclusively owned by one logical run;
/// this bound does not make that runtime, its native handles, or a finalized commit session
/// `Send`/`Sync`. Native handles remain confined by the resolved runtime's ingress protocol or its
/// declared blocking lane. Runtime work uses injected execution services rather than an
/// adapter-owned executor, and host thread safety never makes the native handle movable.
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

    /// Constructs one run-owned runtime without binding execution services.
    ///
    /// The registry validates capabilities, installs blocking lanes, and performs the single
    /// authoritative bind after this returns. Drivers may inspect the context for credentials and
    /// policy, but must not pre-bind its execution services.
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
        inspection.description.validate()?;
        inspection.runtime.validate()?;
        Ok(inspection)
    }

    pub fn resolve(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let mut runtime = self.driver_for_uri(uri)?.resolve(uri, context)?;
        runtime.describe().validate()?;
        if let Some(execution) = context.execution_services() {
            bind_destination_runtime(runtime.as_mut(), execution)?;
        } else {
            runtime.runtime_capabilities().validate()?;
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
