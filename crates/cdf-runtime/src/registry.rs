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
        self.driver_for_uri(uri)?.inspect(uri, context)
    }

    pub fn resolve(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        self.driver_for_uri(uri)?.resolve(uri, context)
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
