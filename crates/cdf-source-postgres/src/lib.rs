#![doc = "Postgres source adapter for cdf."]

mod catalog;
mod driver;
mod source;

pub use catalog::{
    POSTGRES_CATALOG_DISCOVERY_PROBE, PostgresCatalogDiscovery,
    discover_postgres_table_catalog_schema,
};
pub use cdf_postgres::{PostgresIdentifier, PostgresTarget};
pub use driver::PostgresSourceDriver;
pub use source::*;
