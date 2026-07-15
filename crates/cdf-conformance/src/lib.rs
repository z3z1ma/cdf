#![doc = "Conformance harness boundary for cdf."]

pub mod checkpoint_store;
pub mod destination;
mod destination_catalog;
pub mod golden_package;
pub mod live_run;
#[cfg(test)]
mod mvp_acceptance_demo;
pub mod package_replay;
#[cfg(test)]
pub mod property_fuzz;
pub mod resource;
pub mod run_matrix;
pub mod runtime_chaos;
pub mod scope_lease;
mod source_fixture;

#[doc(hidden)]
pub fn test_execution_services() -> cdf_runtime::ExecutionServices {
    static SERVICES: std::sync::OnceLock<cdf_runtime::ExecutionServices> =
        std::sync::OnceLock::new();
    SERVICES
        .get_or_init(|| {
            cdf_engine::StandaloneExecutionHost::default_services(128 * 1024 * 1024)
                .expect("conformance execution host")
                .1
                .with_staging_lease_authority(std::sync::Arc::new(
                    cdf_runtime::ScopeStagingLeaseAuthority::new(std::sync::Arc::new(
                        cdf_state_sqlite::InMemoryScopeLeaseStore::new(),
                    )),
                ))
                .expect("conformance staging lease authority")
        })
        .clone()
}

#[cfg(test)]
pub(crate) fn test_rest_runtime_dependencies(
    transport: impl cdf_http::HttpTransport + 'static,
) -> cdf_declarative::RestRuntimeDependencies {
    let services = test_execution_services();
    services
        .ensure_blocking_lanes(&[cdf_runtime::BlockingLaneSpec {
            lane_id: "rest-source.sync".to_owned(),
            maximum_concurrency: 8,
            cpu_slot_cost: 1,
            native_internal_parallelism: 1,
            affinity: cdf_runtime::LaneAffinity::Shared,
            interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
        }])
        .expect("conformance REST blocking lane");
    cdf_declarative::RestRuntimeDependencies::new(transport, services)
}
