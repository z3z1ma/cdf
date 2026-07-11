#![doc = "Conformance harness boundary for cdf."]

pub mod checkpoint_store;
pub mod destination;
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

#[cfg(test)]
pub(crate) fn test_execution_services() -> cdf_runtime::ExecutionServices {
    static SERVICES: std::sync::OnceLock<cdf_runtime::ExecutionServices> =
        std::sync::OnceLock::new();
    SERVICES
        .get_or_init(|| {
            cdf_engine::StandaloneExecutionHost::default_services(128 * 1024 * 1024)
                .expect("conformance execution host")
                .1
        })
        .clone()
}
