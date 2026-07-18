use std::path::Path;

use cdf_cli_core::{
    args::{Cli, parse_byte_size},
    output::CliError,
};
use serde::Serialize;

const PROVIDER_VERSION: &str = "cdf-runtime-budget-v1";
#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
#[cfg(target_os = "linux")]
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";
const MINIMUM_WORKING_SET_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct RuntimeBudgetReport {
    pub resolution: cdf_memory::MemoryBudgetResolution,
    pub process_budget_source: BudgetValueSource,
    pub spill_budget_source: BudgetValueSource,
    pub memory_authority: MemoryAuthorityReport,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BudgetValueSource {
    Cli,
    Environment,
    DefaultPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct MemoryAuthorityReport {
    pub method: &'static str,
    pub provider_version: &'static str,
    pub enforcement: MemoryEnforcement,
    pub effective_authority_bytes: u64,
    pub caveats: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cgroup_v2: Option<cdf_memory::CgroupV2MemoryReport>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryEnforcement {
    LinuxCgroupV2,
    Unavailable,
}

impl RuntimeBudgetReport {
    pub(crate) fn has_enforced_memory_authority(&self) -> bool {
        matches!(
            self.memory_authority.enforcement,
            MemoryEnforcement::LinuxCgroupV2
        )
    }
}

pub(crate) fn resolve(cli: &Cli) -> Result<RuntimeBudgetReport, CliError> {
    resolve_from(
        cli.memory_budget
            .map(|value| (BudgetValueSource::Cli, value)),
        cli.spill_budget
            .map(|value| (BudgetValueSource::Cli, value)),
        env_byte_size("CDF_MEMORY_BUDGET")?.map(|value| (BudgetValueSource::Environment, value)),
        env_byte_size("CDF_SPILL_BUDGET")?.map(|value| (BudgetValueSource::Environment, value)),
        detect_memory_authority(),
    )
}

fn resolve_from(
    cli_memory_budget: Option<(BudgetValueSource, u64)>,
    cli_spill_budget: Option<(BudgetValueSource, u64)>,
    env_memory_budget: Option<(BudgetValueSource, u64)>,
    env_spill_budget: Option<(BudgetValueSource, u64)>,
    mut memory_authority: MemoryAuthorityReport,
) -> Result<RuntimeBudgetReport, CliError> {
    let (process_budget_source, requested_process_bytes) =
        match cli_memory_budget.or(env_memory_budget) {
            Some((source, value)) => (source, Some(value)),
            None => (BudgetValueSource::DefaultPolicy, None),
        };
    let (spill_budget_source, spill_budget_bytes) = match cli_spill_budget.or(env_spill_budget) {
        Some((source, value)) => (source, value),
        None => (
            BudgetValueSource::DefaultPolicy,
            cdf_memory::DEFAULT_SPILL_BUDGET_BYTES,
        ),
    };
    let fallback_authority =
        requested_process_bytes.unwrap_or(cdf_memory::DEFAULT_PROCESS_BUDGET_BYTES);
    if memory_authority.effective_authority_bytes == 0 {
        memory_authority.effective_authority_bytes = fallback_authority;
    }
    let resolve_budget = if matches!(
        memory_authority.enforcement,
        MemoryEnforcement::LinuxCgroupV2
    ) {
        cdf_memory::resolve_memory_budget
    } else {
        cdf_memory::resolve_unenforced_memory_budget
    };
    let resolution = resolve_budget(
        requested_process_bytes,
        memory_authority.effective_authority_bytes,
        MINIMUM_WORKING_SET_BYTES,
        spill_budget_bytes,
    )?;
    Ok(RuntimeBudgetReport {
        resolution,
        process_budget_source,
        spill_budget_source,
        memory_authority,
    })
}

fn env_byte_size(name: &str) -> Result<Option<u64>, CliError> {
    match std::env::var(name) {
        Ok(value) => parse_byte_size(name, &value).map(Some),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => Err(CliError::usage(format!(
            "{name} must be valid UTF-8 when set"
        ))),
    }
}

fn detect_memory_authority() -> MemoryAuthorityReport {
    #[cfg(target_os = "linux")]
    {
        return memory_authority_from_current_cgroup(
            Path::new(CGROUP_ROOT),
            Path::new(PROC_SELF_CGROUP),
        );
    }
    #[cfg(not(target_os = "linux"))]
    {
        MemoryAuthorityReport {
            method: "portable-unenforced-process-budget",
            provider_version: PROVIDER_VERSION,
            enforcement: MemoryEnforcement::Unavailable,
            effective_authority_bytes: 0,
            caveats: vec![
                "cgroup v2 memory enforcement is unavailable on this platform; process RSS must be measured externally".to_owned(),
            ],
            cgroup_v2: None,
        }
    }
}

#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn memory_authority_from_current_cgroup(
    cgroup_root: &Path,
    proc_self_cgroup: &Path,
) -> MemoryAuthorityReport {
    match cdf_memory::current_cgroup_v2_memory_report_from(cgroup_root, proc_self_cgroup) {
        Ok(report) => memory_authority_from_cgroup_report(report),
        Err(error) => MemoryAuthorityReport {
            method: "linux-cgroup-v2",
            provider_version: PROVIDER_VERSION,
            enforcement: MemoryEnforcement::Unavailable,
            effective_authority_bytes: 0,
            caveats: vec![format!(
                "could not resolve the current cgroup v2 memory path: {error}"
            )],
            cgroup_v2: None,
        },
    }
}

#[cfg_attr(not(test), allow(dead_code))]
fn memory_authority_from_cgroup_report(
    cgroup_v2: cdf_memory::CgroupV2MemoryReport,
) -> MemoryAuthorityReport {
    match cgroup_v2.max_bytes {
        Some(max_bytes) => MemoryAuthorityReport {
            method: "linux-cgroup-v2",
            provider_version: PROVIDER_VERSION,
            enforcement: MemoryEnforcement::LinuxCgroupV2,
            effective_authority_bytes: max_bytes,
            caveats: Vec::new(),
            cgroup_v2: Some(cgroup_v2),
        },
        None => {
            let mut caveats = vec![
                "cgroup v2 memory.max is unbounded or unavailable; CDF can resolve a process budget but cannot claim OS memory enforcement from this provider".to_owned(),
            ];
            if !cgroup_v2.read_errors.is_empty() {
                caveats.push(
                    "one or more cgroup memory files were unreadable; inspect the provider details before treating this host as a constant-memory proof target"
                        .to_owned(),
                );
            }
            MemoryAuthorityReport {
                method: "linux-cgroup-v2",
                provider_version: PROVIDER_VERSION,
                enforcement: MemoryEnforcement::Unavailable,
                effective_authority_bytes: 0,
                caveats,
                cgroup_v2: Some(cgroup_v2),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, path::PathBuf};

    use super::*;

    #[test]
    fn cgroup_authority_reports_bounded_provider_without_conflating_metrics() {
        let mut events = BTreeMap::new();
        events.insert("high".to_owned(), 1);
        let report = memory_authority_from_cgroup_report(cdf_memory::CgroupV2MemoryReport {
            root: PathBuf::from("/sys/fs/cgroup/test.slice"),
            max_bytes: Some(1_073_741_824),
            current_bytes: Some(268_435_456),
            peak_bytes: Some(536_870_912),
            events,
            read_errors: BTreeMap::new(),
        });
        assert_eq!(report.provider_version, PROVIDER_VERSION);
        assert_eq!(report.enforcement, MemoryEnforcement::LinuxCgroupV2);
        assert_eq!(report.effective_authority_bytes, 1_073_741_824);
        let cgroup = report.cgroup_v2.unwrap();
        assert_eq!(cgroup.max_bytes, Some(1_073_741_824));
        assert_eq!(cgroup.current_bytes, Some(268_435_456));
        assert_eq!(cgroup.peak_bytes, Some(536_870_912));
        assert_eq!(cgroup.events["high"], 1);
        assert!(cgroup.read_errors.is_empty(), "{:?}", cgroup.read_errors);
    }

    #[test]
    fn cgroup_authority_keeps_unbounded_provider_as_caveated_unenforced() {
        let mut read_errors = BTreeMap::new();
        read_errors.insert("memory.current".to_owned(), "permission denied".to_owned());
        let report = memory_authority_from_cgroup_report(cdf_memory::CgroupV2MemoryReport {
            root: PathBuf::from("/sys/fs/cgroup/test.slice"),
            max_bytes: None,
            current_bytes: None,
            peak_bytes: None,
            events: BTreeMap::new(),
            read_errors,
        });
        assert_eq!(report.enforcement, MemoryEnforcement::Unavailable);
        assert_eq!(report.effective_authority_bytes, 0);
        assert_eq!(report.caveats.len(), 2);
        assert!(report.cgroup_v2.is_some());
    }

    #[test]
    fn current_cgroup_authority_reads_the_resolved_scope_files() {
        let root = unique_temp_dir("cdf-current-cgroup");
        let scope = root.join("user.slice/user-1000.slice/session-7.scope");
        std::fs::create_dir_all(&scope).unwrap();
        let proc = root.join("proc-self-cgroup");
        std::fs::write(&proc, "0::/user.slice/user-1000.slice/session-7.scope\n").unwrap();
        std::fs::write(scope.join("memory.max"), "2147483648\n").unwrap();
        std::fs::write(scope.join("memory.current"), "1234\n").unwrap();
        std::fs::write(scope.join("memory.peak"), "5678\n").unwrap();
        std::fs::write(scope.join("memory.events"), "oom 0\noom_kill 0\n").unwrap();

        let report = memory_authority_from_current_cgroup(&root, &proc);
        assert_eq!(report.enforcement, MemoryEnforcement::LinuxCgroupV2);
        assert_eq!(report.effective_authority_bytes, 2_147_483_648);
        let cgroup = report.cgroup_v2.unwrap();
        assert_eq!(cgroup.root, scope);
        assert_eq!(cgroup.current_bytes, Some(1234));
        assert_eq!(cgroup.peak_bytes, Some(5678));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn resolution_uses_cgroup_as_ceiling_and_default_policy_as_unenforced_fallback() {
        let gib = 1024 * 1024 * 1024;
        let cgroup = MemoryAuthorityReport {
            method: "linux-cgroup-v2",
            provider_version: PROVIDER_VERSION,
            enforcement: MemoryEnforcement::LinuxCgroupV2,
            effective_authority_bytes: 4 * gib,
            caveats: Vec::new(),
            cgroup_v2: None,
        };
        let error = resolve_from(
            Some((BudgetValueSource::Cli, 8 * gib)),
            None,
            None,
            None,
            cgroup,
        )
        .unwrap_err();
        assert!(
            error
                .message
                .contains("requested process memory budget 8589934592 exceeds effective authority")
        );

        let fallback = MemoryAuthorityReport {
            method: "portable-unenforced-process-budget",
            provider_version: PROVIDER_VERSION,
            enforcement: MemoryEnforcement::Unavailable,
            effective_authority_bytes: 0,
            caveats: Vec::new(),
            cgroup_v2: None,
        };
        let resolved = resolve_from(
            Some((BudgetValueSource::Cli, 8 * gib)),
            None,
            Some((BudgetValueSource::Environment, 2 * gib)),
            Some((BudgetValueSource::Environment, 32 * gib)),
            fallback,
        )
        .unwrap();
        assert_eq!(resolved.resolution.process_budget_bytes, 8 * gib);
        assert_eq!(resolved.resolution.spill_budget_bytes, 32 * gib);
        assert_eq!(resolved.process_budget_source, BudgetValueSource::Cli);
        assert_eq!(resolved.spill_budget_source, BudgetValueSource::Environment);
        assert_eq!(resolved.memory_authority.effective_authority_bytes, 8 * gib);
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "{prefix}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }
}
