use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
};

use cdf_cli_core::{
    args::{Cli, parse_byte_size},
    output::CliError,
};
use serde::Serialize;

const PROVIDER_VERSION: &str = "cdf-runtime-budget-v1";
#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
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
    pub cgroup_v2: Option<CgroupMemoryReport>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum MemoryEnforcement {
    LinuxCgroupV2,
    Unavailable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub(crate) struct CgroupMemoryReport {
    pub root: PathBuf,
    pub max_bytes: Option<u64>,
    pub current_bytes: Option<u64>,
    pub peak_bytes: Option<u64>,
    pub events: BTreeMap<String, u64>,
    pub read_errors: BTreeMap<String, String>,
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
        return memory_authority_from_cgroup_root(Path::new(CGROUP_ROOT));
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

#[cfg_attr(not(test), allow(dead_code))]
fn memory_authority_from_cgroup_root(root: &Path) -> MemoryAuthorityReport {
    let cgroup_v2 = read_cgroup_memory(root);
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

fn read_cgroup_memory(root: &Path) -> CgroupMemoryReport {
    let mut read_errors = BTreeMap::new();
    let max_bytes = read_cgroup_file(root, "memory.max", &mut read_errors)
        .and_then(|value| parse_memory_max(&value).transpose())
        .transpose()
        .unwrap_or_else(|error| {
            read_errors.insert("memory.max".to_owned(), error);
            None
        });
    let current_bytes = read_cgroup_file(root, "memory.current", &mut read_errors)
        .and_then(|value| parse_nonnegative_file_u64("memory.current", &value, &mut read_errors));
    let peak_bytes = read_cgroup_file(root, "memory.peak", &mut read_errors)
        .and_then(|value| parse_nonnegative_file_u64("memory.peak", &value, &mut read_errors));
    let events = read_cgroup_file(root, "memory.events", &mut read_errors)
        .map(|value| parse_memory_events(&value, &mut read_errors))
        .unwrap_or_default();
    CgroupMemoryReport {
        root: root.to_path_buf(),
        max_bytes,
        current_bytes,
        peak_bytes,
        events,
        read_errors,
    }
}

fn read_cgroup_file(
    root: &Path,
    name: &'static str,
    read_errors: &mut BTreeMap<String, String>,
) -> Option<String> {
    match fs::read_to_string(root.join(name)) {
        Ok(value) => Some(value),
        Err(error) => {
            read_errors.insert(name.to_owned(), error.to_string());
            None
        }
    }
}

fn parse_memory_max(value: &str) -> Result<Option<u64>, String> {
    let trimmed = value.trim();
    if trimmed == "max" || trimmed.is_empty() {
        return Ok(None);
    }
    parse_positive_u64("memory.max", trimmed).map(Some)
}

fn parse_nonnegative_file_u64(
    name: &'static str,
    value: &str,
    read_errors: &mut BTreeMap<String, String>,
) -> Option<u64> {
    match value.trim().parse::<u64>() {
        Ok(value) => Some(value),
        Err(error) => {
            read_errors.insert(name.to_owned(), format!("invalid {name}: {error}"));
            None
        }
    }
}

fn parse_positive_u64(name: &'static str, value: &str) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{name} must be nonzero when bounded"));
    }
    Ok(parsed)
}

fn parse_memory_events(
    value: &str,
    read_errors: &mut BTreeMap<String, String>,
) -> BTreeMap<String, u64> {
    let mut events = BTreeMap::new();
    for (index, line) in value.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let mut parts = trimmed.split_whitespace();
        let Some(name) = parts.next() else {
            continue;
        };
        let Some(count) = parts.next() else {
            read_errors.insert(
                "memory.events".to_owned(),
                format!("line {} omitted event count", index + 1),
            );
            continue;
        };
        if parts.next().is_some() {
            read_errors.insert(
                "memory.events".to_owned(),
                format!("line {} contains more than two fields", index + 1),
            );
            continue;
        }
        match count.parse::<u64>() {
            Ok(count) => {
                events.insert(name.to_owned(), count);
            }
            Err(error) => {
                read_errors.insert(
                    "memory.events".to_owned(),
                    format!("line {} has invalid count: {error}", index + 1),
                );
            }
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_authority_reports_bounded_provider_without_conflating_metrics() {
        let root = unique_temp_dir("cdf-cgroup-memory");
        fs::create_dir_all(&root).unwrap();
        fs::write(root.join("memory.max"), "1073741824\n").unwrap();
        fs::write(root.join("memory.current"), "268435456\n").unwrap();
        fs::write(root.join("memory.peak"), "536870912\n").unwrap();
        fs::write(
            root.join("memory.events"),
            "low 0\nhigh 1\nmax 2\noom 0\noom_kill 0\n",
        )
        .unwrap();

        let report = memory_authority_from_cgroup_root(&root);
        assert_eq!(report.provider_version, PROVIDER_VERSION);
        assert_eq!(report.enforcement, MemoryEnforcement::LinuxCgroupV2);
        assert_eq!(report.effective_authority_bytes, 1_073_741_824);
        let cgroup = report.cgroup_v2.unwrap();
        assert_eq!(cgroup.max_bytes, Some(1_073_741_824));
        assert_eq!(cgroup.current_bytes, Some(268_435_456));
        assert_eq!(cgroup.peak_bytes, Some(536_870_912));
        assert_eq!(cgroup.events["high"], 1);
        assert!(cgroup.read_errors.is_empty(), "{:?}", cgroup.read_errors);

        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn cgroup_parsers_accept_bounded_unbounded_and_events() {
        assert_eq!(
            parse_memory_max("1073741824\n").unwrap(),
            Some(1_073_741_824)
        );
        assert_eq!(parse_memory_max("max\n").unwrap(), None);
        assert!(parse_memory_max("0\n").unwrap_err().contains("nonzero"));

        let mut errors = BTreeMap::new();
        let events = parse_memory_events("low 0\nhigh 1\nmax 2\noom 0\noom_kill 0\n", &mut errors);
        assert!(errors.is_empty(), "{errors:?}");
        assert_eq!(events["high"], 1);
        assert_eq!(events["oom_kill"], 0);
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
