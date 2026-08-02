use std::{
    collections::BTreeMap,
    fs,
    path::{Component, Path, PathBuf},
};

use serde::{Deserialize, Serialize};

pub const CGROUP_V2_MEMORY_PROVIDER_VERSION: &str = "cdf-cgroup-v2-memory-v1";

#[cfg(target_os = "linux")]
const CGROUP_ROOT: &str = "/sys/fs/cgroup";
#[cfg(target_os = "linux")]
const PROC_SELF_CGROUP: &str = "/proc/self/cgroup";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CgroupV2MemoryReport {
    pub root: PathBuf,
    pub max_bytes: Option<u64>,
    pub current_bytes: Option<u64>,
    pub peak_bytes: Option<u64>,
    pub events: BTreeMap<String, u64>,
    pub read_errors: BTreeMap<String, String>,
}

pub fn current_cgroup_v2_memory_report() -> std::result::Result<CgroupV2MemoryReport, String> {
    #[cfg(target_os = "linux")]
    {
        current_cgroup_v2_memory_report_from(Path::new(CGROUP_ROOT), Path::new(PROC_SELF_CGROUP))
    }
    #[cfg(not(target_os = "linux"))]
    {
        Err("cgroup v2 memory reporting is only available on Linux".to_owned())
    }
}

pub fn current_cgroup_v2_memory_report_from(
    cgroup_root: &Path,
    proc_self_cgroup: &Path,
) -> std::result::Result<CgroupV2MemoryReport, String> {
    let proc_cgroup = fs::read_to_string(proc_self_cgroup)
        .map_err(|error| format!("read {}: {error}", proc_self_cgroup.display()))?;
    let relative = parse_cgroup_v2_relative_path(&proc_cgroup)?;
    Ok(cgroup_v2_memory_report_from_root(
        &cgroup_root.join(relative),
    ))
}

pub(crate) fn cgroup_v2_memory_report_from_root(root: &Path) -> CgroupV2MemoryReport {
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
    CgroupV2MemoryReport {
        root: root.to_path_buf(),
        max_bytes,
        current_bytes,
        peak_bytes,
        events,
        read_errors,
    }
}

pub(crate) fn parse_cgroup_v2_relative_path(value: &str) -> std::result::Result<PathBuf, String> {
    let path = value
        .lines()
        .find_map(|line| {
            let mut parts = line.splitn(3, ':');
            let hierarchy = parts.next()?;
            let controllers = parts.next()?;
            let path = parts.next()?;
            (hierarchy == "0" && controllers.is_empty()).then_some(path)
        })
        .ok_or_else(|| "no cgroup v2 `0::` entry found in /proc/self/cgroup".to_owned())?;
    let trimmed = path.trim_start_matches('/');
    let relative = Path::new(trimmed);
    let mut sanitized = PathBuf::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => sanitized.push(part),
            Component::CurDir => {}
            Component::RootDir | Component::Prefix(_) | Component::ParentDir => {
                return Err(format!("unsafe cgroup v2 path component in `{path}`"));
            }
        }
    }
    Ok(sanitized)
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

fn parse_memory_max(value: &str) -> std::result::Result<Option<u64>, String> {
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

fn parse_positive_u64(name: &'static str, value: &str) -> std::result::Result<u64, String> {
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
