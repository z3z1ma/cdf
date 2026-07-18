use std::{
    collections::BTreeMap,
    fs,
    io::Read,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use crate::{
    BenchResult, CachePreparation, Capability, ChildCommand, ChildObservation,
    ChildObservationStatus, EffectiveCpu, HostCapabilityProvider, HostFingerprint, IoMode,
    MeasurementProviderIdentity, OsFingerprint, StorageClass, ToolIdentity, bench_error,
};

const PROVIDER_VERSION: &str = "system-host-v1";
const MAX_CHILD_STDOUT_BYTES: usize = 1024 * 1024;
const MAX_CHILD_STDERR_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug)]
pub struct HostProbeConfig {
    pub cdf_version: String,
    pub dependency_versions: BTreeMap<String, String>,
    pub benchmark_profile: String,
    pub storage_target: Option<PathBuf>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Platform {
    MacOs,
    Linux,
    Portable,
}

#[derive(Clone, Debug)]
pub struct SystemHostProvider {
    config: HostProbeConfig,
    platform: Platform,
}

impl SystemHostProvider {
    pub fn new(config: HostProbeConfig) -> Self {
        let platform = match std::env::consts::OS {
            "macos" => Platform::MacOs,
            "linux" => Platform::Linux,
            _ => Platform::Portable,
        };
        Self { config, platform }
    }

    #[cfg(test)]
    fn with_platform(config: HostProbeConfig, platform: Platform) -> Self {
        Self { config, platform }
    }
}

impl HostCapabilityProvider for SystemHostProvider {
    fn fingerprint(&self) -> BenchResult<HostFingerprint> {
        let advertised_logical_cores = u32::try_from(
            thread::available_parallelism()
                .map_err(|error| bench_error(format!("logical CPU probe failed: {error}")))?
                .get(),
        )
        .map_err(|error| bench_error(format!("logical CPU count overflow: {error}")))?;
        let advertised_physical_cores = physical_cores(self.platform);
        let advertised_memory_bytes = memory_bytes(self.platform);
        let effective_cpu = effective_cpu(self.platform, advertised_logical_cores);
        let effective_memory_bytes = effective_memory(self.platform);
        Ok(HostFingerprint {
            schema_version: 1,
            architecture: sanitize(std::env::consts::ARCH),
            cpu_label: cpu_label(self.platform),
            advertised_logical_cores,
            advertised_physical_cores,
            advertised_memory_bytes,
            effective_cpu,
            effective_memory_bytes,
            storage: storage_class(self.platform, self.config.storage_target.as_deref()),
            os: os_fingerprint(self.platform),
            rust_version: command_first_line("rustc", &["--version"])
                .map(|line| sanitize(&line))
                .unwrap_or_else(|| "unavailable".to_owned()),
            cdf_version: sanitize(&self.config.cdf_version),
            dependency_versions: self
                .config
                .dependency_versions
                .iter()
                .map(|(name, version)| (sanitize(name), sanitize(version)))
                .collect(),
            benchmark_profile: sanitize(&self.config.benchmark_profile),
        })
    }

    fn prepare_io_mode(
        &self,
        mode: IoMode,
        allow_privileged: bool,
    ) -> Capability<CachePreparation> {
        if mode != IoMode::Cold {
            return Capability::Supported {
                value: CachePreparation {
                    mode,
                    method: match mode {
                        IoMode::Warm => "retained-os-cache".to_owned(),
                        IoMode::Uncontrolled => "uncontrolled-os-cache".to_owned(),
                        IoMode::Cold => unreachable!(),
                    },
                },
                method: "nonprivileged-cache-policy".to_owned(),
                provider_version: PROVIDER_VERSION.to_owned(),
            };
        }
        if !allow_privileged {
            return Capability::Unavailable {
                reason: "cold cache control is privileged and was not opted in".to_owned(),
                method: "privileged-cache-policy".to_owned(),
                provider_version: PROVIDER_VERSION.to_owned(),
            };
        }
        let result = match self.platform {
            Platform::MacOs => Command::new("purge")
                .status()
                .map(|status| status.success()),
            Platform::Linux => Command::new("sync").status().and_then(|status| {
                if status.success() {
                    fs::write("/proc/sys/vm/drop_caches", b"3\n").map(|_| true)
                } else {
                    Ok(false)
                }
            }),
            Platform::Portable => {
                return Capability::Unavailable {
                    reason: "cold cache control has no provider on this platform".to_owned(),
                    method: "portable-cache-policy".to_owned(),
                    provider_version: PROVIDER_VERSION.to_owned(),
                };
            }
        };
        match result {
            Ok(true) => Capability::Supported {
                value: CachePreparation {
                    mode,
                    method: "privileged-os-cache-eviction".to_owned(),
                },
                method: "privileged-os-cache-eviction".to_owned(),
                provider_version: PROVIDER_VERSION.to_owned(),
            },
            Ok(false) => Capability::Failed {
                error: "cold cache control command returned failure".to_owned(),
                method: "privileged-os-cache-eviction".to_owned(),
                provider_version: PROVIDER_VERSION.to_owned(),
            },
            Err(_) => Capability::Failed {
                error: "cold cache control operation failed".to_owned(),
                method: "privileged-os-cache-eviction".to_owned(),
                provider_version: PROVIDER_VERSION.to_owned(),
            },
        }
    }

    fn observe_child(
        &self,
        command: &ChildCommand,
        timeout: Duration,
    ) -> BenchResult<ChildObservationStatus> {
        observe_child(command, timeout, self.platform)
    }

    fn discover_tool(&self, name: &str) -> Capability<ToolIdentity> {
        let Some(version) = command_first_line(name, &["--version"]) else {
            return Capability::Unavailable {
                reason: format!("{name} executable is not available"),
                method: "path-command-version".to_owned(),
                provider_version: PROVIDER_VERSION.to_owned(),
            };
        };
        Capability::Supported {
            value: ToolIdentity {
                name: sanitize(name),
                version: sanitize(&version),
                executable: sanitize(name),
            },
            method: "path-command-version".to_owned(),
            provider_version: PROVIDER_VERSION.to_owned(),
        }
    }

    fn process_observer_identity(&self) -> MeasurementProviderIdentity {
        let has_time = Path::new("/usr/bin/time").is_file();
        match (self.platform, has_time) {
            (Platform::MacOs, true) => MeasurementProviderIdentity {
                method: "bsd-time-l-child-process".to_owned(),
                version: PROVIDER_VERSION.to_owned(),
                observes_cpu_time: true,
                observes_peak_rss: true,
            },
            (Platform::Linux, true) => MeasurementProviderIdentity {
                method: "gnu-time-v-child-process".to_owned(),
                version: PROVIDER_VERSION.to_owned(),
                observes_cpu_time: true,
                observes_peak_rss: true,
            },
            _ => MeasurementProviderIdentity {
                method: "portable-monotonic-child-process".to_owned(),
                version: PROVIDER_VERSION.to_owned(),
                observes_cpu_time: false,
                observes_peak_rss: false,
            },
        }
    }

    fn cgroup_memory_report(&self) -> Capability<cdf_memory::CgroupV2MemoryReport> {
        if self.platform != Platform::Linux {
            return Capability::Unavailable {
                reason: "cgroup v2 memory authority is only available on Linux".to_owned(),
                method: "linux-current-cgroup-v2-memory".to_owned(),
                provider_version: cdf_memory::CGROUP_V2_MEMORY_PROVIDER_VERSION.to_owned(),
            };
        }
        match cdf_memory::current_cgroup_v2_memory_report() {
            Ok(value) => Capability::Supported {
                value,
                method: "linux-current-cgroup-v2-memory".to_owned(),
                provider_version: cdf_memory::CGROUP_V2_MEMORY_PROVIDER_VERSION.to_owned(),
            },
            Err(error) => Capability::Failed {
                error,
                method: "linux-current-cgroup-v2-memory".to_owned(),
                provider_version: cdf_memory::CGROUP_V2_MEMORY_PROVIDER_VERSION.to_owned(),
            },
        }
    }
}

fn observe_child(
    command: &ChildCommand,
    timeout: Duration,
    platform: Platform,
) -> BenchResult<ChildObservationStatus> {
    let temp = tempfile::tempdir()?;
    let time_path = temp.path().join("time");

    let use_time = Path::new("/usr/bin/time").is_file()
        && matches!(platform, Platform::MacOs | Platform::Linux);
    let mut process = if use_time {
        let mut process = Command::new("/usr/bin/time");
        match platform {
            Platform::MacOs => {
                process.args(["-l", "-o"]);
            }
            Platform::Linux => {
                process.args(["-v", "-o"]);
            }
            Platform::Portable => unreachable!(),
        }
        process.arg(&time_path).arg(&command.program);
        process.args(&command.args);
        process
    } else {
        let mut process = Command::new(&command.program);
        process.args(&command.args);
        process
    };
    process
        .envs(&command.environment)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(current_dir) = &command.current_dir {
        process.current_dir(current_dir);
    }
    let started = Instant::now();
    let mut child = process.spawn()?;
    let mut child_stdout = child
        .stdout
        .take()
        .ok_or_else(|| bench_error("isolated child stdout pipe was not created"))?;
    let mut child_stderr = child
        .stderr
        .take()
        .ok_or_else(|| bench_error("isolated child stderr pipe was not created"))?;
    let stdout_reader = thread::spawn(move || -> std::io::Result<(Vec<u8>, bool)> {
        read_limited(&mut child_stdout, MAX_CHILD_STDOUT_BYTES)
    });
    let stderr_reader = thread::spawn(move || -> std::io::Result<(Vec<u8>, bool)> {
        read_limited(&mut child_stderr, MAX_CHILD_STDERR_BYTES)
    });
    let status = loop {
        if let Some(status) = child.try_wait()? {
            break status;
        }
        if started.elapsed() >= timeout {
            child.kill()?;
            child.wait()?;
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            return Ok(ChildObservationStatus::TimedOut);
        }
        thread::sleep(Duration::from_millis(5));
    };
    if !status.success() {
        let _ = stdout_reader.join();
        let stderr = match stderr_reader.join() {
            Ok(Ok((bytes, overflow))) => child_stderr_text(bytes, overflow),
            Ok(Err(error)) => format!("failed to read child stderr: {error}"),
            Err(_) => "child stderr reader panicked".to_owned(),
        };
        return Ok(ChildObservationStatus::Failed {
            exit_code: status.code(),
            stderr,
        });
    }
    let (stdout, stdout_overflow) = stdout_reader
        .join()
        .map_err(|_| bench_error("isolated child stdout reader panicked"))??;
    let _stderr = stderr_reader
        .join()
        .map_err(|_| bench_error("isolated child stderr reader panicked"))??;
    if stdout_overflow {
        return Err(bench_error(
            "isolated benchmark child exceeded the 1 MiB measurement output limit",
        ));
    }
    let (cpu_time_ns, peak_rss_bytes) = if use_time {
        parse_time_output(platform, &fs::read_to_string(time_path).unwrap_or_default())
    } else {
        (None, None)
    };
    Ok(ChildObservationStatus::Completed(ChildObservation {
        wall_time_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        cpu_time_ns,
        peak_rss_bytes,
        stdout,
    }))
}

fn read_limited(reader: &mut impl Read, limit: usize) -> std::io::Result<(Vec<u8>, bool)> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8192];
    let mut overflow = false;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        let available = limit.saturating_sub(output.len());
        let retained = available.min(read);
        output.extend_from_slice(&buffer[..retained]);
        overflow |= retained != read;
    }
    Ok((output, overflow))
}

fn child_stderr_text(bytes: Vec<u8>, overflow: bool) -> String {
    let mut text = String::from_utf8_lossy(&bytes)
        .chars()
        .map(|character| match character {
            '/' | '\\' | '@' => '-',
            character if character.is_control() => ' ',
            character => character,
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    if overflow {
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str("[stderr truncated]");
    }
    text
}

fn parse_time_output(platform: Platform, output: &str) -> (Option<u64>, Option<u64>) {
    match platform {
        Platform::MacOs => {
            let mut user = None;
            let mut system = None;
            let mut rss = None;
            for line in output.lines() {
                let line = line.trim();
                let parts = line.split_whitespace().collect::<Vec<_>>();
                if parts.len() == 6 && parts[1] == "real" && parts[3] == "user" && parts[5] == "sys"
                {
                    user = seconds_to_ns(parts[2]);
                    system = seconds_to_ns(parts[4]);
                }
                if let Some(value) = line.strip_suffix(" maximum resident set size") {
                    rss = value.trim().parse().ok();
                }
            }
            (sum_options(user, system), rss)
        }
        Platform::Linux => {
            let mut user = None;
            let mut system = None;
            let mut rss_kib = None;
            for line in output.lines() {
                let line = line.trim();
                if let Some(value) = line.strip_prefix("User time (seconds):") {
                    user = seconds_to_ns(value.trim());
                } else if let Some(value) = line.strip_prefix("System time (seconds):") {
                    system = seconds_to_ns(value.trim());
                } else if let Some(value) = line.strip_prefix("Maximum resident set size (kbytes):")
                {
                    rss_kib = value.trim().parse::<u64>().ok();
                }
            }
            (sum_options(user, system), rss_kib.map(|value| value * 1024))
        }
        Platform::Portable => (None, None),
    }
}

fn seconds_to_ns(value: &str) -> Option<u64> {
    let seconds = value.parse::<f64>().ok()?;
    if !seconds.is_finite() || seconds.is_sign_negative() {
        return None;
    }
    Some((seconds * 1_000_000_000.0) as u64)
}

fn sum_options(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        _ => None,
    }
}

fn physical_cores(platform: Platform) -> Capability<u32> {
    let value = match platform {
        Platform::MacOs => command_first_line("sysctl", &["-n", "hw.physicalcpu"])
            .and_then(|value| value.parse().ok()),
        Platform::Linux => linux_physical_cores(),
        Platform::Portable => None,
    };
    capability_from_option(value, "os-cpu-topology", "physical core count unavailable")
}

fn memory_bytes(platform: Platform) -> Capability<u64> {
    let value = match platform {
        Platform::MacOs => {
            command_first_line("sysctl", &["-n", "hw.memsize"]).and_then(|value| value.parse().ok())
        }
        Platform::Linux => fs::read_to_string("/proc/meminfo").ok().and_then(|text| {
            text.lines().find_map(|line| {
                line.strip_prefix("MemTotal:")?
                    .split_whitespace()
                    .next()?
                    .parse::<u64>()
                    .ok()
                    .map(|kib| kib * 1024)
            })
        }),
        Platform::Portable => None,
    };
    capability_from_option(value, "os-memory-authority", "host memory unavailable")
}

fn effective_cpu(platform: Platform, advertised: u32) -> Capability<EffectiveCpu> {
    if platform != Platform::Linux {
        return Capability::Supported {
            value: EffectiveCpu {
                logical_cores: advertised,
                quota_millicores: None,
                affinity_cores: Some(advertised),
            },
            method: "process-available-parallelism".to_owned(),
            provider_version: PROVIDER_VERSION.to_owned(),
        };
    }
    let quota_millicores = fs::read_to_string("/sys/fs/cgroup/cpu.max")
        .ok()
        .and_then(|value| parse_cpu_max(&value));
    Capability::Supported {
        value: EffectiveCpu {
            logical_cores: advertised,
            quota_millicores,
            affinity_cores: Some(advertised),
        },
        method: "linux-cgroup-overlay".to_owned(),
        provider_version: PROVIDER_VERSION.to_owned(),
    }
}

fn effective_memory(platform: Platform) -> Capability<u64> {
    if platform != Platform::Linux {
        return Capability::Unavailable {
            reason: "no container memory overlay on this platform".to_owned(),
            method: "container-memory-overlay".to_owned(),
            provider_version: PROVIDER_VERSION.to_owned(),
        };
    }
    let value = cdf_memory::current_cgroup_v2_memory_report()
        .ok()
        .and_then(|report| report.max_bytes);
    capability_from_option(
        value,
        "linux-current-cgroup-v2-memory",
        "cgroup memory limit unavailable",
    )
}

fn storage_class(platform: Platform, target: Option<&Path>) -> Capability<StorageClass> {
    let Some(target) = target else {
        return Capability::Unavailable {
            reason: "storage class requires an explicit benchmark target probe".to_owned(),
            method: "filesystem-target-probe".to_owned(),
            provider_version: PROVIDER_VERSION.to_owned(),
        };
    };
    let filesystem = match platform {
        Platform::MacOs => mac_filesystem(target),
        Platform::Linux => command_first_line_path("stat", &["-f", "-c", "%T"], target),
        Platform::Portable => None,
    };
    let Some(filesystem) = filesystem else {
        return Capability::Unavailable {
            reason: "target filesystem class is unavailable".to_owned(),
            method: "filesystem-target-probe".to_owned(),
            provider_version: PROVIDER_VERSION.to_owned(),
        };
    };
    let filesystem = sanitize(&filesystem);
    Capability::Supported {
        value: StorageClass {
            medium: "unknown".to_owned(),
            label: format!("{filesystem}-storage-class"),
            filesystem,
            free_bytes: None,
        },
        method: "filesystem-target-probe".to_owned(),
        provider_version: PROVIDER_VERSION.to_owned(),
    }
}

fn mac_filesystem(target: &Path) -> Option<String> {
    let target = target.canonicalize().ok()?;
    let output = Command::new("mount").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let output = String::from_utf8(output.stdout).ok()?;
    output
        .lines()
        .filter_map(|line| {
            let (_, mounted) = line.split_once(" on ")?;
            let (mount_point, options) = mounted.split_once(" (")?;
            if !target.starts_with(mount_point) {
                return None;
            }
            Some((mount_point.len(), options.split(',').next()?.to_owned()))
        })
        .max_by_key(|(length, _)| *length)
        .map(|(_, filesystem)| filesystem)
}

fn cpu_label(platform: Platform) -> String {
    let label = match platform {
        Platform::MacOs => command_first_line("sysctl", &["-n", "machdep.cpu.brand_string"]),
        Platform::Linux => fs::read_to_string("/proc/cpuinfo").ok().and_then(|text| {
            text.lines().find_map(|line| {
                line.split_once(':')
                    .filter(|(key, _)| key.trim() == "model name")
                    .map(|(_, value)| value.trim().to_owned())
            })
        }),
        Platform::Portable => None,
    };
    sanitize(&label.unwrap_or_else(|| format!("{}-cpu-class", std::env::consts::ARCH)))
}

fn os_fingerprint(platform: Platform) -> OsFingerprint {
    let family = match platform {
        Platform::MacOs => "macos",
        Platform::Linux => "linux",
        Platform::Portable => std::env::consts::OS,
    };
    let version = match platform {
        Platform::MacOs => command_first_line("sw_vers", &["-productVersion"]),
        Platform::Linux => fs::read_to_string("/etc/os-release").ok().and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("VERSION_ID="))
                .map(|value| value.trim_matches('"').to_owned())
        }),
        Platform::Portable => None,
    }
    .unwrap_or_else(|| "unknown".to_owned());
    OsFingerprint {
        family: sanitize(family),
        version: sanitize(&version),
        kernel: command_first_line("uname", &["-r"]).map(|value| sanitize(&value)),
    }
}

fn linux_physical_cores() -> Option<u32> {
    let text = fs::read_to_string("/proc/cpuinfo").ok()?;
    let mut pairs = std::collections::BTreeSet::new();
    let mut physical = None;
    let mut core = None;
    for line in text.lines().chain(std::iter::once("")) {
        if line.trim().is_empty() {
            if let (Some(physical), Some(core)) = (physical.take(), core.take()) {
                pairs.insert((physical, core));
            }
        } else if let Some((key, value)) = line.split_once(':') {
            match key.trim() {
                "physical id" => physical = value.trim().parse::<u32>().ok(),
                "core id" => core = value.trim().parse::<u32>().ok(),
                _ => {}
            }
        }
    }
    u32::try_from(pairs.len()).ok().filter(|count| *count > 0)
}

fn parse_cpu_max(value: &str) -> Option<u32> {
    let mut parts = value.split_whitespace();
    let quota = parts.next()?;
    if quota == "max" {
        return None;
    }
    let quota = quota.parse::<u64>().ok()?;
    let period = parts.next()?.parse::<u64>().ok()?;
    u32::try_from(quota.saturating_mul(1000) / period).ok()
}

fn capability_from_option<T>(value: Option<T>, method: &str, reason: &str) -> Capability<T> {
    match value {
        Some(value) => Capability::Supported {
            value,
            method: method.to_owned(),
            provider_version: PROVIDER_VERSION.to_owned(),
        },
        None => Capability::Unavailable {
            reason: reason.to_owned(),
            method: method.to_owned(),
            provider_version: PROVIDER_VERSION.to_owned(),
        },
    }
}

fn command_first_line(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
}

fn command_first_line_path(program: &str, args: &[&str], path: &Path) -> Option<String> {
    let output = Command::new(program).args(args).arg(path).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .next()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|character| match character {
            '/' | '\\' | '@' => '-',
            character if character.is_control() => ' ',
            character => character,
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_output_parsers_use_documented_rss_units() {
        let mac = "0.10 real 0.07 user 0.02 sys\n  123456 maximum resident set size\n";
        assert_eq!(
            parse_time_output(Platform::MacOs, mac),
            (Some(90_000_000), Some(123_456))
        );
        let linux = "User time (seconds): 0.07\nSystem time (seconds): 0.02\nMaximum resident set size (kbytes): 100\n";
        assert_eq!(
            parse_time_output(Platform::Linux, linux),
            (Some(90_000_000), Some(102_400))
        );
    }

    #[test]
    fn cgroup_cpu_quota_parser_handles_bounded_and_unbounded_values() {
        assert_eq!(parse_cpu_max("200000 100000\n"), Some(2_000));
        assert_eq!(parse_cpu_max("max 100000\n"), None);
    }

    #[test]
    fn portable_provider_reports_cold_cache_as_unavailable() {
        let provider = SystemHostProvider::with_platform(
            HostProbeConfig {
                cdf_version: "fixture".to_owned(),
                dependency_versions: BTreeMap::new(),
                benchmark_profile: "test".to_owned(),
                storage_target: None,
            },
            Platform::Portable,
        );
        assert!(matches!(
            provider.prepare_io_mode(IoMode::Cold, false),
            Capability::Unavailable { .. }
        ));
    }

    #[test]
    fn failed_child_retains_bounded_stderr_evidence() {
        let status = observe_child(
            &ChildCommand {
                program: PathBuf::from("/bin/sh"),
                args: vec![
                    "-c".to_owned(),
                    "printf 'useful child failure\\nwith context\\n' >&2; exit 7".to_owned(),
                ],
                environment: BTreeMap::new(),
                current_dir: None,
            },
            Duration::from_secs(5),
            Platform::Portable,
        )
        .unwrap();
        let ChildObservationStatus::Failed { exit_code, stderr } = status else {
            panic!("expected failed child observation");
        };
        assert_eq!(exit_code, Some(7));
        assert!(stderr.contains("useful child failure with context"));
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_time_wrapped_failed_child_retains_stderr_evidence() {
        let status = observe_child(
            &ChildCommand {
                program: PathBuf::from("/bin/sh"),
                args: vec![
                    "-c".to_owned(),
                    "printf 'macos wrapped failure\\n' >&2; exit 9".to_owned(),
                ],
                environment: BTreeMap::new(),
                current_dir: None,
            },
            Duration::from_secs(5),
            Platform::MacOs,
        )
        .unwrap();
        let ChildObservationStatus::Failed { exit_code, stderr } = status else {
            panic!("expected failed child observation");
        };
        assert_eq!(exit_code, Some(9));
        assert!(stderr.contains("macos wrapped failure"));
    }
}
