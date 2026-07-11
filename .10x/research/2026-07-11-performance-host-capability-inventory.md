Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Performance host capability inventory

## Question

How should the P3 lab collect comparable host, RSS, cache, counter, device, and profile evidence across developer macOS, Linux CI, and containers without scattering platform commands through runners?

## Sources and methods

Inspected the current local host through read-only OS/tool probes and compared the observations to L1/L3 report requirements. No cache eviction, profiling capture, benchmark, package installation, or privileged command was run.

## Findings

The current reference development host is Apple arm64 with 18 logical cores, split by performance levels into 6 and 12 cores, 24 GiB memory, and an APFS workspace volume. Rust/Cargo are 1.96.1. Exact host name and device identifiers are intentionally omitted.

`/usr/bin/time -l` is available and reports real/user/system time, maximum RSS, page activity, context switches, instructions, cycles, and peak memory footprint. `vm_stat`, `iostat`, `sample`, `spindump`, `fs_usage`, `powermetrics`, `purge`, and the Xcode tool launcher exist, but several require privileges or are unsuitable as unconditional automation. Cargo flamegraph and the DuckDB CLI are installed. `fio`, Linux `perf`, hyperfine, and Polars are unavailable.

This host can support warm macro measurement, child-process wall/CPU/peak-RSS observation, DuckDB references, and opt-in flamegraph sampling. It cannot truthfully claim a `fio` roofline, Linux hardware-counter profile, Polars comparison, or controlled cold cache without an explicit capability/provider.

Linux CI/container hosts will expose different authorities: `/proc`, cgroup limits/peaks, `/usr/bin/time -v`, optional `perf`, and optional `fio`. Containers may report host CPU while enforcing a smaller cpuset/memory limit, so effective quota/affinity belongs in the fingerprint.

## Conclusion

Host observation must be a provider boundary. One sanitized fingerprint provider and independent measurement capabilities report structured supported/unavailable/error states with exact method/tool version. Macro workloads consume those records and never shell-match the OS themselves.

The first providers should be macOS, Linux/procfs, Linux/cgroup overlay, and portable fallback. Cache control and privileged counters are opt-in; missing authority yields an unavailable cold/counter cell. Device roofline uses an internal sequential-I/O runner for portability and may cross-check `fio` when present. External reference tools remain subprocess-isolated and absent tools do not enter the CDF Cargo graph.

## Limits

This inventory records availability, not measurement correctness. L3 must validate RSS units, cgroup semantics, filesystem/cache behavior, child termination, and counter permissions with fixtures before baseline use.
