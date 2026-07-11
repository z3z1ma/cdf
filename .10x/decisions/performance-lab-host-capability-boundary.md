Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Performance lab host capability boundary

## Context

P3 results must compare host classes while the lab runs on macOS developer systems, Linux CI, and containers. Peak memory, CPU counters, cache control, device rooflines, and profilers have different authorities. Inline platform command branches would make workload runners untestable and silently convert missing tools into missing evidence.

## Decision

The lab owns a typed host/measurement capability boundary. Workload, report, comparator, and envelope code MUST NOT match OS names or execute platform commands directly.

Providers expose sanitized host facts and independent capabilities for:

- effective CPU topology/affinity/quota;
- effective memory/cgroup budget;
- target filesystem/device class and free capacity;
- child wall/CPU/peak-RSS observation;
- warm/cold cache control;
- sequential read/write roofline;
- hardware counters;
- profiler capture;
- external reference tool/version discovery.

Every capability result is `supported`, `unavailable(reason)`, or `failed(error)` with method and version. Unavailable is report data, never an omitted cell or successful zero. Privileged operations are disabled unless the operator explicitly opts in.

Host fingerprints omit hostname, username, paths, serial numbers, cloud instance identity, and stable device identifiers. The comparable fingerprint is derived from performance-relevant sanitized facts plus benchmark profile/toolchain; a user label is annotation, not authority to override a mismatch.

Portable internal roofline runners are primary where they can state exact semantics. `fio`, `perf`, Xcode/Instruments, flamegraph, DuckDB, and Polars are optional providers/cross-checks invoked as isolated subprocesses. External tools do not become CDF runtime dependencies.

## Alternatives considered

- Require one Linux benchmark host: rejected because it blocks local investigation and hides portability problems, though published baselines may still designate canonical Linux classes.
- Parse platform commands inside each workload: rejected because behavior and failure handling would proliferate.
- Treat missing tools as skipped tests: rejected because the envelope must show unavailable cells.
- Hash all machine facts including hostname/serial: rejected for privacy and because it prevents comparable ephemeral CI hosts.

## Consequences

L1 owns capability/report types and sanitized fingerprints. L3 owns provider implementations and validation. Host-specific modules remain behind one boundary; workload definitions stay portable. Envelope claims always name the observation method and never imply cold-cache/counter/profile evidence when unavailable.
