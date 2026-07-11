Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/done/2026-07-10-p3-ws-l3-macro-roofline-runners.md
Verdict: pass

# P3 L3 fairness and architecture review

## Target

Host providers, macro runner, worker CLI, raw/external references, profiling plans, report validation, tests, and evidence.

## Findings

The first review found five significant fairness/architecture defects and all were corrected before closure:

- cold eviction originally happened once for N samples, making samples two through N warm; eviction now occurs before every cold sample and warm mode has an untimed prime;
- parent process wall time included worker startup/JSON parsing; workers now report the exact timed region while the parent remains timeout/CPU/RSS authority;
- process observation method was not serialized; every macro observation now carries the provider method/version and whether CPU/RSS are authoritative;
- child stdout could grow without a metadata bound; a concurrent draining reader retains at most 1 MiB and fails oversized measurement output;
- profiler detection targeted the Cargo subcommand binary, whose direct `--version` fails; it now uses the standalone `flamegraph` executable and records its real version/command.

The review also added command-environment credential rejection and explicit reference bias enforcement. Platform branching is confined to `SystemHostProvider`; workload and report code depend only on `HostCapabilityProvider`. Linux cgroup facts overlay advertised capacity, macOS/Linux time formats have distinct unit parsers, and portable fallback never invents CPU/RSS/cold-cache support. Raw references state only their performed work; Polars is subprocess-isolated and absent from the Cargo graph.

No critical, significant, or minor unresolved finding remains.

## Verdict

Pass.

## Residual risk

Device medium detection remains `unknown` when only filesystem class is authoritative; roofline observations themselves bind the exact target bytes and host class, so this prevents overclaiming rather than weakening comparison. Privileged Linux/macOS cold eviction and Linux cgroup/RSS behavior still need live host evidence in L5. External Polars APIs may change across versions, but version identity is mandatory and failures remain visible cells.
