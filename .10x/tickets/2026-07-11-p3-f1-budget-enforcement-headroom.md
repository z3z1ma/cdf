Status: active
Created: 2026-07-11
Updated: 2026-07-17
Parent: .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md, .10x/tickets/done/2026-07-11-p3-a2-unified-memory-ledger.md, .10x/specs/constant-memory-proof.md

# P3 F1: process-tree enforcement and headroom calibration

## Scope

Implement portable process-tree memory observation, Linux cgroup v2 enforced provider, child-budget propagation, host/runtime/native headroom calibration, exact default/effective budget resolution, and doctor/explain reporting.

## Acceptance criteria

- At least one release host enforces/records cgroup plus process-tree RSS/managed peaks without conflating metrics.
- Exact headroom/default resolution is evidence-backed/versioned; unsafe/small budgets fail before work.
- Python/subprocess children remain inside aggregate authority or enforced sub-budgets.
- Doctor/run JSON and human output report all authorities/caveats accurately.

## Evidence expectations

Host/provider fixtures, calibration reports, cgroup OOM/event tests, child memory cases, cross-platform unavailable labels, redaction, and adversarial metric review.

## Explicit exclusions

No data-plane materialization removal or allocator adoption without separate evidence.

## Blockers

Depends on L5 and A2.

## References

- `.10x/decisions/process-tree-constant-memory-proof.md`
- `.10x/research/2026-07-11-constant-memory-proof-audit.md`
- `.10x/specs/constant-memory-proof.md`

## Journal

- 2026-07-17: Began the operator-budget-control slice after the G4 remote Parquet work surfaced a recurring performance invariant: memory/disk safety defaults must be tunable knobs, not hidden hard caps. Added global CLI/environment budget surfaces in `cdf-cli`: `--memory-budget`, `--spill-budget`, `CDF_MEMORY_BUDGET`, and `CDF_SPILL_BUDGET`. Values accept integer byte sizes with binary suffixes (`B`, `KiB`, `MiB`, `GiB`, `TiB` and short forms). CLI values override environment values.
- 2026-07-17: Wired all command execution-service construction through one resolver so commands share the same memory/spill authority. The resolver keeps cgroup limits as real ceilings when present, but does not treat the 4 GiB default policy as a machine authority on non-cgroup hosts; explicit operator budgets above the default are accepted when no stronger authority is available.
- 2026-07-17: Found a destination-local native-resource smell while triaging the remaining G4 local floor: DuckDB used fixed default ceilings for native memory, temp directory bytes, and internal threads. Kept current defaults unchanged for non-regression, but converted the ceilings into destination-owned explicit knobs: `CDF_DUCKDB_MEMORY_LIMIT`, `CDF_DUCKDB_TEMP_BUDGET`, and `CDF_DUCKDB_THREADS`. The runtime remains destination-neutral; DuckDB parses and applies its own native resources behind its adapter boundary.
- 2026-07-17: Attempted a local 12-file TLC timing comparison after rebuilding release, but invalidated the measurement: host swap was already high (`vm.swapusage` reported 6.4 GiB used), and the default run stalled at 16 segment files with only `real 115.95` before interruption. This is recorded as an invalid host-pressure observation, not performance evidence for or against the budget knobs.

## Evidence

- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli run_jobs_tests --locked -j 12` passed. Supports CLI parsing for `--memory-budget`, `--spill-budget`, suffix parsing, and existing `--jobs`/stats-profile behavior. Limit: focused CLI argument tests only.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli runtime_budget_tests --locked -j 12` passed. Supports resolver precedence, no default-ceiling rejection for explicit non-cgroup budgets, and cgroup authority enforcement. Limit: pure resolver tests, not OS-provider integration.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo check -p cdf-cli --locked -j 12` passed. Supports crate-level type integration for the shared service-construction path. Limit: no runtime execution scenario.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli --all-targets --locked -j 12 -- -D warnings` passed. Supports lint cleanliness for the CLI/test target set. Limit: not a throughput benchmark; this slice is not hot-path data processing.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel human_byte_size --locked -j 12` passed. Supports the shared byte-size parser used by CLI/runtime knobs and DuckDB native-resource knobs. Limit: parser-only.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb native_resource_tests --locked -j 12` passed. Supports current-default preservation, explicit override removal of default DuckDB ceilings, spill reservation accounting for overridden temp budget, and invalid knob rejection. Limit: focused destination resource tests, not throughput evidence.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo check -p cdf-kernel -p cdf-cli -p cdf-dest-duckdb --locked -j 12` passed. Supports type integration across touched crates. Limit: does not run all workspace crates.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-cli -p cdf-dest-duckdb --all-targets --locked -j 12 -- -D warnings` passed. Supports strict lint cleanliness for touched crates. Limit: no live destination timing because the host was under swap pressure.

## Review

- 2026-07-17 self-review: The slice adds knobs and centralizes command service construction without changing default budget values or hot data-plane behavior. It deliberately avoids hard-coded performance caps: cgroup remains an external authority, default policy remains a default, and operators can raise process/spill budgets explicitly.
- 2026-07-17 self-review: The DuckDB native-resource increment removes hidden fixed ceilings only when the operator opts in; default values are intentionally unchanged until a clean performance gate proves a faster default. The adapter owns the knobs and reserves temp budget through the existing spill coordinator, so no DuckDB-specific branch leaks into runtime orchestration.

## Retrospective

- Treat "default" and "authority" as different nouns. The former is policy; the latter is an external or configured ceiling. Collapsing them would create exactly the hidden cap the performance program is trying to avoid.
- Host-pressure checks must precede long performance gates. A heavily swapped laptop can make a known-good 16-second local run look like a 100+ second regression; recording that as "performance evidence" would poison the ticket graph.
