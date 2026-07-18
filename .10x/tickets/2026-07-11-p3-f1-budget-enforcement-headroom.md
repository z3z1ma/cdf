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

## Evidence

- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli run_jobs_tests --locked -j 12` passed. Supports CLI parsing for `--memory-budget`, `--spill-budget`, suffix parsing, and existing `--jobs`/stats-profile behavior. Limit: focused CLI argument tests only.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli runtime_budget_tests --locked -j 12` passed. Supports resolver precedence, no default-ceiling rejection for explicit non-cgroup budgets, and cgroup authority enforcement. Limit: pure resolver tests, not OS-provider integration.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo check -p cdf-cli --locked -j 12` passed. Supports crate-level type integration for the shared service-construction path. Limit: no runtime execution scenario.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli --all-targets --locked -j 12 -- -D warnings` passed. Supports lint cleanliness for the CLI/test target set. Limit: not a throughput benchmark; this slice is not hot-path data processing.

## Review

- 2026-07-17 self-review: The slice adds knobs and centralizes command service construction without changing default budget values or hot data-plane behavior. It deliberately avoids hard-coded performance caps: cgroup remains an external authority, default policy remains a default, and operators can raise process/spill budgets explicitly.

## Retrospective

- Treat "default" and "authority" as different nouns. The former is policy; the latter is an external or configured ceiling. Collapsing them would create exactly the hidden cap the performance program is trying to avoid.
