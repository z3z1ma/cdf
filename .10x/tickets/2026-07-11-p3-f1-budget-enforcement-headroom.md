Status: active
Created: 2026-07-11
Updated: 2026-07-18
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
- 2026-07-18: Added a runtime-budget authority module in `cdf-cli` so command service construction and `cdf doctor` share one budget report instead of an inline `/sys/fs/cgroup/memory.max` read. The report records budget-value provenance (`cli`, `environment`, `default_policy`), the versioned headroom resolution, cgroup v2 `memory.max/current/peak/events` when available, and caveats when enforcement is unavailable.
- 2026-07-18: The doctor check now reports the resolved process budget, managed pool, spill budget, managed-memory snapshot, and cgroup enforcement status without conflating managed ledger bytes with process RSS or cgroup aggregate memory. Hosts without cgroup enforcement are an explicit `unsupported` check, not a false pass.
- 2026-07-18: Fixed a pre-existing default-resolution bug surfaced by the doctor smoke: on non-cgroup hosts the 4 GiB default policy was being treated as an external authority and shaved by the 80% cgroup safety margin to 3.2 GiB. Added `resolve_unenforced_memory_budget` so unenforced policy/requested budgets keep their exact value, while real cgroup authorities still reserve the external-authority margin for default budgets.
- 2026-07-18: Refreshed the retained EC2 benchmark host to the committed authority-report build and ran `cdf doctor --json` against the prepared TLC local workspace. The live host observation is intentionally negative for enforcement: this bare EC2 environment exposes no cgroup v2 `memory.*` files at `/sys/fs/cgroup`, so the product reports the budget as `unsupported`/not cgroup-enforced with read errors in provider details. This proves unavailable labeling on a release host, but does not satisfy the still-open cgroup-enforced acceptance criterion.
- 2026-07-18: Rechecked the EC2 host manually and found the provider was reading the cgroup filesystem root instead of the current process cgroup. Amazon Linux 2023 mounts cgroup v2 at `/sys/fs/cgroup`, but the memory controller files for the command live under the path named by `/proc/self/cgroup` (for example `/sys/fs/cgroup/user.slice/user-1000.slice/session-771.scope`). Fixed the provider to resolve and sanitize the current `0::` cgroup v2 path before reading `memory.max/current/peak/events`.

## Evidence

- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli run_jobs_tests --locked -j 12` passed. Supports CLI parsing for `--memory-budget`, `--spill-budget`, suffix parsing, and existing `--jobs`/stats-profile behavior. Limit: focused CLI argument tests only.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli runtime_budget_tests --locked -j 12` passed. Supports resolver precedence, no default-ceiling rejection for explicit non-cgroup budgets, and cgroup authority enforcement. Limit: pure resolver tests, not OS-provider integration.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo check -p cdf-cli --locked -j 12` passed. Supports crate-level type integration for the shared service-construction path. Limit: no runtime execution scenario.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli --all-targets --locked -j 12 -- -D warnings` passed. Supports lint cleanliness for the CLI/test target set. Limit: not a throughput benchmark; this slice is not hot-path data processing.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel human_byte_size --locked -j 12` passed. Supports the shared byte-size parser used by CLI/runtime knobs and DuckDB native-resource knobs. Limit: parser-only.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo test -p cdf-dest-duckdb native_resource_tests --locked -j 12` passed. Supports current-default preservation, explicit override removal of default DuckDB ceilings, spill reservation accounting for overridden temp budget, and invalid knob rejection. Limit: focused destination resource tests, not throughput evidence.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo check -p cdf-kernel -p cdf-cli -p cdf-dest-duckdb --locked -j 12` passed. Supports type integration across touched crates. Limit: does not run all workspace crates.
- 2026-07-17: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-cli -p cdf-dest-duckdb --all-targets --locked -j 12 -- -D warnings` passed. Supports strict lint cleanliness for touched crates. Limit: no live destination timing because the host was under swap pressure.
- 2026-07-18: `CARGO_BUILD_JOBS=12 cargo test -p cdf-memory --locked -j 12` passed. Supports exact budget-resolution semantics, including the new unenforced-policy resolver that preserves the 4 GiB default and existing external-authority margin behavior. Limit: crate-local tests; no OS cgroup scope is created.
- 2026-07-18: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli runtime_budget --locked -j 12` passed. Supports budget source precedence, cgroup ceiling rejection, fixture-backed cgroup `memory.max/current/peak/events` separation, and parser behavior. Limit: cgroup files are test fixtures, not a live kernel-enforced scope.
- 2026-07-18: `CARGO_BUILD_JOBS=12 cargo check -p cdf-memory -p cdf-cli --locked -j 12` passed. Supports type integration for the shared runtime-budget report and doctor wiring. Limit: touched crates only.
- 2026-07-18: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-memory -p cdf-cli --all-targets --locked -j 12 -- -D warnings` passed. Supports lint cleanliness across the touched crates. Limit: no performance benchmark; this slice does not alter data-plane operators.
- 2026-07-18: `target/debug/cdf --project examples/rest-fixture doctor --json > target/cdf-doctor-runtime-memory-smoke.json` followed by a JSON assertion passed. The `runtime_memory_budget` check reported `process budget 4 GiB; managed pool 3.4 GiB; spill budget 8 GiB; not cgroup-enforced`, with `process_budget_bytes = 4294967296`, `native_headroom_bytes = 644245094`, and the managed-memory snapshot budget matching `managed_pool_bytes`. Limit: macOS/portable smoke labels cgroup enforcement unavailable; it does not satisfy the Linux enforced-provider acceptance criterion.
- 2026-07-18: Source-shaped `gitleaks detect --no-git --redact` passed after copying `git ls-files --cached --modified --others --exclude-standard` into a temporary target scan directory. Supports CI-secret hygiene for the touched source/records. Limit: scan shape mirrors source, not ignored build artifacts.
- 2026-07-18: `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-doctor.json` records a release-host `cdf doctor --json` observation at commit `e39da00850eeb93a7d22d52cb30a945b70c32f9a`. The `runtime_memory_budget` check reports `process budget 4 GiB; managed pool 3.4 GiB; spill budget 8 GiB; not cgroup-enforced`, `enforcement = "unavailable"`, exact default process budget `4294967296`, managed pool `3650722202`, and cgroup read errors for `memory.max/current/peak/events`. Limit: doctor returned nonzero because unrelated workspace health checks failed; the memory check itself is present and parseable. The host is not a cgroup-enforced proof host yet.
- 2026-07-18: `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-revision.env` and `.10x/evidence/.storage/2026-07-18-p3-f1-ec2-runtime-memory-build.env` record the synced/build revision for the EC2 doctor observation. Limit: build markers prove source and binary identity, not performance.
- 2026-07-18: `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli runtime_budget --locked -j 12` passed after the process-cgroup fix. Supports fixture-backed resolution from `/proc/self/cgroup` to the scoped cgroup v2 directory, root-cgroup handling, path-traversal rejection, and cgroup `memory.max/current/peak/events` separation. Limit: fixture-backed local test, not live EC2.
- 2026-07-18: `CARGO_BUILD_JOBS=12 cargo check -p cdf-cli --locked -j 12` and `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli --all-targets --locked -j 12 -- -D warnings` passed after the process-cgroup fix. Limit: touched CLI crate only.
- 2026-07-18: Repeated the local `target/debug/cdf --project examples/rest-fixture doctor --json` smoke after the process-cgroup fix; the `runtime_memory_budget` check remained `unsupported` with exact 4 GiB process default and managed snapshot budget matching the resolved managed pool. Limit: macOS/portable host still cannot prove Linux cgroup enforcement.

## Review

- 2026-07-17 self-review: The slice adds knobs and centralizes command service construction without changing default budget values or hot data-plane behavior. It deliberately avoids hard-coded performance caps: cgroup remains an external authority, default policy remains a default, and operators can raise process/spill budgets explicitly.
- 2026-07-17 self-review: The DuckDB native-resource increment removes hidden fixed ceilings only when the operator opts in; default values are intentionally unchanged until a clean performance gate proves a faster default. The adapter owns the knobs and reserves temp budget through the existing spill coordinator, so no DuckDB-specific branch leaks into runtime orchestration.
- 2026-07-18 self-review: The authority-report slice is intentionally outside the hot data path: it changes service-construction budget resolution and doctor diagnostics only. It removes the remaining inline cgroup read from `commands.rs`, keeps Linux cgroup facts as provider data, and does not add source/destination-specific branches.
- 2026-07-18 self-review: The default-resolution change increases the non-cgroup default from the accidental 3.2 GiB process budget back to the documented 4 GiB policy. That is a hidden-cap removal, not a conservative correctness slowdown; real cgroup ceilings still apply, and explicit budgets above a real authority still fail before work.
- 2026-07-18 self-review: The corrected cgroup provider now follows the process-tree authority boundary rather than the filesystem root. The sanitization rejects `..` and non-v2 entries, so a malformed `/proc/self/cgroup` line cannot redirect reads outside the cgroup mount.

## Retrospective

- Treat "default" and "authority" as different nouns. The former is policy; the latter is an external or configured ceiling. Collapsing them would create exactly the hidden cap the performance program is trying to avoid.
- Host-pressure checks must precede long performance gates. A heavily swapped laptop can make a known-good 16-second local run look like a 100+ second regression; recording that as "performance evidence" would poison the ticket graph.
- Product diagnostics need the same metric taxonomy as the lab: managed-pool bytes, process RSS, and cgroup aggregate bytes are three different observations. The first useful product increment is to make incorrect conflation impossible in JSON before trying to enforce the law.
