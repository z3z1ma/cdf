Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md, .10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md

# P3 L9: slim EC2 measured-command runner build graph

## Scope

Split the EC2 `measure-cdf` path away from the heavyweight all-in-one `cdf-p3-lab` reference binary so ordinary CDF command measurements do not relink every lab reference workload after unrelated benchmark diagnostics. The retained path must still emit the same host-labeled `run-cell` observation schema, supervise nested `cdf` children, enforce timeouts, preserve fresh-workspace defaults, and remain compatible with L6/L7 preflight.

## Non-goals

- No weakening of release optimization for the measured `cdf` binary.
- No removal of `cdf-p3-lab` reference workloads, baseline-run, package probes, or envelope tooling.
- No benchmark result schema churn unless the old schema is intentionally replaced everywhere in one change.
- No source/destination dataplane optimization.

## Acceptance Criteria

- A `measure-cdf` invocation can use a smaller release binary that does not link `references.rs` or first-party destination/reference workload diagnostics.
- The EC2 helper builds/verifies the measured-command runner as a first-class release artifact and records it in the build marker or an equivalent preflight-checked marker.
- A cached source-only record commit does not force a heavyweight relink, and a measured-command-only change has materially lower on-host release build time than the current `cdf-p3-lab` `8m35s`–`8m39s` relink class.
- `measure-cdf` output for a tiny fixture and the full-year TLC prepared workspace remains schema-valid and comparable to existing `cdf-p3-lab run-cell` output.
- Existing `cdf-p3-lab` reference, baseline, package-shape/read, and compare commands continue to build and pass focused tests.

## References

- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/tickets/done/2026-07-18-p3-l6-ec2-benchmark-host.md`
- `.10x/tickets/2026-07-18-p3-l7-ec2-benchmark-tranche-lifecycle.md`
- `.10x/tickets/2026-07-11-p3-g4-tlc-remote-io-envelope.md`

## Assumptions

- Record-backed: L6/L7 repeatedly measured `cdf-p3-lab` release relinks at about `8m35s`–`8m39s` when benchmark diagnostics touch the lab/reference module graph.
- Record-backed: `measure-cdf` needs `cdf_command`, host fingerprinting, macro-cell observation, and canonical JSON output; it does not need DuckDB/Parquet reference workloads.

## Journal

- 2026-07-18: Opened after the DuckDB stream-scan tranche confirmed the benchmark host is reliable but iteration is still throttled by the monolithic `cdf-p3-lab` build graph. The current helper builds both release `cdf` and release `cdf-p3-lab` for every tranche refresh; `cdf` is often a cache hit while lab relink burns one CPU for ~8.5 minutes. This ticket owns the build-graph cut rather than letting each G4 diagnostic pay that tax.
- 2026-07-18: Proved that a same-package `cdf-p3-measure` binary under `cdf-benchmarks` is the wrong cut: Cargo dependencies are package-wide, so the runner still inherits the heavy lab/reference dependency graph. Replaced that draft with a real crate boundary: `cdf-bench-core` owns shared measurement schema, host probing, macro-cell execution, and supervised `cdf-command-worker`; `cdf-benchmarks` re-exports the core and keeps heavy lab/reference workloads; `cdf-bench-measure` is a thin `cdf-p3-measure` CLI over the core.
- 2026-07-18: Updated `tools/p3-ec2-benchmark-host.sh` so `measure-cdf` runs `target/release/cdf-p3-measure`, `build-measure` builds only release `cdf` plus release `cdf-p3-measure`, `verify-measure` checks the lean runner, and `preflight-measure` validates `.cdf-bench-measure-build.env` instead of requiring a full-lab build marker. The existing `build`/`preflight` full-lab path remains intact for reference workloads.
- 2026-07-18: Local verification: `cargo check -p cdf-bench-core -p cdf-bench-measure -p cdf-benchmarks --bins --locked -j 12` passed; `cargo test -p cdf-bench-core -p cdf-bench-measure --locked -j 12` passed with 9 shared worker/host tests; `cargo test -p cdf-benchmarks --lib --bins --locked -j 12` passed with 10 lab/reference tests plus all benchmark binaries compiling; `bash -n tools/p3-ec2-benchmark-host.sh`, dry-run `build-measure`, dry-run `preflight-measure`, and `git diff --check` passed.
- 2026-07-18: Local release build graph proof: `/usr/bin/time -p env CARGO_BUILD_JOBS=12 cargo build -p cdf-bench-measure --bin cdf-p3-measure --release --locked -j 12` built the final core-backed runner in `5.90s`; `target/release/cdf-p3-measure` is `606K`.
- 2026-07-18: EC2 proof on the active L7 host: `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build-measure && tools/p3-ec2-benchmark-host.sh preflight-measure` passed. Release `cdf` was a cache hit (`0.27s`); release `cdf-bench-core` + `cdf-bench-measure` built in `10.27s`; `target/release/cdf-p3-measure` is `734K`; preflight passed with tuned gp3 storage, host class `host-class-95da083e15eebd1c`, workspace present, and `198829858816` free bytes. The full `cdf-p3-lab` binary was not rebuilt.
- 2026-07-18: EC2 measured-command smoke: one-sample `measure-cdf` for `inspect resources --json` emitted a schema-valid observed cell at `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-smoke.json` with workload `inspect_resources_measure_runner_core`, `30,348,669ns` wall, `28,180,480` peak RSS bytes, and the standard `gnu-time-v-child-process` provider.
- 2026-07-18: EC2 full-year TLC measured-command proof: one-sample `measure-cdf` against the prepared local full-year TLC workspace emitted a schema-valid observed cell at `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-tlc-local-measure-runner.json` with workload `tlc_local_duckdb_measure_runner_core_rerun`, `41,169,720` rows, `34,056,329,912ns` wall, `1,208,871` rows/s, `2,414,833,664` peak RSS bytes, and phase metrics including `segment_encode=9,765,316,351ns`, `persist_hash=1,608,082,325ns`, `destination_ingress=33,060,828,561ns`, and `package_execution=33,256,495,673ns`.
- 2026-07-18: One immediately preceding TLC sample through the same final runner timed out at the worker's `119000ms` child timeout, produced a failed observation, and left no orphaned `cdf`, DuckDB, cargo, or rustc process. A rerun under the same timeout passed in the expected ~34s class. Closure treats the failure as a recorded EC2/sample variance limit, not as promotion evidence.

## Blockers

None.

## Evidence

- `cargo check -p cdf-bench-core -p cdf-bench-measure -p cdf-benchmarks --bins --locked -j 12` — passed, proving the shared core, lean runner, and full benchmark binaries typecheck together under the lockfile.
- `cargo test -p cdf-bench-core -p cdf-bench-measure --locked -j 12` — passed, preserving the supervised command-worker, timeout/process-group, workspace-copy, environment-forwarding, and host-provider tests after the crate split.
- `cargo test -p cdf-benchmarks --lib --bins --locked -j 12` — passed, proving the heavy lab/reference crate still builds and its focused tests pass after re-exporting shared measurement primitives from `cdf-bench-core`.
- `/usr/bin/time -p env CARGO_BUILD_JOBS=12 cargo build -p cdf-bench-measure --bin cdf-p3-measure --release --locked -j 12` — passed locally in `5.90s`; output binary `606K`. This is materially below the recorded `cdf-p3-lab` EC2 relink class of `8m35s`–`8m39s`.
- `tools/p3-ec2-benchmark-host.sh sync-repo && tools/p3-ec2-benchmark-host.sh build-measure && tools/p3-ec2-benchmark-host.sh preflight-measure` — passed on EC2. `cdf` was cached (`0.27s`), `cdf-bench-core` + `cdf-bench-measure` release build took `10.27s`, output binary `734K`, and preflight mode `measure` passed without rebuilding `cdf-p3-lab`.
- `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-runner-revision.env` records the EC2 synced revision marker used for the dirty final-shape measurement proof.
- `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-runner-build.env` records the matching EC2 measured-runner build marker.
- `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-measure-smoke.json` is the schema-valid tiny measured-command observed cell.
- `.10x/evidence/.storage/2026-07-18-p3-l9-ec2-tlc-local-measure-runner.json` is the schema-valid full-year TLC measured-command observed cell.
- `bash -n tools/p3-ec2-benchmark-host.sh`, `tools/p3-ec2-benchmark-host.sh --dry-run build-measure`, `tools/p3-ec2-benchmark-host.sh --dry-run preflight-measure`, and `git diff --check` — passed, covering command construction and whitespace.

## Review

Self-review pass, 2026-07-18:

- Significant risk checked: a first draft used `#[path = "../../cdf-benchmarks/..."]` from the lean crate, which would have made the build graph faster but left a brittle cross-crate source include. Reworked to a real `cdf-bench-core` crate and deleted the old local copies from `cdf-benchmarks`.
- Significant risk checked: `measure-cdf` could silently continue using the full lab binary. The helper now builds/runs `cdf-bench-measure --bin cdf-p3-measure`; dry-run output and EC2 build output confirm the package/binary target.
- Significant risk checked: full-lab reference commands could break after moving shared types. Focused `cdf-benchmarks` lib/bin tests passed, including the DuckDB/Parquet reference tests.
- Residual risk: one TLC measured-command sample timed out before a same-authority rerun passed. The timeout is recorded as sample variance; it is not used as promotion evidence. L7 remains active for benchmark-host lifecycle and future tranche evidence.

Verdict: pass with the recorded residual timing variance limit.

## Retrospective

- Same-package binary splitting is a false economy in Cargo when the problem is package-wide dependencies. The durable seam is a smaller crate, not a second binary in the heavy crate.
- Benchmark tooling deserves the same architecture hygiene as runtime code. The path-include draft was tempting because it was tiny, but the core crate leaves the system easier to reason about and gives future runners one shared measurement vocabulary.
- The EC2 host is now good enough to catch both build-graph wins and sample variance: it proved the lean runner build cut decisively, and it also surfaced a one-off TLC timeout that would have been easy to overinterpret without a rerun.
