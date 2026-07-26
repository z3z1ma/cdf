Status: active
Created: 2026-07-25
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-25-stabilization-steady-state-program.md
Depends-On: .10x/tickets/done/2026-07-25-p0-canonical-segment-memory-admission.md

# P0: staged writer memory headroom

## Scope

Guarantee forward progress for staged destination writers under a valid shared memory budget.
Reserve the compiled Parquet writer working set when the staged ingress session begins—before
source extraction can occupy the remaining ledger—and consume that authority throughout the
bounded writer window. Preserve explicit bulk-path writer/batch settings and fail before source
contact only when the complete compiled writer floor genuinely cannot fit.

## Non-goals

- No segment-size reduction, jobs cap, or required operator tuning.
- No global memory-budget increase.
- No source-, engine-, or project-runtime branch keyed to Parquet identity.
- No relaxation or unaccounted writer allocation.

## Acceptance criteria

- A staged Parquet session reserves `writers × bounded writer bytes` before extraction.
- The reservation is held for the session lifetime, released on abort/commit/drop, and is not
  charged again by each writer.
- Finalized/test-only writes retain independent per-writer admission.
- A lifecycle regression proves downstream writer progress while upstream uses the rest of the
  budget.
- The EC2 100 GiB / 2 GiB run passes the writer frontier without user tuning.
- Focused tests, full affected suites, strict Clippy, formatting, and EC2 stress evidence are green.

## References

- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/tickets/2026-07-11-p3-f3-stress-generators-laws.md`
- `.10x/tickets/done/2026-07-25-p0-canonical-segment-memory-admission.md`

## Assumptions

- Record-backed: the selected `PreparedBulkPath` freezes `writers` and `bytes_per_batch` before
  staged ingress begins.
- Record-backed: Parquet staged concurrency never exceeds the prepared writer count.
- Record-backed: `ActiveStagedIngress::begin` constructs the destination session before source
  extraction starts, making session admission the correct headroom boundary.

## Journal

- 2026-07-25: The repaired EC2 F3 run crossed canonical construction, then failed in 1.18 seconds
  because staged inputs left 12,830,145 bytes free while a Parquet writer required its compiled
  16,777,216-byte minimum. The writer reservation currently occurs only after its first input batch
  is opened, allowing upstream work to consume memory that the already-selected destination path
  requires for progress.
- 2026-07-25: Moved staged writer admission to `ParquetStagedIngressSession::new`, which runs before
  extraction. The session reserves the exact prepared `writers × writer working set`, retains that
  aggregate authority through every bounded object group, and passes it into the encoder so no
  writer asks the ledger for the same bytes again. Finalized/test-only encoding retains its
  independent input-then-writer admission path.
- 2026-07-25: Added an adversarial lifecycle with a 64 MiB ledger: the two-writer session reserves
  32 MiB, a simulated upstream consumer occupies every remaining byte, and staged Parquet encoding
  still acknowledges its segment. Abort plus upstream release settles the ledger to zero.

## Blockers

None.

## Evidence

- `cargo test -p cdf-dest-parquet --locked -j 12`: 39 passed, one intentional release benchmark
  ignored, zero failures. The full adapter suite includes staged commit/abort, grouped concurrency,
  duplicate replay, object-store and filesystem publication, and the new zero-free-byte progress
  lifecycle.
- `cargo clippy -p cdf-dest-parquet --all-targets --locked -j 12 -- -D warnings`: green.
- `cargo fmt --all -- --check` and `git diff --check`: green.
- EC2 100 GiB / 2 GiB closure rerun: pending this commit's deployment.

## Review

Fresh-hat local review passes. Writer memory remains destination-owned; generic orchestration sees
only the existing prepared bulk path and ingress trait. The reservation is derived from recorded
batch/writer settings rather than a new hard cap, is acquired before source contact, and is bounded
by the already-validated maximum writer concurrency. No source, engine, or project-runtime
Parquet branch was introduced.

## Retrospective

Backpressure cannot guarantee progress if downstream minimum working sets are discovered only after
upstream admission. A selected physical path must reserve its irreducible downstream floor at
session construction, while optional concurrency remains the tunable portion. This preserves both
constant memory and throughput without forcing operators to reverse-engineer pipeline headroom.
