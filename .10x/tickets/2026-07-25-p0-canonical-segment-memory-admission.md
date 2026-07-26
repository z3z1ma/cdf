Status: active
Created: 2026-07-25
Updated: 2026-07-25
Parent: .10x/tickets/2026-07-25-stabilization-steady-state-program.md

# P0: canonical segment memory admission

## Scope

Repair canonical segment construction accounting so a legal segment can make forward progress
under the shared memory budget without requiring operator tuning. Remove the stale duplicate
retained-input charge introduced before fused-transform leases owned canonical inputs, preserve
bounded concat/ordinal scratch accounting, and prove the complete source-to-package lifecycle
under a budget that admits exactly the real simultaneous working set.

## Non-goals

- No canonical segmentation identity/default change.
- No source- or destination-specific branch.
- No hard-coded jobs cap or user-required tuning.
- No relaxation of retained Arrow, transform, encode, or package memory accounting.

## Acceptance criteria

- Accounted canonical inputs are not charged again as newly allocated concat scratch.
- Concat output and package-row ordinal allocation remain reserved before construction.
- The regression lifecycle fails under the former duplicate charge and completes under a budget
  that admits the actual source, transform, concat, and ordinal working set.
- Package identity/verification and zero-current-memory settlement remain intact.
- The EC2 100 GiB / 2 GiB F3 run crosses the first-segment frontier and completes, or any distinct
  residual receives its own bounded owner.
- Focused tests, strict Clippy, formatting, and a performance-sensitive smoke remain green.

## References

- `.10x/specs/runtime-memory-backpressure.md`
- `.10x/specs/constant-memory-proof.md`
- `.10x/tickets/2026-07-11-p3-f3-stress-generators-laws.md`
- `.10x/tickets/done/2026-07-11-p3-a5b-fused-transform-kernel.md`
- `.10x/tickets/done/2026-07-21-p0-segmentation-v3.md`

## Assumptions

- Record-backed: fused-transform output leases are reconciled to normalized retained output bytes
  and travel with their batches through canonical assembly until durable segment persistence.
- Record-backed: source/frontier payload leases remain independent and already own source-visible
  input allocations while a batch is processed.
- Record-backed: canonicalization may allocate one retained-output-sized concat result plus the
  package-row `UInt64` vector; those new allocations still require admission.

## Journal

- 2026-07-25: F3's first enforced EC2 100 GiB / 2 GiB execution failed before the first segment
  encoded. The engine requested 528,321,296 new bytes for a roughly 256 MiB segment while already
  retaining the fused-transform leases that owned its input. History tracing showed the `2x`
  input-plus-output reservation predates commit `5029ba3a`, which introduced those traveling
  transform leases; the old charge was never reconciled to the new ownership model.
- 2026-07-25: Replaced the coarse duplicate charge with explicit ownership accounting. Canonical
  assembly now records the exact retained bytes that arrived without a traveling lease, including
  mixed owned/unowned segments. Construction reserves concat output, package-row ordinals, and
  only that unaccounted input; all preexisting transform leases remain live through persistence.
- 2026-07-25: Added a deterministic admission-boundary regression and a complete 700,000-row
  source-to-verified-package lifecycle. The fixture proves the former charge exceeds the admitted
  budget by exactly one retained input batch while the real simultaneous working set completes and
  settles the shared ledger to zero.

## Blockers

None.

## Evidence

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-engine --locked -j 12`: 199 passed, 7 ignored, zero
  failures; includes exact mixed-ownership accounting, the former-charge boundary, package
  verification, and zero-current-memory settlement.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-engine --all-targets --locked -j 12 -- -D warnings`:
  green.
- `cargo fmt --all -- --check` and `git diff --check`: green.
- EC2 100 GiB / 2 GiB closure run: pending this commit's deployment.

## Review

Fresh-hat local review passes the accounting boundary: the implementation neither infers ownership
from a segment-wide boolean nor weakens admission. The exact per-chunk unaccounted-byte total is
private to the engine, resets atomically at flush, and is consumed only when reserving construction
allocations. Final closure remains gated on the EC2 stress rerun.

## Retrospective

The regression was created by composing two individually reasonable mechanisms across time: an old
conservative `2x` construction estimate and newer traveling transform leases. Memory policy must
describe allocation ownership at every handoff; retained size alone is not the amount newly
allocated at the next stage. Boundary tests should therefore assert both the admitted real working
set and the rejected superseded estimate.
