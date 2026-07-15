Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# Kernel execution-extent and watermark artifacts

## Observation

CDF now has one versioned kernel-owned execution-lifetime artifact. Bounded plans carry the current v1 extent; drain plans cannot exist without complete cadence, rotation, watermark/late-data, safe-frontier, and termination policy; drain and resident policy are serializable for planning but rejected before source contact until their executors exist. Typed watermark claims replace the former free-form name/position pair. Canonical epoch-frontier shape excludes wall-clock trigger observations and dimensionally checked overshoot, which live in a separate closure-evidence type that A7 must route through an explicitly nonidentity channel.

The engine no longer defines `PlanBoundedness`, `UnboundedDrain`, or `UnboundedLive`. Planner, preview, and package execution validate the kernel extent. Current artifact readers reject the old missing-version shape and unsupported versions under the active pre-production current-format-only decision.

## Procedure

1. Ran `CARGO_BUILD_JOBS=12 cargo check -p cdf-kernel -p cdf-engine -p cdf-project -p cdf-cli -p cdf-conformance -p cdf-benchmarks --all-targets --locked -j 12`; the affected graph passed.
2. Ran strict no-dependency Clippy over the same affected crates with `--all-targets --locked -j 12 --no-deps -- -D warnings`; it passed.
3. Ran `CARGO_BUILD_JOBS=12 cargo test -p cdf-kernel --lib --locked -j 12`; all 45 tests passed, including seven execution-extent/serialization laws.
4. Ran exact engine tests for resident planner rejection, resident execution rejection, drain execution rejection, divergent top-level/explain authority rejection before source contact/package mutation, the workspace-wide engine ownership gate, and required `execution_extent` deserialization; all passed.
5. Ran `cargo tree -p cdf-kernel --locked --edges normal`; the normal dependency graph contains Arrow value crates, serialization, hashing, and `futures-core`, with no DataFusion, Tokio, source, engine, project, or CLI dependency.
6. Ran `cargo fmt --all -- --check`, `git diff --check`, and focused Gitleaks over `crates/cdf-kernel`; all passed.
7. Ran the full engine library suite as a broad diagnostic: 125 tests passed, six were ignored, and two existing non-BX1 tests failed. The failures are the runtime-ownership static gate's known REST/subprocess `futures_executor::block_on` findings and the standalone package-rechunking identity law; neither failure exercises execution extent.

## What it supports or challenges

- Supports the BX1 acceptance criteria that semantic ownership lives in `cdf-kernel`, every unbounded artifact is structurally complete or rejected precisely, and engine/project/product consumers share the same artifact.
- Supports the explicit artifact-format decision: one current v1 shape is tested; no compatibility reader or migration shim remains.
- Supports safe P3 sequencing: A7 can compile source capabilities into these artifacts, A8 can execute finite epochs, and A9 can add aggregation/late-data conformance without inventing another engine-local policy vocabulary.
- Challenges any claim that the broad engine suite is fully green. Its two existing failures remain independently owned and are not hidden by this evidence.

## Limits

- BX1 defines and validates artifacts; it does not compile source declarations into drain policies, execute epochs, aggregate live watermarks, or supervise resident streams. A7, A8, A9, and the later supervisor own those behaviors.
- No performance claim is made. These are small control artifacts outside the payload hot path.
- The adversarial serialization review is recorded in the owning ticket rather than duplicated here.
