Status: done
Created: 2026-07-05
Updated: 2026-07-05
Parent: .10x/tickets/2026-07-05-implement-firn-system.md
Depends-On: None

# Bootstrap Rust workspace and crate skeleton

## Scope

Create the Rust workspace, root metadata, baseline lint/test configuration, and crate directories needed by the active architecture spec. Owns root `Cargo.toml`, repository-level Rust config, initial crate manifests, and minimal compile-only crate entry points.

## Acceptance criteria

- Workspace contains the crate map required by `.10x/specs/architecture-layering-runtime.md`.
- Kernel crate has no DataFusion, DuckDB, Python, network, project, or CLI dependencies.
- Optional/post-MVP crates can compile as placeholders only when explicitly feature-gated or empty, with TODOs avoided unless backed by child tickets.
- `cargo check --workspace` succeeds.

## Evidence expectations

Record `cargo check --workspace` output and a dependency graph check or equivalent evidence that the kernel boundary is clean.

## Explicit exclusions

No behavioral implementation beyond minimal compile scaffolding.

## Progress and notes

- 2026-07-05: Opened from book and specs.
- 2026-07-05: Assigned to worker subagent `019f34b1-4b1e-7572-ae10-908fadf28807` for minimal workspace scaffolding.
- 2026-07-05: Worker 1 created the root Cargo workspace, required compile-only crate skeletons, and `Cargo.lock`; `cargo check --workspace` and kernel dependency cleanliness are recorded in `.10x/evidence/2026-07-05-bootstrap-rust-workspace-check.md`.
- 2026-07-05: Parent review passed in `.10x/reviews/2026-07-05-bootstrap-rust-workspace-review.md`; ticket closed.

## Blockers

None.
