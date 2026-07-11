Status: done
Created: 2026-07-11
Updated: 2026-07-11

# Destination extension touch-surface audit

## Question

What must change today to add a fourth first-party destination, and does that satisfy the active extension invariant before P3 amplifies runtime/bulk behavior?

## Sources and methods

Read the active extension invariant and prior registry decision; searched concrete destination imports, constructors, URI matches, registry use, lock generation, CLI doctor/replay, Cargo edges, and conformance factories across `cdf-project`, `cdf-cli`, and `cdf-conformance`. Inspected the registry/runtime traits, builtin registration, concrete adapters, lockfile sheet function, CLI destination runtime model, and runtime prelude.

## Findings

The settlement skeleton is object-safe and generic after resolution. That part is sound: replay writes segments through `CommitSession`, receipt verification and checkpoint gating do not match destination names.

The extension boundary remains cross-cutting before that point. Production code directly names destination crates in `cdf-project` runtime prelude and adapter modules, `cdf-project` lockfile generation, CLI context/doctor/replay, and one legacy orchestration convenience. `cdf-project`, `cdf-cli`, and `cdf-conformance` all carry concrete destination Cargo edges. `with_builtin_drivers` and `ResolvedProjectDestination` constructors embed the first three destinations. Lock generation independently re-parses the same schemes rather than asking the driver for sheet authority.

Adding a destination therefore requires edits to shared runtime/product code in addition to its adapter and conformance fixture. P3 bulk throughput, staging, streaming-ingress, and memory-ledger declarations would otherwise add more concrete branches to these same points.

The correct dependency inversion cannot be achieved by another helper in `cdf-project`: destination crates cannot implement a trait from `cdf-project` without depending upward on the product layer while `cdf-project` already depends downward on them. The shared object-safe boundary must move to a lower destination-neutral runtime crate.

## Conclusion

The prior registry was a necessary P0 step but not the final extension architecture. `.10x/decisions/destination-runtime-composition-boundary.md` supersedes its built-in permission. The P0 follow-up graph owns extraction, adapter migration, generic product surfaces, and conformance/build-graph enforcement before P3 WS-A/WS-D integrate streaming and bulk behavior.
