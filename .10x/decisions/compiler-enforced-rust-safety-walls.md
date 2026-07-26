Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Compiler-enforced Rust safety walls

## Context

CDF's unsafe-code and hot-path panic policies are currently review conventions. Source inspection
found unsafe production code only in `cdf-dest-duckdb`, `cdf-python`, and `cdf-subprocess`, plus a
benchmark reference module. These are genuine FFI/process-boundary owners. No workspace lint
inheritance exists, and production source in foundational crates still contains unchecked
`unwrap`/`expect` sites mixed with many valid test assertions.

The intended boundary is already stable enough to enforce: safe crates must not acquire unsafe
code casually, while the few FFI owners need narrow, reviewed exceptions.

## Decision

Add workspace lint policy and make every workspace crate opt into it deliberately.

- `unsafe_code` is forbidden in every production crate except the named FFI owners
  `cdf-dest-duckdb`, `cdf-python`, and `cdf-subprocess`. `cdf-benchmarks` may allow unsafe only in
  reference-measurement modules that document the foreign boundary.
- Each exception crate MUST deny unsafe by default at module scope and allow it only in the
  smallest named FFI module. Every unsafe block/function/impl MUST carry a safety comment and an
  active decision or contract reference. Existing release-counter/fuzz/conformance evidence
  remains mandatory where memory ownership crosses the boundary.
- `clippy::unwrap_used` and `clippy::expect_used` are denied in non-test production code for
  kernel, memory, runtime, package, package-contract, engine, task-store, object-access, and
  source/destination extension contracts. Test modules may use explicit local allowances.
- Workspace warnings and selected correctness lints are centralized without turning stylistic
  preference lints into a repository-wide migration.
- CI runs the same lint policy as local quality commands. A crate cannot silently decline the
  policy; an exception is explicit in its manifest/root and named by architecture tests.

## Alternatives considered

- Continue review-only enforcement: rejected because the incoming adapter wave multiplies the
  chance of an accidental unsafe or panic-prone hot path.
- Forbid unsafe at the workspace level with no exceptions: impossible for the measured DuckDB,
  Python Arrow, and subprocess boundary implementations.
- Deny unwraps in all tests: rejected because it creates large mechanical churn without improving
  production failure behavior.
- Enable every Clippy pedantic lint: rejected because noise would obscure the small set of
  architecture and correctness walls this decision exists to enforce.

## Consequences

Existing FFI code is not rewritten merely to satisfy the wall; it is isolated and documented.
The first lint ticket includes the finite production unwrap audit required to turn the policy on,
not a repository-wide formatting crusade. New crates inherit a safe default and must make any
exception visible during review.
