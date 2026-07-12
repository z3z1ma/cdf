Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: .10x/tickets/2026-07-11-p0-dx4-conformance-extension-law.md
Verdict: concerns

# DX4 conformance catalog milestone review

## Target

The first destination-catalog migration tranche in `cdf-conformance`.

## Findings

- No critical or significant correctness finding: resolution flows through driver-owned registry authority with current policy and execution-service inputs; no concrete constructor was restored.
- Minor: the evidence/footprint assertions remain represented by destination-specific fixture handles. DX4 explicitly remains open for the complete data-driven assertion catalog and fourth-driver law.
- Minor: the static import allowlist still admits the project test-only constructors and CLI adapter diagnostics. The production boundary is protected, while `.10x/tickets/2026-07-11-p0-remove-preproduction-compatibility-vestiges.md` owns deletion of compatibility/test helpers.
- Residual: cross-destination live execution cannot yet validate semantic parity because SX1 has not migrated the live fixture to typed source resolution.

## Verdict

Concerns raised, suitable as a committed milestone. Do not close DX4 until the named residuals are proven.

## Residual risk

Postgres conformance now uses the current registry policy (`merge_dedup = fail`) instead of the removed direct adapter's `Last` setting. Fixtures must demonstrate current semantics once SX1 unblocks execution; old fallback semantics must not be restored.
