Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p0-c5-property-fuzz-targets.md
Verdict: pass

# P0 C5 property/fuzz targets review

## Target

Review of the C5 implementation and evidence:

- `crates/cdf-conformance/src/property_fuzz/**`
- `crates/cdf-conformance/src/lib.rs`
- `crates/cdf-conformance/Cargo.toml`
- `Cargo.lock`
- `supply-chain/config.toml`
- `.10x/evidence/2026-07-08-p0-c5-property-fuzz-targets.md`

## Findings

No blocking findings.

`cargo vet` initially failed on the new `proptest` dependency graph. The fix added exact `safe-to-run` current-version exemptions for the nine new dev-test packages and then reran `cargo vet --locked` successfully. This preserves the repo's existing cargo-vet posture: current-version exemptions are backlog markers, not audit claims.

`osv-scanner` remains nonzero only for the already-ratified `paste 1.0.15` / `RUSTSEC-2024-0436` advisory. No new advisory was introduced by C5.

## Assumptions tested

- Contract scope: the property harness targets the active `ValidationProgram` row-disposition lattice over `RuleOutcome::ALL`; it does not invent Workstream-E row-level verdict semantics.
- Position breadth: tests cover every active `SourcePosition` variant and every active `CursorValue` variant, assert `CHECKPOINT_STATE_VERSION`, and round-trip through JSON values and strings.
- NDJSON parser behavior: malformed, mixed valid/invalid, truncated, non-object, invalid UTF-8, arbitrary byte, and oversized/strange scalar inputs either parse as a whole valid read or fail with data errors; invalid fixtures are not partially accepted.
- Singer/Airbyte behavior: malformed protocol messages and truncated streams error; unknown message types are preserved as `Other`; unknown fields are retained in raw messages; foreign-state payloads round-trip into `SourcePosition::ForeignState`.
- Dependency hygiene: new dev-dependencies are used by `cdf-conformance`; scoped machete passes for the touched crate.

## Residual risk

The harness uses bounded property tests rather than coverage-guided native fuzzing: 64 arbitrary-byte NDJSON cases and 32 arbitrary-byte protocol cases per run. That is acceptable for C5 because native fuzz targets were optional and the ticket required quality-cadence wiring plus adversarial coverage, not a fuzz corpus.

CodeQL was not rerun to avoid recreating the expensive Rust database for a test-only conformance harness. This is acceptable for C5 given the passing Semgrep, Gitleaks, direct unsafe/FFI search, cargo-audit, cargo-deny, cargo-vet, OSV, and conformance test evidence.

## Verdict

Pass. C5 has implementation, evidence, supply-chain remediation for its new dev-test graph, and review sufficient to close. Workstream C remains open only for C6 aggregate closure.
