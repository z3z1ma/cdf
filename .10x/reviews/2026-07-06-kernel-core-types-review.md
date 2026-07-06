Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-05-kernel-core-types.md
Verdict: pass

# Kernel core types review

## Target

Review of the `firn-kernel` implementation in `crates/firn-kernel/**`, its ticket/evidence records, and the parent-observed `QUALITY.md` gate run.

## Assumptions tested

- Kernel public API must not depend on upper layers such as DataFusion, DuckDB, Python, network clients, CLI/project crates, or destination drivers.
- Resource and batch vocabulary must match the active specs, especially `ResourceStream`, `QueryableResource`, `Batch`, `SourcePosition`, `StateDelta`, `Receipt`, scope keys, metadata helpers, and error taxonomy.
- Artifact-facing values that need durable storage must have serde support.
- Tests must assert behavior rather than merely exercise constructors.
- Significant-change closure must use tool-backed evidence from `QUALITY.md`.

## Findings

No blocking findings remain.

Minor residual risk: full `cargo deny check` and `cargo vet` are not passable yet because repository policy is unratified, not because of a kernel defect. `cargo deny check advisories`, `cargo audit`, OSV, Gitleaks, Semgrep, and CodeQL passed with zero findings. Policy adoption is tracked by `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`.

Minor residual risk: CodeQL's Rust extractor reported diagnostic extraction warnings and macro limitations while still producing successful analysis with zero SARIF results. This is a tool limitation to keep in mind as source complexity grows.

## Evidence reviewed

- `.10x/evidence/2026-07-05-kernel-core-types.md`
- `.10x/evidence/2026-07-06-kernel-quality-gates.md`
- `crates/firn-kernel/src/lib.rs`
- `crates/firn-kernel/Cargo.toml`

Reviewed evidence maps acceptance criteria to:

- Direct dependency boundary: `arrow-array`, `arrow-schema`, `futures-core`, `serde`, and dev-only `serde_json`.
- Forbidden upper-layer textual scan: no matches in kernel source or manifest.
- Unit tests: 7 kernel tests covering metadata helpers, Arrow batch wrapping, serde round trips, required error taxonomy, display formatting, source-position version dispatch, and receipt negative coverage.
- Mutation testing: 38 kernel mutants, 21 caught, 17 unviable, zero missed after repair.
- Workspace gates: locked check/clippy/test/nextest/doc coverage passed as recorded in evidence.
- Security/supply-chain scans: advisory and scanner findings were zero where policy was configured or default scanner behavior was meaningful.

## Verdict

Pass. The kernel child ticket is safe to close. Remaining supply-chain policy work is separately owned and does not invalidate the kernel implementation.

## Residual risk

The kernel currently defines contracts and artifact shapes but does not prove downstream crates will use them correctly. That risk belongs to dependent child tickets and conformance work.
