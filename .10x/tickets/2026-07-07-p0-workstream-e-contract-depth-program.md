Status: active
Created: 2026-07-07
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-07-p0-structural-debt-program.md

# P0 Workstream E: Activate contract depth

## Scope

Open and execute the P1 contract-depth program so contract-governed movement becomes live-path behavior: row verdicts, quarantine routing, dedup, variant capture, trust-ring ledger events, and drift-quarantine conformance.

Owns the P1 parent and ordered child-ticket graph. Child tickets should own code by crate boundary, primarily:

- `crates/cdf-contract/**`
- `crates/cdf-engine/**`
- `crates/cdf-package/**`
- `crates/cdf-project/**` where live routing and ledger events require it
- destination mirror support where sheets support quarantine
- `crates/cdf-conformance/**`
- focused CLI contract command wiring only when lower-layer behavior exists

## Required outcome

Open a P1 contract-depth program with child tickets in this order:

1. Row-level verdicts in the live operator chain: nullability, domain/enum, range, regex, freshness; vectorized; compiled program serialized into packages.
2. Quarantine routing: rejected rows flow to `quarantine/part-*.parquet` with rule id, error code, source position, and redaction by `pii:*`; destination `_cdf_quarantine` mirror where sheets support it.
3. Dedup: `keys`, `keep = first|last|fail` applied pre-merge so merges are deterministic under redelivery.
4. Variant capture end to end: unknown or violating substructure into `_cdf_variant`, with promotion as a recorded contract-evolution event.
5. Trust promotion/demotion as ledger events with demote-on-anomaly trigger.
6. Drift-quarantine conformance: freeze a resource, drift fixture type, prove quarantined rows, package verdict evidence, and accepted stream progress.

## Acceptance criteria

- P1 parent and ordered executable children exist before implementation starts.
- Each P1 child references `.10x/specs/types-contracts-normalization.md`, `VISION.md` Chapter 11, and relevant package/destination/run specs.
- Contract throughput benchmarks measure type/null/domain rules on 100k-row batches against the spike target of greater than 1 GB/s where the environment supports it.
- Quarantine round-trip, redaction adversarial checks, and drift-quarantine conformance evidence exist.
- Live runs fail closed or quarantine according to the compiled program rather than schema-only checks.

## Evidence expectations

Record P1 child evidence, contract throughput benchmark output, quarantine artifacts, redaction adversarial review, conformance scenario output, and adversarial review for the workstream.

## Explicit exclusions

No public performance claim, no speculative trust UI, no schema-on-read replacement for packages, no DataFusion multi-output-plan fork, and no destination quarantine mirror where the destination sheet does not support it.

## Progress and notes

- 2026-07-07: Opened from P0 stop-line. Current inspection shows `cdf-contract` has schema/program vocabulary and `PackageBuilder::write_quarantine_artifact`, but live execution has not yet routed row-level verdicts into quarantine artifacts.
- 2026-07-07: Read-only subagent inventory found `SchemaSource::Contract` currently rejected by project runtime and `apply_contract_exec` enforcing column coverage rather than row verdict/quarantine routing.
- 2026-07-08: Activated after P0 Workstream C closure. Opened P1 parent `.10x/tickets/2026-07-08-p1-contract-depth-program.md`, ordered children E1-E6, and API/semantics decision `.10x/decisions/contract-live-verdict-execution-semantics.md`. No implementation has started in this graph-shaping slice.
- 2026-07-08: P1 E1-E4 are closed. Contract live path now has row verdicts, quarantine routing/redaction, deterministic pre-merge dedup, and `_cdf_variant` capture with package contract-evolution evidence. E5 trust-ring ledger events and E6 drift-quarantine conformance remain before Workstream E can close.

## Blockers

None for opening the P1 contract-depth program. Individual child tickets must not implement unratified contract semantics beyond the behavior specified here and in active specs.
