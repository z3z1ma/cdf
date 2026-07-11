Status: done
Created: 2026-07-08
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md
Depends-On: .10x/specs/data-onramp-conformance.md, .10x/knowledge/runtime-conformance-throughput-rule.md

# P2 WS-I conformance, parity, and friction regression suite

## Scope

Make the harness catch up as P2 lands: S1-S8 golden paths, preview/run parity law, eighteen-friction regression mapping, deterministic fixtures plus live tier, and widening-lattice property tests.

Split executable child tickets before code for each conformance slice or property target. This workstream accrues throughout P2 and gates every other workstream closure.

## Acceptance criteria

- S1-S8 are represented in conformance, with deterministic fixture coverage for ordinary CI.
- Preview/run parity is a named law per source archetype.
- Each of the eighteen P2 frictions maps to a regression test in closure evidence.
- Widening-lattice property tests prove composition and no value loss for supported Arrow arrays.
- FileManifest state round-trips remain covered across checkpoint `state_version`.
- The final P2 evidence includes a recorded S1+S2 terminal session.

## Evidence expectations

Conformance matrix output, golden hashes, property-test output, fixture provenance, live-tier notes where applicable, and adversarial review of any recorded exclusions.

## Explicit exclusions

This ticket does not require cloud credentials in every push CI run; fixture-backed CI is acceptable when live-tier coverage is separately recorded.

## Progress and notes

- 2026-07-09: Opened I5, now terminal at `.10x/tickets/done/2026-07-09-p2-ws-i5-recorded-http-request-capture-race.md`, after the standalone-green S5 fixture failed reproducibly only under full parallel workspace load because its nonblocking request reader treats `WouldBlock` as EOF. The child owns bounded complete-header capture; S5 assertions remain unchanged.

- 2026-07-08: Opened as P2 workstream owner from the directive.
- 2026-07-08: Split first executable child `.10x/tickets/done/2026-07-08-p2-ws-i1-friction-regression-registry.md` to map the eighteen P2 frictions to existing tests or explicit open coverage gaps before implementation lanes start closing.
- 2026-07-09: Friction registry recorded in `.10x/evidence/2026-07-08-p2-friction-regression-registry.md`. Initial capture classified all eighteen directive frictions as open P2 coverage obligations, with partial primitive or negative coverage named where source/tests supported it.
- 2026-07-09: B1 closed the direct declarative-vocabulary guard for friction 3 through focused `cdf-declarative` tests. WS-I closure still requires replacing the remaining open-owner rows with actual regression tests/conformance scenarios or recorded exclusions.
- 2026-07-09: D1 added local modest-N primitive coverage for friction rows 8 and 9: deterministic per-file partitions and preview/open path parity inside `cdf-declarative`, plus live local-file golden reruns. WS-I still owes S2/S8 conformance coverage for manifest incrementality, remote/public globs, no-op reruns, and parity across source archetypes.
- 2026-07-09: B3 added focused local Parquet reader coverage for friction rows 4 and 5: declared-schema reads now reconcile physical Parquet types through the shared model, materialize `int32 -> int64` and `float32 -> float64`, fail closed for lossy narrowing, and preserve the undeclared physical path. WS-I still owes conformance-level S1/S2/S8 coverage and widening-lattice property tests.
- 2026-07-09: Split executable child `.10x/tickets/done/2026-07-09-p2-ws-i2-preview-run-parity-and-golden-path-matrix.md` for P2 scenario registry and preview/run parity matrix scaffolding.
- 2026-07-09: I2 closed as `.10x/tickets/done/2026-07-09-p2-ws-i2-preview-run-parity-and-golden-path-matrix.md` with evidence in `.10x/evidence/2026-07-09-p2-ws-a7-d3-i2-batch.md` and review in `.10x/reviews/2026-07-09-p2-ws-a7-d3-i2-batch-review.md`. The conformance crate now names S1-S8, records pending/excluded cells honestly, maps all eighteen frictions to actual tests or active owners, and asserts preview/run parity for currently supported local file, REST fixture, and Postgres table archetypes. Full S1-S8 green coverage remains open.
- 2026-07-09: B4 closed as `.10x/tickets/done/2026-07-09-p2-ws-b4-widening-property-conformance.md` with widening-lattice property tests in `cdf-conformance`. Evidence: `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md`. This strengthens the row-4 regression law but does not close full S1/S2/S8.
- 2026-07-09: E2/G1 closure added deterministic fixture coverage for single-file HTTPS Parquet discovery/run and source diagnostics/deep validate. Evidence: `.10x/evidence/2026-07-09-p2-e2-g1-b4-batch.md`. WS-I still owns final public-data S1/S2, remote glob, compression, cloud, and recorded-session conformance.
- 2026-07-09: Split executable child, now terminal at `.10x/tickets/done/2026-07-09-p2-ws-i3-matrix-friction-reconciliation.md`, after audit found stale terminal owners and under-indexed G1/D4/E2/H2 coverage in the executable P2 registry. I3 is ownership/evidence repair only and does not promote any pending golden path.
- 2026-07-09: I3 closed as `.10x/tickets/done/2026-07-09-p2-ws-i3-matrix-friction-reconciliation.md` with `.10x/evidence/2026-07-09-p2-a8-b6-i3-integration.md` and `.10x/reviews/2026-07-09-p2-a8-b6-i3-integration-review.md`. The registry now rejects stale owners and missing test functions while keeping all S1-S8 rows pending until runtime acceptance is complete.
- 2026-07-09: Split executable child, now terminal at `.10x/tickets/done/2026-07-09-p2-ws-i4-s5-s7-standalone-conformance.md`, for exact deterministic S5 REST discover/pin/package and S7 key/disposition scenarios. Only those two matrix rows may be promoted by the child.
- 2026-07-09: I4 closed as `.10x/tickets/done/2026-07-09-p2-ws-i4-s5-s7-standalone-conformance.md` with `.10x/evidence/2026-07-09-p2-c3-i4-integration.md` and `.10x/reviews/2026-07-09-p2-c3-i4-integration-review.md`. S5 and S7 are now standalone deterministic conformance scenarios and the only promoted P2 rows; S1-S4/S6/S8 remain pending.
- 2026-07-10: A10f completed the shared `preview-balanced-stratified-v1` front end, deterministic global payload selection, truthful bounded evidence, and local file/REST/Postgres parity fixtures. S8 remains pending only for its unimplemented HTTP-template/cloud archetype cells; WS-I consumes A10f as downstream conformance authority rather than blocking A10f closure.
- 2026-07-09: I5 closed as `.10x/tickets/done/2026-07-09-p2-ws-i5-recorded-http-request-capture-race.md` with `.10x/evidence/2026-07-09-p2-d5-i5-integration.md` and `.10x/reviews/2026-07-09-p2-d5-i5-integration-review.md`. The S5 recorded server now captures complete bounded headers, uses bounded response writes, and surfaces worker failures without teardown panics. Final parallel workspace nextest passed 809/809 without weakening S5.
- 2026-07-10: E6 promoted S1 and S2 to covered with standalone production-path CLI tests. S1 performs add→pin→plan→run with no typed schema; S2 proves dated HTTP enumeration, preview/run partition parity, manifest initial load, unchanged no-op, and newly-present-month-only loading. S3/S4/S6/S8 remain pending.
- 2026-07-10: Promoted S3 and S8 after extending the recursive object-store gzip fixture through the shared bounded preview path before full execution. S3 now combines remote recursive resolution/discovery/pin/preview/run/FileManifest proof with the existing shared drift-contract conformance. S8 now covers local multi-file, REST, Postgres, dated HTTP, and object-store compressed row formats. Only S4 and S6 remain pending.
- 2026-07-10: G3 promoted S6 to covered. Typed quarantine verdicts now survive plan→project report→JSON/human rendering with exact file, field path, observed/effective types, rule, policy, and remediation while the run remains successful and evidence-preserving. Only S4 remains pending.
- 2026-07-10: H4 promoted S4 to covered with deterministic local-Postgres add/discover/plan/preview/run conformance and private-secret evidence. All eight P2 golden paths are now marked covered; final WS-I work is the aggregate eighteen-friction reconciliation, full suite, and recorded closure evidence.
- 2026-07-10: Workstream closed. The executable registry now records S1-S8 covered, every friction row names concrete regression tests, and no friction retains an open P2 owner. Final matrix/full-suite evidence and review are recorded at the P2 parent.

## Blockers

None.
