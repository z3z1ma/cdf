Status: open
Created: 2026-07-11
Updated: 2026-07-11
Parent: .10x/tickets/2026-07-10-p3-terabyte-scale-program.md
Depends-On: .10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md, .10x/tickets/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md, .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md, .10x/tickets/2026-07-10-p3-ws-c-deterministic-parallelism.md, .10x/tickets/2026-07-10-p3-ws-d-destination-bulk-paths.md, .10x/tickets/2026-07-10-p3-ws-e-hashing-package-io.md, .10x/tickets/2026-07-10-p3-ws-f-constant-memory-guarantee.md, .10x/tickets/done/2026-07-10-p3-ws-g-remote-io-overlap.md, .10x/tickets/2026-07-10-p3-ws-h-interop-boundaries.md, .10x/tickets/2026-07-11-p3-ws-v-vectorized-validation.md

# P3 Z1: envelope, triage, and coverage evidence reconciliation

## Scope

Generate/publish the final host-labeled performance envelope, map every P3 target/current-gap/triage/coverage claim to raw implementation evidence, terminalize absorbed triage with measured/no-action rationale, update README/docs/coverage, and fail closed on unavailable or incomparable cells.

## Target ownership matrix

| Target | Primary evidence owners |
|---|---|
| Parquet file/glob → package | B2, C4, E4 |
| CSV → package | B4, C4, E4 |
| streamed compressed NDJSON/JSON | B1, B5, G3, B13 |
| contract validation | V1–V3 |
| package build/hash | E1–E4 |
| package → DuckDB | D1, D2, D5 |
| package → Postgres | D1, D3, D5 |
| package → Parquet | D1, D4, D5 |
| full-year HTTPS TLC → DuckDB | G4 plus B2/C4/D2 |
| 1 TB glob → Parquet | F4 plus C4/D4 |
| ≤10% correctness overhead | L5/final lab plus A/E/V/D phase evidence |
| constant process-tree memory | F1–F4 |
| jobs invariance | C4/C5 and per-workstream conformance |
| native format breadth | B13 |
| foreign boundaries | H5 |
| VISION 6.1–6.6 | A/C/F/V plus A9 coverage evidence |

## Acceptance criteria

- Every matrix cell links raw machine evidence, host/mode/reference/bias labels, and relevant correctness proof.
- Absolute and roofline targets are both shown; missing/unavailable/failing cells remain visibly non-green.
- All performance triage tickets are moved terminal only after their own criteria map to evidence/no-action rationale; parent backlog reconciles.
- Generated envelope docs and README claims contain no stronger or broader language than evidence.
- VISION coverage/status and P3 parent/workstream/dependency graph agree.

## Evidence expectations

Generated envelope and machine source, reference/provenance links, target-to-record matrix, triage terminal diffs, coverage/README docs, link checker, and adversarial claim/bias review.

## Explicit exclusions

No benchmark rerun to hide a failing cell, baseline reset, implementation repair, target weakening, or unsupported marketing claim.

## Blockers

Blocked on all P3 workstream closures.

## References

- `.10x/decisions/terabyte-scale-performance-envelope.md`
- `.10x/specs/performance-lab-and-envelope.md`
- `.10x/tickets/done/2026-07-07-performance-investigation-backlog.md`
- `.10x/knowledge/vision-coverage-matrix.md`
