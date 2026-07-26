Status: recorded
Created: 2026-07-25
Updated: 2026-07-25
Relates-To: .10x/tickets/done/2026-07-11-p3-z3-program-closure-retrospective.md

# P3 program closure audit

## Observation

The implemented P3 program graph is terminal. Its runtime, codec, parallelism, destination,
package-I/O, memory, remote-I/O, interop, validation, statistics-pruning, and fixed-schema
admission workstreams are closed or deliberately cancelled with retained rationale. Z1 publishes
the machine-generated final envelope without resetting the immutable baseline. Z2 preserves the
full-year TLC and 1.0086 TiB demonstrations and an adversarial matrix with explicit limits.

P3 does not close by claiming that every original numerical ambition became green. It closes the
implemented architecture with every target reconciled to one of `green`, `partial`,
`accepted_residual`, `not_demonstrated`, or `observed` in `docs/performance-envelope.md`. The
original ambitions remain immutable in
`.10x/decisions/terabyte-scale-performance-envelope.md`; future programs may improve those cells
without reopening superseded product paths or falsifying the historical baseline.

## Closure matrix

| Program slice | Terminal owner | Closure authority |
|---|---|---|
| Performance lab | `.10x/tickets/done/2026-07-10-p3-ws-l-performance-lab.md` | Immutable pre-optimization baseline, host-labelled runner, generated closeout envelope |
| Streaming runtime | `.10x/tickets/done/2026-07-10-p3-ws-a-streaming-runtime-pipeline.md` | Injected execution host, one ledger, bounded/fused graph, staged ingress, drain epochs |
| Native decode engines | `.10x/tickets/done/2026-07-10-p3-ws-b-format-decode-engines.md` | Registry-hosted Parquet/CSV/JSON/NDJSON/Arrow IPC, streaming compression, no generic format branches |
| Deterministic parallelism | `.10x/tickets/done/2026-07-10-p3-ws-c-deterministic-parallelism.md` | Jobs invariance, CPU admission, canonical isolated-worker equivalence |
| Destination bulk paths | `.10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md` | One capability-selected ingress model per destination; sole stock DuckDB scanner; Postgres binary COPY; streaming Parquet |
| Hashing/package I/O | `.10x/tickets/done/2026-07-10-p3-ws-e-hashing-package-io.md` | Hash-while-write, bounded verification, no production reread, measured 0.903x write roofline |
| Constant memory | `.10x/tickets/done/2026-07-10-p3-ws-f-constant-memory-guarantee.md` | 5/20/100 GiB 2-GiB law, exact 1.0086 TiB/default-policy run, clean too-small failure |
| Remote overlap | `.10x/tickets/done/2026-07-10-p3-ws-g-remote-io-overlap.md` | Generation-bound reads, growing spool, explicit complete spool, liveness/controller evidence |
| Foreign boundaries | `.10x/tickets/done/2026-07-10-p3-ws-h-interop-boundaries.md` | Neutral incremental Python/subprocess stream, lane/copy/control evidence; no WASM claim |
| Vectorized validation | `.10x/tickets/done/2026-07-11-p3-ws-v-vectorized-validation.md` | All 12 64k hot-kernel cells at 3.016–7.254 GB/s on the controlled host |
| Statistics pruning | `.10x/tickets/done/2026-07-12-p3-j1-evidence-statistics-pruning.md` | Conservative verified-package selection through DataFusion pruning without identity production |
| Fixed-schema admission | `.10x/tickets/done/2026-07-13-p0-fixed-schema-discovery-stream-admission.md` | Fixed output schema before final plan; pinned execution admits observations in-stream without a current-schema prescan |
| Envelope reconciliation | `.10x/tickets/done/2026-07-11-p3-z1-envelope-evidence-reconciliation.md` | Generated `docs/performance-envelope.md` and validated source fixture |
| Scale/adversarial proof | `.10x/tickets/done/2026-07-11-p3-z2-scale-demo-adversarial-review.md` | `.10x/evidence/2026-07-25-p3-z2-scale-demo-adversarial-review.md` |

The direct P3 graph contains 24 children including this closeout. Before moving this ticket and
the parent, the other 23 were terminal: 21 `done` and two `cancelled`. Every broad workstream
listed by the parent is `done`; the cancelled WS-J umbrella retains J1 as the completed bounded
consumer, and cancelled D17 retains its measured no-action rationale.

## Original criterion reconciliation

| Original parent criterion | Terminal result |
|---|---|
| Every numerical target green | Not literally achieved. Every target is instead reconciled in the generated envelope; no baseline or threshold was reset. |
| Aggregate correctness/evidence overhead ≤10% | `not_demonstrated`; component overheads are measured, but no semantically equivalent raw whole-product comparator exists. |
| Jobs-invariance and 100 GiB/2 GiB permanent laws | Green. C4 and F3 own the permanent jobs/memory falsifiers. |
| One-TiB/default-budget run is I/O-bound | Constant-memory/default-budget lifecycle is green. Unique-byte I/O saturation is partial because the generator uses hard links. |
| Ten original data-plane gaps have before/after evidence | Terminal across L/A/B/C/D/E/F/G/H and Z2. Rejected candidates and superseded paths remain recorded but deleted from product code. |
| Generated envelope published and README linked | Green through Z1. |
| Performance triage graph terminal | Green. No active P3 performance-triage ticket remains. |
| VISION 6.1–6.6 leave pending | Green for the implemented finite/drain runtime. Resident supervision remains a parked future product, not a P3 dependency. |
| Adversarial review finds no unresolved critical/high issue or architecture leak | Green through Z2. Lesser limits are explicit and have measured no-action rationale. |

The closure amendment is supported by the user's repeated direction that tickets are not dogma,
measured no-value work may be cancelled, the last G4 residual should be accepted rather than
chased with speculative product logic, and the stabilization graph should reach terminal steady
state. It does not alter the governing performance decision or conceal non-green cells.

## Architecture and artifact coherence

The closure pass traced the shared topology:

```text
compiled source plan
→ canonical partition task authority
→ accounted source/codec publication
→ validation + normalization
→ deterministic canonical segments + hash
→ capability-selected destination ingress
→ verified receipt
→ checkpoint commit
```

- Source formats compose through codec and byte-source registries; generic orchestration contains
  no concrete source/format match branch.
- Destinations expose one ingress category behind the runtime registry. DuckDB's unsafe public-ABI
  scanner is confined to `cdf-dest-duckdb`; the appender/nanoarrow/custom-runtime product paths
  are deleted.
- Package identity remains native and deterministic. DataFusion may prune/analyse but does not
  produce identity-bearing package bytes.
- The serialized isolated-worker task/result protocol reconstructs through registries and proves
  direct/isolated package equivalence without introducing a distributed scheduler.
- Adaptive execution records the policy needed for replay while canonical segmentation remains
  byte-first and plan-versioned.

No package migration or backwards-compatibility shim was introduced. The final package row
ordinal, physical admission identities, source discovery binding, and external task authorities
are current artifact semantics.

## Reference and status audit

The audit:

1. enumerated every direct P3 child by `Parent:` and checked its terminal status;
2. enumerated every active ticket globally and confirmed only P1, P3 closeout, and stabilization
   owners remained before this move;
3. scanned all backtick-delimited `.10x/` references in the P3 parent, Z1, Z2, Z3, and Z2 evidence
   and found zero missing paths;
4. repaired parent/dependency/reference paths as Z2, Z3, and P3 moved to `tickets/done/`;
5. updated the VISION coverage rows for the completed P3/runtime slices without marking parked
   distributed, resident, WASM, enterprise-format, or signing products implemented.

## Findings and disposition

- No critical/high closure finding remains.
- The final envelope has one user-accepted remote TLC residual, three partial cells, and one
  not-demonstrated aggregate overhead cell. These are terminal results, not green claims.
- Enterprise codec breadth, distributed execution, resident streaming/CDC, broader DataFusion
  bridges, WASM/Lua, warehouse/lakehouse destinations, signing, and extreme unbounded metadata
  cardinality remain in `.10x/knowledge/active-backlog-and-future-roadmap.md`. They have no active
  P3 ticket.
- The benchmark EC2 tranche is terminated. P3 closure incurred no new cloud host.

## Retrospective

P3's largest avoidable cost was optimizing hypotheses before measuring the complete product path.
The decisive improvements came from preserving one controlled host, comparing against a named
roofline, and deleting a candidate immediately when it lost. The remote Parquet and DuckDB
investigations also reinforced the architectural rule: a useful mechanism belongs behind a
source/destination capability boundary, not in generic orchestration.

The second lesson is that scale claims need typed bias. Logical bytes, unique physical bytes,
overlapping phase durations, managed memory, process RSS, cgroup memory, and destination-native
reference work are different currencies. Z1's machine reconciliation fixture and Z2's explicit
limits prevent them from being merged into a flattering but false number.

The third lesson is closure discipline. An immutable ambitious target can remain red while an
implementation program becomes terminal, provided the red result is visible, owned by an explicit
acceptance/no-action decision, and does not survive as a speculative active ticket. This yields a
smaller, faster codebase and a trustworthy future roadmap.

## Limits

This record proves the terminal state of the implemented P3 scope, not physical optimality for
every host or workload. The immutable performance decision remains the ambition for future
programs. It does not claim distributed execution, a resident streaming service, WASM, every
enterprise format, every destination, unique-byte exabyte scale, or aggregate overhead that was
not measured.
