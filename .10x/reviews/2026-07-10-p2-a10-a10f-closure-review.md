Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md, .10x/tickets/done/2026-07-09-p2-ws-a10f-multifile-discovery-runtime-conformance.md
Verdict: concerns

# A10 and A10f closure-only graph review

## Closure boundary

This review inspected closure authority only. It did not run verification, modify implementation, repair existing records, change ticket statuses, move tickets, or accept residual risk.

Inspected authority included:

- the A10 parent and A10f child tickets;
- terminal A10a, A10b, A10c, A10d, A10e, and A10g tickets;
- each child's recorded integration evidence and pass review;
- the original A10f fail review, its subsequent resolution, the canonical-identity repair evidence, and the superseding pass review;
- active multi-file aggregation, explicit sampled-discovery, source-identity/preview, and global-preview-budget decisions;
- active sampled-discovery and source-experience specifications;
- the open WS-I conformance owner and current S1–S8 registry state.

## Acceptance and evidence mapping

### A10 parent

- Multi-file Parquet and Arrow IPC discover/pin/diff/no-pin/auto-pin, cardinality-one compatibility, bounded metadata-only probing, and exact runtime-manifest separation are supported by terminal A10c, `.10x/evidence/2026-07-10-p2-a10c-rp3-rp4-integration.md`, and its pass review.
- Deterministic aggregate widening, missing-null semantics, metadata variance, source-name identity, collisions, and total per-file verdicts are supported by terminal A10b plus the A10a/A10b evidence and pass review.
- Versioned content-addressed manifests, sorted candidate participation, budget evidence, legacy v1 compatibility, tamper rejection, and no-clobber publication are supported by terminal A10a and its retrospective knowledge owner `.10x/knowledge/content-addressed-sidecar-publication.md`.
- Immutable baseline pins, compatible effective schemas, typed-null materialization, exact per-observation coercion evidence, package stamping, and source-free replay are supported by terminal A10d evidence/review.
- Named incompatible/freeze quarantine, exact processed positions, all-quarantine packages, receipt/checkpoint-gated advancement, skip/retry, and retained removal history are supported by terminal A10e evidence/review.
- Explicit positive `sample_files`, canonical `stratified-hash-v1`, honest probed/unprobed coverage, no substitution under budget failure, and unseen runtime residual/quarantine routing are supported by terminal A10g evidence/review.
- Plan/preview/run front-end parity, bounded payload membership, global row/byte/batch limits, terminal-quarantine attestation parity, and local/fixture conformance are supported by A10f evidence plus the superseding canonical-identity repair review.

No parent acceptance criterion requires S3/GCS/Azure implementation, HTTP template enumeration, remote Arrow IPC, compression, text sampling, destination mapping, or a live public-network session. Those are explicit exclusions or remain owned by WS-E/WS-I. Pending full S2/S6/S8 rows therefore do not invalidate the completed local/fixture A10 behavior: S2 still lacks HTTP enumeration/public monthly execution, S6 still lacks final rendered remediation coverage, and S8 still lacks HTTP-template/cloud cells.

### A10f child

- Cardinality-one/many binary discovery artifacts and compatible/incompatible runtime behavior are inherited from the terminal A10c–g chain and exercised again through the full affected integration suites.
- The shared engine preview front end replaces the first-partition CLI path and is exercised for local multi-file files, REST fixtures, and Postgres.
- `preview-balanced-stratified-v1` defaults, `K=min(N,B)`, fair quotas, atomic decoded-byte admission, truthful selected/opened/attested/inspected/partial/uninspected evidence, bounded payload opens, and no-write behavior have direct focused tests and full-suite evidence.
- The original independent finding was real: preview initially used noncanonical bounded-identity bytes. `.10x/reviews/2026-07-10-p2-a10f-canonical-identity-repair-review.md` proves the repaired shared kernel identity emits the unchanged historical bytes and score and produces identical discovery/preview membership. No open implementation finding remains from that review chain.

## Closure blockers

### Significant: A10f has an unresolved explicit dependency on open WS-I

A10f declares:

```text
Depends-On: ... .10x/tickets/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md
```

WS-I remains `Status: open`. Under the record graph, an explicit dependency is not closure-coherent while active unless the dependency is narrowed, superseded, or explicitly documented as non-blocking coordination authority. A10f cannot be moved to `done` with that unresolved edge. Because A10f is the parent plan's final conformance child, A10 cannot close while A10f remains active.

Two valid closure paths exist, but closure-only review cannot choose or perform either:

1. wait for WS-I to become terminal; or
2. repair A10f's dependency to the focused conformance/specification authority that is actually satisfied, while retaining WS-I as the downstream owner for public/cloud/final-program rows.

The active A10f ticket is the durable owner of this blocker; no new mailbox ticket is needed.

### Significant: WS-I and the scenario registry contain stale post-A10f state

WS-I was last updated before the A10f bounded selector/parity repair. The current S8 registry rationale still says a ratified global payload bound/selector is pending, although `.10x/decisions/preview-global-budget-and-payload-selection.md` is active and A10f implements and verifies it. S8 may correctly remain `pending` for HTTP-template/cloud cells, but its rationale no longer distinguishes completed bounded-selector work from genuinely open remote work.

This stale statement does not weaken the implementation evidence, but it prevents closure records from agreeing. WS-I owns the registry reconciliation. Closure must not promote S2/S6/S8 beyond proved behavior; it should only remove the obsolete selector claim and retain exact WS-D/WS-E/WS-G/WS-I owners for the remaining cells.

## Dependency and status coherence

- A10a, A10b, A10c, A10d, A10e, and A10g are terminal `done`, reference the A10 parent, and have recorded evidence plus pass reviews.
- Their dependency edges point to active governing decisions/specifications or terminal tickets; no stale terminal-ticket path was found in the A10a–g chain.
- A10 remains `open`, A10f remains `active`, and A10f's two implementation prerequisites A10e/A10g are terminal.
- The original A10f review remains `fail` as historical truth and has an explicit subsequent-resolution pointer; the superseding repair review is `pass`. This review chain is coherent.
- Evidence counts are temporal observations at their recorded integration boundaries. Later unrelated test additions do not invalidate those counts, and the latest A10f evidence records the final repair gates explicitly.

## Retrospective assessment

Material learning has durable owners:

- content-addressed no-clobber publication is in `.10x/knowledge/content-addressed-sidecar-publication.md`;
- aggregate/budget and sampled-selector semantics are in active decisions/specifications;
- immutable baseline/effective-schema and protocol extension seams are in active decisions;
- the canonical selector identity failure and invariant are preserved in the sampled-discovery specification, kernel-owned typed identity, original fail review, and superseding repair review;
- remote/public, rendered-remediation, and final-program conformance remain owned by WS-E/WS-G/WS-I rather than appearing only in final prose.

No new retrospective implementation or research ticket is required. The A10 parent owns the final closure reconciliation note that cites these durable owners and records that no additional learning extraction is outstanding; that bookkeeping must occur only after the dependency/registry blockers above are resolved.

## Verdict

Concerns. The local/fixture A10 and A10f behavioral acceptance criteria are supported, child evidence/reviews are coherent, and no implementation defect remains open. A10f and A10 cannot close now because A10f still depends explicitly on open WS-I and WS-I's S8 rationale is stale relative to the completed selector/parity work. Resolve those graph-only blockers first; then A10f may close, followed by A10, without expanding scope into remote/public P2 work.

## Limits

- This review did not re-run tests or independently regenerate artifacts; it assessed the recorded evidence and current record/source graph as requested.
- It does not authorize S2, S6, or S8 promotion, accept missing remote coverage, or change WS-E/WS-G/WS-I ownership.
- It does not close or move either reviewed ticket.

## Subsequent resolution

Both graph concerns were repaired and re-audited in `.10x/reviews/2026-07-10-p2-a10-a10f-closure-repair-review.md` with verdict `pass`. This review's `concerns` verdict remains the historical assessment before dependency direction and S8 rationale were corrected.
