Status: done
Created: 2026-07-10
Updated: 2026-07-10

# RP9C promotion command and conformance preflight

## Question

What destination-neutral deterministic ordering, recovery, command-output, migration, redaction, and concurrency conformance must RP9C prove once RP9A and RP9B supply exact persisted recovery authority and atomically fenced settlement, without adding a destination strategy or new product semantics?

## Sources and methods

This was a read-only implementation/conformance preflight. It inspected:

- `.10x/tickets/2026-07-10-p2-rp9c-promotion-command-concurrency-conformance.md` and its RP9A/RP9B dependencies;
- `.10x/specs/schema-promotion-corrections.md`, `.10x/specs/checkpoint-state-commit-gate.md`, `.10x/specs/package-lifecycle-determinism.md`, `.10x/specs/cli-error-experience-catalog.md`, and `.10x/specs/project-cli-observability-security.md`;
- the correction-receipt, run-ledger/commit-session, Parquet identifier, and RP9B settlement decisions/research;
- the initial RP9 fail review, implementer review, and current RP9A independent review;
- promotion planning/execution in `crates/cdf-project/src/promotion.rs` and `crates/cdf-project/src/runtime/promotion.rs`;
- CLI resolution, rendering, and promotion fixtures in `crates/cdf-cli/src/schema_command.rs`, `destination_uri.rs`, and `tests.rs`;
- DuckDB, Postgres, and Parquet correction capabilities and sessions;
- checkpoint/lease/publication SQLite storage and migrations;
- the destination, runtime-chaos, run-matrix, and local-Postgres conformance harnesses.

No source, test, generated artifact, ticket status, or external state was modified. No build or mutating verification was run because RP9C remains blocked on RP9A and RP9B.

## Record-backed execution contract

### Canonical target order

The RP5 plan already derives target keys from a `BTreeSet<TargetKey>` and emits targets in `(destination_id, target)` order. Publication targets are independently sorted by the same pair. RP9C should make this existing canonical plan order the sole package, checkpoint, settlement, report, and recovery order. Caller destination-vector order, filesystem enumeration, receipt order, hash-map order, and adapter kind are not authority.

For target sequence `T0..Tn`, the following identities remain target-derived and stable across retries:

- correction package id/hash authority;
- correction checkpoint id;
- persisted target-authority path;
- report and publication target position.

Input permutations must either canonicalize to the same ordered plan and artifacts or fail exact staged-plan validation; they must never change settlement order.

### Multi-target checkpoints form one deterministic chain

All promotion target checkpoints use one resource schema-contract scope, and the checkpoint spec permits exactly one committed head per `(pipeline, resource, scope)`. Therefore multi-target checkpoints cannot be siblings built from one initial head. The only contract consistent with the active specs is:

```text
H0 -> checkpoint(T0) -> checkpoint(T1) -> ... -> checkpoint(Tn)
```

`H0` is the exact authoritative head selected before packaging. Each correction package for `Ti` records the immediately preceding checkpoint as its input/parent authority (or `H0` for `T0`), and its output position is the deterministic promotion projection already defined by the runtime. Packages may be built before destination settlement because predecessor checkpoint ids are deterministic, but a later target checkpoint cannot commit until its predecessor is the exact committed head.

This is not distributed 2PC. Each destination target settles independently and a committed prefix remains durable after a later failure. The lock remains on the old schema and no publication event exists until every exact target checkpoint in the chain is committed.

### Recovery is prefix verification followed by the first incomplete target

On resume, the executor rehydrates the RP9A staged plan and correction packages, then walks targets in canonical order:

1. For a committed prefix target, verify the immutable package, exact stored receipt, live destination receipt, checkpoint receipt, deterministic checkpoint id, and parent/head chain; do not write the destination again.
2. For a package with a receipt but no checkpoint, verify the receipt and perform only RP9B's atomically fenced checkpoint commit.
3. For the first package without a receipt, replay its ordinary correction session idempotently, verify and persist the receipt, then perform the fenced checkpoint commit.
4. Do not settle a later suffix target while an earlier target is incomplete or contradictory.
5. Publish the lock only after the full target chain is committed; publish the event only after the lock CAS; verify exact publication target equality on every complete replay.

This yields deterministic later-target recovery without rollback, re-extraction, source access after packaging, or destination-specific orchestration.

### Persisted phase and recovery reporting

Failure reporting must be derived by re-inspecting persisted staged plan, packages, receipts, checkpoints, lock, and publication event, not from the last in-memory method called. The existing phase vocabulary is sufficient:

- `staged`: exact staged plan/snapshot authority exists but correction packaging is incomplete;
- `packaged`: every exact correction package/target authority exists, with no deeper durable target fact;
- `destination_settled`: the next target has a verified durable receipt not yet checkpointed;
- `checkpointed`: at least one target checkpoint is committed, or all are committed while the lock is still old; target rows disambiguate the committed prefix;
- `lock_published`: every target checkpoint is committed and the exact new lock is installed, but no exact publication event exists;
- `complete`: installed lock and exact publication event agree with every verified target.

The deepest phase alone is not enough for multi-target work. One shared typed recovery report must also carry the ordered per-target package/receipt/checkpoint/committed facts, exact committed prefix, first remaining action, and exact recovery command. Human and JSON rendering must consume the same report. On failure, the stable JSON error envelope remains intact and gains structured promotion recovery details through the existing additive error mechanism; human output names what persisted, what did not, and the same next command. Unreachable enum-only states or an error message assembled from failpoint names are not conformance evidence.

## Required fixture matrix

### Destination command fixtures

| Fixture | Setup and command | Required assertions |
|---|---|---|
| DuckDB in-place | Reuse the canonical `VendorID`/`score` residual fixture and ordinary DuckDB source receipt; run `cdf schema promote local.events --execute`. | `in_place_update`; promoted column values and cleared canonical residuals at original provenance addresses; exact correction receipt/checkpoint; lock then publication; replay no-op; human and JSON parity. |
| Postgres in-place | Reuse `LivePostgres`; create an ordinary loaded table with unique `_cdf_load/_cdf_segment/_cdf_row`, canonical residual source package and live receipt; resolve the database through `secret://`; execute the command. | Same addressed values and provenance uniqueness; atomic target update; typed addressed-correction evidence with original disposition; exact receipt/checkpoint/lock/event; idempotent replay; no destination-name branch. |
| Parquet sidecar | Reuse the existing filesystem Parquet command fixture after the identifier-policy dependency; ordinary source receipt belongs to `parquet_object_store`. | `correction_sidecar`; base target bytes/manifest are unchanged; immutable correction manifest/delta has provenance plus normalized promoted field; receipt/checkpoint settle; report never claims base mutation; replay is idempotent. |

These are three command-level scenarios over the same shared orchestrator. They do not imply a mixed-destination environment configuration or a new routing model.

### Deterministic multi-target fixture

Use one destination kind (DuckDB is the cheapest hermetic choice) with two target tables deliberately named so lexical order is obvious, for example `a_events` and `z_events`. Load the same canonical source package into both targets through ordinary destination commits so the package carries two exact receipts and the RP5 plan naturally contains two target associations. Do not fabricate targets by editing a dry-plan JSON value.

Assertions:

- dry-plan, staged target authorities, correction packages, checkpoints, execution report, and publication event all use `(duckdb, a_events)` then `(duckdb, z_events)` regardless of input receipt/destination vector order;
- package hashes/checkpoint ids are stable across input permutations and resume;
- checkpoint `z_events` names checkpoint `a_events` as its parent/input authority;
- lock publication is absent until both checkpoints commit;
- the publication event target set equals the two exact package/receipt/checkpoint tuples, with no missing or extra target.

### Later-target failure and source-deletion recovery

Provide a test-only destination runtime/protocol decorator or execution hook that fails a selected canonical target and phase, not merely “the next call.” Fail `z_events` after `a_events` checkpoint commit in each material window:

- before second destination settlement;
- after second durable receipt but before checkpoint commit;
- after second checkpoint but before lock CAS;
- after lock CAS but before publication.

After each failure assert the exact persisted phase, ordered target facts, old/new lock state, publication absence/presence, remaining action, and recovery command. For every post-package window delete the original residual source package before resuming. Recovery must use only RP9A staged/correction authority, preserve the first target's destination footprint and receipt, avoid duplicate physical correction on an already-settled target, finish the suffix, and publish exactly once.

### Takeover and race fixtures

Use barriers/hooks and the RP9B deterministic store clock; do not use sleeps.

- **Promotion vs promotion:** owner A pauses during destination settlement; its lease expires and owner B acquires a higher token. A may finish an idempotent destination receipt but cannot checkpoint, CAS the lock, or publish. B verifies the package/receipt, resumes the exact target prefix, and completes once. A retry observes the same final authority.
- **Later-target takeover:** A commits target 0 and settles target 1 after expiry. Its fenced target-1 checkpoint fails. B verifies target 0 and the durable target-1 receipt, commits the suffix, then publishes. No target is physically corrected twice.
- **Pin vs promotion:** both start from the same exact old lock bytes and rendezvous before authority installation. Exactly one replacement wins; the loser reports the observed lock hash and replan/inspect recovery. Final bytes equal one complete candidate and never a hybrid or stale overwrite. Exercise both winner orders.
- **Run vs promotion:** pause a normal run after it has planned/package-staged under the old pinned schema. Complete promotion publication, then let the old run attempt checkpoint advancement. The shared typed commit authority rejects the incompatible old schema; destination durability, if already obtained, remains recoverable evidence but the old run cannot become the new head. Assert no CLI-local mutex or command-name branch supplies this protection. Also show that an unrelated resource/scope is unaffected.

RP9C should consume the RP9B atomic store conformance rather than recreate lease correctness with command-level timing alone.

### Run-ledger v3 to v4 migration fixture

Construct a committed version-3 SQLite fixture with at least one run and multiple ordered run events containing nontrivial typed details. Then open/migrate through the public state path and assert:

- the run-ledger component moves from 3 to 4 additively;
- all prior run records/events, sequences, kinds, ids, and details remain readable and unchanged;
- `cdf_promotion_publications` and append-only triggers exist;
- a new exact multi-target publication can be inserted, read after reopen, replayed idempotently, and conflicted authority is rejected;
- a second `cdf state migrate` is current/no-op;
- checkpoint and lease component versions/history are unchanged.

The current suite proves fresh v4 and publication idempotence separately, but it does not yet prove this exact legacy-v3 preservation path.

### Secret-redaction fixture

Use the Postgres command fixture with `secret://env/RP9C_POSTGRES_URL` resolving to a URI containing a unique username/password sentinel. Exercise success plus a destination failure after staged artifacts exist. Assert the sentinel is absent from:

- staged promotion plan, proposed snapshot, target authority, correction package tree, receipts, lockfile, and serialized publication event;
- human stdout/stderr, JSON success/error/recovery fields, remediation, suggestions, and exact recovery command;
- Debug/error formatting and captured traces/progress.

The secret reference may remain in project configuration; resolved values may be used only by the runtime destination. Reuse `assert_generated_artifacts_exclude`, `assert_secret_absent`, URI-userinfo redaction, and the destination's `secret_redaction` value rather than introducing a promotion-only scrubber.

### P1, JSON, and structured recovery assertions

For success, every destination fixture asserts the same typed fields in JSON and human render: phase, resumed flag, old/new schema, ordered targets, package/receipt/checkpoint ids, committed status, lock/event status, remaining action, and recovery command. Human output uses renderer primitives, has headless/TTY snapshots, and contains no raw debug enum casing.

For every failure boundary, JSON retains `ok`, `error.kind`, `error.message`, `error.exit_code`, `error.not_supported`, stable code, and remediation, plus a structured promotion recovery object. Assert exact values rather than substring-only messages. Human output must render the same phase, committed target prefix, pending target, remaining action, and copyable recovery command. Re-running that rendered command must either complete the exact promotion or return the exact complete no-op report.

## Reusable harness abstractions

- Extend the existing CLI promotion fixture builder into a destination-neutral source-package/receipt builder parameterized by resolved destination and target. Keep format/residual generation common; let adapters perform ordinary source commits.
- Reuse `ProjectDestinationRuntime` and `ResolvedProjectDestination::new` for a test-only faulting decorator keyed by `(destination_id, target, correction phase)`. Do not add failure behavior to production destination adapters.
- Reuse `LivePostgres` and its isolated schema lifecycle; keep the hermetic DuckDB/Parquet cases in ordinary CLI tests and gate only the live Postgres slice.
- Reuse runtime-chaos `ChaosDestinationHandle` concepts—resolved runtime, read-only footprint, trait receipt verification—but add promotion-specific target/checkpoint/publication observations instead of forcing promotion into the ordinary run crash enum.
- Reuse RP9B's deterministic clock, barriers, aggregate settlement store, and takeover helpers. One scenario builder should accept ordered targets, failure boundary, owner transition, and source-deletion toggle.
- Reuse `project_tree_snapshot`, `assert_generated_artifacts_exclude`, `assert_secret_absent`, JSON envelope helpers, and renderer snapshot conventions.
- Add a single read-only promotion observation/recovery classifier used by both executor errors and CLI rendering. Tests should never infer phase independently from directory names.
- Add a committed legacy-v3 SQLite fixture helper beside existing state migration fixtures; preserve raw fixture construction as the historical format authority.

## Code-smell traps

- Do not hand-edit `SchemaPromotionPlanReport.targets` to create multi-target input; that bypasses RP5 canonical identity and tests an impossible authority.
- Do not use destination-name `match` branches in promotion orchestration, reporting, ordering, or recovery. Only fixture setup may choose a concrete adapter.
- Do not treat caller vector, receipt, filesystem, or hash-map order as settlement authority; use the validated canonical target sequence.
- Do not build all target checkpoints from one initial head. Sibling checkpoints violate the single-head invariant and make retry order ambiguous.
- Do not parallelize target checkpoint commits. The active spec promises independent destination settlement, not unordered schema-contract heads or distributed 2PC.
- Do not roll back an already verified earlier target when a later target fails. Preserve the committed prefix and withhold lock/publication.
- Do not rerun an already receipted target merely to make the test pass; verify receipt and continue at the first incomplete boundary.
- Do not use global “after receipt” failpoints that always stop the first target; failure injection must name target and durable boundary.
- Do not use sleeps, wall-clock lease expiry, or probabilistic thread races; use barriers and injected clocks.
- Do not implement pin/run race protection as a CLI mutex, lockfile polling loop, or command-name check. It belongs at exact lock CAS or the shared typed commit authority named by the tickets.
- Do not report an in-memory phase after a write error. Reopen and classify persisted facts, including the lock/event gap.
- Do not serialize resolved destination URIs into a staged report for convenience. Persist identifiers, target names, sheet hashes, and secret references only.
- Do not claim Postgres conformance from RP6 protocol tests, Parquet conformance from sidecar unit tests, or multi-target correctness from a one-target crash matrix. RP9C requires command-level integration plus focused lower-layer races.
- Do not update goldens before inspecting semantic diffs, and do not let nondeterministic timestamps, temp paths, database URLs, or lease tokens enter stable snapshots.

## Conclusions

RP9C needs no new destination or recovery semantics. The active specs already imply a canonical `(destination_id, target)` execution order, a single deterministic schema-contract checkpoint chain, committed-prefix recovery, lock publication only after the entire chain, and exact publication target equality. The conformance work should encode that contract once, then prove DuckDB, Postgres, and Parquet command behavior; later-target/source-free recovery; fenced takeover and run/pin races; v3-to-v4 preservation; secret redaction; and one typed human/JSON recovery report.

RP9C must remain open and inactive until RP9A and RP9B are resolved. This preflight is execution guidance, not authorization to implement or a claim that the current sibling-head construction, failure rendering, run authority gate, or migration evidence already passes.
