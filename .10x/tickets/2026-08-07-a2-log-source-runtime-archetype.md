Status: active
Created: 2026-08-07
Updated: 2026-08-07
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
Depends-On: `.10x/tickets/done/2026-08-03-cdc-source-position-artifact-transition.md`, `.10x/tickets/2026-08-07-a1-5-package-native-keyed-effects.md`

# A2: log-source runtime archetype

## Scope

Implement the neutral finite-drain runtime contract between first-party CDC adapters and the
existing drain/package/receipt/checkpoint authorities. The runtime must admit one ordered source
stream, account a source settlement unit across arbitrarily rechunked Arrow batches, publish only
source-proven safe frontiers, lower complete-image and key-only changes into the existing typed CDC
batch contract, and wait for a safe boundary when package rotation or command termination requests
closure.

The first archetype covers committed PostgreSQL/MySQL transaction boundaries and MongoDB terminal
event-prefix resume tokens without branching on source kind in generic runtime code.

## Non-goals

- real PostgreSQL logical replication, MySQL binlog, or MongoDB change-stream adapters;
- a continuously resident run loop, daemon, leader election, or a `resume` command;
- a first-party destination `cdc_apply` implementation;
- protocol-specific connection, decoding, prerequisite, snapshot, or retention-gap behavior;
- parallel log partitions, cross-database transactions, or undocumented MongoDB transaction
  grouping;
- compatibility readers, migrations, aliases, or legacy artifact support.

## Acceptance criteria

- [ ] One neutral typed archetype represents ordered committed-log transactions and opaque ordered
      event prefixes without database-name branches or a universal wire-protocol trait.
- [ ] A row/byte/time/termination closure request received inside a settlement unit waits for the
      proven terminal boundary, records exact phase-local overshoot, and admits no later unit.
- [ ] No safe frontier, destination mutation, receipt, or checkpoint authority can be produced for
      a partially observed transaction or event prefix; restart retains the prior committed
      checkpoint.
- [ ] Multi-batch transactions remain bounded by the compiled `maximum_transaction_bytes`; the
      resolved host spill budget is the hard ceiling, a resource may only lower it, and exceeding
      the effective limit fails before checkpoint advance.
- [ ] Every admitted CDC batch has validated typed operation and exact source position metadata;
      insert/update require complete rows, delete requires the exact key-only shape, and successful
      finalization delegates to the A1.5 canonical keyed-effect reducer.
- [ ] Source-position scope, regression, unsupported event, inconsistent terminal position, and
      impossible aggregation failures retain narrow typed provenance and fail before publication.
- [ ] A deterministic synthetic source proves committed-frontier and Mongo event-prefix behavior
      under randomized Arrow rechunking, cadence boundaries, cancellation/failure injection,
      within/over-limit settlement units, and `jobs` invariance.
- [ ] A finite-drain conformance certificate proves package finalization, exact receipt settlement,
      checkpoint advancement, and crash recovery without introducing a second runtime lifecycle.
- [ ] Focused affected-package tests, formatting, check, and strict affected-package Clippy pass.

## References

- `.10x/specs/cdc-log-source-foundation.md`
- `.10x/specs/cdc-source-position-artifacts.md`
- `.10x/specs/package-keyed-delete-effects.md`
- `.10x/specs/stream-epochs-watermarks.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/decisions/kernel-owned-stream-epoch-policy.md`
- `.10x/knowledge/developer-build-duckdb-linkage.md`

## Assumptions

- Record-backed: `ExecutionExtent::Drain`, `DrainEpochController`, package finalization,
  destination receipts, and checkpoint commit remain the sole runtime and durable advance
  authorities.
- User-ratified: the resolved host spill budget is the hard maximum for one PostgreSQL/MySQL
  transaction; a resource may lower but never raise it, and no kernel numeric default exists.
- Record-backed: MongoDB publishes an adapter-proven ordered event-prefix terminal token and is not
  grouped into undocumented source transactions.
- Record-backed: initial CDC execution has one ordered source partition; concurrency configuration
  cannot change event order or package identity.

## Journal

- 2026-08-07: Opened the executable A2 owner after A1 source-position authority and A1.5
  package-native keyed effects closed. The active CDC foundation spec contains the complete
  semantics; no behavior-changing assumption remains for this runtime boundary.

- 2026-08-07: Status `open` → `active`. Surveyed the seam before designing. Findings that shape the
  implementation:
  - `DrainEpochController` (`crates/cdf-runtime/src/drain_epoch.rs:106`) already accepts *only*
    canonical safe frontiers through `observe_safe_frontier`, and after it returns
    `DrainEpochDecision::Close` it rejects further observations until `acknowledge_settlement`
    proves that exact frontier settled. The "no later unit after closure" invariant is therefore
    already structural at the controller; the archetype must not re-implement it, only respect it.
  - Because the controller evaluates closure triggers *at observation time*, and the archetype
    observes only at proven unit boundaries, "wait for the boundary" needs no new gate — a trigger
    physically cannot fire mid-unit. What the archetype must add is the *overshoot record*: the
    controller never sees the mid-unit crossing, so it cannot report it.
  - `CdcMetadata` (`crates/cdf-kernel/src/batch.rs:679`) already carries
    `{ operation, position }` and validates one homogeneous operation per batch against the batch
    source position. A2 consumes this rather than redefining it.
  - `maximum_transaction_bytes` exists in the specification only; there is no code for it. A2
    introduces it, bounded by the existing `SpillBudgetCoordinator`/`FixedSpillBudget` authority in
    `cdf-runtime`, with no kernel numeric default.

  Design: a new focused module in `cdf-runtime` beside `drain_epoch.rs`, following
  `.10x/knowledge/rust-crate-organization.md`. Neutrality is carried by a
  `SettlementUnitKind { CommittedTransaction, EventPrefix }` discriminant plus A1's typed positions,
  so no generic runtime code names `postgres`, `mysql`, or `mongodb` and no universal wire-protocol
  trait appears. The archetype owns: begin-unit, admit-batch (validating `CdcMetadata`, accounting
  rows/bytes, enforcing the byte ceiling, and marking a threshold crossing), complete-unit
  (publishing exactly one `DrainSafeFrontierObservation`), and request-closure. Publishing a
  frontier without a completed unit, and beginning a unit after closure is requested, are both made
  unrepresentable rather than merely checked.

- 2026-08-07: Increment 1 landed the core archetype with all affected-package gates green. See
  Evidence for the per-criterion status table; five of nine criteria remain partial or unstarted,
  so the ticket stays `active`. Remaining work, in dependency order: wire the ceiling to a real
  `SpillBudgetCoordinator` snapshot and the compiled plan; delegate finalization to the A1.5
  keyed-effect reducer; drive the archetype through `DrainEpochController` for package/receipt/
  checkpoint integration and crash recovery; add the deterministic synthetic source with
  cancellation/failure injection and `jobs` invariance; then the finite-drain conformance
  certificate.

## Blockers

**AC8 is blocked on the workspace baseline, discovered 2026-08-07.**
`.10x/tickets/2026-08-07-workspace-suite-failing-and-flaky-baseline.md` records 33 pre-existing
failures on `main`. Two clusters sit directly under AC8's subject matter:

- 10 `package_replay` tests, including every crash-matrix helper-process case — AC8 must prove
  "package finalization, exact receipt settlement, checkpoint advancement, and crash recovery";
- `tests::determinism::package_identity_is_invariant_to_source_batch_rechunking`, a determinism
  invariant the settlement archetype's rechunk guarantees build on.

A finite-drain conformance certificate written on top of a red package-replay suite would not be
evidence — it would be a claim resting on unverified ground, and the flakiness confirmed in that
ticket means a green result could not be trusted either. AC8 therefore waits for classification of
those two clusters, not for more A2 implementation.

`jobs` invariance remains unblocked and is ordinary remaining work.

The former AC4 authoring blocker was ratified on 2026-08-07 and implemented; the record of it is kept
below because it explains the grammar choice.

**AC4 authoring half — RESOLVED 2026-08-07.** The CDC foundation spec requires
`maximum_transaction_bytes` to be "a mandatory compiled CDC capability" that "a project/resource MAY
lower". The runtime half is implemented and proven (`TransactionByteCeiling::from_spill_budget`),
but `.10x/specs/cdc-resource-authoring-and-continuous-run.md` defines `mode`, `bootstrap`,
`DISPOSITION CDC_APPLY`, and the `DELETE` clause and **does not define any surface through which a
resource declares this value**. No other active spec does either.

Where it is declared is a user-visible semantic choice, so it is not filled here. Candidates: a
driver-owned `upstream(...)` argument, a resource execution clause in the SQL envelope, a
project-level `cdf.toml` setting, or deliberately not authorable in v1 so the host spill budget is
the only authority. The runtime already behaves correctly under the last option, which is why this
blocks only the authoring half of AC4 and nothing else in the ticket.

The remaining criteria — crash-recovery proof, `jobs` invariance, and the AC8 conformance
certificate — are unblocked and are ordinary remaining work.

## Evidence

### Increment 1 — core archetype (2026-08-07)

Added `crates/cdf-runtime/src/cdc_log_source.rs` exporting `CdcLogSourceRuntime`,
`SettlementUnitKind`, `TransactionByteCeiling`, `SettlementClosureThresholds`,
`SettlementClosureCause`, `SettlementOvershoot`, and `CompletedSettlementUnit`.

```text
cargo fmt --all -- --check                                    exit 0
cargo check -p cdf-runtime --all-targets --locked              ok
cargo clippy -p cdf-runtime --all-targets --locked -- -D warnings   exit 0
cargo test -p cdf-runtime --locked --no-fail-fast              170 passed, 0 failed, 2 ignored
                                                               + 7 passed + 1 passed
```

The 20 new tests in `cdc_log_source::tests` are the observation; what each proves, and its limits:

| Acceptance criterion | Status | Evidence and limit |
|---|---|---|
| 1. Neutral typed archetype, no database branches | **Supported** | Both `CommittedTransaction` and `EventPrefix` drive the same code path; the module names no database in any control-flow position. Limit: proven by construction and review, not by a lint. |
| 2. Closure inside a unit waits, records exact overshoot, admits no later unit | **Supported** | `closure_requested_mid_unit_waits_for_the_boundary_and_records_exact_overshoot` asserts exact requested-at and overshoot counts; `no_later_unit_is_admitted_once_closure_was_requested` asserts the seal. Limit: unit-level; not yet driven through `DrainEpochController`. |
| 3. No frontier/mutation/receipt/checkpoint for a partial unit; restart retains prior checkpoint | **Partial** | `abandoned_unit_publishes_nothing_and_retains_prior_authority` proves the archetype publishes nothing. The restart/checkpoint half is **not proven** — it needs package/receipt/checkpoint integration. |
| 4. Bounded by `maximum_transaction_bytes`, host spill is the hard ceiling, resource may only lower | **Partial** | Four ceiling tests prove resolution and fail-closed admission. **Not yet wired** into the compiled plan or to a real `SpillBudgetCoordinator` snapshot. |
| 5. Validated typed operation and exact position; finalization delegates to the A1.5 reducer | **Partial** | Operation/position validation and scope checks are proven. Delegation to the keyed-effect reducer is **not implemented**. |
| 6. Narrow typed provenance for scope, regression, and aggregation failures | **Partial** | Scope mismatch, position drift, zero-row, and committed-log regression are typed and asserted. Unsupported-event and inconsistent-terminal-position paths are **not yet covered**. |
| 7. Deterministic synthetic source under randomized rechunking, cadence, cancellation, `jobs` invariance | **Partial** | `arbitrary_arrow_rechunking_yields_an_identical_settled_unit` covers rechunking across three splits; the elapsed-cadence test covers one cadence boundary. Cancellation/failure injection, a full synthetic source, and `jobs` invariance are **not yet written**. |
| 8. Finite-drain conformance certificate | **Not started** | — |
| 9. Focused tests, fmt, check, strict Clippy | **Supported** | Commands and results above. |

**A defect the tests caught, recorded because it would have been invisible in production.** The
first implementation of the mid-unit threshold check compared only the current batch against the
epoch limit, ignoring rows already accumulated in the open unit. A transaction crossing a threshold
gradually would have settled with `overshoot: None` — silently under-reporting exactly the quantity
this ticket exists to report. The projection now combines settled epoch counters with the open
unit's counters. Fixed in the same increment; `closure_requested_mid_unit_waits_for_the_boundary_and_records_exact_overshoot`
is the regression guard.

**Limits of this evidence.** These are in-process unit tests over the archetype's own state machine.
They prove its transition rules and arithmetic. They do **not** prove integration with
`DrainEpochController`, package finalization, receipts, checkpoints, or crash recovery, and no live
database was involved. A2 is not closeable on this evidence.

### Increment 2 — controller bridge, keyed-effect delegation, synthetic model (2026-08-07)

```text
cargo fmt --all -- --check                                          exit 0
cargo clippy -p cdf-runtime --all-targets --locked -- -D warnings   exit 0
cargo test -p cdf-runtime --locked --no-fail-fast                   182 passed, 0 failed, 2 ignored
                                                                    + 7 passed + 1 passed
```

32 tests in `cdc_log_source::tests` (up from 20). What changed per criterion:

- **AC3 → Supported for the runtime half.** `CompletedSettlementUnit::into_observation` admits
  exactly one position per unit, so the controller is never offered an interior position to close
  on. `controller_closes_only_at_the_proven_transaction_boundary` drives a transaction that crosses
  a 10-row cadence at row 6 and asserts the real `DrainEpochController` closes on the commit LSN.
  `abandoned_partial_unit_never_reaches_the_controller` asserts the prior committed frontier
  survives. Still **not proven**: crash recovery across a process restart.
- **AC4 → Supported at the runtime boundary.** `TransactionByteCeiling::from_spill_budget` resolves
  against a live `SpillBudgetCoordinator`; `ceiling_resolves_from_the_live_spill_budget` and
  `within_limit_settles_and_over_limit_fails_without_advancing` prove lower-only and fail-closed.
  Still **not wired** into the compiled plan — no resource can yet declare the value.
- **AC5 → Supported for order authority.** `keyed_effect_input_order` derives
  `KeyedEffectInputOrder::SourceProtocol` from the terminal position's protocol identity, and
  `WINNER_POLICY` pins last-change-wins, which is what distinguishes CDC from unordered merge.
  `reduction_scope_is_stable_across_the_settlement_unit` proves the scope does not drift.
  Still **not implemented**: physically constructing package segments through the reducer.
- **AC6 → Supported.** `reject_unsupported_event` gives adapters one typed path for
  truncate/DDL/snapshot events; it abandons the unit so nothing partial can publish. Regression,
  scope mismatch, position drift, and zero-row admission remain typed and asserted.
- **AC7 → Partial.** A deterministic seeded model (`Lcg`, no wall clock, no `rand`) replays 12
  transactions across five seeds and six chunk schedules and asserts the settled frontier sequence
  is byte-identical — a failing schedule is reproducible from its seed alone. Cancellation is
  injected at every transaction index and proven to publish nothing.
  Still **not covered**: `jobs` invariance, which is currently structural rather than tested (the
  archetype takes no concurrency input and owns one ordered stream), and failure injection at the
  package/receipt/checkpoint transitions rather than only at admission.

### Increment 3 — removing a duplicated authority (2026-08-07)

Inspection before extending AC5 found that **A1.5 already implemented the physical CDC reduction**:
`crates/cdf-engine/src/execution/orchestration.rs:6717` builds
`KeyedEffectInputOrder::SourceProtocol` under `KeyedEffectWinnerPolicy::Last` whenever the
disposition is `CdcApply`, consuming a `(protocol, scope_sha256)` tuple — exactly what
`SourcePosition::cdc_protocol_order_identity` returns.

The archetype's first cut of `keyed_effect_input_order()` therefore constructed the same authority a
second time. That is the failure mode the CDC foundation spec names directly — "MUST NOT retain
separate pattern matches with subtly different log semantics". The two sites agreed on the day they
were written and had no shared test: the archetype used `KEYED_EFFECT_ORDER_VERSION` while the
engine hardcodes `version: 1`, so a future bump of the constant would have silently desynchronised
package reduction evidence from the runtime's claim about it.

Replaced with `CompletedSettlementUnit::cdc_order_identity()`, which returns the identity tuple and
feeds the engine's existing single construction site. `WINNER_POLICY` remains as a documented
constant because last-change-wins is what distinguishes CDC from unordered merge.

Gates after the change: 32 tests pass, `clippy -D warnings` exit 0, `fmt --check` exit 0.

**Note, not fixed — outside this ticket's circle.** The engine's `version: 1` literal at
`orchestration.rs:6719` should be `KEYED_EFFECT_ORDER_VERSION`. It is correct today and changing it
is a one-line edit, but it belongs to whoever owns that file rather than to A2's diff. Recorded here
so the hazard has an owner.

### Increment 4 — restart resume (2026-08-07)

`restart_after_an_unobserved_commit_resumes_from_the_prior_checkpoint` settles one transaction,
spools a second whose commit is never observed, drops both the controller and the archetype, then
rebuilds them from the recovered frontier via `bind_initial_committed_state`. The rebuilt controller
resumes at the settled position, the rebuilt archetype reports zero completed units, and replaying
the interrupted transaction settles normally.

```text
cargo fmt --all -- --check                                          exit 0
cargo clippy -p cdf-runtime --all-targets --locked -- -D warnings   exit 0
cargo test -p cdf-runtime --locked --no-fail-fast                   183 passed, 0 failed, 2 ignored
                                                                    + 7 passed + 1 passed
```

**Limit, stated precisely.** This proves the runtime boundary reconstructs correctly from a recovered
frontier. It does **not** exercise the SQLite checkpoint store, the package workspace, receipt
recovery, or a real process kill, so it does not discharge acceptance scenarios 2 and 3 of the CDC
foundation spec — those need the chaos layer, and they belong to AC8.

### Increment 5 — compiled `maximum_transaction_bytes` authority (2026-08-07)

The user ratified the execution clause as the declaration site. Implemented end to end:

- `StreamEpochPolicy` gains `maximum_transaction_bytes: Option<u64>`, rejecting a declared zero;
  `STREAM_EPOCH_POLICY_VERSION` bumped 1 → 2 as a coherent artifact replacement with no compatibility
  reader, per `.10x/knowledge/pre-production-current-only-policy.md`.
- `ExecutionDeclaration::Drain` and the declarative compiler thread the value into the compiled plan.
- Grammar: an optional trailing `MAXIMUM TRANSACTION BYTES n` member of `EXECUTION DRAIN`. It follows
  the existing positional keyword vocabulary (`PACKAGE BYTES n`) rather than the `key => value` form
  sketched during ratification, which would have clashed with the strict member grammar. Absence is
  distinct from a declared value.
- `TransactionByteCeiling::from_extent` resolves the compiled value against live host spill
  authority — the production path.
- `.10x/specs/cdc-resource-authoring-and-continuous-run.md` gained a "Settlement-unit byte ceiling"
  section defining the surface and its rules.

**AC4 is now fully supported.**

#### Regression discipline for the version bump

Bumping a serialized artifact version risks breaking golden hashes across the workspace, so the
change was verified by differential sweep rather than by inspection.

```text
baseline (main, changes stashed):  36 failing tests
with changes, before fix:          36 failing — 1 NEW, 1 flaky-flip
with changes, after fix:           33 failing — 0 NEW vs baseline
```

The one genuine regression was mine: `execution_extent::tests::
every_versioned_nested_artifact_rejects_invalid_deserialization` hardcoded `2` as its
"invalid version" sentinel, which the bump silently made **valid** — converting a rejection test into
a vacuous assertion. Fixed by deriving each sentinel from its own constant
(`STREAM_EPOCH_POLICY_VERSION + 1`, `EPOCH_FRONTIER_VERSION + 1`) so it cannot rot on the next bump.
This is the same magic-number hazard recorded against `orchestration.rs:6719` in increment 3.

Gates: `fmt --check` exit 0; strict Clippy exit 0 for `cdf-kernel`, `cdf-runtime`, `cdf-declarative`,
`cdf-project`, `cdf-engine`.

**The 33 remaining workspace failures are pre-existing on `main` and are NOT caused by this ticket.**
They cluster in schema promotion, package replay, DuckDB doctor drift, and live-run conformance, and
include `tests::determinism::package_identity_is_invariant_to_source_batch_rechunking`. Three tests
differed between sweeps in both directions, so that suite contains flaky live-database tests. This
is a discovered condition with its own owner:
`.10x/tickets/2026-08-07-workspace-suite-failing-and-flaky-baseline.md`.

**Remaining for closure:** `jobs` invariance, and the AC8 finite-drain conformance certificate
including real crash recovery. AC5's physical construction is **already satisfied by A1.5** — the
archetype now feeds it rather than duplicating it, which is a correction to increment 2's status
table.

## Review

Pending implementation and the program-level review barrier.

## Retrospective

Pending implementation.
