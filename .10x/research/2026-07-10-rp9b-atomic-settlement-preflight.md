Status: done
Created: 2026-07-10
Updated: 2026-07-10

# RP9B atomically fenced promotion settlement preflight

## Question

What is the smallest source- and destination-neutral typed store API that can make promotion checkpoint commit and promotion-publication append conditional on the same current schema-contract lease generation inside each authoritative mutation, using SQLite now and transactional remote stores later, without weakening ordinary checkpoint semantics?

## Sources and methods

This was a read-only substrate preflight. It inspected:

- `.10x/tickets/2026-07-10-p2-rp9b-atomically-fenced-promotion-settlement.md`;
- `.10x/specs/schema-promotion-corrections.md`, especially the required checkpoint -> exact lock CAS -> publication order and recovery boundaries;
- `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-independent-review.md`, `.10x/reviews/2026-07-10-p2-rp9-promotion-execution-recovery-gc-review.md`, and `.10x/reviews/2026-07-10-p2-rp9a-promotion-artifact-recovery-independent-review.md`;
- kernel `CheckpointStore`, `ScopeLeaseStore`, `ScopeLease`, `FencingToken`, `PromotionPublicationEvent`, and `PromotionPublicationTarget` types;
- SQLite checkpoint, lease, publication-ledger, migration, transaction, idempotence, and clock implementations;
- current promotion execution ordering and the CLI construction of three independent SQLite handles over the same state path.

No implementation, schema migration, test, build, or other mutating verification was performed.

## Findings

### The required atomicity is two fenced mutations, not one checkpoint-plus-event transaction

The active specification requires target checkpoint commits before exact `cdf.lock` compare-and-swap, and publication only after that filesystem CAS. SQLite cannot and must not pretend to atomically transact the lockfile. A single database transaction containing both checkpoint commit and publication would either publish before lock authority is installed or hold an impractical transaction open across filesystem work, and it would erase the specified recoverable post-lock/pre-event boundary.

RP9B therefore needs two store operations, each of which checks the exact lease scope, owner, fencing token, release state, and expiry inside the same storage transaction as its own authoritative write:

1. fenced promotion checkpoint commit;
2. fenced idempotent promotion-publication append.

The exact lock CAS remains between them and keeps RP4's integrated fence. This completes rather than changes the specified transaction order.

### Smallest generic typed API

Add one kernel-level capability trait whose implementor owns a single transactional consistency domain for checkpoints, leases, and promotion publications. The shape should be equivalent to:

```rust
pub trait PromotionSettlementStore: CheckpointStore + ScopeLeaseStore {
    fn promotion_publication(
        &self,
        promotion_id: &PromotionId,
    ) -> Result<Option<PromotionPublicationEvent>>;

    fn commit_promotion_checkpoint(
        &self,
        lease: &ScopeLease,
        checkpoint_id: &CheckpointId,
        receipt: Receipt,
    ) -> Result<Checkpoint>;

    fn publish_promotion(
        &self,
        lease: &ScopeLease,
        event: PromotionPublicationEvent,
    ) -> Result<PromotionPublicationEvent>;
}
```

Names may follow crate convention, but these are the required semantics. A generic closure/transaction callback would leak storage mechanics, be harder to implement remotely, and permit untyped unrelated writes. Extending ordinary `CheckpointStore::commit` with an optional fence or promotion fields would weaken its general contract and spread promotion semantics into every ordinary caller. Separate source- or destination-specific methods would violate the capability-driven architecture.

`PromotionSettlementStore: CheckpointStore + ScopeLeaseStore` is intentional. It makes acquisition, checkpoint history/proposal, fenced commit, and publication use one typed object and prevents the current construction error where three independently opened handles could accidentally name different databases. Future implementations must provide all operations in one atomic consistency domain; the trait must not have a default implementation that composes unrelated `CheckpointStore`, `ScopeLeaseStore`, and ledger objects after a pre-check.

No new product semantics are needed. The request carries the existing `ScopeLease`, `CheckpointId`, `Receipt`, and `PromotionPublicationEvent` authorities. RP9A owns reconstruction and exact verification of the staged target/package/receipt/checkpoint contract before these methods are called. The store owns only structural validation, ordinary checkpoint invariants, exact-fence evaluation, transactionality, and publication idempotence/conflict.

### Operation contracts

For `commit_promotion_checkpoint`, one write transaction MUST:

1. sample the store clock and verify the lease row matches `lease.scope`, `lease.owner`, and `lease.fencing_token`, is unreleased, and has `expires_at_ms > now`;
2. load the named checkpoint and apply the same receipt/delta, state-version, terminal-state, and committed-head rules as ordinary `CheckpointStore::commit`;
3. commit the checkpoint/head update in that transaction.

The promotion method may preserve the executor's existing retry behavior by returning an already committed checkpoint only when its full receipt equals the supplied receipt. A conflicting committed receipt or any other terminal state remains an error. Ordinary `CheckpointStore::commit` remains unchanged and strict; shared private validation/SQL helpers should prevent semantic drift between the ordinary and fenced paths.

For `publish_promotion`, one write transaction MUST:

1. validate the existing versioned event;
2. return an already stored event only when `same_authority` holds, or reject a conflicting event for the same promotion id;
3. before any insert, sample the store clock and verify the exact current lease row in the same transaction;
4. insert exactly one append-only event and commit.

An already-existing equal event is observation, not advancement, so a retry may return it without requiring a live lease. The insertion path must always be fenced. An already-existing conflicting event must fail regardless of lease state. This preserves current publication idempotence while preventing stale advancement.

The fence check must occur after the SQLite write transaction begins (`BEGIN IMMEDIATE`) and before the protected update/insert. A caller-side `assert_current`, even immediately before the call, is never evidence for the write. The transaction uses the store's clock, not a caller-supplied timestamp. Remote implementations need the equivalent conditional transaction or compare-and-set against the authoritative generation/lease record; a backend unable to do this in one consistency domain cannot implement the trait soundly.

### SQLite substrate

All required tables already coexist in the same state database:

- `cdf_checkpoints` at checkpoint component version 1;
- `cdf_scope_leases` at lease component version 1;
- `cdf_promotion_publications` at run-ledger component version 4.

No data-schema migration is required for the minimal API. Introduce an aggregate SQLite implementation over one `Mutex<Connection>` and one injected `ScopeLeaseClock`, initialize/validate the three existing components on that connection, and implement `CheckpointStore`, `ScopeLeaseStore`, and `PromotionSettlementStore` on it. The existing specialized stores can remain compatibility facades for ordinary callers. Their SQL should be factored into transaction-local private helpers rather than copied with divergent validation.

Promotion execution should receive this one aggregate settlement store instead of separate lease/checkpoint/ledger handles. Ordinary execution may continue using `SqliteCheckpointStore`; its commit behavior and storage format do not change.

The aggregate initializer must tolerate legacy state databases whose three component versions are already current and the established run-ledger v1/v2/v3 -> v4 migration path. A component version bump is warranted only if implementation adds schema objects; the API alone does not justify one. The migration reporter need not add a fourth component because settlement is a transactional capability over the existing three components, not a new persisted schema owner.

### Compatibility constraints

- Kernel additions are additive: a new trait and, only if useful for naming outcomes, new types. Prefer the existing typed arguments and result types; a public request struct is unnecessary at this arity.
- Do not add required methods to `CheckpointStore` or `ScopeLeaseStore`; that would break every downstream implementation and would falsely imply that independent implementations can compose atomically.
- Do not add fields to `ScopeLease`, `Checkpoint`, `Receipt`, or `PromotionPublicationEvent`; their current authority is sufficient and field additions would affect public construction/serialization.
- Existing checkpoint and publication rows remain readable byte-for-byte. Exact event equality continues to ignore `published_at_ms` through the existing `same_authority` rule, while a first insert stores its supplied positive time.
- In-memory conformance needs a single aggregate state object guarded by one mutex. Combining the existing independent in-memory checkpoint and lease stores with a pre-check is not a valid RP9B implementation.
- The project execution request is public. Replacing its three store references is a source-level API change even if project APIs are not yet stable; semver baselines must explicitly record the accepted delta or retain a temporary adapter that cannot be selected for fenced execution.

### Crash and concurrency verification matrix

The minimum deterministic matrix is:

| Boundary/race | Expected observation after reopen or competing call |
|---|---|
| crash before fenced checkpoint transaction | proposed checkpoint remains; no head/receipt change |
| crash after fence read but before checkpoint update | no checkpoint change |
| crash after old head cleared or checkpoint row updated but before transaction commit | rollback restores both old head and proposed checkpoint |
| crash after checkpoint transaction commit but before response | exactly one committed checkpoint; retry with identical receipt returns it; conflicting receipt fails |
| lease expires or is released before checkpoint transaction | no checkpoint/head mutation |
| lease is superseded by a higher token before checkpoint transaction | stale owner cannot mutate; new owner can recover with verified authority |
| same checkpoint, same lease, same receipt raced by two handles | one commit; the other observes identical committed authority, never two heads |
| same checkpoint with conflicting receipts | at most one commits; the other fails without changing the head |
| crash before publication transaction | no event |
| crash after publication fence read/insert but before transaction commit | no event after reopen |
| crash after publication commit but before response | exactly one event; identical retry returns it |
| expired/released/superseded lease before first publication insert | no event |
| same promotion/event raced by current and stale generations | current generation may insert; stale generation cannot; both may subsequently observe the same stored event |
| same promotion id with different event authority | one event wins; every conflicting call fails without overwrite |
| unrelated lease scopes settle concurrently | no cross-scope rejection or authority bleed |
| checkpoint committed, then lock CAS succeeds, then lease expires before publication | no event; new owner verifies RP9A authority and performs the fenced idempotent append |
| destination receipt arrives after lease expiry | receipt remains recoverable evidence; expired executor performs no checkpoint, lock, or publication advancement |

Run the matrix against the aggregate in-memory implementation as kernel/conformance semantics and against two independently opened aggregate SQLite handles sharing one database. SQLite tests need a deterministic injected clock and transaction failpoints at the named pre-commit boundaries. Add a legacy database test that starts at run-ledger v3 with current checkpoint/lease data, opens the aggregate (migrating publication storage to v4), preserves old history/events/leases, and completes both fenced operations. Future remote stores must pass the same behavioral conformance suite, substituting backend-appropriate fault injection.

The higher-level RP9/RP9C suite should retain takeover points during destination settlement, before checkpoint commit, before lock CAS, and before publication insert. RP9B's store tests establish atomic fencing; RP9A/RP9C tests establish that only completely verified promotion authority reaches those methods.

## Conclusions

The implementation substrate is resolved without a new semantic decision. Add one aggregate `PromotionSettlementStore` capability over a single consistency domain, with separately atomic fenced checkpoint-commit and publication-append methods and ordinary checkpoint methods unchanged. SQLite can implement it over its existing tables with no data migration, but promotion must stop composing three independent store handles. The lockfile CAS remains the intentional boundary between the two database mutations, and post-lock recovery is performed by a new lease owner from RP9A-verified authority.

RP9B remains blocked on RP9A's exact persisted recovery authority and must not be activated from this preflight alone.
