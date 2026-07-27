Status: active
Created: 2026-07-26
Updated: 2026-07-26

# Product integration and closure gate

## Purpose

The highest-cost recent regressions passed crate-local tests because discovery, compilation,
external task authority, packaging, destination ingress, and checkpointing were tested with
synthetic identities that never changed across the real lifecycle. This record defines the
minimum product-level proof and ticket-state discipline for core tranches.

## Tests are evidence within their assertions

- A unit test proves the local contract it asserts.
- A crate integration test proves the composition it instantiates.
- A conformance scenario proves the named archetype/path.
- A product smoke proves the actual CLI/compiler/runtime/destination lifecycle it executes.
- A benchmark proves performance only for its recorded cell.
- `cargo check` proves type checking, not runtime behavior.
- Clippy proves selected static properties, not end-to-end correctness.

Do not relabel one form as another.

## Mandatory product smoke matrix

After a core tranche changes plan identity, source discovery/inventory, external task authority,
package lifecycle, destination ingress, receipt/checkpoint state, or generic orchestration, run the
smallest matrix that crosses the real boundaries:

1. Local Parquet → DuckDB.
2. HTTPS Parquet → DuckDB using a recorded/stable fixture when possible.
3. Local multi-file manifest first run → identical no-op rerun.
4. Iceberg/catalog-task source → DuckDB when credentials/fixture are available.
5. Package verify and replay.
6. Preview/run parity on the changed source archetype.
7. One non-DuckDB destination, normally Parquet.

If a cell depends on live public internet or cloud credentials, run the recorded fixture in the
fast gate and the live cell in a deliberate smoke tier. Record the environmental limit; do not
silently skip and claim the whole matrix.

## Identity-transition regression pattern

The file-inventory regression demonstrated:

```text
un-pinned discovery plan identity
→ reusable external inventory
→ schema pin/effective-plan compilation changes full plan hash
→ execution retrieves correctly reusable inventory
→ wrong embedded full-plan identity rejects it
```

The repair separated:

- discovery binding identity for reusable inventory; and
- complete execution-plan identity for partition task authority.

Regression fixtures must contain the transition. Constructing both sides with one synthetic hash
encodes the bug away.

Whenever a new identity appears:

- give semantically different identities distinct newtypes;
- construct them through one required compiler bundle;
- remove optional/string setters that can swap them;
- test lifecycle transitions where one identity changes and another remains stable;
- include serialized/reloaded authority when production does.

## Representative fixture law

Fixtures should exercise:

- more than one file/partition/segment;
- unopened/unseen partitions at pin time;
- non-monotonic task completion;
- real schema metadata differences where reconciliation matters;
- weak and strong remote generation identity;
- destination rollback/retry where supported;
- no-op incremental state;
- enough rows/width to cross batch/segment boundaries.

A one-file, one-batch, identical-hash fixture is insufficient for a multi-file lifecycle.

## Planned versus observed facts

Product output and benchmark reports must not conflate:

- planned selected tasks;
- logical bytes represented;
- physical source bytes transferred;
- package bytes written;
- destination bytes materialized.

Runtime metrics own physical I/O. Reopening every task to reconstruct a byte counter is neither a
correct metric nor a scalable integration test.

## Quality selection

Use `QUALITY.md` to select the smallest sufficient profile. Typical core tranche order:

1. focused unit/integration tests for touched crates;
2. focused conformance/product regression;
3. format and clippy for changed workspace scope;
4. the mandatory product smoke cells affected;
5. performance cell if the hot path/default can change;
6. broader/deep/static/security gates at the owning program boundary.

Avoid N workers running the same whole-workspace suite concurrently. One coordinator owns heavy
checks and routes failures to the responsible change.

Do not run one-TiB stress or heavyweight static analysis on a developer laptop merely for
reassurance. Use the correct EC2/slow-tier protocol.

## Error behavior is product behavior

A core smoke must inspect more than exit code:

- error class/code is correct;
- command wording names the command actually run;
- field/source/destination/file identity is present;
- both sides of a type mismatch are named;
- remediation describes a real operator action;
- secrets and signed access locations are redacted;
- no internal “fix project or retry” catch-all replaces a known remedy.

Environment and I/O failures are not `Internal` unless they prove a CDF invariant bug.

## Ticket closure

A ticket closes only when:

- every acceptance criterion maps to journaled evidence and limits;
- fresh adversarial review passes or residual risk is explicitly accepted;
- active specs, source, assertions, docs/generated artifacts, and ticket status agree;
- performance-sensitive defaults have evidence or remain opt-in;
- superseded code/tests/dependencies are removed;
- discoveries have owners or a no-action rationale;
- retrospective is complete;
- parent/dependency/cross-reference state is repaired.

Move the ticket to `done/` only after those gates. Cancel it when the outcome is invalid,
superseded, unvaluable, or deliberately parked; record why and the reactivation trigger.

Do not leave a ticket open as a vision reminder. Do not mark it done because the current thread is
ending.

## Review efficiency

A reviewer should attempt to falsify the entire bounded claim once, not drip minor observations
across many telephone-game rounds. The review brief should explicitly cover:

- architecture/extension boundary;
- correctness and identity lifecycle;
- performance/memory/I/O;
- cancellation/failure/rollback;
- tests against spec scenarios;
- deletion of superseded paths;
- record and generated-artifact coherence.

Critical/significant findings block closure. Minor/nit findings should be fixed in one bounded
cleanup pass or recorded without reopening the architecture indefinitely.

## Generated and published artifact freshness

When evidence, schemas, CLI snapshots, performance envelopes, release docs, or generated JSON
Schema are part of acceptance:

- regenerate through the owning tool;
- inspect the diff for unrelated churn;
- test the actual published artifact where practical;
- never hand-edit one generated fact;
- record when a generated document intentionally lags newer dated evidence.

The 2026-07-26 one-TiB result is represented in both the reconciliation manifest and generated
`docs/performance-envelope.md`, including its repeated-content and physical-I/O limits.

## Commit discipline

Commit coherent, reviewable tranches as work proceeds. Before committing:

```bash
git status --short
git diff --check
git diff --stat
git diff -- <owned paths>
```

Stage only owned files. Never absorb another worker's dirty change. A green commit should include
the ticket/record state that truthfully describes the code it lands.
