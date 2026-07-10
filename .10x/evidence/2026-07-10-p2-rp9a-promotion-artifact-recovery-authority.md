Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp9a-promotion-artifact-recovery-authority.md, .10x/specs/schema-promotion-corrections.md

# RP9A promotion artifact recovery authority evidence

## What was observed

- RP5 now exposes one public canonical promotion-id recomputation and executable-plan validation path. RP9 staging and hydration call that path; it verifies the exact old lock bytes/hash, typed version-3 snapshot lineage, selected path/type/package/receipt associations, destination sheet authority, unique locked correction-strategy selection, and canonical promotion id.
- Snapshot and plan staging use one create-or-verify helper with temporary-file sync and create-only hard-link installation. Equal bytes are idempotent; conflicting bytes are retained and rejected.
- Pre-package source inventory is deterministic and fails on malformed entries or duplicate package hashes. Source packages pass ordinary replay-preimage validation; exact recorded receipts must cover the package/state/schema/disposition/segment authority and verify against the live resolved destination protocol before row extraction.
- Once a correction package manifest exists, recovery loads it before and without source-package enumeration. Hydration verifies the package manifest, typed correction artifact, exact operation segment, ordinary replay inputs, promotion scope, destination commit preimage, and any stored correction receipt against a reconstructed correction plan plus live destination verification.
- Correction packages record the exact `promotion_scope(resource)`. A command fixture using `contract = "events-contract"` produced a replay-valid `ScopeKey::SchemaContract(events-contract)` state delta instead of falling back to the resource id.
- Every post-package crash boundary recovered successfully after `.cdf/packages/pkg-promote-source` was deleted. The staged-only boundary retained its source because packaging had not yet established replacement recovery authority.
- A staged plan whose target package set was changed and whose promotion id was recomputed was rejected before destination, checkpoint, or lock mutation. A correction package whose identity artifact bytes were changed was also rejected before mutation after the original source package was removed.
- Canonically rebuilt correction packages with valid manifests/preimages but either a missing operation or a substituted value are rejected against the create-only target authority after original sources are removed, before destination/checkpoint/lock/publication mutation.
- A direct API caller supplying a divergent in-memory lock alongside correct staged lock bytes is rejected before staging; replacement lock bytes are derived from the exact staged authority. Existing publication recovery re-verifies loaded packages, live receipts, committed checkpoints, and the exact sorted target tuple before returning complete.
- The earlier Parquet command fixture forged a DuckDB receipt's destination/id without Parquet state; strict live source-receipt verification rejected it. The fixture was corrected to commit the source package through `ParquetDestination`, after which the Parquet correction-sidecar command passed with a real verifiable original receipt.

## Procedure and results

Commands run from the repository root:

```text
cargo check -p cdf-project --lib
```

Passed after the package hydration/source-index signature transition.

```text
cargo test -p cdf-project promotion::tests:: -- --nocapture
```

Passed 16/16 promotion-filtered tests, including the two new runtime inventory/no-clobber tests and the existing RP5 promotion identity/snapshot suite.

```text
cargo test -p cdf-project runtime::promotion::tests:: -- --nocapture
```

Passed 2/2: malformed/duplicate source inventory rejection and create-or-verify conflict preservation.

```text
cargo test -p cdf-project --lib
```

Passed 159/159 after the final stored-receipt verification changes, including live local Postgres-backed project tests.

```text
cargo test -p cdf-cli schema_promote_execute -- --nocapture
```

Passed 3/3: DuckDB execution with custom-contract replay inspection, source-deletion recovery across every persisted boundary, and Parquet correction-sidecar execution from a real source receipt.

```text
cargo test -p cdf-cli schema_promote_rejects_tampered_staged_and_correction_authority_before_mutation -- --nocapture
```

Passed 1/1 across both tamper cases.

```text
cargo clippy -p cdf-project -p cdf-cli --lib --tests -- -D warnings
cargo fmt --check
git diff --check
```

All passed after final receipt-verification and formatting changes.

Post-review repair verification:

```text
cargo test -p cdf-project runtime::promotion::tests:: -- --nocapture
# 3/3
cargo test -p cdf-cli --lib schema_promote_ -- --nocapture
# 7/7
cargo test -p cdf-project --lib
# 163/163
cargo test -p cdf-cli --lib
# 255/255
cargo clippy -p cdf-project -p cdf-cli --lib --tests -- -D warnings
cargo fmt --check
git diff --check
# all pass
```

## What this supports

This supports RP9A's bounded claim that staged promotion/correction artifacts are self-authenticating, no-clobber authority and that completed correction packages recover without original residual source packages. It also supports strict pre-package source/receipt selection, exact custom-contract checkpoint scope, and ordinary replay-preimage coherence.

## Limits

- The source-deletion crash matrix is currently one-target; multi-target command conformance belongs to RP9C.
- The suite proves live original-receipt verification through successful DuckDB and Parquet commits and the rejected forged-Parquet fixture. It does not add a separate synthetic adapter that returns `verified = false`.
- Checkpoint/publication atomic fencing is explicitly outside RP9A and remains owned by RP9B.
- GC classification and Parquet identifier policy are outside this ticket.
