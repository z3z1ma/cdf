Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-a7-schema-pin-show-diff-cli.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/decisions/data-onramp-schema-discovery-reconciliation.md

# P2 WS-A8 durable auto-pin and no-pin inspection

## Scope

Close the current durability gap in first-use discovery: when supported discover-mode resources auto-pin during `cdf plan` or `cdf run`, create or update the semantic lockfile entry as well as the snapshot artifact. Add a no-write inspection mode for plan-time discovery so an operator can inspect the fresh schema without pinning it.

This ticket owns the smallest complete slice:

- Reuse the existing schema snapshot and lockfile helpers rather than introducing a second pin format.
- Auto-pin supported local/HTTPS single-file Parquet, Postgres-table, and REST resources into `.cdf/schemas/` and `cdf.lock` deterministically.
- Add `--no-pin` to `cdf plan` and `cdf explain`; the flag MUST probe and render the discovered schema/plan without writing snapshots, lockfiles, packages, destinations, checkpoints, or run-ledger state.
- `cdf run` remains package-producing and MUST NOT accept an unpinned no-write execution path.
- Human and JSON reports MUST distinguish added, unchanged, refreshed, and inspection-only snapshot outcomes.

## Acceptance criteria

- A project without `cdf.lock` gets a valid lockfile/schema entry on first supported auto-pin.
- A project with `cdf.lock` updates only the selected resource entry and preserves unrelated semantic locks.
- Repeating auto-pin over unchanged source identity is byte-stable and reports unchanged.
- `cdf plan --no-pin <resource>` and `cdf explain --no-pin <resource>` perform bounded discovery and leave the project tree unchanged.
- Secret-backed Postgres/REST discovery does not serialize or render resolved secret values.
- Existing explicit `cdf schema pin|show|diff` behavior remains compatible.

## Evidence expectations

Parser/help tests, local Parquet and REST deterministic fixtures, Postgres coverage when the existing harness is available, no-write tree snapshots, lockfile preservation tests, redaction tests, and the `QUALITY.md` public-API/security/test profiles appropriate to the change set.

## Explicit exclusions

This ticket does not implement `SchemaSource::Hints`, new file-format probes, HTTP multi-file enumeration, cloud transports, or changes to schema reconciliation semantics.

## Progress and notes

- 2026-07-09: Opened after a read-only P2 audit found that first-use auto-pin writes snapshots but does not create/update `cdf.lock`, while the directive's `--no-pin` inspection surface is absent.
- 2026-07-09: Implemented deterministic first-use lockfile creation and selected-resource lockfile upsert through the existing semantic lockfile generator; unrelated existing resource, dependency, and destination locks are preserved. Snapshot and lockfile files are skipped when their canonical bytes are unchanged.
- 2026-07-09: Added `cdf plan --no-pin` and `cdf explain --no-pin`. Both run the same bounded generic discovery and planning path in memory while writing no snapshot, lockfile, package, destination, checkpoint, or ledger artifact. `cdf run` does not expose the flag.
- 2026-07-09: Added additive human/JSON schema-snapshot action reporting to plan/explain/run with `added`, `unchanged`, `refreshed`, and `inspection_only` outcomes; explicit `cdf schema pin` now creates a missing semantic lockfile and uses the same byte-stable write behavior.
- 2026-07-09: Added focused local Parquet tree-snapshot, byte-stability, unrelated-lock preservation, refreshed-schema, human/JSON, and parser/help tests; bounded HTTP Parquet no-pin/unchanged coverage; REST no-write/redaction coverage; and live Postgres no-write/redaction coverage in the existing harness.
- 2026-07-09: Verification: `cargo test -p cdf-project --locked --no-fail-fast` passed 111/111; `cargo test -p cdf-cli --locked --no-fail-fast` passed all 222 library tests and the `doctor_env` integration test. The same command's doctest phase failed because concurrently rebuilt dependency artifacts disappeared from rustdoc (`can't find crate cdf_project/cdf_declarative/cdf_engine`), not from an A8 assertion or compile error. Focused `no_pin`, local auto-pin stability, missing-lock schema pin, bounded HTTP, REST, and live Postgres tests all passed independently. `cargo fmt --all -- --check` passed. Targeted all-feature clippy reached the concurrently edited B6 reader and stopped on two `clippy::useless_asref` findings in `crates/cdf-formats/src/readers.rs`; no A8 lint finding was emitted. `git diff --check` passed for the A8 files.
- 2026-07-09: Closure repair corrected a critical anti-convergence violation: ordinary `plan`, `preview`, and `run` now hydrate the selected resource from the verified snapshot referenced by `cdf.lock` and do not live-probe or silently refresh it. Snapshot artifacts reconstruct their Arrow schema losslessly, validate artifact version/hash/path/metadata references, and fail closed when missing or inconsistent. Fresh probing of pinned resources remains restricted to explicit `schema pin`, `schema diff`, and `plan|explain --no-pin`.
- 2026-07-09: Replaced the prior refreshed-on-plan assertion with negative drift proof: after changing the local Parquet physical schema, ordinary plan/preview/run retain the original locked hash and create no snapshot or lockfile changes; `--no-pin` observes a distinct fresh hash without writes; explicit `schema pin` alone reports `refreshed` and preserves the unrelated resource lock. Added missing-artifact fail-closed coverage and adjusted REST request-count evidence to prove preview/run no longer repeat discovery. Focused local drift, missing-artifact, REST, live Postgres, Arrow snapshot round-trip, and all-target compile checks passed.
- 2026-07-09: Closure-repair verification passed `cargo test -p cdf-cli --lib --locked` (223/223), `cargo test -p cdf-project --lib --locked` (111/111), `cargo check -p cdf-project -p cdf-cli --all-targets --locked`, scoped format/diff checks, and `cargo clippy -p cdf-project -p cdf-cli --all-targets --locked --no-deps -- -D warnings`. Dependency-inclusive clippy remained temporarily blocked only by four concurrent B6 `nonminimal_bool` findings in `crates/cdf-formats/src/readers.rs`; no A8 lint finding was emitted.
- 2026-07-09: Hardened the repair so `--no-pin` can still inspect a fresh schema when the locked artifact is missing, without recreating it or changing the lock; only ordinary package-facing commands require hydration. Added a closed-vocabulary Arrow snapshot reconstruction round-trip covering decimals, timezone timestamps, intervals, view types, nested map/list, union, dictionary, and run-end encoding; its focused test passed.
- 2026-07-09: Closed after integration evidence `.10x/evidence/2026-07-09-p2-a8-b6-i3-integration.md` and passing adversarial review `.10x/reviews/2026-07-09-p2-a8-b6-i3-integration-review.md`. Retrospective: the critical failure mode was hydrating lock state after deciding to probe; future discovery surfaces must establish pinned authority before any source contact.

## Blockers

None. The active schema-intelligence specification already ratifies deterministic auto-pin and no-write inspection semantics.
