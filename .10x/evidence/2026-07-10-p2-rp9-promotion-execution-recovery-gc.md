Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-rp9-promotion-execution-recovery-gc.md, .10x/specs/schema-promotion-corrections.md

# RP9 promotion execution, recovery, and GC evidence

## What was observed

- `cdf schema promote RESOURCE --execute` consumes the exact typed version-3 dry plan, stages the snapshot and plan, builds immutable correction packages, settles canonical destination correction sessions, verifies receipts, commits schema-contract checkpoints, performs fenced exact lock CAS, and appends an idempotent publication record.
- The DuckDB public CLI scenario promoted two retained `/score` residual values by original package/segment/row address. The target gained nullable `score`, both values were written, `_cdf_variant` became null, the checkpoint committed, the lock pinned the version-3 snapshot, and replay returned the same promotion as a no-op.
- The crash matrix passed at all six injected boundaries: after staged artifacts, correction packages, destination receipt, target checkpoint, lock publication, and publication event. Recovery used stored package/receipt/checkpoint/lock evidence. The post-lock/pre-event branch did not call destination settlement.
- Publication records are append-only SQLite authority keyed by promotion id. Same-authority replay returns the first event; conflicting installed-lock authority fails; reopen preserves the record.
- `cdf package gc` reports exact non-null canonical `_cdf_variant` payload bytes and whether a package is the last locally promotable retained package for its resource. It does not change collection policy or infer destination readback.
- CLI help/completions/man pages include `--execute`; human and JSON success reports contain phase, resumed status, target package/receipt/checkpoint, lock/event status, remaining action, and an executable recovery command.

## Procedure

Commands run from the repository root:

```text
cargo test -p cdf-cli schema_promote_execute_commits_correction_checkpoint_lock_and_idempotent_publication -- --nocapture
cargo test -p cdf-cli schema_promote_execute_recovers_every_persisted_crash_boundary -- --nocapture
cargo test -p cdf-cli package_gc_reports_last_locally_promotable_residual_bytes -- --nocapture
cargo test -p cdf-project -p cdf-state-sqlite -p cdf-cli --lib
cargo clippy -p cdf-project -p cdf-cli -p cdf-state-sqlite --all-targets -- -D warnings
cargo run -p cdf-cli --features cli-artifacts --bin cdf-generate-cli-artifacts -- --out-dir crates/cdf-cli/generated --check
cargo fmt --check
```

Observed results:

- affected library suites: `cdf-cli` 251/251, `cdf-project` 157/157, `cdf-state-sqlite` 35/35;
- strict Clippy: pass;
- generated CLI artifact freshness: pass;
- formatting check: pass before the final record-only changes.

## What this supports or challenges

This supports the six-step ordering, immutable correction authority, fenced publication, persisted crash recovery, idempotent event repair, DuckDB in-place execution, and GC local-availability criteria in the RP9 ticket.

Cross-destination probing challenged the assumption that every implemented correction strategy was already executable from the promotion planner. Parquet sidecar settlement exists, but its destination sheet exposes `object-key-component-v1` where promotion needs a column identifier policy. The planner correctly fails before writes. `.10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md` owns that ratified semantic boundary.

## Limits

- The RP9 end-to-end execution test covers DuckDB `in_place_update`; Postgres and Parquet correction protocols retain their RP6/RP8 destination-level conformance evidence.
- Parquet `correction_sidecar` cannot yet pass promotion planning until the separately owned column identifier authority is ratified.
- No live external Postgres service or distributed lease store was added by RP9.

## Subsequent resolution

The separately owned Parquet identifier boundary was ratified and implemented by `.10x/tickets/done/2026-07-10-parquet-promotion-identifier-policy.md`. `.10x/evidence/2026-07-10-parquet-promotion-identifier-policy.md` records a full CLI `correction_sidecar` execution using a real live-verifiable Parquet source receipt. The limitation above remains the historical observation at the time this evidence was recorded, not current implementation state.
