Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-run-ledger-store.md
Verdict: pass

# SQLite run ledger store review

## Target

Review of the run-ledger store slice implemented in `crates/cdf-state-sqlite/src/run_ledger.rs`, exported from `crates/cdf-state-sqlite/src/lib.rs`, and covered by `crates/cdf-state-sqlite/src/tests.rs`.

## Findings

No blocking findings were found.

Minor residual design boundary: the storage API currently provides run creation, event append, per-run event listing, and per-run snapshots. That is enough for `inspect run <id>` and run-id-scoped resume plumbing, but it does not define a broader policy for discovering all interrupted runs. If CLI `cdf resume` needs no-argument discovery, `.10x/tickets/done/2026-07-07-general-run-orchestrator.md` or `.10x/tickets/done/2026-07-07-cli-run-resume-replay-inspect-spine.md` must define that policy rather than inventing it inside storage.

Minor security limit: raw secret values cannot be perfectly detected generically. The store enforces typed `SecretRef` values for sensitive detail keys and rejects untyped `secret://` strings; callers still must avoid placing resolved secrets under innocuous key names. This matches the first storage slice and is reinforced by `.10x/specs/project-cli-observability-security.md`.

## Assumptions Tested

The review checked that the run ledger cannot write checkpoint rows, that checkpoint-state authority remains isolated in `CheckpointStore::commit`, that append-only behavior is enforced below the Rust API by SQLite triggers, that required event families are enumerated in Rust and constrained in SQLite, and that schema-version handling has both a recorded migration row and an unsupported-version failure path.

The review also checked that public API expansion was semver-compatible and that source/security gates passed.

## Verdict

Pass. The run-ledger storage/API slice satisfies the ticket acceptance criteria and leaves orchestrator/CLI policy outside the storage boundary.

## Residual Risk

The next orchestrator and CLI slices must preserve the run ledger as an operational index only. They must not treat `checkpoint_committed` events as state authority, and they must continue to use durable package, destination receipt, and checkpoint-store facts when ledger events disagree with source-of-truth artifacts.
