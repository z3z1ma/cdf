Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-10-p2-ws-a10g-explicit-sampled-binary-discovery.md, .10x/tickets/done/2026-07-10-p2-rp6-postgres-in-place-corrections.md, .10x/tickets/done/2026-07-10-p2-rp7-duckdb-in-place-corrections.md

# P2 A10g, RP6, and RP7 integration evidence

## What was observed

Explicit sampled multi-file binary discovery, the shared destination-neutral correction protocol, Postgres addressed correction, and DuckDB provenance/addressed correction operate together without weakening exhaustive discovery, runtime reconciliation, package identity, receipt verification, replay, or checkpoint gating.

The parent observed a complete all-feature workspace pass after every child change and review repair:

```text
Summary [165.268s] 913 tests run: 913 passed (5 slow), 0 skipped
```

The run included 100-rebuild package determinism, 100-run DuckDB and Parquet live goldens, bounded live Postgres goldens, sampled discovery CLI/package/checkpoint coverage, all-file unseen-drift quarantine, DuckDB correction rollback/readback/replay, and Postgres live correction success plus missing/duplicate/failpoint rollback cases.

Strict workspace lint and mechanical gates also passed:

```text
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
git diff --check
```

## Procedure

1. Inspected all three executable tickets and their active specifications/decisions before activation.
2. Required one shared correction request/plan/session/capability/readback seam with provided unsupported defaults on `DestinationProtocol`; concrete SQL and transaction mechanics remained in destination adapters.
3. Rejected an early parallel correction receipt model. Correction sessions now return canonical kernel `Receipt` and `ReceiptVerification`, and correction requests carry checkpoint-covering `StateSegment`s.
4. Recorded `.10x/decisions/promotion-correction-value-authority.md`; verified one-field canonical `residual-json-v1` envelopes are the sole execution value authority and legacy display JSON is excluded from operation identity.
5. Recorded `.10x/decisions/correction-receipt-operation-and-disposition.md`; verified canonical receipts retain resource disposition context while closed typed evidence declares `addressed_correction`, without falsely using `cdc_apply`.
6. Required kernel-derived/recomputed operation digests, path/output-field bijection, exact typed/non-null provenance, read-only DuckDB planning, and a generic exact-address residual readback hook before destination capabilities could become Supported.
7. Ran the parent workspace nextest, strict Clippy, formatting, and diff checks after child-focused suites were green.

Child-focused evidence recorded in the ticket progress includes:

- A10g affected all-feature matrix: 576/576, zero skipped; 10,000-candidate selector test samples 100 identically under executor concurrency-budget values 1, 8, and 64.
- RP6 kernel/contract/Postgres all-feature matrix: 123/123, zero skipped, including bounded live Postgres.
- RP7 DuckDB adapter: 21/21, plus CLI/project/live conformance and 100-run golden coverage.
- Semver: kernel, project, engine, DuckDB, and Postgres passed 196/196. Declarative passed 195/196 with the sole intentional pre-1.0 public configuration addition `ResourceDeclaration.sample_files`.

## What this supports

- Sampling is explicit, deterministic, transport-neutral evidence that weakens only plan-time observation. Every runtime file still reconciles and receives a total admit/residual/quarantine verdict.
- Exhaustive coverage remains the default; `M <= N` follows the exhaustive manifest/snapshot path.
- Postgres and DuckDB persist and target the same canonical `(original package hash, segment id, row ordinal)` address without introducing semantic merge keys.
- Correction values, operation identity, receipts, readback, replay, and checkpoints have one portable authority rather than destination-specific parsers or settlement paths.
- Adding another destination can inherit unsupported correction hooks or implement the same capability-falsifiable protocol without edits to generic orchestration.

## Limits

- Remote multi-file enumeration/probing is not implemented by A10g; WS-E owns S3/GCS/Azure and HTTP template enumeration. A10g proves the selector over transport-neutral bounded identities, including a canonical S3 identity golden, and exercises local Parquet/Arrow IPC end to end.
- The current local discovery executor is sequential. Selection is completed before probe scheduling and is permutation/budget invariant; distributed scheduling remains owned by the distributed-execution ticket.
- Parquet correction sidecars remain RP8. Promotion dry planning/execution/recovery/conformance remain RP5/RP9/RP10. Preview traversal and final S1/S2/S6/S8 laws remain A10f/WS-I.
