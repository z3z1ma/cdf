Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a10f-multifile-discovery-runtime-conformance.md, .10x/decisions/preview-global-budget-and-payload-selection.md

# A10f preview balanced-stratified policy evidence

## What was observed

The shared engine preview front end applies the ratified `preview-balanced-stratified-v1` global defaults and uses the source-neutral `stratified-hash-v1` selector before payload I/O. Focused tests observed deterministic permutation-independent membership, edge and strata policy, a 64-partition selection/open bound over 10,000 planned partitions, precomputed fair quotas of `3,3,2` for eight batches over three candidates, global row exhaustion without later payload opens, batch-atomic byte rejection with zero admitted rows/bytes, separate decoded/output byte accounting, and exact terminal-quarantine attestation without payload opens.

After independent review found that preview serialized the same logical bounded identity in Rust field order while discovery used key-sorted canonical JSON, the adapters were unified on `cdf_kernel::StratifiedHashBoundedIdentity::canonical_bytes`. For the exact candidate `events.raw` / `s3://acme/events/2026/01.parquet` / `(42, null, etag-value, stable_etag)`, both adapters now emit:

```text
{"modified_at_ms":null,"size_bytes":42,"strength":"stable_etag","value":"etag-value"}
```

Both produce score `1caaf43016737e252f12a8b0568d67951ecfd702010906cc8a7eaddb7b1caa27` and the same two-of-three membership. The committed discovery score golden did not change.

## Procedure

- `cargo test -p cdf-kernel stratified_selection` passed 3/3 selector tests.
- `cargo test -p cdf-engine preview -- --nocapture` passed 7/7 preview-policy tests.
- `cargo test -p cdf-project stratified_hash_selector -- --nocapture` passed 4/4 compatibility tests, including the canonical score golden and 10,000-candidate budget-independence case.
- Focused CLI regressions for sampled unseen drift, compatible multi-file Parquet, and sorted multi-match preview passed 3/3. After the concurrent RP9 Parquet-promotion fixture was repaired in its owning lane, `cargo test -p cdf-cli --lib` passed 253/253.
- Focused conformance S8 and file/REST/Postgres parity tests passed 2/2. The full conformance unit suite passed 83/83, including registry validation and golden repeats.
- Full unit suites passed for `cdf-engine` (50/50), `cdf-kernel` (22/22), and `cdf-project` (159/159).
- `cargo test -p cdf-conformance --doc` passed its zero-example doctest harness after the earlier concurrent artifact race cleared.
- Strict Clippy passed for kernel/engine and for project/CLI/conformance with all targets and `-D warnings`.
- Targeted `rustfmt --edition 2024` completed for the changed kernel, engine, project adapter, CLI report, and conformance source files.
- `cargo fmt --all -- --check` and `git diff --check` passed.

Post-review repair verification:

- `cargo test -p cdf-project discovery_and_preview_adapters_share_canonical_identity_score_and_membership -- --nocapture` passed 1/1.
- `cargo test -p cdf-project stratified_hash_selector_score_has_canonical_golden_bytes -- --nocapture` passed 1/1.
- Focused sampled-drift, sorted multi-match, and multi-file Parquet CLI regressions passed 3/3.
- Final full unit suites passed for CLI (253/253), conformance (83/83), engine (50/50), kernel (22/22), and project (160/160).
- `cargo clippy -p cdf-kernel -p cdf-engine -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed.

The combined non-CLI full-unit command completed every conformance, engine, kernel, and project unit test successfully, then encountered a Cargo/rustdoc artifact race in `cdf-conformance` doctests: rustdoc referenced a `cdf_project` rlib path that no longer existed after concurrent builds. A standalone rerun after the build boundary stabilized passed.

## What this supports or challenges

This supports the engine-level policy contract: membership and quotas are determined independently of enumeration order and runtime exhaustion; payload I/O is bounded by the batch membership budget; decoded batches are admitted atomically; and report evidence distinguishes selected, opened, inspected, partial, attested, and uninspected coverage. It also supports preservation of discovery selector behavior through a single kernel implementation rather than duplicated hashing logic.

## Limits

This evidence does not cover remote transports and does not close A10f. Parent integration must perform the required adversarial review and closure reconciliation.
