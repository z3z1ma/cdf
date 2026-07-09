Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-a8-autopin-lockfile-no-pin.md, .10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md, .10x/tickets/done/2026-07-09-p2-ws-i3-matrix-friction-reconciliation.md

# P2 A8, B6, and I3 integration evidence

## What was observed

The batch satisfies its three bounded outcomes:

- Existing schema pins are authoritative for ordinary `plan`, `explain`, `preview`, and `run`; first-use discovery creates the deterministic snapshot and semantic lock entry, `--no-pin` inspects without writes, and explicit `schema pin` is the only refresh path.
- Local JSON/NDJSON observes accepted physical rows before applying declared constraints, routes through the shared reconciler, localizes row/type drift, materializes allowed casts, and carries exact coercion decisions into package identity evidence.
- The P2 conformance and friction registries now reject terminal/missing/non-ticket active owners, resolve named tests to source functions, and keep S1-S8 honestly pending.

The live-run golden identities changed because `schema/coercion-plan.json` and the corresponding Arrow schema metadata now participate in package identity. The refreshed DuckDB, Parquet-destination, and Postgres fixtures were printed from verified live packages and then passed their deterministic repeat suites.

## Procedure

- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.
- `cargo check --workspace --all-targets --all-features --locked` — passed.
- `cargo check --workspace --all-targets --no-default-features --locked` — passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` — passed.
- `cargo nextest run --workspace --all-features --locked --status-level fail --final-status-level fail` — final post-repair run passed 765/765 tests, with zero skipped.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` — passed.
- `cargo doc --workspace --all-features --no-deps --locked` — passed.
- `cargo deny check` — passed; existing duplicate-version warnings remained policy-allowed.
- `cargo audit --quiet` — passed with the existing allowed `RUSTSEC-2024-0436` unmaintained `paste` warning.
- Changed tracked/untracked files were each scanned with `gitleaks dir --no-banner --redact`; no leaks were found. A whole-directory scan was stopped because it traversed the large ignored build tree without output; this is not evidence for ignored build artifacts.
- `cargo semver-checks --baseline-rev origin/main` — exited successfully.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` — passed; total regions 81.75%, functions 78.12%, and lines 83.27%.
- Live golden tests passed 100 DuckDB runs, 100 Parquet-destination runs, and 10 live Postgres runs; duplicate replay identity/no-op also passed.

## Acceptance mapping

- A8 lock creation, selected-resource preservation, byte stability, no-write inspection, redaction, and explicit refresh are covered by the new CLI/project tests and the 765-test workspace run.
- A8 anti-convergence behavior is covered by changed-source tests proving ordinary commands retain the locked hash and do not probe or mutate, while `--no-pin` observes a distinct fresh hash and `schema pin` refreshes.
- B6 widening, policy-enabled decimal parsing, denied decimal-string quarantine, mixed fractional drift localization, source-name handling, JSON/NDJSON parity, and exact package evidence are covered by contract/formats/engine tests.
- B6 evidence provenance is covered by malformed metadata, false-plan metadata, Arrow IPC/Parquet metadata, metadata-without-header, header-without-metadata including fabricated `Extra`, inconsistent-plan, and legacy-header tests.
- I3 owner-state and named-test validation plus registry consistency are covered by the four executable P2 registry tests.

## What this supports or challenges

This supports closure of A8, B6, and I3. It does not support closure of their parent workstreams or P2: Hints, remaining format reconciliation, destination-policy live selection, remote enumeration/cloud transport, final diagnostics/scaffolding, and complete S1-S8 coverage remain open.

## Limits

The ordinary CI network cases use deterministic local fixtures. No public TLC session was claimed. Mutation testing and full-history secret scanning were not run. The existing allowed advisory and duplicate dependency warnings were not introduced by this batch.
