Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Relates-To: .10x/tickets/done/2026-07-09-p2-ws-c3-live-destination-normalization-duckdb-postgres.md, .10x/tickets/done/2026-07-09-p2-ws-i4-s5-s7-standalone-conformance.md

# P2 C3 and I4 integration evidence

## What was observed

- S5 runs a deterministic REST discover resource with an explicit cursor through pin, byte-stable repin, no-write preview, package production, verified DuckDB receipt, and committed cursor/schema checkpoint. Source-name metadata and secret redaction survive the entire path.
- S7 runs keyless append through validate/plan/preview/run and rejects merge-without-key before secret resolution, HTTP contact, or project mutation with both ratified fixes.
- DuckDB and Postgres destination sheets now determine the effective live identifier policy. Plan output, preview fields, package Arrow schema, and committed destination columns agree; `cdf:source_name` preserves source identity.
- The shared state/package/receipt/checkpoint schema hash remains the declared or pinned resource hash. Destination output identity is separately carried by the serialized full identifier policy, validation column program, and package output schema.
- Stale or spoofed normalization programs fail before writes: preflight validates policy/version, exact column count/order/source/output mapping, destination-policy equality, collisions, and allowed-pattern behavior.

## Procedure

- `cargo fmt --all -- --check` — passed.
- `git diff --check` — passed.
- `cargo check --workspace --all-targets --all-features --locked` — passed.
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` — passed.
- `cargo nextest run --workspace --all-features --locked --no-fail-fast --status-level fail --final-status-level fail` — passed 781/781 tests, zero skipped.
- `cargo test --workspace --doc --all-features --locked --no-fail-fast` — passed.
- `cargo doc --workspace --all-features --no-deps --locked` — passed.
- `cargo deny check` — passed with existing policy-allowed duplicate-version warnings.
- `cargo audit --quiet` — passed with the existing allowed `RUSTSEC-2024-0436` unmaintained `paste` warning.
- `cargo semver-checks --baseline-rev origin/main` — passed.
- `cargo llvm-cov --workspace --all-features --locked --summary-only` — passed: regions 81.87%, functions 78.10%, lines 83.38%.
- The CI-pinned gitleaks v8.18.4 was installed under `/tmp` and run in no-git/redacted mode over every changed tracked and untracked file; no leaks were found.
- Live golden verification passed DuckDB and Parquet across 100 deterministic repeats and Postgres across its bounded live repeats after policy evidence changed package identity.

## Acceptance mapping

- I4's S5 scenario owns discovery probe metadata, snapshot/lock determinism, no-write preview, package/receipt verification, cursor advancement, pinned hash reuse, source-name metadata, and secret absence.
- I4's S7 scenario owns keyless append, no key nudge, one command-correct merge error, two fixes, missing-secret non-resolution, zero source contact, and unchanged project tree.
- C3's DuckDB regression owns automatic `VendorID` handling plus restoration of an unbounded destination identifier from source metadata across plan/preview/package/table.
- C3's Postgres regression owns deterministic 63-byte suffixing and package/table agreement.
- C3 collision and stale-plan regressions own pre-write failures and policy-authority coherence; the legacy-serde test owns backward-compatible defaulting.
- The executable P2 matrix promotes only S5 and S7. S1-S4/S6/S8 remain pending.

## What this supports or challenges

This supports closure of C3 and I4. It does not close WS-C, WS-I, or P2 because Parquet column policy and six golden paths remain open.

## Limits

Parquet's current `object-key-component-v1` sheet rule is not reinterpreted as a column policy. Public TLC/network behavior was not exercised by this batch. Normalization-program coherence is enforced at the project live-run boundary; direct low-level engine execution remains an internal trusted construction path. No action is required while arbitrary external `EnginePlan` execution is unsupported; exposing such a surface is the trigger for moving the same coherence validator into the engine boundary.
