Status: open
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-06-native-arrow-parquet-policy.md, .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md

# Implement scoped RUSTSEC paste exception for native Parquet

## Scope

Update the local supply-chain policy so CDF may intentionally accept `RUSTSEC-2024-0436` only when `paste 1.0.15` is introduced through the native arrow-rs/DataFusion Parquet dependency path ratified by `.10x/decisions/native-arrow-datafusion-parquet-policy.md`.

Expected ownership:

- `deny.toml`
- `supply-chain/**` only if cargo-vet metadata must change for the policy gate
- `.10x/` evidence/review/ticket records for this child

## Acceptance criteria

- `deny.toml` ignores exactly `RUSTSEC-2024-0436` and no other advisory.
- Evidence records the approved path and explains that `paste` is allowed only for native Parquet.
- `cargo deny check` passes after the policy change.
- `cargo audit` and OSV results are recorded honestly. If either reports the ratified advisory despite `cargo deny` policy, the evidence must show no unratified advisories.
- `cargo vet --locked` remains passing or has a precise metadata update.
- No Cargo dependency versions are changed in this policy-only ticket.

## Evidence expectations

Run `git diff --check`, `cargo deny check`, `cargo audit`, `osv-scanner`, and `cargo vet --locked`. Record scanner behavior and limits.

## Explicit exclusions

No Rust source edits, no `Cargo.toml` or `Cargo.lock` dependency changes, no native Parquet reader/writer implementation, no DuckDB removal, no CI workflow changes, no broad unmaintained-advisory exception, and no `.gitignore` edits.

## References

- `.10x/decisions/native-arrow-datafusion-parquet-policy.md`
- `.10x/research/2026-07-06-native-parquet-paste-risk.md`
- `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`
- `QUALITY.md`

## Progress and notes

- 2026-07-06: Opened after explicit user ratification of the native Arrow/DataFusion Parquet policy. This ticket should land before native Parquet dependency additions so scanner behavior is intentional.

## Blockers

None.
