Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md
Verdict: pass

# Scoped RUSTSEC paste exception review

## Target

Review the policy-only change that allows exactly `RUSTSEC-2024-0436` for the native Arrow/DataFusion Parquet path.

## Findings

No blocking findings.

## Assumptions tested

- Scope: the diff changes `deny.toml` and `.10x/` records only. No Rust source, `Cargo.toml`, `Cargo.lock`, cargo-vet metadata, CI workflow, or `.gitignore` change is part of this ticket.
- Narrowness: `[advisories].ignore` contains exactly one advisory id, `RUSTSEC-2024-0436`.
- Current graph: `paste` and `parquet` are absent from the current lockfile graph, so `cargo deny` warns that the advisory is not encountered. That is expected for this policy-before-dependency ticket and prevents confusing current scanner cleanliness with future native-Parquet evidence.
- Supply-chain gates: `cargo deny`, `cargo audit`, OSV, and `cargo vet` all passed after the policy edit.

## Residual risk

The policy now contains a dormant advisory exception. That is intentional and ratified, but it means future dependency updates must prove that the ignored advisory is only exercised through the native Parquet path. The native Parquet reader/writer tickets retain that burden.

## Verdict

Pass. The change is the minimal policy representation of the active decision, preserves supply-chain gates, and unblocks native Parquet implementation without hiding any current vulnerability.
