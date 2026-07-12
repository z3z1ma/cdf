Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-format-delimited, crates/cdf-runtime/src/format.rs, crates/cdf-source-files/src/runtime.rs, crates/cdf-project/src/schema_discovery.rs
Verdict: concerns

# B4 streaming CSV driver review

## Findings

No critical or high finding blocks the extraction slice.

The driver is parser-local, consumes only neutral byte/memory/format contracts, and publishes one accounted batch at a time. Discovery reuses source leases rather than copying samples. Production source/project code contains no CSV parser or concrete driver type, and the old source fallback rejects instead of shadowing the new path.

### Significant follow-up — variable records and decoder-native allocation

Quoted multiline and oversized records can make Arrow CSV retain more native memory than the target output lease before the batch is observed. B4 must add record-boundary admission, randomized rechunking, RSS, and clean-failure proof under the memory ledger.

### Significant follow-up — option authority

The shipped driver currently admits only default headered comma-separated files. Delimiter/header/quote/escape/comment/null/truncated-row options need a canonical versioned option schema and must be identical across discovery and decode before broader CSV/TSV/PSV claims.

## Verdict

Concerns, accepted for the bounded default-CSV migration. Both findings remain owned by open B4 acceptance and do not justify retaining the legacy path.

## Residual risk

Performance and fixed-width behavior are unproven; the B4 envelope remains open.
