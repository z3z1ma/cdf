Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-06-native-parquet-file-source.md
Verdict: pass

# Native Parquet file source review

## Target

Review of the implementation and closure evidence for replacing the `cdf-formats` DuckDB-backed Parquet file-source reader with native arrow-rs Parquet reading.

## Findings

No blocking findings.

Significant reviewed risks:

- The change intentionally introduces `paste 1.0.15` through `parquet 59.0.0`. This is not hidden by the review: cargo-audit reports the unmaintained advisory and OSV reports `RUSTSEC-2024-0436`. The risk is acceptable for this slice because `.10x/decisions/native-arrow-datafusion-parquet-policy.md` and `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md` explicitly ratify the scoped exception for native Arrow/DataFusion Parquet.
- `cargo vet` required new current-version exemptions for Parquet transitive crates. The review treats these as supply-chain backlog acknowledgements, not full audits. `cargo vet --locked` now passes, and the exemptions are exact current versions added by the dependency change.
- `cargo geiger` did not produce a usable final report. The changed crate has no direct unsafe matches by source scan, and the implementation itself is safe Rust over upstream `parquet` APIs. Residual dependency unsafe exposure is governed by the existing supply-chain scanners and vet exemptions.
- Mutation testing produced only unviable mutants for the selected changed functions. This is weak mutation evidence but not a blocker because focused tests, full workspace tests, nextest, clippy, dependency checks, and scanner evidence directly exercise and validate the reader behavior.

## Verdict

Pass. The implementation satisfies the ticket while staying within the explicit exclusions: it does not change destination writers, package archive writer behavior, CLI behavior, the native policy, or `.gitignore`.

## Residual Risk

The ratified `RUSTSEC-2024-0436` advisory remains present until arrow-rs/DataFusion remove `paste` or a safe replacement path becomes available. CodeQL was skipped under the active goal instruction.
