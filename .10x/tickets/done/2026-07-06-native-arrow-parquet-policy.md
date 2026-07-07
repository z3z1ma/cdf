Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md, .10x/tickets/done/2026-07-06-package-archive-persistence-cli.md

# Decide native Arrow/DataFusion Parquet policy

## Scope

Decide whether CDF should replace the current DuckDB-backed Parquet reader/writer workaround with the native arrow-rs/DataFusion Parquet stack and, if so, whether to ratify a narrow advisory exception for `RUSTSEC-2024-0436` through `paste`.

## Context

CDF's architecture makes DataFusion and Arrow core infrastructure. The current Parquet file-source, Parquet destination, and package archive writer avoid the direct arrow-rs `parquet` crate because `parquet 59.0.0` depends unconditionally on `paste 1.0`, and RustSec advisory `RUSTSEC-2024-0436` marks `paste` as unmaintained.

The current ratified supply-chain policy has no advisory ignores. That made the DuckDB-backed Parquet path executable while keeping `cargo audit`, OSV, and `cargo deny` advisory gates clean. The user challenged this workaround on 2026-07-06, noting that DataFusion is central to the design and asking whether accepting this advisory temporarily may be preferable.

## Acceptance criteria

- Recheck current crates.io/latest arrow-rs `parquet` and DataFusion versions, and whether `parquet` still depends unconditionally on `paste`.
- Classify `RUSTSEC-2024-0436` risk precisely: unmaintained dependency, exploitability if any, proc-macro/build-time implications, transitive path, and affected CDF surfaces.
- Decide one of:
  - keep DuckDB-backed Parquet until upstream removes the advisory path;
  - switch to native arrow-rs/DataFusion Parquet with a time-boxed advisory exception;
  - carry a patch/fork that removes `paste`;
  - another explicitly ratified alternative.
- If switching, open bounded implementation tickets for `cdf-formats`, `cdf-dest-parquet`, and `cdf-package` as needed, and update/supersede supply-chain policy records with owner, expiry, and quality-gate expectations.
- If keeping the workaround, record why the architectural cost is acceptable and when to revisit.

## Evidence expectations

Record dependency-tree evidence, advisory-scanner evidence with and without the exception, and a short decision/review record before implementation. Do not change source code or advisory policy in this shaping ticket.

## Explicit exclusions

No Rust source edits, no Cargo dependency changes, no `cargo deny`/`cargo audit` advisory ignores, no replacement of the current DuckDB-backed Parquet writer, and no changes to package archive persistence behavior.

## Progress and notes

- 2026-07-06: Opened after the user questioned the DuckDB-backed Parquet workaround. Current local evidence: `cargo search parquet` reports `parquet = "59.0.0"` as latest; local registry metadata for `parquet-59.0.0` contains an unconditional `[dependencies.paste] version = "1.0"` entry; the current CDF `Cargo.lock` has no `parquet` or `paste` package entry because the workaround avoids that path.
- 2026-07-06: Recorded `.10x/research/2026-07-06-native-parquet-paste-risk.md`. Current evidence shows latest `parquet 59.0.0` still depends unconditionally on `paste`, latest `datafusion 54.0.0` still uses `parquet 58.3.0` behind its Parquet feature, and `RUSTSEC-2024-0436` is an informational unmaintained advisory with no patched version rather than a known exploit advisory. Recommended path, pending user/project ratification, is a narrow time-boxed exception for `paste 1.0.15` only through native arrow-rs/DataFusion Parquet, then replacing the DuckDB-backed Parquet surfaces with native implementations under bounded tickets.
- 2026-07-06: User explicitly ratified this policy ticket. Recorded active decision `.10x/decisions/native-arrow-datafusion-parquet-policy.md`, evidence `.10x/evidence/2026-07-06-native-arrow-parquet-policy-ratification.md`, and review `.10x/reviews/2026-07-06-native-arrow-parquet-policy-review.md`. Opened executable follow-ups `.10x/tickets/done/2026-07-06-rustsec-paste-parquet-exception.md`, `.10x/tickets/done/2026-07-06-native-parquet-file-source.md`, and `.10x/tickets/done/2026-07-06-native-parquet-writer-archive.md`.

## Blockers

None.
