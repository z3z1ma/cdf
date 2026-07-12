Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-e3-streaming-verification-replay-io.md, .10x/tickets/2026-07-11-p3-a5e-streaming-graph-integration.md, .10x/specs/package-io-hashing-durability.md

# Single-pass package consumption verification

## What was observed

The fresh staged run path previously performed a full package verification while reconstructing replay inputs, again while validating replay, again before staged final binding, and again from final-binding reconstruction. The January TLC package contains 16 segments and approximately 104 MiB of identity data, so three of those four scans were redundant within one commit operation.

`cdf-package` now creates a compact `VerifiedPackageReader` only after exact package verification. Its verification authority has private fields and binds the package directory plus package hash. Generic project replay, recovery, prepared commit, staged ingress, destination planning, and final binding consume the same authority. A proof from an identical package in another directory is rejected. Explicit `cdf package verify` remains an independent full verification operation.

## Procedure

- Built the release CLI and ran a fresh local project over `/private/tmp/yellow_tripdata_2024-01.parquet` into DuckDB.
- The run processed 2,964,624 rows, produced 16 canonical segments and a 104 MiB package, verified the destination receipt, and committed the checkpoint in `real 1.64`, `user 2.05`, `sys 0.12` seconds.
- The recent equivalent two-segment/128 MiB staged-ingress control had a three-run median of 1.89 seconds. The observed wall reduction is 0.25 seconds, or 13.2%.
- `cargo test -p cdf-package --lib consumption_verification_authority_is_bound_to_one_package_directory` passed.
- Focused generic staged replay and ordinary durable-publish staging tests passed.
- All 28 active `cdf-runtime` tests passed; one performance benchmark remained ignored.
- Strict all-target/all-feature Clippy passed for `cdf-package`, `cdf-runtime`, and `cdf-project`.

## What this supports or challenges

This supports a single adapter-neutral verification authority per package consumption operation. It removes roughly 312 MiB of redundant TLC package read/hash traffic without caching a success globally, weakening explicit verification, or allowing a destination to invent package authority. The proof travels through neutral runtime context rather than destination-name branches.

## Limits

One full package scan still establishes authority after a fresh build even though the builder hash-while-wrote and durably finalized every identity artifact. E3 still owns replacing that fresh-run scan with a builder-issued finalization proof, fusing replay segment verification with consumer reads, and making explicit high-cardinality verification bounded and parallel.
