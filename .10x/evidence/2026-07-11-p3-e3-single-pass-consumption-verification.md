Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-e3-streaming-verification-replay-io.md, .10x/tickets/done/2026-07-11-p3-a5e-streaming-graph-integration.md, .10x/specs/package-io-hashing-durability.md

# Single-pass package consumption verification

## What was observed

The fresh staged run path previously performed a full package verification while reconstructing replay inputs, again while validating replay, again before staged final binding, and again from final-binding reconstruction. The January TLC package contains 16 segments and approximately 104 MiB of identity data, so three of those four scans were redundant within one commit operation.

`cdf-package` now creates a compact `VerifiedPackageReader` only after exact package verification. Its verification authority has private fields and binds the package directory plus package hash. Generic project replay, recovery, prepared commit, staged ingress, destination planning, and final binding consume the same authority. A proof from an identical package in another directory is rejected. Explicit `cdf package verify` remains an independent full verification operation.

The package finalizer now creates that authority directly after it reconciles every identity path against its hash-while-write receipt, synchronizes the package directory, constructs the canonical manifest identity hash, and atomically publishes the manifest. Ordinary run consumes this authority without reopening package content. Reopened replay cannot construct it and still performs independent verification.

## Procedure

- Built the release CLI and ran a fresh local project over `/private/tmp/yellow_tripdata_2024-01.parquet` into DuckDB.
- The run processed 2,964,624 rows, produced 16 canonical segments and a 104 MiB package, verified the destination receipt, and committed the checkpoint in `real 1.64`, `user 2.05`, `sys 0.12` seconds.
- The recent equivalent two-segment/128 MiB staged-ingress control had a three-run median of 1.89 seconds. The observed wall reduction is 0.25 seconds, or 13.2%.
- After connecting finalization authority directly, three additional fresh controls measured 1.55, 1.58, and 2.13 seconds. Their 1.58-second median is 3.7% below the preceding 1.64-second observation and 16.4% below the 1.89-second staged-ingress control median. The 2.13-second sample is retained as observed host variance.
- `cargo test -p cdf-package --lib consumption_verification_authority_is_bound_to_one_package_directory` passed.
- `cargo test -p cdf-package --lib finalization_authority_opens_for_consumption_without_rereading_identity_files` passed with the identity payload made unreadable after finalization.
- Focused generic staged replay and ordinary durable-publish staging tests passed.
- All 28 active `cdf-runtime` tests passed; one performance benchmark remained ignored.
- Strict all-target/all-feature Clippy passed for `cdf-package`, `cdf-runtime`, and `cdf-project`.

## What this supports or challenges

This supports a single adapter-neutral verification authority per package consumption operation. It removes roughly 312 MiB of redundant TLC package read/hash traffic from reopened consumption and all 104 MiB of post-build package reread traffic from ordinary run, without caching a success globally, weakening explicit verification, or allowing a destination to invent package authority. The proof travels through neutral runtime context rather than destination-name branches.

## Limits

Reopened replay segment consumers still read after initial whole-package verification. E3 retains fusion of replay segment verification with consumer reads plus bounded/parallel explicit high-cardinality verification.
