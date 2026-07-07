Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/2026-07-07-cli-run-general-runtime.md, .10x/tickets/2026-07-07-cli-replay-package-spine.md
Verdict: pass

# CLI Parquet run and replay review

## Target

Filesystem Parquet CLI destination slice in `crates/cdf-cli/src/commands.rs`, `crates/cdf-cli/src/tests.rs`, `crates/cdf-cli/Cargo.toml`, and `Cargo.lock`.

## Findings

No blocking findings.

The implementation follows `.10x/decisions/destination-introspection-package-and-cli-policy.md`: `parquet://<root>` is treated as a filesystem root/prefix, not a single file. Relative roots resolve under the project root, absolute roots are allowed, empty roots and nested URI values fail closed.

Replay uses package artifacts and `replay_parquet_package_from_artifacts`; it does not re-run source extraction. The test fixture verifies checkpoint commit and package receipt append through the package artifact path.

The CLI report distinguishes the user-facing destination kind `parquet` from the receipt destination id `parquet_object_store`, preserving lower-layer receipt truth instead of normalizing it away.

## Residual risk

These tickets are not closed by this review. Postgres run/replay policy parsing and production REST transport registration remain outside this Parquet-only slice and are still active work.

## Verdict

Pass for the Parquet run/replay slice. Keep `.10x/tickets/2026-07-07-cli-run-general-runtime.md` and `.10x/tickets/2026-07-07-cli-replay-package-spine.md` open for their remaining acceptance criteria.
