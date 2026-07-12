Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/2026-07-11-p3-a5c-durable-segment-stream.md

# P3 A5c verified segment stream milestone

## What was observed

`PackageReader` now exposes the first bounded, verified, memory-accounted segment stream. It verifies the package before any item, validates exact state/manifest segment coverage for commit streams, reserves before decode, yields one segment plus its authority and lease, and stops permanently on any error.

The memory consumer key is constant (`package:verified-segment-stream`) rather than per segment, so ledger metadata cardinality does not grow with package cardinality. The stream refuses a second live item before trying another reservation, preventing same-thread self-deadlock and accidental eager collection.

## Procedure

- `cargo test -p cdf-package verified_ -- --nocapture` — the verified stream, commit authority, tamper, undersized-window, and package-input verification cases passed.
- `cargo test -p cdf-package verified_segment_stream -- --nocapture` — the one-live-window guard and tamper/window failure cases passed.
- `cargo test -p cdf-package --lib` — 41 passed and two explicit E1 performance tests ignored before the final guard was added; all focused post-guard tests passed.
- `cargo clippy -p cdf-package --all-targets -- -D warnings` — passed.

## What this supports

- Peak decoded segment memory is bounded by configuration rather than segment/package count.
- No unverified or row-count-inconsistent segment crosses the new handoff.
- Ordinary and commit-authoritative reads share one generic package boundary.

## Limits

Project replay, Postgres, and Parquet now use the accounted stream. DuckDB explicitly declares `MaterializedPackage`, and the package archive compatibility API remains eager; A5c remains open until those declared exceptions are converted, static gates forbid regression, and end-to-end crash/performance evidence is recorded.
