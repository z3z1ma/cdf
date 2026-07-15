Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-c2-parallel-frontier-execution.md, .10x/specs/deterministic-parallel-scheduler.md

# C2 jobs CLI ceiling

## What was observed

`cdf run --jobs N` now carries a nonzero `u16` ceiling through resolved run arguments into `cdf_runtime::resolve_runtime_scheduler`. Omitting the option passes `None` and preserves auto-resolution. Zero and values outside 1..=65535 fail in CLI parsing before project/source/destination work.

## Procedure

- `cargo test -p cdf-cli run_jobs_is_a_nonzero_user_ceiling --locked`
- `cargo clippy -p cdf-cli --all-targets --locked -- -D warnings`
- `cargo fmt --all`

The focused parser law and strict all-target lint passed.

## What this supports or challenges

This supports the specified tunability without introducing another scheduler or treating user jobs as permission to exceed runtime capabilities.

## Limits

This evidence proves parsing and propagation by code inspection/compilation. Scaling and jobs-invariance across permanent archetypes remain C4 work.
