Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md
Verdict: concerns

# CLI spine parent closure audit

## Target

Closure audit for `.10x/tickets/2026-07-07-cli-run-resume-replay-inspect-spine.md` after `run`, `replay package`, `inspect run`, and `resume` child work.

## Findings

- Significant: The parent acceptance criteria include `cdf run` support for table-backed SQL resources. Lower `cdf-project` has live Postgres table-backed SQL source execution evidence, but `crates/cdf-cli/src/tests.rs` currently proves only CLI fail-closed SQL behavior: missing secret fails before writes and resolved secret fails before writes on the ordered-cursor blocker. I did not find a direct CLI success-path test for table-backed Postgres SQL `cdf run`.
- Minor: Resume, replay package, and inspect-run have child closure evidence and review records. Resume closure specifically covers DuckDB, filesystem Parquet, and Postgres finalized-package/no-receipt recovery without source contact.

## Verdict

Concerns. Do not close the parent yet. Add a focused child for CLI table-backed SQL run success and keep the parent open until that child has implementation evidence and review.

## Residual risk

The open parent remains the durable owner for the aggregate CLI run/resume/replay/inspect promise. This audit does not reopen closed child tickets; it identifies a missing parent-level acceptance slice.
