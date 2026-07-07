Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-general-run-non-file-resource-streams.md
Verdict: pass

# General run non-file resource streams review

## Target

Review of the bounded REST and table-backed Postgres SQL project-run implementation in `crates/cdf-project/src/runtime.rs`, `crates/cdf-project/src/runtime_tests.rs`, `crates/cdf-declarative/src/rest_runtime.rs`, `crates/cdf-declarative/src/sql_runtime.rs`, and `crates/cdf-declarative/src/tests.rs`.

## Findings

No blocking findings remain.

The first implementation admitted non-file resources without proving checkpointable cursor semantics and only checked secret-provider presence. That was repaired before closure: project-run validation now requires non-file resources to declare exact zero-lag cursors; inexact or lagged cursors fail closed with a window-close message; REST/SQL runtime dependency preflight resolves the actual secret and rejects missing or empty values before source/destination/package/state mutation.

## Verdict

Pass. The implementation stays inside the ticket's conservative scope, preserves raw `CompiledResource::open` fail-closed behavior for REST/SQL, avoids inventing cursor/page-token aggregation, and has tests at both project-run and declarative runtime layers for the repaired failure modes.

## Residual Risk

The broader Chapter 11/window-close promise for inexact or lagged non-file cursors remains unimplemented in this slice. This is not hidden in this closure; it was tracked by `.10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md`.
