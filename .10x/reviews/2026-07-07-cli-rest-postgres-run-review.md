Status: recorded
Created: 2026-07-07
Updated: 2026-07-07
Target: .10x/tickets/done/2026-07-07-cli-run-general-runtime.md
Verdict: pass

# CLI REST/Postgres run review

## Target

Review of the `cdf run` REST/Postgres completion slice, including `crates/cdf-cli/src/run_command.rs`, `crates/cdf-cli/src/destination_uri.rs`, `crates/cdf-cli/src/http_transport.rs`, the reduced `crates/cdf-cli/src/commands.rs`, project destination policy parsing in `crates/cdf-project/src/models.rs`, and the focused CLI/project tests.

## Findings

No blocking findings.

The review specifically checked:

- unsupported `run --loop` remains fail-closed before runtime mutation;
- REST resources now use an explicit production transport rather than a test transport or hidden source contact shortcut;
- Postgres destination policy is required before secret resolution, preventing unnecessary DSN exposure on missing or malformed policy;
- resolved Postgres DSNs are not serialized into CLI JSON reports and are redacted from propagated errors;
- `commands.rs` no longer owns the `run` execution path or duplicate destination URI helper copies;
- the new direct `reqwest` dependency is used and passed `cargo machete`;
- full workspace tests include the new REST and Postgres `cdf run` cases as well as existing commit-gate and recovery tests.

## Verdict

Pass. The child ticket acceptance criteria are satisfied for this slice, and the architecture moved in the right direction by extracting run behavior out of `commands.rs`.

## Residual risk

`commands.rs` remains a broad command module with high aggregate complexity even after the run extraction. This is tracked separately in `.10x/tickets/2026-07-07-cli-command-module-architecture.md`; it does not block closing the run child because the run-specific vertical code is no longer in the command dispatcher file.
