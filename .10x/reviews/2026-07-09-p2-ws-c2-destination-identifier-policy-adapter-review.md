Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-09-p2-ws-c2-destination-identifier-policy-adapter.md
Verdict: pass

# P2 WS-C2 destination identifier policy adapter review

## Target

Review of the C2 implementation in `crates/cdf-contract/src/policy.rs`, `crates/cdf-contract/src/normalization.rs`, and `crates/cdf-contract/src/tests.rs`.

## Findings

No blocking findings.

## Assumptions tested

- Public adapter exists at the contract boundary. Confirmed: `IdentifierPolicy::from_destination_rules`, `TryFrom<&IdentifierRules>`, and `identifier_policy_from_destination_rules` are exported through `cdf-contract`'s existing `pub use policy::*`.
- DuckDB rules are not silently weakened. Confirmed: `max_length = None` remains no limit, and the current DuckDB regex-backed allowed pattern is enforced after normalization.
- Postgres length semantics are preserved. Confirmed: the adapter accepts `namecase-v1/postgres-quoted-v1`, keeps `max_length = Some(63)`, and the focused test proves a 63-byte normalized output.
- Unsupported non-column/object-key rules fail closed. Confirmed: `object-key-component-v1` returns a contract error naming the rule and saying live column normalization for that rule is not implemented by this adapter.
- Existing default normalization remains stable for C1. Confirmed: `IdentifierPolicy::default()` still uses `namecase-v1`, `max_length = Some(63)`, ASCII lower-snake charset, and no allowed-pattern constraint.
- Backward-compatible serde keeps the default cap. Parent review found that changing `max_length` to `Option<u16>` needed an explicit serde default; `#[serde(default = "default_identifier_max_length")]` plus `identifier_policy_serde_missing_max_length_keeps_default_cap` now prove older serialized policies without `max_length` do not become unbounded.
- Final integration gates are green. The parent run passed repo-wide format, whitespace, check, clippy, and full workspace tests; C2-focused behavior was included in the `cdf-contract` suite.

## Residual risk

The adapter is intentionally exact about current sheet patterns. Future destination sheet strings or a broader Postgres quoted-identifier live-column normalizer should be added through a new ticket/spec update rather than being accepted by this adapter by default.

CodeQL reports three pre-existing hard-coded cryptographic value findings in `crates/cdf-cli/src/tests.rs`, owned by `.10x/tickets/done/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`. OSV reports only the already-ratified `RUSTSEC-2024-0436` `paste` advisory exception. Neither is introduced by C2.

## Verdict

Pass. The implementation satisfies the C2 adapter acceptance criteria, has focused tests for max length, pattern rejection, unsupported rules, and collision behavior, and keeps later plan/run integration out of scope.
