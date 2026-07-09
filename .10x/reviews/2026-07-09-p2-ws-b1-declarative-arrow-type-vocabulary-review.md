Status: recorded
Created: 2026-07-09
Updated: 2026-07-09
Target: .10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md
Verdict: pass

# P2 WS-B1 declarative Arrow type vocabulary review

## Target

Review of the B1 changes to:

- `crates/cdf-declarative/src/declarations.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`

Governing records:

- `.10x/decisions/data-onramp-schema-discovery-reconciliation.md`
- `.10x/specs/data-onramp-schema-intelligence.md`
- `.10x/tickets/done/2026-07-08-p2-ws-b1-declarative-arrow-type-vocabulary.md`

## Assumptions Tested

- The declaration layer must preserve author-provided type spellings while accepting the expanded Arrow vocabulary.
- The compiler may reject invalid strings at compile time as long as the offending type string is named.
- JSON Schema must accept string-form field types instead of enumerating only legacy enum variants.
- Discovery, reconciliation, destination mapping, and runtime support for all new Arrow types remain out of scope.

## Findings

No blocking findings.

Parent review addendum:

- Pass: the parser handles the B1 mandatory ergonomic forms, including nested top-level comma splitting for `struct<amount: decimal(38,9), tags: list<utf8>>`, `map<utf8,int64>`, decimal bounds, timestamp units/timezones, Arrow map entries, and legacy `timestamp_millis`/`timestamp_micros` spellings.
- Pass: invalid nested type strings fail during compilation with the offending original field type string named.
- Pass: the JSON Schema now resolves field `type` to a string schema rather than the old enum.
- Out-of-scope finding: the parent CodeQL rerun completed through the reusable DB and reported three `rust/hard-coded-cryptographic-value` results in unrelated `crates/cdf-cli/src/tests.rs` backfill secret fixtures from P1 WS5C. Those are not introduced by B1 and are owned by `.10x/tickets/2026-07-09-p1-ws5e-codeql-backfill-test-secret-fixtures.md`.

## Verdict

Pass. The implementation keeps the declaration type string-backed, maps existing and required ergonomic spellings into Arrow `DataType`, returns contract errors that name invalid type strings, and updates tests for TOML/YAML compilation plus JSON Schema generation. The evidence record `.10x/evidence/2026-07-09-p2-ws-b1-declarative-arrow-type-vocabulary.md` covers the requested verification commands.

## Residual Risk

`jscpd` reports duplicate blocks in the scoped Rust files while exiting 0; the parent rerun reports `newClones = 0` and `newDuplicatedLines = 0`. The remaining reports are dominated by existing repeated test/runtime patterns and low-threshold repeated match/assertion shapes. This review does not treat that as a B1 blocker.

The current-tree CodeQL gate is not clean because of the three unrelated CLI test-fixture findings above. That blocks claiming whole-tree CodeQL cleanliness, but it does not challenge the B1 review verdict because the B1 touched files have no CodeQL results, no Semgrep findings, no Gitleaks findings, and no unsafe/FFI matches.
