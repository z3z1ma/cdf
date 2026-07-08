Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-contract-depth-program.md
Depends-On: .10x/tickets/2026-07-08-p1-e1-row-level-verdicts-live-chain.md

# P1 E2: Quarantine routing and redaction

## Scope

Route evaluator quarantine candidates into package quarantine artifacts and destination mirrors where supported.

Owns:

- `crates/cdf-package/**` quarantine artifact models/writers;
- `crates/cdf-engine/src/execution.rs` package sink routing;
- `crates/cdf-project/src/runtime/**` only where destination mirror routing requires run/package context;
- destination crates or runtime destination adapters only for sheet-backed `_cdf_quarantine` mirrors;
- focused conformance tests for artifact and mirror behavior.

## Governing records

- `VISION.md` Chapter 11.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/package-lifecycle-determinism.md`.
- `.10x/specs/destination-receipts-guarantees.md`.
- `.10x/specs/run-orchestration-ledger.md`.
- `.10x/decisions/contract-live-verdict-execution-semantics.md`.
- `.10x/knowledge/runtime-conformance-throughput-rule.md`.

## Acceptance criteria

- Rejected/quarantined rows are written under `quarantine/part-*.parquet` as identity-participating package evidence.
- Quarantine records include source row ordinal, rule id, error code, source position, and redacted observed value.
- `pii:*` semantic fields use the compiled redaction decision. SHA-256 hash redaction is deterministic and does not store raw PII in artifacts, logs, run ledger events, or test output.
- Accepted rows continue through package writing and destination commit when the compiled verdict permits quarantine rather than run failure.
- Destination `_cdf_quarantine` mirrors are populated only for destinations whose sheets declare quarantine-table support; unsupported sheets record an explicit non-mirror outcome rather than silently skipping package quarantine.
- Package verification covers quarantine artifacts when present.

## Evidence expectations

Record quarantine artifact round-trip tests, redaction adversarial checks, destination mirror tests or sheet-backed exclusions, package verification output, jscpd and `rust-code-analysis-cli` metrics, direct unsafe scan, security scans for secret/PII leakage, and adversarial review.

## Explicit exclusions

No new destination category. No DataFusion multi-output-plan fork. No trust promotion/demotion logic beyond emitting enough evidence for later ledger events.

## Blockers

None once E1 is closed.
