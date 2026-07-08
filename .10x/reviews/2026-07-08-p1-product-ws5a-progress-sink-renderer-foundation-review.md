Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md
Verdict: pass

# P1 product WS5A progress sink and renderer foundation review

## Target

Implementation and evidence for `.10x/tickets/done/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`.

Evidence: `.10x/evidence/2026-07-08-p1-product-ws5a-progress-sink-renderer-foundation.md`.

## Assumptions tested

- WS5A must build a progress foundation without wiring run/replay/resume/backfill command paths end to end.
- Progress must consume the durable run-event spine as a non-authoritative subscriber.
- The `RunEventSink` implementation must not block the run if progress is contended or full.
- Interactive and headless progress must use the same renderer config and redaction boundary.
- Quiet and verbose modes must be represented without inventing parser wiring outside this child.
- Event ordering edge cases must be deterministic: accepted, dropped, duplicate, out-of-order, and terminal.
- Tests must cover phase mapping, redaction, drop behavior, headless formatting, and quiet/verbose behavior.

## Findings

No blocking findings.

Pass: `CliProgressSink` implements `RunEventSink` and uses `try_lock` in `try_emit`. A contended progress state returns `Dropped` immediately. A full milestone buffer drops nonterminal events without changing runtime state.

Pass: Terminal handling is deterministic and bounded. `run_succeeded` and `run_failed` are terminal; terminal events evict the oldest milestone when the buffer is full, while later higher-sequence events become `AfterTerminal` no-ops. `run_failed` stays on the current failed phase instead of jumping to gate.

Pass: Duplicate and out-of-order sequence handling is deterministic and tested. Duplicate sequences and lower-than-max unseen sequences do not move the phase or append milestones.

Pass: Phase mapping covers every current `RunEventKind`. Segment progress and destination segment acknowledgment mapping is explicit, tested, and documented in evidence as the current foundation mapping for WS1C event vocabulary.

Pass: Headless and interactive rendering share `ProgressConfig` and the existing `RenderConfig`. Headless output is line-oriented and ANSI-free; interactive output uses the existing renderer primitives.

Pass after parent correction: Redaction is applied before rendering. Tests prove URI userinfo, typed `SecretRef`, and sensitive-key raw-string fallback values do not appear in either headless or interactive rendering. Parent review added the shared `is_sensitive_key` renderer helper so progress display uses the same key-shape vocabulary as other run-event redaction paths.

Pass: Quiet and verbose behavior are represented in `DisplayVerbosity`. Quiet suppresses nonterminal live milestones while preserving terminal output; verbose includes event/run/sequence/detail fields. Parser wiring for `-v`/`-q` remains out of scope because WS5A excludes command wiring and WS2C did not add those parser fields.

Pass: Verification covers focused progress tests, full `cdf-cli` tests, fmt, clippy, unsafe-token scan, Semgrep, Gitleaks, jscpd, complexity metrics, and whitespace diff checks. Parent review reran focused progress tests, cdf-cli clippy, fmt, scoped jscpd, scoped complexity, scoped Semgrep, scoped Gitleaks, unsafe-token scan, banned-phrase scan, and whitespace checks after the redaction hardening.

Residual risk: `crates/cdf-cli/src/progress.rs` is intentionally dormant and marked with a scoped dead-code allowance until WS5B/WS5C wire commands. Later wiring tickets must remove or narrow dead code as the API becomes live.

Residual risk: This foundation renders snapshot-like progress state, not a final live terminal experience. Rate limiting, spinners/bars, terminal session evidence, and multi-resource summary behavior remain later WS5 work.

Residual risk: The progress module currently retains all seen sequence numbers for deterministic duplicate/out-of-order classification. This is acceptable for WS5A test and foundation scope; if long-running live loops produce very large event streams, a later ticket should bound or compact this state without weakening deterministic behavior.

## Verdict

Pass. WS5A acceptance criteria are supported by implementation and evidence. Remaining progress work is explicitly excluded command wiring and richer live terminal behavior owned by later WS5 child tickets.
