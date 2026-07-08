Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-e4-variant-capture-evolution-event.md
Verdict: pass

# P1 E4 variant capture and contract-evolution review

## Target

Review of P1 E4 implementation and closure evidence for `_cdf_variant` capture and contract-evolution evidence.

## Findings

- Pass: Compiled `NestedAction::CaptureVariant` columns now materialize a semantic `json` `_cdf_variant` column in the live engine normalization path, and source nested columns are removed from the output batch instead of being silently ignored.
- Pass: Variant evidence is deterministic. Captured source field names are ordered through canonical JSON, and the package-level `schema/contract-evolution.json` artifact sorts capture entries by source field, variant column, and semantic tag.
- Pass: Promotion is not implicit. The artifact records `promotion_events: []` and `implicit_promotion_count: 0`, matching the E4 scope and leaving actual trust transition events to E5.
- Pass: Package identity and replay evidence include the new artifact. The focused test opens the package, verifies it, confirms `schema/contract-evolution.json` is checked, and confirms `replay_view()` exposes the segment.
- Pass: Unsupported variant materialization fails closed at contract boundaries for unsupported Arrow types and non-finite float values.
- Pass: Quarantine redaction still protects PII observed values in a run that also performs variant capture; the focused test confirms the raw rejected email value is not present in the quarantine Parquet artifact.
- Pass: Conformance owns compiler coverage for nested unknown structure compiling to `NestedAction::CaptureVariant`.
- Pass: The parent review corrected an initial structural concern by moving variant capture out of `execution.rs` into `variant_capture.rs`, then split scalar conversion helpers. The new maximum hotspot in the new module is cyclomatic 18 rather than the initial 39.

## Verdict

Pass. E4 acceptance criteria are supported by focused engine tests, conformance coverage, package evidence inspection, replay/verification assertions, quality gate output, and this adversarial review.

## Residual Risk

Destination-specific variant storage mappings, child-table expansion, trust-ring promotion/demotion ledger events, and the drift-quarantine conformance scenario remain outside E4 and are owned by downstream P1 tickets. The existing `execute_to_package_inner` complexity remains a known engine hotspot, but E4 did not worsen it and avoided adding new monolithic execution logic.
