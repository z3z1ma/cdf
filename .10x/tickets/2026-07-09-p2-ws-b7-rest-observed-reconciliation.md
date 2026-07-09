Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/tickets/done/2026-07-09-p2-ws-b6-json-family-observed-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/knowledge/schema-coercion-evidence-provenance.md

# P2 WS-B7 REST observed-first reconciliation

## Scope

Route declarative REST JSON pages through the shared observed-physical-schema reconciliation front end established for local JSON/NDJSON. REST discovery, preview, and package-producing execution must not retain a separate declared-schema decoder truth.

This ticket owns REST response record materialization and reconciliation only. Reuse B6 helpers where their extraction does not widen behavior; preserve REST pagination, selector, cursor, retry, auth, egress, and redaction semantics.

## Acceptance criteria

- Each bounded REST page observes accepted JSON record types before applying the pinned or declared schema constraint.
- Lossless width/decimal widenings materialize automatically; parse coercions and lossy mappings remain governed by the existing explicit type policy.
- Row-local scalar drift produces deterministic `source_type_mismatch` quarantine without aborting accepted records or changing their order.
- Extra/missing fields, `cdf:source_name`, physical provenance, and exact coercion decisions match the local JSON/NDJSON reconciliation semantics.
- The trusted coercion plan reaches package validation evidence only through the dual-channel provenance invariant; source response metadata cannot synthesize it.
- Preview and run share the same REST response reconciliation front end, including multi-page consistency and fail-closed inconsistent plans.
- Existing cursor advancement, selector, pagination, retry, secret redaction, and egress tests remain green.

## Evidence expectations

Focused `cdf-declarative`, `cdf-contract`, `cdf-engine`, CLI preview/run, and conformance REST tests; multi-page mixed-width/drift cases; package artifact evidence; malformed/adversarial JSON; redaction checks; and the applicable `QUALITY.md` input-boundary/security/test profiles.

## Explicit exclusions

This ticket does not change REST discovery sample bounds, cursor inference, pagination policy, HTTP retries, `cdf add`, source authentication syntax, CSV/Arrow/SQL reconciliation, or contract flag vocabulary.

## Progress and notes

- 2026-07-09: Opened after B6 closed the local JSON/NDJSON split and left REST as the next live JSON-family path still requiring the same shared observed-first compiler semantics.

## Blockers

None. B6, the active schema-intelligence spec, and the evidence-provenance knowledge record fully govern this slice.
