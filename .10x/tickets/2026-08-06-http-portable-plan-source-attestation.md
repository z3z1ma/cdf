Status: open
Created: 2026-08-06
Updated: 2026-08-06

# HTTP portable-plan source attestation

## Scope

Make the portable-plan boundary honest and useful for HTTP file sources whose metadata resolves to
weak generation authority. Either establish a stable exact attestation that survives an immediate
plan/run round trip, or reject portable export during planning with a diagnostic that names the
missing authority and corrective action.

## Non-goals

- Weakening portable source-change preflight.
- Treating volatile redirects, signed URLs, or response timestamps as content identity.
- Schema-authority or portable state-precondition changes.

## Acceptance Criteria

1. An unchanged HTTP object never produces the generic "source generation changed" diagnostic
   solely because the adapter cannot reproduce the planned attestation.
2. A genuinely changed object still fails before package, destination, checkpoint, or ledger
   effects.
3. Supported HTTP sources complete `cdf plan RESOURCE --out plan.json` followed immediately by
   `cdf run --plan plan.json`; unsupported sources fail during plan with exact remediation.
4. Tests cover redirected/CDN metadata and weak-versus-strong identity without network flakiness.

## References

- `.10x/specs/portable-plan-artifact.md`
- `.10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md`
- `.10x/tickets/done/2026-08-06-s2-state-backed-preparation-portable-plan.md`

## Assumptions

- Record-backed: portable execution MUST preserve exact source-change preflight.
- Blocked: whether weak HTTP identity can be strengthened without downloading the complete object
  during planning needs source-adapter investigation.

## Journal

- 2026-08-06: Release binary planned `fineweb.documents` successfully, then immediate
  `run --plan .cdf/s2-fineweb-plan.json` failed before effects with "source generation for portable
  plan partition `files` changed". The same unchanged resource completed through ordinary run:
  1.1M rows, 2.1 GiB, 14 segments, durable receipt/checkpoint. A local-file portable plan completed
  and established first-use schema authority, isolating the defect to HTTP source attestation rather
  than portable state preflight.

## Blockers

- Decide from adapter evidence whether the correct current behavior is stable attestation or
  plan-time unsupported rejection.

## Evidence

Pending execution.

## Review

Pending execution.

## Retrospective

Pending execution.
