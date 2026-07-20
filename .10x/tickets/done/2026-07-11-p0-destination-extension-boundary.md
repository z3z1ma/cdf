Status: done
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-05-implement-cdf-system.md
Depends-On: .10x/decisions/destination-runtime-composition-boundary.md, .10x/specs/destination-extension-runtime-contract.md

# P0 destination extension boundary completion

## Scope

Complete the dependency inversion required for one-step destination additions before P3 streaming/bulk changes amplify concrete driver wiring. Extract the neutral runtime contract, move adapters into destination crates, inject registry authority through project/product surfaces, and make conformance data-driven.

This parent is a plan. Its children are the executable units.

## Child tickets

- `.10x/tickets/done/2026-07-11-p0-dx1-neutral-runtime-crate.md`
- `.10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md`
- `.10x/tickets/done/2026-07-11-p0-dx3-generic-lock-doctor-replay.md`
- `.10x/tickets/done/2026-07-11-p0-dx4-conformance-extension-law.md`

## Acceptance criteria

- The synthetic Quasar destination satisfies the active extension-contract scenario without editing shared project/CLI/conformance-engine code.
- `cdf-project` has no concrete destination dependencies.
- CLI concrete destination imports are restricted to one composition module or explicitly owned adapter-only diagnostics.
- Lockfile, doctor, replay, correction, and runtime planning consume driver inspection/runtime traits.
- P3 streaming/bulk/memory declarations live in sheet/runtime data, never destination-name branches.
- Build-graph evidence records the reduced impact of editing one destination crate.

## Blockers

None after shaping. P3 WS-A and WS-D wait for the relevant children; WS-L remains unblocked.

## Progress and notes

- 2026-07-11: DX1 closed with neutral runtime registry/inspection/prepared-commit contracts and unchanged project runtime behavior. DX2 is unblocked and owns driver-crate adapter migration plus the single CLI composition root.
- 2026-07-12: DX2 closed after its acceptance criteria were mapped to existing execution evidence and a fresh adversarial pass. Driver adapters are destination-owned, `cdf-project`'s normal graph is destination-neutral, and the CLI has one explicit first-party registry; DX3 remains the dependent owner for its bounded generic product-surface work.
- 2026-07-17: DX3 closed after consuming DX3A's terminal public CLI registry/resume evidence. DX4 is now the remaining child for conformance extension laws and build-graph proof.
- 2026-07-18: DX4 closed after a distinct synthetic Quasar driver reached generic project/CLI
  product paths, the generated source/disposition and 16-case crash matrices, exact logical
  payload replay/recovery, receipt/checkpoint gates, and duplicate suppression. Static gates
  cover the complete project production source and derive every guarded conformance identity
  from the destination catalog. The normal Cargo graph proves destination-only edits cannot
  invalidate `cdf-project` or `cdf-runtime`. All four children are terminal.

## Evidence

- The synthetic Quasar scenario and generated conformance laws are recorded in
  `.10x/tickets/done/2026-07-11-p0-dx4-conformance-extension-law.md`, including exact focused
  commands, full workspace all-target/all-feature Clippy, formatting, and fast-quality results.
- `.10x/tickets/done/2026-07-11-p0-dx2-driver-owned-adapters-composition.md` records the normal
  dependency inversion and single explicit production composition root.
- `.10x/tickets/done/2026-07-11-p0-dx3-generic-lock-doctor-replay.md` and terminal DX3A evidence
  record neutral lock, doctor, replay, correction, run, and resume authority.

## Review

The DX4 closure review first failed on identity-equivalent fixture behavior, concrete service
lifecycle leakage, weak payload oracles, and incomplete gates. The repaired-slice review found
two additional dishonest capability/disposition claims and two remaining chaos/static gaps.
None was waived. Final targeted independent review passed with no critical or significant
finding. Child reviews and evidence jointly cover every parent acceptance criterion.

## Retrospective

Dependency inversion is incomplete until tests and product composition obey it: test-only
constructors on production types and destination-labeled generic assertions still make a new
adapter a shared-code edit. One catalog should own enrollment data; shared laws should derive
their cases, identities, and applicability from truthful driver sheets. Distinct identity and
typed payload equality are stronger extension proofs than case labels or receipt counts.
