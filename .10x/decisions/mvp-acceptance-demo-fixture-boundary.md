Status: active
Created: 2026-07-08
Updated: 2026-07-08

# MVP Acceptance Demo Fixture Boundary

## Context

`.10x/specs/conformance-governance-roadmap.md` requires the MVP acceptance demo to exercise Tier-0 GitHub issues, plan output, DuckDB load, `cdf sql`, contract freeze and drift quarantine, crash between destination commit and checkpoint commit, resume without source contact, replay into a second database, duplicate replay handling, and state history.

The conformance harness must be deterministic enough for CI and local developer runs. A live unauthenticated GitHub API call would add network, rate-limit, pagination, and data-shape nondeterminism to the acceptance path. A credentialed call would add secret setup and egress policy concerns that belong to operational hardening, not the first conformance proof.

Current CDF already has deterministic Tier-0 REST execution, CLI run/replay/resume/inspect/state/sql surfaces, contract freeze/test commands, drift-quarantine conformance fixtures, and generic chaos coverage. The missing slice is an aggregate demo harness that proves these mechanisms compose.

## Decision

The first MVP acceptance demo conformance harness will use a deterministic GitHub-Issues-shaped Tier-0 REST fixture, not a live GitHub network dependency.

The fixture MUST preserve the meaningful GitHub Issues contract:

- project/source naming uses the GitHub Tier-0 REST shape;
- endpoint path and query shape correspond to a GitHub issues resource;
- row fields include issue identifiers and timestamps sufficient for planning, loading, SQL querying, checkpoint state, and replay proof;
- the fixture transport/server is deterministic, local to the test, and redacts any token-like values from recorded output.

The harness MAY invoke CLI commands where the user-facing beat is explicitly a CLI beat, such as `cdf plan`, `cdf sql`, `cdf contract freeze/test`, `cdf resume`, `cdf replay package`, and `cdf state history`. It MAY call lower `cdf-project` APIs for failure injection where the public CLI lacks a test-only crash hook, but the recorded evidence MUST name that boundary and still prove the same package/receipt/checkpoint invariant.

This fixture harness is sufficient to close a focused acceptance demo foundation child ticket. It is not, by itself, a claim that live GitHub API credentials, rate limits, or production egress policy are complete.

## Alternatives considered

Require live unauthenticated GitHub in the conformance test.

Rejected. It would make CI and local proof depend on mutable external data, rate limits, and network availability, while proving less about CDF's deterministic artifact semantics.

Require a credentialed live GitHub integration immediately.

Rejected. Secret-provider, egress, and operational setup are real product concerns, but they are not needed to prove the MVP artifact/commit/replay/demo spine and would make the first harness harder to run.

Avoid the GitHub shape and use a generic REST fixture.

Rejected. The demo commitment names GitHub issues; the first fixture should preserve the path/resource/field shape enough that future live GitHub substitution is mechanical rather than a new semantic design.

## Consequences

The MVP acceptance proof becomes runnable under normal local/CI conditions and can be hardened into a live-GitHub smoke later without changing the conformance harness's artifact assertions.

The conformance parent remains open until the broader parent acceptance criteria are satisfied or an active record explicitly scopes live GitHub credentials out of 1.0.
