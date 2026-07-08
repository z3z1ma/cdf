Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-mvp-acceptance-demo-fixture-harness.md
Verdict: pass

# MVP acceptance demo fixture harness review

## Target

Adversarial closure review for `.10x/tickets/done/2026-07-08-mvp-acceptance-demo-fixture-harness.md`, the implementation in `crates/cdf-conformance/src/mvp_acceptance_demo.rs`, and evidence `.10x/evidence/2026-07-08-mvp-acceptance-demo-fixture-harness.md`.

## Findings

None blocking.

## Assumptions tested

The fixture does not pretend to be a live GitHub integration. It uses a deterministic GitHub-Issues-shaped resource under the active decision `.10x/decisions/mvp-acceptance-demo-fixture-boundary.md`, records the lower API boundaries, and leaves live credentials/rate-limit/egress proof outside this child.

The crash-window assertion is meaningful. The hook fires after destination receipt verification while `CheckpointStore::head` is still empty; resume then commits the checkpoint without increasing the recorded REST request count.

The duplicate replay assertion is testing destination idempotency rather than checkpoint collision behavior. The test intentionally uses a fresh checkpoint store for duplicate artifact replay and compares the DuckDB mirror before and after replay.

The test does not leak source secrets into durable output. The evidence struct serializes only redacted authorization text, asserts the raw demo secret is absent, and gitleaks scans over `crates` and `.10x` reported no findings.

The quality gate matches the user's stated concerns. `jscpd` was run and reports no new clones; rust-code-analysis was run over `crates`; Semgrep, CodeQL, source-only gitleaks, cargo deny/audit/vet, OSV, workspace clippy, and workspace tests were run. OSV/cargo-audit residuals are only the already-ratified `paste` advisory.

The implementation is scoped to conformance. Production CLI behavior is not widened by adding crash flags, live-provider shortcuts, or destination/source semantics solely for the demo.

## Verdict

Pass. The focused child is safe to close. The broader conformance parent should remain open because this is a deterministic foundation harness, not a live-provider operational smoke test or final conformance closure.

## Residual risk

`cdf sql` in this fixture proves local system package/state queryability while target-table row checks use local DuckDB SQL directly. That boundary is explicit in evidence and acceptable for this child because the acceptance criterion allowed the CLI surface or the same local SQL surface.

The drift-quarantine proof is composed through the existing DuckDB drift fixture rather than the GitHub-shaped resource. This avoids inventing new drift semantics in the acceptance harness and remains sufficient for the focused child; a later integrated provider-specific drift scenario would need its own ticket if required.
