Status: done
Created: 2026-07-10
Updated: 2026-07-10
Parent: .10x/tickets/2026-07-09-p2-ws-a10-multi-file-schema-discovery-pin.md
Depends-On: .10x/specs/sampled-schema-discovery-coverage.md, .10x/tickets/done/2026-07-09-p2-ws-a10a-discovery-manifest-artifact-budget.md, .10x/tickets/done/2026-07-09-p2-ws-a10b-aggregate-schema-join-core.md, .10x/tickets/done/2026-07-09-p2-ws-a10c-exhaustive-local-binary-discovery.md, .10x/tickets/done/2026-07-09-p2-ws-a10d-effective-schema-runtime-evidence.md, .10x/tickets/done/2026-07-09-p2-ws-a10e-file-quarantine-processed-positions.md, .10x/tickets/done/2026-07-10-p2-rp2-residual-verdict-runtime-package.md

# P2 WS-A10g explicit sampled binary discovery

## Scope

Add explicit `sample_files = N` discovery coverage to the format-neutral discovery-set orchestrator using `stratified-hash-v1`. Wire declarative validation, plan/snapshot/package evidence, CLI rendering, and local/transport-fixture Parquet/Arrow IPC behavior without changing exhaustive defaults.

## Acceptance criteria

- `sample_files` is positive, explicit, schema-validated, and absent by default.
- Candidate selection implements every edge and canonical score/stratum rule in `.10x/specs/sampled-schema-discovery-coverage.md`.
- `M <= N` records exhaustive coverage; `M > N` records sampled coverage and exact probed/unprobed entries.
- Selection precedes scheduling and is invariant to enumeration permutation, concurrency, and probe completion order.
- Budgets fail selected probes without substitution or membership changes.
- Selected incompatible schemas fail initial pin with the complete selected report; unprobed runtime drift flows to compiled residual/quarantine verdicts rather than mutating the pin.
- Discover/pin/diff/no-pin/auto-pin/preview/run render coverage and counts consistently in human and JSON output.
- Legacy exhaustive snapshot/manifest bytes remain stable where version/optional-field rules require.

## Evidence expectations

Selector unit/property tests, canonical manifest goldens, large-N fixture runs under varied budgets/concurrency, no-write failure tests, sampled pin/runtime package inspection, exhaustive compatibility regressions, and adversarial review.

## Explicit exclusions

No adaptive/statistical sampling, confidence estimates, row sampling inside text files, source-format-specific selector, promotion execution, or distributed scheduler.

## Progress and notes

- 2026-07-10: Opened after exact selector ratification. This child follows the exhaustive orchestrator so both coverage modes share one candidate/probe/aggregate model.
- 2026-07-10: Dependency audit after A10c closure found the acceptance criterion for unseen runtime drift cannot be proven by selector/pin code alone. A10g now explicitly waits for A10d effective-schema runtime evidence plus A10e file quarantine and RP2 residual verdict routing; sampling may weaken plan-time observation only after both total runtime outcomes exist. This repairs sequencing and does not change the ratified selector or coverage semantics.
- 2026-07-10: Activated after A10e and RP2 closed with parent-observed 883/883 workspace verification. Assigned to `/root/impl_a10`; implementation must remain selector/orchestrator-neutral and prove unseen files enter the same runtime reconciliation, residual, quarantine, package, receipt, and checkpoint path.
- 2026-07-10: Implemented explicit declarative `sample_files = N` with JSON Schema minimum 1, compile-time positive/file-only/binary-only validation, no scaffold/default value, and a private compiled-resource carrier so generic file plans do not acquire sampling policy. Text-row sampling fails with the recorded exclusion rather than reusing binary semantics.
- 2026-07-10: Implemented pure `stratified-hash-v1` selection over only resource id, canonical transport location, and canonical bounded identity bytes. The selector sorts before membership, implements exact K=1/K=2/K>=3 edges, earlier-remainder contiguous interior strata, SHA-256 length-prefix scoring, score/location/identity tie order, and records selected scores/identities/boundaries. Manifest hydration recomputes the selector and rejects forged membership, score, identity, or stratum evidence. Sampling is resolved before probe iteration; initial pins probe only selected candidates, while `M <= N` returns the existing exhaustive path with no selector field or changed exhaustive bytes.
- 2026-07-10: Preserved total runtime behavior. Runtime effective-schema preparation observes every matched candidate even when its discovery participation was `unprobed`, classifies all observations through the existing reconciliation/residual/file-quarantine authority, and leaves the sampled baseline immutable. Sampled coverage is optional typed plan authority and package `schema/per-observation-coercion.json` evidence; exhaustive plans/packages omit the optional field. A three-file end-to-end run pins the first/last sample, routes an incompatible unprobed middle file to `schema-observation:incompatible`, writes quarantine plus processed-observation evidence, obtains a destination receipt, and commits all three exact file positions through the ordinary checkpoint gate.
- 2026-07-10: Added one discovery-coverage report shape for human and JSON rendering across `schema discover`, `schema pin`, `schema show`, `schema diff`, plan/explain `--no-pin`, first-use auto-pin, pinned plan, preview, and run. It names coverage, selector, requested sample size, matched count, probed count, and unprobed count. The runtime preparation adapter returns current manifest evidence without persisting an ordinary-run pin refresh; generic CLI/engine/destination orchestration has no Parquet/Arrow or selector branch.
- 2026-07-10: Focused evidence is green: selector edge/permutation/canonical-score tests 4/4 including 10,000 candidates sampled to 100 under concurrency-budget values 1, 8, and 64; sampled Parquet/Arrow IPC, deterministic bytes, `M <= N` exhaustive compatibility, selected incompatibility no-write, selected budget no-substitution, exact manifest-validation, runtime unseen-quarantine, kernel coverage round-trip, declarative schema/validation, and the full CLI/package/checkpoint scenario all pass. The final all-feature affected matrix passed 576/576 with zero skipped. Strict affected all-target/all-feature Clippy with `-D warnings`, `cargo fmt --all -- --check`, and `git diff --check` passed.
- 2026-07-10: Semver audit with `cargo-semver-checks 0.48.0 --baseline-rev HEAD --all-features` passed `cdf-kernel`, `cdf-project`, and `cdf-engine` at 196/196. `cdf-declarative` passed 195/196; its sole major-class finding is the intentional pre-1.0 addition of public `ResourceDeclaration.sample_files`, the exact new declarative surface this ticket requires. The internal compiled carrier and new project/kernel evidence types produced no other finding.
- 2026-07-10: Limits are explicit. This child exercises local Parquet and Arrow IPC plus a canonical S3 bounded-identity selector golden; remote multi-file enumeration/probing remains WS-E and no live cloud service was contacted. The current local discovery executor is sequential, so scheduling independence is established by selection completing before probe iteration plus permutation and varied executor-budget tests, not by a parallel completion-order harness. Adaptive/statistical sampling, text row sampling, promotion, and distributed scheduling remain excluded. Ticket remains active for parent-owned evidence record, adversarial review, integration reconciliation, and closure; no commit, stage, move, or close was performed by the executor.
- 2026-07-10: Parent integration verification passed 913/913 all-feature workspace tests, strict workspace Clippy, formatting, and diff checks. Evidence: `.10x/evidence/2026-07-10-p2-a10g-rp6-rp7-integration.md`. Adversarial review passed after shared-authority repairs: `.10x/reviews/2026-07-10-p2-a10g-rp6-rp7-integration-review.md`. Acceptance criteria are fully supported; downstream remote enumeration and final parity/conformance retain their existing owners.

## Blockers

None.
