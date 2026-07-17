Status: open
Created: 2026-07-13
Updated: 2026-07-17
Parent: .10x/tickets/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/done/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md, .10x/tickets/done/2026-07-13-p0-sa3-fused-codec-admission.md, .10x/tickets/2026-07-13-p0-sa4-dynamic-producer-admission.md

# P0 SA5: fixed-schema discovery/admission conformance closure

## Scope

Prove cold-freeze and pinned-stream-admission laws across source archetypes, both coverage axes, cache/spool states, preview/run, retry/replay, and residual/quarantine outcomes.

## Non-goals

No implementation repair beyond closure findings.

## Acceptance criteria

- Transport/process counters distinguish inventory, bounded probes, full payload transfer, duplicate bounded bytes, and same-command spool reuse.
- Preview/run share admission semantics and do not duplicate source execution.
- Jobs 1/N, cache hit/miss, and retry/replay retain deterministic package identity.
- Adversarial review passes with every finding resolved or durably accepted.

## References

- `.10x/specs/schema-discovery-and-stream-admission.md`

## Assumptions

None beyond referenced completed children.

## Journal

- 2026-07-17: Live G4 Hugging Face mirror setup exposed validate/run parity cases that SA5 must cover. `validate --deep` accepted a stale/disposable project whose pinned schema/source authority no longer matched the current resource root and accepted an older schema artifact-version state that `run` rejected later. These are not G4 performance blockers, but they violate the SA5 law that preview/validate/plan/run share the same fixed-schema admission authority and that a clean deep validation cannot miss a run-visible schema-authority error.
- 2026-07-17: Repaired the stale source-authority parity slice. `validate --deep` now hydrates locked schema snapshots like plan/preview/run and invokes the same pinned snapshot source-driver/version/discovery-plan authority check before source runtime resolution or fixed-schema preflight. A stale pin now reports `source_schema_authority` with the same compiled/recorded authority mismatch that `run` would reject, and the affected resource does not contact/resolve the runtime path under the stale authority. This is a correctness/diagnostic repair only; it does not change package-producing hot paths.

## Blockers

Depends on SA0-SA4.

## Evidence

- 2026-07-17 stale source-authority parity:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli validate_deep_rejects_stale_pinned_source_authority_without_runtime_probe --lib --locked -j 12` — passed. The test pins a Parquet discovery snapshot, changes the source root while keeping matching data available, and proves `validate --deep` fails with `source_schema_authority` and performs no package/state/destination writes.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli validate_deep --lib --locked -j 12` — passed, 6 passed. Proves the existing unpinned deep-discovery and malformed/quarantine diagnostics still work after lock hydration.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli --lib --locked -j 12 -- -D warnings` — passed.
  - Live playground check: `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --locked -j 12 && timeout 45s target/debug/cdf --project /Users/alexanderbut/code_projects/tmp validate --deep --json` exited `3` in `real 7.62s` with zero package/destination/checkpoint/schema/lock writes. It now reports stale `source_schema_authority` for `fineweb.documents`, `redpajama.documents`, and `tlc.yellow`; `github.userdata` passes partition/destination checks. Remaining playground failures are configuration facts: `imdb.training_data` redirects to `us.aws.cdn.hf.co`, which is not in that resource's egress allowlist, and `local.events` matches no `*.ndjson` files.
- This is partial SA5 evidence only. Transport/process counters, preview/run source-execution counters, jobs/retry/replay package identity, residual/quarantine serialization, and adversarial closeout remain open.

## Review

Pending.

## Retrospective

Pending.
