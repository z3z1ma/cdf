Status: done
Created: 2026-07-13
Updated: 2026-07-25
Parent: .10x/tickets/done/2026-07-13-p0-fixed-schema-discovery-stream-admission.md
Depends-On: .10x/tickets/done/2026-07-13-p0-sa0-cold-discovery-final-plan-lifecycle.md, .10x/tickets/done/2026-07-13-p0-sa1-compiled-stream-admission-plan.md, .10x/tickets/done/2026-07-13-p0-sa2-metadata-inventory-observation-cache.md, .10x/tickets/done/2026-07-13-p0-sa3-fused-codec-admission.md, .10x/tickets/done/2026-07-13-p0-sa4-dynamic-producer-admission.md

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
- 2026-07-18: Cross-check while closing F2 direct-destination binding found the broader `cargo test -p cdf-conformance package_replay --locked -j 12 -- --nocapture` filter failing because prepared package replay fixtures still lack the now-required `plan/schema-admission.json` identity artifact (`verified package identity does not contain artifact plan/schema-admission.json`). This is not an F2 regression, but SA5 owns the conformance update: package replay fixtures must include compiled schema-admission evidence or the replay harness must intentionally build legacy-free current-package fixtures.
- 2026-07-18: Repaired the package-replay fixture fossil without adding legacy compatibility. Prepared replay packages now use `cdf_engine::Planner` to compile the validation, scan, and schema-admission artifacts, then write the matching stream-admission evidence, lineage summary, and processed-observation evidence expected by current replay validation. The fixture keeps its intentional synthetic `schema-v1` replay identity through the declared resource descriptor, so replay still proves recorded package authority rather than re-deriving a source schema. This touches conformance fixture construction only; no source/runtime/destination hot path changed.
- 2026-07-25: Audited the complete SA0-SA4 assertion surface rather than treating passing test names as closure evidence. Discovery records file and within-file coverage, probe/source bytes, and cache outcomes separately; execution records physical/useful/logical bytes and reuse through `SourceIoMetrics`; request counters prove metadata-only inventory, bounded discovery, exact same-command spool reuse, and one producer invocation. Jobs, retry/replay, preview/run, residual, and quarantine laws all assert identity or evidence rather than merely successful completion.
- 2026-07-25: The complete source-file and conformance suites passed. No implementation repair was needed in SA5: the closure gap was stale record evidence, not a missing runtime path.

## Blockers

None.

## Evidence

- 2026-07-17 stale source-authority parity:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli validate_deep_rejects_stale_pinned_source_authority_without_runtime_probe --lib --locked -j 12` — passed. The test pins a Parquet discovery snapshot, changes the source root while keeping matching data available, and proves `validate --deep` fails with `source_schema_authority` and performs no package/state/destination writes.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli validate_deep --lib --locked -j 12` — passed, 6 passed. Proves the existing unpinned deep-discovery and malformed/quarantine diagnostics still work after lock hydration.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-cli --lib --locked -j 12 -- -D warnings` — passed.
  - Live playground check: `CARGO_BUILD_JOBS=12 cargo build -p cdf-cli --locked -j 12 && timeout 45s target/debug/cdf --project /Users/alexanderbut/code_projects/tmp validate --deep --json` exited `3` in `real 7.62s` with zero package/destination/checkpoint/schema/lock writes. It now reports stale `source_schema_authority` for `fineweb.documents`, `redpajama.documents`, and `tlc.yellow`; `github.userdata` passes partition/destination checks. Remaining playground failures are configuration facts: `imdb.training_data` redirects to `us.aws.cdn.hf.co`, which is not in that resource's egress allowlist, and `local.events` matches no `*.ndjson` files.
- 2026-07-18 package-replay fixture gap:
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance package_replay --locked -j 12 -- --nocapture` — failed: 2 passed, 9 failed. Failing cases all route through prepared package fixtures whose verified package identity is missing `plan/schema-admission.json`; helper-process crash tests then observe the wrong panic exit code. This is recorded as an SA5 fixture/admission-conformance gap, not as evidence for F2.
  - `CARGO_BUILD_JOBS=12 cargo test -p cdf-conformance package_replay --locked -j 12 -- --nocapture` — passed after fixture repair, 11 passed / 0 failed. Proves prepared package replay, artifact replay, duplicate replay, receipt recovery, helper-process crash recovery, bad recovery inputs, and the negative self-tests all operate on current package fixtures that include fixed-schema admission, stream-admission evidence, lineage, and processed-observation artifacts.
  - `cargo fmt --check` — passed after formatting.
  - `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-conformance --all-targets --locked -j 12 -- -D warnings` — passed.
- 2026-07-25 aggregate fixed-schema admission evidence:
  - `CARGO_BUILD_JOBS=12 cargo nextest run -p cdf-source-files -p cdf-conformance --no-fail-fast` — passed 143/143. This includes payload-free remote inventory, bounded and full-content retained handoffs, transformed-spool reuse, payload-cache hit/corruption/generation-miss behavior, remote Parquet spool/range laws, preview/run parity across registered source archetypes, drift quarantine, package replay, and chaos/retry conformance.
  - `CARGO_BUILD_JOBS=12 cargo nextest run -p cdf-cli -p cdf-project --no-fail-fast` — passed 491/491 in the immediately preceding SA4/SA5 tranche. Relevant assertions prove cold auto-pin without a second preparation pass, exact observation-cache hit/miss I/O, one-transfer weak HTTP spool reuse, jobs-1/jobs-N package identity, REST jobs identity, immutable pins under residual/quarantine drift, and one-invocation Python bootstrap.
  - `remote_inventory_never_reads_payload_for_format_or_compression_detection` asserts zero payload opens during inventory.
  - `remote_observation_cache_exact_hit_avoids_schema_io_and_generation_change_misses` asserts cache-hit discovery performs no GET and records zero discovery source bytes, while a changed generation misses and performs schema I/O.
  - `retained_sequential_window_replays_then_continues_one_source_invocation`, `gzip_parquet_composes_transform_spool_with_registered_format_driver`, and `unversioned_http_parquet_runs_and_commits_terminal_content_identity` assert one source open/transfer, exact retained payload consumption after the plan barrier, zero pending handoffs, and released memory/spill leases.
  - `recorded_http_multifile_packages_are_jobs_invariant` asserts serial and parallel package hashes, canonical segments, profiles, lineage, positions, and terminal schema quarantines are identical while parallel execution actually overlaps source streams.
  - `rest_source_jobs_matrix_preserves_package_receipt_and_checkpoint_identity`, current package-replay conformance, and runtime-chaos duplicate retry assertions cover deterministic receipt/checkpoint identity and no second destination write.
  - `p2_preview_run_parity_law_covers_supported_archetypes` and `p2_s8_multifile_preview_traverses_the_same_planned_partitions_as_run` prove shared admission behavior and multifile partition traversal; the Python cold-run product test proves preview/run planning does not invoke declared producers and bootstrap discovery invokes user code once.

## Review

Fresh adversarial pass inspected the governing spec, telemetry structures, and the assertions behind the aggregate suites. It attempted to falsify closure at the four riskiest seams: discovery/extraction byte accounting, retained-payload ownership, schedule-independent package identity, and replay/quarantine evidence.

Findings:

- No critical or significant finding.
- The spec's “duplicate bounded probe bytes” requirement is satisfied by separate discovery (`probe_bytes_read` / `discovery_source_bytes_read`) and extraction (`SourceIoMetrics`) authorities plus exact request-counter assertions. It does not require retaining every small probe or adding a second identity-bearing counter.
- Full-content inference engines and selective Parquet optimization are not hidden SA5 residuals; their implementation tickets were independently completed or rescoped before this closeout.

Verdict: **pass**. Residual risk is limited to live-provider variability outside the recorded deterministic transport fixtures; transport conformance and nightly live tiers own that risk.

## Retrospective

The final closure work was evidence synthesis, not more source code. The useful discipline was to inspect assertions and authority boundaries before rerunning broad suites: it showed that discovery and extraction measurements are intentionally separate and that exact transport counters supply the cross-phase proof. Future aggregate conformance tickets should name these assertion-level mappings when their implementation children close, preventing a later session from reconstructing them from test names.
