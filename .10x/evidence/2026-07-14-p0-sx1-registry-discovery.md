Status: recorded
Created: 2026-07-14
Updated: 2026-07-14

# SX1 registry-driven project and CLI discovery

## Observation

Project schema discovery now accepts only the neutral `SourceRegistry`, the exact `CompiledSourcePlan` already produced for the command, and an injected `SourceResolutionContext`. The source driver owns candidate inventory and bounded observation; project owns deterministic selection, reconciliation, manifests, snapshots, cache authority, normalization, and effective-schema evidence. `cdf add`, schema discover/pin/diff/promote, first-run auto-pin, and the discovery portion of deep validation use that path without file/REST/Postgres dispatch. First-run execution rebinds compiler-owned schema authority onto that plan without re-invoking the driver; pinned execution verifies driver id, driver version, and physical-plan hash before source resolution.

The generic artifact records source driver id/version/plan hash. Single-candidate discovery preserves trusted physical field metadata. Candidate size and payload-byte accounting remain independent: catalog discovery can truthfully record unknown object size and zero payload bytes. The observation-cache result boxes the large hit payload rather than inflating every miss value on the stack.

Operational candidate locations remain available only to the driver during the command. Durable candidate and observation evidence uses one typed redacted location implementation that removes URI userinfo, fragments, and query values; candidate validation recomputes that projection and cache-entry validation rejects any URI-shaped evidence retaining userinfo, a non-redacted query, or a fragment before a hit is admitted. Driver-provided evidence is namespaced below `driver.*`, so it cannot replace framework-owned driver, transport, coverage, cache, or manifest authority. Malformed strong cache identities now fail before the unknown-size cache-bypass branch.

## Procedure

- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli schema_discover_ --locked --no-fail-fast` passed all three selected local-Parquet, REST, and live-Postgres catalog cases. The cases also assert no project/package/destination/checkpoint writes and secret redaction.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project project_external_codec_discovers_pins_previews_and_runs_over_remote_provider --locked --no-fail-fast` passed. The external format is reached through the file source driver and neutral source registry, then pins, previews, packages, receipts, and checkpoints.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project observation_cache --locked --no-fail-fast` initially passed six selected cache/bounded-I/O tests; after adversarial repair it passed seven, adding rejection/removal of a syntactically valid cache artifact containing unredacted URI evidence.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project discovery_manifest_is_canonical_content_addressed_and_fail_closed --locked --no-fail-fast` passed, including an observed metadata candidate with unknown size and zero payload bytes.
- The selected `add_local_parquet`, `add_rest`, local Parquet auto-pin/run, and NDJSON auto-pin/run CLI cases passed.
- Exact legacy discovery regression cases for verified-baseline identity, object-store multi-file aggregation, sampled budget failure without candidate substitution, and bounded format-confirmation failure passed after the shared adapter refactor.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-project -p cdf-cli --all-targets --no-deps --locked -- -D warnings` passed.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-runtime source_registry --locked` passed four selected boundary tests, including ordering/duplicate rejection, budget/identity drift, URI evidence redaction/forgery rejection, and the registry compile/resolve case. The latter proves schema rebinding preserves the exact compiled-source hash, rejects mutation of non-schema resource authority, and changes the stable hash when redacted options change even if the physical payload does not.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-project terminal_evidence_tests --locked` passed four tests, including driver-evidence namespace isolation and fail-closed malformed strong identity.
- `CARGO_BUILD_JOBS=12 cargo test -p cdf-cli source_authority_tests --locked` passed the pinned-plan mismatch rejection. The local Arrow IPC discover/pin/preview/run case and byte-stable first-run auto-pin case passed against the new source snapshot authority; the HTTP Parquet add-and-run S1 case also passed.
- `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-kernel -p cdf-runtime -p cdf-source-files -p cdf-project -p cdf-cli --all-targets --no-deps --locked -- -D warnings` passed after the authority and evidence repairs.
- `cargo fmt --all` and `git diff --check` passed.

A broad `cdf-project` library run observed 177/187 passing before the focused repairs. Four discovery failures introduced or exposed by the shared-adapter refactor were repaired and rerun exactly. The remaining failures were already-active runtime/golden work: compiled admission/quarantine agreement, processed-position aggregation, telemetry version expectation, zero-segment failpoint ordering, and the separate pinned-HTTP pre-probe lifecycle. No clean full-suite claim is made here.

## What it supports or challenges

This supports the SX1 requirement that a source can inherit project/CLI discovery behavior without adding source-kind branches to those command paths. It also closes the catalog zero-byte manifest bug, prevents discovery/execution plan recompilation drift, protects framework evidence authority, and preserves deterministic multi-candidate selection and physical provenance.

It challenges closure of SX1 as a whole: the closed declarative `SourceDeclaration`/`CompiledResourcePlan`, legacy project discovery entry points, declared-file preparation, file-specific deep reconciliation, doctor/add suggestion hooks, and Python CLI special case remain. The ticket therefore stays active.

## Limits

This is focused milestone evidence, not the P3 throughput envelope and not SX1 closure. Network-live behavior is represented by local HTTP fixtures and the live Postgres test; public-endpoint smoke and build-graph closure remain separate acceptance work.
