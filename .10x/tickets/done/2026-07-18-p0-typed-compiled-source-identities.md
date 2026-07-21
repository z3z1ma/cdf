Status: done
Created: 2026-07-18
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-18-p0-post-iceberg-integration-stabilization.md
Depends-On: .10x/tickets/done/2026-07-18-p0-external-partition-authority.md

# P0: type compiled source identities

## Scope

Replace interchangeable strings for discovery binding, complete compiled source plan, physical plan, schema binding, and source semantics with distinct validated identity types and one required construction/binding path.

## Acceptance Criteria

- The compiler rejects identity-category substitution.
- Optional/single ambiguous setters are absent.
- Every source driver binds discovery and execution identities through one shared typed API.
- Partition observation/source bindings cross runtime and worker-protocol boundaries as `SchemaObservationBinding`, never an unvalidated `String`.
- Lifecycle and forgery tests cover cold discovery, pinned execution, replay, and external tasks.

## References

- `.10x/tickets/done/2026-07-18-p0-file-inventory-discovery-identity-regression.md`

## Assumptions

- User-ratified: stringly typed authority caused a proven P0 regression and is not an acceptable steady state.

## Journal

- 2026-07-18: Activated after the closed partition-authority and discovery-inventory regression tickets. The immediate code still accepts two correctly named but interchangeable `String` arguments in `FileResource::with_compiled_source_identities`; this ticket removes that remaining category-substitution hazard at the runtime/compiler boundary.
- 2026-07-18: Removed the registry's copied executable-schema authority. Resolved resources must now expose and exactly match the compiler-owned compiled-plan identity, effective schema runtime, baseline observation catalog, descriptor, schema, capabilities, and allowances. Registry wrappers validate and delegate; they no longer manufacture missing observation bindings. File, REST, Postgres, Python, Iceberg, and Glue adapters now carry compiler authority explicitly, and source-owned planning binds each partition observation.
- 2026-07-18: Replaced partition observation/source binding strings throughout the runtime and portable worker protocol with validated `SchemaObservationBinding`. `StreamAdmissionCompletion::Pending` replaces the former invalid empty-string placeholder. The exact workspace barrier completed 1,771/1,771 green and strict all-target Clippy passed.

## Blockers

None. The partition-authority dependency is closed.

## Evidence

- Compiler/source identity tests reject swapped discovery, complete-plan, physical-plan, semantics, and observation identities before source contact.
- Cold discovery to pinned execution, remote retained-inventory reuse, forged external-task authority, and portable worker protocol round-trip tests passed in the workspace barrier.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo nextest run --workspace --locked -j 12 --no-fail-fast` ran 1,771 tests: 1,771 passed, 40 explicitly skipped.
- `CARGO_BUILD_JOBS=12 DUCKDB_DOWNLOAD_LIB=1 cargo clippy --workspace --all-targets --locked -j 12 -- -D warnings` passed.

## Review

Verdict: pass. A fresh-hat sequential review followed identity construction from compiler output through registry resolution, each first-party source, external task authority, partition observation, execution, and worker serialization. Distinct newtypes prevent category substitution, registry code validates and delegates source-owned facts, and no ambiguous optional hash setter remains.

Residual risk: post-construction partition-authority mutation was closed by `.10x/tickets/done/2026-07-18-p0-source-planning-authority-closure.md`. The collaboration thread limit prevented commissioning a new independent agent without reusing an old reviewer.

## Retrospective

Correct names on `String` parameters did not create type safety. The discovery-to-pinned regression was enabled by structurally interchangeable hashes and a registry that could manufacture source facts. Identity categories now enter through one compiler-owned bundle and remain typed across serialization boundaries.
