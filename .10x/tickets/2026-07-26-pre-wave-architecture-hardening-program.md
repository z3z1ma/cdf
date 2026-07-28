Status: active
Created: 2026-07-26
Updated: 2026-07-27

# Pre-wave architecture hardening program

## Scope

Complete the second-order architecture CDF needs before the next source/destination wave:
reusable built-in catalog composition, compiler-enforced safety walls, an explicit driver
concurrency canon, proven catalog-task source commons, destination common services, an honest
environment error taxonomy, and one holistic typed-report CLI experience.

This is a parent plan and prioritization authority, not an executable implementation ticket.

## Workstreams and sequence

### WS-A — Enforced product boundaries

1. `.10x/tickets/done/2026-07-26-prewave-a1-builtin-driver-catalog.md`
2. `.10x/tickets/done/2026-07-26-prewave-a2-rust-safety-lint-walls.md`
3. `.10x/tickets/done/2026-07-26-prewave-a3-driver-concurrency-conformance.md`

A1 creates one reusable first-party catalog authority without moving composition into neutral
runtime. A2 turns safety and panic posture into compiler gates. A3 makes the existing source and
destination concurrency model executable authoring law.

### WS-B — Source archetype hardening

1. `.10x/tickets/done/2026-07-26-prewave-b1-typed-task-set-reader.md`
2. `.10x/tickets/done/2026-07-26-prewave-b2-spill-task-planning-lifecycle.md`
3. `.10x/tickets/done/2026-07-26-prewave-b3-file-runtime-modularization.md`

B1 and B2 extract only the concrete duplicated external-task machinery already proven by Iceberg
and Glue. B3 removes the 9,138-line file-source monolith as the misleading authoring template.
No universal catalog semantic model is introduced.

### WS-C — Destination archetype hardening

1. `.10x/tickets/done/2026-07-26-prewave-c1-receipt-clock-authority.md`
2. `.10x/tickets/done/2026-07-26-prewave-c2-sql-mirror-commons.md`
3. `.10x/tickets/done/2026-07-27-prewave-c1b-promotion-receipt-clock-injection.md`

C1 eliminates receipt assembly and process-wall-clock duplication. C2 centralizes typed SQL mirror
lifecycle without erasing native transaction/dialect behavior.

### WS-D — CLI authority and experience

1. `.10x/tickets/done/2026-07-26-prewave-d1-environment-error-taxonomy.md`
2. `.10x/tickets/done/2026-07-26-prewave-d1b-adapter-error-audit.md`
3. `.10x/tickets/done/2026-07-26-prewave-d1c-product-error-audit.md`
4. `.10x/tickets/done/2026-07-26-prewave-d2-typed-cli-report-authority.md`
5. `.10x/tickets/done/2026-07-26-prewave-d3-holistic-cli-experience.md`

The error taxonomy lands before the visual pass so the renderer does not polish incorrect
diagnostics. Typed report authority lands before the holistic pass so one renderer-side change
governs every command rather than reopening command execution modules.

### WS-Z — Extension proof and aggregate closure

1. `.10x/tickets/2026-07-26-prewave-z1-extension-authoring-proof.md`

The final child performs the source/destination authoring exercise, build-graph/unsafe scans,
focused product smoke, performance regression check, record/reference audit, and adversarial
program review.

## Acceptance criteria

- Every executable child is terminal with mapped evidence, retrospective, and adversarial review.
- A first-party source or destination catalog entry is added in one leaf plus one data-driven
  conformance fixture; generic project/runtime/CLI command modules remain unchanged.
- Iceberg and Glue share task-set retention/spill machinery without sharing source-specific
  snapshot, authorization, delete, schema, or reader semantics.
- DuckDB/Postgres share receipt and mirror lifecycle while all three destinations use one receipt
  assembly and injected clock authority.
- Workspace safety walls reject unsafe code outside named FFI modules and reject unchecked
  production unwrap/expect in the named foundational crates.
- Every CLI command has one typed report authority; environment errors are distinct from internal
  defects; the holistic renderer passes TTY/headless/JSON/redaction/performance conformance.
- Existing package/plan/receipt/checkpoint identities and P3 performance floors do not regress.
- The active ticket graph returns to zero and the roadmap/coverage matrix reflects the terminal
  state before a new feature wave starts.

## Non-goals

- No dynamic plugin ABI, linker inventory, universal catalog client, universal SQL executor, ORM,
  or destination/source mega-trait.
- No new source, destination, format, or product feature.
- No compatibility shim, legacy report path, second registry authority, or identity artifact
  version.
- No removal of useful concrete first-party examples from dependency-light CLI help.
- No unsafe FFI rewrite without measured need.

## References

- `.10x/decisions/builtin-driver-catalog-composition.md`
- `.10x/decisions/driver-session-concurrency-canon.md`
- `.10x/decisions/compiler-enforced-rust-safety-walls.md`
- `.10x/specs/catalog-task-source-commons.md`
- `.10x/specs/destination-common-services.md`
- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/knowledge/source-destination-extension-invariant.md`

## Assumptions

- User-ratified: execute the complete recommended pre-wave program.
- Record-backed: current package/plan/receipt/checkpoint identities and measured performance are
  regression floors, not refactoring freedoms.
- Source-backed: `cdf-project` production and `cdf-cli-core` are already concrete-adapter-free;
  this program preserves those working boundaries.

## Journal

- 2026-07-26: Activated after zero-backlog stabilization and CPU-saturation closure. Fresh source
  inspection confirmed the task-reader/mirror/receipt duplication, 9,138-line file runtime,
  undocumented concurrency law, absent workspace lint walls, 1,094 `CdfError::internal` call
  sites, and partial—not absent—typed report architecture. The internal-error audit is split by
  foundation, adapter, and product ownership so no one ticket hides a thousand-site semantic
  sweep. The graph is shaped around those exact facts rather than the audit's overstated
  production dependency and CLI-string findings.
- 2026-07-26: Fresh-hat shaping review challenged invalid dependency claims, speculative
  mega-traits, stringly SQL, clock placement, FFI exceptions, release feature forwarding, ticket
  cardinality, and authority conflicts. It retained concrete help examples, chose typed task and
  mirror mechanisms, reused `ExecutionHost::unix_now`, split the error audit into three bounded
  owners, and updated every conflicting active spec. Fourteen child references and all dependency
  paths resolve. `graphify-out/graph.json` exists, but the `graphify` executable is unavailable in
  this environment; verification used the current source/import/Cargo graph directly.
- 2026-07-26: A1 closed with one process-scoped `cdf-builtin-drivers` catalog leaf, all-feature
  Cargo-metadata boundary gates, complete catalog artifact hashes, shared
  product/benchmark/conformance/project-test consumption, and a final delegated OCR review pass.
- 2026-07-26: A2 closed after three delegated OCR passes. All 51 crates inherit workspace safety
  lints; unsafe allowances, functions, caller contracts, blocks, macro tokens, impls, and traits
  form an AST-enforced closed inventory; eight foundational crates deny production unwrap/expect;
  poison handling fails closed; and strict all-target/all-feature workspace Clippy passes.
- 2026-07-26: A3 closed after three delegated OCR passes. Public authoring docs and executable
  fixtures now enforce positive driver/staged/host bounds, legitimate non-`Send` finalized
  lifecycles, capability-selected ingress, contact-free compile, and fail-closed portable worker
  admission without destination identity branches.
- 2026-07-26: B1 closed with one typed external-task reader shared by Iceberg and Glue. It owns
  encoded/decoded retention and identity checks while adapters retain catalog, position, schema,
  retry, and partition semantics. Review-driven repairs preserve each adapter's memory admission
  policy and share the whole retained payload under one parse lease; source suites and a
  five-sample baseline comparison pass.
- 2026-07-26: B2 closed with one typed ordered/canonical task-planning lifecycle and one accounted
  source-index workspace envelope. Glue now delegates its provider-order index to the shared
  builder; Iceberg retains its manifest/delete index semantics while sharing resource ownership
  and final publication. Three OCR rounds repaired memory deadlock, spill retry, duplicate,
  identity, cancellation, and hot-path issues; source suites and a same-cell 5,000-task comparison
  pass.
- 2026-07-26: B3 closed after replacing the 9,139-line file runtime with an explicit acyclic
  internal module graph. Delegated review drove the split from lexical files to compiler-visible
  ownership boundaries; 48 focused tests, strict Clippy, and matched TLC/FineWeb profiles passed
  within the 10% variance envelope. A broader project-suite timing failure was reproduced at the
  exact pre-B3 baseline and recorded as an integration limit rather than misattributed.
- 2026-07-26: C1 closed with one typed ordinary/correction receipt finalizer and injected host-clock
  authority across DuckDB, Postgres, and Parquet. Seven review findings repaired string-map and
  migration authority leaks, historical acknowledgement ordering, zero-data validation, byte-count
  drift, and premature durable marker publication. Contract, runtime, adapter, replay/crash,
  strict-lint, formatting, and two independent final review gates pass.
- 2026-07-27: D1 closed with an ownership-based `Environment` kind, stable CLI mapping and
  recursive redaction, production host/data/private-scratch classifiers, safe journal-free SQLite
  admission, typed codec/writer error preservation, generated reference freshness, foundational
  tests/strict lints, durable knowledge, and two independent delegated review passes.
- 2026-07-27: D1's broader product gate exposed a direct schema-promotion path that bypasses C1's
  execution-service binding. The existing C1 semantics are unchanged; bounded follow-up C1b now
  owns the two reproducing crash/multi-target tests before aggregate closure.
- 2026-07-27: C1b closed with one neutral destination bind boundary, post-bind capability/lane
  validation, recoverable facade caching, actual spill-authority transfer for DuckDB, and direct
  promotion binding for only selected targets. The full 272-test CLI gate, adapter/resource
  regressions, strict lint/format gates, and two independent final delegated reviews pass.
- 2026-07-27: D1b closed after a complete 135-file adapter audit. Host/process/local-I/O failures
  now remain distinct from external source data, durable destination artifacts, private scratch,
  remote providers, and CDF invariants across subprocess, HTTP, Parquet, DuckDB, file source, and
  Iceberg wrappers. Review-driven source-chain and provenance repairs pass focused/full suites,
  strict lint, two independent final reviews, and are distilled into error-ownership knowledge plus
  the mirrored `audit-error-ownership` skill.
- 2026-07-27: D1c closed with a durable 203-file/345-constructor product inventory, explicit
  configured-versus-managed path provenance, typed private-state validation without unbounded
  ordinary-open scans, no-follow immutable publication, nested typed-wrapper preservation, 67
  state tests, 297 CLI tests, strict affected-root lint, and two bounded final reviewer PASS
  verdicts. The audit skill now requires a durable per-site ledger and one frozen review batch.
- 2026-07-27: D2 closed with one typed serializable success authority per command, report-owned
  renderer modules, stable transparent/flattened JSON shapes, redacted report projections, and an
  all-non-renderer static fence. The 297-test CLI suite, 53-test CLI-core suite, strict Clippy,
  80/84-package core graphs, focused parity repairs, and bounded OCR review pass. Report-authority
  knowledge plus a mirrored audit skill preserve the procedure for D3 and future commands.
- 2026-07-27: D3 closed by reusing WS9's accepted renderer/hosted authority, centralizing five
  cross-family headings, adding a nine-family terminal-policy matrix and complete 10,000-row
  lifecycle benchmark, and recording fresh local/public-HTTPS product smokes. The bounded OCR
  review produced no critical/high findings; one integration-matrix limit is explicitly accepted
  and the benchmark blind spot was repaired once without another review cycle.

## Blockers

None.

## Evidence

Pending child execution.

## Review

Pending aggregate closure.

## Retrospective

Pending aggregate closure.
