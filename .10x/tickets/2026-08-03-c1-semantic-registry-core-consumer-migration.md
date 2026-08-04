Status: done
Created: 2026-08-03
Updated: 2026-08-03
Parent: `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`

# C1 semantic registry core and consumer migration

## Scope

Implement the canonical semantic-reference and data-only registry kernel, the six ratified
built-in definition families, exact resolution/validation/hash behavior, and direct migration of
all current producers and behavior consumers.

The executor owns the smallest acyclic crate/module placement supported by the current workspace
dependency graph. The intended boundary is a focused data-only `cdf-semantic` crate below
contract/declarative/project/adapters and above `cdf-kernel`; a materially smaller boundary is
acceptable only if it preserves one resolver without moving project/driver code into the kernel.

## Non-goals

- project-defined semantic definition files (C2);
- lock/manifest snapshot publication (D1);
- SQL semantic annotation syntax (D3);
- Python/Wasm predicates or externally loaded registries;
- broad destination mapping rewrites unrelated to existing semantic behavior.

## Acceptance criteria

1. Canonical reference parsing/rendering enforces the active grammar, parameter schema, ordering,
   bounds, and round-trip laws; aliases and unversioned forms do not exist.
2. Built-in catalog construction validates uniqueness and produces stable definition hashes for
   `cdf.variant@1`, `cdf.package_row_ordinal@1`, parameterized `cdf.pii@1`, and all three
   PostgreSQL exact-text definitions.
3. Every current producer writes only the canonical references. Old semantic strings and the
   configurable variant invalid state are removed directly with no compatibility reader.
4. Declarative/authored unknowns fail `Contract`; source-observed unknowns fail `Data`; compiled
   runtime absence fails `Internal` at the appropriate existing boundaries.
5. Contract redaction uses resolved privacy classification rather than `pii:` prefix inference and
   preserves all current PII verdict actions across Arrow types.
6. Variant/package-row-ordinal ownership uses resolved canonical definitions plus existing shape
   fences and cannot be forged by an unrelated semantic.
7. PostgreSQL JSON/JSONB/NUMERIC exact-value discovery, mapping, physical-provenance validation,
   binary COPY, replay, and correction behavior remain lossless through adapter-owned mapping
   profile ids; unknown semantics do not silently invoke native reconstruction.
8. Destination semantic mapping resolution is deterministic, most-specific, ambiguity rejecting,
   and permits base Arrow fallback only when the definition says so.
9. Focused tests cover valid/invalid grammar, hash determinism, unknown ownership, direct migration,
   redaction equivalence, destination ambiguity, and PostgreSQL exact-value equivalence.
10. Formatting, `git diff --check`, affected-crate tests/checks, and affected-crate strict Clippy
    pass. No whole-workspace test suite is run.

## References

- `.10x/specs/semantic-type-registry.md`
- `.10x/decisions/semantic-reference-registry-and-unknown-policy.md`
- `.10x/research/2026-08-03-semantic-authority-inventory.md`
- `.10x/knowledge/net-new-no-compatibility-policy.md`
- `.10x/knowledge/type-policy-authority.md`

## Assumptions

- Canonical grammar, built-in ids, fail-closed unknown behavior, project-definition staging, and
  mapping-selector boundary are user-ratified in the governing records.
- Arrow remains the canonical type system; semantics cannot change physical values at runtime.
- Existing PII actions and PostgreSQL exact-value fidelity are behavior to preserve, not legacy
  spellings to preserve.

## Journal

- 2026-08-03: Opened after C0 inventory and user ratification. No product code changed in this
  shaping turn.
- 2026-08-03: Execution started on `main`. The user reiterated that every legacy spelling and
  compatibility path must be deleted rather than supported. The scoped error audit covers the new
  semantic crate plus changed contract/declarative/package/Postgres source/destination boundaries;
  authored invalidity is Contract, unapproved source metadata is Data, and impossible compiled
  absence is Internal.
- 2026-08-03: Added the canonical `SemanticReference`/parameter grammar to `cdf-kernel` and a
  focused data-only `cdf-semantic` crate. The registry validates definitions before admission,
  hashes their canonical serialized descriptors, composes project definitions with built-ins, and
  resolves field compatibility, privacy classification, and most-specific destination mappings.
  The initial six definitions are `cdf.variant@1`, `cdf.package_row_ordinal@1`, parameterized
  `cdf.pii@1`, and PostgreSQL JSON/JSONB/NUMERIC exact-text definitions.
- 2026-08-03: Migrated declarative compilation, contract redaction/variant ownership, package-row
  ownership, Postgres discovery and native reconstruction, engine variant producers, destination
  preflight, and all affected fixtures directly. Deleted the old Postgres constants and the
  configurable variant semantic field; no aliases, compatibility readers, or permissive unknown
  fallback remain. The package golden identity intentionally changed because the package-row
  semantic is artifact-bearing.
- 2026-08-03: A build-graph test caught an upward dependency from `cdf-package-contract` to
  `cdf-semantic`. Repaired the layering instead of exempting it: kernel owns only canonical
  reference syntax and the framework ordinal id, while `cdf-semantic` owns the full definition.
- 2026-08-03: The user clarified that project authors must be able to define parameterized types
  such as `finance.currency@1(code="USD")`. The public descriptor/catalog model already composes
  those definitions with built-ins. Closed the authoring boundary further: malformed exact Arrow
  selectors, noncanonical/disallowed mapping parameters, empty or duplicate metadata predicates,
  undeclared validation/equivalence parameters, malformed related references, and fail-open PII
  classifiers now fail catalog construction. A decimal currency definition with USD/EUR
  constraints and a USD-specific destination profile proves the extension seam. Loading project
  files and binding their snapshot remains C2, without requiring a registry-model redesign.
- 2026-08-03: Completed the scoped error-ownership audit. The reproducible manifest, seven-site
  ledger, and classification note live under
  `.10x/evidence/2026-08-03-c1-semantic-error-ownership.md`; all new `Internal` sites are owned
  CDF invariants, while authored and observed invalidity remain Contract and Data respectively.
- 2026-08-03: Attempted the required post-edit `graphify update .`; the local `graphify`
  executable is unavailable (`command not found`), so no graph artifact was regenerated.
- 2026-08-03: Fast Quality run `30871800891` passed locked metadata, formatting, core Clippy,
  secret scanning, and every core library target except one stale `cdf-package` fixed-fixture hash.
  CI and a local isolated rerun both produced
  `sha256:f2d84641b917a373ad126b809fe314f08f07f67b23687e49152653d1c611f5ba` after the intentional
  package-row semantic identity change. Updated only that expected artifact identity; the test's
  first-vs-second build equality remained its independent determinism proof, and the isolated
  locked rerun passed 1/1.
- 2026-08-03: The single independent red-team pass returned `fail` with four significant and no
  critical findings: JSON-unaware reference delimiters/whitespace, permissive textual exact-Arrow
  selectors plus lossy observed-type reconstruction, suppressed residual unknown-semantic errors,
  and active records still mandating deleted spellings. The user had authorized closure repairs
  and prohibited an infinite review cycle, so all four were repaired together and validated once.
- 2026-08-03: Canonical references now rely on JSON parsing plus final render equality rather than
  scanning inside JSON strings; spaces, parentheses, commas, and equals signs inside canonical JSON
  strings round trip while syntactic whitespace still fails. Exact Arrow selectors now store
  structured `CanonicalArrowType`, eliminating aliases/casing from definition identity.
  `ObservedField` carries exact canonical Arrow authority in addition to its deliberately coarse
  contract classification; missing, invalid, or contradictory authority fails `Data`, and an exact
  UTF-8 semantic cannot validate `LargeUtf8`.
- 2026-08-03: Residual candidates now resolve semantics exactly once under `Observed` authority
  before capture/quarantine classification, reuse the resulting redaction decision for encoding,
  evidence, and quarantine, and propagate unknown/malformed references as `Data`. This removes the
  suppressed-error path without adding per-use registry work.
- 2026-08-03: Updated all affected active specs and reissued the three accepted decisions whose
  closed classifiers still named historical strings. Their prior text is preserved in
  `.10x/decisions/superseded/`; active authority names only canonical semantic references, while the
  registry spec labels old spellings as rejected migration history rather than current authority.

## Blockers

None.

## Evidence

1. Canonical grammar: `cargo test -p cdf-kernel semantic` passed all three selected tests,
   including canonical render/serde round trips and explicit rejection of unversioned, aliased,
   duplicate, unordered, whitespace-bearing, null, array, and noncanonical references.
2. Registry/built-ins/custom seam: `cargo test -p cdf-semantic` passed 6/6. It proves six unique
   deterministic definition hashes, parameter enforcement and authority ownership, Arrow/
   nullability/metadata fences, mapping specificity/ambiguity rejection, project+built-in
   composition, constrained `finance.currency@1(code="USD")`, and malformed project descriptor
   rejection.
3. Direct producer/consumer migration: source search finds old spellings only in negative rejection
   tests and governing historical records. `cdf-postgres` no longer defines or reexports the old
   constants; `VariantColumnSpec` no longer admits a semantic override; active `VISION.md`
   examples use canonical references and resolved privacy.
4. Error ownership: `.10x/evidence/2026-08-03-c1-semantic-error-ownership.md` freezes seven scoped
   behavior files, six justified `Internal` construction sites, and one test assertion. Authored
   unknowns are Contract, observed unknowns are Data, and compiled absence is Internal.
5. Contract behavior: final `cargo test -p cdf-contract` passed 96 tests with 2 deliberate performance
   tests ignored. This includes registry-based PII redaction, exact framework variant recognition,
   recursive destination semantic preflight, and unchanged verdict actions.
6. Package/engine ownership: `cargo test -p cdf-package-contract` passed 17 unit tests plus its
   build-graph integration test after the acyclic boundary repair. Targeted engine variant,
   redaction, schema-admission, and package-evidence tests passed. The golden package hash changed
   from `5009…` to `13dac4da32ed673cbfbf4e0074da28abf5ca3a14760fdbf2055dad584b4f5817`;
   isolated one-batch/many-batch equality proved determinism before updating the expected identity.
   CI surfaced the independent `cdf-package` fixed fixture, whose repeated-build equality likewise
   proved the new stable identity
   `sha256:f2d84641b917a373ad126b809fe314f08f07f67b23687e49152653d1c611f5ba`; its single locked test
   passed after the expected hash was updated.
7. PostgreSQL exact fidelity: `cargo test -p cdf-dest-postgres exact_` passed 8/8, including live
   exact/replay/correction coverage. With the existing local DuckDB dylib made visible to the
   linker, `cargo test -p cdf-conformance postgres_exact` passed the live binary-source to native-
   destination JSON/JSONB/NUMERIC equivalence case. `cargo test -p cdf-source-postgres` passed 25
   tests with its opt-in live test ignored.
8. Destination resolution: the semantic crate mapping test proves most-specific selection and
   equal-specificity rejection; seven contract destination-mapping tests and governed SQLite/
   DuckDB variant tests passed; Postgres exact mapping is selected only through adapter-owned
   profile ids and unknown semantics fail closed.
9. Focused migration coverage also passed for `cdf-declarative` (22), `cdf-package-contract` (17),
   affected engine selectors, and the exact Postgres conformance path. No whole-workspace test suite
   was run.
10. Quality: explicit affected-package `cargo check --tests` and strict Clippy covered
    `cdf-kernel`, `cdf-semantic`, `cdf-contract`, `cdf-declarative`, `cdf-package-contract`, both
    Postgres adapters, `cdf-engine`, SQLite/DuckDB destinations, `cdf-project`, `cdf-cli`, and
    `cdf-conformance`. The final kernel/semantic/contract strict-Clippy delta passed after the
    custom-definition closure. `cargo fmt --all -- --check` and `git diff --check` pass.
11. Red-team repair validation: the kernel semantic selector passed with canonical JSON strings
    containing spaces and delimiters; `cdf-semantic` passed 6/6 with exact structured Arrow
    selectors; the new contract exact-Arrow test passed and the full contract crate remained green;
    the engine unknown-residual ownership test and existing residual capture/redaction/control test
    both passed; engine strict Clippy passed. The project lockfile semantic-diff test passed after
    exposing the already-built local DuckDB dylib to the linker. No whole-workspace test ran.

## Review

One independent read-only red-team review inspected commit `17c74ffe`, the governing records, and
actual source. Its pre-repair verdict was `fail`, with four significant findings and no critical
findings:

1. JSON string content was incorrectly treated as reference syntax. Resolved by removing global
   whitespace/interior-parenthesis scans and retaining parser-level delimiter checks plus canonical
   render equality; delimiter-bearing JSON strings now round trip in tests.
2. Textual exact Arrow patterns admitted aliases into hashes, and `ObservedSchema` collapsed exact
   UTF-8 variants. Resolved by replacing string selectors with structured `CanonicalArrowType` and
   carrying/validating exact observed authority alongside coarse contract classification. Tests
   prove `LargeUtf8` cannot satisfy exact `Utf8`.
3. Residual redaction suppressed registry errors. Resolved by making the shared field helper typed
   and fallible and resolving each residual once under `Observed`; the focused engine test proves
   an unknown semantic exits as `Data`.
4. Active specs/decisions conflicted with the direct migration. Resolved by updating active specs
   and reissuing/superseding accepted decisions; old spellings remain only explicit rejected
   migration examples, negative tests, or terminal history.

Post-repair closure verdict: **pass**. Each finding maps to source change plus focused evidence; no
finding is waived and no residual correctness risk is accepted. Per the user's explicit direction,
there is no second review cycle.

## Retrospective

- The existing package-crate layer fence was valuable: canonical syntax belongs in the kernel,
  while behavioral definitions belong in the registry. Keeping those separate made the package
  artifact independently parseable without pulling policy upward.
- Artifact-bearing semantic metadata correctly changes schema/package identity. Isolating the
  golden comparison distinguished an intended hash update from nondeterministic packaging.
- The user-defined currency scenario exposed the right final hardening target: validating a
  definition means rejecting dead selectors and fail-open classifiers at registration, not merely
  validating references later. The same public descriptor now serves C2 directly.
- The local no-run Postgres link initially could not find `libduckdb`; using the already-built
  `target/debug/deps/libduckdb.dylib` through `LIBRARY_PATH`/`DYLD_LIBRARY_PATH` recovered the
  targeted validation without broad rebuilding or weakening tests.
- Canonical syntax must distinguish grammar whitespace/delimiters from the contents of a parsed
  scalar. Canonical render equality is both simpler and more correct than pre-scanning JSON text.
- Definition identity should store typed authority, not a string accepted by a permissive parser.
  Reusing `CanonicalArrowType` removed an alias-normalization problem and preserved the project's
  single Arrow type system.
- The residual repair improved correctness without trading throughput: semantic resolution moved
  to one up-front pass per candidate and its redaction decision is reused across every artifact.
