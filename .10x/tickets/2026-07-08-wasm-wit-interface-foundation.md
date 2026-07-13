Status: blocked
Created: 2026-07-08
Updated: 2026-07-12
Parent: .10x/tickets/2026-07-05-wasm-components-registry-signing.md
Depends-On: .10x/specs/resource-authoring-planning-batches.md, .10x/specs/conformance-governance-roadmap.md

# Implement WASM WIT interface foundation

## Scope

Add the first reviewable Tier-3 WASM component interface artifact under `crates/cdf-wasm/**`.

Owns:

- The CDF resource WIT package/world file(s).
- Minimal crate documentation and tests that make the WIT file discoverable from `cdf-wasm`.
- Any small validation script or test fixture needed to prove the WIT file contains the ratified package, imports, exports, and async stream shape.

## References

- `.10x/research/2026-07-12-wit-recursive-value-projection.md` evaluates lossless WIT projections for recursive scope/source-position values and records the remaining confirm-or-correct decisions; it is research, not semantic authority.

## Acceptance criteria

- The WIT package is named `cdf:resource@0.1.0`.
- The exported world is named `resource`.
- The world imports host-mediated `cdf:host/http`, `cdf:host/secrets`, and `cdf:host/log` interfaces.
- The world exports `describe`, `negotiate`, and `open`.
- `open` is declared as an async function taking a partition value and returning `stream<u8>` of Arrow IPC bytes.
- The WIT artifact models enough descriptor, scan request, scan plan, partition, and error/result structure that the interface is reviewable and not a placeholder.
- The implementation does not add a Wasmtime host, sandbox execution, registry admission, signing, component SDK, or conformance-on-component runtime.

## Evidence expectations

Run `cargo fmt --all --check`, `cargo test -p cdf-wasm --locked`, `cargo clippy -p cdf-wasm --all-targets --locked -- -D warnings`, source-only Gitleaks over `crates/cdf-wasm` and changed `.10x` records, direct unsafe/FFI scan over `crates/cdf-wasm`, and a WIT syntax check if a local `wasm-tools` or equivalent validator is available without weakening the dependency graph. If no WIT validator is locally available, record that limit and compensate with focused text-shape tests.

## Explicit exclusions

No Wasmtime host, no component execution, no sandbox-denial tests, no host HTTP/secrets/log implementation, no registry admission, no signing implementation, no guest SDK, no runtime integration, no Cargo dependency additions unless strictly needed for local validation.

## Blockers

None. The WIT shape is ratified by `VISION.md` §9.5 / D-26 and `.10x/specs/resource-authoring-planning-batches.md`.

## Progress and notes

- 2026-07-08: Split from `.10x/tickets/2026-07-05-wasm-components-registry-signing.md` because the full WASM/registry/signing ticket is too broad to execute as one slice. This child only makes the WIT interface artifact concrete and reviewable.

## Journal

- 2026-07-12: Shaping research in `.10x/research/2026-07-12-wit-recursive-value-projection.md` compared canonical rooted arenas, opaque canonical bytes, resource handles, and split recursive host calls against kernel fidelity, deterministic replay/hashing, SDK ergonomics, copy/streaming behavior, and bounded validation. It recommends—but does not ratify—a typed canonical arena/index tree. The ticket remains blocked pending confirmation of the projection, canonical-form/identity rules, and exact limits/error semantics; capability discovery and host/error surfaces remain separately unresolved.

- 2026-07-12: Execution began after reading this ticket, both referenced active specs, the parent ticket, VISION D-26 and §9.5, the normative resource-model sketches in VISION §8, the current `cdf-kernel` resource vocabulary, and the downstream P3 H4 interface-review ticket. The child is executable and dependency-ready: its direct spec dependencies are active, its parent explicitly split this isolated interface artifact, and H4 depends on this foundation. The existing `cdf-wasm` crate is clean; unrelated dirty DX3, J0, kernel, runtime, destination, package-record, workspace lockfile, and CLI files are outside scope and will not be touched. No local `wasm-tools`, `wasmtime`, or `wit-bindgen` executable is available, so source work will pair the WIT artifact with focused crate-local text-shape tests and will record the external syntax-validation limit. Cargo execution remains reserved to the orchestrator/J0 owner.
- 2026-07-12: Repair execution read the failed adversarial review and rechecked every modeled shape against the active resource/conformance specs, VISION D-26/§§8 and 9.5, and current kernel `resource.rs`, `scope.rs`, `position.rs`, `error.rs`, and `destination.rs`. The review findings were confirmed: reserved identifiers were unescaped, host imports had no resolvable package, and descriptor/request/plan/position/error fields had drifted from current authority.
- 2026-07-12: Replaced the text-shape test with a real `wit_parser::Resolve::push_dir` plus world-selection gate. Existing binaries, workspace dependencies, lockfile packages, Cargo caches, and git caches contained no callable WIT text parser or validator; `wasmparser` only validates Wasm binaries and transitive `wit-bindgen` exposes no repository validator. Selected exact, dev-only `wit-parser = 0.252.0` with only its `std` feature, the smallest published parser version already present in the local crates.io index that supports WASI 0.3 WIT async/stream syntax. Lockfile resolution and supply-chain gates await the orchestrator's Cargo slot.
- 2026-07-12: Added `wit/deps/cdf-host/host.wit` so `cdf:host/http`, `secrets`, and `log` resolve as package interfaces. The dependency deliberately declares only the three governed import names: VISION governs mediation properties but no callable HTTP/secrets/log WIT function signatures, so this ticket does not invent a host API or runtime implementation.
- 2026-07-12: Corrected the package/world and scope identifiers to grammar-valid `%resource`/`%stream` spellings while preserving canonical semantic names. Rebuilt descriptor, capability, request, plan, partition, position, and error value shapes from current kernel authority: discovered/hints/contract schema sources, deduplication, optional contract, inexact/unordered cursor ordering, full non-recursive scopes, request scope, delivery guarantee, estimates, and retry timing are present; the unsupported mandatory error code and checkpoint-view request were removed.
- 2026-07-12: VISION D-26, §9.5, and the active resource spec establish a sequential `stream<u8>` of Arrow IPC streaming-format bytes, so the `open` documentation now carries that contract. They do not establish guest write grouping, canonical-ABI transfer chunk boundaries, host read chunk boundaries, or an item/batch correspondence. No such semantics were invented.
- 2026-07-12: Exact kernel parity is blocked for two recursive variants. `ScopeKey::Composite(Vec<ScopeKey>)` and `SourcePosition::Composite(BTreeMap<String, SourcePosition>)` are recursive, while the current WIT specification requires an acyclic type graph. The initial attempted recursive WIT records were removed rather than replaced with unratified opaque bytes, flattening, or a fixed depth. A governing foreign-boundary projection decision/spec is required before this independently versioned artifact can represent these variants.
- 2026-07-12: Cleanup followed the fresh blocker re-review's option (a): restored `crates/cdf-wasm/Cargo.toml` and `crates/cdf-wasm/src/lib.rs` to `HEAD` and removed the two untracked WIT files. No implementation or dependency diff from this ticket remains. The shared `Cargo.lock` was not touched; its current modification belongs to J0.

## Blockers

Semantic blocker: current kernel composite scope and composite source-position values are recursive, but WIT value types cannot be recursive. No active record defines an allowed foreign-boundary projection (flattened paths, opaque canonical encoding, resource handle, or bounded depth), and each option changes serialized semantics. Before a future implementation ticket opens, shaping must ratify one lossless foreign-boundary representation, including its canonical encoding, validation, limits, errors, and compatibility/migration behavior, and must reconcile capability discovery, host-call signatures, and `describe`/`negotiate` error surfaces. The active records define a logical sequential Arrow IPC byte stream but not transfer chunk grouping; chunk grouping requires ratification only if the published WIT is intended to promise more than that existing logical stream contract. No partial implementation or dependency change remains from this blocked ticket.

## Evidence

The pre-review evidence below records the failed first implementation and is superseded by the repair journal and repair evidence that follow the Review section.

- Package/world identity and host imports: `crates/cdf-wasm/wit/resource.wit` declares `cdf:resource@0.1.0`, world `resource`, and the exact three `cdf:host/http`, `cdf:host/secrets`, and `cdf:host/log` imports. A source-only exact-shape loop found every required declaration; an `awk` brace-balance check passed. This proves the reviewed text shape, not full WIT name resolution.
- Export contract: the same source-only check found `describe`, `negotiate`, and `open`; `open` is exactly `async func(part: partition) -> stream<u8>`. The crate-local tests independently compact the embedded artifact and assert those exports and imports.
- Reviewable semantic types: the WIT defines the descriptor fields mandated by `.10x/specs/resource-authoring-planning-batches.md`, resource capabilities, a scan request with projection/filter/limit/order/checkpoint inputs, a scan plan with predicate fidelity/partitions/order/estimates, typed partition checkpoint positions, and the VISION error taxonomy. `crates/cdf-wasm/src/lib.rs` embeds the artifact with `include_str!` and asserts the governed fields remain present.
- Scope/exclusions: `git diff --quiet HEAD -- Cargo.toml crates/cdf-wasm/Cargo.toml` passed. No dependency or manifest edit was made. `Cargo.lock` was already dirty at executor start under unrelated J0/package work and remained outside scope. Scoped status contains only this ticket, `crates/cdf-wasm/src/lib.rs`, and the new WIT file.
- Source-only quality observations: `rustfmt --edition 2024 --check crates/cdf-wasm/src/lib.rs`, scoped `git diff --check`, a trailing-whitespace scan over all touched files, and a direct unsafe/FFI scan over `crates/cdf-wasm` all passed. Gitleaks 8.21.2 directory/file scans over `crates/cdf-wasm` and this ticket each reported `no leaks found`.
- WIT validation limit: no local `wasm-tools`, `wasmtime`, or `wit-bindgen` executable exists. The current official WebAssembly Component Model WIT grammar was inspected as a source-level cross-check: worlds permit external interface imports, world-local `use` and typedef items, `async func`, and `stream<T>`. This is not a substitute for parser/name-resolution validation. Per the ticket, focused text-shape tests compensate until the orchestrator can run an available validator.
- Cargo gates remain pending by orchestration instruction: `cargo fmt --all --check`, `cargo test -p cdf-wasm --locked`, and `cargo clippy -p cdf-wasm --all-targets --locked -- -D warnings` were not run by this executor. Root/J0 owns Cargo and broad workspace checks.

## Review

Fresh adversarial review (2026-07-12):

### Findings

- **Critical — the WIT text does not conform to the current WIT lexical grammar.** `resource` and `stream` are reserved keywords and must be escaped with `%` when used as identifiers. The artifact uses bare reserved identifiers in the package name (`package cdf:resource@0.1.0`, line 1), world name (`world resource`, line 275), `state-scope` cases (`resource` and `stream`, lines 41 and 45), and `scope-kind` cases (`resource` and `stream`, lines 65 and 69). The current Component Model grammar explicitly says keyword identifiers require the `%` form. The crate tests make this worse by requiring the invalid compact spellings `packagecdf:resource@0.1.0;` and `worldresource{`; they would reject the grammar-correct escaped source while continuing to pass malformed WIT. Source: <https://github.com/WebAssembly/component-model/blob/main/design/mvp/WIT.md#wit-identifiers>.
- **Significant — the package is not name-resolvable as a standalone published WIT artifact.** The world imports `cdf:host/http`, `cdf:host/secrets`, and `cdf:host/log`, but the repository contains no WIT definitions or dependency packages for those interfaces. WIT import paths name interfaces, not opaque package labels; a resolver needs the full imported interface definitions. Consequently, even after fixing reserved identifiers, a real package resolver cannot validate or generate bindings for this directory. This also leaves the promised host mediation semantics (HTTP limiting/egress/redaction, use-without-reading secrets, logging) entirely outside the interface artifact. Source: <https://component-model.bytecodealliance.org/design/wit.html>.
- **Significant — the independently versioned WIT freezes semantics that conflict with the active kernel vocabulary without a governing reconciliation.** Compared with `cdf-kernel`, `schema-source` omits `discovered` and `contract` and gives `hints` a different mandatory hash/source shape; cursor ordering omits `inexact` and `unordered`; state scopes omit `schema-contract`, `destination-load`, and `composite`; source positions omit `composite`; `scan-request` replaces the required first-scan `scope` with an optional checkpoint, making a scoped initial request inexpressible; `scan-plan` omits `delivery-guarantee`; and `resource-error` omits `retry-after-ms` while adding an unratified mandatory `code`. Descriptor deduplication is also absent. Some differences may be intentional foreign-boundary projections, but no active record ratifies those translations, and artifact version `0.1.0` makes them downstream obligations. The current evidence claim of fidelity to the current kernel vocabulary is therefore false.
- **Significant — `stream<u8>` has the ratified transport shape but the artifact never states its Arrow IPC protocol.** WIT alone says only “a stream of bytes”; it does not establish Arrow IPC stream format, schema/message framing, arbitrary chunk boundaries, or whether an item/chunk corresponds to a batch. The VISION contract specifically says Arrow IPC and downstream H4 requires arbitrary-chunk incremental IPC. Because this WIT is independently versioned and published, that semantic contract must travel with the artifact (for example as normative WIT documentation and parser-tested fixtures); otherwise any byte stream satisfies the type while violating CDF. The direct `async func(part: partition) -> stream<u8>` spelling itself matches D-26 and §9.5 and should not be wrapped in a result merely to address this finding.
- **Significant — text-shape tests are not adequate closure evidence for an interface artifact.** They cannot detect reserved keywords, malformed type syntax, duplicate or misplaced declarations, unknown interfaces, cyclic uses, or ownership/name-resolution errors. They currently prove only substrings and have encoded the lexical failure above. Dependency inspection found no usable WIT text parser or validator in the workspace graph: `wit-bindgen 0.57.1` is present only through target-specific `wasip2` support and exposes no validator binary/parser dependency here; no `wit-parser`, `wit-component`, `wasm-tools`, or Wasmtime package is locked; cached `wasmparser` sources parse WebAssembly binaries, not WIT text. A real, pinned WIT 0.3-capable parser/validator gate is required before closure. This review does not prescribe a new Cargo dependency: the smallest governed choice may be a pinned validator tool or a crate-local dev dependency, but existing dependencies do not supply it.

### Negative findings

- No unsafe, FFI, Wasmtime host, registry/signing implementation, guest SDK, runtime integration, or manifest dependency addition appears under `crates/cdf-wasm`.
- The model is value-oriented and introduces no speculative WIT `resource` handles or borrow/own lifecycle. The only async ownership surface is the ratified returned stream; cancellation and mid-stream failure semantics remain a D-26/WASI 0.3 train risk rather than something this artifact implements.
- Descriptor, capabilities, planning, partition, typed-position, and shared error-category structures are substantial enough not to be a placeholder, subject to the semantic conflicts above.

### Verdict

**Fail.** The critical lexical errors mean the WIT is not presently parser-valid, and unresolved host dependencies prevent package-level name-resolution evidence. Semantic drift and the undocumented Arrow IPC contract must also be reconciled before this independently versioned artifact can be treated as the CDF boundary.

### Residual risk

Even after the named findings are repaired and parser validation passes, WASI 0.3 async stream cancellation, mid-stream error propagation, arbitrary-chunk incremental IPC framing, and generated guest/host binding compatibility remain unproved until the downstream H4 and runtime tickets exercise them. This review did not run broad Cargo/build gates and did not repeat the executor's recorded source-only checks.

Fresh blocker re-review (2026-07-12):

### Findings

- **Critical — faithful current-kernel value representation is genuinely blocked by WIT's acyclic type graph.** `ScopeKey::Composite(Vec<ScopeKey>)` and `SourcePosition::Composite(CompositePosition { positions: BTreeMap<String, SourcePosition>, ... })` each contain themselves transitively. The current WIT specification requires Component Model type definitions to be ordered and acyclic; the WIT parser topologically sorts definitions and rejects a cycle. A direct `list<state-scope>` or `map<string, source-position>` therefore cannot represent either kernel variant. Every available escape changes the foreign contract: flattening requires a canonical path/key grammar, bytes require a canonical codec and validation/error rules, a resource handle changes value/ownership/lifetime semantics, and bounded nesting rejects valid kernel values. No inspected active record ratifies one of those projections. The checkpoint spec separately requires typed `Composite` positions, so omission is not faithful parity. This is a real shaping blocker, not a parser-workaround problem.
- **Significant — additional stream chunk semantics remain unratified, but they are not an independent blocker to the ticket's literal `stream<u8>` acceptance criterion.** D-26, VISION section 9.5, and the active resource spec ratify one logical sequential Arrow IPC byte stream. VISION's prose mentions one copy per batch, while the downstream foreign-stream spec requires later validation of arbitrary-chunk incremental IPC, but no active contract defines canonical-ABI transfer chunk boundaries, a chunk-to-record-batch mapping, or whether transport reads preserve guest write grouping. The current WIT documentation correctly claims only a sequential IPC payload and does not invent those semantics. Shaping must ratify them only if this independently versioned artifact is expected to promise more than the existing logical byte-stream contract; the ticket's blocker text should not imply that unspecified chunk grouping alone prevents its present acceptance criteria.
- **Significant — the remaining partial WIT is not a coherent reduced foundation.** It defines `resource-capabilities` but exposes no operation or descriptor field through which a host can obtain it, despite the active resource contract requiring pushdown-capable resources to expose capability claims. It imports three empty host interfaces, which makes names resolvable but gives a guest no HTTP, secret-use, or logging operation and therefore cannot realize the governed host-mediated behavior. Its `describe` result wrapper also differs from VISION section 9.5's direct descriptor return without an active foreign-boundary error-surface reconciliation. These are semantic surfaces that cannot be completed by parser repair alone.
- **Significant — the crate-facing embedding is incomplete and currently disrupts locked workspace validation.** `RESOURCE_WORLD_WIT` embeds only `resource.wit`, not its required `deps/cdf-host/host.wit`, so the advertised constant is not a self-contained resolvable package even though the filesystem test resolves the directory. The exact dev-only `wit-parser` gate is a reasonable candidate after semantics are ratified, but it has no independently useful behavior while the artifact is blocked. It is absent from the current shared `Cargo.lock`; the partial manifest edit therefore makes unrelated `--locked` Cargo tests fail immediately and would require lockfile and supply-chain work that should not be retained for a rejected interface draft.

### Negative findings

- The repaired non-recursive fields closely track the inspected kernel descriptor/request/plan/error vocabulary, and the parser-based directory-resolution test is materially stronger than the superseded substring assertions. These are useful drafting lessons, not an independently shippable subset of an artifact whose versioned value model and host/error surfaces remain unresolved.
- No build, test, formatter, dependency resolution, or implementation mutation was performed by this review. Conclusions come from the active records, kernel definitions, current diff, Cargo index metadata, and the current WebAssembly Component Model WIT specification.

### Verdict

**Blocked; recommend option (a): revert every partial implementation/dependency file under this ticket and preserve only durable blocker/review records.** Specifically, restore `crates/cdf-wasm/Cargo.toml` and `crates/cdf-wasm/src/lib.rs`, and remove the untracked `crates/cdf-wasm/wit/resource.wit` and `crates/cdf-wasm/wit/deps/cdf-host/host.wit`. Do not alter the shared `Cargo.lock` for this ticket. There is no coherent independently useful subset: the WIT is the independently versioned contract and cannot faithfully model required values; the Rust constant/test and parser dependency exist only to publish/validate that blocked contract; and the empty host package resolves names without supplying governed capability behavior. Retain the projection alternatives, chunk-semantics limit, parser lesson, and this review in durable records so a later ratified ticket can reconstruct the work without inheriting a misleading partial API or dependency obligation.

### Residual risk and next action

Shaping should ratify a single lossless foreign-boundary representation for recursive scope and position values, including canonical encoding, validation, limits, errors, and compatibility/migration behavior. It should also reconcile capability discovery, host-call signatures, and `describe`/`negotiate` error surfaces. Chunk grouping needs a decision only if the published WIT is intended to promise grouping beyond a logical sequential Arrow IPC byte stream. After those contracts exist, reopen a fresh executable ticket and reconsider a pinned parser dependency with lockfile and supply-chain evidence.

## Repair evidence

- Cleanup inspection found no diff under `crates/cdf-wasm/Cargo.toml`, `crates/cdf-wasm/src/lib.rs`, or `crates/cdf-wasm/wit/**` after restoring the tracked files to `HEAD` and removing the untracked WIT files. `Cargo.lock` was not modified by cleanup; its remaining diff adds only J0's `cdf-kernel` `arrow-arith 59.1.0` dependency, so the rejected WASM parser dependency no longer creates a locked-manifest mismatch.
- `rustfmt --edition 2024 --check crates/cdf-wasm/src/lib.rs` passed after applying the formatter's one source-only layout correction. `git diff --check -- crates/cdf-wasm .10x/tickets/2026-07-08-wasm-wit-interface-foundation.md` passed.
- Gitleaks 8.21.2 scans over `crates/cdf-wasm` and this ticket each completed with `no leaks found`. A direct source scan found no `unsafe`, FFI declaration/link attribute, or raw pointer under `crates/cdf-wasm`.
- Scoped status contains this ticket, `crates/cdf-wasm/Cargo.toml`, `src/lib.rs`, and the two WIT files. The already-dirty workspace `Cargo.lock` still contains only the unrelated `cdf-kernel` dependency change observed before this repair; no lockfile mutation has been made without the orchestrator's Cargo slot.
- The WIT parser/name-resolution test, lockfile resolution, `cargo test -p cdf-wasm --locked`, Clippy, Cargo audit/deny/vet, and fresh adversarial review are pending. Source inspection cannot substitute for those gates. Ticket closure is independently blocked by the unratified recursive composite projection and chunk-boundary semantics described above.

## Retrospective

- The main risk was freezing opaque or speculative semantics into an independently versioned interface. Reusing the active spec, VISION §8 sketches, and current kernel vocabulary kept the artifact concrete without introducing a second runtime API.
- A first draft wrapped `open` in `result<stream<u8>, resource-error>` and represented checkpoint positions as bytes. Literal acceptance review caught both before verification: `open` now returns `stream<u8>` directly, while invocation errors remain modeled on `describe`/`negotiate`, and control positions are typed rather than opaque.
- No dependency was needed. Embedding the WIT with `include_str!` plus focused text-shape tests is the smallest local discoverability/verification seam while a real WIT validator is unavailable.
- Repair retrospective: substring tests created false confidence because they encoded invalid grammar and could not resolve package imports. A pinned `wit-parser::Resolve` gate is the minimum durable proof for a published WIT package. Cross-model parity also must be checked for representability, not only field names: recursive Rust values cannot be transferred as recursive WIT values, so the foreign projection needs authority before an artifact version freezes it.
