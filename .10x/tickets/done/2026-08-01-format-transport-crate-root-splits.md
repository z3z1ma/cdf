Status: done
Created: 2026-08-01
Updated: 2026-08-01
Parent: .10x/tickets/done/2026-08-01-rust-crate-architecture-cleanup.md

# Split monolithic format and HTTP transport roots

## Scope

Make the roots of `cdf-format-json`, `cdf-format-protobuf`, `cdf-format-avro`, and `cdf-transport-http` thin facades. Extract focused driver/configuration, discovery/framing, decode-session, validation, byte-source, response-body, and error-classification modules according to each crate's existing responsibilities.

## Non-goals

- No format detection, parsing, schema inference, malformed-input isolation, memory bound, HTTP policy, timeout, retry, or performance-default change.
- No shared universal codec abstraction.
- No public API change.

## Acceptance Criteria

- Each scoped `lib.rs` is a thin documented facade with explicit exports.
- Production module names communicate one coherent responsibility and imports are explicit and acyclic.
- Existing format fixtures, streaming boundaries, malformed-input tests, HTTP transport/range tests, and strict lint pass.
- No test/golden assertion is weakened and no default changes.

## References

- `.10x/knowledge/rust-crate-organization.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
- `.10x/specs/native-format-codec-runtime.md`

## Assumptions

- Record-backed: the scoped roots contain between 1,455 and 2,118 production lines and multiple named responsibilities.
- User-ratified: this child is an organization-only split.

## Journal

- 2026-08-01: Ticket opened from the crate-root layout audit.
- 2026-08-01: Execution began. Read the ticket and all governing organization, extension-boundary, and native-format-runtime records completely. The split will preserve each format-specific authority inside its adapter and keep HTTP policy inside the transport crate; no shared codec layer will be introduced.
- 2026-08-01: `cdf-transport-http` now has a 14-line facade over focused `policy`, `request`, `errors`, `response_body`, `byte_source`, and `provider` modules; its formerly inline white-box tests moved intact to `tests.rs`. `cargo fmt -p cdf-transport-http` and `cargo check -p cdf-transport-http --all-targets --locked` passed.
- 2026-08-01: `cdf-format-avro` now has a 14-line facade over `options`, `driver`, `decode`, `byte_source`, `planning`, `validation`, and `errors`; existing tests remain in their dedicated module with explicit imports. `cargo fmt -p cdf-format-avro` and `cargo check -p cdf-format-avro --all-targets --locked` passed.
- 2026-08-01: `cdf-format-protobuf` now has a 15-line facade while preserving its existing `schema` and `wire` modules and adding focused `options`, `driver`, `framing`, `decode`, and `materialize` modules. `cargo fmt -p cdf-format-protobuf` and `cargo check -p cdf-format-protobuf --all-targets --locked` passed.
- 2026-08-01: `cdf-format-json` now has a 15-line facade over `options`, `discovery`, `driver`, `framing`, `decode`, `raw`, and `selection`; its formerly inline tests moved intact to `tests.rs`. `cargo fmt -p cdf-format-json` and `cargo check -p cdf-format-json --all-targets --locked` passed.
- 2026-08-01: `graphify query` could not be used because the `graphify` executable is absent from this environment. Source, active records, and import chains were inspected directly instead.
- 2026-08-01: The combined scoped library-test run passed all executable Avro tests (15/15), JSON tests (17/17, with the existing three release-performance tests ignored), and Protobuf tests (13/13, with the existing release-performance test ignored). HTTP passed all seven tests that do not bind local sockets; its other nine tests failed before exercising transport behavior because this sandbox denies local-listener creation with `Os { code: 1, kind: PermissionDenied }`. An escalated rerun was requested but waited for approval without producing output and was interrupted; this is an evidence-environment limit, not an observed HTTP behavior regression.
- 2026-08-01: Strict scoped Clippy passed for all targets in all four crates with `-D warnings`. Scoped `cargo fmt --check`, tracked and untracked whitespace checks, public-item parity inspection, and moved-test function-name parity checks passed. Existing Avro and Protobuf test-file diffs contain import changes only.
- 2026-08-01: `graphify update .` was attempted after the source changes and could not run because the `graphify` executable is absent. No graph output was mutated.
- 2026-08-01: Parent integration reran `cargo test -p cdf-transport-http --lib --locked` outside sandbox restrictions. All 16 tests passed, including the nine loopback-dependent tests that previously stopped at listener creation.

## Blockers

None.

## Evidence

- Thin facades: `wc -l` reports 15 lines for `cdf-format-json/src/lib.rs`, 14 for `cdf-format-protobuf`, 14 for `cdf-format-avro`, and 13 for `cdf-transport-http`. Each facade contains only module declarations, explicit public re-exports, and its test-module declaration.
- Public API parity: a baseline/current public-item scan found the same externally public types, constructors, function, and public selection fields: JSON's two drivers plus `BoundedJsonSelection`/`select_bounded_json_records`; Protobuf's driver; Avro's two drivers; and HTTP's provider.
- Compile boundary: `CARGO_BUILD_JOBS=12 cargo check -p <crate> --all-targets --locked` passed independently for each of the four crates after its split. This proves the new internal module graph resolves for library and test targets; it does not by itself prove runtime behavior.
- Format behavior: `CARGO_BUILD_JOBS=12 cargo test -p cdf-format-json -p cdf-format-protobuf -p cdf-format-avro -p cdf-transport-http --lib --locked -j 12` ran the format suites successfully: Avro 15 passed; JSON 17 passed and 3 pre-existing release-performance tests were ignored; Protobuf 13 passed and 1 pre-existing release-performance test was ignored. This covers the existing fixtures, discovery/framing boundaries, malformed-input isolation, projection/schema authority, residual evidence, and memory-bound assertions encoded by those tests. The command's overall exit was nonzero only because nine HTTP loopback tests could not bind listeners in the sandbox.
- HTTP behavior within the sandbox: the same run passed 7 HTTP tests, including bounded response bodies, transport-frame slicing under one lease, sequential fallback selection, and retry taxonomy. Nine loopback-server tests stopped at listener creation with OS permission denied. Their assertions were not reached, so this run neither supports nor challenges those HTTP behaviors.
- HTTP behavior outside the sandbox: `cargo test -p cdf-transport-http --lib --locked` completed successfully after loopback access was authorized: 16 passed, 0 failed, 0 ignored. This executes the previously blocked generation-preconditioned streaming/ranges, cancellation, idle deadlines, control-request bodies, range coalescing, header-only metadata, and signed-URL redaction assertions.
- Test preservation: JSON and HTTP moved-test function-name lists match their original inline modules exactly; both retain the same test attribute counts/names after extraction. Avro and Protobuf kept their existing dedicated test modules, and their diffs alter imports only. No assertion, ignored marker, default, or fixture changed.
- Strict lint and formatting: `CARGO_BUILD_JOBS=12 cargo clippy -p cdf-format-json -p cdf-format-protobuf -p cdf-format-avro -p cdf-transport-http --all-targets --locked -j 12 -- -D warnings` passed. Scoped `cargo fmt --check` and whitespace checks for both tracked and new files passed.
- Tooling limit: both required graph operations (`graphify query` before inspection and `graphify update .` after edits) failed with `command not found`; direct source/record/import inspection was used, and no graph refresh is claimed.

## Review

### Findings

- **Significant — the required HTTP behavior gate is not yet supported.** Nine of the sixteen HTTP tests did not reach their assertions: `crates/cdf-transport-http/src/tests.rs:90`, `:186`, `:251`, `:321`, `:391`, `:433`, `:479`, `:555`, and `:596` each bind a loopback listener before exercising the behavior under test. The recorded `PermissionDenied` environment limit is consistent with that source structure, but it leaves generation-preconditioned streaming/ranges, cancellation, idle deadlines, control-request bodies, range coalescing, header-only metadata reads, and signed-URL redaction unexecuted. Consequently the acceptance criterion requiring HTTP transport/range tests to pass is not closed by the seven socket-free tests or by all-target compilation. This is an evidence blocker rather than an observed implementation defect; closure requires the unchanged nine tests to pass in an environment that permits loopback listeners.

### Verdict

**Concerns.** Source review found no public API, parsing/framing/discovery, malformed-input isolation, memory-bound/default, HTTP policy, retry taxonomy, or range-policy change in the split. The four facades enumerate the same public root items and fields as `HEAD`; production top-level item-name multisets and all codec/HTTP limit constants are unchanged. The explicit module edges are acyclic. JSON and HTTP test names, attributes, ignored markers, and assertion-site counts match their former inline modules; Avro and Protobuf test diffs change imports only, and their attributes/assertions are unchanged. The ticket remains active solely because the nine behavior-bearing HTTP tests lack runtime evidence.

### Residual Risk

- Until the loopback suite runs, this review cannot certify the runtime behavior of the nine HTTP paths listed above; source/test preservation substantially narrows the risk but does not replace execution of their assertions.
- Public API parity was checked from the before/current public definitions and explicit facade exports, not with a downstream semver consumer corpus. No mismatch was found.
- `graphify` was unavailable, so the source-derived acyclic module graph was not cross-checked against refreshed graph artifacts.

## Closure Re-review — 2026-08-01

### Findings

- **None.** Parent integration's authorized `cargo test -p cdf-transport-http --lib --locked` result passed all 16 library tests with 0 failures and 0 ignored, including the nine loopback-dependent tests named in the prior finding. The current crate root still declares its extracted `tests` module under `#[cfg(test)]`, and that module contains the same 16 test-function names as the original inline suite, so the command's scope covers the preserved behavior gate rather than a reduced substitute.

### Verdict

**Pass.** The sole prior concern was missing runtime evidence for the nine loopback-dependent HTTP paths. Their unchanged assertions have now executed successfully in an environment that permits listener creation, closing the HTTP transport/range acceptance criterion. No source defect or additional closure blocker was found; status remains active for orchestrator closure.

### Residual Risk

- Public API parity remains supported by before/current definition and explicit-export inspection rather than a downstream semver consumer corpus; no mismatch was found.
- This organization-only ticket proves preservation against the existing test and lint surfaces. It does not claim behavioral coverage beyond those surfaces.

## Retrospective

- What worked: splitting one crate at a time and compiling every all-target boundary localized move-only import/visibility fallout. Keeping format-specific parsing and memory authority in each adapter avoided introducing the prohibited shared codec abstraction.
- What surprised: moving an inline test module can hide trait/type dependencies previously supplied by `use super::*`; replacing those glob imports with explicit module imports made the real white-box seams visible. A line-range move also initially omitted `BoundedJsonSelection`'s derive attribute; the all-target check caught the resulting public trait regression immediately.
- Dead end/limit: the first escalated HTTP test rerun spent time waiting for loopback permission and was interrupted without output. Future runs in this managed sandbox should either have loopback approval ready before starting or record the listener-bind limit immediately and leave the independent reviewer/integration environment to supply that evidence.
- Durable outcome: roots now advertise only crate topology and public authority, while discovery, framing, decoding, byte access, validation, policy, and error classification have named homes. The compiler and strict lint enforce an explicit acyclic import graph, and tests no longer depend on broad root-level private imports.
