Status: done
Created: 2026-07-11
Updated: 2026-07-18
Parent: .10x/tickets/2026-07-10-p3-ws-b-format-decode-engines.md
Depends-On: .10x/tickets/done/2026-07-11-p0-fx1-native-format-extension-boundary.md, .10x/tickets/done/2026-07-10-p3-ws-l5-preoptimization-baseline.md

# P3 B10: descriptor-bound length-delimited Protobuf codec

## Scope

Add native dynamic Protobuf decode from explicit descriptor set/message, length-delimited framing, Arrow mapping, presence/oneof/enum/map/repeated/well-known handling, and unknown-field residual policy.

## Acceptance criteria

- Unframed or descriptorless streams fail plan; message limits are bounded.
- Field-number/presence/oneof/unknown provenance survives; unknowns never drop silently.
- Schema evolution, malformed varints/messages, random chunks, and jobs remain deterministic.
- Native reference ratio and memory/security evidence are green.

## Evidence expectations

Dependency review, protoc/reference cross-checks, descriptor evolution matrix, malformed/fuzz corpus, unknown-field goldens, and profiles.

## Explicit exclusions

No gRPC transport or ambient schema registry.

## Blockers

None. FX1 and L5 are done.

## References

- `.10x/specs/native-enterprise-format-catalog.md`
- `.10x/specs/native-format-codec-runtime.md`
- `.10x/research/2026-07-18-protobuf-arrow-decoder-admission.md`

## Journal

- 2026-07-18: Revalidated the executable dependencies and active Arrow tuple. Rejected `ptars-core 0.0.21` because its direct path skips unknown wire fields, does not enforce oneof last-wins, and pins Arrow 59. Admitted `prost-reflect 0.16.5` for descriptor authority and conformance only. The production path will be one CDF-owned bounded wire-to-Arrow decoder; exact unknown occurrences enter the existing compiled residual-verdict program. See `.10x/research/2026-07-18-protobuf-arrow-decoder-admission.md`.
- 2026-07-18: Added `cdf-format-protobuf` behind the existing `FormatDriver` boundary and registered it only in the standard product composition point. The driver requires an inline `FileDescriptorSet`, fully qualified message, and `length_delimited` framing; discovery is descriptor-only and reads zero payload bytes. No generic file runtime, compiler, transport, destination, or sibling codec changed.
- 2026-07-18: Implemented direct bounded wire-to-Arrow decode for every scalar width, proto2/proto3 presence/defaults, required fields, last-member-wins oneofs, packed/unpacked repeated fields, canonical last-key-wins maps, enums, nested messages, recursive-message wire preservation, canonical well-known types, and exact unknown wire occurrences. Legal Timestamp/Duration values remain lossless as `(seconds, nanos)` structs because their complete ranges cannot fit Arrow nanoseconds.
- 2026-07-18: Rejected the first hot-path draft after the release-mode reference gate measured only `0.182x` the deliberately favorable `prost-reflect` decode-only oracle. Removed per-field wire scans, repeated descriptor decoding, nested evidence/required passes, per-field heap indexes, conformant-path string allocation, and ordinary scalar occurrence vectors. The final direct Protobuf-to-Arrow median was `4.048333ms` for 4,096 nested rows versus `2.640625ms` for decode-only `prost-reflect`, or `0.652x`; this is stricter than equivalent-work comparison because the reference does not build Arrow or provenance.
- 2026-07-18: Added the product regression `protobuf_descriptor_discovery_run_and_duckdb_commit_share_one_native_driver`; it executes real descriptor discovery, first-use pinning, file decode, package production, provenance, DuckDB commit, and checkpoint publication for two rows.
- 2026-07-18: The dependency loop admitted exact `prost-reflect 0.16.5` plus its exact Prost tuple. `cargo deny check` and `cargo audit` passed (the repository's allowed `paste` maintenance warning remains). Cargo-vet now clears the complete Protobuf tuple and stops only on the independently introduced/unvetted Iceberg git revision.

## Evidence

- Descriptor/framing/bounds: `descriptor_and_framing_are_mandatory_plan_authority`, `malformed_length_and_bounded_message_corpus_fail_closed`, and `forged_well_known_type_layout_is_rejected_at_plan_time` pass. These prove plan-time authority and the exercised size/layout fences; they do not claim gRPC or registry support, which is excluded.
- Semantic/provenance fidelity: `decodes_nested_repeated_map_oneof_wkt_recursive_and_unknown_provenance` and `missing_proto2_required_field_fails_even_before_destination_admission` pass. The golden checks oneof clearing, canonical map ordering/last-key-wins, packed fields, nested and well-known mappings, recursive wire preservation, and six exact nested/top-level unknown candidates.
- Evolution/determinism/malformed input: `schema_evolution_is_hash_stable_and_changes_only_with_descriptor_authority`, `decoder_microbatch_sizes_do_not_change_rows_or_unknown_ordinals`, 1..=17-byte chunk-boundary coverage, the 4,096-case deterministic malformed corpus, group mismatch coverage, and no-partial-window malformed payload test pass.
- Memory/security: the end-to-end physical decode test proves all ledger leases return to zero; descriptor/message/output/depth authorities are bounded and adversarial overlength/malformed/forged-WKT inputs fail closed. There is no `unsafe`, ambient lookup, network access, or dynamic-message production fallback in the codec.
- Performance: `CARGO_BUILD_JOBS=12 cargo test --release -p cdf-format-protobuf tests::direct_arrow_decoder_meets_native_dynamic_reference_floor -- --ignored --exact --nocapture` passed at `0.652x` the same-host decode-only native oracle (`4.048333ms` direct Arrow/provenance, `2.640625ms` oracle). The initial `0.182x` failure was retained in the journal as the rejected baseline.
- Product/quality: 13 ordinary codec tests plus the malformed corpus passed; strict clippy passed for `cdf-format-protobuf` and the reachable `cdf-cli` product graph; the exact CLI run-to-DuckDB regression passed. `cargo check --workspace --all-targets --locked`, `cargo deny check`, and `cargo audit` passed.

## Review

- Findings: No critical or significant finding remains. Fresh-hat review found and corrected four pre-closure defects: quadratic field lookup, silent required-field omission under projection, legal Timestamp/Duration range loss, and nested map/well-known wrong-wire evidence loss. It also replaced the only production `unreachable!` and unchecked size sums with typed errors.
- Verdict: pass.
- Residual risk: Recursive message cycles are preserved losslessly as binary wire values instead of pretending Arrow has a finite recursive type. Malformed framed records currently fail their sequential decode unit rather than publishing record quarantine; that behavior is declared truthfully as `DecodeUnit` isolation and does not lose accepted rows or unknown-field evidence.

## Retrospective

- What worked: the reference gate ran before product admission and turned a plausible direct decoder into a measured optimization loop. A compact indexed wire view plus inline zero/one occurrence representation removed the dominant allocations without importing another parser or leaking format logic into the runtime.
- What surprised: evidence and required-field correctness were individually cheap, but independently reparsing each nested field made their combined cost dominant. Also, Arrow nanosecond timestamps look natural but cannot represent Protobuf's legal Timestamp/Duration ranges.
- Five whys: the first draft was slow because nested values were parsed three times; that happened because Arrow materialization, unknown evidence, and required validation were implemented as separate traversals; they stayed separate because the initial code optimized conceptual clarity before measurement; the reference gate exposed the cost; consolidating inspection and sharing indexed occurrence authority restored the intended direct-path advantage while retaining total verdict evidence.
- Durable lesson: binary codecs should parse each wire envelope once, represent zero/one repeated observations without heap allocation, and compare against a deliberately favorable native oracle before registry admission. No follow-up ticket is required; the shared format boundary already contains the implementation and B13 owns catalog-wide matrix closure.
