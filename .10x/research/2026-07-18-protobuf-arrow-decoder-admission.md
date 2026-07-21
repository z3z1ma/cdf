Status: done
Created: 2026-07-18
Updated: 2026-07-18

# Protobuf-to-Arrow decoder admission

## Question

Which native Rust dependency and ownership boundary can implement P3 B10's descriptor-bound, length-delimited Protobuf codec without weakening unknown-field, oneof, memory, dependency-tuple, or performance requirements?

## Sources and methods

- Inspected the published `ptars-core 0.0.21` source and dependency manifest after downloading the exact crate with `cargo info ptars-core@0.0.21`.
- Inspected the published `prost-reflect 0.16.5` source and dependency manifest after downloading the exact crate with `cargo info prost-reflect@0.16.5`.
- Traced CDF's existing `PreContractResidualCandidate` and residual-verdict execution path in `cdf-kernel` and `cdf-engine`.
- Compared each candidate against `.10x/specs/native-format-codec-runtime.md`, `.10x/specs/native-enterprise-format-catalog.md`, and `.10x/decisions/native-enterprise-format-catalog-v1.md`.
- No throughput conclusion was inferred from implementation shape. B10 still requires measured same-host native-reference and memory evidence before product admission.

## Findings

### `ptars-core 0.0.21` is not a semantic authority CDF can admit

`ptars-core` provides an attractive direct Protobuf-wire-to-Arrow path, but its decoder advances over unknown field numbers through `skip_field(...)` without returning their exact bytes or provenance. Its direct builders also do not clear sibling oneof fields when a later member of the same oneof is decoded. A legal wire payload containing multiple oneof members can therefore materialize more than one member instead of applying Protobuf's last-member-wins rule.

Both behaviors conflict directly with B10: unknowns would disappear before the compiled residual program could decide capture/quarantine, and oneof semantics would differ from a conforming Protobuf implementation. The crate also pins Arrow 59 while CDF's active D-28 tuple is Arrow 58.3, so direct use would add an Arrow-major bridge in a format hot path.

The version-independent `ptars` facade does not repair those facts: it routes binary-array decoding through the same direct decoder and would add an Arrow C Data bridge while retaining the semantic defects.

### `prost-reflect 0.16.5` is suitable for descriptor authority and conformance

`prost-reflect` validates a `FileDescriptorSet`, resolves a fully qualified message descriptor, enforces oneof exclusivity by clearing sibling fields, and retains unknown fields. `UnknownField` exposes its field number, wire type, encoded length, and exact re-encoding. It has no Arrow dependency and therefore does not disturb the active Arrow tuple.

Its `DynamicMessage` representation is not admitted as the production hot decoder. Dynamic maps, vectors, strings, and nested messages allocate inside the dependency after CDF hands it a byte slice. Reserving an estimate before decode would not make the memory ledger authoritative over those allocations, and decoding a second time merely to build Arrow would add avoidable work. `DynamicMessage` remains useful as the independent semantic oracle in format-specific conformance tests.

### CDF already owns the correct unknown-field policy boundary

The format codec can attach exact unknown wire occurrences to a `BatchHeader` as `PreContractResidualCandidate`s. The engine's compiled validation program then makes the total verdict: capture into the governed residual column under evolve policy, quarantine under freeze/strict policy, or reject when required. Protobuf-specific code does not need a competing drift policy or a physical `_unknown` payload column.

## Conclusion

B10 will use one production path:

1. `prost-reflect 0.16.5` validates and resolves the explicit descriptor set and supplies descriptor metadata.
2. A CDF-owned bounded length-delimited wire decoder maps fields directly into Arrow builders under the shared memory ledger.
3. Exact unknown wire occurrences, including field number and wire type provenance, enter the existing pre-contract residual path.
4. `prost-reflect::DynamicMessage` is test/reference authority only, never a production fallback.

`ptars` and `ptars-core` are rejected. No Arrow-major bridge, dynamic-message production decoder, or competing unknown-field policy is retained.

## Limits

- This investigation establishes semantic and architectural fitness, not performance. B10 remains open until the direct decoder meets its same-host reference ratio and memory profile.
- The exact Arrow representation for every well-known type and nested/map shape remains governed by B10's implementation evidence and format goldens.
- The accepted dependency versions still require the repository dependency loop (`cargo deny`, `cargo audit`, and `cargo vet`) before closure.
