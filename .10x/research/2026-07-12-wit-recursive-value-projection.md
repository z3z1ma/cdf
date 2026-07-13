Status: done
Created: 2026-07-12
Updated: 2026-07-12

# WIT recursive value projection

## Question

How can the independently versioned `cdf:resource` WIT boundary faithfully carry the current recursive kernel values `ScopeKey::Composite(Vec<ScopeKey>)` and `SourcePosition::Composite(BTreeMap<String, SourcePosition>)`, given that WIT value definitions must be acyclic? Which projection best preserves value semantics, deterministic replay/hashing, cross-language SDK usability, bounded validation, and the project's incremental-memory posture?

This is shaping research. It recommends a direction and asks for ratification; it does not establish boundary semantics, limits, error behavior, or a WIT contract.

## Sources and methods

Inspected on 2026-07-12:

- `VISION.md` D-26 and sections 8, 9.5, 12.5, 13.3-13.4, 20, and 22: WIT is independently versioned; resource interchange remains descriptor plus Arrow batches; state positions are typed and versioned; scopes preserve checkpoint/concurrency meaning; replay artifacts are deterministic.
- `.10x/specs/resource-authoring-planning-batches.md`: Tier 3 exports `describe`, `negotiate`, and async `open(partition) -> stream<u8>`; its control model must remain faithful to the resource vocabulary.
- `.10x/specs/foreign-stream-interop.md`: foreign boundaries are incremental and bounded; WIT must eventually prove typed state/control, cancellation, and arbitrary-chunk IPC behavior.
- `.10x/tickets/2026-07-08-wasm-wit-interface-foundation.md`, including both adversarial reviews and cleanup evidence: direct recursive WIT was rejected, and no partial implementation remains.
- `crates/cdf-kernel/src/scope.rs`: `Composite.parts` is an ordered `Vec<ScopeKey>` and can nest without an application-level depth bound.
- `crates/cdf-kernel/src/position.rs`: `CompositePosition.positions` is a `BTreeMap<String, SourcePosition>`, carries its own `u16` version, and can nest without an application-level depth bound.
- `crates/cdf-state-sqlite/src/support.rs`: current ledger persistence uses Serde JSON behind a checkpoint `state_version`; boundary ABI bytes are not current checkpoint identity bytes.
- `crates/cdf-package/src/json.rs` and `VISION.md` section 12.5: CDF has explicit canonical JSON machinery for package artifacts, but it is not currently a general WIT canonical-ABI serialization contract.
- Official WebAssembly Component Model WIT specification: <https://github.com/WebAssembly/component-model/blob/main/design/mvp/WIT.md>. WIT/component type definitions are ordered and acyclic; a parser topologically sorts definitions and rejects cycles. WIT `map<K,V>` is semantically a list of pairs with last-duplicate-key-wins behavior, while resources are indirect handles for entities that cannot or should not be copied by value.
- Official Component Model Canonical ABI explainer: <https://github.com/WebAssembly/component-model/blob/main/design/mvp/CanonicalABI.md>. Canonical lowering/lifting is an ABI operation, not a durable, portable byte-serialization format that CDF can hash as an artifact contract.
- Official Component Model concurrency explainer: <https://github.com/WebAssembly/component-model/blob/main/design/mvp/Concurrency.md>. Streams and futures use handle-table entries and copy values through caller-provided component memory buffers; a WIT stream is useful for incremental payloads but does not make ordinary control records zero-copy.
- Bytecode Alliance WIT reference: <https://component-model.bytecodealliance.org/design/wit.html>. Generated bindings are intended to map records, variants, lists, maps, and resources into language-native shapes; resources carry ownership/lifetime behavior rather than ordinary value behavior.

No local checkout of the official Component Model prose or unpacked `wit-parser` source was available. The official upstream sources above were therefore read from their primary repositories/documentation. No build, parser run, prototype, benchmark, or implementation mutation was performed.

## Findings

### Required semantic invariants

A faithful projection has to preserve more than reachability:

- `ScopeKey::Composite.parts` order is observable and must not be sorted or treated as a set.
- `CompositePosition.positions` has unique string keys and deterministic ascending key iteration from `BTreeMap`; WIT `map`'s last-duplicate-wins semantics alone are weaker and would admit values the kernel type cannot express.
- Both Rust types are trees by ownership. A generic index graph can additionally express cycles, shared subtrees, multiple roots, and unreachable nodes; those forms must be rejected or the boundary changes the model.
- All leaf variants and their exact integer/string/byte fields must round-trip. In particular, `u64`, `s64`, opaque bytes, per-position `version`, optional strings, and composite nesting cannot be coerced through a language's generic JSON-number/object model.
- The canonical ABI's lowered memory layout is not a stable persistence or hashing format. Replay/equality must be defined over the decoded CDF value (or a separately ratified canonical codec), never over incidental guest memory, allocation order, or runtime handle numbers.
- Any finite boundary limit rejects some values accepted by the currently unbounded Rust constructors. A depth/node/byte cap is operationally necessary for an untrusted guest but is a semantic restriction requiring explicit ratification and a typed failure.

### Option comparison

| Projection | Existing semantics | Determinism and replay | SDK ergonomics | Copy/streaming | Validation and limits |
|---|---|---|---|---|---|
| Canonical rooted arena/index tree | Lossless for every admitted finite value if the arena is constrained to exactly one tree; preserves scope child order and position map keys/order | Can have exactly one representation if root/index/order rules are normative. Decode to kernel values before persistence/hash; do not hash canonical-ABI bytes | Generated records/variants/lists remain typed in every SDK, though SDK adapters must hide indices behind ordinary recursive language types | One bounded control value is copied/lifted. It is not zero-copy or streaming, but avoids one call per node and control values should be capped | Strongest: iterative validation can reject out-of-range/backward refs, cycles, aliases, unreachable nodes, duplicate/unsorted keys, excess depth/nodes/bytes, and invalid leaves before kernel use |
| Opaque canonical bytes | Potentially lossless only after selecting and versioning an exact codec. Reusing current Serde JSON shape is tempting but would turn an internal representation into the foreign contract | Excellent only if one canonical encoder, numeric rules, Unicode rules, versioning, and hash domain are specified. Otherwise semantically equal values can have different bytes | Weak: WIT exposes `list<u8>`; each SDK needs a CDF codec and loses generated field/variant help. Generic JSON APIs risk 64-bit integer loss | Still copies the whole byte list in an ordinary call. A separate `stream<u8>` codec would add framing/cancellation/error semantics and change control signatures | Byte cap is easy, semantic validation is delayed until decode, and decompression/expansion risks arise if a compressed codec is ever allowed |
| WIT resource handles | Can represent recursive traversal, but changes an immutable transferable value into host/guest-owned identity with drop, borrow, lifetime, and possible mutation/TOCTOU semantics | Handle numbers and call schedules are not value identity. Deterministic replay requires a separately specified snapshot traversal and mutation exclusion | Idiomatic as objects in some languages, but materially heavier than enum/record values; ownership mistakes become user-visible | Avoids transferring the whole graph at once and can page children, but every field/child requires host calls and returned strings/lists still cross memory | Requires quotas for live handles, calls, traversal, depth, bytes, and lifetime cleanup; cancellation and drop become correctness surfaces |
| Split recursive host calls / builder-visitor protocol | Can reconstruct the tree, but replaces one value with a stateful multi-call transaction and introduces partial-construction states | Determinism depends on a normative call/order protocol plus atomic finalize; retry/cancellation can otherwise leave ambiguous partial state | Poorest: SDKs need protocol state machines and language-specific builders/visitors rather than generated values | Can stream nodes and bound peak memory, but pays a cross-boundary call per node/chunk and complicates backpressure | Must govern session IDs, ordering, duplicate writes, rollback, timeout, cancellation, quotas, and final validation; largest error surface |

### Arena shape that merits ratification

The smallest typed candidate is two ordinary WIT arena values, not a WIT `resource`:

- `scope-tree { root: u32, nodes: list<scope-node> }`, where non-composite cases carry the current leaf fields and `composite` carries `list<u32>` child indices.
- `source-position-tree { root: u32, nodes: list<source-position-node> }`, where non-composite cases carry current leaf records and `composite` carries `{ version: u16, entries: list<position-entry> }`; each entry is `{ key: string, value: u32 }`.

For a unique encoding, the candidate should require root index zero, depth-first preorder nodes, child references to later indices, every non-root node referenced exactly once, no unreachable nodes, scope children emitted in existing vector order, and composite-position entries emitted in strictly ascending unique UTF-8 key order. The host should validate iteratively and construct the recursive kernel value only after the complete arena passes. An outbound host encoder should produce only this normal form; an inbound noncanonical form should fail rather than be silently normalized, because acceptance-plus-normalization hides guest bugs and creates multiple wire representations.

This arena is a transport projection, not a new checkpoint model. After decoding, equality, replay, persistence, and any semantic hash should use the same kernel/state-version authority as native resources. If the WIT projection itself later needs a signed or content-addressed byte identity, that requires a separate canonical serialization decision; the canonical ABI is not that serialization.

### Performance boundary

None of the four choices proves zero-copy. Typed arenas and opaque bytes both cross as bounded lists in an ordinary control call. Resource handles and split calls can reduce peak transfer size but trade it for many calls and lifecycle/state-machine complexity; strings and child lists still transfer. The existing `open -> stream<u8>` is the correct streaming surface for bulk Arrow IPC. Recursive scope/position values are bounded control data unless future evidence shows otherwise, so optimizing them into a resource/session protocol now would spend complexity without a named throughput requirement.

An arena can be validated and decoded iteratively with O(nodes + aggregate payload bytes) time. Peak memory includes the canonical-ABI-lifted arena plus the decoded kernel tree, so the host must charge both against the control-memory budget. A future implementation should measure generated bindings and copies, but no performance claim is supported by this research.

## Conclusions

The typed canonical arena/index tree is the smallest complete candidate. It preserves all admitted kernel value distinctions, keeps generated SDKs typed, makes malicious graph forms explicitly rejectable, needs only one call, and does not introduce ownership or transactional semantics. Opaque bytes are a defensible fallback only if preserving an already-governed byte codec outranks SDK usability; no such general scope/position codec is currently authoritative. Resource handles and split calls solve a scale/lifetime problem these control values have not demonstrated and materially change semantics.

This recommendation does not unblock implementation until the following values are confirmed or corrected and then captured in a governing spec/decision. Exact limits and error mapping remain unratified.

## Confirm or correct

1. Should the WIT boundary represent each recursive scope or source position as one typed canonical rooted arena/index tree, rather than opaque bytes, resource handles, or a multi-call builder/visitor? **Decision unlocked: foreign-boundary value model.** I recommend the typed arena because it is lossless for admitted values, SDK-visible, and introduces no lifetime/session semantics. Confirm or correct before implementation.
2. Should canonical input require root `0`, depth-first preorder, forward-only child indices, exactly one parent per non-root node, no unreachable nodes, preserved scope child order, and strictly ascending unique composite-position keys, with persistence/replay/hashing performed only after decoding to the existing kernel value? **Decision unlocked: deterministic validation and identity.** I recommend yes; canonical-ABI memory bytes and handle/index allocation must not become artifact identity. Confirm or correct before implementation.
3. What exact boundary limits and failure category should govern these untrusted control values? **Decision unlocked: resource-exhaustion behavior and the admitted semantic subset.** I recommend a starting cap of depth 64, 65,536 nodes, and 16 MiB aggregate variable payload per value, checked before recursive allocation with no truncation and a typed non-retryable resource-limit error; confirm these values/error semantics or provide replacements before implementation.

## Limits

- No generated Rust, Python, JavaScript, or other bindings were inspected, so SDK ergonomics are reasoned from WIT's official type mappings rather than observed code generation.
- No prototype measured canonical-ABI copies, allocation peaks, call overhead, or parser/toolchain support.
- The current kernel types have no explicit construction caps; the proposed numbers are recommendations, not facts or ratified behavior.
- This research addresses only recursive scope/position projection. Capability discovery, concrete host HTTP/secrets/log calls, and `describe`/`negotiate` error surfaces remain separate blockers already named by the owning ticket.
- Stream chunk grouping is not part of this control-value choice. Existing authority promises a logical sequential Arrow IPC stream, not canonical-ABI transfer grouping.
