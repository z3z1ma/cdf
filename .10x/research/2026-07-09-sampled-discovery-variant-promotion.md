Status: done
Created: 2026-07-09
Updated: 2026-07-09

# Sampled discovery, residual variant capture, and governed promotion

## Question

Can CDF permit an explicit sampled discovery mode, continue processing values that conform to the sampled pinned schema, preserve nonconforming values in `_cdf_variant`, and later promote newly discovered fields by replaying retained evidence into destination columns?

This question was raised while A10 was about to implement exhaustive multi-file binary discovery. It is architectural shaping, not authorization to implement guessed sampling, row-identity, retention, or destination-mutation semantics.

## Sources and methods

Inspected:

- `VISION.md` §§7.5, 8.6, 12.3-12.5, and 13;
- `.10x/specs/types-contracts-normalization.md`;
- `.10x/decisions/contract-live-verdict-execution-semantics.md`;
- `.10x/decisions/multi-file-discovery-aggregation-and-budget.md`;
- `.10x/decisions/backfill-window-planner-command-contract.md`;
- `crates/cdf-engine/src/variant_capture.rs`;
- P1 E4 ticket, evidence, and review for live variant capture;
- current package-retention and cursor-window backfill implementation surfaces.

## Findings

### Sampling is not the violated invariant

The unsafe behavior is not sampling itself. The unsafe behavior is representing a sampled observation as exhaustive, silently switching to sampling when an executor budget is exhausted, or allowing unobserved runtime data to bypass a total contract verdict.

The active multi-file decision already says binary discovery MUST NOT **silently** sample and budget exhaustion MUST NOT automatically change candidate membership. It does not need to be superseded to support a separately selected, evidence-bearing sampled mode. Exhaustive footer/schema-block discovery remains the default for Parquet and Arrow IPC because it is relatively cheap and gives stronger first-pin evidence.

An explicit sampled mode can preserve CDF's invariants if the plan/package records at least:

- `coverage = sampled`, never `exhaustive`;
- total matched, probed, admitted, incompatible, and unprobed counts;
- every matched candidate's bounded identity and participation status;
- the deterministic selection algorithm and its version, parameters, and resolved executor budget;
- the fact that unprobed candidates have no physical-schema hash or discovery-time schema verdict;
- the sampled baseline hash separately from later effective-schema and runtime-observation identities.

Every runtime partition must still reconcile against the pinned baseline/effective schema. Sampling changes plan-time knowledge; it does not weaken runtime validation, package evidence, checkpoint gating, or replay.

### The residual-column idea is already part of the vision

`VISION.md` §7.5 and the active type/contract specification define `_cdf_variant` tagged `json` for unknown or contract-violating substructure and require promotion to typed columns to be a recorded contract-evolution event. P1 E4 implemented the first live slice:

- selected Struct/List/Map source columns are removed from normalized output;
- each row receives one deterministic JSON object keyed by source field in a UTF-8 `_cdf_variant` column;
- the package records capture configuration and zero implicit promotions;
- unsupported Arrow types and non-finite floats fail closed.

The current implementation does **not** yet capture arbitrary extra top-level fields, scalar type mismatches, parse failures, or only the offending path while retaining other conforming values from the same source row. It also does not implement promotion.

The user's proposal generalizes the existing mechanism correctly: accepted fields continue as typed columns; residual nonconforming paths are preserved in `_cdf_variant`; nothing is silently dropped; later discovery may propose promotion into a new pinned schema.

### JSON at the destination is not sufficient exact evidence by itself

A destination-facing string column is broadly portable, but ordinary JSON cannot faithfully represent every Arrow value without an envelope. Decimals, binary values, timestamps/timezones, map keys, NaN/infinity, and large integers need canonical type-aware encoding. The package's Arrow data remains the canonical evidence. `_cdf_variant` may remain a semantic `json` projection if the encoded residual object includes enough type/path information to reconstruct values without guessing, or if an exact typed residual artifact accompanies it.

The smallest candidate contract is one canonical object per row keyed by normalized source path, with each non-JSON-native value represented by a versioned `{type, value}` envelope. Source file/partition, package, schema, and rule provenance should normally remain package/segment metadata rather than be duplicated into every cell.

### Promotion is a compiler and package operation, not an UPDATE shortcut

Rediscovery can produce a proposed snapshot diff and a deterministic extraction/coercion plan from retained residual evidence. That plan must create a new immutable package, validation verdicts, destination receipt, and checkpoint transition. It must never rewrite old packages or silently mutate the pin.

The existing `cdf backfill` contract is cursor-window source re-extraction and currently rejects file resources. Reusing that command name for residual promotion would conflate two different evidence sources and state models. A focused surface such as `cdf schema promote RESOURCE` is the clearer candidate: inspect residual paths, propose/accept typed fields, compile a correction package, and execute through the ordinary gate.

Destination repair is capability-dependent:

- a merge resource can address existing rows by its governed merge key;
- a CDF-loaded append resource could be addressable by a framework-owned immutable row provenance identity, but that identity and its destination persistence are not currently ratified;
- an append-only sink that cannot address prior rows must receive a correction sidecar/versioned rematerialization or a rebuild, not a fictitious UPDATE;
- package GC must not delete the only promotable residual bytes while promotion remains promised; otherwise the CLI must report that only tombstone evidence remains and source re-extraction or rebuild is required.

This destination mutation question is the largest unresolved semantic branch. Append must continue to require no user key, so promotion cannot quietly reintroduce a fake-key requirement.

### Quarantine and residual capture are different verdicts

Field/path residual capture is appropriate when the accepted projection remains meaningful and row identity/provenance survives. Row or file quarantine remains necessary when a required field, cursor, merge key, parser boundary, or whole physical encoding makes partial acceptance unsafe. The validation program must make this a total named verdict rather than treating variant capture as unconditional error suppression.

## Candidate contract awaiting ratification

1. Parquet/Arrow IPC discovery remains exhaustive by default. Sampling is an explicit configured coverage mode, never an automatic response to a budget.
2. A sampled pin is permitted only when the plan and snapshot identify it as sampled and the selected contract has a total runtime route for unseen schema: compatible admission, residual capture, row quarantine, or file quarantine.
3. Runtime evaluates every processed partition regardless of discovery coverage. Conforming fields proceed; nonconforming paths may be captured into `_cdf_variant` without discarding conforming fields from the row when required identity fields remain valid.
4. `_cdf_variant` is a canonical, versioned, type-aware JSON residual object at the portable destination boundary; the package retains exact replayable evidence.
5. Promotion is explicit and plan-first. It produces a schema diff, new pin only on explicit acceptance, a correction/rebuild plan selected from destination capabilities, a new package and receipt, and a gated checkpoint transition.
6. Keyless append remains keyless. If CDF adopts framework-owned row provenance for corrections, it is an internal evidence identity rather than a user-declared semantic key.
7. Retention policy and `cdf package gc` expose whether residual promotion remains possible and protect promotable bytes only under an explicit policy, rather than promising indefinite backfill from already-collected data.

## Open execution-critical questions

1. Should CDF persist a framework-owned immutable row address for every committed row so keyless append destinations that support UPDATE can receive residual promotions in place? If not, keyless append promotion must use correction sidecars or versioned rematerialization.
2. Is the recommended command boundary correct: `cdf schema promote` for residual-evidence promotion, keeping `cdf backfill` for source/cursor re-extraction?
3. Should residual capture be the default unseen-schema verdict only for discovery/experimental/evolve contracts, while governed/freeze contracts quarantine unless explicitly enabled?

## Limits and next step

The user ratified the candidate architectural contract on 2026-07-09. `.10x/decisions/explicit-sampled-discovery-and-residual-promotion.md` is now authoritative for explicit sampling, residual capture, and governed promotion. The exact sampling selector, residual envelope, row-address encoding, promotion transaction ordering, and retention mechanics remain specification blockers rather than research questions; no implementation is authorized to invent them.
