Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md
Depends-On: .10x/decisions/data-onramp-schema-discovery-reconciliation.md, .10x/specs/data-onramp-schema-intelligence.md

# P2 WS-B1 declarative Arrow type vocabulary

## Scope

Expand Tier-0 declarative field type parsing and compilation from the current small `FieldTypeDeclaration` enum to the Arrow vocabulary required by P2, without yet implementing full observed-vs-declared reconciliation.

Owned write scope:

- `crates/cdf-declarative/src/declarations.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`
- generated declarative JSON Schema artifacts if the repository has an existing committed location for them

## Acceptance criteria

- Existing type spellings remain accepted: `string`, `utf8`, `int64`, `uint64`, `float64`, `boolean`, `date32`, `timestamp_millis`, `timestamp_micros`, and `json`.
- New ergonomic string forms parse from TOML/YAML and compile to Arrow `DataType`:
  - integer widths: `int8`, `int16`, `int32`, `int64`, `uint8`, `uint16`, `uint32`, `uint64`
  - floats: `float16`, `float32`, `float64`
  - decimals: `decimal(38,9)` and at least one `decimal256(...)` spelling
  - dates/times/timestamps/durations with units, including `timestamp(us, UTC)`
  - binary/large variants: `binary`, `large_binary`, `utf8`, `large_utf8`
  - nested forms: `list<int64>`, `struct<amount: decimal(38,9), tags: list<utf8>>`, and `map<utf8,int64>`
- Structured forms may be added if they simplify schema validation, but the string forms above are mandatory.
- Invalid forms fail at parse/compile time with the offending type string named.
- The generated JSON Schema accepts the string form rather than enumerating only the old variants.
- Tests cover representative round-trips from TOML into compiled Arrow `DataType` and JSON Schema generation.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-declarative <new type parser/schema tests> --locked`
- `cargo test -p cdf-declarative --locked`
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- `jscpd` scoped to touched Rust files
- `git diff --check`

If broader dependency gates fail for unrelated dirty work, record the exact limit and keep this ticket open until scoped evidence is strong enough.

## Explicit exclusions

This ticket does not implement schema discovery probes, pinned snapshots, widening/coercion reconciliation, destination mapping decisions, or package validation-program serialization.

## Progress and notes

- 2026-07-08: Opened as the first WS-B executable slice. Source inspection found the current vocabulary in `crates/cdf-declarative/src/declarations.rs` and Arrow mapping in `crates/cdf-declarative/src/compiled.rs`.

## Blockers

None.
