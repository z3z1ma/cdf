Status: done
Created: 2026-07-06
Updated: 2026-07-06

# Singer and Airbyte protocol scope

## Question

What protocol behavior is sufficiently concrete to implement the first Firn Singer/Airbyte subprocess adapter slice without inventing semantics or touching the Parquet archive work that remains supply-chain gated?

## Sources and methods

- Inspected `.10x/specs/resource-authoring-planning-batches.md`, `.10x/specs/checkpoint-state-firn-line.md`, `.10x/specs/package-lifecycle-determinism.md`, and `.10x/tickets/2026-07-05-singer-airbyte-and-package-archive.md`.
- Checked the current Singer specification at `https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md`.
- Checked the current Airbyte protocol documentation at `https://docs.airbyte.com/platform/understanding-airbyte/airbyte-protocol`.

## Findings

Singer protocol facts are stable enough for a parser slice:

- A tap writes one JSON object per stdout line.
- Message `type` values are case-insensitive.
- `RECORD` messages require `stream` and object `record`; `time_extracted` is optional.
- `SCHEMA` messages require `stream`, JSON Schema `schema`, and `key_properties`; `bookmark_properties` is optional.
- `STATE` messages require `value`, and the value's semantics are tap-defined. Firn MUST preserve it as opaque foreign state unless a later adapter knows the tap-specific meaning.

Airbyte protocol facts are stable enough for a parser slice:

- Protocol messages are AirbyteMessage envelopes with a required `type` and forward-compatible additional properties.
- Relevant message types for the first source-adapter slice are `RECORD`, `STATE`, and `CATALOG`; `LOG`, `TRACE`, `SPEC`, and `CONNECTION_STATUS` can be parsed or ignored without becoming data batches.
- Record messages identify `stream`, optional `namespace`, required JSON `data`, and required `emitted_at`.
- Catalog streams expose stream names, optional namespaces, JSON schemas, supported sync modes, cursor fields, and configured stream sync mode/primary-key choices.
- State messages include legacy black-box state and newer stream/global state. Airbyte explicitly treats state contents as black-box data owned by the source, so Firn should map them to scoped `ForeignState` positions rather than interpreting the state blob.

## Conclusions

Open a focused child ticket for Singer/Airbyte protocol parsing and `ForeignState` mapping in `firn-subprocess`. The child can use existing JSON and Arrow/NDJSON machinery, add no Parquet dependency, and preserve the crate-organization convention by using focused modules rather than growing `src/lib.rs`.

Do not include `firn package archive` in that child. Archive Parquet transcode remains blocked by `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md` or by a later ratified supply-chain policy/alternative writer.

## Limits

This research does not choose a full connector execution lifecycle, state migration UI, or Airbyte destination behavior. It only ratifies the minimum message shapes and opaque-state handling needed for a parser/adapter slice.
