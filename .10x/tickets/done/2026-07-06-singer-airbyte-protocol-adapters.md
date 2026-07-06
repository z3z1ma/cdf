Status: done
Created: 2026-07-06
Updated: 2026-07-06
Parent: .10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md
Depends-On: .10x/tickets/done/2026-07-05-formats-and-subprocess.md, .10x/tickets/done/2026-07-05-package-builder-reader.md

# Implement Singer and Airbyte protocol adapters

## Scope

Implement the first fast-follow Tier 4 Singer/Airbyte source-adapter slice in `firn-subprocess`: protocol message parsing, record-to-batch conversion, schema/catalog exposure where practical, and opaque protocol state mapping to `SourcePosition::ForeignState`.

Owns `crates/firn-subprocess/**`, its manifest if a small JSON dependency change is required, and its own evidence/review records. It may add focused modules such as `singer.rs` or `airbyte.rs`; do not grow a monolithic `src/lib.rs`.

## Acceptance criteria

- Singer stdout parsing accepts newline-delimited JSON messages with case-insensitive `type`, recognizes `SCHEMA`, `RECORD`, and `STATE`, rejects malformed required fields as `Data` errors, and permits unknown fields without data loss.
- Singer records produce `FormatRead`/kernel batches grouped by stream or otherwise surfaced with explicit stream identity; Singer `STATE.value` is preserved as opaque `ForeignState` with protocol `singer` and a deterministic blob hash.
- Airbyte stdout parsing recognizes `RECORD`, `STATE`, and `CATALOG` AirbyteMessage envelopes, rejects malformed required fields as `Data` errors, and permits forward-compatible unknown fields.
- Airbyte records produce `FormatRead`/kernel batches with stream identity preserved; legacy, stream, and global state messages are preserved as opaque `ForeignState` values with protocol `airbyte` and deterministic blob hashes.
- Parser output can be written to and replayed from a Firn package using the existing package APIs.
- No raw state blob or secret-bearing protocol payload is included in error messages, traces, or examples.
- The implementation follows `.10x/knowledge/rust-crate-organization.md` and keeps crate-root exports thin.

## Evidence expectations

Record tests for Singer schema/record/state parsing, Airbyte catalog/record/legacy-stream-global state parsing, malformed message handling, deterministic `ForeignState` hashes, package write/replay compatibility, formatting, clippy, and targeted tests for `firn-subprocess`.

## Explicit exclusions

No `firn package archive`, Parquet transcode, Airbyte destination support, full connector lifecycle, external process invocation contract changes, or state migration UI. Parquet archive work remains gated by `.10x/tickets/done/2026-07-06-parquet-format-source-supply-chain.md` or a later ratified policy/alternative writer.

## References

- `.10x/research/2026-07-06-singer-airbyte-protocol-scope.md`
- `.10x/specs/resource-authoring-planning-batches.md`
- `.10x/specs/checkpoint-state-firn-line.md`
- `.10x/knowledge/rust-crate-organization.md`

## Progress and notes

- 2026-07-06: Split from `.10x/tickets/done/2026-07-05-singer-airbyte-and-package-archive.md` so protocol parsing and `ForeignState` mapping can proceed independently from Parquet archive work.
- 2026-07-06: Worker subagent started implementation after reading the ticket, referenced research/spec/knowledge records, current `firn-subprocess` files, `firn-formats` read model, kernel batch/position types, and package writer/reader APIs. Existing dirty `.gitignore` is unrelated and must remain untouched.
- 2026-07-06: Implemented focused `protocol`, `singer`, and `airbyte` modules; added thin exports; added direct JSON/hash dependencies and refreshed `Cargo.lock` metadata required for locked verification; added tests for Singer schema/record/state, Airbyte catalog/record/legacy-stream-global state, malformed required fields without raw state leakage, deterministic state hashes, and package write/replay compatibility.
- 2026-07-06: Verification evidence recorded in `.10x/evidence/2026-07-06-singer-airbyte-protocol-adapters.md`; closure review recorded in `.10x/reviews/2026-07-06-singer-airbyte-protocol-adapters.md` with verdict `pass`. Worker considers the ticket ready for parent review/closure.
- 2026-07-06: Parent review found and fixed nondeterministic JSON-object ordering in `ForeignState` hashing by canonicalizing state JSON before hashing. Mutation testing initially found 24 missed parser-edge mutants; tests were hardened and final mutation testing reported 109 mutants tested, 82 caught, 27 unviable, 0 missed.
- 2026-07-06: Final QUALITY evidence is recorded in `.10x/evidence/2026-07-06-singer-airbyte-protocol-adapters.md`; review remains `pass`. Ticket closed and moved to `tickets/done/`.

## Blockers

None.
