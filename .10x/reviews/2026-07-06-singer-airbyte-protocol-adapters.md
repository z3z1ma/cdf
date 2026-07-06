Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md
Verdict: pass

# Review of Singer and Airbyte protocol adapters

## Target

Implementation for `.10x/tickets/done/2026-07-06-singer-airbyte-protocol-adapters.md`, covering the new `firn-subprocess` Singer/Airbyte protocol parsing modules, shared protocol helpers, crate-root exports, manifest/lockfile dependency updates, and focused tests.

## Findings

No blocking findings remain.

Parent review found one significant pre-closure test/design gap: opaque `ForeignState` hashing originally used ordinary `serde_json` object serialization, which made hashes depend on JSON object insertion order. The implementation was corrected to emit sorted-key canonical JSON before hashing. Tests now assert canonical bytes and equal hashes for reordered Singer state.

Mutation testing then found 24 missed mutants in parser edge assertions. The tests were hardened rather than accepting weak coverage. The final mutation result was 109 mutants tested, 82 caught, 27 unviable, and 0 missed.

The implementation stays within the intended subprocess adapter surface. It does not modify `.gitignore`, Parquet/archive code, Airbyte destination code, package archive CLI code, or unrelated crates. The root `Cargo.lock` update is directly tied to the permitted `firn-subprocess` dependency additions and was required for `--locked` verification.

## Assumptions tested

- Singer `type` matching is case-insensitive and required malformed `SCHEMA`, `RECORD`, and `STATE` fields become `Data` errors.
- Airbyte `RECORD`, `STATE`, and `CATALOG` envelopes validate their required fields; legacy, stream, and global state blobs remain opaque.
- Unknown envelope fields are retained in raw parsed message values instead of being discarded by typed deserialization.
- State errors identify protocol/type/field/line only and do not include raw secret-bearing blobs.
- Record batches preserve stream identity through explicit `StreamIdentity` output and `ResourceDescriptor.state_scope`.
- Batch IDs include sanitized stream identity without collapsing separators.
- Existing package writer/reader APIs can write and replay batches produced by the protocol adapters.
- Crate-root exports remain thin and implementation stays split across focused modules.

## Verdict

Pass. The ticket is ready to close.

## Residual risk

The adapters intentionally implement only the parser/batch/state slice. They do not define a full subprocess connector lifecycle, Airbyte destination support, state migration UX, or Parquet/package archive behavior.

`cargo deny check` and `cargo vet` remain blocked by unratified repository supply-chain policy, with an existing owner in `.10x/tickets/done/2026-07-06-ratify-supply-chain-policy.md`. CodeQL Rust extraction still has known macro-resolution warnings; SARIF results for this run were 0.
