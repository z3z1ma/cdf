Status: open
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md
Depends-On: .10x/decisions/data-onramp-source-identity-preview-disposition.md, .10x/specs/data-onramp-source-experience-cli.md

# P2 WS-F1 append default and merge-key error

## Scope

Make declarative resource disposition defaults match P2: append is the default and requires no key; merge requires an explicit merge identity and emits one precise plan-time error when missing.

Owned write scope:

- `crates/cdf-declarative/src/declarations.rs`
- `crates/cdf-declarative/src/compiled.rs`
- `crates/cdf-declarative/src/tests.rs`
- scaffold/example text only if current scaffolds require a key for append

## Acceptance criteria

- A declarative resource with no `write_disposition`, no `primary_key`, and no `merge_key` compiles as append.
- Append resources do not require keys and do not emit key-related validation errors.
- A resource with `write_disposition = "merge"` and no `merge_key` fails before execution with remediation naming both fixes: add `merge_key`, or use append.
- `merge_key` no longer silently defaults to `primary_key` for new declarations unless an existing compatibility test proves that behavior is still required; if compatibility is retained, record it explicitly in this ticket before closure.
- Tests cover append default, keyless append, merge-without-key failure, and merge-with-key success.

## Evidence expectations

Record focused evidence for:

- `cargo test -p cdf-declarative <new disposition tests> --locked`
- `cargo test -p cdf-declarative --locked`
- `cargo clippy -p cdf-declarative --all-targets --locked -- -D warnings`
- `cargo fmt --all -- --check`
- source scan proving no current append scaffold/example still nudges fake keys, or a follow-up ticket if broader docs are out of scope
- `git diff --check`

## Explicit exclusions

This ticket does not implement `cdf add` key suggestions, exact-row dedup, or destination merge semantics beyond declarative compile-time validation.

## Progress and notes

- 2026-07-08: Opened after source inspection found `compile_resource` currently requires `write_disposition` and defaults missing `merge_key` to `primary_key`.

## Blockers

None.
