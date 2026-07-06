Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Relates-To: .10x/tickets/done/2026-07-05-declarative-resources.md

# Declarative resources implementation evidence

## What was observed

`crates/firn-declarative` now exposes a Tier 0 declarative model for TOML/YAML resource files, parser helpers, JSON Schema artifact generation, semantic validation, and compiled plan/resource structures for REST, SQL, and file resources.

The implementation compiles declarations into `firn-kernel` `ResourceDescriptor`, `ResourceCapabilities`, and `QueryableResource` plan behavior. REST plans use `firn-http` auth, pagination, rate-limit, secret URI, and egress allowlist concepts. REST cursor predicates negotiate as `Inexact` by default and only become `Exact` when `cursor.filter_fidelity = "exact"` is declared.

No shared type additions were made.

## Procedure

Inspected the owning ticket, active resource/type/project specs, glossary, quality-gate knowledge, closed kernel/HTTP/contract dependency tickets, the book's Tier 0 REST example, and the public APIs for `firn-kernel`, `firn-http`, and `firn-contract`.

Added focused tests in `crates/firn-declarative/src/lib.rs` covering:

- book-style TOML REST parsing and compilation into a `QueryableResource`;
- REST cursor pushdown defaulting to `Inexact` and explicit exact override;
- YAML SQL and file resource MVP descriptor compilation;
- JSON Schema artifact generation for editor/project validation;
- semantic validation failures for missing declared-schema key fields and missing sample cursor fields;
- SQL filter negotiation as exact MVP pushdown.

## Command Results

`cargo fmt -p firn-declarative` passed.

`cargo test -p firn-declarative --locked --no-fail-fast` passed: 7 unit tests passed, 0 failed; doc-tests ran 0 tests and passed.

`cargo clippy -p firn-declarative --all-targets --locked -- -D warnings` passed.

`git diff --check` passed after the final record updates.

## What This Supports

This supports the ticket acceptance criteria for parser coverage, JSON Schema artifact/model generation, REST planning and pushdown fidelity, semantic cursor/key validation, and SQL/file MVP descriptor compilation.

## Limits

The crate remains a compiler/planning boundary. Concrete REST/SQL/file byte execution is outside this child ticket; `CompiledResource::open` returns an internal error rather than performing I/O. `Cargo.lock` was refreshed while other workers' engine/formats/subprocess dependency changes were present in the shared workspace, so the lockfile reflects the current workspace dependency graph, not only declarative dependencies.
