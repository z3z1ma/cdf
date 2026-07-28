---
name: audit-cli-report-authority
description: "Use when adding or changing a CLI success report, moving command rendering, preserving JSON output shapes, or auditing JSON/human redaction parity."
metadata:
  created: 2026-07-27
  updated: 2026-07-27
---

# Audit CLI Report Authority

## Objective

Prove that each successful command constructs one typed serializable report and that the same
report is the sole input to stable JSON and renderer-owned human output.

## Prerequisites

1. Read `.10x/knowledge/cli-report-authority.md` and the governing CLI spec/ticket.
2. Resolve the command execution files, report types, renderer modules, and existing JSON/human
   tests in scope.
3. Record the pre-change JSON shape and secret-bearing fields for each affected outcome.

## Procedure

### 1. Inventory success construction

Search every success transport call and anonymous JSON value:

```sh
rg -n 'CommandOutput::rendered|rendered_with_(progress|exit_code|progress_and_exit_code)' \
  crates/cdf-cli/src -g '*.rs'
rg -n 'serde_json::json!|\bjson!\(' crates/cdf-cli/src -g '*command.rs'
```

For each call, identify the human renderer input and serialized value. They must be the same named
report or the same typed domain report; a wrapper used only by one path is not authority.

### 2. Separate execution from layout

Move document construction into a sibling `render.rs` or established report module. Command
executors may call a renderer but must not import `RenderDocument`, panels, status lines, tables,
glyphs, width policy, or ANSI styling. Keep execution and mutation out of renderer modules.

### 3. Preserve machine shape

Use explicit typed fields. For a legacy top-level array, prefer a named
`#[serde(transparent)]` report. For an existing object wrapped with presentation context, use
`#[serde(flatten)]` for the serialized object and `#[serde(skip)]` for presentation-only fields.
Do not replace known report fields with string maps or unstructured `Value` merely to satisfy the
transport. Compare the old and new JSON shapes in a focused test.

### 4. Make redaction path-independent

Redact URI userinfo and other sensitive values before either output path. When a structured typed
field can contain sensitive strings, use a typed serialization adapter that recursively redacts
its serialized representation while the renderer consumes only redacted display values. Add one
secret-bearing parity test that asserts the secret is absent and the redacted value is present in
both JSON and human output.

### 5. Fence and validate

Maintain a static source gate over `commands.rs`, every `*_command.rs`, and nested command
executors. Reject `RenderDocument` and primitive layout construction there, plus legacy
plain-string output shims.

Use the shared panel constructors for `Summary`, `Proof`, `Effects`, `Recovery`, and `Attention`;
do not reintroduce those headings as repeated strings. Extend the renderer family matrix when a
new outcome category is introduced. The matrix must preserve outcome-first hierarchy, named
facts, no-color behavior, and width at 40/80/160 columns across TTY/headless and ASCII/Unicode.
Keep exact rich/headless primitive snapshots, JSON isolation, and progressive-disclosure tests as
separate fences.

Run:

```sh
cargo fmt --all -- --check
cargo test -p cdf-cli-core --all-features --locked
cargo test -p cdf-cli --lib
cargo check -p cdf-cli-benchmarks --benches --locked
cargo bench -p cdf-cli-benchmarks --bench cli_renderer --locked
cargo tree -p cdf-cli-core -e normal --prefix none --locked
cargo tree -p cdf-cli-core --all-features -e normal --prefix none --locked
git diff --check
```

Use the repository's DuckDB library environment when product CLI tests require it. Record graph
counts, forbidden-edge results, test totals, the million-event and large-static-report benchmark
cells, and limits rather than claiming global correctness. For a coordinated experience change,
retain one real local and one public-HTTPS product smoke without treating them as exhaustive.

## Validation

- Human and JSON transport consume the same typed report at every scoped success call.
- Command execution modules contain no layout assembly.
- Transparent/flattened wrappers preserve prior JSON shapes.
- Secret-bearing parity tests pass for JSON and human output.
- Headless/rich renderer tests and progress throughput floors pass.
- `cdf-cli-core` stays below its package ceilings and contains no forbidden product edges.
- The static migration gate, formatting, and diff checks pass.
- Shared section headings come from the code-owned vocabulary constructors.
- The family matrix, progress benchmark, large-report benchmark, and scoped product smokes pass.
