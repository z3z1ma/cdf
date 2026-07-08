Status: active
Created: 2026-07-08
Updated: 2026-07-08

# CLI design language and renderer

## Context

P1 requires CDF's human CLI to become a first-class terminal product. Current source inspection shows human output is plain `String` data carried by `CommandOutput` in `crates/cdf-cli/src/output.rs`. Commands format output locally with `format!` or `human_message()` helpers, including `RunCliReport::human_message()` in `crates/cdf-cli/src/reports.rs` and `format_scan_report()` in `crates/cdf-cli/src/scan_command.rs`.

There is no central renderer, TTY detection, width handling, color policy, table primitive, snapshot strategy, or redaction boundary for human rendering. JSON output is already centralized and stable; it must remain unaffected.

P1 requires semantic color, box drawing and glyphs with ASCII fallback, width-aware tables, panels, humanized units, next-command affordances, headless degradation, and redaction checks.

## Decision

Create a dedicated renderer boundary in `cdf-cli`, initially as `crates/cdf-cli/src/render/**`. Do not create a new crate until reuse outside the CLI is proven.

Use the following library posture:

- `anstyle`/`anstream` or clap-compatible color types for ANSI styling when sufficient.
- `comfy-table` for width-aware tables if local primitives are insufficient.
- `indicatif` only for WS5 live progress, not for static command panels.
- `insta` for renderer snapshots.
- Avoid `console` and `owo-colors` unless implementation evidence shows they reduce complexity beyond the chosen stack.

The renderer owns:

- TTY/headless mode detection.
- Width detection and truncation policy.
- `NO_COLOR`, `CLICOLOR_FORCE`, and `--no-color`.
- Glyph set selection.
- Semantic colors.
- Table, key-value, section, status line, and final-panel primitives.
- Humanized rows, bytes, rates, percentages, and durations.
- Next-command affordance rendering.
- Redaction application for human display values.

The semantic palette is:

| Role | 8-color base | Rich enhancement |
|---|---|---|
| success | green | bright green |
| warning | yellow | amber/yellow |
| error | red | bright red |
| accent | cyan | cyan/blue depending terminal support |
| dim | default dim | gray/dim |
| primary text | default | default |

Glyphs:

- Success: `✓`, ASCII `OK`.
- Failure: `✗`, ASCII `ERR`.
- Flow/next: `→`, ASCII `->`.
- Active bullet: `●`, ASCII `*`.
- Artifact/section marker: `◆`, ASCII `#`.
- Rule lines use Unicode box drawing in rich TTY mode and plain `-` lines in ASCII/headless mode.

Rendering rules:

- JSON mode bypasses the human renderer except for error metadata that is also represented in JSON.
- Headless mode emits no ANSI, no spinners, and line-oriented output readable in CI logs.
- Rendering must never feed package artifacts, hashes, receipts, checkpoints, ledgers, or golden package identity.
- Renderers receive already-redacted values or call the shared redaction registry before display. Secret values must not be smuggled through formatting helpers.
- Every human panel ends with the next useful operator command when there is a natural next step.
- Text must be width-aware and must not wrap into ambiguous tables; truncate with a visible ellipsis only when the full value is also available through JSON or a referenced inspect command.

Snapshot policy:

- Static command renderers require TTY-rich and headless snapshots.
- Error renderers require per-kind snapshots.
- Live progress uses recorded terminal sessions plus non-TTY milestone snapshots under WS5.
- A migration gate must fail if new command output bypasses the renderer after WS3 foundation closes.

## Alternatives considered

- Keep each command formatting its own strings. Rejected because P1 requires consistent TTY/headless behavior, redaction, width handling, and snapshots across all commands.
- Create a new `cdf-cli-render` crate immediately. Rejected because no non-CLI consumer exists yet; a module keeps the first implementation smaller and easier to review. A crate split remains allowed once reuse or compile-time isolation justifies it.
- Use a heavy TUI framework. Rejected because P1 asks for terminal command output, not a resident dashboard or full-screen UI.
- Use color/glyph output in every human context unconditionally. Rejected because headless-first degradation, `NO_COLOR`, and ASCII fallback are hard requirements.

## Consequences

Renderer migration must be staged by command family. The first implementation child should build the renderer types and adapt `CommandOutput` without changing command semantics. Later children migrate plan/run first, then recovery/state/backfill, then the remaining command families.

Every migration child must include snapshots and redaction checks. Raw `human: String` construction remains tolerated only until the migration gate closes; after that, new commands must use renderer primitives.
