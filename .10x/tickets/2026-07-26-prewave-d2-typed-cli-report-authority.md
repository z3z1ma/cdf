Status: active
Created: 2026-07-26
Updated: 2026-07-26
Parent: `.10x/tickets/2026-07-26-pre-wave-architecture-hardening-program.md`
Depends-On: `.10x/tickets/done/2026-07-26-prewave-d1c-product-error-audit.md`

# Complete typed CLI report authority

## Scope

Make one typed serializable report the sole success authority for every command and move all
human layout construction out of command execution modules into report renderers. Remove
superseded command-local layout helpers and plain compatibility surfaces.

## Non-goals

- No command grammar, execution behavior, JSON field removal, or dynamic renderer plugin.
- No trait-per-command service architecture.
- No visual redesign beyond consistency required to complete the authority migration.

## Acceptance criteria

- Every command constructs one typed report consumed by JSON and human rendering.
- Command execution modules contain no `RenderDocument`/primitive layout assembly.
- Report renderers own TTY/headless documents and have paired snapshots plus JSON parity tests.
- Redaction occurs before both render paths.
- Static migration gates reject new command-local layout and legacy plain-string output.
- CLI-core build-graph and renderer throughput floors remain green.

## References

- `.10x/specs/cli-report-authority-and-environment-errors.md`
- `.10x/decisions/cli-design-language-and-renderer.md`
- `.10x/specs/product-build-graph-boundaries.md`

## Assumptions

- Source-backed: `CommandOutput` already accepts only `RenderDocument`; this ticket completes the
  partially migrated report-to-document ownership rather than replacing output transport.

## Journal

- 2026-07-26: Inventory found many typed reports and a closed plain-output gate, but layout
  methods/functions remain distributed through command modules.
- 2026-07-27: Activated after D1c closure. Governing records preserve command behavior, JSON
  fields, redaction, and the existing `CommandOutput::rendered` transport boundary; this ticket
  moves only human layout construction and adds the migration fence.
- 2026-07-27: Moved command-family layout into report-owned renderer modules. Replaced the
  remaining anonymous/mismatched success values with typed reports while preserving JSON shapes
  through transparent/flattened serialization, and made report-only presentation inputs
  (`--explain-memory`, plan command/URI, state scope context) explicit non-serialized fields.
- 2026-07-27: Extended the static migration gate across every command execution module. A full
  `cdf-cli` library run passed 297/297 after the authority migration; the focused inspect parity
  test also proves URI userinfo is absent from both JSON and headless human rendering.

## Blockers

None.

## Evidence

Pending.

## Review

Pending.

## Retrospective

Pending.
