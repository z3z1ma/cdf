Status: active
Created: 2026-07-07
Updated: 2026-07-07

# Non-File Window-Close Checkpoint Semantics

## Context

`.10x/specs/resource-authoring-planning-batches.md` says inexact cursor ordering or nonzero cursor lag must use window-close semantics: committed cursor advances to `max(cursor) - lag`, not the naive maximum.

The first general-run non-file stream slice intentionally supports only exact zero-lag cursor source positions. `.10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md` was opened for inexact, lagged, multi-segment, page-token, and mixed source-position cases.

The user ratified the recommended boundary on 2026-07-07.

## Decision

Project-run checkpoint advancement for non-file resources MAY implement window-close semantics only for ordered cursor value kinds where arithmetic is unambiguous:

- numeric cursors;
- timestamp cursors;
- date cursors.

For these cursor kinds, nonzero lag advances the committed cursor to the deterministic window-close value, not to the raw maximum observed cursor.

Page tokens are pagination transport state, not durable checkpoint high-water marks, unless paired with a ratified ordered cursor. Page-token-only resources and mixed page-token/cursor positions MUST fail closed before checkpoint mutation until a later decision ratifies their durable state semantics.

Multi-segment non-file runs MUST aggregate compatible ordered cursor positions deterministically. Divergent source-position variants, incompatible cursor fields, or unsupported cursor value kinds MUST fail closed before checkpoint mutation.

## Alternatives considered

Treat page tokens as checkpoint positions by default.

Rejected. Page tokens are API transport artifacts and do not necessarily represent a stable high-water mark.

Use naive maximum cursor for inexact or lagged cursors.

Rejected. It contradicts the active resource spec and risks data loss across eventual-consistency windows.

Accept arbitrary cursor value kinds and define lag later.

Rejected. Lag arithmetic is type-specific and must be explicit before tests encode it.

## Consequences

`.10x/tickets/done/2026-07-07-non-file-window-close-checkpoint-semantics.md` is no longer blocked on user input for numeric/timestamp/date cursor semantics and proceeded within this boundary.

Unsupported cursor kinds and page-token-only checkpointing remain fail-closed behavior, not partially implemented semantics.
