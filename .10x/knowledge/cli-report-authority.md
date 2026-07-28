Status: active
Created: 2026-07-27
Updated: 2026-07-27

# CLI report authority

Every successful CLI outcome has one serializable typed report. Command execution constructs
domain values and that report; a report-owned renderer maps only the report to `RenderDocument`.
JSON serialization consumes the same report. Command execution modules do not import
`RenderDocument` or layout primitives.

Presentation-only facts still belong to the report when they affect human output. Preserve the
machine contract with `#[serde(skip)]` fields rather than passing a second renderer argument.
When a named report replaces a legacy JSON array, `#[serde(transparent)]` preserves the array
shape. When it wraps an existing object report, `#[serde(flatten)]` preserves the object shape.
These attributes require explicit parity tests because a compiling serializer does not prove the
old JSON contract.

Redaction is a report-boundary responsibility. Construct redacted display values before rendering
and apply typed serialization adapters to structured fields that may contain URI userinfo; test
the same secret-bearing report through JSON and human output. A human-only redaction helper is
not evidence that JSON is safe.

Renderer modules may humanize, truncate, select glyphs, and arrange panels/tables. They must not
perform execution, resolve secrets, mutate durable state, or invent facts absent from the report.
The static migration gate treats `commands.rs`, every `*_command.rs`, and nested command executors
as the protected surface and rejects layout types there.

Shared report vocabulary is code-owned rather than repeated string convention. Use
`KeyValuePanel::summary()`, `proof()`, `effects()`, `recovery()`, and `attention()` for those
cross-family sections. Specialized domain headings remain valid when they name a distinct
operator concept. `Effects` names both committed writes and an explicit no-op (`writes: none`);
`Proof` contains evidence rather than another outcome summary.

Holistic CLI review is a conformance problem, not a command-by-command aesthetic rewrite. Pair
exact primitive snapshots with a family matrix spanning inspect, plan, execute, mutate, recover,
list, no-op, warning, and failure at 40/80/160 columns under TTY/headless, ASCII/Unicode, and
no-color policies. Preserve JSON isolation and progressive disclosure separately. Measure both
the million-event progress path and a representative large static report, then retain a real
local and public-HTTPS product smoke for composition evidence.

Use `.10x/skills/audit-cli-report-authority/SKILL.md` when adding a command, changing a success
report, or auditing report/renderer ownership.
