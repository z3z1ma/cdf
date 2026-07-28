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

Use `.10x/skills/audit-cli-report-authority/SKILL.md` when adding a command, changing a success
report, or auditing report/renderer ownership.
