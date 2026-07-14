Status: active
Created: 2026-07-08
Updated: 2026-07-13

# CLI error experience catalog

## Purpose and scope

This specification governs CDF CLI error codes, structured remediation, suggestion behavior, redaction, JSON compatibility, and generated error documentation.

It derives from `VISION.md` Chapters 18, 20, 22, and 23; `.10x/specs/project-cli-observability-security.md`; `.10x/decisions/cli-current-command-grammar.md`; `.10x/decisions/cli-design-language-and-renderer.md`; and `.10x/tickets/done/2026-07-08-p1-product-ws4-error-experience-catalog.md`.

## Behavior

Every CLI error MUST carry:

- shared `ErrorKind`,
- stable exit code,
- stable error code,
- redaction-safe human message,
- optional structured remediation,
- optional structured offending value or location when it is safe to show,
- `not_supported` marker where applicable.

Existing JSON fields are stable. JSON error envelopes MUST retain `ok`, `error.kind`, `error.message`, `error.exit_code`, and `error.not_supported`. Error-code work MAY add fields such as `error.code`, `error.remediation`, `error.location`, and `error.suggestions`, but MUST NOT remove or rename existing fields.

Human errors MUST show what failed, why it failed, and the next useful action. Once the WS3 renderer foundation exists, human errors MUST render through the renderer with TTY/headless snapshots. Before that, structured fields may exist behind the current plain text path.

Error codes MUST be product-stable identifiers, not internal source line labels. The code format is `CDF-<AREA>-<SLUG>`, where:

- `AREA` is a short uppercase product area such as `CLI`, `PROJECT`, `RESOURCE`, `RUN`, `DEST`, `STATE`, `PACKAGE`, `CONTRACT`, `SQL`, `DOCTOR`, or `INTERNAL`.
- `SLUG` is an uppercase hyphenated phrase that names the product failure, for example `UNKNOWN-COMMAND`, `MISSING-RESOURCE`, `DESTINATION-NOT-SUPPORTED`, or `PYTHON-INTERPRETER-FAILED`.

Codes MAY be grouped in source by module, but generated docs MUST list code, area, error kind, exit code, meaning, remediation, and representative command.

`CliError::not_supported` MUST keep exit code 78 and MUST include the lower layer, owning ticket or layer when known, and a remediation that tells the user what path is currently supported.

The CLI MUST use suggestions where project or grammar inventory is available and safe:

- unknown commands and subcommands SHOULD suggest the nearest valid command;
- unknown project resource ids SHOULD suggest nearest configured resource ids;
- unknown destinations or targets SHOULD suggest configured environment destinations or the expected URI shape;
- suggestions MUST be bounded, deterministic, and redacted;
- low-confidence suggestions MUST be omitted instead of noisy.

Redaction is mandatory. Errors and remediation MUST NOT print resolved secret values, tokens, private keys, secret environment variables, raw interpreter stderr/stdout that may contain secrets, or unredacted destination credentials. Redacted values MAY preserve scheme, host, path shape, and key names when useful.

## Exit-code taxonomy

The existing exit-code taxonomy remains:

| Condition | Exit code |
|---|---:|
| CLI usage/parser error | 2 |
| Contract/configuration error | 3 |
| Authentication or secret resolution error | 4 |
| Data/package/source data error | 5 |
| Destination error | 6 |
| Transient or rate-limited retryable error | 75 |
| Not supported in the current implementation | 78 |
| Internal error | 70 |

Commands that intentionally encode command-specific status, such as contract drift or status freshness breach, MAY continue returning their existing nonzero domain exit codes if JSON and docs explain them.

## Acceptance criteria

- Every `CliError` construction site carries a stable error code or converts from a lower-layer error through a documented mapping.
- `--json` errors include additive `code` and `remediation` fields without breaking existing fields.
- Human errors include remedial next action once renderer integration lands.
- Unknown command/resource/destination cases produce deterministic suggestions when confidence is high enough.
- Not-supported errors retain exit 78 and name the required lower layer or owner.
- Redaction tests prove secrets do not appear in messages, remediation, suggestions, JSON, or human output.
- Generated docs fail freshness checks when the code catalog changes without regeneration.

## Explicit exclusions

This spec does not change the shared kernel `ErrorKind` taxonomy, existing stable exit codes, success JSON envelopes, or product behavior that produced the underlying failure. It does not require renderer implementation before WS3B exists.
