Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md
Verdict: pass

# P1 product WS3B renderer foundation review

## Target

Implementation and closure evidence for `.10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md`.

Evidence: `.10x/evidence/2026-07-08-p1-product-ws3b-renderer-foundation.md`.

## Assumptions tested

- WS3B must create the renderer boundary without migrating command families owned by WS3C/WS3D.
- JSON output must retain the existing success envelope and bypass rendered human output.
- TTY/headless, width, `NO_COLOR`, `CLICOLOR_FORCE`, and `--no-color` must be represented in one config boundary.
- Headless output must remain line-oriented, ASCII-safe, and ANSI-free.
- The renderer must provide primitives broad enough for later migration without adding speculative crates or live-progress behavior.
- Redaction must be present as a renderer boundary primitive without weakening existing secret handling.

## Findings

No blocking findings.

Pass: `crates/cdf-cli/src/render/**` is split into focused modules for config, styling/glyphs, primitives, humanization, and redaction. The module is intentionally marked as staged dead code because WS3B installs the boundary before command-family migration tickets consume it.

Pass: `RenderConfig` owns the required display inputs. `CLICOLOR_FORCE` is represented but does not override headless mode, matching the active decision that headless mode emits no ANSI.

Pass: `CommandOutput` now has a typed rendered-human hook while existing command families continue through `HumanOutput::Plain`. This keeps WS3B from silently migrating plan/run/replay/resume/backfill/state output.

Pass: JSON bypass is tested with a `Rendered(RenderDocument)` output. The test proves the human text is absent from the JSON envelope and the machine JSON result remains present.

Pass: snapshot-style tests cover rich TTY and headless/static output for representative primitives. The headless fixture proves ASCII fallback and no ANSI. Width/truncation behavior is covered with a narrow table fixture.

Pass: No dependency was added. Avoiding `comfy-table`, `anstyle`, or `insta` in this foundation is acceptable because the local primitive implementation is small, tested, and dependency-free. Later migration tickets can still add a ratified dependency if real command layouts outgrow these primitives.

Pass: Quality gates in evidence cover focused renderer tests, full `cdf-cli` tests, fmt, clippy, direct unsafe scan, Semgrep, source-only Gitleaks, jscpd, complexity metrics, and reusable CodeQL.

Pass: Parent review reran fmt, focused renderer/parser tests, full `cdf-cli` tests, clippy, scoped diff whitespace checks, and direct unsafe-token scanning over the renderer slice. No parent-observed finding blocks closure.

Residual risk: the table width implementation uses simple character counts rather than Unicode display width. Current tested renderer values are ASCII plus known box glyphs outside cells; if future command output needs arbitrary wide Unicode cell content, the migration ticket should either constrain values or add a width-aware dependency.

Residual risk: the renderer redaction helper is an exact-value primitive, not a complete secret registry. This matches WS3B's boundary scope; command migrations must continue passing already-redacted values or explicitly using shared redaction at the call site until a broader registry is introduced.

Residual risk: `CLICOLOR_FORCE` is stored and detected but has no visible effect in headless mode by design. A future display-policy ticket can add an explicit always-color mode only if it supersedes the current headless no-ANSI rule.

## Verdict

Pass. The WS3B acceptance criteria are supported by implementation and evidence, and no review finding blocks closing the ticket.

## Residual risk

WS3C/WS3D still need to migrate command-family output to the renderer. WS3E still needs the migration gate that prevents new raw human output from bypassing the renderer after staged migration closes.
