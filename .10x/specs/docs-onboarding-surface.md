Status: active
Created: 2026-07-08
Updated: 2026-07-08

# Documentation and onboarding surface

## Purpose and scope

This specification governs CDF's in-repository documentation and onboarding surface for P1: quickstart, command reference, error catalog, operator guides, architecture overview, runnable examples, and init-scaffold links.

It derives from `VISION.md` Chapters 18, 20, 22, and 23, `.10x/specs/project-cli-observability-security.md`, `.10x/specs/conformance-governance-roadmap.md`, `.10x/specs/versioning-lts-release-policy.md`, and `.10x/tickets/2026-07-08-p1-product-ws6-docs-onboarding.md`.

## Documentation topology

The repository MUST have a `docs/` tree with these surfaces:

- `docs/quickstart.md`: executable first-run path from clean checkout.
- `docs/commands/`: generated per-command reference from clap definitions.
- `docs/errors/`: generated error-code catalog once WS4 defines the catalog.
- `docs/operators/`: operational guides for recovery, replay, backfill, doctor/status in cron, release/install basics, and troubleshooting.
- `docs/architecture.md`: short architecture overview that links to `VISION.md` and active specs instead of duplicating the book.

The repository MUST have an `examples/` tree with runnable sample projects. The first required examples are a REST-fixture project and a Postgres project.

## Quickstart contract

The quickstart MUST be executable by a capable stranger from a clean checkout with documented prerequisites.

The quickstart MUST cover:

- `cdf init`;
- `cdf validate`;
- `cdf plan`;
- `cdf run`;
- `cdf sql`;
- contract freeze and drift quarantine;
- crash/resume using the existing conformance-safe fixture path rather than depending on external network behavior;
- package replay;
- state or run inspection after replay.

The quickstart MUST distinguish commands the user runs from expected output. Expected output MAY be abbreviated, but it MUST not invent fields or command behavior that parser, renderer, or error records do not support.

## Generated reference contract

Command reference pages MUST be generated from clap command definitions after WS2D provides the generator. Hand-authored command prose MAY introduce concepts, but flags, argument syntax, defaults, aliases, and help text MUST come from the generator.

Error catalog pages MUST be generated from the WS4 error-code catalog. Hand-authored operator guidance MAY link to errors, but stable codes, taxonomy, remediation fields, and exit-code mappings MUST come from the catalog source of truth.

CI MUST fail when generated command reference or error catalog output is stale.

## Examples contract

Each example project MUST include:

- a project file;
- resource definitions;
- fixture data or documented local service setup;
- a README with exact commands;
- a conformance or test hook that executes the example as a living test.

Examples MUST avoid real credentials and external network dependencies unless explicitly named as optional. Secret examples MUST use `secret://` references only.

## Init scaffold contract

`cdf init` MUST scaffold a README once WS6D lands. The README MUST point to `docs/quickstart.md` and describe only commands supported by the current parser and command layer.

The scaffold README MUST not contain secrets, generated runtime state, absolute local paths, or environment-specific assumptions.

## Build and freshness

Docs build and freshness checks MUST run in CI after WS8A lands. At minimum, freshness covers generated command reference and generated error catalog.

Documentation examples MUST be exercised by conformance or an equivalent test target before WS6 closes.

## Explicit exclusions

No external docs site is required. No marketing landing page is required. This spec does not duplicate `VISION.md`, define new CLI behavior, or authorize docs to get ahead of implemented parser, renderer, error, or runtime semantics.
