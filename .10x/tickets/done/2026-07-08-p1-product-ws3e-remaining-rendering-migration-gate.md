Status: done
Created: 2026-07-08
Updated: 2026-07-08
Parent: .10x/tickets/2026-07-08-p1-product-ws3-rendering-system-design-language.md
Depends-On: .10x/tickets/done/2026-07-08-p1-product-ws3b-renderer-foundation.md, .10x/tickets/done/2026-07-08-p1-product-ws3c-plan-run-rendering.md, .10x/tickets/done/2026-07-08-p1-product-ws3d-recovery-state-backfill-rendering.md, .10x/decisions/cli-design-language-and-renderer.md

# P1 product WS3E: Remaining rendering migration gate

## Scope

Migrate remaining command families to the renderer and add the gate that prevents new raw human-output paths.

Owns project/init/validate/diff, contract, package, doctor, status, SQL, general help/version where not already owned by parser/generated help, and the migration gate.

## Acceptance criteria

- Remaining commands render through the renderer in TTY-rich and headless modes.
- SQL results use table rendering rather than ad hoc text where result shape allows.
- Doctor/status/package/contract outputs use consistent status glyphs and panels.
- A test or static check fails when new command output bypasses the renderer after this ticket closes.
- The WS3 migration checklist proves no command remains on raw `human: String` formatting except compatibility shims explicitly owned by the renderer.
- JSON output remains stable.

## Evidence expectations

Snapshots for every remaining command family, migration-gate output, redaction checks, fmt/clippy, source-only Gitleaks, direct unsafe scan, focused `jscpd`, and adversarial review.

## Explicit exclusions

No live progress. No parser grammar changes. No docs generation beyond output snapshots.

## Blockers

Depends on WS3B, WS3C, and WS3D.

## Progress and notes

- 2026-07-08: Worker inspected `VISION.md`, `QUALITY.md`, this WS3E ticket, `.10x/decisions/cli-design-language-and-renderer.md`, WS3B/WS3C/WS3D tickets plus evidence and reviews, governing CLI/renderer specs, grammar and run-ledger records, and current `cdf-cli` output call sites before editing. Existing unrelated WASM records are outside scope and remain untouched.
- 2026-07-08: Migrated project/init/validate/diff, contract, package, doctor, status, SQL, preview, inspect, and resume-report compatibility output to `RenderDocument`-backed human rendering. Removed the raw `HumanOutput::Plain` variant; help/version generated text now uses a documented `RenderDocument::text` compatibility shim in `commands.rs`.
- 2026-07-08: Added renderer table support for dynamic SQL result shapes, static migration gate coverage, and human-mode regression tests for preview, inspect inventory, status, SQL tables, package archive, and `package ls` JSON compatibility.
- 2026-07-08: Review caught and fixed a `package ls` JSON compatibility regression; JSON result remains the pre-existing array shape while human output uses renderer panels/tables.
- 2026-07-08: Quality evidence recorded in `.10x/evidence/2026-07-08-p1-product-ws3e-remaining-rendering-migration-gate.md`; adversarial review recorded in `.10x/reviews/2026-07-08-p1-product-ws3e-remaining-rendering-migration-gate-review.md`.
