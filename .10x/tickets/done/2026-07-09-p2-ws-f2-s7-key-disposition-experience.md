Status: done
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/done/2026-07-08-p2-ws-f-keys-dispositions.md
Depends-On: .10x/tickets/done/2026-07-08-p2-ws-f1-append-default-merge-key-error.md, .10x/specs/data-onramp-source-experience-cli.md, .10x/specs/data-onramp-conformance.md, .10x/decisions/data-onramp-source-identity-preview-disposition.md

# P2 WS-F2 S7 key and disposition experience

## Scope

Complete the ratified S7 operator experience on top of F1: append must validate, plan, preview, and run keylessly without warnings or scaffold/docs pressure to invent a key; merge without `merge_key` must fail once before source/destination mutation with command-correct remediation naming both fixes.

Audit current project scaffolds, quick examples, source-specific messages, plan rendering, and CLI validation paths for obsolete primary/composite-key nudges. Change only P2 source-experience material; historical examples that intentionally demonstrate merge remain valid.

## Acceptance criteria

- Keyless append succeeds through validate/plan/preview/run for representative file and REST resources and renders no key warning or suggestion.
- Merge without `merge_key` emits one plan/validation failure before source contact or writes, names `merge_key`, and says either add it or use append.
- The error identifies the command/resource context and uses the P1 error catalog/remediation shape.
- Local scaffolds and append-oriented current docs/examples omit primary/merge keys unless their semantics independently require them.
- Merge examples retain explicit keys; protective destination merge tests are not weakened.
- The executable P2 friction registry names the S7 regression tests and does not promote S7 until the exact conformance scenario is present.

## Evidence expectations

CLI human/JSON snapshots, no-write/source-contact guards, declarative and project tests, repository message/example audit, focused S7 conformance registration, and the applicable `QUALITY.md` profiles.

## Explicit exclusions

Exact-row keyless dedup remains excluded pending the explicit option-name/ordering checkpoint. This ticket does not infer keys, add `cdf add` suggestions, change merge semantics, or implement SCD2.

## Progress and notes

- 2026-07-09: Opened after F1 established compiler defaults but the P2 audit showed S7 still lacks end-to-end CLI/conformance and message/documentation proof.
- 2026-07-09: Merge-without-key compilation now preserves the canonical `<source>.<resource>` id and maps through the command-correct `CDF-PROJECT-MERGE-KEY` catalog entry. JSON and human CLI regressions cover `validate`, `plan`, `preview`, and `run`, require the named resource and both remediations, and prove the compiler failure occurs before HTTP contact or project-tree mutation.
- 2026-07-09: Added representative file and REST regressions proving keyless append succeeds through validate/plan/preview/run without primary-key or merge-key nudges. The runs commit two rows and retain append/effectively-once-per-package semantics.
- 2026-07-09: Audited `README.md`, `docs/`, `cdf-project` scaffolding, and `cdf add` output: current append-oriented shipped surfaces contain no `primary_key` or `merge_key`. Added a scaffold regression locking that absence. Intentional merge examples and destination protective merge checks remain unchanged.
- 2026-07-09: Registered the three CLI regressions under S7 and friction 17 while deliberately retaining `CoverageStatus::Pending`; the exact standalone S7 conformance scenario and exact-row dedup remain outside this slice. Updated `.10x/evidence/2026-07-08-p2-friction-regression-registry.md` with the same boundary.
- 2026-07-09: Verification passed: `cargo fmt -p cdf-declarative -p cdf-cli -p cdf-conformance -p cdf-project -- --check`; `cargo test -p cdf-project -p cdf-declarative -p cdf-cli -p cdf-conformance --lib --locked` (495 passed, 0 failed); and `cargo clippy -p cdf-project -p cdf-declarative -p cdf-cli -p cdf-conformance --all-targets --locked --no-deps -- -D warnings`.
- 2026-07-09: Closed after parent-observed integration verification passed 773/773 workspace tests and adversarial review reconfirmed pre-contact failure, command-correct remediation, and the absence of append key nudges. Closure evidence: `.10x/evidence/2026-07-09-p2-b7-f2-integration.md`. Review: `.10x/reviews/2026-07-09-p2-b7-f2-integration-review.md`. The exact standalone S7 conformance scenario remains an independently owned WS-I obligation and was not used to hold this bounded experience slice open.

## Blockers

None. Exact-row dedup is explicitly excluded; append and merge behavior are already ratified.
