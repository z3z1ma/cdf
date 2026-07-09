Status: open
Created: 2026-07-09
Updated: 2026-07-09
Parent: .10x/tickets/2026-07-08-p2-ws-f-keys-dispositions.md
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

## Blockers

None. Exact-row dedup is explicitly excluded; append and merge behavior are already ratified.
