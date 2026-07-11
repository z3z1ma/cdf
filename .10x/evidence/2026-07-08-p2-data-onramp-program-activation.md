Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Relates-To: .10x/tickets/done/2026-07-08-p2-data-onramp-program.md, .10x/knowledge/vision-coverage-matrix.md

# P2 data onramp program activation evidence

## What was observed

The P2 data-onramp directive was converted into a durable 10x program graph:

- Parent: `.10x/tickets/done/2026-07-08-p2-data-onramp-program.md`.
- Workstream owners:
  - `.10x/tickets/done/2026-07-08-p2-ws-a-discovery-compiler-stage.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-b-schema-reconciliation-arrow-vocabulary.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-c-source-identity-normalization.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-d-file-source-globs-manifest-compression.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-e-remote-transports.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-f-keys-dispositions.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-g-source-diagnostics-deep-validate.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-h-scaffolding-id-model-two-minute-path.md`
  - `.10x/tickets/done/2026-07-08-p2-ws-i-conformance-parity-friction-suite.md`
- Focused governing decisions:
  - `.10x/decisions/data-onramp-schema-discovery-reconciliation.md`
  - `.10x/decisions/data-onramp-file-source-transport-manifest.md`
  - `.10x/decisions/data-onramp-source-identity-preview-disposition.md`
- Focused governing specs:
  - `.10x/specs/data-onramp-schema-intelligence.md`
  - `.10x/specs/data-onramp-file-sources-transports.md`
  - `.10x/specs/data-onramp-source-experience-cli.md`
  - `.10x/specs/data-onramp-conformance.md`

The old preview one-batch decision was moved to `.10x/decisions/superseded/preview-one-batch-sampling-semantics.md` and superseded by `.10x/decisions/data-onramp-source-identity-preview-disposition.md`.

The full-system parent `.10x/tickets/2026-07-05-implement-cdf-system.md` now lists the P2 parent, and `.10x/knowledge/vision-coverage-matrix.md` has a dedicated active P2 row plus P2 owners on the Chapter 7.5, 8.2, 8.6, 9.2, 13.3, D-14, and D-15 rows.

## Procedure

Inspected governing and adjacent records:

- `VISION.md`, targeted sections around Chapters 7, 8, 11, 13, 19, and 20.
- `.10x/knowledge/vision-coverage-matrix.md`.
- `.10x/specs/resource-authoring-planning-batches.md`.
- `.10x/specs/types-contracts-normalization.md`.
- `.10x/specs/project-cli-observability-security.md`.
- `.10x/specs/conformance-governance-roadmap.md`.
- `.10x/decisions/source-decode-type-drift-quarantine.md`.
- `.10x/decisions/superseded/preview-one-batch-sampling-semantics.md`.
- `.10x/tickets/2026-07-05-implement-cdf-system.md`.

Inspected source anchors for the P2 diagnosis:

- `crates/cdf-kernel/src/resource.rs` has `SchemaSource::Discovered { schema_hash: Option<SchemaHash> }`.
- `crates/cdf-declarative/src/declarations.rs` currently has a small `FieldTypeDeclaration` vocabulary.
- `crates/cdf-declarative/src/compiled.rs` currently turns missing resource schemas into discovered schema with no hash and requires declared `write_disposition`.
- `crates/cdf-declarative/src/file_runtime.rs` currently rejects multi-file run and samples the first preview file.
- `crates/cdf-declarative/src/rest_runtime.rs` currently rejects non-declared schema hashes for execution.

Quality commands run:

```text
rg -n "killer demo" . --glob '!target/**' --glob '!reports/**' --glob '!.git/**' -i || true
rg -n "preview-one-batch-sampling-semantics" .10x --glob '!target/**' || true
git diff --check
for f in <27 changed/new P2 activation record files>; do gitleaks detect --no-git --redact --source "$f" >/dev/null || exit 1; done
jscpd <19 new P2 activation records> --format txt --formats-exts txt:md --min-lines 10 --no-gitignore --reporters json,console --output target/quality/reports/jscpd-p2-records-final --ignore "**/target/**,**/.git/**,**/reports/**"
```

## Results

- Forbidden phrase scan: zero matches.
- Stale preview-decision reference scan: only historical records, the superseded decision path, and the new superseding decision remain.
- `git diff --check`: passed.
- Gitleaks over 27 changed/new record files: passed; no leaks found.
- jscpd over 19 new P2 activation records: passed; 0 clones, 0 duplicated lines, 0 duplicated tokens.

## What this supports or challenges

This supports that the P2 directive is durable in the 10x graph, visible in the coverage matrix, and ready for workstream-specific execution splits without starting implementation.

## Limits

This is activation evidence only. It does not implement discovery, schema reconciliation, file globs, transports, `cdf add`, preview/run parity, or conformance scenarios.
