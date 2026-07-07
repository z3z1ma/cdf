Status: recorded
Created: 2026-07-06
Updated: 2026-07-06
Target: .10x/tickets/done/2026-07-06-declarative-file-preview-execution.md
Verdict: pass

# Declarative file preview execution review

## Target

Review of the implementation and closure evidence for `.10x/tickets/done/2026-07-06-declarative-file-preview-execution.md`.

## Findings

No unresolved findings.

Issues found during review and resolved before closure:

- Partition validation was initially under-tested. A mutation changing `validate_partition` to unconditional success survived the first mutation run. Added `file_runtime_rejects_partition_metadata_that_does_not_match_plan` to prove mismatched partition metadata is rejected before runtime open.
- Glob and path traversal coverage was initially too narrow. Added tests for zero-match roots, missing literal intermediate paths, wildcard directory components, `?`, `**`, unreadable path errors, symlink-directory loops, and symlink-directory aliases.
- Recursive glob traversal needed symlink hardening. The final implementation uses symlink metadata to descend only into physical directories and canonicalizes matched files under the source root.
- The implementation keeps the current supply-chain boundary. It delegates Parquet preview to the existing DuckDB-backed `firn-formats::FileResource` path and does not add native arrow-rs `parquet` or `paste`.

## Verdict

Pass. The implementation satisfies the child ticket's single-match declarative local file preview scope, and the recorded evidence maps to the ticket acceptance criteria.

## Residual risk

The CodeQL Rust extractor still has the known macro-expansion warning limitation. The local wrapper passed with explicit extraction errors at 0 and scanned 147 of 147 Rust files, so this is accepted as a tooling limit for this ticket.

REST, SQL, Arrow IPC declarative file preview, and multi-file scan semantics remain out of scope and are not closed by this review.
