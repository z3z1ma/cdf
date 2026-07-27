Status: active
Created: 2026-07-27
Updated: 2026-07-27

# Error ownership taxonomy

## Purpose

Error kind names the actor that can repair the primary failure. It is not a synonym for exit code,
retryability, or the layer that happened to observe the error.

## Ownership rules

- `Environment` means the executing host or process failed to provide a required facility:
  current directory, temporary storage, permissions, file descriptors, device health, physical
  free space, memory, executable availability, or another local OS resource.
- `Data` means external source, package, task, or replay evidence is missing, truncated,
  malformed, non-canonical, or has the wrong filesystem shape.
- `Contract` means the caller supplied invalid configuration, arguments, bounds, or an unsupported
  request.
- `Internal` means a CDF-owned invariant failed: poisoned state after an unexpected partial
  mutation, impossible authority/state, counter overflow, serialization logic, or corrupted
  private scratch that CDF created and exclusively owns.

A configured memory or spill ceiling is governed input and therefore `Data` or `Contract` as its
owning API specifies. Physical `ENOMEM`, `ENOSPC`, permissions, descriptor exhaustion, and device
failures are `Environment`.

## Read and write boundaries

For external artifacts and sources, missing files, premature EOF, invalid data, directory/file
shape mismatches, and symlink loops are `Data`; host permissions and device/resource failures are
`Environment`.

For private CDF scratch, missing/truncated/invalid/wrong-shape content after successful creation is
`Internal`; host permissions, device/resource failures, and physical exhaustion remain
`Environment`.

Decoder and writer adapters MUST walk error source chains. An embedded typed `CdfError` keeps its
kind, retry delay, and primary message while the caller adds context. A codec/shape failure with no
host source is `Data`; an underlying host I/O source is classified by the rules above. Never
flatten a typed source into a string and reconstruct a new kind.

## Journal-free SQLite scratch

When `journal_mode=OFF`, retrying a failed statement is unsafe because statement rollback is not
guaranteed. A scratch index must:

1. reserve the complete configured spill capacity before mutation;
2. raise `max_page_count` before the statement;
3. execute once;
4. poison on every unexpected mutation failure;
5. classify `SQLITE_FULL` as `Internal` only when the observed page count proves the admitted page
   ceiling was reached; otherwise treat physical device exhaustion as `Environment`.

## CLI boundary

`Environment` keeps the stable exit code 70 and maps to `CDF-ENV-HOST`. Human, headless, and JSON
errors must carry the same kind, code, facts, and host-specific remediation.

Redaction is a construction and render-boundary invariant. Apply it recursively to messages,
details, suggestions, remediation, URI userinfo, and sensitive-key values, including private-key
labels. A stable exit code shared by `Environment` and `Internal` does not make the kinds
interchangeable.
