Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p0-sx1-source-extension-boundary.md

# File source ownership isolation

## What was observed

File format/compression declarations, local/HTTP/object-store transports, ranged and sequential readers, glob partition resolution, content identity, format/compression confirmation, attestation, spool handling, and file execution now live in `cdf-source-files`.

The source crate depends only on neutral kernel/runtime/memory-adjacent format/HTTP/object-store libraries. `FileResource` no longer owns or exposes a declarative compiler object; construction takes a descriptor, schema, resource/execution capabilities, type policy, effective schema runtime, physical plan, and transport dependencies.

## Procedure

- `cargo test -p cdf-source-files --lib` — passed.
- declarative/project/CLI all-target checks — passed.
- strict Clippy across file source, declarative, project, and CLI targets — passed.

## What this supports

File/object-store breadth and P3 decode/parallel/ranged-I/O optimization can evolve within a source adapter without recompiling destination implementations or leaking file branches into the scheduler.

## Limits

The file driver/options artifact and registry product resolution remain open. Declarative still builds the compatibility physical plan, and generic discovery/doctor hooks still inspect file plan variants.
