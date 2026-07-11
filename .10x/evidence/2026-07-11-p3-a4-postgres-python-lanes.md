Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/tickets/done/2026-07-11-p3-a4-injected-execution-host.md

# Postgres and Python use adapter-owned blocking lanes

## What was observed

Registry-resolved Postgres destinations attach execution services to their protocol implementation. Ordinary commit sessions wrap their owned state so migration, segment write, finalize, and abort each execute on `postgres.sync`; transaction/client state is returned to the caller between joined operations.

Managed Python resources inspect the attached interpreter and resource parallel declaration, register either a serialized GIL lane or a free-threaded lane bounded by host CPU slots, and execute the Python callable/capsule conversion on that lane. The adapter owns the declaration and selection; CLI/project orchestration only passes neutral services.

## Procedure

- live CLI Postgres destination secret-resolution/commit/checkpoint test — passed
- Python plan/preview/run/replay product-spine test — passed
- checks for Postgres, Python, and CLI targets — passed
- strict Clippy across runtime/Postgres/Python/CLI targets — passed

## What this supports

All current production object-store and blocking/FFI adapter classes now execute through host-owned I/O or declared blocking lanes. Adding another adapter requires capability declaration and service attachment, not scheduler edits.

## Limits

Postgres still uses CSV COPY and Python still materializes the current bridge read; D3/H2 own the measured bulk and incremental boundary improvements.
