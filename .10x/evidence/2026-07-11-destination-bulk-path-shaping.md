Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/schema-planned-destination-bulk-paths.md, .10x/specs/destination-bulk-path-runtime.md, .10x/tickets/done/2026-07-10-p3-ws-d-destination-bulk-paths.md

# Destination bulk-path shaping evidence

## What was observed

Kernel commit segments carry batch vectors; replay collects all segments. DuckDB scalarizes Arrow to row vectors; Postgres scalarizes to string rows and CSV COPY despite a binary declaration; Parquet retains every segment/batch until finalize. No shared descriptor governs bulk schema eligibility, fallback, memory, staging, lane, tuning, or evidence.

## Procedure

Traced sheets, package readers, commit sessions, row conversion, COPY/appender/writer paths, staging/finalization, and existing destination extension contracts.

## What this supports

Neutral schema-planned bulk descriptors, bounded segment/batch writers, driver-owned exact fallback, nonidentity tuning evidence, and focused native first-party paths.

## Limits

This is shaping evidence. D2-D4 must select/measure concrete native mechanisms and falsify declarations live.
