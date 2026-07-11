Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Relates-To: .10x/decisions/destination-staged-ingress-final-package-binding.md, .10x/specs/streaming-destination-ingress.md, .10x/tickets/done/2026-07-11-p3-a1-staged-ingress-final-binding.md

# Staged ingress contract shaping evidence

## What was observed

Engine output is written per accepted batch, but project replay preloads all commit segments and destination helpers materialize whole packages/row vectors. More importantly, current destination sessions require the final package hash/token before `begin`, while that identity is unavailable until outcome-dependent package evidence is complete.

## Procedure

Traced engine execution, package builder/reader, project orchestration/replay, kernel destination request/session types, and DuckDB/Postgres/Parquet package/session implementations. Reconciled these with active package circularity, receipt/idempotency, crash-matrix, P3 streaming, and extension-boundary records.

## What this supports

The observation supports an explicit non-commit staged-ingress state keyed by non-identity attempt authority, followed by exact final binding to the verified package hash/token. It rejects provisional package hashes and destination-local token hacks.

## Limits

This is shaping evidence. A1 must prove type separation/artifact invariance; later destination children must prove actual invisibility, recovery mode, bulk throughput, and crash behavior.
