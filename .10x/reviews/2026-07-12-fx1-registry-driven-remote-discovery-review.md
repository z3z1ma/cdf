Status: recorded
Created: 2026-07-12
Updated: 2026-07-12
Target: crates/cdf-runtime/src/format.rs, crates/cdf-format-parquet, crates/cdf-source-files, crates/cdf-project/src/schema_discovery.rs, crates/cdf-declarative, crates/cdf-formats
Verdict: pass

# Registry-driven remote discovery review

## Findings

No critical or significant finding remains.

- Pass: Project and declarative layers contain no Parquet-name discovery branch or Parquet-specific probe API.
- Pass: The generic source runtime depends on existing `ByteSource`, transform registry, format registry, and neutral access capabilities; it does not import leaf drivers.
- Pass: Format evidence is driver-owned but cannot collide with source identity authority. Parquet footer evidence remains deterministic and is tested at the driver and project boundary.
- Pass: Bounded range discovery reserves memory before transport allocation and refuses weak identities. The fallback reattests generation around each range; final providers retain stronger request preconditions through their native byte sources.
- Pass: The old discovery module and tests were removed, not wrapped or deprecated.
- Pass: Full-scan execution is untouched; bounded footer ranges have not leaked back into ordinary remote Parquet runs.

## Residual risk

The range adapter's reattestation fallback cannot provide the same single-request precondition strength as a final native provider and exists only for transports not yet migrated to `open_byte_source`. G1 owns its deletion. Compressed adaptive discovery still waits for indexed/splittable transforms or growing-spool readers.

## Verdict

Pass. Discovery now uses one extension architecture and one Parquet implementation while preserving bounded remote metadata I/O.
