Status: recorded
Created: 2026-07-11
Updated: 2026-07-11
Target: crates/cdf-format-arrow-ipc, crates/cdf-source-files/src/runtime.rs
Verdict: pass

# Adversarial review: native Arrow IPC file driver milestone

## Assumptions tested

- The driver cannot depend on filesystem, HTTP, object-store, project, CLI, or source-driver types.
- Discovery must be bounded before requesting footer bytes.
- Arrow arrays must not outlive memory accounting for their source buffers.
- Remote execution must preserve generation verification and bounded spool policy.
- Production routing must not add another Arrow-specific execution branch.

## Findings

No critical or significant milestone defect was found. The codec depends only on neutral runtime/kernel/contract/memory contracts plus Arrow parser crates. Footer and block arithmetic is checked, extents are constrained to the pre-footer data region, host endianness and schema identity are validated, and cancellation crosses every byte-source read. `Bytes::from_owner(AccountedBytes)` binds lease lifetime to the Arrow buffer. Local and remote-spooled inputs invoke the same `stream_registered_format` routine.

The benchmark is highly favorable because it measures zero-copy batch construction rather than full value consumption; the evidence explicitly labels that bias and does not promote it to the storage envelope. The direct-format schema-attestation match tree and legacy CSV/JSON/NDJSON fallback remain architectural debt, but are pre-existing explicitly owned FX1 scope rather than reasons to reject this bounded milestone.

## Verdict

Pass for committing the Arrow IPC file-driver milestone. Do not close B3 or FX1 until stream framing, malformed/fuzz and storage evidence, generic attestation, and remaining fallback deletion are complete.

## Residual risk

Dictionary-heavy and compressed IPC files need broader adversarial fixtures. Projection currently preserves requested-column order via Arrow's `FileDecoder`; nested dictionary interactions are not yet fuzzed. Both remain inside open B3 acceptance criteria.
