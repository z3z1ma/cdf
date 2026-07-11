Status: active
Created: 2026-07-11
Updated: 2026-07-11

# Canonical segmentation plan-artifact gate

## Context

Before A3, segment boundaries and ids were hidden execution behavior derived from source batch arrival. Plans could not explain or pin the rule, so equivalent source rechunking changed package identity. Canonical segmentation must become explicit identity-participating plan data before the writer changes bytes.

## Decision

Every newly compiled package-sink operator records the complete validated `canonical-segmentation-v1` policy. The policy is duplicated in explain through the existing operator-chain representation and therefore participates in package plan evidence. Execution must read the policy from the plan; a missing package sink or multiple policies fails closed.

Legacy serialized plans whose package sink lacks the field deserialize to `canonical-segmentation-v1` for structural readability, but package replay continues to consume already-recorded segment files and never resegments them. New package hashes intentionally change once because plan evidence gains the policy; golden updates must cite A3 evidence. Existing package manifests remain readable under their recorded bytes.

## Alternatives considered

- Keep policy as a hidden engine constant: rejected because plans would not pin artifact semantics.
- Put adaptive runtime targets in the plan: rejected because pressure is non-deterministic and not canonical segmentation.
- Rewrite existing packages: rejected because content-addressed evidence is immutable.
- Add a second independent plan file: rejected because the package-sink operator already owns this execution contract.

## Consequences

The writer migration can be reviewed separately from policy serialization. Legacy plan parsing remains compatible, while new plans and package hashes explicitly bind the canonical rule.
