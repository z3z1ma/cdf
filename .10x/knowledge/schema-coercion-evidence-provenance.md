Status: active
Created: 2026-07-09
Updated: 2026-07-09

# Schema coercion evidence provenance

Schema coercion evidence is identity-bearing proof, not a hint. Package execution accepts it only when an internally reconciled batch carries the same valid plan in both the reserved Arrow schema metadata key `cdf:schema_coercion_plan` and `BatchHeader.schema_coercion_plan`.

The engine MUST fail closed when either channel is missing while the other claims evidence, when JSON is malformed, when the decoded plans differ, when batch plans disagree, or when field order, identities, observed/constraint types, decisions, outcomes, reasons, or extra-field ordering are inconsistent with the reconciled output schema.

Raw Arrow IPC, Parquet, or other source metadata is untrusted input. It MUST NOT become validation-program or package evidence without the internal matching channel. This boundary protects evidence from source-controlled metadata while keeping exact widening, policy coercion, lossy allowance, and extra-field decisions reproducible.

The batch-header field is serde-optional so older serialized headers remain readable. Absence means no coercion evidence, not permission to synthesize it heuristically.
