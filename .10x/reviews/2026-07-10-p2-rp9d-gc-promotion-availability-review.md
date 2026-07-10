Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-rp9d-gc-promotion-availability.md
Verdict: pass

# RP9D truthful GC promotion availability implementer review

## Target

Adversarial review of the RP9D shared availability service in `crates/cdf-project/src/promotion.rs`, its integration in `crates/cdf-cli/src/package_command.rs`, and focused project/CLI fixtures.

## Assumptions tested

- A field named `_cdf_variant` is not authority without the exact framework semantic metadata and canonical codec.
- Raw non-null UTF-8 bytes are not promotable until canonical decode succeeds.
- Canonical bytes alone are not executable authority without exact recorded receipt/target association.
- Package verification and resource attribution precede every positive byte/promotion claim.
- “Last local copy” and “collection removes the last copy” are different facts.
- A retention action is input authority; availability reporting cannot turn `retain` into collection or infer destination readback.

## Findings

- Pass: CLI GC delegates package/resource/residual/receipt classification and last-copy assessment to public typed `cdf-project` APIs. No Arrow decoding or promotion-evidence policy remains in the CLI.
- Pass: The scanner is shared with RP5 inventory, streams package segments, rejects duplicate semantic residual columns, and uses strict `decode_residual_json_v1`. Malformed envelopes and noncanonical fields cannot contribute bytes or path authority.
- Pass: Receipt validation is exact across package, state, schema, commit target/disposition, idempotency token, and segment acknowledgements. Unreceipted canonical bytes remain visible as local bytes but have zero promotable bytes.
- Pass: Collection consequence is derived from the complete resource/action set. A collected artifact is not reported as removing the last authority while any promotable retained copy remains; all-collect and single-copy cases are covered.
- Pass: Existing GC classifications/actions are unchanged. Receipted packages remain protected, no destructive operation exists, and no destination readback claim is synthesized.
- Pass: JSON carries exact resource, package, artifact, byte, action, authority, association, final-risk, and remediation facts. Human rendering exposes the actionable subset and explicitly says destination readback was not inferred.

## Residual risk

Retention tombstones remove identity files, so resource attribution may no longer be available in the promotion assessment even though the GC artifact table still names and protects the tombstone. This does not create a false positive: the artifact is never locally promotable. A future tombstone format that preserves typed resource attribution could improve reporting, but changing tombstone retention layout is outside RP9D and is not required for truthful safety.

The current retention planner protects every receipted package, so command-level output cannot naturally exercise a true `collection_removes_last_local_promotable_copy` without changing retention. The shared action matrix covers that calculation directly; adding a destructive CLI fixture would violate the ticket exclusions.

## Verdict

Pass. The implementation reports promotion read availability rather than byte presence or GC reachability, preserves existing retention, fails closed on damaged/unratified authority, and meets the bounded RP9D acceptance criteria. Parent review and ticket closure remain separate.
