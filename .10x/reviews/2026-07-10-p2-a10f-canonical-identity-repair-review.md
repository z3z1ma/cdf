Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/reviews/2026-07-10-p2-a10f-independent-adversarial-review.md
Verdict: pass

# A10f canonical identity repair review

## Target

This review independently re-evaluates the sole blocking finding in `.10x/reviews/2026-07-10-p2-a10f-independent-adversarial-review.md`: discovery and preview previously supplied different byte encodings for the same logical bounded identity while both claimed `stratified-hash-v1`. No implementation was modified during this review.

## Finding resolution

### Resolved — significant: discovery and preview now share canonical identity authority

`cdf-kernel` now owns the typed `StratifiedHashBoundedIdentity`, its identity-strength vocabulary, and `canonical_bytes()`. `canonical_bytes()` converts the typed identity to JSON, recursively sorts object keys, and emits compact bytes. `StratifiedHashCandidate::from_bounded_identity` is the single typed bridge into the selector's opaque byte input.

Discovery aliases the kernel identity and strength types directly; its previous private canonical-byte helper is gone. Preview constructs the same kernel identity and calls `from_bounded_identity`. The production preview selector and the test-only candidate adapter therefore execute the same canonicalization rather than maintaining parallel serialization.

For the exact historical golden candidate:

```text
resource: events.raw
location: s3://acme/events/2026/01.parquet
identity: size=42, modified=null, value=etag-value, strength=stable_etag
```

both adapters now emit exactly:

```text
{"modified_at_ms":null,"size_bytes":42,"strength":"stable_etag","value":"etag-value"}
```

Both score it as:

```text
1caaf43016737e252f12a8b0568d67951ecfd702010906cc8a7eaddb7b1caa27
```

The three-candidate cross-adapter regression compares discovery and preview `(canonical_location, score)` vectors and proves identical two-member selection: January and March. The original discovery golden remains unchanged.

## Exact checks

The following independent commands passed:

```text
cargo test -p cdf-project discovery_and_preview_adapters_share_canonical_identity_score_and_membership -- --nocapture
# 1/1

cargo test -p cdf-project stratified_hash_selector_score_has_canonical_golden_bytes -- --nocapture
# 1/1

cargo test -p cdf-engine --lib
# 50/50, including all seven preview policy tests

cargo test -p cdf-kernel stratified_selection -- --nocapture
# 3/3

cargo clippy -p cdf-kernel -p cdf-engine -p cdf-project -p cdf-cli -p cdf-conformance --all-targets -- -D warnings
cargo fmt --check
git diff --check
# pass
```

The recorded integration evidence in `.10x/evidence/2026-07-10-p2-a10f-preview-balanced-stratified-policy.md` also accurately reports post-repair full suites: CLI 253/253, conformance 83/83, engine 50/50, kernel 22/22, and project 160/160 at that integration boundary. This reviewer independently observed the same CLI, conformance, kernel, and project integration passes while completing the adjacent Parquet seam review; subsequent unrelated RP9D additions raised the current project suite count and were separately reported green by their owning lane.

## Adversarial checks

- The repair did not merely update preview's field order: both adapters consume the same public typed identity and the same kernel `canonical_bytes()` method.
- The historical score is asserted independently and in the cross-adapter test; a change to key order, enum spelling, compactness, length-prefix domain, or value mapping breaks the regression.
- Membership, not only bytes and one score, is compared across adapters for `K=2,N=3`.
- Production preview selection calls `StratifiedHashCandidate::from_bounded_identity`, the same constructor used by the exposed preview-candidate adapter.
- Kernel permutation/edge/large-membership tests and all engine preview budget/quota/byte/evidence tests remain green.

## Verdict

Pass. The blocking canonical-identity seam is resolved without changing the historical selector golden. The original fail review remains accurate history for the pre-repair implementation; this review supersedes its open finding.

## Limits

- This repair proves equality when discovery and preview hold the same logical bounded identity. It does not require their candidate sets to be identical when planning occurs at different lifecycle stages and stronger transport identity becomes available later.
- Remote cloud/public-network execution remains outside A10f and under WS-E/WS-I.
- Concurrent/distributed preview execution is not implemented; the repaired canonical identity, precomputed membership, and serialized quotas preserve the deterministic extension point reviewed previously.
