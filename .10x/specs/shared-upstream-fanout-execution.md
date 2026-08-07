Status: draft
Created: 2026-08-07
Updated: 2026-08-07

# Shared-upstream fan-out execution

## Purpose

Define the project-level execution graph that reads one compatible physical upstream once and
fans its accounted batches into several selected resource graphs without merging resource,
package, destination, receipt, or checkpoint identities.

## Physical upstream signature

The compiler MUST derive a versioned `PhysicalUpstreamSignature` that excludes canonical resource
id, logical target, disposition, branch transforms/contracts, and destination. It includes every
fact whose difference can change physical source bytes or restart semantics:

- source kind/version and configured-source effective semantic identity;
- canonical upstream relation arguments and source object/catalog identity;
- input schema/generation authority;
- snapshot/CDC mode, bootstrap/start scope, and exact initial frontier;
- source consistency/session semantics;
- source-owned partition and ordering requirements;
- a hash of the compiler-produced shared extraction plan.

Configured-source name equality alone is insufficient. Existing per-resource source-plan hashes
remain resource execution authority and MUST NOT be reused as this signature.

## Compatible-group compilation

For every selected resource set, the compiler groups only resources for which one extraction plan
is provably equivalent to independent reads:

- source projection is the ordered union of branch-required input fields;
- only predicates common and exact for every branch may be hoisted; all other predicates remain
  branch-local residuals;
- resource-local limits, sampling, ordering, cursor transforms, contracts, and SQL transforms stay
  after fan-out unless exact common hoisting is proven;
- initial source frontier, bootstrap, consistency, schema generation, and source transaction scope
  must agree;
- every branch receives its exact compiled input schema and canonical order;
- running a branch alone or in any compatible group yields identical accepted rows, package bytes,
  evidence, destination effects, and checkpoint position.

When equivalence cannot be proven, the resources receive separate source nodes. Sharing is an
optimization with identity-bearing proof, never a heuristic based on text or source name.

## Project execution graph

One project-level graph owns:

```text
physical source/open/read/decode
  -> accounted bounded fan-out
      -> resource A operator/package/destination/checkpoint graph
      -> resource B operator/package/destination/checkpoint graph
      -> ...
  -> shared-frontier settlement barrier
```

The existing per-resource compiled operator graph remains the branch authority after its source
edge. The project graph records group membership, shared signature, extraction-plan hash, branch
graph hashes, edge capacities, and settlement barrier. Branch membership and ordering are
canonical and independent of CLI selector order.

Fan-out shares retained Arrow payloads and memory leases; it does not deep-clone decoded batches.
Every branch has a bounded queue, and backpressure propagates to the source. A slow branch cannot
cause unbounded buffering; an implementation may use the existing accounted spill/replay boundary
when required by declared source pause capability.

## Settlement and recovery

Each branch writes its own canonical package, destination receipt, and resource checkpoint. The
shared source frontier/external acknowledgement advances only after every selected branch covering
that frontier has a verified receipt and committed checkpoint.

Settlement MAY commit recoverable branch receipts/checkpoints one at a time because arbitrary
destinations cannot share a database transaction. The group remains incomplete until all settle.
On crash or failure:

- already finalized branch packages and receipts are recovered idempotently;
- an already committed branch is not rolled back and is not extracted again;
- the shared frontier acknowledgement remains at the prior settled group frontier;
- the failed branch settles from its durable package when possible;
- a later selection whose branch frontiers differ forms separate extraction groups until exact
  compatibility is restored.

Package collection cannot tombstone a group member needed to recover an incomplete settlement.

## Failure and cancellation

The first extraction or branch-construction failure cancels further source admission. Branches
that already reached a durable package may settle only under the recovery contract above; no group
success or shared frontier advance is reported until all selected branches settle.

For continuous CDC, first interruption requests closure at the next source-safe frontier and then
settles every branch. A second interruption aborts unfinished branch construction/settlement,
leaves the prior shared frontier authoritative, and preserves any durable recovery artifacts.

## Acceptance scenarios

1. Two resources with one physical signature open/read/decode the source once and produce the same
   branch packages as independent runs.
2. Different projections share their union at extraction while each branch receives only its
   compiled input; different filters/limits remain branch-local unless common hoisting is proven.
3. Different bootstrap/frontier/schema-generation/consistency authority prevents sharing.
4. One slow branch backpressures within declared memory/disk bounds without duplicating payloads.
5. One destination failure leaves the group frontier unchanged; restart settles durable branch
   packages without re-extraction or double application.
6. Selector order and jobs do not change group identity, branch package bytes, or checkpoint
   positions.
7. A shared CDC transaction boundary is acknowledged upstream only after every branch receipt and
   checkpoint covers it.

## Open blockers

1. Ratify the all-branches settlement barrier and recovery behavior above.
2. Ratify that sharing requires identical start/frontier authority; divergent branches execute
   separately rather than reading from the oldest frontier and filtering positions.
3. Ratify that projection union is the only initial widening optimization; branch-specific
   predicates, limits, and ordering are not hoisted without an exact common proof.

## References

- `.10x/research/2026-08-07-routed-target-shared-extraction-readiness.md`
- `.10x/specs/streaming-operator-graph.md`
- `.10x/decisions/compiled-fused-streaming-operator-graph.md`
- `.10x/specs/checkpoint-state-commit-gate.md`
- `.10x/knowledge/source-destination-extension-invariant.md`
