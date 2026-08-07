Status: done
Created: 2026-08-07
Updated: 2026-08-07

# Routed target families and shared-extraction readiness

## Question

What current identities and orchestration seams must change so one logical resource can commit to
multiple physical destination tables, and multiple selected resources with one physical upstream
can share one extraction?

## Sources and methods

Inspected the active project identity, SQL authoring, operator-graph, package, destination,
receipt/checkpoint, and source-extension authorities together with current `main` at `2bcff9f3`.
Traced query compilation through `ProjectQueryCompilation::source_node_id`, CLI multi-resource run
preparation/execution, `ProjectRunRequest`, the compiled per-resource operator graph,
`DestinationCommitRequest`, destination sheets, package tombstoning, and state-backed checkpoint
settlement. No external system was contacted and no product code was changed.

## Findings

1. Query compilation hashes the canonical resource id into `source_node_id` alongside configured
   source and upstream arguments. Two resources cannot currently share a source node even when
   their physical upstream relation is identical.
2. `cdf run` prepares every selected resource, commits schema authorities together, then invokes
   `execute_prepared` serially. Each invocation owns one `ProjectRunRequest`, source, package,
   destination binding, receipt, and checkpoint. Selection is not compiled into a project-level
   execution graph.
3. The existing operator graph is deliberately one resource transition with exactly one source
   node, one destination-binding node, and one commit gate. Its typed/accounted edges and durable
   package boundary are reusable, but it cannot represent a shared source node feeding multiple
   resource subgraphs without a project-level graph above it.
4. A compiled source plan contains resource-specific descriptor and pushdown decisions. Its hash is
   therefore not the correct shared-read identity. Sharing needs a separate physical upstream
   signature plus a compiler-produced extraction plan that proves one read preserves every branch.
5. Projection union is safe only when each branch later receives its exact compiled input.
   Resource-specific predicates, limits, ordering, transforms, contracts, and destinations cannot
   silently become common source pushdown. Common pushdown is legal only when it is identical for
   all branches or when the source/compiler proves a broader read plus branch-local residuals is
   semantically equivalent.
6. Shared extraction cannot weaken settlement. Per-resource packages remain the replay and
   destination payload authorities. A shared source frontier or external source acknowledgement
   cannot advance until every selected branch covering that frontier has an accepted receipt and
   committed checkpoint. Already committed branches must recover idempotently rather than force a
   second extraction.
7. Destination APIs currently bind one logical target. One resource routing to many physical
   targets needs an identity-bearing route map, destination capability, multi-target commit plan,
   receipt coverage, and checkpoint gate. Treating each route as an implicit resource would lose
   the requested single-resource state and package semantics.
8. PostgreSQL and DuckDB can naturally use one database transaction for a package spanning several
   tables. Destinations that cannot truthfully provide package-atomic multi-target settlement must
   reject the plan rather than partially advance the resource.
9. Arbitrary routing values create identifier, collision, disclosure, and unbounded-cardinality
   risks. A current contract must define one deterministic fold, reject null/unsupported values,
   protect the route field as control data, and enforce a compiled hard maximum physical-target
   count.
10. Fan-out and routing compose cleanly as distinct graph operations: one physical extraction may
    feed several logical resources; each logical resource may then partition its package effects
    among a bounded target family. Resource identities, package identities, and checkpoints remain
    independent even when physical read work is shared.

## Conclusions

- Add a project-level compiled execution graph above the existing per-resource operator graphs.
- Define shared-read identity independently from resource identity and compile one exact
  extraction plan per compatible group.
- Preserve per-resource package bytes and semantics regardless of whether the resource runs alone
  or inside a shared extraction group.
- Add routed target families as one logical destination binding with one package-atomic receipt and
  checkpoint gate, not as generated resources.
- Resolve naming/cardinality/privacy and fan-out frontier/failure semantics before opening their
  executable tickets.

## Limits

- This was a static authority/readiness inspection, not a throughput experiment.
- No destination was exercised with a multi-table transaction.
- Exact SQL grammar, target-name folding, maximum route count, and source-group recovery artifacts
  remain shaping choices in the focused draft specifications.
