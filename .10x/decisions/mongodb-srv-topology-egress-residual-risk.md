Status: active
Created: 2026-08-07
Updated: 2026-08-07

# MongoDB `srv`/topology discovery ships with accepted egress residual risk

## Context

`.10x/tickets/done/2026-08-02-mongodb-source-connector.md` was `blocked` on a gap between the advertised
MongoDB connection surface and CDF's egress authority.

CDF authorizes source network egress through `SourceEgressAuthorizer`
(`crates/cdf-runtime/src/source.rs:424`), a per-URI check whose only production implementation
delegates to `EgressAllowlist`. `SourceEgressScope::authorize` is called with an operational URI
before a driver connects.

The official MongoDB driver does not fit that shape. Given `mongodb+srv://` — or any seed that
resolves to a replica set or sharded cluster — the driver performs DNS SRV resolution and ongoing
topology monitoring, then opens sockets to hosts it *learned* after the initial seed was authorized.
The driver exposes no pre-connect hook for those learned hosts, so they never reach
`SourceEgressAuthorizer`.

The MongoDB source spec advertises `mongodb+srv` and topology discovery. The connector cannot close
while the advertised surface and the enforced authority disagree.

## Decision

Ship `mongodb+srv` and topology discovery as advertised, and explicitly accept that driver-learned
hosts bypass `SourceEgressAuthorizer`.

The allowlist remains authoritative for the seed URI and advisory for learned hosts. This is
recorded as accepted residual risk, not as a closed gap.

The MongoDB source ticket is unblocked by this decision. A4 MongoDB change-stream CDC, which extends
the same `cdf-source-mongodb` crate, inherits the same posture.

## Alternatives considered

**Narrow the release to direct `mongodb://host[:port]` and reject `srv`.** Steelmanned: this
preserves the egress fence completely and is cheap — a config-validation rejection with a typed
error. It was the recommended option. Rejected because it removes replica-set and sharded-cluster
auto-discovery, which is the normal deployment topology for MongoDB; requiring operators to enumerate
hosts explicitly makes the connector materially worse for its actual users, and host lists drift as
clusters are reconfigured. Stays rejected unless the threat model changes to include hostile DNS.

**Shape an egress-aware transport with a pre-connect hook for learned hosts.** Steelmanned: this is
the only option that both keeps `srv` and closes the gap, and the resulting seam would serve every
future driver with its own topology discovery. Rejected for sequencing, not merit: it is a net-new
transport design that would block the finite MongoDB source, the MongoDB destination, and A4 behind
it. Stays rejected until either a concrete threat justifies the cost or another driver needs the
same seam, at which point this decision should be revisited rather than extended.

## Consequences

- The finite MongoDB source may close, and A4 MongoDB CDC may proceed on the same crate.
- `EgressAllowlist` no longer describes the complete set of hosts a MongoDB run may contact. Any
  documentation, security review, or operator-facing claim that the allowlist bounds egress MUST
  exclude MongoDB, or it is false.
- The exposure is: an attacker who controls DNS responses for the configured SRV domain, or who can
  reconfigure the cluster's advertised topology, can cause CDF to open connections to hosts the
  operator never approved. This is a data-path redirection and exfiltration concern, not a
  code-execution one.
- The MongoDB SRV specification is understood to require that hosts returned by SRV lookup share the
  seed's parent domain, which would bound the exposure considerably. This is **not verified** and is
  recorded here as a hypothesis to confirm during A4 protocol research, not as a mitigation being
  relied upon today. If confirmed, it should be recorded as evidence and this decision annotated.
- Revisit if: CDF gains a multi-tenant or hostile-operator threat model; a second driver needs
  topology-aware egress; or the MongoDB driver adds a pre-connect callback.

## References

- `.10x/tickets/done/2026-08-02-mongodb-source-connector.md`
- `.10x/tickets/2026-08-03-cdc-semantic-sql-project-foundation-program.md`
- `crates/cdf-runtime/src/source.rs`
