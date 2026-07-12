Status: active
Created: 2026-07-12
Updated: 2026-07-12

# Object-store security pin

## Context

CDF's cloud transports directly used `object_store 0.13.2`, whose cloud feature graph resolved `quick-xml 0.39.4`. RUSTSEC-2026-0195 reports an unbounded namespace-declaration allocation in `quick-xml::NsReader`; the fixed line is `quick-xml >= 0.41.0`. The latest crates.io `object_store 0.14.0` still requires the vulnerable XML line. Apache's upstream commit `c7316d29face118e7409eead0cda098f38589428` identifies `object_store 0.14.1` and requires `quick-xml 0.41.0`.

The pinned DataFusion Arrow-59 revision still declares featureless `object_store 0.13.2`. Apache DataFusion has not yet published or merged a 0.14 object-store update. Forking DataFusion or vendoring either dependency would make CDF own an avoidable compatibility surface.

## Decision

All CDF-owned object-store consumers use Apache `arrow-rs-object-store` at exact revision `c7316d29face118e7409eead0cda098f38589428`. CDF's direct Reqwest consumer uses the same 0.13 line selected by that transport graph. Cargo Deny allows this exact upstream repository class; Cargo Vet treats the pinned package as its crates.io identity and records the graph under the repository's existing exemption policy.

DataFusion's featureless internal 0.13.2 edge may remain temporarily because it does not enable or resolve the vulnerable `quick-xml` package and no CDF cloud transport accepts or returns that type. It MUST be removed when Apache DataFusion advances its object-store dependency; CDF MUST NOT add another first-party 0.13 declaration or bridge object-store values between the two versions.

## Alternatives considered

- Fork DataFusion solely to change its object-store requirement. Rejected: it creates a permanent CDF-maintained engine fork for an upstream dependency transition.
- Vendor `object_store 0.13.2` or `quick-xml 0.39.4` and backport the security fix. Rejected: it leaves CDF responsible for superseded parser/transport code.
- Ignore or allow the advisory because CDF does not parse arbitrary XML directly. Rejected: S3, GCS, and Azure responses are network-controlled XML inputs on production paths.
- Wait for crates.io/upstream publication. Rejected: the active production graph is known vulnerable now.

## Consequences

The cloud transport graph uses `quick-xml 0.41.0`, Reqwest 0.13, and the upstream AWS-LC-backed TLS default. Provider constructors for S3, GCS, and Azure remain behind the shared `FileTransport` facade. The temporary duplicate featureless object-store package is visible build debt but not a second CDF transport implementation; the DataFusion tuple review owns its removal.

