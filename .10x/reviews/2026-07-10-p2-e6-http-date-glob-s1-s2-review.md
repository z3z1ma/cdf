Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/tickets/done/2026-07-10-p2-ws-e6-http-date-glob-and-s1-s2-conformance.md
Verdict: pass

# P2 E6 HTTP date glob and S1/S2 review

## Findings

- Architecture: pass. Candidate generation does not perform I/O, and existence is represented by a transport trait operation rather than HTTP status parsing in the file runtime. New source transports can preserve their own existence semantics.
- Error integrity: pass. Only 404 is absent; 401/403, 408/5xx, 429, malformed metadata, and range failures remain typed failures. Zero candidates uses the ordinary no-match error.
- Determinism: pass. Month expansion is fixed 01–12 and sorted file identities drive discovery, preview, package, and FileManifest state. Scheduling and response order do not alter identity.
- Scope: pass. The heuristic accepts only one wildcard immediately following a four-digit year plus separator. Arbitrary HTTP wildcards, index scraping, and general guessing remain rejected.
- Live evidence: no implementation finding. CloudFront advertised ranges on cached HEAD but returned 403 to independent CDF and curl GETs. Treating that 403 as absence or falling back silently would weaken security/error truth, so the run correctly failed.

## Verdict

Pass. No unresolved critical, high, or significant findings remain.

## Residual risk

Some datasets use quarters, days, or non-numeric partition names. Those require explicit finite templates or future typed enumerators; this ticket intentionally does not guess them.
