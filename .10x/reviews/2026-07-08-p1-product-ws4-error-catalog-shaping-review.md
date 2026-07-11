Status: recorded
Created: 2026-07-08
Updated: 2026-07-08
Target: .10x/tickets/2026-07-08-p1-product-ws4-error-experience-catalog.md
Verdict: pass

# P1 WS4 error catalog shaping review

## Target

The WS4 shaping records:

- `.10x/specs/cli-error-experience-catalog.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4a-error-envelope-foundation.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4b-error-construction-site-migration.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4c-error-suggestions.md`
- `.10x/tickets/done/2026-07-08-p1-product-ws4d-error-rendering-docs.md`
- `.10x/evidence/2026-07-08-p1-product-ws4-error-catalog-shaping.md`

## Findings

No blocking findings.

The split preserves JSON compatibility by placing additive envelope fields in WS4A before construction-site migration. It avoids coupling suggestions to the intermediate parser state by making WS4C depend on WS2C. It also keeps final human presentation and generated docs behind WS3B and WS6B, which matches the P1 sequencing constraints.

The spec keeps the existing exit-code taxonomy intact and treats command-specific nonzero statuses as explicit exceptions rather than accidental failures. That prevents error-catalog work from changing command behavior while still allowing stable codes and remediation.

## Residual risk

The exact catalog size may be large because `cdf-cli` has many construction sites. WS4B requires an inventory before/after and permits documented generic lower-layer mappings to prevent overfitting one code per source line.

## Verdict

Pass. The child tickets are bounded and executable, and unresolved implementation dependencies are modeled as ticket dependencies rather than hidden assumptions.
