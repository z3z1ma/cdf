Status: recorded
Created: 2026-07-10
Updated: 2026-07-10
Target: .10x/specs/sampled-schema-discovery-coverage.md, .10x/specs/residual-variant-capture.md, .10x/specs/schema-promotion-corrections.md, .10x/tickets/done/2026-07-10-p2-residual-schema-promotion-program.md
Verdict: pass

# P2 sampled discovery, residual capture, and promotion specification review

## Target

Adversarial review of the exact contracts ratified by the user on 2026-07-10 and their executable ticket decomposition.

## Assumptions tested

- Sampling is allowed without weakening pin truthfulness or runtime validation.
- Residual capture can preserve part of a row without turning contract failures into silent nulls.
- JSON can retain exact Arrow values when it is a typed canonical envelope rather than generic stringification.
- Keyless append rows can be corrected without inferred business keys.
- Destination correction and lock publication can survive crashes without distributed 2PC.
- A local promotion lease does not force CLI/filesystem semantics into the future distributed model.
- Retention can stay finite without promising promotion from deleted bytes.

## Findings

- Pass: `stratified-hash-v1` is fully deterministic, handles `K=0/1/2/many`, serializes candidate/stratum evidence, and separates selection from budgets. An executor cannot silently substitute or shrink the sample.
- Pass: sampled evidence never invents a schema hash or verdict for an unprobed file. Runtime still reconciles every processed partition, so sampling changes knowledge rather than correctness.
- Pass: selected incompatibility fails the initial sampled pin instead of granting arbitrary first-file authority. Unseen runtime incompatibility has explicit residual/quarantine routes.
- Pass: residual capture has a closed safety boundary. Control-critical and unisolatable failures quarantine; only safe nullable fields become typed null plus exact residual. This prevents convenience from weakening cursor/key/required-field semantics.
- Pass: `residual-json-v1` defines precision-safe encodings for integer/decimal/binary/temporal/non-finite/nested/map values and a named quarantine outcome for unsupported exact encoding.
- Pass: the correction address reuses source-observed current architecture: Postgres already persists package token, segment id, and segment-local row ordinal. The specs elevate that tuple into a kernel concept instead of adding an unrelated UUID or semantic key.
- Pass: destination strategies are explicit sheet claims and conformance targets. Parquet sidecars do not pretend to mutate base objects; unsupported readback/rematerialization cannot be inferred.
- Pass after repair: the first draft called a lock-advanced crash complete even if the promotion event was absent. The spec now requires an idempotent publication event keyed by promotion-plan id and a recovery branch that repairs that event without touching destinations.
- Pass: the lease model is scoped and executor-neutral. RP4 coordinates with, rather than duplicates, the future distributed scheduler ticket.
- Pass: GC removes no existing finite-retention guarantee. It reports loss of the last local promotable bytes and promotion refuses tombstone-only evidence.

## Verdict

Pass. The records are sufficiently explicit for cold-start child execution without inventing sample selection, residual exactness, row identity, destination mutation, lease, publication, crash recovery, or retention semantics.

## Residual risk

- Destination readback remains unsupported until a sheet and conformance probe prove it; the program must rely on retained packages meanwhile and report that limit.
- Filesystem rename/fsync guarantees vary by platform. RP4 must record platform limits and conformance behavior rather than claim universal storage atomicity.
- Remote fenced lease stores and worker scheduling remain outside this P2 program; the kernel primitive must be reused by the existing distributed execution ticket.
