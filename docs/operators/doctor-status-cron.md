# Doctor and Status in Cron

`cdf doctor` checks environment health. `cdf status` evaluates freshness SLOs and
returns a nonzero exit code on serving-resource breach when configured data
supports that evaluation.

## Doctor

```bash
cdf --project /path/to/project doctor
```

Current checks include project parsing, compiled resources, secret reference
resolution, Python interpreter health where relevant, destination runtime
support, DuckDB ICU status, and ledger/destination drift where applicable.

Use JSON mode for scheduler logs:

```bash
cdf --project /path/to/project --json doctor
```

## Status

```bash
cdf --project /path/to/project status
```

Use JSON mode for cron or other schedulers:

```bash
cdf --project /path/to/project --json status
```

Example crontab entry:

```cron
*/15 * * * * cd /path/to/cdf-checkout && target/debug/cdf --project /path/to/project --json status >> /var/log/cdf-status.jsonl 2>&1
```

Prefer absolute paths in cron because scheduler environments usually have a
small `PATH` and no shell profile.

## Exit Handling

- Treat `doctor` exit code `0` as "no failed checks"; unsupported checks can
  still be reported in the output.
- Treat `status` nonzero as a freshness breach or evaluation failure requiring
  operator attention.
- Do not parse human text for automation when `--json` is available.

## Related Contracts

- [Project CLI, observability, and security](../../.10x/specs/project-cli-observability-security.md)
- [Status freshness evidence](../../.10x/evidence/2026-07-06-status-freshness-local-ledger.md)
- [Doctor evidence](../../.10x/evidence/2026-07-06-doctor-secrets-duckdb-icu-health.md)
