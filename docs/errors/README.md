# Error reference

Generated from the CLI error catalog. Do not edit this page by hand.

| Code | Area | Kind | Exit | Meaning | Remediation | Representative command |
|---|---|---|---:|---|---|---|
| `CDF-CLI-ARTIFACTS` | CLI | internal | 70 | artifacts | Regenerate or re-check the CLI artifacts with the repository artifact command. Run the artifact command shown in the error message. | `cdf help` |
| `CDF-CLI-ARTIFACTS-USAGE` | CLI | contract | 2 | artifacts usage | Point the artifact check at the generated CLI artifact directory. Run the artifact command shown in the error message when artifacts are stale. | `cdf help` |
| `CDF-CLI-JSON-SERIALIZATION` | CLI | internal | 70 | json serialization | Retry the command and report this internal serialization failure if it repeats. Include the command and error code in the report. | `cdf help` |
| `CDF-CLI-NOT-SUPPORTED` | CLI | contract | 78 | not supported | Use a currently supported path or wait for the named lower layer to land. Inspect the error message for the required lower layer. | `cdf help` |
| `CDF-CLI-USAGE` | CLI | contract | 2 | usage | Correct the command arguments and run the command again. Run `cdf help <command>` for the accepted syntax. | `cdf help` |
| `CDF-CONTRACT-ARGUMENT` | CONTRACT | contract | 2 | argument | Use a supported contract subcommand or trust policy. Run `cdf help contract` for accepted contract commands. | `cdf contract show` |
| `CDF-CONTRACT-LOCKFILE` | CONTRACT | contract | 3 | lockfile | Create or update the contract lockfile before testing contract drift. Run `cdf contract freeze` for the selected project. | `cdf contract show` |
| `CDF-DEST-ERROR` | DEST | destination | 6 | error | Inspect the destination URI, target, policy, and destination health. Run `cdf doctor` for the selected project and environment when available. | `cdf plan` |
| `CDF-DEST-NOT-SUPPORTED` | DEST | contract | 78 | not supported | Use a registered destination driver and URI shape for this command. Supported local paths include `duckdb://path` and `parquet://root` where applicable. | `cdf plan` |
| `CDF-DOCTOR-DRIFT` | DOCTOR | data | 5 | drift | Inspect the checkpoint ledger and destination drift inputs. Run `cdf doctor` again after repairing unreadable ledger or JSON values. | `cdf doctor` |
| `CDF-INTERNAL-UNEXPECTED` | INTERNAL | internal | 70 | unexpected | Retry with the same inputs and capture the command output if it repeats. Report the failure with the error code and the command that triggered it. | `cdf help` |
| `CDF-PACKAGE-ARGUMENT` | PACKAGE | contract | 2 | argument | Correct the package command arguments and retry. Run `cdf help package` for package command syntax. | `cdf package verify` |
| `CDF-PACKAGE-ARTIFACT` | PACKAGE | data | 5 | artifact | Inspect the package directory and referenced package artifacts. Run `cdf package verify <package>` for a package directory when available. | `cdf package verify` |
| `CDF-PACKAGE-DATA` | PACKAGE | data | 5 | data | Inspect the referenced data, package, or source artifact. Use the matching `cdf inspect` command when an artifact id or path is shown. | `cdf package verify` |
| `CDF-PACKAGE-REPLAY-ARGUMENT` | PACKAGE | contract | 2 | replay argument | Provide the replay package destination arguments required by the destination. For Postgres replay, pass `--target schema.table --merge-dedup fail`. | `cdf package verify` |
| `CDF-PACKAGE-REPLAY-CONTRACT` | PACKAGE | contract | 3 | replay contract | Make replay arguments match the package replay contract. Use the package destination target recorded at package creation time. | `cdf package verify` |
| `CDF-PROJECT-AUTH` | PROJECT | auth | 4 | auth | Check the configured secret reference or credential provider. Run `cdf validate` for the selected project and environment. | `cdf validate` |
| `CDF-PROJECT-CONTRACT` | PROJECT | contract | 3 | contract | Fix the project, command, schema, or contract input and retry. Run `cdf validate` when the failure references project configuration. | `cdf validate` |
| `CDF-PROJECT-INIT-ARGUMENT` | PROJECT | contract | 2 | init argument | Correct the init argument and run `cdf init` again. Use a non-empty `--name` value when naming the project. | `cdf validate` |
| `CDF-PROJECT-IO` | PROJECT | internal | 70 | io | Check the selected project path and filesystem access, then retry. Run the command from an accessible directory or pass `--project` explicitly. | `cdf validate` |
| `CDF-PROJECT-MERGE-KEY` | PROJECT | contract | 3 | merge key | Choose append or declare the merge identity before contacting the source or destination. Add `merge_key = [...]` when rows should merge by an explicit identity. Otherwise use `write_disposition = "append"` to append rows without a key. | `cdf validate` |
| `CDF-PROJECT-RESOURCE-MAPPING` | PROJECT | contract | 3 | resource mapping | Update the project resource mapping to a compiled resource id or source wildcard. Run `cdf inspect resources` after correcting the `[resources]` key. | `cdf validate` |
| `CDF-PYTHON-RESOURCE` | PYTHON | contract | 3 | resource | Repair the configured Python interpreter or resource target, then retry. Run `cdf doctor` to verify the configured interpreter without executing resource code. | `cdf help` |
| `CDF-RESOURCE-NOT-COMPILED` | RESOURCE | contract | 3 | not compiled | Use one of the compiled resource ids or repair the project resource mapping. Run `cdf inspect resources` to see compiled ids and their source files. If the id is expected, update the `[resources]` mapping to `<source>.<resource>` or `<source>.*`. | `cdf inspect resources` |
| `CDF-RUN-ARGUMENT` | RUN | contract | 2 | argument | Provide the required run resource and stable identifiers. Run `cdf help run` for accepted run arguments. | `cdf run` |
| `CDF-RUN-ARTIFACT-INTERNAL` | RUN | internal | 70 | artifact internal | Retry the run and report the artifact path failure if it repeats. Include the selected package id, checkpoint id, and project path in the report. | `cdf run` |
| `CDF-RUN-ARTIFACT-PATH` | RUN | data | 5 | artifact path | Use a writable package, checkpoint, or state path inside the selected project. Avoid path traversal in package ids and ensure parent directories are writable. | `cdf run` |
| `CDF-RUN-LOOP-NOT-SUPPORTED` | RUN | contract | 78 | loop not supported | Run one package at a time in this slice. Use `cdf run RESOURCE` without `--loop` until the loop supervisor lands. | `cdf run` |
| `CDF-RUN-PREVIEW-RUNTIME-NOT-SUPPORTED` | RUN | contract | 78 | preview runtime not supported | Preview only works when the resource runtime can open a direct stream. Use a file, REST, or SQL resource with an implemented preview stream. | `cdf run` |
| `CDF-RUN-RATE-LIMITED` | RUN | transient | 75 | rate limited | Retry the command after the rate limit window clears. Reduce request concurrency or wait for the upstream quota to recover. | `cdf run` |
| `CDF-RUN-SCAN-ARGUMENT` | RUN | contract | 2 | scan argument | Correct the scan, plan, explain, or preview argument and retry. Run `cdf help plan`, `cdf help explain`, or `cdf help preview` for syntax. | `cdf run` |
| `CDF-RUN-TRANSIENT` | RUN | transient | 75 | transient | Retry the command after the transient condition clears. If the error repeats, inspect the source, destination, and network health. | `cdf run` |
| `CDF-SQL-INTERNAL` | SQL | internal | 70 | internal | Retry the system-history SQL query and report the failure if it repeats. Include the query shape and error code, but do not include secret values. | `cdf sql` |
| `CDF-SQL-QUERY` | SQL | contract | 2 | query | Submit one read-only SELECT or WITH query against system history. Remove mutating SQL, extra statements, or unterminated strings/comments. | `cdf sql` |
| `CDF-SQL-RESULT` | SQL | data | 5 | result | Inspect the local system-history data that the SQL query reads. Run a narrower `cdf sql` query to isolate the unreadable row or value. | `cdf sql` |
| `CDF-STATE-RESUME-LEDGER` | STATE | data | 5 | resume ledger | Select an environment with a run ledger or provide an explicit resumable run. Run `cdf inspect run <id>` when a run id is known. | `cdf state show` |
| `CDF-STATE-RESUME-MULTI-RUN-NOT-SUPPORTED` | STATE | contract | 78 | resume multi run not supported | Resume one interrupted run explicitly. Pass `cdf resume RUN_ID` for the run you want to drain. | `cdf state show` |
| `CDF-STATE-SCOPE-ARGUMENT` | STATE | contract | 2 | scope argument | Provide a valid state scope in one accepted format. Use either repeated `--scope key=value` values or one `--scope-json` value. | `cdf state show` |
| `CDF-STATUS-FRESHNESS` | STATUS | internal | 70 | freshness | Retry status after checking the local state database and package artifacts. Run `cdf doctor` for the selected project when the status check repeats. | `cdf status` |
