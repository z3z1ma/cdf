use cdf_kernel::ErrorKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ErrorMapping {
    pub(crate) code: &'static str,
    pub(crate) exit_code: i32,
    pub(crate) remediation: Option<RemediationTemplate>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RemediationTemplate {
    pub(crate) summary: &'static str,
    pub(crate) steps: &'static [&'static str],
}

pub(crate) const USAGE: ErrorMapping = ErrorMapping {
    code: "CDF-CLI-USAGE",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Correct the command arguments and run the command again.",
        steps: &["Run `cdf help <command>` for the accepted syntax."],
    }),
};

pub(crate) const NOT_SUPPORTED: ErrorMapping = ErrorMapping {
    code: "CDF-CLI-NOT-SUPPORTED",
    exit_code: 78,
    remediation: Some(RemediationTemplate {
        summary: "Use a currently supported path or wait for the named lower layer to land.",
        steps: &["Inspect the error message for the required lower layer."],
    }),
};

pub(crate) const CLI_JSON: ErrorMapping = ErrorMapping {
    code: "CDF-CLI-JSON-SERIALIZATION",
    exit_code: 70,
    remediation: Some(RemediationTemplate {
        summary: "Retry the command and report this internal serialization failure if it repeats.",
        steps: &["Include the command and error code in the report."],
    }),
};

#[cfg(feature = "cli-artifacts")]
pub(crate) const CLI_ARTIFACTS: ErrorMapping = ErrorMapping {
    code: "CDF-CLI-ARTIFACTS",
    exit_code: 70,
    remediation: Some(RemediationTemplate {
        summary: "Regenerate or re-check the CLI artifacts with the repository artifact command.",
        steps: &["Run the artifact command shown in the error message."],
    }),
};

#[cfg(feature = "cli-artifacts")]
pub(crate) const CLI_ARTIFACTS_USAGE: ErrorMapping = ErrorMapping {
    code: "CDF-CLI-ARTIFACTS-USAGE",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Point the artifact check at the generated CLI artifact directory.",
        steps: &["Run the artifact command shown in the error message when artifacts are stale."],
    }),
};

pub(crate) const PROJECT_INIT_ARGUMENT: ErrorMapping = ErrorMapping {
    code: "CDF-PROJECT-INIT-ARGUMENT",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Correct the init argument and run `cdf init` again.",
        steps: &["Use a non-empty `--name` value when naming the project."],
    }),
};

pub(crate) const PROJECT_IO: ErrorMapping = ErrorMapping {
    code: "CDF-PROJECT-IO",
    exit_code: 70,
    remediation: Some(RemediationTemplate {
        summary: "Check the selected project path and filesystem access, then retry.",
        steps: &["Run the command from an accessible directory or pass `--project` explicitly."],
    }),
};

pub(crate) const PROJECT_RESOURCE_MAPPING: ErrorMapping = ErrorMapping {
    code: "CDF-PROJECT-RESOURCE-MAPPING",
    exit_code: 3,
    remediation: Some(RemediationTemplate {
        summary: "Update the project resource mapping to a compiled resource id or source wildcard.",
        steps: &["Run `cdf inspect resources` after correcting the `[resources]` key."],
    }),
};

pub(crate) const PROJECT_MERGE_KEY: ErrorMapping = ErrorMapping {
    code: "CDF-PROJECT-MERGE-KEY",
    exit_code: 3,
    remediation: Some(RemediationTemplate {
        summary: "Choose append or declare the merge identity before contacting the source or destination.",
        steps: &[
            "Add `merge_key = [...]` when rows should merge by an explicit identity.",
            "Otherwise use `write_disposition = \"append\"` to append rows without a key.",
        ],
    }),
};

pub(crate) const PYTHON_RESOURCE: ErrorMapping = ErrorMapping {
    code: "CDF-PYTHON-RESOURCE",
    exit_code: 3,
    remediation: Some(RemediationTemplate {
        summary: "Repair the configured Python interpreter or resource target, then retry.",
        steps: &[
            "Run `cdf doctor` to verify the configured interpreter without executing resource code.",
        ],
    }),
};

pub(crate) const RESOURCE_NOT_COMPILED: ErrorMapping = ErrorMapping {
    code: "CDF-RESOURCE-NOT-COMPILED",
    exit_code: 3,
    remediation: Some(RemediationTemplate {
        summary: "Use one of the compiled resource ids or repair the project resource mapping.",
        steps: &[
            "Run `cdf inspect resources` to see compiled ids and their source files.",
            "If the id is expected, update the `[resources]` mapping to `<source>.<resource>` or `<source>.*`.",
        ],
    }),
};

pub(crate) const CONTRACT_ARGUMENT: ErrorMapping = ErrorMapping {
    code: "CDF-CONTRACT-ARGUMENT",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Use a supported contract subcommand or trust policy.",
        steps: &["Run `cdf help contract` for accepted contract commands."],
    }),
};

pub(crate) const CONTRACT_LOCKFILE: ErrorMapping = ErrorMapping {
    code: "CDF-CONTRACT-LOCKFILE",
    exit_code: 3,
    remediation: Some(RemediationTemplate {
        summary: "Create or update the contract lockfile before testing contract drift.",
        steps: &["Run `cdf contract freeze` for the selected project."],
    }),
};

pub(crate) const SCAN_ARGUMENT: ErrorMapping = ErrorMapping {
    code: "CDF-RUN-SCAN-ARGUMENT",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Correct the scan, plan, explain, or preview argument and retry.",
        steps: &["Run `cdf help plan`, `cdf help explain`, or `cdf help preview` for syntax."],
    }),
};

pub(crate) const PREVIEW_RUNTIME_NOT_SUPPORTED: ErrorMapping = ErrorMapping {
    code: "CDF-RUN-PREVIEW-RUNTIME-NOT-SUPPORTED",
    exit_code: 78,
    remediation: Some(RemediationTemplate {
        summary: "Preview only works when the resource runtime can open a direct stream.",
        steps: &["Use a file, REST, or SQL resource with an implemented preview stream."],
    }),
};

pub(crate) const DESTINATION_NOT_SUPPORTED: ErrorMapping = ErrorMapping {
    code: "CDF-DEST-NOT-SUPPORTED",
    exit_code: 78,
    remediation: Some(RemediationTemplate {
        summary: "Use a registered destination driver and URI shape for this command.",
        steps: &[
            "Supported local paths include `duckdb://path` and `parquet://root` where applicable.",
        ],
    }),
};

pub(crate) const RUN_ARGUMENT: ErrorMapping = ErrorMapping {
    code: "CDF-RUN-ARGUMENT",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Provide the required run resource and stable identifiers.",
        steps: &["Run `cdf help run` for accepted run arguments."],
    }),
};

pub(crate) const RUN_LOOP_NOT_SUPPORTED: ErrorMapping = ErrorMapping {
    code: "CDF-RUN-LOOP-NOT-SUPPORTED",
    exit_code: 78,
    remediation: Some(RemediationTemplate {
        summary: "Run one package at a time in this slice.",
        steps: &["Use `cdf run RESOURCE` without `--loop` until the loop supervisor lands."],
    }),
};

pub(crate) const RUN_ARTIFACT_PATH: ErrorMapping = ErrorMapping {
    code: "CDF-RUN-ARTIFACT-PATH",
    exit_code: 5,
    remediation: Some(RemediationTemplate {
        summary: "Use a writable package, checkpoint, or state path inside the selected project.",
        steps: &["Avoid path traversal in package ids and ensure parent directories are writable."],
    }),
};

pub(crate) const RUN_ARTIFACT_INTERNAL: ErrorMapping = ErrorMapping {
    code: "CDF-RUN-ARTIFACT-INTERNAL",
    exit_code: 70,
    remediation: Some(RemediationTemplate {
        summary: "Retry the run and report the artifact path failure if it repeats.",
        steps: &["Include the selected package id, checkpoint id, and project path in the report."],
    }),
};

pub(crate) const REPLAY_ARGUMENT: ErrorMapping = ErrorMapping {
    code: "CDF-PACKAGE-REPLAY-ARGUMENT",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Provide the replay package destination arguments required by the destination.",
        steps: &["For Postgres replay, pass `--target schema.table --merge-dedup fail`."],
    }),
};

pub(crate) const REPLAY_PACKAGE_CONTRACT: ErrorMapping = ErrorMapping {
    code: "CDF-PACKAGE-REPLAY-CONTRACT",
    exit_code: 3,
    remediation: Some(RemediationTemplate {
        summary: "Make replay arguments match the package replay contract.",
        steps: &["Use the package destination target recorded at package creation time."],
    }),
};

pub(crate) const RESUME_LEDGER: ErrorMapping = ErrorMapping {
    code: "CDF-STATE-RESUME-LEDGER",
    exit_code: 5,
    remediation: Some(RemediationTemplate {
        summary: "Select an environment with a run ledger or provide an explicit resumable run.",
        steps: &["Run `cdf inspect run <id>` when a run id is known."],
    }),
};

pub(crate) const RESUME_MULTI_RUN_NOT_SUPPORTED: ErrorMapping = ErrorMapping {
    code: "CDF-STATE-RESUME-MULTI-RUN-NOT-SUPPORTED",
    exit_code: 78,
    remediation: Some(RemediationTemplate {
        summary: "Resume one interrupted run explicitly.",
        steps: &["Pass `cdf resume RUN_ID` for the run you want to drain."],
    }),
};

pub(crate) const STATE_SCOPE_ARGUMENT: ErrorMapping = ErrorMapping {
    code: "CDF-STATE-SCOPE-ARGUMENT",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Provide a valid state scope in one accepted format.",
        steps: &["Use either repeated `--scope key=value` values or one `--scope-json` value."],
    }),
};

pub(crate) const PACKAGE_ARGUMENT: ErrorMapping = ErrorMapping {
    code: "CDF-PACKAGE-ARGUMENT",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Correct the package command arguments and retry.",
        steps: &["Run `cdf help package` for package command syntax."],
    }),
};

pub(crate) const PACKAGE_ARTIFACT: ErrorMapping = ErrorMapping {
    code: "CDF-PACKAGE-ARTIFACT",
    exit_code: 5,
    remediation: Some(RemediationTemplate {
        summary: "Inspect the package directory and referenced package artifacts.",
        steps: &["Run `cdf package verify <package>` for a package directory when available."],
    }),
};

pub(crate) const SQL_QUERY: ErrorMapping = ErrorMapping {
    code: "CDF-SQL-QUERY",
    exit_code: 2,
    remediation: Some(RemediationTemplate {
        summary: "Submit one read-only SELECT or WITH query against system history.",
        steps: &["Remove mutating SQL, extra statements, or unterminated strings/comments."],
    }),
};

pub(crate) const SQL_RESULT: ErrorMapping = ErrorMapping {
    code: "CDF-SQL-RESULT",
    exit_code: 5,
    remediation: Some(RemediationTemplate {
        summary: "Inspect the local system-history data that the SQL query reads.",
        steps: &["Run a narrower `cdf sql` query to isolate the unreadable row or value."],
    }),
};

pub(crate) const SQL_INTERNAL: ErrorMapping = ErrorMapping {
    code: "CDF-SQL-INTERNAL",
    exit_code: 70,
    remediation: Some(RemediationTemplate {
        summary: "Retry the system-history SQL query and report the failure if it repeats.",
        steps: &["Include the query shape and error code, but do not include secret values."],
    }),
};

pub(crate) const STATUS_FRESHNESS: ErrorMapping = ErrorMapping {
    code: "CDF-STATUS-FRESHNESS",
    exit_code: 70,
    remediation: Some(RemediationTemplate {
        summary: "Retry status after checking the local state database and package artifacts.",
        steps: &["Run `cdf doctor` for the selected project when the status check repeats."],
    }),
};

pub(crate) const DOCTOR_DRIFT: ErrorMapping = ErrorMapping {
    code: "CDF-DOCTOR-DRIFT",
    exit_code: 5,
    remediation: Some(RemediationTemplate {
        summary: "Inspect the checkpoint ledger and destination drift inputs.",
        steps: &["Run `cdf doctor` again after repairing unreadable ledger or JSON values."],
    }),
};

#[cfg(feature = "cli-artifacts")]
pub(crate) fn reference_entries() -> Vec<(&'static str, ErrorMapping)> {
    let mut entries = vec![
        ("USAGE", USAGE),
        ("NOT_SUPPORTED", NOT_SUPPORTED),
        ("CLI_JSON", CLI_JSON),
        ("CLI_ARTIFACTS", CLI_ARTIFACTS),
        ("CLI_ARTIFACTS_USAGE", CLI_ARTIFACTS_USAGE),
        ("PROJECT_INIT_ARGUMENT", PROJECT_INIT_ARGUMENT),
        ("PROJECT_IO", PROJECT_IO),
        ("PROJECT_RESOURCE_MAPPING", PROJECT_RESOURCE_MAPPING),
        ("PROJECT_MERGE_KEY", PROJECT_MERGE_KEY),
        ("PYTHON_RESOURCE", PYTHON_RESOURCE),
        ("RESOURCE_NOT_COMPILED", RESOURCE_NOT_COMPILED),
        ("CONTRACT_ARGUMENT", CONTRACT_ARGUMENT),
        ("CONTRACT_LOCKFILE", CONTRACT_LOCKFILE),
        ("SCAN_ARGUMENT", SCAN_ARGUMENT),
        (
            "PREVIEW_RUNTIME_NOT_SUPPORTED",
            PREVIEW_RUNTIME_NOT_SUPPORTED,
        ),
        ("DESTINATION_NOT_SUPPORTED", DESTINATION_NOT_SUPPORTED),
        ("RUN_ARGUMENT", RUN_ARGUMENT),
        ("RUN_LOOP_NOT_SUPPORTED", RUN_LOOP_NOT_SUPPORTED),
        ("RUN_ARTIFACT_PATH", RUN_ARTIFACT_PATH),
        ("RUN_ARTIFACT_INTERNAL", RUN_ARTIFACT_INTERNAL),
        ("REPLAY_ARGUMENT", REPLAY_ARGUMENT),
        ("REPLAY_PACKAGE_CONTRACT", REPLAY_PACKAGE_CONTRACT),
        ("RESUME_LEDGER", RESUME_LEDGER),
        (
            "RESUME_MULTI_RUN_NOT_SUPPORTED",
            RESUME_MULTI_RUN_NOT_SUPPORTED,
        ),
        ("STATE_SCOPE_ARGUMENT", STATE_SCOPE_ARGUMENT),
        ("PACKAGE_ARGUMENT", PACKAGE_ARGUMENT),
        ("PACKAGE_ARTIFACT", PACKAGE_ARTIFACT),
        ("SQL_QUERY", SQL_QUERY),
        ("SQL_RESULT", SQL_RESULT),
        ("SQL_INTERNAL", SQL_INTERNAL),
        ("STATUS_FRESHNESS", STATUS_FRESHNESS),
        ("DOCTOR_DRIFT", DOCTOR_DRIFT),
    ];
    entries.extend([
        (
            "LOWER_TRANSIENT",
            generic_lower_layer_mapping(&ErrorKind::Transient),
        ),
        (
            "LOWER_RATE_LIMITED",
            generic_lower_layer_mapping(&ErrorKind::RateLimited),
        ),
        ("LOWER_AUTH", generic_lower_layer_mapping(&ErrorKind::Auth)),
        (
            "LOWER_CONTRACT",
            generic_lower_layer_mapping(&ErrorKind::Contract),
        ),
        ("LOWER_DATA", generic_lower_layer_mapping(&ErrorKind::Data)),
        (
            "LOWER_DESTINATION",
            generic_lower_layer_mapping(&ErrorKind::Destination),
        ),
        (
            "LOWER_INTERNAL",
            generic_lower_layer_mapping(&ErrorKind::Internal),
        ),
    ]);
    entries
}

/// Generic lower-layer mappings for `CdfError` values whose owning crate, not
/// the CLI call site, carries the domain semantics. These preserve the existing
/// exit-code taxonomy and intentionally name broad product areas.
pub(crate) fn generic_lower_layer_mapping(kind: &ErrorKind) -> ErrorMapping {
    match kind {
        ErrorKind::Transient => ErrorMapping {
            code: "CDF-RUN-TRANSIENT",
            exit_code: 75,
            remediation: Some(RemediationTemplate {
                summary: "Retry the command after the transient condition clears.",
                steps: &[
                    "If the error repeats, inspect the source, destination, and network health.",
                ],
            }),
        },
        ErrorKind::RateLimited => ErrorMapping {
            code: "CDF-RUN-RATE-LIMITED",
            exit_code: 75,
            remediation: Some(RemediationTemplate {
                summary: "Retry the command after the rate limit window clears.",
                steps: &["Reduce request concurrency or wait for the upstream quota to recover."],
            }),
        },
        ErrorKind::Auth => ErrorMapping {
            code: "CDF-PROJECT-AUTH",
            exit_code: 4,
            remediation: Some(RemediationTemplate {
                summary: "Check the configured secret reference or credential provider.",
                steps: &["Run `cdf validate` for the selected project and environment."],
            }),
        },
        ErrorKind::Contract => ErrorMapping {
            code: "CDF-PROJECT-CONTRACT",
            exit_code: 3,
            remediation: Some(RemediationTemplate {
                summary: "Fix the project, command, schema, or contract input and retry.",
                steps: &["Run `cdf validate` when the failure references project configuration."],
            }),
        },
        ErrorKind::Data => ErrorMapping {
            code: "CDF-PACKAGE-DATA",
            exit_code: 5,
            remediation: Some(RemediationTemplate {
                summary: "Inspect the referenced data, package, or source artifact.",
                steps: &[
                    "Use the matching `cdf inspect` command when an artifact id or path is shown.",
                ],
            }),
        },
        ErrorKind::Destination => ErrorMapping {
            code: "CDF-DEST-ERROR",
            exit_code: 6,
            remediation: Some(RemediationTemplate {
                summary: "Inspect the destination URI, target, policy, and destination health.",
                steps: &[
                    "Run `cdf doctor` for the selected project and environment when available.",
                ],
            }),
        },
        ErrorKind::Internal => ErrorMapping {
            code: "CDF-INTERNAL-UNEXPECTED",
            exit_code: 70,
            remediation: Some(RemediationTemplate {
                summary: "Retry with the same inputs and capture the command output if it repeats.",
                steps: &[
                    "Report the failure with the error code and the command that triggered it.",
                ],
            }),
        },
    }
}
