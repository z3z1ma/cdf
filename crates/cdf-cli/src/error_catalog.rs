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

/// Generic lower-layer mappings used while WS4B migrates individual construction
/// sites to product-specific codes. These preserve the existing exit-code
/// taxonomy and intentionally name broad product areas instead of call sites.
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
