pub mod config;
pub mod humanize;
pub mod primitives;
pub mod redaction;
mod style;

pub use config::RenderConfig;
pub use primitives::RenderDocument;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::{
        config::{DisplayMode, RenderEnv},
        humanize::{humanize_bytes, humanize_duration, humanize_rate, humanize_rows},
        primitives::{
            ErrorBlock, KeyValuePanel, NextCommand, RenderPrimitive, StatusKind, StatusLine, Table,
        },
    };
    use crate::terminal::{PolicyMode, TerminalPolicy};

    fn rich_config() -> RenderConfig {
        RenderConfig::new(
            DisplayMode::Tty,
            72,
            RenderEnv {
                no_color: false,
                clicolor_force: false,
                unicode_supported: true,
            },
            TerminalPolicy::default(),
        )
    }

    fn headless_config() -> RenderConfig {
        RenderConfig::new(
            DisplayMode::Headless,
            56,
            RenderEnv {
                no_color: false,
                clicolor_force: true,
                unicode_supported: false,
            },
            TerminalPolicy::default(),
        )
    }

    #[test]
    fn rich_snapshot_covers_representative_primitives() {
        let document = representative_document();
        let rendered = document.render(&rich_config());

        assert_eq!(
            rendered,
            concat!(
                "\u{1b}[32m✓\u{1b}[0m \u{1b}[1mpackage finalized\u{1b}[0m\n",
                "\n",
                "\u{1b}[1mRun summary\u{1b}[0m\n",
                "\u{1b}[2m  rows      \u{1b}[0m12.3k\n",
                "\u{1b}[2m  bytes     \u{1b}[0m2.5 MiB\n",
                "\u{1b}[2m  duration  \u{1b}[0m1m 05s\n",
                "\n",
                "\u{1b}[2mresource  rows        rate   \u{1b}[0m\n",
                "events    12.3k       4 MiB/s\n",
                "users     [redacted]  988 B/s\n",
                "\n",
                "\u{1b}[2mNext:\u{1b}[0m \u{1b}[36mcdf inspect run run-123\u{1b}[0m\n"
            )
        );
    }

    #[test]
    fn headless_snapshot_covers_ascii_static_output() {
        let document = representative_document();
        let rendered = document.render(&headless_config());

        assert_eq!(
            rendered,
            concat!(
                "OK package finalized\n",
                "\n",
                "Run summary\n",
                "  rows      12.3k\n",
                "  bytes     2.5 MiB\n",
                "  duration  1m 05s\n",
                "\n",
                "resource  rows        rate   \n",
                "events    12.3k       4 MiB/s\n",
                "users     [redacted]  988 B/s\n",
                "\n",
                "Next: cdf inspect run run-123\n"
            )
        );
    }

    #[test]
    fn no_color_policy_disables_ansi_without_disabling_rich_glyphs() {
        let config = RenderConfig::new(
            DisplayMode::Tty,
            32,
            RenderEnv {
                no_color: false,
                clicolor_force: true,
                unicode_supported: true,
            },
            TerminalPolicy {
                color: PolicyMode::Never,
                ..TerminalPolicy::default()
            },
        );
        let rendered = StatusLine::new(StatusKind::Success, "done").render(&config);

        assert_eq!(rendered, "✓ done\n");
        assert!(!rendered.contains("\u{1b}["));
    }

    #[test]
    fn cx1_color_and_unicode_policy_respect_tty_redirection_and_explicit_override() {
        use crate::terminal::{OutputChannel, TerminalEnvironment};

        let redirected = TerminalEnvironment {
            no_color: false,
            clicolor_force: true,
            ..TerminalEnvironment::default()
        };
        let always_redirected = RenderConfig::from_environment(
            TerminalPolicy {
                color: PolicyMode::Always,
                unicode: PolicyMode::Always,
                ..TerminalPolicy::default()
            },
            OutputChannel::Stdout,
            redirected,
        );
        assert!(!always_redirected.color_enabled());
        assert!(always_redirected.rich_glyphs());

        let tty_no_color = TerminalEnvironment {
            stdout_is_terminal: true,
            no_color: true,
            ..TerminalEnvironment::default()
        };
        let automatic = RenderConfig::from_environment(
            TerminalPolicy::default(),
            OutputChannel::Stdout,
            tty_no_color,
        );
        let explicit = RenderConfig::from_environment(
            TerminalPolicy {
                color: PolicyMode::Always,
                ..TerminalPolicy::default()
            },
            OutputChannel::Stdout,
            tty_no_color,
        );
        assert!(!automatic.color_enabled());
        assert!(!automatic.rich_glyphs());
        assert!(explicit.color_enabled());

        let utf8_tty = RenderConfig::from_environment(
            TerminalPolicy::default(),
            OutputChannel::Stdout,
            TerminalEnvironment {
                stdout_is_terminal: true,
                unicode_supported: true,
                ..TerminalEnvironment::default()
            },
        );
        assert!(utf8_tty.rich_glyphs());
    }

    #[test]
    fn json_mode_bypasses_rendered_human_output() {
        let output = crate::output::CommandOutput::rendered(
            "renderer-test",
            representative_document(),
            serde_json::json!({ "machine": true }),
        )
        .unwrap();

        let result = crate::output::InvocationResult::from_output(true, &rich_config(), output);

        assert_eq!(result.exit_code, 0);
        assert!(!result.stdout.contains("package finalized"));
        assert!(result.stdout.contains("\"machine\": true"));
    }

    #[test]
    fn terminal_matrix_preserves_width_and_semantics_at_40_80_and_160_columns() {
        for width in [40, 80, 160] {
            for display_mode in [DisplayMode::Tty, DisplayMode::Headless] {
                for unicode_supported in [false, true] {
                    let config = RenderConfig::new(
                        display_mode,
                        width,
                        RenderEnv {
                            no_color: true,
                            clicolor_force: false,
                            unicode_supported,
                        },
                        TerminalPolicy {
                            color: PolicyMode::Never,
                            ..TerminalPolicy::default()
                        },
                    );
                    let rendered = representative_document().render(&config);
                    assert!(rendered.contains("package finalized"));
                    assert!(rendered.contains("12.3k"));
                    assert!(rendered.contains("cdf inspect run run-123"));
                    for line in rendered.lines() {
                        assert!(
                            unicode_width::UnicodeWidthStr::width(line) <= width,
                            "{display_mode:?}/{unicode_supported}/{width}: {line:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn representative_report_family_matrix_preserves_shared_hierarchy_and_facts() {
        for width in [40, 80, 160] {
            for display_mode in [DisplayMode::Tty, DisplayMode::Headless] {
                for unicode_supported in [false, true] {
                    let config = RenderConfig::new(
                        display_mode,
                        width,
                        RenderEnv {
                            no_color: true,
                            clicolor_force: false,
                            unicode_supported,
                        },
                        TerminalPolicy {
                            color: PolicyMode::Never,
                            unicode: if unicode_supported {
                                PolicyMode::Always
                            } else {
                                PolicyMode::Never
                            },
                            ..TerminalPolicy::default()
                        },
                    );

                    for (family, document, facts) in report_family_documents() {
                        let rendered = document.render(&config);
                        assert!(
                            rendered.starts_with("OK ")
                                || rendered.starts_with("WARN ")
                                || rendered.starts_with("ERR ")
                                || rendered.starts_with('✓')
                                || rendered.starts_with('!')
                                || rendered.starts_with('✗')
                                || rendered.starts_with("error["),
                            "{family}/{display_mode:?}/{unicode_supported}/{width} lacked an outcome-first hierarchy:\n{rendered}"
                        );
                        for fact in facts {
                            assert!(
                                rendered.contains(fact),
                                "{family}/{display_mode:?}/{unicode_supported}/{width} lost {fact:?}:\n{rendered}"
                            );
                        }
                        assert!(
                            !rendered.contains("\u{1b}["),
                            "{family}/{display_mode:?}/{unicode_supported}/{width} ignored no-color"
                        );
                        assert!(
                            rendered
                                .lines()
                                .all(|line| unicode_width::UnicodeWidthStr::width(line) <= width),
                            "{family}/{display_mode:?}/{unicode_supported}/{width} exceeded width:\n{rendered}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn report_family_proof_is_progressively_disclosed() {
        let normal = RenderConfig::headless_for_width(80);
        let verbose = RenderConfig::new(
            DisplayMode::Headless,
            80,
            RenderEnv::default(),
            TerminalPolicy {
                verbosity: crate::terminal::Verbosity::Verbose(1),
                ..TerminalPolicy::default()
            },
        );
        let execute = execute_report();

        assert!(!execute.render(&normal).contains("receipt-execute"));
        assert!(execute.render(&verbose).contains("receipt-execute"));
    }

    #[test]
    fn compact_primitives_wrap_without_losing_narrow_terminal_content() {
        let config = RenderConfig::headless_for_width(40);
        let document = RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                "loaded a resource whose outcome needs a second line",
            ))
            .push(
                KeyValuePanel::new("A deliberately long summary title that wraps").row(
                    "a-deliberately-long-evidence-key-that-wraps",
                    "and its complete evidence value remains visible",
                ),
            );
        let rendered = document.render(&config);
        let actionable = RenderDocument::new()
            .push(NextCommand::new(
                "cdf inspect run run-with-a-deliberately-long-identifier",
            ))
            .push(
                ErrorBlock::new(
                    "CDF-CLI-DELIBERATELY-LONG",
                    "the causal error message remains readable at narrow widths",
                )
                .detail("offending-value", "a complete value that wraps")
                .help("run a complete corrective command without truncation")
                .suggestion("cdf validate --deep"),
            )
            .render(&config);

        let compact = rendered
            .chars()
            .filter(|character| !character.is_whitespace())
            .collect::<String>();
        for expected in [
            "loaded a resource",
            "a-deliberately-long-evidence-key",
            "complete evidence value remains visible",
        ] {
            let compact_expected = expected
                .chars()
                .filter(|character| !character.is_whitespace())
                .collect::<String>();
            assert!(
                compact.contains(&compact_expected),
                "missing {expected:?}:\n{rendered}"
            );
        }
        assert!(
            rendered
                .lines()
                .all(|line| unicode_width::UnicodeWidthStr::width(line) <= 40),
            "narrow primitive output exceeded width:\n{rendered}"
        );
        for expected in [
            "Next: cdf inspect run run-with-a-deliberately-long-identifier",
            "error[CDF-CLI-DELIBERATELY-LONG]: the causal error message remains readable at narrow widths",
            "offending-value: a complete value that wraps",
            "help: run a complete corrective command without truncation",
            "try: cdf validate --deep",
        ] {
            assert!(
                actionable.contains(expected),
                "actionable output was not copyable:\n{actionable}"
            );
        }
    }

    #[test]
    fn progressive_disclosure_keeps_proof_available_without_dominating_normal_output() {
        let document = RenderDocument::new()
            .push(StatusLine::new(StatusKind::Success, "loaded"))
            .push_verbose(KeyValuePanel::proof().row("hash", "sha256:abc"));
        let normal = document.render(&rich_config());
        let verbose = document.render(&RenderConfig::new(
            DisplayMode::Tty,
            72,
            RenderEnv {
                no_color: false,
                clicolor_force: false,
                unicode_supported: true,
            },
            TerminalPolicy {
                verbosity: crate::terminal::Verbosity::Verbose(1),
                ..TerminalPolicy::default()
            },
        ));

        assert!(normal.contains("loaded"));
        assert!(!normal.contains("sha256:abc"));
        assert!(verbose.contains("sha256:abc"));
    }

    #[test]
    fn width_is_applied_to_rules_and_cell_values() {
        let config = RenderConfig::headless_for_width(24);
        let rendered = RenderDocument::new()
            .push(
                Table::new(["name", "value"])
                    .row(["long-resource-name", "full value is available via json"]),
            )
            .render(&config);

        assert_eq!(
            rendered
                .lines()
                .filter_map(|line| line.strip_prefix("  "))
                .collect::<String>(),
            "long-resource-namefull value is available via json"
        );
        assert!(
            rendered
                .lines()
                .all(|line| unicode_width::UnicodeWidthStr::width(line) <= 24)
        );
        assert!(!rendered.contains('~'));
    }

    #[test]
    fn cx1_tables_measure_display_width_and_ascii_mode_uses_ascii_truncation() {
        let unicode = RenderConfig::new(
            DisplayMode::Tty,
            20,
            RenderEnv {
                unicode_supported: true,
                ..RenderEnv::default()
            },
            TerminalPolicy {
                color: PolicyMode::Never,
                ..TerminalPolicy::default()
            },
        );
        let ascii = RenderConfig::new(
            DisplayMode::Tty,
            20,
            RenderEnv::default(),
            TerminalPolicy {
                color: PolicyMode::Never,
                unicode: PolicyMode::Never,
                ..TerminalPolicy::default()
            },
        );
        let table = Table::new(["name", "value"]).row(["東京", "abcdefghijkl"]);

        let unicode_rendered = table.render(&unicode);
        let ascii_rendered = Table::new(["name", "value"])
            .row(["tokyo", "abcdefghijkl"])
            .render(&ascii);

        assert!(
            unicode_rendered
                .lines()
                .all(|line| unicode_width::UnicodeWidthStr::width(line) <= 20)
        );
        assert!(ascii_rendered.is_ascii());
    }

    #[test]
    fn cx1_forty_column_five_field_table_stacks_without_losing_values() {
        let config = RenderConfig::headless_for_width(40);
        let rendered = Table::new(["resource", "phase", "rows", "bytes", "duration"])
            .row(["local.events", "validated", "12345", "987654", "12 seconds"])
            .render(&config);

        assert!(rendered.contains("resource:\n  local.events\n"));
        assert!(rendered.contains("duration:\n  12 seconds\n"));
        assert!(
            rendered
                .lines()
                .all(|line| unicode_width::UnicodeWidthStr::width(line) <= 40),
            "narrow output exceeded width:\n{rendered}"
        );
        assert!(!rendered.contains('~'));
    }

    #[test]
    fn cx1_one_and_two_column_tables_stack_before_any_value_truncates() {
        let config = RenderConfig::headless_for_width(40);
        let one_value = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        let one = Table::new(["value"]).row([one_value]).render(&config);
        let two = Table::new(["left", "right"])
            .row(["abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"])
            .render(&config);

        assert_eq!(stacked_payload(&one), one_value);
        assert_eq!(
            stacked_payload(&two),
            "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        );
        for rendered in [&one, &two] {
            assert!(
                rendered
                    .lines()
                    .all(|line| unicode_width::UnicodeWidthStr::width(line) <= 40)
            );
            assert!(!rendered.contains('~'));
            assert!(!rendered.contains('…'));
        }
    }

    fn stacked_payload(rendered: &str) -> String {
        rendered
            .lines()
            .filter_map(|line| line.strip_prefix("  "))
            .collect()
    }

    fn report_family_documents() -> Vec<(&'static str, RenderDocument, Vec<&'static str>)> {
        vec![
            (
                "inspect",
                RenderDocument::new()
                    .push(StatusLine::new(StatusKind::Success, "run inspected"))
                    .blank_line()
                    .push(KeyValuePanel::summary().row("run", "run-inspect"))
                    .blank_line()
                    .push(KeyValuePanel::proof().row("receipt", "receipt-inspect"))
                    .blank_line()
                    .push(NextCommand::new("cdf inspect package pkg-inspect")),
                vec![
                    "run inspected",
                    "Summary",
                    "run-inspect",
                    "Proof",
                    "receipt-inspect",
                    "Next:",
                ],
            ),
            (
                "plan",
                RenderDocument::new()
                    .push(StatusLine::new(StatusKind::Success, "plan ready"))
                    .blank_line()
                    .push(KeyValuePanel::summary().row("resources", "2"))
                    .blank_line()
                    .push(NextCommand::new("cdf run plan-plan")),
                vec!["plan ready", "Summary", "resources", "Next:"],
            ),
            (
                "execute",
                execute_report(),
                vec!["run complete", "Summary", "Effects", "rows", "Next:"],
            ),
            (
                "mutate",
                RenderDocument::new()
                    .push(StatusLine::new(StatusKind::Success, "schema promoted"))
                    .blank_line()
                    .push(KeyValuePanel::effects().row("schema", "v2"))
                    .blank_line()
                    .push(NextCommand::new("cdf schema show events")),
                vec!["schema promoted", "Effects", "schema", "Next:"],
            ),
            (
                "recover",
                RenderDocument::new()
                    .push(StatusLine::new(StatusKind::Success, "state recovered"))
                    .blank_line()
                    .push(KeyValuePanel::recovery().row("checkpoint", "checkpoint-recover"))
                    .blank_line()
                    .push(KeyValuePanel::proof().row("receipt", "receipt-recover"))
                    .blank_line()
                    .push(NextCommand::new("cdf inspect run run-recover")),
                vec![
                    "state recovered",
                    "Recovery",
                    "checkpoint-recover",
                    "Proof",
                    "Next:",
                ],
            ),
            (
                "list",
                RenderDocument::new()
                    .push(StatusLine::new(StatusKind::Success, "resources listed"))
                    .blank_line()
                    .push(KeyValuePanel::summary().row("resources", "1"))
                    .blank_line()
                    .push(Table::new(["resource", "state"]).row(["events", "ready"]))
                    .blank_line()
                    .push(NextCommand::new("cdf inspect resource events")),
                vec!["resources listed", "Summary", "events", "ready", "Next:"],
            ),
            (
                "no-op",
                RenderDocument::new()
                    .push(StatusLine::new(StatusKind::Success, "already current"))
                    .blank_line()
                    .push(KeyValuePanel::summary().row("resource", "events"))
                    .blank_line()
                    .push(KeyValuePanel::effects().row("writes", "none"))
                    .blank_line()
                    .push(NextCommand::new("cdf status")),
                vec!["already current", "Summary", "Effects", "none", "Next:"],
            ),
            (
                "warning",
                RenderDocument::new()
                    .push(StatusLine::new(StatusKind::Warning, "run needs attention"))
                    .blank_line()
                    .push(KeyValuePanel::attention().row("resource", "events"))
                    .blank_line()
                    .push(NextCommand::new("cdf doctor")),
                vec!["run needs attention", "Attention", "events", "Next:"],
            ),
            (
                "failure",
                RenderDocument::new().push(
                    ErrorBlock::new("E_CONFIG", "invalid config")
                        .detail("path", "cdf.toml")
                        .help("correct the invalid value")
                        .suggestion("cdf validate"),
                ),
                vec![
                    "error[E_CONFIG]",
                    "invalid config",
                    "path:",
                    "help:",
                    "try:",
                    "cdf validate",
                ],
            ),
        ]
    }

    fn execute_report() -> RenderDocument {
        RenderDocument::new()
            .push(StatusLine::new(StatusKind::Success, "run complete"))
            .blank_line()
            .push(KeyValuePanel::summary().row("rows", "100"))
            .blank_line()
            .push(KeyValuePanel::effects().row("packages", "1"))
            .blank_line()
            .push_verbose(KeyValuePanel::proof().row("receipt", "receipt-execute"))
            .blank_line()
            .push(NextCommand::new("cdf inspect run run-execute"))
    }

    fn representative_document() -> RenderDocument {
        RenderDocument::new()
            .push(StatusLine::new(StatusKind::Success, "package finalized"))
            .blank_line()
            .push(
                KeyValuePanel::new("Run summary")
                    .row("rows", humanize_rows(12_345))
                    .row("bytes", humanize_bytes(2_621_440))
                    .row(
                        "duration",
                        humanize_duration(std::time::Duration::from_secs(65)),
                    ),
            )
            .blank_line()
            .push(
                Table::new(["resource", "rows", "rate"])
                    .row([
                        "events".to_owned(),
                        humanize_rows(12_345),
                        humanize_rate(4_194_304.0),
                    ])
                    .row([
                        "users".to_owned(),
                        redaction::redacted(),
                        humanize_rate(988.0),
                    ]),
            )
            .blank_line()
            .push(NextCommand::new("cdf inspect run run-123"))
    }
}
