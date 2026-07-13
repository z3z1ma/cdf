#![allow(dead_code)] // 10x: WS3B creates the renderer boundary before WS3C/WS3D migrate command families into it.

pub(crate) mod config;
pub(crate) mod humanize;
pub(crate) mod primitives;
pub(crate) mod redaction;
mod style;

pub(crate) use config::RenderConfig;
pub(crate) use primitives::RenderDocument;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::{
        config::{DisplayMode, RenderEnv},
        humanize::{humanize_bytes, humanize_duration, humanize_rate, humanize_rows},
        primitives::{
            KeyValuePanel, NextCommand, RenderPrimitive, SectionRule, StatusKind, StatusLine, Table,
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
                "\u{1b}[36m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\u{1b}[0m\n",
                "\u{1b}[32m✓\u{1b}[0m package finalized\n",
                "\n",
                "\u{1b}[36mRun summary\u{1b}[0m\n",
                "  rows      12.3k\n",
                "  bytes     2.5 MiB\n",
                "  duration  1m 05s\n",
                "\n",
                "┌──────────┬────────────┬─────────┐\n",
                "│ resource │ rows       │ rate    │\n",
                "├──────────┼────────────┼─────────┤\n",
                "│ events   │ 12.3k      │ 4 MiB/s │\n",
                "│ users    │ [redacted] │ 988 B/s │\n",
                "└──────────┴────────────┴─────────┘\n",
                "\n",
                "\u{1b}[36m→\u{1b}[0m cdf inspect run run-123\n"
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
                "--------------------------------------------------------\n",
                "OK package finalized\n",
                "\n",
                "Run summary\n",
                "  rows      12.3k\n",
                "  bytes     2.5 MiB\n",
                "  duration  1m 05s\n",
                "\n",
                "+----------+------------+---------+\n",
                "| resource | rows       | rate    |\n",
                "+----------+------------+---------+\n",
                "| events   | 12.3k      | 4 MiB/s |\n",
                "| users    | [redacted] | 988 B/s |\n",
                "+----------+------------+---------+\n",
                "\n",
                "-> cdf inspect run run-123\n"
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
    fn width_is_applied_to_rules_and_cell_values() {
        let config = RenderConfig::headless_for_width(24);
        let rendered = RenderDocument::new()
            .push(SectionRule::new())
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
            RenderEnv::default(),
            TerminalPolicy::default(),
        );
        let ascii = RenderConfig::new(
            DisplayMode::Tty,
            20,
            RenderEnv::default(),
            TerminalPolicy {
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

    fn representative_document() -> RenderDocument {
        RenderDocument::new()
            .push(SectionRule::new())
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
