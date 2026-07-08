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

    fn rich_config() -> RenderConfig {
        RenderConfig::new(
            DisplayMode::Tty,
            72,
            RenderEnv {
                no_color: false,
                clicolor_force: false,
            },
            false,
        )
    }

    fn headless_config() -> RenderConfig {
        RenderConfig::new(
            DisplayMode::Headless,
            56,
            RenderEnv {
                no_color: false,
                clicolor_force: true,
            },
            false,
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
            },
            true,
        );
        let rendered = StatusLine::new(StatusKind::Success, "done").render(&config);

        assert_eq!(rendered, "✓ done\n");
        assert!(!rendered.contains("\u{1b}["));
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
            rendered,
            concat!(
                "------------------------\n",
                "+-----------+----------+\n",
                "| name      | value    |\n",
                "+-----------+----------+\n",
                "| long-res… | full va… |\n",
                "+-----------+----------+\n"
            )
        );
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
