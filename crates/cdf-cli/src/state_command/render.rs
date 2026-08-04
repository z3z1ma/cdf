use cdf_kernel::{
    CommittedLogPosition, MongoResumeMode, MongoResumeTokenSource, MongoWatchLevel,
    ResumeTokenPosition, SourcePosition, TableSnapshotSelector,
};
use serde_json::Value;

use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
};

pub(super) fn show_document(report: &StateShowReport) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            if report.head.is_some() {
                StatusKind::Success
            } else {
                StatusKind::Warning
            },
            if report.head.is_some() {
                "state head found"
            } else {
                "no committed state head"
            },
        ))
        .blank_line()
        .push(scope_panel(
            "Scope",
            &report.args,
            &report.pipeline_id,
            &report.scope,
        ));

    document = match &report.head {
        Some(head) => document.blank_line().push(checkpoint_panel("Head", head)),
        None => document.blank_line().push(
            KeyValuePanel::new("Head")
                .row("checkpoint", "none")
                .row("status", "missing")
                .row("mutation performed", "none"),
        ),
    };

    document
        .blank_line()
        .push(NextCommand::new(state_scope_command(
            "cdf state history",
            &report.args,
        )))
}

pub(super) fn history_document(report: &StateHistoryReport) -> RenderDocument {
    let table = report.history.iter().fold(
        Table::new(["checkpoint", "status", "head", "package", "receipt"]),
        |table, checkpoint| {
            table.row([
                checkpoint.delta.checkpoint_id.to_string(),
                checkpoint.status.as_str().to_owned(),
                yes_no(checkpoint.is_head).to_owned(),
                checkpoint.delta.package_hash.to_string(),
                checkpoint
                    .receipt
                    .as_ref()
                    .map(|receipt| receipt.receipt_id.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
            ])
        },
    );

    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("{} checkpoint(s)", report.history.len()),
        ))
        .blank_line()
        .push(scope_panel(
            "Scope",
            &report.args,
            &report.pipeline_id,
            &report.scope,
        ))
        .blank_line()
        .push(history_panel(&report.history))
        .blank_line()
        .push(table)
        .blank_line()
        .push(NextCommand::new(state_scope_command(
            "cdf state show",
            &report.args,
        )))
}

pub(super) fn rewind_document(report: &StateRewindReport) -> RenderDocument {
    let outcome = &report.outcome;
    let table = outcome
        .packages_ahead
        .iter()
        .fold(Table::new(["package ahead of state"]), |table, package| {
            table.row([package.to_string()])
        });

    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("rewound to {}", outcome.head.delta.checkpoint_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Rewind")
                .row("marker", outcome.marker.delta.checkpoint_id.to_string())
                .row(
                    "target",
                    outcome
                        .marker
                        .rewind_target_checkpoint_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| outcome.head.delta.checkpoint_id.to_string()),
                )
                .row("new head", outcome.head.delta.checkpoint_id.to_string())
                .row("marker status", outcome.marker.status.as_str())
                .row("head status", outcome.head.status.as_str())
                .row("packages ahead", outcome.packages_ahead.len().to_string())
                .row("mutation performed", "rewind marker checkpoint appended"),
        )
        .blank_line()
        .push(table)
        .blank_line()
        .push(NextCommand::new(state_scope_command(
            "cdf state show",
            &report.args,
        )))
}

fn scope_panel(
    title: &str,
    args: &StateScopeArgs,
    pipeline_id: &PipelineId,
    scope: &ScopeKey,
) -> KeyValuePanel {
    KeyValuePanel::new(title)
        .row("pipeline", pipeline_id.to_string())
        .row("resource", args.resource_id.clone())
        .row(
            "scope",
            serde_json::to_string(scope).unwrap_or_else(|_| "<unavailable>".to_owned()),
        )
}

fn checkpoint_panel(title: &str, checkpoint: &cdf_kernel::Checkpoint) -> KeyValuePanel {
    let mut panel = KeyValuePanel::new(title)
        .row("checkpoint", checkpoint.delta.checkpoint_id.to_string())
        .row("status", checkpoint.status.as_str())
        .row("is head", yes_no(checkpoint.is_head))
        .row("package", checkpoint.delta.package_hash.to_string())
        .row(
            "receipt",
            checkpoint
                .receipt
                .as_ref()
                .map(|receipt| receipt.receipt_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
        .row(
            "source position",
            checkpoint.delta.output_position.kind().as_str(),
        );
    match &checkpoint.delta.output_position {
        SourcePosition::TableSnapshot(position) => {
            panel = panel
                .row("table protocol", position.protocol.clone())
                .row("catalog", position.catalog.clone())
                .row(
                    "table",
                    format!("{}.{}", position.namespace.join("."), position.table),
                )
                .row("selector", table_selector_summary(&position.selector))
                .row("snapshot", position.snapshot_id.to_string())
                .row("sequence", position.sequence_number.to_string())
                .row(
                    "parent snapshot",
                    position
                        .parent_snapshot_id
                        .map_or_else(|| "none".to_owned(), |value| value.to_string()),
                )
                .row("metadata generation", position.metadata_generation.clone());
        }
        SourcePosition::Log(position) => match position.as_ref() {
            CommittedLogPosition::PostgreSql(position) => {
                panel = panel
                    .row("log protocol", "postgresql")
                    .row(
                        "system identifier",
                        position.scope.system_identifier.clone(),
                    )
                    .row("database OID", position.scope.database_oid.to_string())
                    .row("slot", position.scope.slot.clone())
                    .row("output plugin", position.scope.output_plugin.clone())
                    .row("commit LSN", position.commit_lsn.to_string())
                    .row("end LSN", position.end_lsn.to_string())
                    .row("transaction ID", position.xid.to_string())
                    .row("capture semantics", position.scope.semantics_sha256.clone());
            }
            CommittedLogPosition::MySql(position) => {
                panel = panel
                    .row("log protocol", "mysql")
                    .row("source binding", position.scope.source_binding.clone())
                    .row("server UUID", position.scope.active_server_uuid.clone())
                    .row("binlog file", position.binlog_file.clone())
                    .row("binlog sequence", position.file_sequence.to_string())
                    .row("end log position", position.end_log_position.to_string())
                    .row("transaction GTID", position.transaction_gtid.clone())
                    .row("executed GTID set", position.executed_gtid_set.clone())
                    .row("capture semantics", position.scope.semantics_sha256.clone());
            }
        },
        SourcePosition::ResumeToken(position) => {
            let ResumeTokenPosition::MongoChangeStream(position) = position.as_ref();
            panel = panel
                .row("resume protocol", "mongodb_change_stream")
                .row("source binding", position.scope.source_binding.clone())
                .row("watch target", mongo_watch_target(position))
                .row("resume mode", mongo_resume_mode(position.resume_mode))
                .row("token source", mongo_token_source(position.token_source))
                .row("token SHA-256", position.token_sha256.clone())
                .row("pipeline", position.scope.pipeline_sha256.clone())
                .row("capture semantics", position.scope.options_sha256.clone());
        }
        _ => {}
    }
    panel
}

fn mongo_resume_mode(mode: MongoResumeMode) -> &'static str {
    match mode {
        MongoResumeMode::ResumeAfter => "resume_after",
        MongoResumeMode::StartAfter => "start_after",
    }
}

fn mongo_token_source(source: MongoResumeTokenSource) -> &'static str {
    match source {
        MongoResumeTokenSource::Event => "event",
        MongoResumeTokenSource::PostBatch => "post_batch",
    }
}

fn mongo_watch_target(position: &cdf_kernel::MongoChangeStreamResumeToken) -> String {
    match position.scope.watch_level {
        MongoWatchLevel::Cluster => "cluster".to_owned(),
        MongoWatchLevel::Database => position
            .scope
            .database
            .clone()
            .unwrap_or_else(|| "<invalid database target>".to_owned()),
        MongoWatchLevel::Collection => format!(
            "{}.{}",
            position.scope.database.as_deref().unwrap_or("<invalid>"),
            position.scope.collection.as_deref().unwrap_or("<invalid>")
        ),
    }
}

fn table_selector_summary(selector: &TableSnapshotSelector) -> String {
    match selector {
        TableSnapshotSelector::Current => "current".to_owned(),
        TableSnapshotSelector::Branch { name } => format!("branch:{name}"),
        TableSnapshotSelector::Tag { name } => format!("tag:{name}"),
        TableSnapshotSelector::Snapshot { snapshot_id } => format!("snapshot:{snapshot_id}"),
        TableSnapshotSelector::Timestamp { timestamp_ms } => format!("timestamp:{timestamp_ms}"),
    }
}

fn history_panel(history: &[cdf_kernel::Checkpoint]) -> KeyValuePanel {
    KeyValuePanel::new("History")
        .row("checkpoints", history.len().to_string())
        .row(
            "oldest",
            history
                .first()
                .map(|checkpoint| checkpoint.delta.checkpoint_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
        .row(
            "newest",
            history
                .last()
                .map(|checkpoint| checkpoint.delta.checkpoint_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
        .row(
            "head",
            history
                .iter()
                .find(|checkpoint| checkpoint.is_head)
                .map(|checkpoint| checkpoint.delta.checkpoint_id.to_string())
                .unwrap_or_else(|| "none".to_owned()),
        )
}

fn state_scope_command(prefix: &str, args: &StateScopeArgs) -> String {
    let mut command = format!("{prefix} {}", args.resource_id);
    if let Some(pipeline_id) = &args.pipeline_id {
        command.push_str(" --pipeline ");
        command.push_str(pipeline_id);
    }
    if let Some(scope_json) = &args.scope_json {
        append_scope_json_as_command_args(&mut command, scope_json);
    }
    for pair in &args.scope {
        command.push_str(" --scope ");
        command.push_str(pair);
    }
    command
}

fn append_scope_json_as_command_args(command: &mut String, scope_json: &str) {
    let Ok(Value::Object(scope)) = serde_json::from_str::<Value>(scope_json) else {
        command.push_str(" --scope-json ");
        command.push_str(scope_json);
        return;
    };

    let mut pairs = Vec::new();
    for (key, value) in scope {
        let Value::String(value) = value else {
            command.push_str(" --scope-json ");
            command.push_str(scope_json);
            return;
        };
        pairs.push(format!("{key}={value}"));
    }

    for pair in pairs {
        command.push_str(" --scope ");
        command.push_str(&pair);
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
