use super::*;
use crate::render::{
    RenderDocument,
    humanize::humanize_bytes,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
    redaction::redact_uri_userinfo,
};

impl PackageListReport {
    pub(super) fn render_document(&self) -> RenderDocument {
        let table = self.packages.iter().fold(
            Table::new(["path", "hash", "status", "segments"]),
            |table, package| {
                table.row([
                    redact_uri_userinfo(&package.path),
                    package.package_hash.clone(),
                    package.status.clone(),
                    package.segments.to_string(),
                ])
            },
        );

        let mut document = RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!("{} package(s)", self.packages.len()),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Packages")
                    .row("count", self.packages.len().to_string())
                    .row("source", "package root"),
            );

        if !self.packages.is_empty() {
            document = document.blank_line().push(table);
        }

        document
            .blank_line()
            .push(NextCommand::new("cdf package verify <package>"))
    }
}

impl PackageGcPlanReport {
    pub(super) fn render_document(&self) -> RenderDocument {
        let table = self.artifacts.iter().fold(
            Table::new(["artifact", "classification", "action", "reason"]),
            |table, artifact| {
                table.row([
                    artifact_display(artifact),
                    classification_name(&artifact.classification).to_owned(),
                    planned_action_name(&artifact.planned_action).to_owned(),
                    artifact.retention_reason.as_str().to_owned(),
                ])
            },
        );

        let mut document = RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!("planned package gc for {}", self.package_root),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Package GC")
                    .row("root", redact_uri_userinfo(&self.package_root))
                    .row("mode", self.mode)
                    .row("artifacts", self.artifacts.len().to_string())
                    .row("collectible", self.counts.collectible.to_string())
                    .row("collected", self.counts.collected.to_string())
                    .row("protected", self.counts.protected.to_string())
                    .row("tombstoned", self.counts.tombstoned.to_string())
                    .row("corrupt", self.counts.corrupt.to_string())
                    .row("missing", self.counts.missing.to_string()),
            );

        if !self.artifacts.is_empty() {
            document = document.blank_line().push(table);
        }

        if !self.promotion_availability.is_empty() {
            document = document
                .blank_line()
                .push(self.promotion_availability.iter().fold(
                    Table::new([
                        "resource",
                        "package",
                        "local bytes",
                        "promotable",
                        "action",
                        "removes last local authority",
                    ]),
                    |table, item| {
                        table.row([
                            item.resource_id.clone(),
                            item.package_hash.clone(),
                            humanize_bytes(item.local_residual_bytes),
                            yes_no(item.locally_promotable).to_owned(),
                            item.planned_action.as_str().to_owned(),
                            yes_no(item.collection_removes_last_local_promotable_copy).to_owned(),
                        ])
                    },
                ))
                .blank_line()
                .push(
                    KeyValuePanel::new("Promotion availability")
                        .row(
                            "remediation",
                            self.promotion_availability[0].remediation.clone(),
                        )
                        .row("destination readback inferred", "no"),
                );
        }

        document
            .blank_line()
            .push(NextCommand::new("cdf package verify <package>"))
    }
}

impl PackageVerifyReport {
    pub(super) fn render_document(&self) -> RenderDocument {
        RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!("verified package {}", self.package_hash),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Integrity")
                    .row("package", self.package_hash.clone())
                    .row("files", self.checked_file_count.to_string())
                    .row("archive segments", self.checked_archive_count.to_string()),
            )
            .blank_line()
            .push(NextCommand::new("cdf inspect package <package>"))
    }
}

impl PackageArchiveCliReport {
    pub(super) fn render_document(&self) -> RenderDocument {
        RenderDocument::new()
            .push(StatusLine::new(
                StatusKind::Success,
                format!("archived package {}", self.package_hash),
            ))
            .blank_line()
            .push(
                KeyValuePanel::new("Archive")
                    .row("package", self.package_hash.clone())
                    .row("format", self.format.clone())
                    .row("status", package_archive_status(&self.status))
                    .row("segments", self.segment_count.to_string())
                    .row("rows", self.row_count.to_string())
                    .row("bytes", humanize_bytes(self.archive_byte_count))
                    .row("index", redact_uri_userinfo(&self.segment_index_path))
                    .row("fidelity", redact_uri_userinfo(&self.fidelity_report_path))
                    .row("statement", self.fidelity_statement.clone()),
            )
            .blank_line()
            .push(NextCommand::new("cdf package verify <package>"))
    }
}

fn package_archive_status(status: &cdf_package::PackageArchiveWriteStatus) -> &'static str {
    match status {
        cdf_package::PackageArchiveWriteStatus::Written => "written",
        cdf_package::PackageArchiveWriteStatus::Skipped => "skipped",
        cdf_package::PackageArchiveWriteStatus::Replaced => "replaced",
    }
}

fn artifact_display(artifact: &PackageCollectionArtifact) -> String {
    artifact
        .package_path
        .as_deref()
        .or(artifact.package_hash.as_deref())
        .map(redact_uri_userinfo)
        .unwrap_or_else(|| "unknown".to_owned())
}

fn classification_name(classification: &PackageCollectionClassification) -> &'static str {
    match classification {
        PackageCollectionClassification::Retained => "retained",
        PackageCollectionClassification::Collectible => "collectible",
        PackageCollectionClassification::Collected => "collected",
        PackageCollectionClassification::Missing => "missing",
        PackageCollectionClassification::Corrupt => "corrupt",
        PackageCollectionClassification::Protected => "protected",
        PackageCollectionClassification::Tombstoned => "tombstoned",
    }
}

fn planned_action_name(action: &PackageCollectionAction) -> &'static str {
    match action {
        PackageCollectionAction::Retain => "retain",
        PackageCollectionAction::WouldCollect => "would_collect",
        PackageCollectionAction::Collected => "collected",
        PackageCollectionAction::RestoreRequired => "restore_required",
        PackageCollectionAction::AlreadyTombstoned => "already_tombstoned",
    }
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
