use std::path::Path;

use super::*;
use crate::render::{
    RenderDocument,
    primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
    redaction::redact_uri_userinfo,
};

pub(super) fn project_document(report: &InspectProjectReport) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "project {} env {}",
                report.config.project.name, report.environment.name
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project")
                .row("root", path_display(&report.root))
                .row("name", report.config.project.name.clone())
                .row("environment", report.environment.name.clone())
                .row("resources", report.resource_count.to_string())
                .row(
                    "destination",
                    redact_uri_userinfo(&report.environment.destination),
                ),
        )
        .blank_line()
        .push(NextCommand::new("cdf inspect resources"))
}

pub(super) fn resources_document(report: &InspectResourcesReport) -> RenderDocument {
    let table = report.0.iter().fold(
        Table::new([
            "compiled id",
            "configured source",
            "namespace",
            "resource",
            "resource file",
            "target",
        ]),
        |table, resource| {
            table.row([
                resource.descriptor.resource_id.to_string(),
                resource.configured_source.clone(),
                resource.namespace.clone(),
                resource.resource_name.clone(),
                resource.resource_file.clone(),
                resource.target.clone(),
            ])
        },
    );

    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("{} compiled resource(s)", report.0.len()),
        ))
        .blank_line()
        .push(table)
        .blank_line()
        .push(NextCommand::new("cdf plan"))
}

pub(super) fn resource_document(resource: &ResourceSummary) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("resource {}", resource.descriptor.resource_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Resource")
                .row("id", resource.descriptor.resource_id.to_string())
                .row("configured source", resource.configured_source.clone())
                .row("namespace", resource.namespace.clone())
                .row("resource", resource.resource_name.clone())
                .row("resource file", resource.resource_file.clone())
                .row("target", resource.target.clone())
                .row(
                    "trust",
                    format!("{:?}", resource.descriptor.trust_level).to_lowercase(),
                )
                .row(
                    "state scope",
                    state_scope_display(&resource.descriptor.state_scope),
                )
                .row(
                    "cursor",
                    resource
                        .descriptor
                        .cursor
                        .as_ref()
                        .map(|cursor| cursor.field.clone())
                        .unwrap_or_else(|| "none".to_owned()),
                )
                .row("capabilities", format!("{:?}", resource.capabilities))
                .row(
                    "stream capabilities",
                    resource
                        .stream_capabilities
                        .as_ref()
                        .map_or_else(|| "bounded".to_owned(), |value| format!("{value:?}")),
                ),
        )
        .blank_line()
        .push(NextCommand::new(format!(
            "cdf plan {}",
            resource.descriptor.resource_id
        )))
}

pub(super) fn destinations_document(report: &InspectDestinationsReport) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            "inspected destination capabilities",
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Destination")
                .row("environment", report.environment_destination.clone())
                .row("runtime", report.runtime.kind.clone()),
        );
    if let Some(capabilities) = &report.runtime.capabilities {
        let selected = capabilities.bulk_path.as_deref();
        let paths = capabilities.bulk_paths.iter().fold(
            Table::new(["path", "version", "selection", "fallback", "evidence"]),
            |table, path| {
                table.row([
                    path.path_id.clone(),
                    path.version.to_string(),
                    if selected == Some(path.path_id.as_str()) {
                        "selected".to_owned()
                    } else {
                        "available".to_owned()
                    },
                    path.fallback.to_string(),
                    path.measured_evidence_version
                        .clone()
                        .unwrap_or_else(|| "unmeasured".to_owned()),
                ])
            },
        );
        document = document.blank_line().push(paths);
    }
    document.blank_line().push(NextCommand::new("cdf plan"))
}

pub(super) fn package_document(report: &InspectPackageReport) -> RenderDocument {
    let manifest = &report.manifest;
    let (content_kind, effect_summary) = match &manifest.identity.content {
        cdf_kernel::PackageContentAuthority::Rows { .. } => ("rows", "ordinary rows".to_owned()),
        cdf_kernel::PackageContentAuthority::KeyedChanges {
            key,
            reduction,
            deletion_capture,
            delete_application,
            ..
        } => (
            "keyed_changes",
            format!(
                "{} key(s) · {} upsert(s) · {} delete(s) · capture {} · apply {:?}",
                key.fields.len(),
                reduction.surviving.upserts,
                reduction.surviving.deletes,
                if deletion_capture.enabled {
                    "enabled"
                } else {
                    "disabled"
                },
                delete_application,
            ),
        ),
    };
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "package {} status {}",
                manifest.package_hash,
                manifest.lifecycle.status.as_str()
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Package")
                .row("path", path_display(&report.path))
                .row("package", manifest.identity.package_id.to_string())
                .row("hash", manifest.package_hash.to_string())
                .row("status", manifest.lifecycle.status.as_str().to_owned())
                .row("content", content_kind)
                .row("effects", effect_summary)
                .row("files", manifest.identity.files.len().to_string())
                .row("segments", manifest.identity.segments.len().to_string()),
        )
        .blank_line()
        .push(NextCommand::new("cdf package verify"))
}

fn path_display(path: &Path) -> String {
    redact_uri_userinfo(path.display().to_string())
}

fn state_scope_display(scope: &cdf_kernel::ScopeKey) -> String {
    serde_json::to_string(scope).unwrap_or_else(|_| format!("{scope:?}"))
}
