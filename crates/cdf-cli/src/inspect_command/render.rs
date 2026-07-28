use std::path::Path;

use cdf_package_contract::PackageManifest;

use super::*;
use crate::{
    context::DestinationRuntime,
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
        redaction::redact_uri_userinfo,
    },
};

pub(super) fn project_document(context: &ProjectContext) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "project {} env {}",
                context.config.project.name, context.environment.name
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Project")
                .row("root", path_display(&context.root))
                .row("name", context.config.project.name.clone())
                .row("environment", context.environment.name.clone())
                .row("resources", context.resources.len().to_string())
                .row(
                    "destination",
                    redact_uri_userinfo(&context.environment.destination),
                ),
        )
        .blank_line()
        .push(NextCommand::new("cdf inspect resources"))
}

pub(super) fn resources_document(resources: &[ResourceSummary]) -> RenderDocument {
    let table = resources.iter().fold(
        Table::new([
            "compiled id",
            "source",
            "resource",
            "source file",
            "mapping",
        ]),
        |table, resource| {
            table.row([
                resource.descriptor.resource_id.to_string(),
                resource.source_name.clone(),
                resource.resource_name.clone(),
                resource
                    .source_file
                    .clone()
                    .unwrap_or_else(|| "n/a".to_owned()),
                mapping_display(resource),
            ])
        },
    );

    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("{} compiled resource(s)", resources.len()),
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
                .row("source", resource.source_name.clone())
                .row("resource", resource.resource_name.clone())
                .row(
                    "source file",
                    resource
                        .source_file
                        .clone()
                        .unwrap_or_else(|| "n/a".to_owned()),
                )
                .row("mapping", mapping_display(resource))
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

fn mapping_display(resource: &ResourceSummary) -> String {
    match (&resource.mapping_status, &resource.mapping_pattern) {
        (Some(status), Some(pattern)) => format!("{status} {pattern}"),
        (Some(status), None) => status.clone(),
        (None, Some(pattern)) => pattern.clone(),
        (None, None) => "n/a".to_owned(),
    }
}

pub(super) fn lock_document(lock: &cdf_project::CdfLock) -> RenderDocument {
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!(
                "lockfile v{} for project {}",
                lock.version, lock.project.name
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Lock")
                .row("version", lock.version.to_string())
                .row("project", lock.project.name.clone())
                .row("default env", lock.project.default_environment.clone())
                .row("resources", lock.resources.len().to_string())
                .row("destinations", lock.destinations.len().to_string()),
        )
        .blank_line()
        .push(NextCommand::new("cdf validate"))
}

pub(super) fn destinations_document(
    context: &ProjectContext,
    runtime: &DestinationRuntime,
) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            "inspected destination capabilities",
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Destination")
                .row(
                    "environment",
                    redact_uri_userinfo(&context.environment.destination),
                )
                .row("runtime", runtime.kind.clone())
                .row(
                    "locked",
                    context
                        .lock
                        .as_ref()
                        .map(|lock| lock.destinations.len().to_string())
                        .unwrap_or_else(|| "none".to_owned()),
                ),
        );
    if let Some(capabilities) = &runtime.capabilities {
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

pub(super) fn package_document(path: &Path, manifest: &PackageManifest) -> RenderDocument {
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
                .row("path", path_display(path))
                .row("package", manifest.identity.package_id.to_string())
                .row("hash", manifest.package_hash.to_string())
                .row("status", manifest.lifecycle.status.as_str().to_owned())
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
