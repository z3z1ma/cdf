use super::*;
use crate::{
    render::{
        RenderDocument,
        primitives::{KeyValuePanel, NextCommand, StatusKind, StatusLine, Table},
    },
    reports::discovery_coverage_panel,
};

pub(super) fn schema_show_document(report: &SchemaShowReport) -> RenderDocument {
    schema_snapshot_document(
        "pinned",
        &format!("showing pinned schema for {}", report.snapshot.resource_id),
        SnapshotDocumentData {
            base: &report.snapshot,
            writes: &report.writes,
            source_identity: None,
            discovery: report.discovery.as_ref(),
            unsupported: &[],
            next_command: Some(&report.next_command),
        },
    )
}

struct SnapshotDocumentData<'a> {
    base: &'a SchemaSnapshotReportBase,
    writes: &'a SchemaWrites,
    source_identity: Option<&'a BTreeMap<String, String>>,
    discovery: Option<&'a DiscoveryCoverageReport>,
    unsupported: &'a [String],
    next_command: Option<&'a str>,
}

fn schema_snapshot_document(
    label: &str,
    status: &str,
    data: SnapshotDocumentData<'_>,
) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(StatusLine::new(StatusKind::Success, status))
        .blank_line()
        .push(
            KeyValuePanel::new("Schema")
                .row("project", data.base.project.clone())
                .row("environment", data.base.environment.clone())
                .row("resource", data.base.resource_id.clone())
                .row("state", label.to_owned())
                .row("hash", data.base.schema_hash.clone())
                .row("path", data.base.schema_snapshot_path.clone())
                .row(
                    "probe",
                    data.base
                        .snapshot_metadata
                        .get("probe")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                )
                .row(
                    "normalizer",
                    data.base
                        .snapshot_metadata
                        .get("cdf:normalizer")
                        .cloned()
                        .unwrap_or_else(|| "unknown".to_owned()),
                ),
        )
        .blank_line()
        .push(field_table(&data.base.fields));

    if let Some(source_identity) = data.source_identity {
        document = document
            .blank_line()
            .push(key_value_table("Source Identity", source_identity));
    }
    if let Some(discovery) = data.discovery {
        document = document
            .blank_line()
            .push(discovery_coverage_panel(discovery));
    }
    document = document.blank_line().push(writes_panel(data.writes));
    if !data.unsupported.is_empty() {
        document = document.blank_line().push(
            data.unsupported
                .iter()
                .fold(KeyValuePanel::new("Unsupported"), |panel, reason| {
                    panel.row("lockfile reference", reason.clone())
                }),
        );
    }
    if let Some(next_command) = data.next_command {
        document = document
            .blank_line()
            .push(NextCommand::new(next_command.to_owned()));
    }
    document
}

pub(super) fn schema_diff_document(report: &SchemaDiffReport) -> RenderDocument {
    let status = if report.summary.changed {
        StatusKind::Warning
    } else {
        StatusKind::Success
    };
    let document = RenderDocument::new()
        .push(StatusLine::new(
            status,
            if report.summary.changed {
                format!("schema drift detected for {}", report.resource_id)
            } else {
                format!("schema matches fresh probe for {}", report.resource_id)
            },
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Schema Diff")
                .row("project", report.project.clone())
                .row("environment", report.environment.clone())
                .row("resource", report.resource_id.clone())
                .row("pinned hash", report.pinned_schema_hash.clone())
                .row("fresh hash", report.fresh_schema_hash.clone())
                .row("pinned path", report.pinned_schema_snapshot_path.clone())
                .row(
                    "fresh candidate path",
                    report.fresh_schema_snapshot_path.clone(),
                ),
        )
        .blank_line()
        .push(
            KeyValuePanel::summary()
                .row("added fields", report.summary.added_fields.to_string())
                .row("removed fields", report.summary.removed_fields.to_string())
                .row(
                    "type changes",
                    report.summary.type_changed_fields.to_string(),
                )
                .row(
                    "nullable changes",
                    report.summary.nullable_changed_fields.to_string(),
                )
                .row(
                    "metadata changes",
                    report.summary.metadata_changed_fields.to_string(),
                )
                .row(
                    "snapshot metadata changes",
                    report.summary.snapshot_metadata_changed.to_string(),
                ),
        )
        .blank_line()
        .push(diff_table(report));
    let document = if let Some(discovery) = &report.discovery {
        document
            .blank_line()
            .push(discovery_coverage_panel(discovery))
    } else {
        document
    };
    document.blank_line().push(writes_panel(&report.writes))
}

pub(super) fn schema_promotion_execution_document(
    report: &cdf_project::SchemaPromotionExecutionReport,
) -> RenderDocument {
    let targets = report.targets.iter().fold(
        Table::new([
            "destination",
            "target",
            "package",
            "receipt",
            "checkpoint",
            "committed",
        ]),
        |table, target| {
            table.row([
                target.destination.clone(),
                target.target.clone(),
                target.correction_package_hash.clone(),
                target
                    .receipt_id
                    .clone()
                    .unwrap_or_else(|| "pending".to_owned()),
                target
                    .checkpoint_id
                    .clone()
                    .unwrap_or_else(|| "pending".to_owned()),
                yes_no(target.committed).to_owned(),
            ])
        },
    );
    RenderDocument::new()
        .push(StatusLine::new(
            StatusKind::Success,
            format!("schema promotion complete for {}", report.resource_id),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Promotion execution")
                .row("promotion", report.promotion_id.clone())
                .row("phase", format!("{:?}", report.phase).to_lowercase())
                .row("resumed", yes_no(report.resumed))
                .row("old schema", report.old_schema_hash.clone())
                .row("new schema", report.new_schema_hash.clone())
                .row("staged plan", report.staged_plan_path.clone())
                .row("snapshot", report.snapshot_path.clone())
                .row("lock published", yes_no(report.lock_published))
                .row(
                    "publication event",
                    yes_no(report.publication_event_recorded),
                )
                .row("remaining action", report.remaining_action.clone()),
        )
        .blank_line()
        .push(targets)
        .blank_line()
        .push(NextCommand::new(report.recovery_command.clone()))
}

pub(super) fn schema_promote_document(
    report: &cdf_project::SchemaPromotionPlanReport,
) -> RenderDocument {
    let mut document = RenderDocument::new()
        .push(StatusLine::new(
            if report.executable {
                StatusKind::Success
            } else {
                StatusKind::Warning
            },
            format!(
                "promotion plan {} for {}",
                if report.executable {
                    "ready"
                } else {
                    "blocked"
                },
                report.resource_id
            ),
        ))
        .blank_line()
        .push(
            KeyValuePanel::new("Promotion")
                .row("id", report.promotion_id.clone())
                .row("old schema", report.old_schema_hash.clone())
                .row(
                    "new schema",
                    report
                        .new_schema_hash
                        .clone()
                        .unwrap_or_else(|| "blocked".to_owned()),
                )
                .row(
                    "snapshot path",
                    report
                        .new_schema_snapshot_path
                        .clone()
                        .unwrap_or_else(|| "blocked".to_owned()),
                )
                .row(
                    "fresh discovery",
                    report
                        .fresh_discovery_schema_hash
                        .clone()
                        .unwrap_or_else(|| "unavailable".to_owned()),
                )
                .row(
                    "discovery coverage",
                    report
                        .fresh_discovery_file_coverage
                        .as_ref()
                        .map(|coverage| {
                            match coverage {
                                cdf_project::DiscoveryFileCoverage::AllFiles => "all_files",
                                cdf_project::DiscoveryFileCoverage::SampledFiles => "sampled_files",
                            }
                            .to_owned()
                        })
                        .unwrap_or_else(|| "unavailable".to_owned()),
                )
                .row(
                    "discovery manifest",
                    report
                        .fresh_discovery_manifest_hash
                        .clone()
                        .unwrap_or_else(|| "unavailable".to_owned()),
                )
                .row("lock precondition", report.lock_precondition_sha256.clone()),
        );
    if !report.fresh_discovery_content_identity.is_empty() {
        document = document
            .blank_line()
            .push(report.fresh_discovery_content_identity.iter().fold(
                KeyValuePanel::new("Fresh discovery identity"),
                |panel, (key, value)| panel.row(key, value),
            ));
    }
    let mut table = Table::new(["path", "source", "observed", "count", "selected", "output"]);
    let mut path_evidence = Table::new(["path", "coercions", "packages", "address examples"]);
    for path in &report.paths {
        table = table.row([
            path.path.clone(),
            path.source_name.clone(),
            path.observed_types.join(", "),
            path.observed_count.to_string(),
            path.selected_type
                .clone()
                .unwrap_or_else(|| "required".to_owned()),
            path.output_field.clone(),
        ]);
        path_evidence = path_evidence.row([
            path.path.clone(),
            path.coercion_verdicts
                .iter()
                .map(|verdict| {
                    format!(
                        "{}→{}:{}",
                        verdict.observed_type.to_arrow().map_or_else(
                            |_| "invalid".to_owned(),
                            |data_type| data_type.to_string()
                        ),
                        verdict.selected_type.to_arrow().map_or_else(
                            |_| "invalid".to_owned(),
                            |data_type| data_type.to_string()
                        ),
                        promotion_coercion_label(verdict.decision)
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
            path.affected_packages.join(", "),
            path.affected_row_examples
                .iter()
                .map(|address| {
                    format!(
                        "{}/{}/{}",
                        address.original_package_hash,
                        address.original_segment_id,
                        address.original_row_ordinal
                    )
                })
                .collect::<Vec<_>>()
                .join(", "),
        ]);
    }
    document = document
        .blank_line()
        .push(table)
        .blank_line()
        .push(path_evidence);
    let mut evidence = Table::new(["package", "availability", "rows", "receipts"]);
    for item in &report.evidence {
        evidence = evidence.row([
            item.package_hash
                .clone()
                .unwrap_or_else(|| item.artifact_location.clone()),
            promotion_availability_label(&item.availability).to_owned(),
            item.residual_rows.to_string(),
            item.recorded_receipts.len().to_string(),
        ]);
    }
    document = document.blank_line().push(evidence);
    let mut targets = Table::new(["destination", "target", "strategy", "migrations"]);
    for target in &report.targets {
        targets = targets.row([
            target.destination.clone(),
            target.target.clone(),
            target
                .strategy
                .map(promotion_strategy_label)
                .unwrap_or_else(|| "unsupported".to_owned()),
            target.migrations.len().to_string(),
        ]);
    }
    document = document.blank_line().push(targets);
    for target in &report.targets {
        document = document.blank_line().push(
            KeyValuePanel::new(format!(
                "Target evidence {}:{}",
                target.destination, target.target
            ))
            .row("sheet hash", target.destination_sheet_hash.clone())
            .row(
                "receipt verification",
                promotion_receipt_verification_label(&target.receipt_verification),
            )
            .row("receipts", target.recorded_receipt_ids.join(", "))
            .row("packages", target.affected_packages.join(", "))
            .row("paths", target.affected_paths.join(", "))
            .row(
                "evidence availability",
                target
                    .evidence
                    .iter()
                    .map(|item| {
                        format!(
                            "{}:{}",
                            item.package_hash,
                            promotion_availability_label(&item.availability)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
        );
    }
    let mut migrations = Table::new([
        "target",
        "path",
        "output",
        "destination field",
        "mapping",
        "fidelity",
    ]);
    for target in &report.targets {
        for migration in &target.migrations {
            migrations = migrations.row([
                format!("{}:{}", target.destination, target.target),
                migration.path.clone(),
                migration.output_field.clone(),
                migration
                    .destination_field
                    .clone()
                    .unwrap_or_else(|| "blocked".to_owned()),
                format!(
                    "{} -> {}",
                    migration.arrow_type,
                    migration
                        .destination_type
                        .as_deref()
                        .unwrap_or("unsupported")
                ),
                migration
                    .fidelity
                    .as_ref()
                    .map(promotion_fidelity_label)
                    .unwrap_or("missing")
                    .to_owned(),
            ]);
        }
    }
    document = document.blank_line().push(migrations);
    let evidence_details = report.evidence.iter().filter_map(|item| {
        item.detail.as_ref().map(|detail| {
            (
                item.package_hash
                    .as_deref()
                    .unwrap_or(&item.artifact_location),
                detail.as_str(),
            )
        })
    });
    let mut details_panel = KeyValuePanel::new("Evidence constraints");
    let mut has_evidence_details = false;
    for (location, detail) in evidence_details {
        has_evidence_details = true;
        details_panel = details_panel.row(location, detail);
    }
    if has_evidence_details {
        document = document.blank_line().push(details_panel);
    }
    document = document.blank_line().push(
        KeyValuePanel::effects()
            .row("snapshot", yes_no(report.writes.schema_snapshot))
            .row("lockfile", yes_no(report.writes.lockfile))
            .row("package", yes_no(report.writes.package))
            .row("destination", yes_no(report.writes.destination))
            .row("checkpoint", yes_no(report.writes.checkpoint))
            .row("lease", yes_no(report.writes.lease))
            .row("ledger", yes_no(report.writes.ledger)),
    );
    if !report.conflicts.is_empty() {
        document = document.blank_line().push(report.conflicts.iter().fold(
            KeyValuePanel::new("Conflicts"),
            |panel, conflict| {
                panel.row(
                    &conflict.code,
                    format!("{} Fix: {}", conflict.message, conflict.remediation),
                )
            },
        ));
    }
    if !report.execution_preconditions.is_empty() {
        document =
            document
                .blank_line()
                .push(report.execution_preconditions.iter().enumerate().fold(
                    KeyValuePanel::new("Execution preconditions"),
                    |panel, (index, precondition)| {
                        panel.row(format!("{}", index + 1), precondition)
                    },
                ));
    }
    document
        .blank_line()
        .push(NextCommand::new(report.recovery_command.clone()))
}

fn promotion_availability_label(
    availability: &cdf_project::SchemaPromotionEvidenceAvailability,
) -> &'static str {
    match availability {
        cdf_project::SchemaPromotionEvidenceAvailability::RetainedPackage => "retained_package",
        cdf_project::SchemaPromotionEvidenceAvailability::DestinationReadback => {
            "destination_readback"
        }
        cdf_project::SchemaPromotionEvidenceAvailability::TombstoneOnly => "tombstone_only",
        cdf_project::SchemaPromotionEvidenceAvailability::Missing => "missing",
    }
}

fn promotion_strategy_label(strategy: cdf_kernel::CorrectionStrategy) -> String {
    match strategy {
        cdf_kernel::CorrectionStrategy::InPlaceUpdate => "in_place_update",
        cdf_kernel::CorrectionStrategy::CorrectionSidecar => "correction_sidecar",
        cdf_kernel::CorrectionStrategy::VersionedRematerialization => "versioned_rematerialization",
    }
    .to_owned()
}

fn promotion_fidelity_label(fidelity: &cdf_kernel::TypeMappingFidelity) -> &'static str {
    match fidelity {
        cdf_kernel::TypeMappingFidelity::Lossless => "lossless",
        cdf_kernel::TypeMappingFidelity::LossyRequiresContractAllowance => {
            "lossy_requires_contract_allowance"
        }
        cdf_kernel::TypeMappingFidelity::Unsupported => "unsupported",
    }
}

fn promotion_receipt_verification_label(
    verification: &cdf_project::SchemaPromotionReceiptVerification,
) -> &'static str {
    match verification {
        cdf_project::SchemaPromotionReceiptVerification::StructuralCoverageVerifiedDestinationVerificationPending => {
            "structural_coverage_verified_destination_verification_pending"
        }
    }
}

fn promotion_coercion_label(decision: cdf_contract::FieldCoercionDecision) -> &'static str {
    match decision {
        cdf_contract::FieldCoercionDecision::Preserved => "preserved",
        cdf_contract::FieldCoercionDecision::Rebound => "rebound",
        cdf_contract::FieldCoercionDecision::Widened => "widened",
        cdf_contract::FieldCoercionDecision::SourceMaterializedExact => "source_materialized_exact",
        cdf_contract::FieldCoercionDecision::CoercedByPolicy => "coerced_by_policy",
        cdf_contract::FieldCoercionDecision::LossyAllowed => "lossy_allowed",
        cdf_contract::FieldCoercionDecision::LossyRejected => "lossy_rejected",
        cdf_contract::FieldCoercionDecision::Unsupported => "unsupported",
        cdf_contract::FieldCoercionDecision::Missing => "missing",
        cdf_contract::FieldCoercionDecision::Extra => "extra",
    }
}

fn diff_table(report: &SchemaDiffReport) -> Table {
    let mut table = Table::new(["kind", "field", "before", "after"]);
    for field in &report.added_fields {
        table = table.row([
            "added".to_owned(),
            field.name.clone(),
            String::new(),
            format!("{:?}", field.data_type),
        ]);
    }
    for field in &report.removed_fields {
        table = table.row([
            "removed".to_owned(),
            field.name.clone(),
            format!("{:?}", field.data_type),
            String::new(),
        ]);
    }
    for change in &report.type_changed_fields {
        table = table.row([
            "type".to_owned(),
            change.name.clone(),
            format!("{:?}", change.before),
            format!("{:?}", change.after),
        ]);
    }
    for change in &report.nullable_changed_fields {
        table = table.row([
            "nullable".to_owned(),
            change.name.clone(),
            yes_no(change.before).to_owned(),
            yes_no(change.after).to_owned(),
        ]);
    }
    for change in &report.metadata_changed_fields {
        table = table.row([
            "metadata".to_owned(),
            change.name.clone(),
            metadata_keys(&change.before),
            metadata_keys(&change.after),
        ]);
    }
    for change in &report.snapshot_metadata_changed {
        table = table.row([
            "snapshot metadata".to_owned(),
            change.key.clone(),
            change.before.clone().unwrap_or_default(),
            change.after.clone().unwrap_or_default(),
        ]);
    }
    table
}

fn field_table(fields: &[SchemaFieldReport]) -> Table {
    fields.iter().fold(
        Table::new(["field", "type", "nullable", "source"]),
        |table, field| {
            table.row([
                field.name.clone(),
                format!("{:?}", field.data_type),
                yes_no(field.nullable).to_owned(),
                field
                    .source_name
                    .clone()
                    .unwrap_or_else(|| field.name.clone()),
            ])
        },
    )
}

fn key_value_table(title: &str, values: &BTreeMap<String, String>) -> KeyValuePanel {
    values
        .iter()
        .fold(KeyValuePanel::new(title), |panel, (key, value)| {
            panel.row(key.clone(), value.clone())
        })
}

fn writes_panel(writes: &SchemaWrites) -> KeyValuePanel {
    KeyValuePanel::effects()
        .row("schema snapshot", yes_no(writes.schema_snapshot))
        .row("lockfile", yes_no(writes.lockfile))
        .row("package", yes_no(writes.package))
        .row("destination", yes_no(writes.destination))
        .row("checkpoint", yes_no(writes.checkpoint))
}

fn metadata_keys(metadata: &BTreeMap<String, String>) -> String {
    metadata.keys().cloned().collect::<Vec<_>>().join(", ")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}
