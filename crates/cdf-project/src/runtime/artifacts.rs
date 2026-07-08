use super::prelude::*;
#[cfg(test)]
use super::types::LocalFileDuckDbRunRequest;

pub(super) fn write_run_state_commit_artifacts(
    builder: &cdf_package::PackageBuilder,
    draft: EnginePackageDraft<'_>,
    context: &StateCommitArtifactContext<'_>,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: &Option<Checkpoint>,
) -> Result<()> {
    let state_delta = state_delta_preimage_from_run_draft(
        context,
        draft.segments,
        draft.segment_positions,
        schema_hash,
        scope,
        head.as_ref(),
    )?;
    let commit_plan = DestinationCommitPlanPreimage::package_hash_token(
        context.target.clone(),
        context.descriptor.write_disposition.clone(),
        context.descriptor.merge_key.clone(),
        schema_hash.clone(),
        state_delta.segments.clone(),
    );
    builder.write_input_checkpoint_artifact(head)?;
    builder.write_state_delta_preimage_artifact(&state_delta)?;
    builder.write_commit_plan_preimage_artifact(&commit_plan)?;
    Ok(())
}

pub(super) struct StateCommitArtifactContext<'a> {
    pub(super) descriptor: &'a ResourceDescriptor,
    pub(super) schema: &'a Schema,
    pub(super) pipeline_id: &'a PipelineId,
    pub(super) checkpoint_id: &'a CheckpointId,
    pub(super) target: &'a TargetName,
}

#[cfg(test)]
pub(crate) fn state_delta_from_run(
    request: &LocalFileDuckDbRunRequest<'_>,
    output: &EngineRunOutputWithSegmentPositions,
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
) -> Result<StateDelta> {
    let schema = request.resource.schema();
    let context = StateCommitArtifactContext {
        descriptor: request.resource.descriptor(),
        schema: schema.as_ref(),
        pipeline_id: &request.pipeline_id,
        checkpoint_id: &request.checkpoint_id,
        target: &request.target,
    };
    let preimage = state_delta_preimage_from_run_draft(
        &context,
        &output.output.segments,
        &output.segment_positions,
        schema_hash,
        scope,
        head,
    )?;
    Ok(preimage.into_state_delta(PackageHash::new(
        output.output.manifest.package_hash.clone(),
    )?))
}

fn state_delta_preimage_from_run_draft(
    context: &StateCommitArtifactContext<'_>,
    segments: &[SegmentEntry],
    segment_positions: &[cdf_engine::EngineSegmentPosition],
    schema_hash: &SchemaHash,
    scope: &ScopeKey,
    head: Option<&Checkpoint>,
) -> Result<StateDeltaPreimage> {
    let positions = segment_positions_by_id(segments, segment_positions)?;
    let mut segment_evidence = Vec::with_capacity(segments.len());

    for segment in segments {
        let segment_position = positions
            .get(&segment.segment_id)
            .ok_or_else(|| {
                CdfError::internal(format!(
                    "engine output omitted source position evidence for segment {}",
                    segment.segment_id
                ))
            })?
            .clone()
            .ok_or_else(|| {
                CdfError::data(format!(
                    "package segment {} has no source position evidence; cdf run cannot checkpoint without source position evidence",
                    segment.segment_id
                ))
            })?;
        let segment_position = normalize_source_position_for_scope(segment_position, scope);
        segment_evidence.push((segment, segment_position));
    }

    let output_positions = segment_evidence
        .iter()
        .map(|(_, position)| position.clone())
        .collect::<Vec<_>>();
    let output_position = aggregate_output_position(context, &output_positions)?;
    let state_segments = segment_evidence
        .into_iter()
        .map(|(segment, segment_position)| StateSegment {
            segment_id: segment.segment_id.clone(),
            scope: scope.clone(),
            output_position: segment_position,
            row_count: segment.row_count,
            byte_count: segment.byte_count,
        })
        .collect();
    Ok(StateDeltaPreimage {
        checkpoint_id: context.checkpoint_id.clone(),
        pipeline_id: context.pipeline_id.clone(),
        resource_id: context.descriptor.resource_id.clone(),
        scope: scope.clone(),
        state_version: CHECKPOINT_STATE_VERSION,
        parent_checkpoint_id: head.map(|checkpoint| checkpoint.delta.checkpoint_id.clone()),
        input_position: head.map(|checkpoint| checkpoint.delta.output_position.clone()),
        output_position,
        schema_hash: schema_hash.clone(),
        segments: state_segments,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CursorArithmetic {
    I64,
    U64,
    TimestampMicros,
    Date32,
}

fn aggregate_output_position(
    context: &StateCommitArtifactContext<'_>,
    positions: &[SourcePosition],
) -> Result<SourcePosition> {
    if positions.is_empty() {
        return Err(CdfError::data(
            "package execution produced no output segments to checkpoint",
        ));
    }
    if context.descriptor.cursor.is_some() {
        aggregate_cursor_output_position(context, positions)
    } else {
        identical_output_position(positions)
    }
}

fn identical_output_position(positions: &[SourcePosition]) -> Result<SourcePosition> {
    let first = positions
        .first()
        .expect("aggregate_output_position checks non-empty positions");
    if positions.iter().any(|position| position != first) {
        return Err(CdfError::data(
            "single resource run produced divergent segment source positions",
        ));
    }
    Ok(first.clone())
}

fn aggregate_cursor_output_position(
    context: &StateCommitArtifactContext<'_>,
    positions: &[SourcePosition],
) -> Result<SourcePosition> {
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("aggregate_output_position routes only cursor descriptors");
    if cursor.ordering == CursorOrderingClaim::Unordered {
        return Err(CdfError::contract(format!(
            "resource `{}` cursor field `{}` is unordered and cannot advance checkpoints",
            context.descriptor.resource_id, cursor.field
        )));
    }

    let arithmetic = cursor_arithmetic(context)?;
    let cursor_positions = cursor_positions_for_aggregation(context, positions)?;
    let mut max_position = None::<&CursorPosition>;
    for position in cursor_positions {
        if position.field != cursor.field {
            return Err(CdfError::data(format!(
                "source position cursor field `{}` does not match resource cursor field `{}`",
                position.field, cursor.field
            )));
        }
        ensure_cursor_value_supported(context, arithmetic, &position.value)?;
        if max_position
            .is_none_or(|current| cursor_value_greater(arithmetic, &position.value, &current.value))
        {
            max_position = Some(position);
        }
    }

    let max_position =
        max_position.expect("cursor_positions_for_aggregation returns one cursor per position");
    let closed_value = close_cursor_value(
        context,
        arithmetic,
        &max_position.value,
        cursor.lag_tolerance_ms,
    )?;
    Ok(SourcePosition::Cursor(CursorPosition {
        version: max_position.version,
        field: cursor.field.clone(),
        value: closed_value,
    }))
}

fn cursor_arithmetic(context: &StateCommitArtifactContext<'_>) -> Result<CursorArithmetic> {
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("cursor_arithmetic is called only for cursor descriptors");
    let field = context.schema.field_with_name(&cursor.field).map_err(|_| {
        CdfError::contract(format!(
            "resource `{}` cursor field `{}` is missing from the declared schema",
            context.descriptor.resource_id, cursor.field
        ))
    })?;
    match field.data_type() {
        DataType::Int64 => Ok(CursorArithmetic::I64),
        DataType::UInt64 => Ok(CursorArithmetic::U64),
        DataType::Timestamp(
            TimeUnit::Second | TimeUnit::Millisecond | TimeUnit::Microsecond | TimeUnit::Nanosecond,
            _,
        ) => Ok(CursorArithmetic::TimestampMicros),
        DataType::Date32 => Ok(CursorArithmetic::Date32),
        other => Err(CdfError::contract(format!(
            "resource `{}` cursor field `{}` has unsupported cursor value kind {other}; only int64, uint64, timestamp, and date32 cursors have ratified window-close semantics",
            context.descriptor.resource_id, cursor.field
        ))),
    }
}

fn cursor_positions_for_aggregation<'a>(
    context: &StateCommitArtifactContext<'_>,
    positions: &'a [SourcePosition],
) -> Result<Vec<&'a CursorPosition>> {
    let mut cursor_positions = Vec::with_capacity(positions.len());
    let mut saw_cursor = false;
    let mut saw_page_token = false;
    let mut saw_non_cursor_variant = false;

    for position in positions {
        match position {
            SourcePosition::Cursor(cursor) => {
                saw_cursor = true;
                cursor_positions.push(cursor);
            }
            SourcePosition::PageToken(_) => saw_page_token = true,
            SourcePosition::Composite(composite) => {
                saw_non_cursor_variant = true;
                let summary = composite_position_summary(composite);
                saw_cursor |= summary.saw_cursor;
                saw_page_token |= summary.saw_page_token;
            }
            SourcePosition::Log(_)
            | SourcePosition::FileManifest(_)
            | SourcePosition::ForeignState(_) => saw_non_cursor_variant = true,
        }
    }

    if saw_page_token && saw_cursor {
        return Err(CdfError::data(format!(
            "resource `{}` produced mixed cursor/page-token source positions; mixed pagination transport and checkpoint cursor semantics are not ratified",
            context.descriptor.resource_id
        )));
    }
    if saw_page_token {
        return Err(CdfError::data(format!(
            "resource `{}` produced page-token-only source positions; page tokens are pagination transport and cannot advance checkpoints",
            context.descriptor.resource_id
        )));
    }
    if saw_non_cursor_variant || cursor_positions.len() != positions.len() {
        return Err(CdfError::data(format!(
            "resource `{}` produced divergent source-position variants; non-file checkpoint advancement requires cursor positions only",
            context.descriptor.resource_id
        )));
    }

    Ok(cursor_positions)
}

#[derive(Clone, Copy, Debug, Default)]
struct PositionSummary {
    saw_cursor: bool,
    saw_page_token: bool,
}

fn composite_position_summary(composite: &cdf_kernel::CompositePosition) -> PositionSummary {
    let mut summary = PositionSummary::default();
    for position in composite.positions.values() {
        match position {
            SourcePosition::Cursor(_) => summary.saw_cursor = true,
            SourcePosition::PageToken(_) => summary.saw_page_token = true,
            SourcePosition::Composite(nested) => {
                let nested = composite_position_summary(nested);
                summary.saw_cursor |= nested.saw_cursor;
                summary.saw_page_token |= nested.saw_page_token;
            }
            SourcePosition::Log(_)
            | SourcePosition::FileManifest(_)
            | SourcePosition::ForeignState(_) => {}
        }
    }
    summary
}

fn ensure_cursor_value_supported(
    context: &StateCommitArtifactContext<'_>,
    arithmetic: CursorArithmetic,
    value: &CursorValue,
) -> Result<()> {
    let supported = matches!(
        (arithmetic, value),
        (
            CursorArithmetic::I64 | CursorArithmetic::Date32,
            CursorValue::I64(_)
        ) | (CursorArithmetic::U64, CursorValue::U64(_))
            | (
                CursorArithmetic::TimestampMicros,
                CursorValue::TimestampMicros { .. }
            )
    );
    if supported {
        return Ok(());
    }
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("ensure_cursor_value_supported is called only for cursor descriptors");
    Err(CdfError::data(format!(
        "resource `{}` cursor field `{}` produced unsupported cursor value kind for its declared schema",
        context.descriptor.resource_id, cursor.field
    )))
}

fn cursor_value_greater(
    arithmetic: CursorArithmetic,
    left: &CursorValue,
    right: &CursorValue,
) -> bool {
    match (arithmetic, left, right) {
        (
            CursorArithmetic::I64 | CursorArithmetic::Date32,
            CursorValue::I64(left),
            CursorValue::I64(right),
        ) => left > right,
        (CursorArithmetic::U64, CursorValue::U64(left), CursorValue::U64(right)) => left > right,
        (
            CursorArithmetic::TimestampMicros,
            CursorValue::TimestampMicros { micros: left, .. },
            CursorValue::TimestampMicros { micros: right, .. },
        ) => left > right,
        _ => false,
    }
}

fn close_cursor_value(
    context: &StateCommitArtifactContext<'_>,
    arithmetic: CursorArithmetic,
    value: &CursorValue,
    lag_tolerance_ms: u64,
) -> Result<CursorValue> {
    match (arithmetic, value) {
        (CursorArithmetic::I64, CursorValue::I64(value)) => {
            let lag = i64::try_from(lag_tolerance_ms)
                .map_err(|_| incompatible_cursor_lag(context, lag_tolerance_ms))?;
            value
                .checked_sub(lag)
                .map(CursorValue::I64)
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))
        }
        (CursorArithmetic::U64, CursorValue::U64(value)) => value
            .checked_sub(lag_tolerance_ms)
            .map(CursorValue::U64)
            .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms)),
        (CursorArithmetic::TimestampMicros, CursorValue::TimestampMicros { micros, timezone }) => {
            let lag_micros = lag_tolerance_ms
                .checked_mul(1_000)
                .and_then(|value| i64::try_from(value).ok())
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))?;
            micros
                .checked_sub(lag_micros)
                .map(|micros| CursorValue::TimestampMicros {
                    micros,
                    timezone: timezone.clone(),
                })
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))
        }
        (CursorArithmetic::Date32, CursorValue::I64(value)) => {
            const MILLIS_PER_DAY: u64 = 86_400_000;
            if !lag_tolerance_ms.is_multiple_of(MILLIS_PER_DAY) {
                return Err(incompatible_cursor_lag(context, lag_tolerance_ms));
            }
            let lag_days = i64::try_from(lag_tolerance_ms / MILLIS_PER_DAY)
                .map_err(|_| incompatible_cursor_lag(context, lag_tolerance_ms))?;
            value
                .checked_sub(lag_days)
                .map(CursorValue::I64)
                .ok_or_else(|| incompatible_cursor_lag(context, lag_tolerance_ms))
        }
        _ => Err(incompatible_cursor_lag(context, lag_tolerance_ms)),
    }
}

fn incompatible_cursor_lag(
    context: &StateCommitArtifactContext<'_>,
    lag_tolerance_ms: u64,
) -> CdfError {
    let cursor = context
        .descriptor
        .cursor
        .as_ref()
        .expect("incompatible_cursor_lag is called only for cursor descriptors");
    CdfError::data(format!(
        "resource `{}` cursor field `{}` has incompatible cursor lag {}ms for the observed cursor value",
        context.descriptor.resource_id, cursor.field, lag_tolerance_ms
    ))
}

fn normalize_source_position_for_scope(
    position: SourcePosition,
    scope: &ScopeKey,
) -> SourcePosition {
    match (scope, position) {
        (ScopeKey::File { path }, SourcePosition::FileManifest(mut manifest)) => {
            for file in &mut manifest.files {
                file.path = path.clone();
            }
            SourcePosition::FileManifest(manifest)
        }
        (_, position) => position,
    }
}

fn segment_positions_by_id(
    segments: &[SegmentEntry],
    segment_positions: &[cdf_engine::EngineSegmentPosition],
) -> Result<BTreeMap<SegmentId, Option<SourcePosition>>> {
    if segment_positions.len() != segments.len() {
        return Err(CdfError::internal(format!(
            "engine output has {} segment(s) but {} segment source position record(s)",
            segments.len(),
            segment_positions.len()
        )));
    }

    let positions = segment_positions
        .iter()
        .map(|position| {
            (
                position.segment_id.clone(),
                position.output_position.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    if positions.len() != segment_positions.len() {
        return Err(CdfError::internal(
            "engine output contains duplicate segment source position records",
        ));
    }
    Ok(positions)
}
