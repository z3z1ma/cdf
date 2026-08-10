use arrow_schema::Schema;
use cdf_http::SecretUri;
use cdf_kernel::{DestinationProtocol, ResourceStream, Result, SchemaHash};
use cdf_package_contract::{PackageReplayInputs, VerifiedPackageAccess};
use cdf_runtime::{
    DestinationCommitPlanningInputs, DestinationCommitPlanningOutcome, DestinationDescription,
    DestinationDriver, DestinationHealthProbe, DestinationHealthResult, DestinationHealthStatus,
    DestinationIngressMode, DestinationInspection, DestinationPlanningContext,
    DestinationReceiptReportingPolicy, DestinationResolutionContext, DestinationRuntime,
    DestinationRuntimeCapabilities, DestinationWriterModel, PreparedDestinationCommit,
    artifact_hash,
};
use std::collections::BTreeMap;
use url::Url;

use crate::{
    client::ClickHouseConnectionOptions,
    identifier::ClickHouseIdentifier,
    mapping::columns_for_schema,
    models::{
        ClickHouseCommitRequest, ClickHouseDestination, ClickHouseLoadPlanInput,
        ClickHouseMergeMode,
    },
    package::expected_segments_for_session,
    session::{ClickHouseCommitSession, validate_session_begin_inputs},
};

pub struct ClickHouseRuntimeDriver;

const CLICKHOUSE_SCHEMES: &[&str] = &["clickhouse", "clickhouses"];

impl DestinationDriver for ClickHouseRuntimeDriver {
    fn schemes(&self) -> &'static [&'static str] {
        CLICKHOUSE_SCHEMES
    }

    fn inspect(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<DestinationInspection> {
        validate_uri_reference(uri)?;
        let destination = ClickHouseDestination::new()?;
        let sheet_artifact = destination.sheet_artifact()?;
        Ok(DestinationInspection {
            description: destination_description(&destination),
            sheet_artifact_hash: artifact_hash(&sheet_artifact)?,
            sheet_artifact,
            runtime: clickhouse_runtime_capabilities(),
            health_probes: vec![DestinationHealthProbe {
                probe_id: "catalog".to_owned(),
                description: "connect and inspect ClickHouse database/table capabilities"
                    .to_owned(),
                requires_credentials: true,
                mutates_destination: false,
            }],
        })
    }

    fn resolve(
        &self,
        uri: &str,
        context: &DestinationResolutionContext<'_>,
    ) -> Result<Box<dyn DestinationRuntime>> {
        let (raw, secret_redaction, allow_credentials) = if uri.starts_with("secret://") {
            let secret = SecretUri::new(uri.to_owned())?;
            let value = context
                .secret_provider()?
                .resolve(&secret)?
                .as_str()?
                .to_owned();
            (value.clone(), Some(value), true)
        } else {
            (uri.to_owned(), None, false)
        };
        let connection = parse_uri(&raw, allow_credentials)?;
        for (key, _) in context.policy_entries(crate::CLICKHOUSE_DESTINATION_ID) {
            if key != "merge_mode" {
                return Err(cdf_kernel::CdfError::contract(format!(
                    "ClickHouse destination policy key `{key}` is unsupported"
                )));
            }
        }
        let target = ClickHouseIdentifier::user(context.target()?.as_str())?;
        let merge_mode = ClickHouseMergeMode::parse(
            context.optional_policy_value(crate::CLICKHOUSE_DESTINATION_ID, "merge_mode"),
        )?;
        Ok(Box::new(ClickHouseRuntime {
            destination: ClickHouseDestination::for_runtime(
                connection,
                target,
                secret_redaction,
                merge_mode,
            )?,
        }))
    }

    fn health(
        &self,
        uri: &str,
        _context: &DestinationResolutionContext<'_>,
    ) -> Result<Vec<DestinationHealthResult>> {
        validate_uri_reference(uri)?;
        Ok(vec![DestinationHealthResult {
            probe_id: "catalog".to_owned(),
            status: DestinationHealthStatus::Passed,
            message: "ClickHouse destination capability probe is available at run preflight"
                .to_owned(),
            details: Default::default(),
        }])
    }

    fn replay_target(&self, target: &str) -> Result<cdf_kernel::TargetName> {
        cdf_kernel::TargetName::new(ClickHouseIdentifier::user(target)?.as_str())
    }
}

pub(crate) struct ClickHouseRuntime {
    destination: ClickHouseDestination,
}

impl DestinationRuntime for ClickHouseRuntime {
    fn protocol(&self) -> &dyn DestinationProtocol {
        &self.destination
    }

    fn ingress(&mut self) -> cdf_runtime::DestinationIngress<'_> {
        cdf_runtime::DestinationIngress::FinalizedPackage(self)
    }

    fn bind_execution_services(
        &mut self,
        execution: &cdf_runtime::ExecutionServices,
    ) -> Result<()> {
        self.destination = self
            .destination
            .clone()
            .with_execution_services(Some(execution.clone()));
        Ok(())
    }

    fn describe(&self) -> DestinationDescription {
        destination_description(&self.destination)
    }

    fn runtime_capabilities(&self) -> DestinationRuntimeCapabilities {
        clickhouse_runtime_capabilities()
    }

    fn commit_policy(
        &self,
        disposition: &cdf_kernel::WriteDisposition,
    ) -> BTreeMap<String, String> {
        clickhouse_commit_policy(disposition, self.destination.merge_mode)
    }

    fn prepare_bulk_paths(
        &mut self,
        input: &cdf_runtime::BulkPathPreparationInput<'_>,
    ) -> Result<cdf_runtime::BulkPathPreparation> {
        columns_for_schema(input.output_schema)?;
        cdf_runtime::BulkPathPreparation::from_capabilities(&self.runtime_capabilities())
    }

    fn validate_run_preflight(
        &mut self,
        resource: &dyn ResourceStream,
        output_schema: &Schema,
        _schema_hash: &SchemaHash,
    ) -> Result<()> {
        let columns = columns_for_schema(output_schema)?;
        validate_merge_keys(
            &resource.descriptor().write_disposition,
            &resource.descriptor().merge_key,
            &columns,
        )
    }

    fn plan_resource_commit(
        &mut self,
        resource: &dyn ResourceStream,
        output_schema: &Schema,
        inputs: &DestinationCommitPlanningInputs,
    ) -> Result<DestinationCommitPlanningOutcome> {
        let load_plan = self.destination.plan_load(load_plan_input(
            &PackageReplayInputs {
                input_checkpoint: None,
                state_delta: inputs.state_delta.clone(),
                destination_commit: inputs.destination_commit.clone(),
                schema_hash: inputs.schema_hash.clone(),
                merge_keys: resource.descriptor().merge_key.clone(),
                destination_policy: self.commit_policy(&inputs.destination_commit.disposition),
                run_schema_authority: None,
            },
            &self.destination,
            columns_for_schema(output_schema)?,
        )?)?;
        Ok(DestinationCommitPlanningOutcome::new(
            self.destination.sheet().clone(),
            load_plan.kernel,
        ))
    }

    fn secret_redaction(&self) -> Option<&str> {
        self.destination.secret_redaction.as_deref()
    }
}

impl cdf_runtime::FinalizedPackageIngress for ClickHouseRuntime {
    fn prepare_package_commit(
        &mut self,
        inputs: &PackageReplayInputs,
        context: &DestinationPlanningContext<'_>,
    ) -> Result<PreparedDestinationCommit> {
        let expected_policy = self.commit_policy(&inputs.destination_commit.disposition);
        if inputs.destination_policy != expected_policy {
            return Err(cdf_kernel::CdfError::contract(format!(
                "ClickHouse destination policy recorded in the package differs from the resolved runtime: package={:?}, runtime={expected_policy:?}",
                inputs.destination_policy
            )));
        }
        self.runtime_capabilities()
            .validate_prepared_bulk_path(context.bulk_path)?;
        let columns = columns_from_package(context.verified_package.as_ref())?;
        validate_merge_dedup_authority(inputs, context.verified_package.as_ref())?;
        let load_plan =
            self.destination
                .plan_load(load_plan_input(inputs, &self.destination, columns)?)?;
        let segments = expected_segments_for_session(
            context.verified_package.as_ref(),
            &load_plan,
            &inputs.destination_commit,
        )?;
        let request = ClickHouseCommitRequest {
            plan: load_plan.clone(),
            segments,
        };
        Ok(PreparedDestinationCommit::from_verified_inputs(
            inputs,
            load_plan.kernel,
            context.bulk_path.clone(),
            DestinationReceiptReportingPolicy::DestinationCommitReceiptOnly,
        )?
        .with_pending_context(request))
    }

    fn begin_prepared_commit(
        &mut self,
        prepared: &mut PreparedDestinationCommit,
    ) -> Result<Box<dyn cdf_kernel::CommitSession + '_>> {
        let request = prepared.take_pending_context::<ClickHouseCommitRequest>("ClickHouse")?;
        validate_session_begin_inputs(prepared.commit(), prepared.plan(), &request.plan)?;
        let execution = self.destination.execution.clone().ok_or_else(|| {
            cdf_kernel::CdfError::contract("ClickHouse commit requires injected ExecutionServices")
        })?;
        let connection = self.destination.connection.clone().ok_or_else(|| {
            cdf_kernel::CdfError::internal("ClickHouse runtime has no resolved connection")
        })?;
        Ok(Box::new(ClickHouseCommitSession::new(
            connection,
            execution,
            self.destination.client.clone(),
            request,
        )))
    }
}

fn clickhouse_commit_policy(
    disposition: &cdf_kernel::WriteDisposition,
    merge_mode: ClickHouseMergeMode,
) -> BTreeMap<String, String> {
    if *disposition == cdf_kernel::WriteDisposition::Merge {
        [("merge_mode".to_owned(), merge_mode.as_str().to_owned())]
            .into_iter()
            .collect()
    } else {
        BTreeMap::new()
    }
}

fn validate_merge_dedup_authority(
    inputs: &PackageReplayInputs,
    package: &dyn VerifiedPackageAccess,
) -> Result<()> {
    if inputs.destination_commit.disposition != cdf_kernel::WriteDisposition::Merge {
        return Ok(());
    }
    let summary = package.verified_dedup_summary()?.ok_or_else(|| {
        cdf_kernel::CdfError::contract(
            "ClickHouse merge requires identity-bound package dedup authority",
        )
    })?;
    let package_rows = inputs
        .state_delta
        .segments
        .iter()
        .try_fold(0_u64, |rows, segment| rows.checked_add(segment.row_count))
        .ok_or_else(|| cdf_kernel::CdfError::data("ClickHouse package row count overflowed"))?;
    if summary.keys != inputs.merge_keys || summary.output_rows != package_rows {
        return Err(cdf_kernel::CdfError::contract(
            "ClickHouse merge package dedup authority does not match its merge keys and canonical rows",
        ));
    }
    Ok(())
}

fn load_plan_input(
    inputs: &PackageReplayInputs,
    destination: &ClickHouseDestination,
    columns: Vec<crate::mapping::ClickHouseColumn>,
) -> Result<ClickHouseLoadPlanInput> {
    let target = destination
        .target
        .clone()
        .ok_or_else(|| cdf_kernel::CdfError::internal("ClickHouse runtime has no target"))?;
    if target.as_str() != inputs.destination_commit.target.as_str() {
        return Err(cdf_kernel::CdfError::contract(
            "explicit ClickHouse replay target does not match package destination commit target",
        ));
    }
    let merge_keys = inputs
        .merge_keys
        .iter()
        .map(ClickHouseIdentifier::user)
        .collect::<Result<Vec<_>>>()?;
    validate_merge_keys(
        &inputs.destination_commit.disposition,
        &inputs.merge_keys,
        &columns,
    )?;
    Ok(ClickHouseLoadPlanInput {
        package_hash: inputs.state_delta.package_hash.clone(),
        content: inputs.destination_commit.content.clone(),
        idempotency_token: inputs.destination_commit.idempotency_token.clone(),
        target,
        disposition: inputs.destination_commit.disposition.clone(),
        schema_hash: inputs.schema_hash.clone(),
        segments: inputs.state_delta.segments.clone(),
        columns,
        merge_keys,
        merge_mode: destination.merge_mode,
        resource_id: Some(inputs.state_delta.resource_id.clone()),
        state_delta: Some(inputs.state_delta.clone()),
    })
}

fn validate_merge_keys(
    disposition: &cdf_kernel::WriteDisposition,
    merge_keys: &[String],
    columns: &[crate::mapping::ClickHouseColumn],
) -> Result<()> {
    if disposition != &cdf_kernel::WriteDisposition::Merge {
        if merge_keys.is_empty() {
            return Ok(());
        }
        return Err(cdf_kernel::CdfError::contract(
            "ClickHouse merge keys are valid only for merge disposition",
        ));
    }
    if merge_keys.is_empty() {
        return Err(cdf_kernel::CdfError::contract(
            "ClickHouse merge requires at least one normalized merge key",
        ));
    }
    let mut distinct = std::collections::BTreeSet::new();
    for key in merge_keys {
        let key = ClickHouseIdentifier::user(key)?;
        if !distinct.insert(key.clone()) {
            return Err(cdf_kernel::CdfError::contract(format!(
                "ClickHouse merge key {} is declared more than once",
                key
            )));
        }
        let column = columns
            .iter()
            .find(|column| column.name == key)
            .ok_or_else(|| {
                cdf_kernel::CdfError::contract(format!(
                    "ClickHouse merge key {} is absent from the output schema",
                    key
                ))
            })?;
        if column.nullable {
            return Err(cdf_kernel::CdfError::contract(format!(
                "ClickHouse merge key {} must be non-nullable",
                key
            )));
        }
    }
    Ok(())
}

fn columns_from_package(
    package: &dyn VerifiedPackageAccess,
) -> Result<Vec<crate::mapping::ClickHouseColumn>> {
    let schema = package.runtime_arrow_schema()?;
    columns_for_schema(schema.as_ref())
}

fn destination_description(destination: &ClickHouseDestination) -> DestinationDescription {
    DestinationDescription::new(
        destination.sheet().destination.clone(),
        CLICKHOUSE_SCHEMES,
        "clickhouse",
    )
    .with_product_location_field("database")
}

pub(crate) fn clickhouse_runtime_capabilities() -> DestinationRuntimeCapabilities {
    DestinationRuntimeCapabilities {
        blocking_lanes: Vec::new(),
        staged_ingress_lane: None,
        final_binding_lane: None,
        ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
        staged_ingress: None,
        writer_model: DestinationWriterModel::SingleWriter,
        commit_payload_mode: cdf_runtime::DestinationCommitPayloadMode::SegmentStreaming,
        max_in_flight_segments: Some(1),
        max_in_flight_bytes: Some(crate::client::ARROW_WRITER_BYTES),
        bulk_paths: vec![cdf_runtime::BulkPathDescriptor {
            path_id: "arrowstream".to_owned(),
            version: 1,
            ingress_mode: DestinationIngressMode::FinalizedPackageOnly,
            writer_model: DestinationWriterModel::SingleWriter,
            ordering: cdf_runtime::BulkOrdering::ManifestOrder,
            rows: cdf_runtime::BulkSizeRange {
                minimum: 1_000,
                preferred: 65_536,
                maximum: 1_000_000,
            },
            bytes: cdf_runtime::BulkSizeRange {
                minimum: 1024 * 1024,
                preferred: 16 * 1024 * 1024,
                maximum: crate::client::MAXIMUM_INPUT_BATCH_BYTES,
            },
            batch_mode: cdf_runtime::BulkBatchMode::PassThrough,
            maximum_writers: 1,
            blocking_lane: None,
            native_internal_parallelism: 1,
            external_staging: false,
            fallback: cdf_runtime::BulkFallbackMode::Forbidden,
            schema_preflight_version: "clickhouse-arrow-mapping@1".to_owned(),
            evidence: cdf_runtime::BulkPathEvidence::Measured {
                version: "clickhouse-destination-roofline-v1".to_owned(),
            },
        }],
        bulk_path: Some("arrowstream".to_owned()),
        replay_requires_explicit_target: true,
        replay_target_hint: Some("table".to_owned()),
    }
}

fn validate_uri_reference(uri: &str) -> Result<()> {
    if uri.starts_with("secret://") {
        SecretUri::new(uri.to_owned()).map(drop)
    } else {
        parse_uri(uri, false).map(drop)
    }
}

fn parse_uri(uri: &str, allow_credentials: bool) -> Result<ClickHouseConnectionOptions> {
    let mut parsed = Url::parse(uri).map_err(|error| {
        cdf_kernel::CdfError::contract(format!("ClickHouse destination URI is invalid: {error}"))
    })?;
    if !matches!(parsed.scheme(), "clickhouse" | "clickhouses")
        || parsed.host_str().is_none()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(cdf_kernel::CdfError::contract(
            "ClickHouse destination URI must be clickhouse:// or clickhouses:// with one database path and no query or fragment",
        ));
    }
    let database = parsed
        .path_segments()
        .map(|segments| {
            segments
                .filter(|segment| !segment.is_empty())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if database.len() != 1 {
        return Err(cdf_kernel::CdfError::contract(
            "ClickHouse destination URI must end with exactly one /database path",
        ));
    }
    let database = ClickHouseIdentifier::user(database[0])?;
    let has_credentials = !parsed.username().is_empty() || parsed.password().is_some();
    if has_credentials && !allow_credentials {
        return Err(cdf_kernel::CdfError::auth(
            "ClickHouse destination credentials must be supplied through a secret:// URI",
        ));
    }
    let username = (!parsed.username().is_empty()).then(|| parsed.username().to_owned());
    let password = parsed.password().map(str::to_owned);
    parsed
        .set_username("")
        .map_err(|()| cdf_kernel::CdfError::contract("clear ClickHouse URI username"))?;
    parsed
        .set_password(None)
        .map_err(|()| cdf_kernel::CdfError::contract("clear ClickHouse URI password"))?;
    parsed.set_path("");
    let operational = parsed
        .as_str()
        .strip_prefix("clickhouse://")
        .map(|authority| format!("http://{authority}"))
        .or_else(|| {
            parsed
                .as_str()
                .strip_prefix("clickhouses://")
                .map(|authority| format!("https://{authority}"))
        })
        .ok_or_else(|| {
            cdf_kernel::CdfError::contract("normalize ClickHouse destination URI scheme")
        })?;
    let parsed = Url::parse(&operational).map_err(|error| {
        cdf_kernel::CdfError::contract(format!(
            "normalized ClickHouse destination endpoint is invalid: {error}"
        ))
    })?;
    Ok(ClickHouseConnectionOptions {
        endpoint: parsed.to_string().trim_end_matches('/').to_owned(),
        database,
        username,
        password,
    })
}

#[cfg(test)]
pub(crate) fn parse_uri_for_test(
    uri: &str,
    allow_credentials: bool,
) -> Result<ClickHouseConnectionOptions> {
    parse_uri(uri, allow_credentials)
}
