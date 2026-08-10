use std::collections::BTreeMap;

use cdf_http::SecretUri;
use cdf_kernel::{CanonicalArrowSchema, CdfError, ResourceId, Result};
use cdf_runtime::{
    SourceCatalogCandidate, SourceCatalogDiscoverer, SourceCatalogDiscovery, SourceCatalogRequest,
    SourceDriver, SourceResolutionContext,
};
use mysql_async::{Conn, Opts, Row, prelude::Queryable};

use crate::{
    driver::{MySqlSourceDriver, MySqlSourceOptions, decode_options},
    identifier::{MySqlIdentifier, MySqlTarget},
    resource::apply_session_options,
    schema::schema_from_columns,
};

impl SourceCatalogDiscoverer for MySqlSourceDriver {
    fn discover_catalog(
        &self,
        request: &SourceCatalogRequest,
        context: &SourceResolutionContext<'_>,
    ) -> Result<SourceCatalogDiscovery> {
        request.validate()?;
        let source: MySqlSourceOptions =
            decode_options("MySQL source", request.source_options.clone())?;
        let options = source.native_options()?;
        let reference = SecretUri::new(source.connection)?;
        let connection = context.secret_provider().resolve(&reference)?;
        let connection = connection.as_str()?.to_owned();
        let maximum = request.maximum_candidates;
        let egress = context.egress_scope(&self.descriptor().driver_id);
        let tables = context.execution().run_io(async move {
            egress.authorize(&connection)?;
            let opts = Opts::from_url(&connection)
                .map_err(|_| CdfError::auth("MySQL source connection URI is invalid"))?;
            let mut connection = Conn::new(opts)
                .await
                .map_err(|error| crate::error::classify_mysql_error("connect for MySQL catalog discovery", error))?;
            apply_session_options(&mut connection, &options, &cdf_runtime::RunCancellation::default()).await?;
            let database = connection
                .query_first::<String, _>("SELECT DATABASE()")
                .await
                .map_err(|error| crate::error::classify_mysql_error("read MySQL catalog database", error))?
                .ok_or_else(|| CdfError::contract("MySQL configured-source discovery requires a database in the connection URI"))?;
            MySqlIdentifier::user(database.clone())?;
            let bound = maximum.saturating_add(1);
            let rows = connection
                .exec::<Row, _, _>(
                    concat!(
                        "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.tables ",
                        "WHERE TABLE_SCHEMA = ? AND TABLE_TYPE IN ('BASE TABLE', 'VIEW') ",
                        "ORDER BY TABLE_NAME LIMIT ?"
                    ),
                    (database.clone(), u64::try_from(bound).map_err(|_| CdfError::contract("MySQL catalog bound exceeds u64"))?),
                )
                .await
                .map_err(|error| crate::error::classify_mysql_error("list MySQL catalog relations", error))?;
            let mut tables = Vec::with_capacity(rows.len());
            for row in rows {
                let (table, relation_type) = mysql_async::from_row_opt::<(String, String)>(row)
                    .map_err(|_| CdfError::data("MySQL catalog returned invalid relation metadata"))?;
                tables.push((database.clone(), table, relation_type));
            }
            Ok(tables)
        })?;
        let complete = tables.len() <= maximum;
        let candidates = tables
            .into_iter()
            .take(maximum)
            .map(|(database, table, relation_type)| {
                let target = MySqlTarget::parse(&format!("{database}.{table}"))?;
                Ok(SourceCatalogCandidate {
                    relation_id: target.display_name(),
                    display_label: target.display_name(),
                    relation_kind: if relation_type == "VIEW" {
                        "view"
                    } else {
                        "table"
                    }
                    .to_owned(),
                    resource_token: catalog_resource_token(&table),
                    resource_options: BTreeMap::from([(
                        "table".to_owned(),
                        serde_json::json!(target.display_name()),
                    )]),
                    schema: None,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        SourceCatalogDiscovery::new(
            request,
            "mysql_relation",
            candidates,
            complete,
            (!complete).then(|| "narrow relation selectors to a complete catalog set".to_owned()),
        )
    }

    fn discover_catalog_schema(
        &self,
        request: &SourceCatalogRequest,
        candidate: &SourceCatalogCandidate,
        context: &SourceResolutionContext<'_>,
    ) -> Result<Option<CanonicalArrowSchema>> {
        request.validate()?;
        candidate.validate()?;
        let table = candidate
            .resource_options
            .get("table")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| CdfError::contract("MySQL catalog candidate omitted its table"))?;
        let target = MySqlTarget::parse(table)?;
        let source: MySqlSourceOptions =
            decode_options("MySQL source", request.source_options.clone())?;
        let options = source.native_options()?;
        let reference = SecretUri::new(source.connection)?;
        let connection = context.secret_provider().resolve(&reference)?;
        let connection = connection.as_str()?.to_owned();
        let egress = context.egress_scope(&self.descriptor().driver_id);
        let relation_id = candidate.relation_id.clone();
        let schema = context.execution().run_io(async move {
            egress.authorize(&connection)?;
            let opts = Opts::from_url(&connection)
                .map_err(|_| CdfError::auth("MySQL source connection URI is invalid"))?;
            let mut connection = Conn::new(opts).await.map_err(|error| {
                crate::error::classify_mysql_error("connect for MySQL catalog schema", error)
            })?;
            apply_session_options(
                &mut connection,
                &options,
                &cdf_runtime::RunCancellation::default(),
            )
            .await?;
            let statement = connection
                .prep(format!("SELECT * FROM {} LIMIT 0", target.sql()))
                .await
                .map_err(|error| {
                    crate::error::classify_mysql_error("prepare MySQL catalog relation", error)
                })?;
            schema_from_columns(
                &ResourceId::new(format!("catalog.{relation_id}"))?,
                statement.columns().as_ref(),
            )
        })?;
        Ok(Some(CanonicalArrowSchema::from_arrow(&schema)?))
    }
}

impl MySqlSourceOptions {
    fn native_options(&self) -> Result<crate::native::MySqlNativeOptions> {
        if !self
            .dialect
            .as_deref()
            .is_none_or(|dialect| dialect.eq_ignore_ascii_case("mysql"))
        {
            return Err(CdfError::contract(
                "MySQL source dialect must be `mysql` when declared",
            ));
        }
        crate::native::MySqlNativeOptions::from_authored(
            self.isolation.unwrap_or_default(),
            self.fetch_rows,
            self.output_batch_rows,
            self.max_execution_time_ms,
            self.lock_wait_timeout_ms,
            self.use_invisible_indexes.unwrap_or(false),
        )
    }
}

fn catalog_resource_token(value: &str) -> Option<String> {
    let mut bytes = value.bytes();
    (value.len() <= 128
        && bytes.next().is_some_and(|byte| byte.is_ascii_lowercase())
        && bytes.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_'))
    .then(|| value.to_owned())
}
