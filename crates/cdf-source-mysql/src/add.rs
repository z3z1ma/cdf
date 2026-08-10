use std::{collections::BTreeMap, path::PathBuf};

use cdf_http::{SecretUri, SecretValue};
use cdf_kernel::{CdfError, Result};
use cdf_runtime::{
    SourceAddCursor, SourceAddCursorOrdering, SourceAddPlanner, SourceAddPrivateFile,
    SourceAddProposal, SourceAddRequest, SourceEvidenceLocation,
};

use crate::{
    driver::MySqlSourceDriver,
    native::{MySqlIsolation, MySqlNativeOptions, MySqlSourceInput},
};

impl SourceAddPlanner for MySqlSourceDriver {
    fn propose_add(&self, request: &SourceAddRequest) -> Result<Option<SourceAddProposal>> {
        request.validate()?;
        let Some((scheme, _)) = request.location.split_once("://") else {
            return Ok(None);
        };
        if scheme != "mysql" {
            return Ok(None);
        }
        const OPTIONS: &[&str] = &[
            "query",
            "cursor",
            "isolation",
            "fetch_rows",
            "output_batch_rows",
            "max_execution_time_ms",
            "lock_wait_timeout_ms",
            "use_invisible_indexes",
        ];
        if let Some(key) = request
            .options
            .keys()
            .find(|key| !OPTIONS.contains(&key.as_str()))
        {
            return Err(CdfError::contract(format!(
                "MySQL cdf add option `{key}` is not supported"
            )));
        }

        let mut parsed = url::Url::parse(&request.location).map_err(|error| {
            CdfError::contract(format!("cdf add could not parse MySQL DSN: {error}"))
        })?;
        if parsed.fragment().is_some() {
            return Err(CdfError::contract(
                "cdf add MySQL DSN must not contain fragment text",
            ));
        }
        let mut segments = parsed
            .path_segments()
            .map(|segments| {
                segments
                    .filter(|segment| !segment.is_empty())
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let query = request.options.get("query").cloned();
        let (input, display_selection) = if let Some(query) = query {
            if segments.len() != 1 {
                return Err(CdfError::contract(
                    "cdf add MySQL query DSN must end with exactly `/database`",
                ));
            }
            let input = MySqlSourceInput::from_authored(None, Some(query))?;
            let display = input.location_summary();
            (input, display)
        } else {
            if segments.len() != 2 {
                return Err(CdfError::contract(
                    "cdf add MySQL table DSN must end with exactly `/database/table`",
                ));
            }
            let table = segments.pop().expect("length checked");
            let database = segments.pop().expect("length checked");
            parsed.set_path(&format!("/{database}"));
            let input = MySqlSourceInput::from_authored(Some(table.clone()), None)?;
            (input, table)
        };
        let dsn = parsed.to_string();
        let relative_path =
            PathBuf::from(format!(".cdf/secrets/sources/{}.dsn", request.source_name));
        let reference = SecretUri::new(format!(
            "secret://file/.cdf/secrets/sources/{}.dsn",
            request.source_name
        ))?;

        let isolation = request
            .options
            .get("isolation")
            .map(|value| {
                serde_json::from_value::<MySqlIsolation>(serde_json::json!(value)).map_err(|_| {
                    CdfError::contract(
                        "MySQL cdf add isolation must be read_committed, repeatable_read, or serializable",
                    )
                })
            })
            .transpose()?
            .unwrap_or_default();
        let fetch_rows = parse_u64(&request.options, "fetch_rows")?;
        let output_batch_rows = parse_u64(&request.options, "output_batch_rows")?;
        let max_execution_time_ms = parse_u64(&request.options, "max_execution_time_ms")?;
        let lock_wait_timeout_ms = parse_u64(&request.options, "lock_wait_timeout_ms")?;
        let use_invisible_indexes = request
            .options
            .get("use_invisible_indexes")
            .map(|value| {
                value.parse::<bool>().map_err(|_| {
                    CdfError::contract("MySQL cdf add use_invisible_indexes must be true or false")
                })
            })
            .transpose()?
            .unwrap_or(false);
        MySqlNativeOptions::from_authored(
            isolation,
            fetch_rows,
            output_batch_rows,
            max_execution_time_ms,
            lock_wait_timeout_ms,
            use_invisible_indexes,
        )?;

        let mut resource_options = match &input {
            MySqlSourceInput::Table { target } => {
                BTreeMap::from([("table".to_owned(), serde_json::json!(target.display_name()))])
            }
            MySqlSourceInput::Query { sql, .. } => {
                BTreeMap::from([("query".to_owned(), serde_json::json!(sql))])
            }
        };
        for key in [
            "isolation",
            "fetch_rows",
            "output_batch_rows",
            "max_execution_time_ms",
            "lock_wait_timeout_ms",
            "use_invisible_indexes",
        ] {
            if let Some(value) = request.options.get(key) {
                let value = match key {
                    "fetch_rows"
                    | "output_batch_rows"
                    | "max_execution_time_ms"
                    | "lock_wait_timeout_ms" => {
                        serde_json::json!(value.parse::<u64>().expect("validated integer"))
                    }
                    "use_invisible_indexes" => {
                        serde_json::json!(value.parse::<bool>().expect("validated boolean"))
                    }
                    _ => serde_json::json!(value),
                };
                resource_options.insert(key.to_owned(), value);
            }
        }

        Ok(Some(SourceAddProposal {
            source_kind: "mysql".to_owned(),
            source_options: BTreeMap::from([(
                "connection".to_owned(),
                serde_json::json!(reference.as_str()),
            )]),
            resource_options,
            cursor: request.options.get("cursor").map(|field| SourceAddCursor {
                field: field.clone(),
                parameter: None,
                ordering: SourceAddCursorOrdering::Exact,
                lag_tolerance_ms: 0,
            }),
            display_location: SourceEvidenceLocation::from_operational(&dsn)?,
            display_selection,
            private_files: vec![SourceAddPrivateFile {
                reference,
                relative_path,
                value: SecretValue::new(dsn),
            }],
        }))
    }
}

fn parse_u64(options: &BTreeMap<String, String>, key: &str) -> Result<Option<u64>> {
    options
        .get(key)
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|_| CdfError::contract(format!("MySQL cdf add {key} must be an integer")))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(location: &str) -> SourceAddRequest {
        SourceAddRequest {
            source_name: "warehouse".to_owned(),
            resource_name: "orders".to_owned(),
            location: location.to_owned(),
            project_root: PathBuf::from("/project"),
            current_dir: PathBuf::from("/project"),
            options: BTreeMap::new(),
            project_options: None,
        }
    }

    #[test]
    fn table_add_stores_dsn_privately_without_inventing_a_cursor() {
        let proposal = MySqlSourceDriver::new()
            .unwrap()
            .propose_add(&request(
                "mysql://reader:password@db.example:3306/warehouse/orders",
            ))
            .unwrap()
            .unwrap();
        assert_eq!(
            proposal.source_options["connection"],
            "secret://file/.cdf/secrets/sources/warehouse.dsn"
        );
        assert_eq!(proposal.resource_options["table"], "orders");
        assert!(proposal.cursor.is_none());
        assert_eq!(proposal.private_files.len(), 1);
        assert!(!format!("{proposal:?}").contains("password"));
    }

    #[test]
    fn cursor_and_controls_are_explicit_resource_options() {
        let mut request = request("mysql://reader@db.example:3306/warehouse/orders");
        request.options = BTreeMap::from([
            ("cursor".to_owned(), "id".to_owned()),
            ("fetch_rows".to_owned(), "4096".to_owned()),
            ("use_invisible_indexes".to_owned(), "true".to_owned()),
        ]);
        let proposal = MySqlSourceDriver::new()
            .unwrap()
            .propose_add(&request)
            .unwrap()
            .unwrap();
        assert_eq!(proposal.cursor.unwrap().field, "id");
        assert_eq!(proposal.resource_options["fetch_rows"], 4096);
        assert_eq!(proposal.resource_options["use_invisible_indexes"], true);
    }
}
