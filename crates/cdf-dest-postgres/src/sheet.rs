use crate::commit::validate_session_begin_inputs;
use crate::*;
use crate::{api::*, ddl::*, validate::*};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresDestination {
    pub(crate) sheet: PostgresDestinationSheet,
    #[serde(skip)]
    pub(crate) database_url: Option<String>,
    #[serde(skip)]
    pub(crate) pending_commit: Option<PostgresCommitRequest>,
}

impl Default for PostgresDestination {
    fn default() -> Self {
        Self {
            sheet: postgres_destination_sheet(),
            database_url: None,
            pending_commit: None,
        }
    }
}

impl PostgresDestination {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn postgres_sheet(&self) -> &PostgresDestinationSheet {
        &self.sheet
    }

    pub fn plan_load(&self, input: PostgresLoadPlanInput) -> Result<PostgresLoadPlan> {
        plan_postgres_load(input, &self.sheet)
    }

    pub fn with_commit_request(mut self, request: PostgresCommitRequest) -> Self {
        self.pending_commit = Some(request);
        self
    }
}

impl DestinationProtocol for PostgresDestination {
    fn sheet(&self) -> &DestinationSheet {
        &self.sheet.kernel
    }

    fn plan_commit(&self, request: &DestinationCommitRequest) -> Result<CommitPlan> {
        ensure_supported_disposition(&request.disposition)?;
        Ok(CommitPlan {
            plan_id: plan_id(
                &request.target,
                &request.disposition,
                request.package_hash.as_str(),
            )?,
            target: request.target.clone(),
            disposition: request.disposition.clone(),
            idempotency: IdempotencySupport::PackageToken,
            migrations: system_table_migrations(),
            delivery_guarantee: delivery_guarantee(&request.disposition),
        })
    }

    fn begin(
        &self,
        request: DestinationCommitRequest,
        plan: CommitPlan,
    ) -> Result<Box<dyn CommitSession + '_>> {
        let pending = self.pending_commit.as_ref().ok_or_else(|| {
            CdfError::contract(
                "PostgresDestination::begin requires PostgresDestination::with_commit_request",
            )
        })?;
        validate_session_begin_inputs(&request, &plan, &pending.plan)?;
        Ok(Box::new(self.begin_commit_session(pending.clone())?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresDestinationSheet {
    pub kernel: DestinationSheet,
    pub type_mappings: Vec<PostgresTypeMapping>,
    pub bulk_paths: Vec<String>,
    pub migration_operations: Vec<String>,
}

pub fn postgres_destination_sheet() -> PostgresDestinationSheet {
    let type_mappings = postgres_type_mappings();
    let kernel = DestinationSheet {
        destination: DestinationId::new(POSTGRES_DESTINATION_ID).expect("static destination id"),
        supported_dispositions: vec![
            WriteDisposition::Append,
            WriteDisposition::Replace,
            WriteDisposition::Merge,
        ],
        transactions: TransactionSupport::AtomicPackage,
        idempotency: IdempotencySupport::PackageToken,
        type_mappings: type_mappings
            .iter()
            .map(PostgresTypeMapping::as_kernel_mapping)
            .collect(),
        identifier_rules: IdentifierRules {
            normalizer: "namecase-v1/postgres-quoted-v1".to_owned(),
            max_length: Some(63),
            allowed_pattern: Some(
                "quoted UTF-8 identifier without NUL; cdf reserves _cdf_*".to_owned(),
            ),
        },
        migration_support: CapabilitySupport::Supported,
        quarantine_tables: CapabilitySupport::Supported,
        concurrency: ConcurrencyLimit {
            max_writers: Some(1),
        },
    };

    PostgresDestinationSheet {
        kernel,
        type_mappings,
        bulk_paths: vec![
            "copy_binary".to_owned(),
            "copy_csv".to_owned(),
            "extended_insert".to_owned(),
        ],
        migration_operations: vec![
            "create_schema_table".to_owned(),
            "add_nullable_column".to_owned(),
            "transactional_truncate_insert_replace".to_owned(),
        ],
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresTypeMapping {
    pub arrow_type: String,
    pub postgres_type: String,
    pub fidelity: PostgresTypeFidelity,
}

impl PostgresTypeMapping {
    pub fn new(
        arrow_type: impl Into<String>,
        postgres_type: impl Into<String>,
        fidelity: PostgresTypeFidelity,
    ) -> Self {
        Self {
            arrow_type: arrow_type.into(),
            postgres_type: postgres_type.into(),
            fidelity,
        }
    }

    pub fn as_kernel_mapping(&self) -> TypeMapping {
        TypeMapping {
            arrow_type: self.arrow_type.clone(),
            destination_type: self.postgres_type.clone(),
            fidelity: self.fidelity.as_kernel_fidelity(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PostgresTypeFidelity {
    Exact,
    Widening,
    LossyRequiresContractAllowance,
    Unsupported,
}

impl PostgresTypeFidelity {
    fn as_kernel_fidelity(&self) -> TypeMappingFidelity {
        match self {
            Self::Exact | Self::Widening => TypeMappingFidelity::Lossless,
            Self::LossyRequiresContractAllowance => {
                TypeMappingFidelity::LossyRequiresContractAllowance
            }
            Self::Unsupported => TypeMappingFidelity::Unsupported,
        }
    }
}

pub fn postgres_type_mappings() -> Vec<PostgresTypeMapping> {
    use PostgresTypeFidelity::{Exact, LossyRequiresContractAllowance, Unsupported, Widening};

    vec![
        PostgresTypeMapping::new("Boolean", "BOOLEAN", Exact),
        PostgresTypeMapping::new("Int8", "SMALLINT", Widening),
        PostgresTypeMapping::new("Int16", "SMALLINT", Exact),
        PostgresTypeMapping::new("Int32", "INTEGER", Exact),
        PostgresTypeMapping::new("Int64", "BIGINT", Exact),
        PostgresTypeMapping::new("UInt8", "SMALLINT", Widening),
        PostgresTypeMapping::new("UInt16", "INTEGER", Widening),
        PostgresTypeMapping::new("UInt32", "BIGINT", Widening),
        PostgresTypeMapping::new("UInt64", "NUMERIC(20,0)", Widening),
        PostgresTypeMapping::new("Float32", "REAL", Exact),
        PostgresTypeMapping::new("Float64", "DOUBLE PRECISION", Exact),
        PostgresTypeMapping::new("Decimal128(p,s)", "NUMERIC(p,s)", Exact),
        PostgresTypeMapping::new("Decimal256(p,s)", "NUMERIC(p,s)", Exact),
        PostgresTypeMapping::new("Utf8", "TEXT", Exact),
        PostgresTypeMapping::new("LargeUtf8", "TEXT", Exact),
        PostgresTypeMapping::new("Binary", "BYTEA", Exact),
        PostgresTypeMapping::new("LargeBinary", "BYTEA", Exact),
        PostgresTypeMapping::new("Date32", "DATE", Exact),
        PostgresTypeMapping::new("Time64(Microsecond)", "TIME", Exact),
        PostgresTypeMapping::new("Timestamp(Microsecond,None)", "TIMESTAMP", Exact),
        PostgresTypeMapping::new("Timestamp(Microsecond,Some(_))", "TIMESTAMPTZ", Exact),
        PostgresTypeMapping::new(
            "Timestamp(Nanosecond,*)",
            "TIMESTAMPTZ",
            LossyRequiresContractAllowance,
        ),
        PostgresTypeMapping::new("Struct", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("List", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Map", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Union", "JSONB", LossyRequiresContractAllowance),
        PostgresTypeMapping::new("Dictionary", "unsupported", Unsupported),
        PostgresTypeMapping::new("Duration", "unsupported", Unsupported),
        PostgresTypeMapping::new("Interval", "unsupported", Unsupported),
    ]
}
