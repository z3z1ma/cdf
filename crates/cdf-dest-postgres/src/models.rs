use cdf_kernel::{
    DestinationCorrectionCommitPlan, DestinationCorrectionCommitRequest, DestinationSheet,
    TypeMapping, TypeMappingFidelity, VerifyClause,
};
use cdf_postgres::{PostgresIdentifier, PostgresTarget};
use serde::{Deserialize, Serialize};

use crate::{
    identifiers::{PostgresColumn, PostgresExistingTable},
    plan::PostgresStatement,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PostgresDestination {
    pub(crate) sheet: PostgresDestinationSheet,
    #[serde(skip)]
    pub(crate) database_url: Option<String>,
    #[serde(skip)]
    pub(crate) pending_correction: Option<PostgresCorrectionCommitRequest>,
    #[serde(skip)]
    pub(crate) execution: Option<cdf_runtime::ExecutionServices>,
}

impl PartialEq for PostgresDestination {
    fn eq(&self, other: &Self) -> bool {
        self.sheet == other.sheet
            && self.database_url == other.database_url
            && self.pending_correction == other.pending_correction
    }
}

impl Eq for PostgresDestination {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresDestinationSheet {
    pub kernel: DestinationSheet,
    pub type_mappings: Vec<PostgresTypeMapping>,
    pub migration_operations: Vec<String>,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostgresCorrectionPlanInput {
    pub request: DestinationCorrectionCommitRequest,
    pub existing_table: PostgresExistingTable,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresCorrectionFieldPlan {
    pub promoted_path: String,
    pub column: PostgresColumn,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostgresCorrectionPlan {
    pub kernel: DestinationCorrectionCommitPlan,
    pub target: PostgresTarget,
    pub stage_table: PostgresIdentifier,
    pub fields: Vec<PostgresCorrectionFieldPlan>,
    pub system_ddl: Vec<PostgresStatement>,
    pub target_ddl: Vec<PostgresStatement>,
    pub create_stage: PostgresStatement,
    pub update_sql: Vec<PostgresStatement>,
    pub verify: VerifyClause,
}

#[derive(Clone)]
pub(crate) struct PostgresCorrectionCommitRequest {
    pub(crate) package: cdf_package_contract::SharedVerifiedPackageAccess,
    pub(crate) plan: PostgresCorrectionPlan,
}
