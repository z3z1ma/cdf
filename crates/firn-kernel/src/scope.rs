use serde::{Deserialize, Serialize};

use crate::ids::{ContractRef, DestinationId, PartitionId, TargetName};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ScopeKey {
    Resource,
    Partition {
        partition_id: PartitionId,
    },
    Window {
        start: String,
        end: String,
    },
    File {
        path: String,
    },
    Stream {
        name: String,
    },
    SchemaContract {
        contract: ContractRef,
    },
    DestinationLoad {
        destination: DestinationId,
        target: TargetName,
    },
    Composite {
        parts: Vec<ScopeKey>,
    },
}

impl ScopeKey {
    pub fn kind(&self) -> ScopeKind {
        match self {
            Self::Resource => ScopeKind::Resource,
            Self::Partition { .. } => ScopeKind::Partition,
            Self::Window { .. } => ScopeKind::Window,
            Self::File { .. } => ScopeKind::File,
            Self::Stream { .. } => ScopeKind::Stream,
            Self::SchemaContract { .. } => ScopeKind::SchemaContract,
            Self::DestinationLoad { .. } => ScopeKind::DestinationLoad,
            Self::Composite { .. } => ScopeKind::Composite,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScopeKind {
    Resource,
    Partition,
    Window,
    File,
    Stream,
    SchemaContract,
    DestinationLoad,
    Composite,
}
