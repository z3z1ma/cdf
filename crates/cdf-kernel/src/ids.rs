use std::fmt;

use serde::{Deserialize, Serialize};

use crate::error::{CdfError, Result};

macro_rules! string_id {
    ($name:ident) => {
        #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            pub fn new(value: impl Into<String>) -> Result<Self> {
                let value = value.into();
                if value.trim().is_empty() {
                    return Err(CdfError::contract(concat!(
                        stringify!($name),
                        " cannot be empty"
                    )));
                }
                Ok(Self(value))
            }

            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl AsRef<str> for $name {
            fn as_ref(&self) -> &str {
                self.as_str()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(self.as_str())
            }
        }
    };
}

string_id!(BatchId);
string_id!(CheckpointId);
string_id!(ContractRef);
string_id!(DestinationId);
string_id!(IdempotencyToken);
string_id!(PackageHash);
string_id!(PartitionId);
string_id!(PipelineId);
string_id!(PlanId);
string_id!(PredicateId);
string_id!(ReceiptId);
string_id!(ResourceId);
string_id!(RunId);
string_id!(SchemaHash);
string_id!(SegmentId);
string_id!(SourceId);
string_id!(TargetName);
string_id!(ValidationProgramHash);
