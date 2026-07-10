use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{
    CapabilitySupport, CdfError, DestinationSheet, IdempotencySupport, PackageHash, PromotionId,
    Result, SchemaHash, SegmentId, TransactionSupport,
};

pub const DESTINATION_CORRECTION_CAPABILITIES_VERSION: u16 = 1;
pub const DESTINATION_PROTOCOL_CAPABILITIES_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowProvenanceAddress {
    pub original_package_hash: PackageHash,
    pub original_segment_id: SegmentId,
    pub original_row_ordinal: u64,
}

impl RowProvenanceAddress {
    pub fn new(
        original_package_hash: PackageHash,
        original_segment_id: SegmentId,
        original_row_ordinal: u64,
    ) -> Self {
        Self {
            original_package_hash,
            original_segment_id,
            original_row_ordinal,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct RowProvenanceCapabilities {
    pub persistence: CapabilitySupport,
    pub targetability: CapabilitySupport,
}

impl RowProvenanceCapabilities {
    pub fn new(persistence: CapabilitySupport, targetability: CapabilitySupport) -> Self {
        Self {
            persistence,
            targetability,
        }
    }
}

impl Default for RowProvenanceCapabilities {
    fn default() -> Self {
        Self {
            persistence: CapabilitySupport::Unsupported,
            targetability: CapabilitySupport::Unsupported,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrectionStrategy {
    InPlaceUpdate,
    CorrectionSidecar,
    VersionedRematerialization,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct CorrectionStrategyCapability {
    pub strategy: CorrectionStrategy,
    pub transaction_guarantee: TransactionSupport,
    pub idempotency_guarantee: IdempotencySupport,
}

impl CorrectionStrategyCapability {
    pub fn new(
        strategy: CorrectionStrategy,
        transaction_guarantee: TransactionSupport,
        idempotency_guarantee: IdempotencySupport,
    ) -> Self {
        Self {
            strategy,
            transaction_guarantee,
            idempotency_guarantee,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DestinationCorrectionCapabilities {
    pub version: u16,
    pub row_provenance: RowProvenanceCapabilities,
    pub residual_readback: CapabilitySupport,
    pub strategies: Vec<CorrectionStrategyCapability>,
}

impl Default for DestinationCorrectionCapabilities {
    fn default() -> Self {
        Self {
            version: DESTINATION_CORRECTION_CAPABILITIES_VERSION,
            row_provenance: RowProvenanceCapabilities::default(),
            residual_readback: CapabilitySupport::Unsupported,
            strategies: Vec::new(),
        }
    }
}

impl DestinationCorrectionCapabilities {
    pub fn with_row_provenance(mut self, row_provenance: RowProvenanceCapabilities) -> Self {
        self.row_provenance = row_provenance;
        self
    }

    pub fn with_residual_readback(mut self, residual_readback: CapabilitySupport) -> Self {
        self.residual_readback = residual_readback;
        self
    }

    pub fn with_strategy(mut self, strategy: CorrectionStrategyCapability) -> Self {
        self.strategies.push(strategy);
        self
    }

    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn validate(
        &self,
        destination_transactions: &TransactionSupport,
        destination_idempotency: &IdempotencySupport,
    ) -> Result<()> {
        if self.version != DESTINATION_CORRECTION_CAPABILITIES_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported destination correction capabilities version {}; expected {}",
                self.version, DESTINATION_CORRECTION_CAPABILITIES_VERSION
            )));
        }
        if self.row_provenance.targetability == CapabilitySupport::Supported
            && self.row_provenance.persistence != CapabilitySupport::Supported
        {
            return Err(CdfError::contract(
                "destination correction capabilities cannot claim targetable row provenance without persisted row provenance",
            ));
        }
        if self.residual_readback == CapabilitySupport::Supported
            && self.row_provenance.persistence != CapabilitySupport::Supported
        {
            return Err(CdfError::contract(
                "destination correction capabilities cannot claim residual readback without persisted row provenance",
            ));
        }

        let mut seen = BTreeSet::new();
        for capability in &self.strategies {
            if !seen.insert(capability.strategy) {
                return Err(CdfError::contract(format!(
                    "destination correction capabilities declare {:?} more than once",
                    capability.strategy
                )));
            }
            if capability.transaction_guarantee != *destination_transactions {
                return Err(CdfError::contract(format!(
                    "destination correction strategy {:?} claims transaction guarantee {:?} but the destination sheet declares {:?}",
                    capability.strategy, capability.transaction_guarantee, destination_transactions
                )));
            }
            if capability.idempotency_guarantee != *destination_idempotency {
                return Err(CdfError::contract(format!(
                    "destination correction strategy {:?} claims idempotency guarantee {:?} but the destination sheet declares {:?}",
                    capability.strategy, capability.idempotency_guarantee, destination_idempotency
                )));
            }
            if capability.transaction_guarantee == TransactionSupport::None {
                return Err(CdfError::contract(format!(
                    "destination correction strategy {:?} requires an atomic transaction guarantee",
                    capability.strategy
                )));
            }
            if capability.idempotency_guarantee == IdempotencySupport::None {
                return Err(CdfError::contract(format!(
                    "destination correction strategy {:?} requires an idempotency guarantee",
                    capability.strategy
                )));
            }
            match capability.strategy {
                CorrectionStrategy::InPlaceUpdate => {
                    if self.row_provenance.targetability != CapabilitySupport::Supported {
                        return Err(CdfError::contract(
                            "in_place_update requires targetable persisted row provenance",
                        ));
                    }
                    if capability.transaction_guarantee != TransactionSupport::AtomicPackage {
                        return Err(CdfError::contract(
                            "in_place_update requires an atomic_package transaction guarantee",
                        ));
                    }
                }
                CorrectionStrategy::CorrectionSidecar => {}
                CorrectionStrategy::VersionedRematerialization => {
                    if capability.transaction_guarantee != TransactionSupport::AtomicTarget {
                        return Err(CdfError::contract(
                            "versioned_rematerialization requires an atomic_target transaction guarantee",
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    pub fn strategy(&self, strategy: CorrectionStrategy) -> Option<&CorrectionStrategyCapability> {
        self.strategies
            .iter()
            .find(|capability| capability.strategy == strategy)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DestinationProtocolCapabilities {
    pub version: u16,
    pub corrections: DestinationCorrectionCapabilities,
}

impl Default for DestinationProtocolCapabilities {
    fn default() -> Self {
        Self {
            version: DESTINATION_PROTOCOL_CAPABILITIES_VERSION,
            corrections: DestinationCorrectionCapabilities::default(),
        }
    }
}

impl DestinationProtocolCapabilities {
    pub fn is_default(&self) -> bool {
        self == &Self::default()
    }

    pub fn with_corrections(mut self, corrections: DestinationCorrectionCapabilities) -> Self {
        self.corrections = corrections;
        self
    }

    pub fn validate(&self, sheet: &DestinationSheet) -> Result<()> {
        if self.version != DESTINATION_PROTOCOL_CAPABILITIES_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported destination protocol capabilities version {}; expected {}",
                self.version, DESTINATION_PROTOCOL_CAPABILITIES_VERSION
            )));
        }
        self.corrections
            .validate(&sheet.transactions, &sheet.idempotency)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DestinationSheetArtifact {
    #[serde(flatten)]
    pub sheet: DestinationSheet,
    #[serde(
        default,
        skip_serializing_if = "DestinationProtocolCapabilities::is_default"
    )]
    pub protocol_capabilities: DestinationProtocolCapabilities,
}

impl DestinationSheetArtifact {
    pub fn new(
        sheet: DestinationSheet,
        protocol_capabilities: DestinationProtocolCapabilities,
    ) -> Result<Self> {
        protocol_capabilities.validate(&sheet)?;
        Ok(Self {
            sheet,
            protocol_capabilities,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResidualCorrectionOperation {
    RemovePromotedPath,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCorrectionRequest {
    pub promotion_id: PromotionId,
    pub original_row: RowProvenanceAddress,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub promoted_path: String,
    pub promoted_value_json: String,
    pub residual_operation: ResidualCorrectionOperation,
    pub selected_strategy: CorrectionStrategy,
}

impl DestinationCorrectionRequest {
    pub fn validate(&self) -> Result<()> {
        if !self.promoted_path.starts_with('/') {
            return Err(CdfError::contract(
                "destination correction promoted_path must be a non-root JSON pointer beginning with `/`",
            ));
        }
        if self.promoted_value_json.trim().is_empty() {
            return Err(CdfError::contract(
                "destination correction promoted_value_json cannot be empty",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DestinationCorrectionPlan {
    pub request: DestinationCorrectionRequest,
    pub transaction_guarantee: TransactionSupport,
    pub idempotency_guarantee: IdempotencySupport,
}

impl DestinationCorrectionPlan {
    pub fn validate_for(
        &self,
        capabilities: &DestinationCorrectionCapabilities,
        destination_transactions: &TransactionSupport,
        destination_idempotency: &IdempotencySupport,
    ) -> Result<()> {
        self.request.validate()?;
        capabilities.validate(destination_transactions, destination_idempotency)?;
        let capability = capabilities
            .strategy(self.request.selected_strategy)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "destination does not support correction strategy {:?}",
                    self.request.selected_strategy
                ))
            })?;
        if self.transaction_guarantee != capability.transaction_guarantee
            || self.idempotency_guarantee != capability.idempotency_guarantee
        {
            return Err(CdfError::contract(
                "destination correction plan guarantees do not match the selected destination capability",
            ));
        }
        Ok(())
    }
}
