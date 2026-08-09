use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    CanonicalArrowField, CapabilitySupport, CdfError, CommitCounts, CommitPlan, DeliveryGuarantee,
    DestinationSheet, IdempotencySupport, IdempotencyToken, PackageHash, PromotionId, Receipt,
    Result, SchemaHash, SegmentAck, SegmentId, StateSegment, TargetName, TransactionSupport,
    WriteDisposition,
};

pub const DESTINATION_CORRECTION_CAPABILITIES_VERSION: u16 = 1;
pub const DESTINATION_PROTOCOL_CAPABILITIES_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub object_key_rules: Option<ObjectKeyRules>,
    pub routed_target_families: CapabilitySupport,
}

impl Default for DestinationProtocolCapabilities {
    fn default() -> Self {
        Self {
            version: DESTINATION_PROTOCOL_CAPABILITIES_VERSION,
            corrections: DestinationCorrectionCapabilities::default(),
            object_key_rules: None,
            routed_target_families: CapabilitySupport::Unsupported,
        }
    }
}

impl DestinationProtocolCapabilities {
    pub fn with_corrections(mut self, corrections: DestinationCorrectionCapabilities) -> Self {
        self.corrections = corrections;
        self
    }

    pub fn with_object_key_rules(mut self, rules: ObjectKeyRules) -> Self {
        self.object_key_rules = Some(rules);
        self
    }

    pub fn with_routed_target_families(mut self) -> Self {
        self.routed_target_families = CapabilitySupport::Supported;
        self
    }

    pub fn object_key_rules(&self) -> Option<&ObjectKeyRules> {
        self.object_key_rules.as_ref()
    }

    pub fn validate(&self, sheet: &DestinationSheet) -> Result<()> {
        if self.version != DESTINATION_PROTOCOL_CAPABILITIES_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported destination protocol capabilities version {}; expected {}",
                self.version, DESTINATION_PROTOCOL_CAPABILITIES_VERSION
            )));
        }
        self.corrections
            .validate(&sheet.transactions, &sheet.idempotency)?;
        if let Some(rules) = &self.object_key_rules {
            rules.validate()?;
        }
        if self.routed_target_families == CapabilitySupport::Supported
            && (sheet.transactions != TransactionSupport::AtomicPackage
                || sheet.idempotency != IdempotencySupport::PackageToken)
        {
            return Err(CdfError::contract(
                "routed target families require atomic-package transactions and package-token idempotency",
            ));
        }
        Ok(())
    }
}

pub const OBJECT_KEY_RULES_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectKeyRules {
    pub version: u16,
    pub policy: ObjectKeyPolicy,
}

impl ObjectKeyRules {
    pub fn component_v1() -> Self {
        Self {
            version: OBJECT_KEY_RULES_VERSION,
            policy: ObjectKeyPolicy::ComponentV1,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.version != OBJECT_KEY_RULES_VERSION {
            return Err(CdfError::contract(format!(
                "unsupported object-key rules version {}; expected {OBJECT_KEY_RULES_VERSION}",
                self.version
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectKeyPolicy {
    #[serde(rename = "object-key-component-v1")]
    ComponentV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct DestinationSheetArtifact {
    #[serde(flatten)]
    pub sheet: DestinationSheet,
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

pub const DESTINATION_CORRECTION_RECEIPT_EVIDENCE_VERSION: u16 = 1;
pub const DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY: &str = "cdf.correction.v1";
pub const DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_VERSION: u16 = 1;
pub const DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_KEY: &str = "cdf.correction.sidecar.v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationCorrectionOperation {
    pub correction: DestinationCorrectionPlan,
    pub output_field: CanonicalArrowField,
    pub promoted_value_residual_json_v1: Vec<u8>,
}

pub fn correction_operations_digest(
    operations: &[DestinationCorrectionOperation],
) -> Result<String> {
    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct AuthorityOperation<'a> {
        promotion_id: &'a PromotionId,
        original_row: &'a RowProvenanceAddress,
        old_schema_hash: &'a SchemaHash,
        new_schema_hash: &'a SchemaHash,
        promoted_path: &'a str,
        residual_operation: ResidualCorrectionOperation,
        selected_strategy: CorrectionStrategy,
        transaction_guarantee: &'a TransactionSupport,
        idempotency_guarantee: &'a IdempotencySupport,
        output_field: &'a CanonicalArrowField,
        promoted_value_residual_json_v1: &'a [u8],
    }

    if operations.is_empty() {
        return Err(CdfError::contract(
            "destination correction operations cannot be empty",
        ));
    }
    let mut canonical = operations.to_vec();
    canonical.sort_by(|left, right| {
        let left_request = &left.correction.request;
        let right_request = &right.correction.request;
        (
            &left_request.original_row,
            left_request.promoted_path.as_str(),
        )
            .cmp(&(
                &right_request.original_row,
                right_request.promoted_path.as_str(),
            ))
    });
    let authority = canonical
        .iter()
        .map(|operation| {
            let plan = &operation.correction;
            let request = &plan.request;
            AuthorityOperation {
                promotion_id: &request.promotion_id,
                original_row: &request.original_row,
                old_schema_hash: &request.old_schema_hash,
                new_schema_hash: &request.new_schema_hash,
                promoted_path: &request.promoted_path,
                residual_operation: request.residual_operation,
                selected_strategy: request.selected_strategy,
                transaction_guarantee: &plan.transaction_guarantee,
                idempotency_guarantee: &plan.idempotency_guarantee,
                output_field: &operation.output_field,
                promoted_value_residual_json_v1: &operation.promoted_value_residual_json_v1,
            }
        })
        .collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&authority).map_err(|error| {
        CdfError::internal(format!(
            "serialize destination correction operations: {error}"
        ))
    })?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

pub fn correction_authority_digest(
    fields: &[DestinationCorrectionFieldAuthority],
    superseded_packages: &[DestinationSupersededPackageEvidence],
    operations: &[DestinationCorrectionOperation],
) -> Result<String> {
    if superseded_packages.is_empty() {
        return correction_operations_digest(operations);
    }
    #[derive(Serialize)]
    #[serde(deny_unknown_fields)]
    struct Authority<'a> {
        fields: &'a [DestinationCorrectionFieldAuthority],
        superseded_packages: &'a [DestinationSupersededPackageEvidence],
        operation_digest: Option<String>,
    }
    let mut fields = fields.to_vec();
    fields.sort_by(|left, right| left.promoted_path.cmp(&right.promoted_path));
    let mut superseded_packages = superseded_packages.to_vec();
    superseded_packages.sort_by(|left, right| left.package_hash.cmp(&right.package_hash));
    let operation_digest = (!operations.is_empty())
        .then(|| correction_operations_digest(operations))
        .transpose()?;
    let bytes = serde_json::to_vec(&Authority {
        fields: &fields,
        superseded_packages: &superseded_packages,
        operation_digest,
    })
    .map_err(|error| {
        CdfError::internal(format!(
            "serialize destination correction authority: {error}"
        ))
    })?;
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(bytes))))
}

impl DestinationCorrectionOperation {
    pub fn validate_structure(&self) -> Result<()> {
        self.correction.request.validate()?;
        if !self.output_field.nullable {
            return Err(CdfError::contract(format!(
                "destination correction output field {:?} must be nullable",
                self.output_field.name
            )));
        }
        if self.output_field.name.trim().is_empty() {
            return Err(CdfError::contract(
                "destination correction output field name cannot be empty",
            ));
        }
        if self.promoted_value_residual_json_v1.is_empty() {
            return Err(CdfError::contract(
                "executable destination correction requires promoted_value_residual_json_v1 authority",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationCorrectionFieldAuthority {
    pub promoted_path: String,
    pub output_field: CanonicalArrowField,
}

impl DestinationCorrectionFieldAuthority {
    pub fn validate(&self) -> Result<()> {
        if !self.promoted_path.starts_with('/') {
            return Err(CdfError::contract(
                "destination correction field path must be a non-root JSON pointer beginning with `/`",
            ));
        }
        if self.output_field.name.trim().is_empty() || !self.output_field.nullable {
            return Err(CdfError::contract(
                "destination correction output fields must be named and nullable",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationSupersededPackageEvidence {
    pub package_hash: PackageHash,
    pub operation_count: u64,
    pub operation_digest: String,
}

impl DestinationSupersededPackageEvidence {
    pub fn validate(&self) -> Result<()> {
        PackageHash::new(self.package_hash.as_str()).map(drop)?;
        SchemaHash::new(self.operation_digest.as_str()).map(drop)?;
        if self.operation_count == 0 {
            return Err(CdfError::contract(
                "superseded correction package evidence requires at least one proven absent operation",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationCorrectionCommitRequest {
    pub correction_package_hash: PackageHash,
    pub idempotency_token: IdempotencyToken,
    pub target: TargetName,
    pub resource_disposition: WriteDisposition,
    pub promotion_id: PromotionId,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub strategy: CorrectionStrategy,
    pub fields: Vec<DestinationCorrectionFieldAuthority>,
    pub superseded_packages: Vec<DestinationSupersededPackageEvidence>,
    pub segments: Vec<StateSegment>,
    pub operations_digest: String,
    pub corrections: Vec<DestinationCorrectionOperation>,
}

impl DestinationCorrectionCommitRequest {
    pub fn new(
        correction_package_hash: PackageHash,
        idempotency_token: IdempotencyToken,
        target: TargetName,
        resource_disposition: WriteDisposition,
        segments: Vec<StateSegment>,
        corrections: Vec<DestinationCorrectionOperation>,
    ) -> Result<Self> {
        let first = corrections.first().ok_or_else(|| {
            CdfError::contract(
                "destination correction construction requires an operation or explicit supersession authority",
            )
        })?;
        let request = &first.correction.request;
        let mut fields = corrections
            .iter()
            .map(|operation| DestinationCorrectionFieldAuthority {
                promoted_path: operation.correction.request.promoted_path.clone(),
                output_field: operation.output_field.clone(),
            })
            .collect::<Vec<_>>();
        fields.sort_by(|left, right| left.promoted_path.cmp(&right.promoted_path));
        fields.dedup();
        Self::new_with_authority(
            correction_package_hash,
            idempotency_token,
            target,
            resource_disposition,
            request.promotion_id.clone(),
            request.old_schema_hash.clone(),
            request.new_schema_hash.clone(),
            request.selected_strategy,
            fields,
            Vec::new(),
            segments,
            corrections,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_authority(
        correction_package_hash: PackageHash,
        idempotency_token: IdempotencyToken,
        target: TargetName,
        resource_disposition: WriteDisposition,
        promotion_id: PromotionId,
        old_schema_hash: SchemaHash,
        new_schema_hash: SchemaHash,
        strategy: CorrectionStrategy,
        mut fields: Vec<DestinationCorrectionFieldAuthority>,
        mut superseded_packages: Vec<DestinationSupersededPackageEvidence>,
        segments: Vec<StateSegment>,
        corrections: Vec<DestinationCorrectionOperation>,
    ) -> Result<Self> {
        fields.sort_by(|left, right| left.promoted_path.cmp(&right.promoted_path));
        superseded_packages.sort_by(|left, right| left.package_hash.cmp(&right.package_hash));
        let operations_digest =
            correction_authority_digest(&fields, &superseded_packages, &corrections)?;
        let request = Self {
            correction_package_hash,
            idempotency_token,
            target,
            resource_disposition,
            promotion_id,
            old_schema_hash,
            new_schema_hash,
            strategy,
            fields,
            superseded_packages,
            segments,
            operations_digest,
            corrections,
        };
        request.validate_structure()?;
        Ok(request)
    }

    pub fn validate_structure(&self) -> Result<()> {
        if self.fields.is_empty()
            || (self.corrections.is_empty() && self.superseded_packages.is_empty())
        {
            return Err(CdfError::contract(
                "destination correction commit requires promoted fields and either operations or superseded-package evidence",
            ));
        }
        let computed_digest = correction_authority_digest(
            &self.fields,
            &self.superseded_packages,
            &self.corrections,
        )?;
        if self.operations_digest != computed_digest {
            return Err(CdfError::contract(format!(
                "destination correction operations digest {} does not match computed {}",
                self.operations_digest, computed_digest
            )));
        }

        let mut field_paths = BTreeSet::new();
        let mut field_names = std::collections::BTreeMap::new();
        for field in &self.fields {
            field.validate()?;
            if !field_paths.insert(field.promoted_path.as_str()) {
                return Err(CdfError::contract(
                    "destination correction fields must have unique promoted paths",
                ));
            }
            if let Some(existing_path) = field_names.insert(
                field.output_field.name.as_str(),
                field.promoted_path.as_str(),
            ) {
                return Err(CdfError::contract(format!(
                    "destination correction output field {:?} maps from conflicting promoted paths {:?} and {:?}",
                    field.output_field.name, existing_path, field.promoted_path
                )));
            }
        }
        let mut superseded_packages = BTreeSet::new();
        for evidence in &self.superseded_packages {
            evidence.validate()?;
            if !superseded_packages.insert(&evidence.package_hash) {
                return Err(CdfError::contract(
                    "destination correction superseded-package evidence must be unique",
                ));
            }
        }
        if !self.superseded_packages.is_empty()
            && (self.resource_disposition != WriteDisposition::Replace
                || self.strategy != CorrectionStrategy::InPlaceUpdate)
        {
            return Err(CdfError::contract(
                "superseded-package correction evidence is valid only for replace in-place correction",
            ));
        }
        let mut segment_ids = BTreeSet::new();
        let mut segment_rows = 0_u64;
        for segment in &self.segments {
            if !segment_ids.insert(segment.segment_id.clone()) {
                return Err(CdfError::contract(format!(
                    "destination correction commit contains duplicate segment {}",
                    segment.segment_id
                )));
            }
            segment_rows = segment_rows.checked_add(segment.row_count).ok_or_else(|| {
                CdfError::contract("destination correction segment row count overflow")
            })?;
        }
        if segment_rows != self.corrections.len() as u64 {
            return Err(CdfError::contract(format!(
                "destination correction package has {segment_rows} segment row(s) but {} correction operation(s)",
                self.corrections.len()
            )));
        }

        let mut operations = BTreeSet::new();
        let mut fields = std::collections::BTreeMap::new();
        let mut field_names = std::collections::BTreeMap::new();
        let mut operation_packages = BTreeSet::new();
        for operation in &self.corrections {
            operation.validate_structure()?;
            let request = &operation.correction.request;
            if request.promotion_id != self.promotion_id
                || request.old_schema_hash != self.old_schema_hash
                || request.new_schema_hash != self.new_schema_hash
                || request.selected_strategy != self.strategy
            {
                return Err(CdfError::contract(
                    "destination correction commit must contain one promotion, schema transition, and strategy",
                ));
            }
            if !self.fields.iter().any(|field| {
                field.promoted_path == request.promoted_path
                    && field.output_field == operation.output_field
            }) {
                return Err(CdfError::contract(
                    "destination correction operation is absent from promoted-field authority",
                ));
            }
            operation_packages.insert(&request.original_row.original_package_hash);
            if !operations.insert((request.original_row.clone(), request.promoted_path.clone())) {
                return Err(CdfError::contract(format!(
                    "destination correction commit repeats address/path operation {} {} {} {:?}",
                    request.original_row.original_package_hash,
                    request.original_row.original_segment_id,
                    request.original_row.original_row_ordinal,
                    request.promoted_path
                )));
            }
            if let Some(existing) = fields.insert(
                request.promoted_path.clone(),
                operation.output_field.clone(),
            ) && existing != operation.output_field
            {
                return Err(CdfError::contract(format!(
                    "destination correction path {:?} maps to conflicting output fields",
                    request.promoted_path
                )));
            }
            if let Some(existing_path) = field_names.insert(
                operation.output_field.name.clone(),
                request.promoted_path.clone(),
            ) && existing_path != request.promoted_path
            {
                return Err(CdfError::contract(format!(
                    "destination correction output field {:?} maps from conflicting promoted paths {:?} and {:?}",
                    operation.output_field.name, existing_path, request.promoted_path
                )));
            }
        }
        if operation_packages
            .iter()
            .any(|package| superseded_packages.contains(package))
        {
            return Err(CdfError::contract(
                "destination correction package cannot be both executable and superseded",
            ));
        }
        Ok(())
    }

    pub fn validate_for(
        &self,
        capabilities: &DestinationCorrectionCapabilities,
        destination_transactions: &TransactionSupport,
        destination_idempotency: &IdempotencySupport,
    ) -> Result<()> {
        self.validate_structure()?;
        for operation in &self.corrections {
            operation.correction.validate_for(
                capabilities,
                destination_transactions,
                destination_idempotency,
            )?;
        }
        Ok(())
    }

    pub fn promotion_id(&self) -> &PromotionId {
        &self.promotion_id
    }

    pub fn old_schema_hash(&self) -> &SchemaHash {
        &self.old_schema_hash
    }

    pub fn new_schema_hash(&self) -> &SchemaHash {
        &self.new_schema_hash
    }

    pub fn strategy(&self) -> CorrectionStrategy {
        self.strategy
    }

    pub fn addressed_row_count(&self) -> u64 {
        self.corrections
            .iter()
            .map(|operation| &operation.correction.request.original_row)
            .collect::<BTreeSet<_>>()
            .len() as u64
    }

    pub fn segment_acks(&self) -> Vec<SegmentAck> {
        self.segments
            .iter()
            .map(|segment| SegmentAck {
                kind: segment.kind,
                segment_id: segment.segment_id.clone(),
                row_count: segment.row_count,
                byte_count: segment.byte_count,
            })
            .collect()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationResidualReadback {
    pub original_row: RowProvenanceAddress,
    pub residual_json_v1: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationCorrectionCommitPlan {
    pub kernel: CommitPlan,
    pub correction_package_hash: PackageHash,
    pub promotion_id: PromotionId,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub strategy: CorrectionStrategy,
    pub operations_digest: String,
    pub correction_count: u64,
}

impl DestinationCorrectionCommitPlan {
    pub fn validate_for(
        &self,
        request: &DestinationCorrectionCommitRequest,
        capabilities: &DestinationCorrectionCapabilities,
        destination_transactions: &TransactionSupport,
        destination_idempotency: &IdempotencySupport,
    ) -> Result<()> {
        request.validate_for(
            capabilities,
            destination_transactions,
            destination_idempotency,
        )?;
        if self.kernel.target != request.target
            || self.kernel.disposition != request.resource_disposition
            || self.kernel.idempotency != IdempotencySupport::PackageToken
            || self.kernel.delivery_guarantee != DeliveryGuarantee::EffectivelyOncePerPackage
            || self.correction_package_hash != request.correction_package_hash
            || self.promotion_id != *request.promotion_id()
            || self.old_schema_hash != *request.old_schema_hash()
            || self.new_schema_hash != *request.new_schema_hash()
            || self.strategy != request.strategy()
            || self.operations_digest != request.operations_digest
            || self.correction_count != request.corrections.len() as u64
        {
            return Err(CdfError::contract(
                "destination correction commit plan does not match its request",
            ));
        }
        Ok(())
    }

    pub fn validate_receipt(
        &self,
        request: &DestinationCorrectionCommitRequest,
        receipt: &Receipt,
    ) -> Result<DestinationCorrectionReceiptEvidence> {
        if receipt.target != request.target
            || receipt.package_hash != request.correction_package_hash
            || receipt.idempotency_token != request.idempotency_token
            || receipt.disposition != request.resource_disposition
            || receipt.schema_hash != *request.new_schema_hash()
            || receipt.segment_acks != request.segment_acks()
            || receipt.migrations != self.kernel.migrations
        {
            return Err(CdfError::destination(
                "destination correction receipt does not match its request and plan",
            ));
        }
        let evidence = DestinationCorrectionReceiptEvidence::from_receipt(receipt)?;
        let expected = DestinationCorrectionReceiptEvidence::for_request(request);
        if evidence != expected {
            return Err(CdfError::destination(
                "destination correction receipt evidence does not match its request",
            ));
        }
        match request.strategy() {
            CorrectionStrategy::InPlaceUpdate => {
                if receipt.counts.row_outcomes()
                    != Some((
                        expected.addressed_rows,
                        Some(0),
                        Some(expected.addressed_rows),
                        Some(0),
                    ))
                {
                    return Err(CdfError::destination(
                        "destination correction receipt counts do not match addressed updates",
                    ));
                }
            }
            CorrectionStrategy::CorrectionSidecar => {
                if receipt.counts.row_outcomes()
                    != Some((
                        expected.correction_count,
                        Some(expected.correction_count),
                        Some(0),
                        Some(0),
                    ))
                {
                    return Err(CdfError::destination(
                        "destination correction receipt counts do not match immutable sidecar operations",
                    ));
                }
                let sidecar = DestinationCorrectionSidecarReceiptEvidence::from_receipt(receipt)?;
                if sidecar.operation_count != expected.correction_count {
                    return Err(CdfError::destination(
                        "destination correction sidecar receipt operation count does not match its request",
                    ));
                }
            }
            CorrectionStrategy::VersionedRematerialization => {
                return Err(CdfError::destination(
                    "versioned rematerialization receipt validation requires a materialization-specific evidence contract",
                ));
            }
        }
        Ok(evidence)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationCorrectionReceiptEvidence {
    pub version: u16,
    pub operation: DestinationCorrectionOperationKind,
    pub promotion_id: PromotionId,
    pub old_schema_hash: SchemaHash,
    pub new_schema_hash: SchemaHash,
    pub strategy: CorrectionStrategy,
    pub operations_digest: String,
    pub correction_count: u64,
    pub addressed_rows: u64,
    pub residual_paths_removed: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DestinationCorrectionOperationKind {
    AddressedCorrection,
}

impl DestinationCorrectionReceiptEvidence {
    pub fn for_request(request: &DestinationCorrectionCommitRequest) -> Self {
        Self {
            version: DESTINATION_CORRECTION_RECEIPT_EVIDENCE_VERSION,
            operation: DestinationCorrectionOperationKind::AddressedCorrection,
            promotion_id: request.promotion_id().clone(),
            old_schema_hash: request.old_schema_hash().clone(),
            new_schema_hash: request.new_schema_hash().clone(),
            strategy: request.strategy(),
            operations_digest: request.operations_digest.clone(),
            correction_count: request.corrections.len() as u64,
            addressed_rows: request.addressed_row_count(),
            residual_paths_removed: request.corrections.len() as u64,
        }
    }

    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|error| CdfError::internal(error.to_string()))
    }

    pub fn from_receipt(receipt: &Receipt) -> Result<Self> {
        let transaction = receipt.transaction.as_ref().ok_or_else(|| {
            CdfError::destination("destination correction receipt is missing transaction evidence")
        })?;
        let json = transaction
            .values
            .get(DESTINATION_CORRECTION_RECEIPT_EVIDENCE_KEY)
            .ok_or_else(|| {
                CdfError::destination(
                    "destination correction receipt is missing closed correction evidence",
                )
            })?;
        let evidence: Self = serde_json::from_str(json).map_err(|error| {
            CdfError::destination(format!(
                "destination correction receipt evidence is invalid: {error}"
            ))
        })?;
        if evidence.version != DESTINATION_CORRECTION_RECEIPT_EVIDENCE_VERSION {
            return Err(CdfError::destination(format!(
                "unsupported destination correction receipt evidence version {}; expected {}",
                evidence.version, DESTINATION_CORRECTION_RECEIPT_EVIDENCE_VERSION
            )));
        }
        Ok(evidence)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationCorrectionSidecarObjectEvidence {
    pub key: String,
    pub sha256: String,
    pub byte_count: u64,
    pub operation_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationCorrectionSidecarReceiptEvidence {
    pub version: u16,
    pub manifest_key: String,
    pub manifest_sha256: String,
    pub operation_count: u64,
    pub atomic_manifest_publication: bool,
    pub base_target_unchanged: bool,
    pub objects: Vec<DestinationCorrectionSidecarObjectEvidence>,
}

impl DestinationCorrectionSidecarReceiptEvidence {
    pub fn validate(&self) -> Result<()> {
        if self.version != DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_VERSION {
            return Err(CdfError::destination(format!(
                "unsupported destination correction sidecar receipt evidence version {}; expected {}",
                self.version, DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_VERSION
            )));
        }
        if self.manifest_key.trim().is_empty() {
            return Err(CdfError::destination(
                "destination correction sidecar receipt manifest key cannot be empty",
            ));
        }
        validate_sidecar_sha256("manifest", &self.manifest_sha256)?;
        if !self.atomic_manifest_publication {
            return Err(CdfError::destination(
                "destination correction sidecar receipt must prove atomic manifest publication",
            ));
        }
        if !self.base_target_unchanged {
            return Err(CdfError::destination(
                "destination correction sidecar receipt must state that the base target is unchanged",
            ));
        }
        if self.operation_count == 0 || self.objects.is_empty() {
            return Err(CdfError::destination(
                "destination correction sidecar receipt must contain at least one operation and object",
            ));
        }
        let mut keys = BTreeSet::new();
        let mut object_operations = 0_u64;
        for object in &self.objects {
            if object.key.trim().is_empty() || !keys.insert(object.key.as_str()) {
                return Err(CdfError::destination(
                    "destination correction sidecar receipt object keys must be non-empty and unique",
                ));
            }
            validate_sidecar_sha256("object", &object.sha256)?;
            if object.byte_count == 0 || object.operation_count == 0 {
                return Err(CdfError::destination(
                    "destination correction sidecar receipt objects must have non-zero bytes and operations",
                ));
            }
            object_operations = object_operations
                .checked_add(object.operation_count)
                .ok_or_else(|| {
                    CdfError::destination(
                        "destination correction sidecar receipt operation count overflow",
                    )
                })?;
        }
        if object_operations != self.operation_count {
            return Err(CdfError::destination(format!(
                "destination correction sidecar receipt objects contain {object_operations} operations but evidence declares {}",
                self.operation_count
            )));
        }
        Ok(())
    }

    pub fn to_json(&self) -> Result<String> {
        self.validate()?;
        serde_json::to_string(self).map_err(|error| CdfError::internal(error.to_string()))
    }

    pub fn from_receipt(receipt: &Receipt) -> Result<Self> {
        let transaction = receipt.transaction.as_ref().ok_or_else(|| {
            CdfError::destination(
                "destination correction sidecar receipt is missing transaction evidence",
            )
        })?;
        let json = transaction
            .values
            .get(DESTINATION_CORRECTION_SIDECAR_RECEIPT_EVIDENCE_KEY)
            .ok_or_else(|| {
                CdfError::destination(
                    "destination correction receipt is missing closed sidecar evidence",
                )
            })?;
        let evidence: Self = serde_json::from_str(json).map_err(|error| {
            CdfError::destination(format!(
                "destination correction sidecar receipt evidence is invalid: {error}"
            ))
        })?;
        evidence.validate()?;
        Ok(evidence)
    }
}

fn validate_sidecar_sha256(kind: &str, value: &str) -> Result<()> {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return Err(CdfError::destination(format!(
            "destination correction sidecar {kind} hash must use sha256:<hex>"
        )));
    };
    if hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(CdfError::destination(format!(
            "destination correction sidecar {kind} hash must contain exactly 64 hexadecimal characters"
        )));
    }
    Ok(())
}

pub trait CorrectionCommitSession {
    fn apply_migrations(&mut self) -> Result<()>;

    fn apply_corrections(&mut self) -> Result<CommitCounts>;

    fn finalize(self: Box<Self>) -> Result<Receipt>;

    fn abort(self: Box<Self>) -> Result<()>;
}
