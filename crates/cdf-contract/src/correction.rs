use crate::decode_residual_json_v1;
use arrow_array::ArrayRef;
use cdf_kernel::{
    CdfError, DestinationCorrectionCommitRequest, DestinationCorrectionOperation, Result,
};

pub fn correction_operations_digest(
    operations: &[DestinationCorrectionOperation],
) -> Result<String> {
    cdf_kernel::correction_operations_digest(operations)
}

pub fn decode_destination_correction_value(
    operation: &DestinationCorrectionOperation,
) -> Result<ArrayRef> {
    operation.validate_structure()?;
    let mut decoded =
        decode_residual_json_v1(&operation.promoted_value_residual_json_v1).map_err(|error| {
            CdfError::contract(format!(
                "destination correction exact value authority is invalid: {error}"
            ))
        })?;
    if decoded.len() != 1 {
        return Err(CdfError::contract(
            "destination correction exact value authority must contain exactly one residual field",
        ));
    }
    let decoded = decoded.pop().expect("length checked");
    if decoded.path != operation.correction.request.promoted_path {
        return Err(CdfError::contract(format!(
            "destination correction exact value path {:?} does not match promoted path {:?}",
            decoded.path, operation.correction.request.promoted_path
        )));
    }
    let output = operation.output_field.to_arrow()?;
    if decoded.array.data_type() != output.data_type() {
        return Err(CdfError::contract(format!(
            "destination correction exact value type {:?} does not match compiled output field {:?}",
            decoded.array.data_type(),
            output.data_type()
        )));
    }
    if decoded.array.len() != 1 {
        return Err(CdfError::internal(
            "decoded destination correction value did not contain one Arrow scalar",
        ));
    }
    Ok(decoded.array)
}

pub fn validate_destination_correction_commit_request(
    request: &DestinationCorrectionCommitRequest,
) -> Result<()> {
    request.validate_structure()?;
    for operation in &request.corrections {
        decode_destination_correction_value(operation)?;
    }
    let digest = correction_operations_digest(&request.corrections)?;
    if digest != request.operations_digest {
        return Err(CdfError::contract(format!(
            "destination correction operations digest {} does not match computed {}",
            request.operations_digest, digest
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::Int64Array;
    use arrow_schema::{DataType, Field};
    use cdf_kernel::{
        CanonicalArrowField, CorrectionStrategy, CursorPosition, CursorValue,
        DestinationCorrectionCommitRequest, DestinationCorrectionOperation,
        DestinationCorrectionPlan, DestinationCorrectionRequest, IdempotencySupport,
        IdempotencyToken, PackageHash, PromotionId, ResidualCorrectionOperation,
        RowProvenanceAddress, SchemaHash, ScopeKey, SegmentId, SourcePosition, StateSegment,
        TargetName, TransactionSupport, WriteDisposition,
    };

    use super::*;
    use crate::{ResidualFieldRef, encode_residual_json_v1};

    fn operation(path: &str, field: Field) -> DestinationCorrectionOperation {
        let values = Int64Array::from(vec![42_i64]);
        let exact = encode_residual_json_v1([ResidualFieldRef::new(
            [path.trim_start_matches('/')],
            &values,
            0,
        )
        .unwrap()])
        .unwrap();
        DestinationCorrectionOperation {
            correction: DestinationCorrectionPlan {
                request: DestinationCorrectionRequest {
                    promotion_id: PromotionId::new("promotion-contract").unwrap(),
                    original_row: RowProvenanceAddress::new(
                        PackageHash::new("sha256:original").unwrap(),
                        SegmentId::new("seg-original").unwrap(),
                        0,
                    ),
                    old_schema_hash: SchemaHash::new("sha256:old").unwrap(),
                    new_schema_hash: SchemaHash::new("sha256:new").unwrap(),
                    promoted_path: path.to_owned(),
                    promoted_value_json: "inspection-only".to_owned(),
                    residual_operation: ResidualCorrectionOperation::RemovePromotedPath,
                    selected_strategy: CorrectionStrategy::InPlaceUpdate,
                },
                transaction_guarantee: TransactionSupport::AtomicPackage,
                idempotency_guarantee: IdempotencySupport::PackageToken,
            },
            output_field: CanonicalArrowField::from_arrow(&field).unwrap(),
            promoted_value_residual_json_v1: exact,
        }
    }

    fn request(operation: DestinationCorrectionOperation) -> DestinationCorrectionCommitRequest {
        DestinationCorrectionCommitRequest::new(
            PackageHash::new("sha256:correction").unwrap(),
            IdempotencyToken::new("sha256:correction").unwrap(),
            TargetName::new("orders").unwrap(),
            WriteDisposition::Append,
            vec![StateSegment {
                segment_id: SegmentId::new("seg-correction").unwrap(),
                scope: ScopeKey::Resource,
                output_position: SourcePosition::Cursor(CursorPosition {
                    version: 1,
                    field: "correction".to_owned(),
                    value: CursorValue::U64(1),
                }),
                row_count: 1,
                byte_count: 1,
            }],
            vec![operation],
        )
        .unwrap()
    }

    #[test]
    fn exact_correction_value_authority_binds_path_and_arrow_type() {
        let request = request(operation("/age", Field::new("age", DataType::Int64, true)));
        validate_destination_correction_commit_request(&request).unwrap();
        let decoded = decode_destination_correction_value(&request.corrections[0]).unwrap();
        assert_eq!(decoded.data_type(), &DataType::Int64);

        let mut wrong_path = request.clone();
        wrong_path.corrections[0].correction.request.promoted_path = "/years".to_owned();
        wrong_path.operations_digest =
            correction_operations_digest(&wrong_path.corrections).unwrap();
        assert!(
            validate_destination_correction_commit_request(&wrong_path)
                .unwrap_err()
                .to_string()
                .contains("does not match promoted path")
        );

        let mut wrong_type = request.clone();
        wrong_type.corrections[0].output_field =
            CanonicalArrowField::from_arrow(&Field::new("age", DataType::Utf8, true)).unwrap();
        wrong_type.operations_digest =
            correction_operations_digest(&wrong_type.corrections).unwrap();
        assert!(
            validate_destination_correction_commit_request(&wrong_type)
                .unwrap_err()
                .to_string()
                .contains("does not match compiled output field")
        );
    }
}
