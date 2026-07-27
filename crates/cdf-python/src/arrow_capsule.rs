//! Arrow/Python FFI safety exception governed by
//! `.10x/decisions/compiler-enforced-rust-safety-walls.md`.

use std::{ffi::CStr, sync::Arc};

use arrow_array::{
    Array, RecordBatch, RecordBatchOptions, StructArray,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi},
    ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream},
};
use arrow_schema::{DataType, Field, Schema};
use pyo3::{
    Bound, PyAny, PyResult,
    exceptions::{PyTypeError, PyValueError},
    types::{PyAnyMethods, PyCapsule, PyCapsuleMethods, PyTuple, PyTupleMethods},
};

const ARROW_SCHEMA_CAPSULE_NAME: &CStr = c"arrow_schema";
const ARROW_ARRAY_CAPSULE_NAME: &CStr = c"arrow_array";
const ARROW_STREAM_CAPSULE_NAME: &CStr = c"arrow_array_stream";

pub(crate) fn import_record_batch(object: &Bound<'_, PyAny>) -> PyResult<RecordBatch> {
    let capsules = object.call_method0("__arrow_c_array__")?;
    let capsules = capsules.cast::<PyTuple>()?;
    if capsules.len() != 2 {
        return Err(PyTypeError::new_err(
            "__arrow_c_array__ must return exactly schema and array capsules",
        ));
    }
    let schema_capsule = capsules.get_item(0)?.cast_into::<PyCapsule>()?;
    let array_capsule = capsules.get_item(1)?.cast_into::<PyCapsule>()?;
    import_record_batch_capsules(&schema_capsule, &array_capsule)
}

fn import_record_batch_capsules(
    schema_capsule: &Bound<'_, PyCapsule>,
    array_capsule: &Bound<'_, PyCapsule>,
) -> PyResult<RecordBatch> {
    let schema_ptr = schema_capsule
        .pointer_checked(Some(ARROW_SCHEMA_CAPSULE_NAME))?
        .cast::<FFI_ArrowSchema>();
    let array_ptr = array_capsule
        .pointer_checked(Some(ARROW_ARRAY_CAPSULE_NAME))?
        .cast::<FFI_ArrowArray>();

    // SAFETY: both capsule identities were checked above. Arrow's `from_raw` moves the array
    // out of the producer capsule and replaces it with an empty released value, so exactly the
    // imported Arrow owner invokes the producer's release callback.
    let ffi_array = unsafe { FFI_ArrowArray::from_raw(array_ptr.as_ptr()) };
    // SAFETY: the checked schema capsule remains alive for the complete synchronous conversion.
    let ffi_schema = unsafe { schema_ptr.as_ref() };
    let field =
        Field::try_from(ffi_schema).map_err(|error| PyTypeError::new_err(error.to_string()))?;
    // SAFETY: the array owner and borrowed schema were imported from the two checked capsules,
    // and both remain live for this synchronous conversion.
    let data = unsafe { from_ffi(ffi_array, ffi_schema) }
        .map_err(|error| PyTypeError::new_err(error.to_string()))?;
    let DataType::Struct(fields) = field.data_type() else {
        return Err(PyTypeError::new_err(
            "Arrow C array for a record batch must have struct type",
        ));
    };
    let array = StructArray::from(data);
    if array.null_count() != 0 {
        return Err(PyValueError::new_err(
            "Arrow C record-batch struct must not contain top-level nulls",
        ));
    }
    RecordBatch::try_new_with_options(
        Arc::new(Schema::new_with_metadata(
            fields.clone(),
            field.metadata().clone(),
        )),
        array.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(array.len())),
    )
    .map_err(|error| PyValueError::new_err(error.to_string()))
}

pub(crate) fn import_record_batch_stream(
    object: &Bound<'_, PyAny>,
) -> PyResult<ArrowArrayStreamReader> {
    let capsule = object
        .call_method0("__arrow_c_stream__")?
        .cast_into::<PyCapsule>()?;
    import_record_batch_stream_capsule(&capsule)
}

fn import_record_batch_stream_capsule(
    capsule: &Bound<'_, PyCapsule>,
) -> PyResult<ArrowArrayStreamReader> {
    let stream_ptr = capsule
        .pointer_checked(Some(ARROW_STREAM_CAPSULE_NAME))?
        .cast::<FFI_ArrowArrayStream>();
    // SAFETY: the capsule identity was checked above. Arrow's `from_raw` moves the stream and
    // empties the capsule, transferring its release callback to the returned reader exactly once.
    let stream = unsafe { FFI_ArrowArrayStream::from_raw(stream_ptr.as_ptr()) };
    ArrowArrayStreamReader::try_new(stream).map_err(|error| PyTypeError::new_err(error.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use arrow_array::{Int64Array, RecordBatchIterator, RecordBatchReader, StringArray};
    use arrow_buffer::{Buffer, OffsetBuffer, ScalarBuffer};
    use cdf_kernel::{CdfError, PartitionId, ResourceId};
    use pyo3::{Python, types::PyModule};

    use super::*;

    struct TrackedAllocation {
        values: Box<[u8]>,
        releases: Arc<AtomicUsize>,
    }

    impl AsRef<[u8]> for TrackedAllocation {
        fn as_ref(&self) -> &[u8] {
            &self.values
        }
    }

    impl Drop for TrackedAllocation {
        fn drop(&mut self) {
            self.releases.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn fixture_batch() -> RecordBatch {
        RecordBatch::try_from_iter([("value", Arc::new(Int64Array::from(vec![1, 2, 3])) as _)])
            .unwrap()
    }

    fn tracked_values(releases: Arc<AtomicUsize>) -> StringArray {
        let owner = TrackedAllocation {
            values: b"cdf".to_vec().into_boxed_slice(),
            releases,
        };
        StringArray::new(
            OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, 1, 2, 3])),
            Buffer::from(bytes::Bytes::from_owner(owner)),
            None,
        )
    }

    fn tracked_batch(releases: Arc<AtomicUsize>) -> RecordBatch {
        RecordBatch::try_from_iter([(
            "value",
            Arc::new(tracked_values(releases)) as Arc<dyn Array>,
        )])
        .unwrap()
    }

    #[test]
    fn array_capsule_moves_ownership_and_preserves_batch() {
        Python::attach(|py| {
            let expected = fixture_batch();
            let field = Field::new_struct("", expected.schema_ref().fields().clone(), false)
                .with_metadata(expected.schema_ref().metadata().clone());
            let struct_array = StructArray::from(expected.clone());
            let schema_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowSchema::try_from(&field).unwrap(),
                ARROW_SCHEMA_CAPSULE_NAME,
            )
            .unwrap();
            let array_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowArray::new(&struct_array.to_data()),
                ARROW_ARRAY_CAPSULE_NAME,
            )
            .unwrap();

            let actual = import_record_batch_capsules(&schema_capsule, &array_capsule).unwrap();
            assert_eq!(actual, expected);
            let moved = array_capsule
                .pointer_checked(Some(ARROW_ARRAY_CAPSULE_NAME))
                .unwrap()
                .cast::<FFI_ArrowArray>();
            // SAFETY: the capsule remains alive and contains the moved-from FFI value.
            assert!(unsafe { moved.as_ref() }.release.is_none());
        });
    }

    #[test]
    fn stream_capsule_moves_ownership_and_preserves_batches() {
        Python::attach(|py| {
            let expected = fixture_batch();
            let iterator = RecordBatchIterator::new(
                vec![Ok(expected.clone()), Ok(expected.clone())].into_iter(),
                expected.schema(),
            );
            let capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowArrayStream::new(Box::new(iterator)),
                ARROW_STREAM_CAPSULE_NAME,
            )
            .unwrap();

            let mut reader = import_record_batch_stream_capsule(&capsule).unwrap();
            assert_eq!(reader.schema(), expected.schema());
            assert_eq!(
                reader.by_ref().collect::<Result<Vec<_>, _>>().unwrap(),
                [expected.clone(), expected]
            );
            let moved = capsule
                .pointer_checked(Some(ARROW_STREAM_CAPSULE_NAME))
                .unwrap()
                .cast::<FFI_ArrowArrayStream>();
            // SAFETY: the capsule remains alive and contains the moved-from FFI value.
            assert!(unsafe { moved.as_ref() }.release.is_none());
        });
    }

    #[test]
    fn array_release_runs_once_after_producer_deletion_and_downstream_thread_drop() {
        let releases = Arc::new(AtomicUsize::new(0));
        let imported = Python::attach(|py| {
            let expected = tracked_batch(Arc::clone(&releases));
            let field = Field::new_struct("", expected.schema_ref().fields().clone(), false);
            let struct_array = StructArray::from(expected);
            let schema_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowSchema::try_from(&field).unwrap(),
                ARROW_SCHEMA_CAPSULE_NAME,
            )
            .unwrap();
            let array_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowArray::new(&struct_array.to_data()),
                ARROW_ARRAY_CAPSULE_NAME,
            )
            .unwrap();
            let imported = import_record_batch_capsules(&schema_capsule, &array_capsule).unwrap();
            drop(array_capsule);
            drop(schema_capsule);
            assert_eq!(releases.load(Ordering::SeqCst), 0);
            imported
        });

        let observed = Arc::clone(&releases);
        std::thread::spawn(move || drop(imported)).join().unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn array_release_runs_once_when_downstream_cancels_an_outcome() {
        let releases = Arc::new(AtomicUsize::new(0));
        Python::attach(|py| {
            let expected = tracked_batch(Arc::clone(&releases));
            let field = Field::new_struct("", expected.schema_ref().fields().clone(), false);
            let struct_array = StructArray::from(expected);
            let schema_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowSchema::try_from(&field).unwrap(),
                ARROW_SCHEMA_CAPSULE_NAME,
            )
            .unwrap();
            let array_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowArray::new(&struct_array.to_data()),
                ARROW_ARRAY_CAPSULE_NAME,
            )
            .unwrap();
            let module = PyModule::from_code(
                py,
                c"
class Value:
    def __init__(self, schema, array):
        self.schema = schema
        self.array = array
    def __arrow_c_array__(self):
        return self.schema, self.array

def one(schema, array):
    yield Value(schema, array)
",
                c"cdf_release_probe.py",
                c"cdf_release_probe",
            )
            .unwrap();
            let iterable = module
                .getattr("one")
                .unwrap()
                .call1((schema_capsule, array_capsule))
                .unwrap();
            let bridge = crate::PythonResourceBridge::new(crate::PythonBridgeOptions::new(
                ResourceId::new("python.release").unwrap(),
                PartitionId::new("python-000001").unwrap(),
            ));
            let error = bridge
                .visit_python_foreign_iterable(&iterable, |_outcome, _kind| {
                    Err(CdfError::data("downstream cancelled release probe"))
                })
                .unwrap_err();
            assert_eq!(error.message, "downstream cancelled release probe");
        });
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn array_release_runs_once_when_import_rejects_the_physical_type() {
        let releases = Arc::new(AtomicUsize::new(0));
        Python::attach(|py| {
            let field = Field::new("value", DataType::Utf8, false);
            let values = tracked_values(Arc::clone(&releases));
            let schema_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowSchema::try_from(&field).unwrap(),
                ARROW_SCHEMA_CAPSULE_NAME,
            )
            .unwrap();
            let array_capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowArray::new(&values.to_data()),
                ARROW_ARRAY_CAPSULE_NAME,
            )
            .unwrap();
            let error = import_record_batch_capsules(&schema_capsule, &array_capsule).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("record batch must have struct type")
            );
        });
        assert_eq!(releases.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn stream_release_runs_once_when_reader_is_cancelled_before_eof() {
        let releases = Arc::new(AtomicUsize::new(0));
        Python::attach(|py| {
            let first = tracked_batch(Arc::clone(&releases));
            let second = tracked_batch(Arc::clone(&releases));
            let schema = first.schema();
            let iterator =
                RecordBatchIterator::new(vec![Ok(first), Ok(second)].into_iter(), schema);
            let capsule = PyCapsule::new_with_value(
                py,
                FFI_ArrowArrayStream::new(Box::new(iterator)),
                ARROW_STREAM_CAPSULE_NAME,
            )
            .unwrap();
            let mut reader = import_record_batch_stream_capsule(&capsule).unwrap();
            let first = reader.next().unwrap().unwrap();
            assert_eq!(first.num_rows(), 3);
            drop(first);
            assert_eq!(releases.load(Ordering::SeqCst), 1);
            drop(capsule);
            assert_eq!(releases.load(Ordering::SeqCst), 1);
            drop(reader);
        });
        assert_eq!(releases.load(Ordering::SeqCst), 2);
    }
}
