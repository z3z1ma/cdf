use crate::internal::*;
use crate::*;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InterpreterReport {
    pub executable: PathBuf,
    pub major: u8,
    pub minor: u8,
    pub micro: u8,
    pub implementation: String,
    pub gil_enabled: bool,
    pub free_threaded_build: bool,
}

impl InterpreterReport {
    pub fn version_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.micro)
    }

    pub fn can_parallelize_python(&self) -> bool {
        self.free_threaded_build && !self.gil_enabled
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterpreterRequirement {
    pub executable: Option<PathBuf>,
    pub min_major: u8,
    pub min_minor: u8,
    pub require_free_threaded: bool,
}

impl Default for InterpreterRequirement {
    fn default() -> Self {
        Self {
            executable: None,
            min_major: 3,
            min_minor: 12,
            require_free_threaded: false,
        }
    }
}

impl InterpreterRequirement {
    pub fn check(&self, report: &InterpreterReport) -> Result<()> {
        if (report.major, report.minor) < (self.min_major, self.min_minor) {
            return Err(CdfError::contract(format!(
                "Python interpreter {} is older than required {}.{}",
                report.version_string(),
                self.min_major,
                self.min_minor
            )));
        }
        if self.require_free_threaded && !report.can_parallelize_python() {
            return Err(CdfError::contract(
                "configured Python resource requires a free-threaded interpreter with the GIL disabled",
            ));
        }
        if let Some(expected) = &self.executable
            && !same_path(expected, &report.executable)?
        {
            return Err(CdfError::contract(format!(
                "configured Python interpreter `{}` does not match attached interpreter `{}`",
                expected.display(),
                report.executable.display()
            )));
        }
        Ok(())
    }
}

pub fn inspect_interpreter(py: Python<'_>) -> Result<InterpreterReport> {
    let sys = PyModule::import(py, "sys").map_err(py_error)?;
    let platform = PyModule::import(py, "platform").map_err(py_error)?;
    let sysconfig = PyModule::import(py, "sysconfig").map_err(py_error)?;
    let version_info = sys.getattr("version_info").map_err(py_error)?;
    let gil_enabled = match sys.getattr("_is_gil_enabled") {
        Ok(function) => function
            .call0()
            .and_then(|value| value.extract())
            .map_err(py_error)?,
        Err(_) => true,
    };
    let py_gil_disabled = sysconfig
        .call_method1("get_config_var", ("Py_GIL_DISABLED",))
        .ok()
        .and_then(|value| value.extract::<Option<i64>>().ok().flatten())
        .unwrap_or(0);

    Ok(InterpreterReport {
        executable: PathBuf::from(
            sys.getattr("executable")
                .map_err(py_error)?
                .extract::<String>()
                .map_err(py_error)?,
        ),
        major: version_info
            .getattr("major")
            .map_err(py_error)?
            .extract()
            .map_err(py_error)?,
        minor: version_info
            .getattr("minor")
            .map_err(py_error)?
            .extract()
            .map_err(py_error)?,
        micro: version_info
            .getattr("micro")
            .map_err(py_error)?
            .extract()
            .map_err(py_error)?,
        implementation: platform
            .call_method0("python_implementation")
            .map_err(py_error)?
            .extract()
            .map_err(py_error)?,
        gil_enabled,
        free_threaded_build: py_gil_disabled == 1,
    })
}

pub fn validate_attached_interpreter(
    executable: PathBuf,
    require_free_threaded: bool,
) -> Result<InterpreterReport> {
    let report = Python::attach(inspect_interpreter)?;
    InterpreterRequirement {
        executable: Some(executable),
        require_free_threaded,
        ..InterpreterRequirement::default()
    }
    .check(&report)?;
    Ok(report)
}

pub fn attached_interpreter_report() -> Result<InterpreterReport> {
    Python::attach(inspect_interpreter)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonConcurrencyMode {
    GilSerialized,
    FreeThreadedParallel,
    ParallelDisabled,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PythonExecutionSemantics {
    pub mode: PythonConcurrencyMode,
    pub requested_parallelism: usize,
    pub effective_parallelism: usize,
    pub holds_python_lock_while_producing: bool,
    pub rust_conversion_detaches_from_python: bool,
}

pub fn execution_semantics(
    interpreter: &InterpreterReport,
    resource_parallel: bool,
    requested_parallelism: usize,
) -> PythonExecutionSemantics {
    let requested_parallelism = requested_parallelism.max(1);
    if !resource_parallel {
        return PythonExecutionSemantics {
            mode: PythonConcurrencyMode::ParallelDisabled,
            requested_parallelism,
            effective_parallelism: 1,
            holds_python_lock_while_producing: true,
            rust_conversion_detaches_from_python: true,
        };
    }
    if interpreter.can_parallelize_python() {
        PythonExecutionSemantics {
            mode: PythonConcurrencyMode::FreeThreadedParallel,
            requested_parallelism,
            effective_parallelism: requested_parallelism,
            holds_python_lock_while_producing: false,
            rust_conversion_detaches_from_python: true,
        }
    } else {
        PythonExecutionSemantics {
            mode: PythonConcurrencyMode::GilSerialized,
            requested_parallelism,
            effective_parallelism: 1,
            holds_python_lock_while_producing: true,
            rust_conversion_detaches_from_python: true,
        }
    }
}

pub fn python_execution_lane_spec(
    semantics: &PythonExecutionSemantics,
) -> cdf_runtime::BlockingLaneSpec {
    cdf_runtime::BlockingLaneSpec {
        lane_id: "python.source".to_owned(),
        maximum_concurrency: u16::try_from(semantics.effective_parallelism)
            .unwrap_or(u16::MAX)
            .max(1),
        cpu_slot_cost: 1,
        native_internal_parallelism: 1,
        affinity: cdf_runtime::LaneAffinity::Shared,
        interruption: cdf_runtime::InterruptionSafety::CooperativeOnly,
    }
}
