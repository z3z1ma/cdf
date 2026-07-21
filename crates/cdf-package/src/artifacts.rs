use std::{fs::File, io::BufReader, path::Path};

use cdf_kernel::Result;
use serde::Deserialize;

use crate::{
    json::json_error,
    storage::{io_error, package_path},
};

pub(crate) fn read_json_artifact<T: for<'de> Deserialize<'de>>(
    package_dir: &Path,
    relative_path: &str,
) -> Result<T> {
    let path = package_path(package_dir, relative_path);
    let file =
        File::open(&path).map_err(|error| io_error(format!("open {}", path.display()), error))?;
    serde_json::from_reader(BufReader::new(file)).map_err(json_error)
}

pub(crate) fn read_optional_json_artifact<T: for<'de> Deserialize<'de>>(
    package_dir: &Path,
    relative_path: &str,
) -> Result<Option<T>> {
    let path = package_path(package_dir, relative_path);
    match File::open(&path) {
        Ok(file) => serde_json::from_reader(BufReader::new(file))
            .map(Some)
            .map_err(json_error),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_error(format!("open {}", path.display()), error)),
    }
}
