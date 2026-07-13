use crate::sql::*;
use crate::*;

#[derive(Debug)]
pub(crate) struct WriterLock {
    path: PathBuf,
}

impl WriterLock {
    pub(crate) fn acquire(path: PathBuf) -> Result<Self> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|error| io_error(format!("create {}", parent.display()), error))?;
        }

        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .map_err(|error| {
                if error.kind() == std::io::ErrorKind::AlreadyExists {
                    CdfError::destination(format!(
                        "DuckDB writer lock is already held at {}",
                        path.display()
                    ))
                } else {
                    io_error(format!("create {}", path.display()), error)
                }
            })?;
        use std::io::Write;
        writeln!(file, "pid={}", std::process::id())
            .map_err(|error| io_error(format!("write {}", path.display()), error))?;
        file.sync_all()
            .map_err(|error| io_error(format!("sync {}", path.display()), error))?;
        Ok(Self { path })
    }
}

impl Drop for WriterLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}
