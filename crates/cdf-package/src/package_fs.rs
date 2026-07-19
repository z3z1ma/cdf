use std::{
    io::{self, Read},
    path::{Path, PathBuf},
};

use cap_fs_ext::{DirExt, FollowSymlinks, MetadataExt, OpenOptionsFollowExt};
use cap_std::{
    ambient_authority,
    fs::{Dir, File, OpenOptions},
};
use cdf_kernel::{CdfError, Result};
use cdf_package_contract::{FileEntry, RECEIPTS_FILE, REQUIRED_DIRECTORIES, TRACE_FILE};
use sha2::{Digest, Sha256};

use crate::storage::{validate_canonical_relative_path, validate_portable_path_component};

/// A package directory anchored to an open filesystem capability.
///
/// All descendant lookup is one component at a time with symlink following
/// disabled. This is the sole reopened-package filesystem authority; callers
/// receive opened handles or values derived from them. Any exported pathname
/// spelling is diagnostic only and is never reopened as access authority.
pub(crate) struct PackageRoot {
    dir: Dir,
    path: PathBuf,
    identity: (u64, u64),
}

impl std::fmt::Debug for PackageRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PackageRoot")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PackageEntryKind {
    RegularFile,
    NonRegular,
}

impl PackageRoot {
    pub(crate) fn open(package_dir: &Path) -> Result<Self> {
        let absolute = if package_dir.is_absolute() {
            package_dir.to_path_buf()
        } else {
            std::env::current_dir()
                .map_err(|error| package_io_error("read current directory", error))?
                .join(package_dir)
        };
        let parent = absolute.parent().ok_or_else(|| {
            CdfError::data(format!(
                "package directory has no parent: {}",
                package_dir.display()
            ))
        })?;
        let name = absolute.file_name().ok_or_else(|| {
            CdfError::data(format!(
                "package directory has no final component: {}",
                package_dir.display()
            ))
        })?;
        let parent = Dir::open_ambient_dir(parent, ambient_authority()).map_err(|error| {
            package_io_error(
                format!("open package parent directory {}", parent.display()),
                error,
            )
        })?;
        let dir = parent.open_dir_nofollow(name).map_err(|error| {
            package_io_error(
                format!(
                    "open package directory {} without following links",
                    package_dir.display()
                ),
                error,
            )
        })?;
        let metadata = dir
            .dir_metadata()
            .map_err(|error| package_io_error("inspect package directory handle", error))?;
        if !metadata.is_dir() {
            return Err(CdfError::data(format!(
                "package path is not a directory: {}",
                package_dir.display()
            )));
        }
        Ok(Self {
            dir,
            path: absolute,
            identity: (metadata.dev(), metadata.ino()),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    pub(crate) fn same_object(&self, other: &Self) -> bool {
        self.identity == other.identity
    }

    pub(crate) fn read(&self, relative_path: &str) -> Result<Vec<u8>> {
        self.read_optional(relative_path)?.ok_or_else(|| {
            CdfError::data(format!(
                "package path is missing or not a regular file: {relative_path}"
            ))
        })
    }

    pub(crate) fn read_optional(&self, relative_path: &str) -> Result<Option<Vec<u8>>> {
        let Some(mut file) = self.try_open_regular_file(relative_path)? else {
            return Ok(None);
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|error| {
            package_io_error(format!("read package file {relative_path}"), error)
        })?;
        Ok(Some(bytes))
    }

    pub(crate) fn file_entry(&self, relative_path: &str) -> Result<Option<FileEntry>> {
        let Some(mut file) = self.try_open_regular_file(relative_path)? else {
            return Ok(None);
        };
        let mut hasher = Sha256::new();
        let byte_count = io::copy(&mut file, &mut hasher).map_err(|error| {
            package_io_error(format!("hash package file {relative_path}"), error)
        })?;
        Ok(Some(FileEntry {
            path: relative_path.to_owned(),
            byte_count,
            sha256: hex::encode(hasher.finalize()),
        }))
    }

    pub(crate) fn open_regular_file(&self, relative_path: &str) -> Result<File> {
        self.try_open_regular_file(relative_path)?.ok_or_else(|| {
            CdfError::data(format!(
                "package path is missing or not a regular file: {relative_path}"
            ))
        })
    }

    pub(crate) fn open_std_file(&self, relative_path: &str) -> Result<std::fs::File> {
        self.open_regular_file(relative_path).map(File::into_std)
    }

    fn try_open_regular_file(&self, relative_path: &str) -> Result<Option<File>> {
        validate_canonical_relative_path(relative_path)?;
        let mut components = relative_path.split('/').peekable();
        let mut directory = self
            .dir
            .try_clone()
            .map_err(|error| package_io_error("clone package root capability", error))?;
        while let Some(component) = components.next() {
            if components.peek().is_some() {
                directory = match directory.open_dir_nofollow(component) {
                    Ok(directory) => directory,
                    Err(error) if missing_or_nonregular(&error) => return Ok(None),
                    Err(error) => {
                        return Err(package_io_error(
                            format!(
                                "open package directory component {component:?} without following links"
                            ),
                            error,
                        ));
                    }
                };
                continue;
            }
            let mut options = OpenOptions::new();
            options.read(true).follow(FollowSymlinks::No);
            let file = match directory.open_with(component, &options) {
                Ok(file) => file,
                Err(error) if missing_or_nonregular(&error) => return Ok(None),
                Err(error) => {
                    return Err(package_io_error(
                        format!("open package file {relative_path} without following links"),
                        error,
                    ));
                }
            };
            if !file
                .metadata()
                .map_err(|error| {
                    package_io_error(format!("inspect package file {relative_path}"), error)
                })?
                .is_file()
            {
                return Ok(None);
            }
            return Ok(Some(file));
        }
        Err(CdfError::data("package file path cannot be empty"))
    }

    pub(crate) fn visit_identity_entries(
        &self,
        mut visit: impl FnMut(String, PackageEntryKind) -> Result<()>,
    ) -> Result<()> {
        for directory_name in REQUIRED_DIRECTORIES {
            self.visit_tree_entries(directory_name, &mut visit)?;
        }
        if let Some(kind) = self.entry_kind(TRACE_FILE)? {
            visit(TRACE_FILE.to_owned(), kind)?;
        }
        Ok(())
    }

    pub(crate) fn visit_tree_entries(
        &self,
        relative_directory: &str,
        mut visit: impl FnMut(String, PackageEntryKind) -> Result<()>,
    ) -> Result<()> {
        validate_canonical_relative_path(relative_directory)?;
        let Some(directory) = self.try_open_directory(relative_directory)? else {
            return Ok(());
        };
        visit_directory(&directory, relative_directory, &mut visit)
    }

    fn try_open_directory(&self, relative_path: &str) -> Result<Option<Dir>> {
        let mut directory = self
            .dir
            .try_clone()
            .map_err(|error| package_io_error("clone package root capability", error))?;
        for component in relative_path.split('/') {
            directory = match directory.open_dir_nofollow(component) {
                Ok(directory) => directory,
                Err(error) if missing_or_nonregular(&error) => return Ok(None),
                Err(error) => {
                    return Err(package_io_error(
                        format!(
                            "open package directory component {component:?} without following links"
                        ),
                        error,
                    ));
                }
            };
        }
        Ok(Some(directory))
    }

    fn entry_kind(&self, relative_path: &str) -> Result<Option<PackageEntryKind>> {
        let mut entries = self
            .dir
            .entries()
            .map_err(|error| package_io_error("read package root directory", error))?;
        for entry in &mut entries {
            let entry =
                entry.map_err(|error| package_io_error("read package root directory", error))?;
            if entry.file_name() != relative_path {
                continue;
            }
            let file_type = entry.file_type().map_err(|error| {
                package_io_error(format!("inspect package entry {relative_path}"), error)
            })?;
            return Ok(Some(if file_type.is_file() {
                PackageEntryKind::RegularFile
            } else {
                PackageEntryKind::NonRegular
            }));
        }
        Ok(None)
    }
}

fn visit_directory(
    directory: &Dir,
    prefix: &str,
    visit: &mut impl FnMut(String, PackageEntryKind) -> Result<()>,
) -> Result<()> {
    let entries = directory
        .entries()
        .map_err(|error| package_io_error(format!("read package directory {prefix}"), error))?;
    for entry in entries {
        let entry = entry
            .map_err(|error| package_io_error(format!("read package directory {prefix}"), error))?;
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(|| {
            CdfError::data(format!(
                "package directory {prefix} contains a non-UTF-8 entry"
            ))
        })?;
        validate_portable_path_component(name)?;
        let relative_path = format!("{prefix}/{name}");
        if relative_path == RECEIPTS_FILE {
            continue;
        }
        let file_type = entry.file_type().map_err(|error| {
            package_io_error(format!("inspect package entry {relative_path}"), error)
        })?;
        if file_type.is_dir() {
            let child = directory.open_dir_nofollow(name).map_err(|error| {
                package_io_error(
                    format!("open package directory {relative_path} without following links"),
                    error,
                )
            })?;
            visit_directory(&child, &relative_path, visit)?;
        } else if file_type.is_file() {
            visit(relative_path, PackageEntryKind::RegularFile)?;
        } else {
            visit(relative_path, PackageEntryKind::NonRegular)?;
        }
    }
    Ok(())
}

fn package_io_error(context: impl Into<String>, error: io::Error) -> CdfError {
    CdfError::internal(format!("{}: {error}", context.into()))
}

fn missing_or_nonregular(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn package_root_rejects_final_and_descendant_symlinks() {
        let temp = tempfile::tempdir().unwrap();
        let package = temp.path().join("package");
        fs::create_dir_all(package.join("data")).unwrap();
        fs::write(package.join("data/segment.arrow"), b"segment").unwrap();
        let root = PackageRoot::open(&package).unwrap();
        assert_eq!(
            root.read("data/segment.arrow").unwrap(),
            b"segment".to_vec()
        );

        #[cfg(unix)]
        {
            std::os::unix::fs::symlink("segment.arrow", package.join("data/link.arrow")).unwrap();
            assert!(root.read("data/link.arrow").is_err());
            std::os::unix::fs::symlink(&package, temp.path().join("package-link")).unwrap();
            assert!(PackageRoot::open(&temp.path().join("package-link")).is_err());
        }
    }

    #[test]
    fn package_root_keeps_the_opened_directory_when_its_name_is_replaced() {
        let temp = tempfile::tempdir().unwrap();
        let package = temp.path().join("package");
        fs::create_dir_all(package.join("data")).unwrap();
        fs::write(package.join("data/segment.arrow"), b"original").unwrap();
        let root = PackageRoot::open(&package).unwrap();

        let moved = temp.path().join("moved-package");
        fs::rename(&package, &moved).unwrap();
        fs::create_dir_all(package.join("data")).unwrap();
        fs::write(package.join("data/segment.arrow"), b"replacement").unwrap();

        assert_eq!(
            root.read("data/segment.arrow").unwrap(),
            b"original".to_vec()
        );
        assert!(!root.same_object(&PackageRoot::open(&package).unwrap()));
    }
}
