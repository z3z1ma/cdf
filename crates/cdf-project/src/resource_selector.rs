use std::{collections::BTreeMap, fmt, path::Path};

use cdf_kernel::CdfError;
use glob::{MatchOptions, Pattern};
use serde::{Deserialize, Serialize};

use crate::project_inputs::{
    ProjectResourcePath, inventory_project_resource_paths, resolve_exact_project_resource_path,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectResourceSelection {
    pub positive: Vec<String>,
    pub exclude: Vec<String>,
    pub resolved: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProjectResourceSelectionResolution {
    pub selection: ProjectResourceSelection,
    pub resources: Vec<ProjectResourcePath>,
    pub(crate) complete_resource_surface: bool,
}

#[derive(Debug)]
pub enum ProjectResourceSelectionError {
    Project(CdfError),
    InvalidPattern {
        selector: String,
        reason: String,
    },
    ExactNoMatch {
        selector: String,
        candidates: Vec<String>,
    },
    GlobNoMatch {
        selector: String,
    },
    ExcludeWithoutPositive,
    Empty,
}

impl ProjectResourceSelectionError {
    pub fn candidates(&self) -> &[String] {
        match self {
            Self::ExactNoMatch { candidates, .. } => candidates,
            _ => &[],
        }
    }
}

impl fmt::Display for ProjectResourceSelectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Project(error) => error.fmt(formatter),
            Self::InvalidPattern { selector, reason } => {
                write!(
                    formatter,
                    "invalid resource selector {selector:?}: {reason}"
                )
            }
            Self::ExactNoMatch { selector, .. } => {
                write!(
                    formatter,
                    "resource selector {selector:?} matched no resource"
                )
            }
            Self::GlobNoMatch { selector } => {
                write!(
                    formatter,
                    "resource selector glob {selector:?} matched no resource"
                )
            }
            Self::ExcludeWithoutPositive => write!(
                formatter,
                "--exclude requires at least one positive resource selector"
            ),
            Self::Empty => write!(
                formatter,
                "resource selectors resolved to an empty set after exclusions"
            ),
        }
    }
}

impl std::error::Error for ProjectResourceSelectionError {}

impl From<CdfError> for ProjectResourceSelectionError {
    fn from(error: CdfError) -> Self {
        Self::Project(error)
    }
}

pub fn resolve_project_resource_selection(
    project_root: &Path,
    positive: &[String],
    exclude: &[String],
) -> Result<ProjectResourceSelectionResolution, ProjectResourceSelectionError> {
    if positive.is_empty() {
        if !exclude.is_empty() {
            return Err(ProjectResourceSelectionError::ExcludeWithoutPositive);
        }
        let catalog = inventory_project_resource_paths(project_root)?;
        let resolved = catalog
            .resources
            .iter()
            .map(|resource| resource.resource_id.to_string())
            .collect();
        return Ok(ProjectResourceSelectionResolution {
            selection: ProjectResourceSelection {
                positive: Vec::new(),
                exclude: Vec::new(),
                resolved,
            },
            resources: catalog.resources,
            complete_resource_surface: true,
        });
    }

    let needs_catalog = positive.iter().any(|selector| is_glob(selector));
    let mut catalog = if needs_catalog {
        Some(inventory_project_resource_paths(project_root)?)
    } else {
        None
    };
    let mut selected = BTreeMap::<String, ProjectResourcePath>::new();
    for selector in positive {
        if is_glob(selector) {
            let pattern = compile_pattern(selector)?;
            let mut matched = false;
            let catalog = catalog.as_ref().ok_or_else(|| {
                ProjectResourceSelectionError::Project(CdfError::internal(
                    "resource selector glob lost its path catalog",
                ))
            })?;
            for resource in &catalog.resources {
                if matches(&pattern, resource.resource_id.as_str()) {
                    matched = true;
                    selected.insert(resource.resource_id.to_string(), resource.clone());
                }
            }
            if !matched {
                return Err(ProjectResourceSelectionError::GlobNoMatch {
                    selector: selector.clone(),
                });
            }
        } else {
            match resolve_exact_project_resource_path(project_root, selector) {
                Ok(Some(resource)) => {
                    selected.insert(resource.resource_id.to_string(), resource);
                }
                Ok(None) => {
                    let catalog = match catalog.as_ref() {
                        Some(catalog) => catalog,
                        None => catalog.insert(inventory_project_resource_paths(project_root)?),
                    };
                    return Err(ProjectResourceSelectionError::ExactNoMatch {
                        selector: selector.clone(),
                        candidates: catalog
                            .resources
                            .iter()
                            .map(|resource| resource.resource_id.to_string())
                            .collect(),
                    });
                }
                Err(error) => {
                    return Err(ProjectResourceSelectionError::InvalidPattern {
                        selector: selector.clone(),
                        reason: error.message,
                    });
                }
            }
        }
    }

    let exclusions = exclude
        .iter()
        .map(|selector| compile_pattern(selector).map(|pattern| (selector, pattern)))
        .collect::<Result<Vec<_>, _>>()?;
    selected.retain(|resource_id, _| {
        !exclusions
            .iter()
            .any(|(_, pattern)| matches(pattern, resource_id))
    });
    if selected.is_empty() {
        return Err(ProjectResourceSelectionError::Empty);
    }
    let complete_resource_surface = catalog
        .as_ref()
        .is_some_and(|catalog| selected.len() == catalog.resources.len());
    let (resolved, resources) = selected.into_iter().unzip();
    Ok(ProjectResourceSelectionResolution {
        selection: ProjectResourceSelection {
            positive: positive.to_vec(),
            exclude: exclude.to_vec(),
            resolved,
        },
        resources,
        complete_resource_surface,
    })
}

fn is_glob(selector: &str) -> bool {
    selector
        .bytes()
        .any(|byte| matches!(byte, b'*' | b'?' | b'['))
}

fn compile_pattern(selector: &str) -> Result<Pattern, ProjectResourceSelectionError> {
    Pattern::new(selector).map_err(|error| ProjectResourceSelectionError::InvalidPattern {
        selector: selector.to_owned(),
        reason: error.to_string(),
    })
}

fn matches(pattern: &Pattern, resource_id: &str) -> bool {
    pattern.matches_with(
        resource_id,
        MatchOptions {
            case_sensitive: true,
            require_literal_separator: true,
            require_literal_leading_dot: true,
        },
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn project() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for path in [
            "cdf/alpha/one.cdf.sql",
            "cdf/alpha/two.cdf.sql",
            "cdf/beta/one.cdf.sql",
        ] {
            let path = root.path().join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, "SELECT 1").unwrap();
        }
        root
    }

    #[test]
    fn selector_union_exclusion_dedup_and_order_are_canonical() {
        let root = project();
        let selected = resolve_project_resource_selection(
            root.path(),
            &[
                "beta.one".to_owned(),
                "alpha.*".to_owned(),
                "alpha.o?e".to_owned(),
            ],
            &["*.two".to_owned(), "missing.*".to_owned()],
        )
        .unwrap();
        assert_eq!(selected.selection.resolved, ["alpha.one", "beta.one"]);
        assert_eq!(selected.resources.len(), 2);
    }

    #[test]
    fn bracket_classes_and_positive_match_requirements_are_enforced() {
        let root = project();
        let selected =
            resolve_project_resource_selection(root.path(), &["alpha.[ot]??".to_owned()], &[])
                .unwrap();
        assert_eq!(selected.selection.resolved, ["alpha.one", "alpha.two"]);

        let error = resolve_project_resource_selection(
            root.path(),
            &["alpha.*".to_owned(), "missing.*".to_owned()],
            &[],
        )
        .unwrap_err();
        assert!(matches!(
            error,
            ProjectResourceSelectionError::GlobNoMatch { .. }
        ));
    }

    #[test]
    fn exact_resolution_does_not_inventory_malformed_siblings() {
        let root = project();
        fs::write(root.path().join("cdf/alpha/broken.CDF.SQL"), "not SQL").unwrap();
        let selected =
            resolve_project_resource_selection(root.path(), &["beta.one".to_owned()], &[]).unwrap();
        assert_eq!(selected.selection.resolved, ["beta.one"]);

        let error =
            resolve_project_resource_selection(root.path(), &["*".to_owned()], &[]).unwrap_err();
        assert!(matches!(error, ProjectResourceSelectionError::Project(_)));
    }

    #[test]
    fn exact_miss_exposes_candidates_and_exclusions_cannot_empty_selection() {
        let root = project();
        let error = resolve_project_resource_selection(root.path(), &["alpha.on".to_owned()], &[])
            .unwrap_err();
        assert!(matches!(
            error,
            ProjectResourceSelectionError::ExactNoMatch { .. }
        ));
        assert_eq!(error.candidates().len(), 3);

        let error = resolve_project_resource_selection(
            root.path(),
            &["alpha.*".to_owned()],
            &["alpha.*".to_owned()],
        )
        .unwrap_err();
        assert!(matches!(error, ProjectResourceSelectionError::Empty));
    }

    #[test]
    fn unscoped_selection_is_explicitly_distinct_from_exclusion_only() {
        let root = project();
        let selected = resolve_project_resource_selection(root.path(), &[], &[]).unwrap();
        assert!(selected.selection.positive.is_empty());
        assert_eq!(selected.selection.resolved.len(), 3);
        assert!(selected.complete_resource_surface);

        let error = resolve_project_resource_selection(root.path(), &[], &["alpha.*".to_owned()])
            .unwrap_err();
        assert!(matches!(
            error,
            ProjectResourceSelectionError::ExcludeWithoutPositive
        ));
    }
}
