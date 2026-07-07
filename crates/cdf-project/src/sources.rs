use crate::*;

pub trait ResourceSourceResolver {
    fn resolve(&self, source: &str) -> Result<ResolvedResourceSource>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ResolvedResourceSource {
    Toml(String),
    Yaml(String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct InMemoryResourceSourceResolver {
    sources: BTreeMap<String, ResolvedResourceSource>,
}

impl InMemoryResourceSourceResolver {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_toml(mut self, source: impl Into<String>, contents: impl Into<String>) -> Self {
        self.sources
            .insert(source.into(), ResolvedResourceSource::Toml(contents.into()));
        self
    }

    pub fn with_yaml(mut self, source: impl Into<String>, contents: impl Into<String>) -> Self {
        self.sources
            .insert(source.into(), ResolvedResourceSource::Yaml(contents.into()));
        self
    }
}

impl ResourceSourceResolver for InMemoryResourceSourceResolver {
    fn resolve(&self, source: &str) -> Result<ResolvedResourceSource> {
        self.sources.get(source).cloned().ok_or_else(|| {
            CdfError::contract(format!("resource source `{source}` is not available"))
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileResourceSourceResolver {
    root: PathBuf,
}

impl FileResourceSourceResolver {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
}

impl ResourceSourceResolver for FileResourceSourceResolver {
    fn resolve(&self, source: &str) -> Result<ResolvedResourceSource> {
        let path = self.root.join(source);
        let contents = fs::read_to_string(&path).map_err(|error| {
            CdfError::contract(format!(
                "resource source `{source}` could not be read: {error}"
            ))
        })?;
        match path.extension().and_then(|extension| extension.to_str()) {
            Some("toml") => Ok(ResolvedResourceSource::Toml(contents)),
            Some("yaml" | "yml") => Ok(ResolvedResourceSource::Yaml(contents)),
            _ => Err(CdfError::contract(format!(
                "resource source `{source}` must be TOML or YAML"
            ))),
        }
    }
}
