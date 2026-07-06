use crate::internal::*;
use crate::*;

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct SecretRef(String);

impl SecretRef {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        SecretUri::new(value.clone())?;
        let (_, key) = split_secret_parts(&value)?;
        if key.trim().is_empty() {
            return Err(FirnError::contract("secret key cannot be empty"));
        }
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn provider(&self) -> Result<&str> {
        split_secret_parts(self.as_str()).map(|(provider, _)| provider)
    }

    pub fn key(&self) -> Result<&str> {
        split_secret_parts(self.as_str()).map(|(_, key)| key)
    }

    pub fn to_secret_uri(&self) -> Result<SecretUri> {
        SecretUri::new(self.as_str().to_owned())
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for SecretRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<String> for SecretRef {
    type Error = FirnError;

    fn try_from(value: String) -> Result<Self> {
        Self::new(value)
    }
}

impl From<SecretRef> for String {
    fn from(value: SecretRef) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EnvSecretProvider {
    vars: BTreeMap<String, String>,
    inherit_process: bool,
}

impl EnvSecretProvider {
    pub fn process() -> Self {
        Self {
            vars: BTreeMap::new(),
            inherit_process: true,
        }
    }

    pub fn from_map(
        vars: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        Self {
            vars: vars
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
            inherit_process: false,
        }
    }
}

impl SecretProvider for EnvSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        let (provider, key) = split_secret_uri(uri)?;
        if provider != "env" {
            return Err(FirnError::auth(format!(
                "secret provider `{provider}` is not handled by env provider"
            )));
        }
        if let Some(value) = self.vars.get(key) {
            return Ok(SecretValue::new(value.clone()));
        }
        if self.inherit_process {
            return env::var(key)
                .map(SecretValue::new)
                .map_err(|_| FirnError::auth(format!("secret {uri} is not resolvable")));
        }
        Err(FirnError::auth(format!("secret {uri} is not resolvable")))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileSecretProvider {
    root: Option<PathBuf>,
}

impl FileSecretProvider {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: Some(root.into()),
        }
    }

    pub fn without_root() -> Self {
        Self { root: None }
    }
}

impl SecretProvider for FileSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        let (provider, key) = split_secret_uri(uri)?;
        if provider != "file" {
            return Err(FirnError::auth(format!(
                "secret provider `{provider}` is not handled by file provider"
            )));
        }
        let path = if Path::new(key).is_absolute() {
            PathBuf::from(key)
        } else if let Some(root) = &self.root {
            root.join(key)
        } else {
            PathBuf::from(key)
        };
        let value = fs::read_to_string(&path)
            .map_err(|error| FirnError::auth(format!("secret {uri} is not resolvable: {error}")))?;
        Ok(SecretValue::new(value.trim_end_matches(['\r', '\n'])))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DefaultSecretProvider {
    env: EnvSecretProvider,
    file: FileSecretProvider,
}

impl Default for DefaultSecretProvider {
    fn default() -> Self {
        Self {
            env: EnvSecretProvider::process(),
            file: FileSecretProvider::without_root(),
        }
    }
}

impl DefaultSecretProvider {
    pub fn new(env: EnvSecretProvider, file: FileSecretProvider) -> Self {
        Self { env, file }
    }
}

impl SecretProvider for DefaultSecretProvider {
    fn resolve(&self, uri: &SecretUri) -> Result<SecretValue> {
        match split_secret_uri(uri)?.0 {
            "env" => self.env.resolve(uri),
            "file" => self.file.resolve(uri),
            "keychain" | "os-keychain" | "macos-keychain" => Err(FirnError::auth(format!(
                "secret provider `{}` is not available in this firn-project build",
                split_secret_uri(uri)?.0
            ))),
            provider => Err(FirnError::auth(format!(
                "secret provider `{provider}` is not configured"
            ))),
        }
    }
}
