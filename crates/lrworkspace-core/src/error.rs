use thiserror::Error;

#[derive(Error, Debug)]
pub enum CoreError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("YAML error: {0}")]
    Yaml(#[from] serde_yaml::Error),

    #[error("TOML error: {0}")]
    Toml(#[from] toml::ser::Error),

    #[error("TOML deserialize error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Permission denied: {0}")]
    PermissionDenied(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Connection error: {0}")]
    Connection(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("{0}")]
    Other(String),
}

pub type CoreResult<T> = Result<T, CoreError>;
