//! Configuration system for LrWorkspace Remote.
//!
//! Configuration is loaded from (in priority order):
//! 1. Command-line arguments
//! 2. Environment variables (LRWORKSPACE_*)
//! 3. User-level config: `~/.config/lrworkspace/config.toml`

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::error::CoreResult;

/// Top-level configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LrWorkspaceConfig {
    /// Maximum remote file size returned through MCP (default 1 MB).
    #[serde(default = "default_file_threshold")]
    pub file_threshold_bytes: u64,

    /// Maximum audit storage before cleanup prompt (default 1 GB).
    #[serde(default = "default_audit_max_bytes")]
    pub audit_max_bytes: u64,

    /// Sensitive file patterns to exclude from remote indexes and searches.
    #[serde(default = "default_sensitive_patterns")]
    pub sensitive_patterns: Vec<String>,

    /// SSH connection timeout in seconds.
    #[serde(default = "default_ssh_timeout")]
    pub ssh_timeout_secs: u64,

    /// Local directory for workspace metadata, temporary state, and audit logs.
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
}

fn default_file_threshold() -> u64 {
    1_048_576 // 1 MB
}

fn default_audit_max_bytes() -> u64 {
    1_073_741_824 // 1 GB
}

fn default_sensitive_patterns() -> Vec<String> {
    vec![
        ".env".into(),
        ".env.*".into(),
        "*.key".into(),
        "*.pem".into(),
        "*.pfx".into(),
        "*.p12".into(),
        "credentials*".into(),
        "secret*".into(),
        ".ssh/id_*".into(),
        ".aws/credentials".into(),
        ".gcloud/*.json".into(),
        "token*".into(),
        "*.token".into(),
    ]
}

fn default_ssh_timeout() -> u64 {
    120
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn operation_timeout_defaults_to_120_seconds_in_new_and_partial_configs() {
        assert_eq!(LrWorkspaceConfig::default().ssh_timeout_secs, 120);
        let config: LrWorkspaceConfig = toml::from_str("").unwrap();
        assert_eq!(config.ssh_timeout_secs, 120);
    }
}

/// A single home contract for LRW metadata and installation. Native SSH keeps
/// its own OS home/config resolution unless LRWORKSPACE_SSH_CONFIG is set.
/// LRWORKSPACE_HOME is an explicit portable/test override; Windows otherwise
/// prefers USERPROFILE to MSYS/Git's possibly POSIX-form HOME.
pub fn user_home()->PathBuf {
    if let Some(home)=std::env::var_os("LRWORKSPACE_HOME").filter(|s|!s.is_empty()){return PathBuf::from(home);}
    #[cfg(windows)] let home=std::env::var_os("USERPROFILE").or_else(||std::env::var_os("HOME"));
    #[cfg(not(windows))] let home=std::env::var_os("HOME");
    home.map(PathBuf::from).or_else(dirs::home_dir).unwrap_or_else(||PathBuf::from("."))
}

pub fn expand_user_path(path:&std::path::Path)->PathBuf {
    let text=path.to_string_lossy();if text=="~" {return user_home();}
    if let Some(relative)=text.strip_prefix("~/").or_else(||text.strip_prefix("~\\")){return user_home().join(relative);}
    path.to_path_buf()
}

fn default_data_dir()->PathBuf {user_home().join(".lrworkspace")}

impl Default for LrWorkspaceConfig {
    fn default() -> Self {
        Self {
            file_threshold_bytes: default_file_threshold(),
            audit_max_bytes: default_audit_max_bytes(),
            sensitive_patterns: default_sensitive_patterns(),
            ssh_timeout_secs: default_ssh_timeout(),
            data_dir: default_data_dir(),
        }
    }
}

impl LrWorkspaceConfig {
    /// Load configuration from standard locations.
    pub fn load() -> CoreResult<Self> {
        Self::load_with_overrides(None, None)
    }

    /// Load configuration with optional overrides from CLI flags.
    ///
    /// Priority (highest to lowest):
    /// 1. `data_dir` / `config_path` arguments
    /// 2. Environment variables (LRWORKSPACE_*)
    /// 3. User-level config: `~/.config/lrworkspace/config.toml`
    pub fn load_with_overrides(
        config_path: Option<&std::path::Path>,
        data_dir: Option<&std::path::Path>,
    ) -> CoreResult<Self> {
        let mut config = Self::default();

        let explicit_config = config_path.map(|p| p.to_path_buf())
            .or_else(|| std::env::var_os("LRWORKSPACE_CONFIG").map(PathBuf::from));
        // Explicit paths must exist; silently falling back can select the wrong store.
        if let Some(user_config) = explicit_config.clone()
            .or_else(|| dirs::config_dir().map(|d| d.join("lrworkspace").join("config.toml")))
        {
            if explicit_config.is_some() || user_config.exists() {
                let content = std::fs::read_to_string(&user_config)?;
                let user_cfg: LrWorkspaceConfig = toml::from_str(&content)?;
                config = config.merge(user_cfg);
            }
        }

        // Environment overrides precede explicit CLI arguments.
        config.apply_env_overrides()?;

        if let Some(dd) = data_dir {
            config.data_dir = dd.to_path_buf();
        }

        if config.ssh_timeout_secs == 0 {
            return Err(crate::error::CoreError::Validation("ssh_timeout_secs must be positive".into()));
        }

        config.data_dir=expand_user_path(&config.data_dir);
        if !config.data_dir.is_absolute(){return Err(crate::CoreError::Validation("data_dir must be an absolute path (or ~/...) so GUI clients do not depend on their working directory".into()));}

        // 4. Ensure data directory exists
        std::fs::create_dir_all(&config.data_dir)?;

        Ok(config)
    }

    /// Merge another config into self (other takes precedence for non-default values).
    pub fn merge(self, other: LrWorkspaceConfig) -> Self {
        Self {
            file_threshold_bytes: other.file_threshold_bytes,
            audit_max_bytes: other.audit_max_bytes,
            sensitive_patterns: if other.sensitive_patterns != default_sensitive_patterns() {
                other.sensitive_patterns
            } else {
                self.sensitive_patterns
            },
            ssh_timeout_secs: other.ssh_timeout_secs,
            data_dir: other.data_dir,
        }
    }

    fn apply_env_overrides(&mut self) -> CoreResult<()> {
        if let Ok(v) = std::env::var("LRWORKSPACE_FILE_THRESHOLD_MB") {
            self.file_threshold_bytes = v.parse::<u64>().ok()
                .and_then(|mb| mb.checked_mul(1_048_576))
                .ok_or_else(|| crate::error::CoreError::Validation(
                    "LRWORKSPACE_FILE_THRESHOLD_MB must be a non-negative integer that fits in bytes".into()
                ))?;
        }
        if let Ok(v) = std::env::var("LRWORKSPACE_SSH_TIMEOUT") {
            self.ssh_timeout_secs = v.parse::<u64>().ok().filter(|s| *s > 0)
                .ok_or_else(|| crate::error::CoreError::Validation(
                    "LRWORKSPACE_SSH_TIMEOUT must be a positive integer".into()
                ))?;
        }
        if let Ok(v) = std::env::var("LRWORKSPACE_DATA_DIR") {
            self.data_dir = PathBuf::from(v);
        }
        Ok(())
    }
}
