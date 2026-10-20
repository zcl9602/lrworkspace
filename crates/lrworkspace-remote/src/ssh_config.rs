//! SSH config parser — reads `~/.ssh/config` to resolve Host aliases,
//! HostName, User, Port, and ProxyJump directives.
//!
//! When the user specifies a host that matches an SSH config `Host` entry,
//! we automatically apply HostName, User, Port, and ProxyJump from the
//! config file. No custom JumpHost type in workspace config — the user's
//! existing `~/.ssh/config` is the single source of truth.

use std::path::PathBuf;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

/// A resolved host entry from `~/.ssh/config`.
#[derive(Debug, Clone, Default)]
pub struct SshHostEntry {
    pub hostname: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    pub proxy_jump: Option<String>,
    pub proxy_command: Option<String>,
    pub identity_file: Option<PathBuf>,
}

/// Parsed SSH config — a map from Host pattern to resolved settings.
#[derive(Debug, Clone, Default)]
pub struct SshConfig {
    entries: Vec<(String, SshHostEntry)>,
}

impl SshConfig {
    /// OpenSSH owns Include/Match/wildcard precedence. Resolve once during
    /// registration; a deliberately supplied port/user still takes precedence.
    pub fn effective(host:&str,user:Option<&str>,port:Option<u16>)->lrworkspace_core::CoreResult<SshHostEntry> {
        use lrworkspace_core::CoreError;
        if host.is_empty() || host.starts_with('-') || host.chars().any(|c|c.is_whitespace()||c.is_control()) {
            return Err(CoreError::Validation("SSH host must be a hostname, IP or config alias, not command options".into()));
        }
        let mut command=crate::openssh::ssh_command();command.arg("-G");
        if let Some(user)=user {command.arg("-l").arg(user);}
        if let Some(port)=port {if port==0{return Err(CoreError::Validation("SSH port must be 1-65535".into()));}command.arg("-p").arg(port.to_string());}
        command.arg("--").arg(host);
        let output=crate::process::capture(&mut command,std::time::Duration::from_secs(5),"OpenSSH configuration resolution")?;
        if !output.status.success(){return Err(CoreError::Validation(format!("ssh -G failed: {}",String::from_utf8_lossy(&output.stderr).trim())));}
        let mut entry=SshHostEntry::default();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let Some((key,value))=line.split_once(' ') else {continue};let value=value.trim();
            match key {
                "hostname"=>entry.hostname=Some(value.into()),"user"=>entry.user=Some(value.into()),"port"=>entry.port=value.parse().ok(),
                "proxycommand" if value!="none"=>entry.proxy_command=Some(value.into()),"proxyjump" if value!="none"=>entry.proxy_jump=Some(value.into()),
                "identityfile" if entry.identity_file.is_none()=>entry.identity_file=Some(resolve_path(value)),_=>{},
            }
        }
        if entry.port.is_none() || entry.user.is_none(){return Err(CoreError::Validation("ssh -G did not return a user and port; verify the selected native OpenSSH executable".into()));}
        Ok(entry)
    }

    /// Load and parse `~/.ssh/config`.
    pub fn load() -> Self {
        let path = local_home_dir().join(".ssh").join("config");

        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(_) => return Self::default(),
        };

        Self::parse(&content)
    }

    fn parse(content: &str) -> Self {
        let mut config = Self::default();
        let mut current_hosts: Vec<String> = Vec::new();
        let mut current_entry = SshHostEntry::default();

        for line in content.lines() {
            let line = line.trim();
            // Skip comments and blanks
            if line.is_empty() || line.starts_with('#') {
                continue;
            }

            let parts: Vec<&str> = line.splitn(2, [' ', '\t']).collect();
            if parts.len() < 2 {
                continue;
            }
            let keyword = parts[0].to_lowercase();
            let value = parts[1].trim().to_string();

            match keyword.as_str() {
                "host" => {
                    // Save previous entry
                    if !current_hosts.is_empty() {
                        for h in &current_hosts {
                            config.entries.push((h.clone(), current_entry.clone()));
                        }
                    }
                    current_hosts = value.split_whitespace().map(|s| s.to_string()).collect();
                    current_entry = SshHostEntry::default();
                }
                "hostname" => current_entry.hostname = Some(value),
                "port" => {
                    if let Ok(p) = value.parse() {
                        current_entry.port = Some(p);
                    }
                }
                "user" => current_entry.user = Some(value),
                "proxyjump" => {
                    current_entry.proxy_jump = Some(value);
                }
                "proxycommand" => {
                    current_entry.proxy_command = Some(value);
                }
                "identityfile" => {
                    current_entry.identity_file = Some(resolve_path(&value));
                }
                _ => {}
            }
        }

        // Save last entry
        if !current_hosts.is_empty() {
            for h in &current_hosts {
                config.entries.push((h.clone(), current_entry.clone()));
            }
        }

        config
    }

    /// Look up a host in the config. Returns the resolved entry if found,
    /// matching wildcard patterns like `*.example.com`.
    pub fn lookup(&self, host: &str) -> Option<SshHostEntry> {
        // Exact match first
        for (pattern, entry) in &self.entries {
            if pattern == host {
                return Some(entry.clone());
            }
        }
        // Wildcard match
        for (pattern, entry) in &self.entries {
            if pattern.starts_with('*') && host.ends_with(pattern.trim_start_matches('*')) {
                return Some(entry.clone());
            }
        }
        None
    }

    /// Build the proxy jump command that creates a tunnel.
    /// Returns `(jump_host, jump_port, target_host, target_port)` or None if no proxy.
    pub fn resolve_proxy_jump(&self, host: &str) -> Option<(String, u16, String, u16)> {
        let entry = self.lookup(host)?;
        let proxy = entry.proxy_jump.as_ref()?;

        // ProxyJump format: "user@host:port" or "host:port" or just "host"
        let proxy = proxy.trim();
        let (proxy_user_host, proxy_port_override) = if let Some((uh, p)) = proxy.rsplit_once(':') {
            (uh.to_string(), p.parse::<u16>().unwrap_or(22))
        } else {
            (proxy.to_string(), 22)
        };

        let (proxy_user, proxy_host) = if let Some((user, host)) = proxy_user_host.split_once('@') {
            (Some(user.to_string()), host.to_string())
        } else {
            // No user in ProxyJump — look up from SSH config if available
            let resolved_user = self.lookup(&proxy_user_host).and_then(|e| e.user);
            (resolved_user, proxy_user_host)
        };

        // Resolve jump host port: explicit port from ProxyJump string wins,
        // otherwise fall back to the jump host's SSH config entry port
        let resolved_jport = if proxy_port_override != 22 {
            proxy_port_override
        } else {
            self.lookup(&proxy_host).and_then(|e| e.port).unwrap_or(22)
        };

        // Resolve the proxy host through SSH config too (it might itself be an alias)
        let proxy_hostname = self
            .lookup(&proxy_host)
            .and_then(|e| e.hostname)
            .unwrap_or(proxy_host);

        let target_hostname = entry.hostname.unwrap_or_else(|| host.to_string());
        let target_port = entry.port.unwrap_or(22);

        // Build user@host for the jump
        let jump = if let Some(u) = proxy_user {
            format!("{}@{}", u, proxy_hostname)
        } else {
            proxy_hostname
        };

        Some((jump, resolved_jport, target_hostname, target_port))
    }

    /// Resolve the effective hostname for a given host alias.
    pub fn resolve_hostname(&self, host: &str) -> String {
        self.lookup(host)
            .and_then(|e| e.hostname)
            .unwrap_or_else(|| host.to_string())
    }

    /// Resolve the effective SSH port.
    pub fn resolve_port(&self, host: &str, default: u16) -> u16 {
        self.lookup(host).and_then(|e| e.port).unwrap_or(default)
    }

    /// SSH alias for display — omit `:22` when port is default.
    pub fn display_label(&self, host: &str, default_port: u16) -> String {
        let port = self.resolve_port(host, default_port);
        if port == 22 {
            host.to_string()
        } else {
            format!("{host}:{port}")
        }
    }

    /// Resolve the effective user.
    pub fn resolve_user(&self, host: &str) -> Option<String> {
        self.lookup(host).and_then(|e| e.user)
    }

    /// Resolve the identity file.
    pub fn resolve_identity_file(&self, host: &str) -> Option<PathBuf> {
        self.lookup(host).and_then(|e| e.identity_file)
    }
}

/// User-facing SSH target label (alias from `~/.ssh/config`, port only if non-default).
pub fn connection_display_label(workspace: &lrworkspace_core::types::Workspace) -> String {
    let connection = &workspace.connection;
    if connection.port == 22 { connection.host.clone() }
    else { format!("{}:{}", connection.host, connection.port) }
}

/// Stable OpenSSH ControlPath for background connection reuse.
///
/// Callers should prefer [`control_path_for_connection`] so the path matches
/// the registered user and port used by execution and transfer.
pub fn control_path(host: &str, user: &str, port: u16) -> PathBuf {
    let mut hasher = DefaultHasher::new();
    host.hash(&mut hasher);
    user.hash(&mut hasher);
    port.hash(&mut hasher);
    std::env::var_os("LRWORKSPACE_SSH_CONFIG").hash(&mut hasher);
    #[cfg(unix)]
    // macOS TMPDIR can exceed the Unix socket path limit. A short, private
    // directory per OS user also avoids cross-user master socket collisions.
    let directory=PathBuf::from("/tmp").join(format!("lrw-ssh-{}",unsafe{libc::geteuid()}));
    #[cfg(not(unix))]
    let directory=std::env::temp_dir().join("lrworkspace-ssh-control");
    directory.join(format!("{:016x}.sock",hasher.finish()))
}

#[cfg(unix)]
pub(crate) fn prepare_control_directory(socket:&std::path::Path)->lrworkspace_core::CoreResult<()> {
    use std::os::unix::fs::{DirBuilderExt,MetadataExt,PermissionsExt};
    let directory=socket.parent().ok_or_else(||lrworkspace_core::CoreError::Validation("SSH socket needs a parent directory".into()))?;
    match std::fs::DirBuilder::new().mode(0o700).create(directory) {Ok(())=>{},Err(e) if e.kind()==std::io::ErrorKind::AlreadyExists=>{},Err(e)=>return Err(e.into())}
    let metadata=std::fs::symlink_metadata(directory)?;
    if !metadata.is_dir()||metadata.uid()!=unsafe{libc::geteuid()}||metadata.permissions().mode()&0o077!=0 {
        return Err(lrworkspace_core::CoreError::Validation(format!("SSH control directory must be a private directory owned by this user: {}",directory.display())));
    }
    Ok(())
}

/// ControlPath using the same user/port resolution as [`exec_via_system_ssh`].
pub fn control_path_for_connection(config: &lrworkspace_core::types::ConnectionConfig) -> PathBuf {
    if let lrworkspace_core::AuthMethod::LocalPassword{session_id,..}=&config.auth {
        control_path(&format!("{}:local:{session_id}:{}",config.host,crate::local_auth::control_tag(*session_id)),&config.user,config.port)
    }else{control_path(&config.host, &config.user, config.port)}
}

fn resolve_path(path: &str) -> PathBuf {
    let path = path.trim();
    if path.starts_with('~') {
        return local_home_dir().join(path.trim_start_matches(['~', '/', '\\']));
    }
    PathBuf::from(path)
}

fn local_home_dir()->PathBuf {lrworkspace_core::config::user_home()}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn unix_control_socket_is_short_and_its_directory_is_private() {
        use std::os::unix::fs::{MetadataExt,PermissionsExt};
        let socket=control_path("fixture","alice",2222);
        assert!(socket.as_os_str().len()<100,"{}",socket.display());
        prepare_control_directory(&socket).unwrap();let metadata=std::fs::symlink_metadata(socket.parent().unwrap()).unwrap();
        assert_eq!(metadata.uid(),unsafe{libc::geteuid()});assert_eq!(metadata.permissions().mode()&0o077,0);
    }

    #[test]
    fn test_parse_simple() {
        let config =
            SshConfig::parse("Host myserver\n  HostName 10.0.0.1\n  User alice\n  Port 2222\n");
        let entry = config.lookup("myserver").unwrap();
        assert_eq!(entry.hostname.as_deref(), Some("10.0.0.1"));
        assert_eq!(entry.user.as_deref(), Some("alice"));
        assert_eq!(entry.port, Some(2222));
    }

    #[test]
    fn test_parse_proxy_jump() {
        let config = SshConfig::parse(
            "Host jump\n  HostName jump.example.com\n  User bob\n\nHost target\n  HostName internal.cluster\n  ProxyJump jump\n"
        );
        let entry = config.lookup("target").unwrap();
        assert_eq!(entry.hostname.as_deref(), Some("internal.cluster"));
        assert_eq!(entry.proxy_jump.as_deref(), Some("jump"));
    }

    #[test]
    fn test_resolve_proxy_jump() {
        let config = SshConfig::parse(
            "Host jump\n  HostName gw.example.com\n  User bob\n  Port 2222\n\nHost cluster\n  HostName login01.cluster.local\n  ProxyJump jump\n"
        );
        let (jump, jport, target, tport) = config.resolve_proxy_jump("cluster").unwrap();
        assert_eq!(jump, "bob@gw.example.com");
        assert_eq!(jport, 2222);
        assert_eq!(target, "login01.cluster.local");
        assert_eq!(tport, 22);
    }

    #[test]
    fn control_path_uses_explicit_registered_user_and_port() {
        let unique = "lrw-connection-test".to_string();

        let conn = lrworkspace_core::types::ConnectionConfig {
            host: unique.clone(),
            port: 22,
            user: "alice".into(),
            auth: lrworkspace_core::types::AuthMethod::Agent,
            jump_hosts: vec![],
        };
        let resolved = control_path_for_connection(&conn);
        let naive = control_path(&unique, "alice", 22);
        assert_eq!(resolved, naive);
        assert_ne!(resolved, control_path(&unique, "bob", 2222));

    }
}
