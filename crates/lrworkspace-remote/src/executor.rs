//! RemoteExecutor implementation — command execution, environment capture,
//! and connection health checks on the remote host.
//!
//! Wraps the SSH session layer with the high-level `RemoteExecutor` trait
//! contract. Handles output truncation, exit-code extraction, and proper
//! working-directory / environment setup for each command.

use async_trait::async_trait;
use std::collections::HashMap;
#[cfg(not(windows))]
use std::io::IsTerminal;
use std::io::Read;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
#[cfg(windows)]
use std::{
    process::{Child, Stdio},
    sync::{mpsc, Arc, Mutex},
};
use tracing::{debug, info, instrument, warn};
use uuid::Uuid;

use lrworkspace_core::error::{CoreError, CoreResult};
use lrworkspace_core::safety::{
    normalize_remote_absolute, remote_path_is_within, safe_join, shell_quote,
};
use lrworkspace_core::traits::RemoteExecutor;
use lrworkspace_core::types::*;

#[cfg(not(windows))]
use crate::ssh_config::SshConfig;

// ---------------------------------------------------------------------------
// Remote Executor Implementation
// ---------------------------------------------------------------------------

/// Maximum bytes to read from stdout/stderr before truncation.
const DEFAULT_MAX_OUTPUT_BYTES: usize = 10 * 1024 * 1024; // 10 MB

fn scheduler_control_command(command:&str)->String {
    // Match scheduler discovery without replacing an explicitly configured PATH.
    format!("export PATH=\"${{PATH:-/usr/local/bin:/usr/bin:/bin}}:/opt/pbs/bin:/opt/torque/bin:/usr/local/pbs/bin:/opt/gvpbs/gridview/pbs/dispatcher/bin:/opt/gvpbs/gridview/jobscheduler/pbs_gridview/dispatcher/src/cmds:/usr/local/slurm/bin:/opt/slurm/bin\"\n{command}")
}

/// Refresh the command manifest from the PATH produced by the user's login
/// shell. This intentionally uses shell globbing, never recursive `find`.
const BIN_PATH_REFRESH_SCRIPT: &str = r#"
umask 077
tmp="$HOME/.bin_path_file.$$"
trap 'rm -f "$tmp"' EXIT HUP INT TERM
if [ -f "$HOME/.bin_path_file" ] && ! grep -q '^# lrworkspace:bin_path' "$HOME/.bin_path_file" 2>/dev/null; then
    echo "LRW: refusing to overwrite user-owned $HOME/.bin_path_file (move or remove it first)" >&2
    exit 3
fi
{
    printf '# lrworkspace:bin_path\n'
    old_ifs="$IFS"
    IFS=:
    for dir in $PATH; do
        [ -n "$dir" ] || dir=.
        [ -d "$dir" ] || continue
        for path in "$dir"/*; do
            [ -f "$path" ] && [ -x "$path" ] || continue
            printf '%s\t%s\n' "${path##*/}" "$path"
        done
    done
    IFS="$old_ifs"
} | awk -F '\t' '/^# lrworkspace:bin_path$/ { print; next } !seen[$1]++' > "$tmp"
mv -f "$tmp" "$HOME/.bin_path_file"
trap - EXIT HUP INT TERM
"#;

/// Environment capture command — a single shell invocation that collects
/// everything needed for an EnvSnapshot.
const ENV_CAPTURE_SCRIPT: &str = r#"
echo "---OS---"
uname -s
echo "---OS_VERSION---"
uname -r
echo "---ARCH---"
uname -m
echo "---SHELL---"
basename "$SHELL" 2>/dev/null || echo "unknown"
echo "---HOME---"
echo "$HOME"
echo "---PWD---"
pwd
echo "---PATH---"
echo "$PATH"
echo "---ENV_VARS---"
env | grep -E '^(PATH=|SHELL=|CONDA_|VIRTUAL_ENV|PYTHONPATH|LD_LIBRARY|CUDA_|MODULE|LMOD_|SLURM_|PBS_|QSUB|BEDTOOLS|SAMTOOLS)' 2>/dev/null || true
echo "---PROGRAMS---"
grep -v '^$' "$HOME/.bin_path_file" 2>/dev/null | awk -F '\t' 'NF >= 2 { print "PROG:" $1 ":" $2 ":" }' || true
echo "---END---"
"#;

/// Production-grade implementation of `RemoteExecutor`.
///
/// ## Features
/// - SSH session pooling for connection reuse
/// - Timeout enforcement per command
/// - Output size limits with truncation markers
/// - Proper exit-code extraction (including signal handling)
/// - Working-directory switching
/// - Environment variable injection
/// - Idempotent environment snapshot via a single remote script
pub struct RemoteExecutorImpl {
    max_output_bytes: usize,
    timeout: Duration,
    #[cfg(windows)]
    windows_sessions: Arc<Mutex<HashMap<String, Vec<Arc<Mutex<WindowsSshSession>>>>>>,
}

impl RemoteExecutorImpl {
    /// SSH command / transfer timeout budget.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    async fn exec_system_ssh(
        &self,
        config: &ConnectionConfig,
        command: &str,
        login_shell: bool,
    ) -> CoreResult<(i32, String, String, bool)> {
        self.exec_system_ssh_with_timeout(config, command, login_shell, self.timeout)
            .await
    }

    async fn exec_system_ssh_with_timeout(
        &self,
        config: &ConnectionConfig,
        command: &str,
        login_shell: bool,
        timeout: Duration,
    ) -> CoreResult<(i32, String, String, bool)> {
        crate::local_auth::check_session(config)?;
        #[cfg(not(windows))]
        {
            exec_via_system_ssh(config, command, timeout, login_shell, self.max_output_bytes).await
        }

        #[cfg(windows)]
        {
            let config = config.clone();
            let command = command.to_string();
            let sessions = self.windows_sessions.clone();
            let deadline=std::time::Instant::now()+timeout;
            let limit=self.max_output_bytes;
            tokio::task::spawn_blocking(move || {
                let key = windows_session_key(&config);
                loop {
                    let candidate = {
                        let mut sessions = lock_before(&sessions, deadline)?;
                        let pool = sessions.entry(key.clone()).or_default();
                        pool.retain(|session| session.try_lock().map(|guard|guard.healthy).unwrap_or(true));
                        if let Some(idle) = pool.iter().find(|session|session.try_lock().is_ok()) {
                            Some(idle.clone())
                        } else if pool.len() < 32 {
                            // Spawn only reserves local pipes; authentication occurs outside the map lock.
                            let session = Arc::new(Mutex::new(WindowsSshSession::spawn(&config,
                                deadline.saturating_duration_since(std::time::Instant::now()))?));
                            pool.push(session.clone());
                            Some(session)
                        } else { None }
                    };
                    if let Some(session) = candidate {
                        if let Ok(mut guard) = session.try_lock() {
                            let result = guard.execute(&command, login_shell, deadline, limit);
                            if result.is_err() { guard.healthy=false; let _=guard.child.kill(); }
                            drop(guard);
                            // Keep at most two idle sessions, retaining every busy session.
                            if let Ok(mut sessions)=sessions.try_lock() {
                              if let Some(pool)=sessions.get_mut(&key) {
                                let mut idle=0;
                                pool.retain(|item|match item.try_lock() {
                                    Ok(guard)=> { idle+=1; guard.healthy && idle<=2 },
                                    Err(_)=>true,
                                });
                              }
                            }
                            return result;
                        }
                    }
                    if std::time::Instant::now()>=deadline {
                        return Err(CoreError::Timeout("SSH session pool deadline exceeded".into()));
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            })
            .await
            .map_err(|error| CoreError::Connection(format!("SSH session join error: {error}")))?
        }
    }
}

impl RemoteExecutorImpl {
    /// Create a new executor with the given timeout.
    pub fn new(timeout: Duration) -> Self {
        Self {
            max_output_bytes: DEFAULT_MAX_OUTPUT_BYTES,
            timeout,
            #[cfg(windows)]
            windows_sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Revoke a private login without deleting registration or remote files.
    pub fn close_local_session(&self,id:Uuid) {
        #[cfg(unix)]
        let previous=crate::local_auth::session_connection(id).map(|config| {
            let socket=crate::ssh_config::control_path_for_connection(&config);(config,socket)
        });
        crate::local_auth::remove_session(id);
        #[cfg(windows)] {
            let marker=format!(":local:{id}:");
            self.windows_sessions.lock().unwrap_or_else(|e|e.into_inner()).retain(|key,session|{
                if key.contains(&marker){
                    // Never wait on an in-flight command's session mutex. It
                    // owns an Arc until completion; dropping it then closes SSH.
                    for session in session {if let Ok(mut idle)=session.try_lock(){let _=idle.child.kill();}}
                    false
                }else{true}
            });
        }
        #[cfg(unix)] if let Some((config,socket))=previous {
            let _=crate::process::capture(crate::openssh::ssh_command_for(&config).args(["-F","none","-O","exit","-o"]).arg(format!("ControlPath={}",socket.display())).arg(&config.host),Duration::from_secs(1),"close local session");
        }
    }

    /// Override this call's budget while retaining pooled SSH sessions.
    pub fn with_timeout(&self, timeout: Duration) -> Self {
        Self {
            max_output_bytes: self.max_output_bytes,
            timeout,
            #[cfg(windows)]
            windows_sessions: self.windows_sessions.clone(),
        }
    }

    /// Override the max output bytes before truncation.
    pub fn with_max_output(mut self, bytes: usize) -> Self {
        self.max_output_bytes = bytes;
        self
    }

    /// Execute a read-only command on the login node (fast path, no login shell).
    pub async fn execute_read_only(
        &self,
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> CoreResult<ExecutionRecord> {
        // Queries stay non-login and share scheduler lookup with discovery.
        self.execute_scheduler_control(workspace, command, working_dir, env).await
    }

    /// Inspect the directory layout without changing remote permissions.
    pub async fn inspect_workspace_layout(
        &self,
        workspace: &Workspace,
    ) -> CoreResult<WorkspaceLayoutReport> {
        let work_path = normalize_remote_absolute(&workspace.remote_work_dir)?;
        let mut script = format!(
            "work={}\n\
             [ -d \"$work\" ] && echo work_exists=1 || echo work_exists=0\n\
             [ -r \"$work\" ] && echo work_readable=1 || echo work_readable=0\n\
             [ -w \"$work\" ] && echo work_writable=1 || echo work_writable=0\n\
             work_real=$(realpath -e -- \"$work\" 2>/dev/null || true)\n",
            shell_quote(&work_path)
        );
        for (index, data_dir) in workspace.remote_data_dirs.iter().enumerate() {
            let data_path = normalize_remote_absolute(data_dir)?;
            script.push_str(&format!(
                "data={}; [ -d \"$data\" ] && [ -r \"$data\" ] && echo data_{index}_readable=1 || echo data_{index}_readable=0; \
                 data_real=$(realpath -e -- \"$data\" 2>/dev/null || true); \
                 overlap=0; \
                 if [ -n \"$work_real\" ] && [ -n \"$data_real\" ]; then \
                   case \"$data_real/\" in \"$work_real/\"*) overlap=1;; esac; \
                   case \"$work_real/\" in \"$data_real/\"*) overlap=1;; esac; \
                 fi; echo data_{index}_overlap=$overlap\n",
                shell_quote(&data_path)
            ));
        }
        let record = self
            .execute_fast(
                workspace,
                &script,
                Some(Path::new("/")),
                &Default::default(),
            )
            .await?;
        if record.exit_code != Some(0) {
            let detail = record.stderr.trim();
            return Err(CoreError::Connection(if detail.is_empty() {
                format!(
                    "Workspace layout probe failed with exit status {:?}",
                    record.exit_code
                )
            } else {
                format!("Workspace layout probe failed: {detail}")
            }));
        }
        let flag = |name: &str| {
            record
                .stdout
                .lines()
                .any(|line| line.trim() == format!("{name}=1"))
        };
        let work_exists = flag("work_exists");
        let work_readable = flag("work_readable");
        let work_writable = flag("work_writable");
        let mut data_dirs_readable = Vec::new();
        let mut errors = Vec::new();
        if !work_exists {
            errors.push(format!(
                "remote_work_dir does not exist: {}",
                workspace.remote_work_dir.display()
            ));
        }
        if !work_readable {
            errors.push("remote_work_dir is not readable by the SSH identity".into());
        }
        if !work_writable {
            errors.push("remote_work_dir is not writable by the SSH identity".into());
        }
        for (index, data_dir) in workspace.remote_data_dirs.iter().enumerate() {
            let readable = flag(&format!("data_{index}_readable"));
            data_dirs_readable.push(readable);
            if !readable {
                errors.push(format!(
                    "remote_data_dir is missing or unreadable: {}",
                    data_dir.display()
                ));
            }
            if flag(&format!("data_{index}_overlap")) {
                errors.push(format!(
                    "remote_data_dir overlaps remote_work_dir: {}",
                    data_dir.display()
                ));
            }
        }
        Ok(WorkspaceLayoutReport {
            remote_work_dir: workspace.remote_work_dir.clone(),

            remote_data_dirs: workspace.remote_data_dirs.clone(),
            work_exists,
            work_readable,
            work_writable,
            data_dirs_readable,
            valid: errors.is_empty(),
            errors,
        })
    }

    /// Reject path traversal and symlink escape before an MCP file write.
    pub async fn validate_mcp_write_target(
        &self,
        workspace: &Workspace,
        relative_path: &Path,
    ) -> CoreResult<PathBuf> {
        let work_path = normalize_remote_absolute(&workspace.remote_work_dir)?;
        let target = safe_join(&workspace.remote_work_dir, relative_path)?;
        let target = normalize_remote_absolute(&target)?;
        let script = format!(
            "work=$(realpath -e -- {} 2>/dev/null) || exit 20\n\
             target=$(realpath -m -- {} 2>/dev/null) || exit 21\n\
             [ ! -d \"$target\" ] || exit 22\n\
             case \"$target\" in \"${{work%/}}\"/*) printf '%s' \"$target\";; *) exit 22;; esac",
            shell_quote(&work_path),
            shell_quote(&target)
        );
        let record = self
            .execute_fast(
                workspace,
                &script,
                Some(Path::new("/")),
                &Default::default(),
            )
            .await?;
        if record.exit_code != Some(0) || record.stdout.trim().is_empty() {
            return Err(CoreError::PermissionDenied(format!(
                "MCP write target escapes remote_work_dir (including through a symlink): {}",
                relative_path.display()
            )));
        }
        Ok(PathBuf::from(record.stdout.trim()))
    }

    /// Resolve a requested file and reject symlinks that escape its configured
    /// work/data root before an MCP read.
    pub async fn validate_mcp_read_target(
        &self,
        workspace: &Workspace,
        root_kind: RemoteRootKind,
        relative_path: &Path,
    ) -> CoreResult<PathBuf> {
        let root = workspace
            .remote_root_for(root_kind)
            .ok_or_else(|| CoreError::Validation(format!("Unknown remote root: {root_kind:?}")))?;
        let root_path = normalize_remote_absolute(root)?;
        let target = safe_join(root, relative_path)?;
        let target = normalize_remote_absolute(&target)?;
        let script = format!(
            "root=$(realpath -e -- {} 2>/dev/null) || exit 20\n\
             target=$(realpath -e -- {} 2>/dev/null) || exit 21\n\
             case \"$target\" in \"$root\"|\"${{root%/}}\"/*) printf '%s' \"$target\";; *) exit 22;; esac",
            shell_quote(&root_path),
            shell_quote(&target)
        );
        let record = self
            .execute_fast(
                workspace,
                &script,
                Some(Path::new("/")),
                &Default::default(),
            )
            .await?;
        if record.exit_code != Some(0) || record.stdout.trim().is_empty() {
            return Err(CoreError::PermissionDenied(format!(
                "MCP read target does not exist or escapes its configured remote root: {}",
                relative_path.display()
            )));
        }
        Ok(PathBuf::from(record.stdout.trim()))
    }

    /// Build the command script for an MCP workload. Only the working
    /// directory boundary is enforced (it must be inside `remote_work_dir`);
    /// the command itself runs under the SSH user's real permissions because
    /// LRW no longer provides an OS-level read-only sandbox.
    pub fn build_mcp_command(
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> CoreResult<String> {
        let cwd = Self::resolve_mcp_working_dir(workspace, working_dir)?;
        Self::build_command_string(command, Some(&cwd), env)
    }

    /// Resolve and validate the MCP working directory inside `remote_work_dir`.
    pub(crate) fn resolve_mcp_working_dir(
        workspace: &Workspace,
        working_dir: Option<&Path>,
    ) -> CoreResult<PathBuf> {
        match working_dir {
            None => Ok(PathBuf::from(normalize_remote_absolute(&workspace.remote_work_dir)?)),
            Some(path) if path.to_string_lossy().replace('\\', "/").starts_with('/') => {
                let normalized = normalize_remote_absolute(path)?;
                if !remote_path_is_within(Path::new(&normalized), &workspace.remote_work_dir)? {
                    return Err(CoreError::PermissionDenied(format!(
                        "MCP working_dir must be under remote_work_dir: {}",
                        path.display()
                    )));
                }
                Ok(PathBuf::from(normalized))
            }
            Some(path) => Ok(PathBuf::from(normalize_remote_absolute(&safe_join(&workspace.remote_work_dir, path)?)?)),
        }
    }

    /// Execute an MCP command under the SSH user's real permissions. The
    /// `working_dir` must be inside `remote_work_dir`; data-root protection is
    /// behavioral only — the file API rejects data writes and agents are
    /// instructed not to modify data roots.
    pub async fn execute_mcp_guarded(
        &self,
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> CoreResult<ExecutionRecord> {
        let cwd = Self::resolve_mcp_working_dir(workspace, working_dir)?;
        self.execute(workspace, command, Some(&cwd), env).await
    }

    /// Execute a small command without starting the user's login shell.
    ///
    /// This is intended for LRW internal probes and file metadata operations
    /// where sourcing shell startup files would dominate the cost.
    pub async fn execute_fast(
        &self,
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> CoreResult<ExecutionRecord> {
        self.execute_with_timeout(
            workspace,
            command,
            working_dir,
            env,
            self.timeout,
        )
        .await
    }

    /// Execute an internal file/metadata control command with the configured budget.
    pub async fn execute_control(
        &self,
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> CoreResult<ExecutionRecord> {
        self.execute_with_timeout(workspace, command, working_dir, env, self.timeout)
            .await
    }

    /// Scheduler control-plane commands use the same fallback locations as discovery.
    pub async fn execute_scheduler_control(&self,workspace:&Workspace,command:&str,working_dir:Option<&Path>,env:&HashMap<String,String>)->CoreResult<ExecutionRecord> {
        self.execute_control(workspace,&scheduler_control_command(command),working_dir,env).await
    }

    async fn execute_with_timeout(
        &self,
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
        timeout: Duration,
    ) -> CoreResult<ExecutionRecord> {
        let id = Uuid::new_v4();
        let started_at = chrono::Utc::now();
        let agent = AgentKind::CliAgent;
        let cwd = working_dir.unwrap_or(&workspace.remote_work_dir);
        let script = Self::build_command_string(command, Some(cwd), env)?;

        if system_ssh_available() {
            let (exit_code, stdout, stderr, output_truncated) = self
                .exec_system_ssh_with_timeout(
                    &workspace.connection,
                    &script,
                    false,
                    timeout,
                )
                .await?;
            let finished_at = chrono::Utc::now();
            return Ok(ExecutionRecord {
                id,
                workspace_id: workspace.id,
                triggered_by: agent,
                command: command.to_string(),
                working_dir: cwd.to_path_buf(),
                env: env.clone(),
                started_at,
                finished_at: Some(finished_at),
                exit_code: if exit_code == -1 {
                    None
                } else {
                    Some(exit_code)
                },
                stdout,
                stderr,
                output_truncated,
                remote_stdout_path: None,
                remote_stderr_path: None,
            });
        }

        self.execute(workspace, command, working_dir, env).await
    }

    /// Build the full command string with working directory and environment setup.
    fn build_command_string(
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> CoreResult<String> {
        let mut parts: Vec<String> = Vec::new();

        // Environment exports
        for (key, value) in env {
            if !is_valid_env_key(key) {
                return Err(CoreError::Validation(format!(
                    "Invalid environment key: {}",
                    key
                )));
            }
            parts.push(format!("export {}={}", key, shell_quote(value)));
        }

        // Change directory
        if let Some(dir) = working_dir {
            let dir = normalize_remote_absolute(dir)?;
            parts.push(format!("cd {} || exit 1", shell_quote(&dir)));
        }

        // The actual command
        parts.push(command.to_string());

        Ok(parts.join("\n"))
    }
}

#[async_trait]
impl RemoteExecutor for RemoteExecutorImpl {
    #[instrument(skip(self, workspace), fields(ws = %workspace.id))]
    async fn execute(
        &self,
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &HashMap<String, String>,
    ) -> CoreResult<ExecutionRecord> {
        let id = Uuid::new_v4();
        let started_at = chrono::Utc::now();
        let agent = AgentKind::CliAgent;

        let cwd = working_dir.unwrap_or(&workspace.remote_work_dir);
        let script = Self::build_command_string(command, Some(cwd), env)?;
        debug!(%command, cwd = %cwd.display(), "Executing remote command");

        // Primary transport: the system `ssh` binary with a multiplexed master
        // connection (authenticate once, then reuse for every later command).
        // The in-process ssh2 client is only used when `ssh` is unavailable.
        if !system_ssh_available() {
            if let Err(e) = crate::openssh::ensure_openssh_client() {
                warn!("OpenSSH client prerequisite missing: {e}");
            }
        }
        let use_system_ssh = system_ssh_available();
        debug!(
            host = %workspace.connection.host,
            use_system_ssh,
            "Remote execution transport selected"
        );
        if use_system_ssh {
            let (exit_code, stdout, stderr, output_truncated) = self
                .exec_system_ssh(&workspace.connection, &script, true)
                .await?;

            let finished_at = chrono::Utc::now();
            return Ok(ExecutionRecord {
                id,
                workspace_id: workspace.id,
                triggered_by: agent,
                command: command.to_string(),
                working_dir: cwd.to_path_buf(),
                env: env.clone(),
                started_at,
                finished_at: Some(finished_at),
                exit_code: if exit_code == -1 {
                    None
                } else {
                    Some(exit_code)
                },
                stdout,
                stderr,
                output_truncated,
                remote_stdout_path: None,
                remote_stderr_path: None,
            });
        }

        #[cfg(windows)]
        return Err(CoreError::NotFound(
            "Windows OpenSSH Client (ssh.exe and scp.exe) is required; LRW does not use WSL".into(),
        ));

        #[cfg(not(windows))]
        {
            // Unix fallback: direct TCP through the in-process ssh2 client.
            let session = create_session(&workspace.connection, self.timeout).await?;
            let command=command.to_string();let cwd=cwd.to_path_buf();let env=env.clone();
            let workspace_id=workspace.id;let limit=self.max_output_bytes;
            tokio::task::spawn_blocking(move || {
            let mut channel = session
                .channel_session()
                .map_err(|e| CoreError::Connection(format!("Failed to open channel: {}", e)))?;
            channel
                .exec(&script)
                .map_err(|e| CoreError::Connection(format!("Failed to exec command: {}", e)))?;
            let (stdout, stdout_truncated) =
                read_stream_with_limit(&mut channel, false, limit)?;
            let (stderr, stderr_truncated) =
                read_stream_with_limit(&mut channel, true, limit)?;
            channel.wait_close().map_err(|e| {
                CoreError::Connection(format!("Failed to wait for channel close: {}", e))
            })?;
            let exit_code = channel.exit_status().ok();
            let finished_at = chrono::Utc::now();

            Ok(ExecutionRecord {
                id,
                workspace_id,
                triggered_by: agent,
                command: command.to_string(),
                working_dir: cwd.to_path_buf(),
                env: env.clone(),
                started_at,
                finished_at: Some(finished_at),
                exit_code,
                stdout,
                stderr,
                output_truncated: stdout_truncated || stderr_truncated,
                remote_stdout_path: None,
                remote_stderr_path: None,
            })
            }).await.map_err(|error|CoreError::Connection(format!("SSH fallback worker failed: {error}")))?
        }
    }

    #[instrument(skip(self, workspace))]
    async fn check_connection(&self, workspace: &Workspace) -> CoreResult<bool> {
        debug!("Checking connection to {}", workspace.connection.host);

        if !system_ssh_available() {
            if let Err(e) = crate::openssh::ensure_openssh_client() {
                warn!("OpenSSH client prerequisite missing: {e}");
            }
        }
        let use_system_ssh = system_ssh_available();
        debug!(
            host = %workspace.connection.host,
            use_system_ssh,
            "Connection check transport selected"
        );
        if use_system_ssh {
            let (exit_code, stdout, _, _) = self
                .exec_system_ssh(&workspace.connection, "echo ok", false)
                .await?;
            let ok = exit_code == 0 && stdout.contains("ok");
            debug!(
                authenticated = ok,
                "Connection check (system ssh): {}", exit_code
            );
            return Ok(ok);
        }

        #[cfg(windows)]
        {
            warn!(
                "Windows OpenSSH Client is not available; WSL fallback is intentionally disabled"
            );
            Ok(false)
        }

        #[cfg(not(windows))]
        {
            // Unix fallback: in-process ssh2.
            match create_session(&workspace.connection, self.timeout).await {
                Ok(session) => {
                    let ok = session.authenticated();
                    debug!(authenticated = ok, "Connection check result");
                    Ok(ok)
                }
                Err(e) => {
                    warn!("Connection check failed: {}", e);
                    Ok(false)
                }
            }
        }
    }

    #[instrument(skip(self, workspace))]
    async fn capture_env(&self, workspace: &Workspace) -> CoreResult<EnvSnapshot> {
        info!(
            "Capturing environment snapshot from {}",
            workspace.connection.host
        );

        let refresh = Self::build_command_string(
            BIN_PATH_REFRESH_SCRIPT,
            Some(&workspace.remote_work_dir),
            &Default::default(),
        )?;
        let refresh_result = if system_ssh_available() {
            self.exec_system_ssh_with_timeout(
                &workspace.connection,
                &refresh,
                true,
                self.timeout,
            )
            .await
        } else {
            self.execute(
                workspace,
                BIN_PATH_REFRESH_SCRIPT,
                Some(&workspace.remote_work_dir),
                &Default::default(),
            )
            .await
            .map(|record| (record.exit_code.unwrap_or(-1), record.stdout, record.stderr, record.output_truncated))
        }?;
        if refresh_result.0 != 0 {
            return Err(CoreError::Connection(format!(
                "Executable manifest refresh failed: {}",
                refresh_result.2
            )));
        }

        let record = self
            .execute_mcp_guarded(
                workspace,
                ENV_CAPTURE_SCRIPT,
                Some(&workspace.remote_work_dir),
                &Default::default(),
            )
            .await?;
        if record.exit_code != Some(0) {
            return Err(CoreError::Connection(format!(
                "Environment capture failed: {}",
                record.stderr
            )));
        }
        parse_env_snapshot(workspace.id, &record.stdout)
    }
}

fn is_valid_env_key(key: &str) -> bool {
    let mut chars = key.chars();
    matches!(chars.next(), Some(c) if c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

impl RemoteExecutorImpl {
    /// Measure round-trip latency by timing `echo ping`.
    #[instrument(skip(self, workspace))]
    pub async fn measure_latency(&self, workspace: &Workspace) -> CoreResult<f64> {
        self.measure_latency_with_command_timeout(workspace, self.timeout)
            .await
    }

    async fn measure_latency_with_command_timeout(
        &self,
        workspace: &Workspace,
        command_timeout: Duration,
    ) -> CoreResult<f64> {
        let executor = Self {
            max_output_bytes: self.max_output_bytes,
            timeout: command_timeout,
            #[cfg(windows)]
            windows_sessions: self.windows_sessions.clone(),
        };
        let t0 = std::time::Instant::now();
        executor
            .execute_fast(workspace, "echo ping", None, &Default::default())
            .await?;
        let elapsed = t0.elapsed();
        Ok(elapsed.as_secs_f64() * 1000.0) // milliseconds
    }

    /// Full health check: connectivity + latency + error diagnosis.
    #[instrument(skip(self, workspace))]
    pub async fn check_health(&self, workspace: &Workspace) -> CoreResult<ConnectionStatus> {
        let connected = match self.check_connection(workspace).await {
            Ok(true) => true,
            Ok(false) => false,
            Err(e) => {
                return Ok(ConnectionStatus::from_workspace_health(
                    workspace,
                    false,
                    None,
                    Some(e.to_string()),
                    crate::ssh_config::connection_display_label(workspace),
                ));
            }
        };

        let latency_ms = if connected {
            self.measure_latency(workspace).await.ok()
        } else {
            None
        };

        Ok(ConnectionStatus::from_workspace_health(
            workspace,
            connected,
            latency_ms,
            if connected {
                None
            } else {
                Some("SSH authentication failed".into())
            },
            crate::ssh_config::connection_display_label(workspace),
        ))
    }
}

// ---------------------------------------------------------------------------
// Helper: create an SSH session
// ---------------------------------------------------------------------------

#[cfg(not(windows))]
async fn create_session(config: &ConnectionConfig, timeout: Duration) -> CoreResult<ssh2::Session> {
    open_ssh2_session(config, timeout).await
}

#[cfg(not(windows))]
pub(crate) async fn open_ssh2_session(
    config: &ConnectionConfig,
    timeout: Duration,
) -> CoreResult<ssh2::Session> {
    let config = config.clone();
    let username = config.user.clone();
    let ssh_cfg = SshConfig::load();

    // If SSH config has ProxyCommand or ProxyJump, the system's ssh binary
    // handles transport completely (nc tunnel, SOCKS proxy, etc.). We cannot
    // use ssh2 crate in this case — instead, callers should use `exec_via_system_ssh`.
    // Signal this by returning a special error.
    let entry = ssh_cfg.lookup(&config.host);
    let has_proxy = entry
        .as_ref()
        .map(|e| e.proxy_jump.is_some() || e.proxy_command.is_some())
        .unwrap_or(false);

    if has_proxy {
        return Err(CoreError::Connection(format!(
            "HOST_USES_SYSTEM_SSH:{}:{}",
            ssh_cfg.resolve_hostname(&config.host),
            config.port
        )));
    }

    tokio::task::spawn_blocking(move || {
        use std::net::TcpStream;

        let resolved_host = ssh_cfg.resolve_hostname(&config.host);
        let resolved_port = config.port;
        let resolved_user = username;

        let addr = format!("{}:{}", resolved_host, resolved_port);
        let tcp = TcpStream::connect_timeout(
            &addr
                .parse()
                .map_err(|e| CoreError::Connection(format!("Bad address {}: {}", addr, e)))?,
            timeout,
        )
        .map_err(|e| {
            if e.kind() == std::io::ErrorKind::TimedOut {
                CoreError::Timeout(format!("Timeout connecting to {}", addr))
            } else {
                CoreError::Connection(format!("TCP connect to {} failed: {}", addr, e))
            }
        })?;

        tcp.set_read_timeout(Some(timeout)).ok();
        tcp.set_write_timeout(Some(timeout)).ok();

        let mut session = ssh2::Session::new()
            .map_err(|e| CoreError::Connection(format!("Session create: {}", e)))?;

        session.set_tcp_stream(tcp);
        session
            .handshake()
            .map_err(|e| CoreError::Connection(format!("Handshake: {}", e)))?;

        authenticate_session(&mut session, &resolved_user, &config.auth)?;
        Ok(session)
    })
    .await
    .map_err(|e| CoreError::Connection(format!("Join error: {}", e)))?
}

/// Ensure a multiplexed SSH master connection exists for `config`.
///
/// If a master is already alive (established by an earlier command) this is a
/// no-op. When attached to an interactive terminal, a master is established
/// with inherited stdio so OpenSSH can prompt for a password or key passphrase
/// directly on the user's TTY — the secret goes straight to `ssh` and is never
/// seen, processed, or stored by LRW. In non-interactive contexts such as MCP,
/// no master is established here: the actual command reuses a master a
/// foreground command already set up, or authenticates via key/agent.
#[cfg(not(windows))]
pub(crate) fn ensure_ssh_master(
    config: &ConnectionConfig,
    resolved_user: &str,
    resolved_port: u16,
    control_path: &Path,
    timeout: Duration,
    interactive: bool,
) -> CoreResult<()> {
    crate::ssh_config::prepare_control_directory(control_path)?;
    if !interactive||crate::local_auth::is_local_password(config) {
        // No TTY to prompt on — let the real command try with BatchMode so it
        // either reuses key/agent auth or fails fast with a clear error. We
        // skip the probe entirely so a slow/hung ssh cannot stall the caller.
        return Ok(());
    }

    // Already up? Cheap probe against the control socket (local, fast).
    let started=std::time::Instant::now();
    let alive=crate::process::capture(crate::openssh::ssh_command_for(&config)
        .arg("-O").arg("check").arg("-o").arg(format!("ControlPath={}",control_path.display())).arg(&config.host),
        timeout.min(Duration::from_secs(2)),"SSH master probe").is_ok_and(|output|output.status.success());
    if alive {
        return Ok(());
    }

    let mut cmd = crate::openssh::ssh_command_for(&config);
    cmd.arg("-f")
        .arg("-N")
        .arg("-o")
        .arg(crate::local_auth::host_key_mode(&config))
        .arg("-o")
        .arg(format!("ConnectTimeout={}", timeout.as_secs().max(1)))
        .arg("-o")
        .arg("ControlMaster=auto")
        .arg("-o")
        .arg("ControlPersist=10m")
        .arg("-o")
        .arg(format!("ControlPath={}", control_path.display()))
        .arg("-o")
        .arg("ServerAliveInterval=60")
        .arg("-o")
        .arg("ServerAliveCountMax=3")
        .arg("-l")
        .arg(resolved_user);
    cmd.arg("-p").arg(resolved_port.to_string());
    if let AuthMethod::Key { path, .. } = &config.auth {
        cmd.arg("-i").arg(path);
    }
    cmd.arg(&config.host);
    // Inherit stdio so the password / passphrase prompt reaches the user.
    cmd.stdin(std::process::Stdio::inherit())
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());

    let remaining=timeout.checked_sub(started.elapsed()).filter(|d|!d.is_zero())
        .ok_or_else(||CoreError::Timeout("SSH authentication deadline exceeded".into()))?;
    let status=crate::process::inherited_status(&mut cmd,remaining,"SSH authentication")?;
    if !status.success() {
        return Err(CoreError::Connection(format!(
            "Failed to establish SSH connection to {} (authentication failed or cancelled)",
            config.host
        )));
    }
    Ok(())
}

#[cfg(windows)]
fn lock_before<T>(mutex:&Mutex<T>,deadline:std::time::Instant)->CoreResult<std::sync::MutexGuard<'_,T>> {
    loop {
        match mutex.try_lock() {
            Ok(guard)=>return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(_))=>return Err(CoreError::InvalidState("SSH session lock poisoned".into())),
            Err(std::sync::TryLockError::WouldBlock)=> {
                if std::time::Instant::now()>=deadline {return Err(CoreError::Timeout("SSH session lock deadline exceeded".into()));}
                std::thread::sleep(Duration::from_millis(2));
            }
        }
    }
}

#[cfg(windows)]
enum WindowsStreamEvent {
    Data(bool, Vec<u8>),
    Closed(bool),
}

#[cfg(windows)]
struct WindowsSshSession {
    child: Child,
    healthy: bool,
    writes: mpsc::SyncSender<String>,
    written: mpsc::Receiver<std::io::Result<()>>,
    events: mpsc::Receiver<WindowsStreamEvent>,
}

#[cfg(windows)]
impl WindowsSshSession {
    fn spawn(config: &ConnectionConfig, timeout: Duration) -> CoreResult<Self> {
        let resolved_user = config.user.clone();
        let resolved_port = config.port;
        let mut command = crate::openssh::ssh_command_for(&config);
        command
            .arg("-T")
            .arg("-o")
            .arg(crate::local_auth::host_key_mode(&config))
            .arg("-o")
            .arg(format!("ConnectTimeout={}", timeout.as_secs().max(1)))
            .arg("-o")
            .arg("ServerAliveInterval=60")
            .arg("-o")
            .arg("ServerAliveCountMax=3")
            .arg("-o")
            .arg(crate::local_auth::batch_mode(&config))
            .arg("-l")
            .arg(&resolved_user);
        command.arg("-p").arg(resolved_port.to_string());
        if let AuthMethod::Key { path, .. } = &config.auth {
            command.arg("-i").arg(path);
        }
        command.args(crate::local_auth::ssh_arguments(config,false)?);
        crate::local_auth::apply(config,&mut command,false)?;
        command
            .arg(&config.host)
            .arg("exec /bin/bash --noprofile --norc")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        configure_local_child_lifetime(&mut command);

        let mut child = command
            .spawn()
            .map_err(|error| CoreError::Connection(format!("persistent ssh failed: {error}")))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| CoreError::Connection("persistent ssh stdin unavailable".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| CoreError::Connection("persistent ssh stdout unavailable".into()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| CoreError::Connection("persistent ssh stderr unavailable".into()))?;
        let (writes,requests)=mpsc::sync_channel::<String>(1);
        let (ack,written)=mpsc::sync_channel(1);
        std::thread::spawn(move || {
            while let Ok(frame)=requests.recv() {
                let result=stdin.write_all(frame.as_bytes()).and_then(|_|stdin.flush());
                let failed=result.is_err();
                if ack.send(result).is_err() || failed {break;}
            }
        });
        let (sender, events) = mpsc::sync_channel(16);
        spawn_windows_stream_reader(stdout, true, sender.clone());
        spawn_windows_stream_reader(stderr, false, sender);
        Ok(Self {
            child,
            healthy: true,
            writes,
            written,
            events,
        })
    }

    fn execute(&mut self,command:&str,login_shell:bool,deadline:std::time::Instant,limit:usize)->CoreResult<(i32,String,String,bool)> {
        if !self.healthy {return Err(CoreError::Connection("SSH session is invalid after an earlier transport failure; command was not sent".into()));}
        let remaining=||deadline.checked_duration_since(std::time::Instant::now()).filter(|d|!d.is_zero())
            .ok_or_else(||CoreError::Timeout("SSH deadline exceeded (including session wait and pipe I/O)".into()));
        let token=Uuid::new_v4().simple().to_string();
        let frame=windows_remote_frame(command,login_shell,&token,remaining()?);
        remaining()?;
        self.writes.try_send(frame).map_err(|e|CoreError::Connection(format!("SSH writer unavailable: {e}")))?;
        self.written.recv_timeout(remaining()?).map_err(|e|match e {
            mpsc::RecvTimeoutError::Timeout=>CoreError::Timeout("SSH stdin write exceeded its deadline".into()),
            _=>CoreError::Connection("SSH writer stopped".into()),
        })?.map_err(|e|CoreError::Connection(format!("persistent ssh write failed: {e}")))?;
        let mut stdout=crate::stream::FrameReader::new(&token,true,limit);
        let mut stderr=crate::stream::FrameReader::new(&token,false,limit);
        while !stdout.done || !stderr.done {
            match self.events.recv_timeout(remaining()?) {
                Ok(WindowsStreamEvent::Data(is_stdout,bytes))=> {
                    if is_stdout {stdout.push(&bytes)?;} else {stderr.push(&bytes)?;}
                }
                Ok(WindowsStreamEvent::Closed(is_stdout)) if (is_stdout && !stdout.done)||(!is_stdout && !stderr.done)=>return Err(CoreError::Connection(format!("persistent SSH stream closed before the command frame completed: {}",stderr.diagnostic().trim()))),
                Ok(WindowsStreamEvent::Closed(_))=>{},
                Err(mpsc::RecvTimeoutError::Timeout)=>return Err(CoreError::Timeout("SSH command exceeded its deadline".into())),
                Err(_)=>return Err(CoreError::Connection(format!("persistent SSH readers stopped: {}",stderr.diagnostic().trim()))),
            }
        }
        let code=stdout.code.unwrap_or(-1);
        let (stdout,out_cut)=stdout.into_text();let (stderr,err_cut)=stderr.into_text();
        Ok((code,stdout,stderr,out_cut||err_cut))
    }

}

#[cfg(windows)]
impl Drop for WindowsSshSession {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

#[cfg(windows)]
fn spawn_windows_stream_reader<R: Read + Send + 'static>(
    mut stream: R,
    is_stdout: bool,
    sender: mpsc::SyncSender<WindowsStreamEvent>,
) {
    std::thread::spawn(move || {
        let mut buffer = [0_u8; 8192];
        loop {
            match stream.read(&mut buffer) {
                Ok(0) | Err(_) => {
                    let _ = sender.send(WindowsStreamEvent::Closed(is_stdout));
                    break;
                }
                Ok(size) => {
                    if sender
                        .send(WindowsStreamEvent::Data(is_stdout, buffer[..size].to_vec()))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    });
}

#[cfg(any(windows, test))]
fn windows_remote_frame(
    command: &str,
    login_shell: bool,
    token: &str,
    timeout: Duration,
) -> String {
    let encoded = crate::b64::encode(command.as_bytes());
    let runner = if login_shell {
        r#"lrw_remote_shell=${SHELL:-/bin/sh}
case "$lrw_remote_shell" in
    */zsh|*/bash|*/ksh)
        LRW_COMMAND="$lrw_command"
        export LRW_COMMAND
        lrw_spawn "$lrw_remote_shell" -lic -c "$LRW_COMMAND"
        ;;
    *) lrw_spawn /bin/sh -c "$lrw_command" ;;
esac"#
    } else {
        r#"lrw_spawn /bin/sh -c "$lrw_command""#
    };
    r#"printf '\036LRW_BEGIN:__TOKEN__\037'
printf '\036LRW_BEGIN:__TOKEN__\037' >&2
lrw_command=$(printf '%s' '__ENCODED__' | base64 -d)
(
    lrw_child=""
    lrw_group=0
    lrw_cleanup() {
        lrw_status=$?
        trap - INT TERM HUP EXIT
        if [ -n "$lrw_child" ]; then
            if [ "$lrw_group" = 1 ]; then
                kill -TERM "-$lrw_child" 2>/dev/null || true
            else
                kill -TERM "$lrw_child" 2>/dev/null || true
            fi
            sleep 0.2 2>/dev/null || true
            if [ "$lrw_group" = 1 ]; then
                kill -KILL "-$lrw_child" 2>/dev/null || true
            else
                kill -KILL "$lrw_child" 2>/dev/null || true
            fi
            wait "$lrw_child" 2>/dev/null || true
        fi
        exit "$lrw_status"
    }
    lrw_spawn() {
        if command -v setsid >/dev/null 2>&1; then
            lrw_group=1
            if command -v timeout >/dev/null 2>&1; then
                setsid timeout -k 2s __TIMEOUT__s "$@" &
            else
                setsid "$@" &
            fi
        else
            lrw_group=0
            if command -v timeout >/dev/null 2>&1; then
                timeout -k 2s __TIMEOUT__s "$@" &
            else
                "$@" &
            fi
        fi
        lrw_child=$!
    }
    trap lrw_cleanup INT TERM HUP EXIT
    __RUNNER__
    wait "$lrw_child"
    lrw_status=$?
    lrw_child=""
    trap - INT TERM HUP EXIT
    exit "$lrw_status"
)
lrw_status=$?
printf '\036LRW_END:__TOKEN__:%s\037' "$lrw_status"
printf '\036LRW_END:__TOKEN__\037' >&2
"#
    .replace("__TOKEN__", token)
    .replace("__ENCODED__", &encoded)
    .replace(
        "__TIMEOUT__",
        &timeout.as_secs().saturating_sub(1).max(1).to_string(),
    )
    .replace("__RUNNER__", runner)
}

#[cfg(windows)]
fn windows_session_key(config: &ConnectionConfig) -> String {
    let user = config.user.clone();
    let port = config.port;
    let identity = match &config.auth {
        AuthMethod::Key { path, .. } => path.to_string_lossy(),
        _ => std::borrow::Cow::Borrowed("agent"),
    };
    if let AuthMethod::LocalPassword{session_id,..}=&config.auth {format!("{}@{}:{port}:local:{session_id}:{}",user,config.host,crate::local_auth::control_tag(*session_id))}else{format!("{}@{}:{port}:{identity}", user, config.host)}
}

#[cfg(test)]
fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Run a shell command via the system's `ssh` binary.
///
/// This is the primary transport for all hosts. Unix uses ControlMaster
/// multiplexing; native Windows reuses a non-interactive Win32-OpenSSH session
/// per endpoint. A pure IP host with no SSH config entry works in both cases.
#[cfg(not(windows))]
async fn exec_via_system_ssh(
    config: &ConnectionConfig,
    command: &str,
    timeout: Duration,
    login_shell: bool,
    max_output: usize,
) -> CoreResult<(i32, String, String, bool)> {
    let config = config.clone();
    let command = command.to_string();
    let started=std::time::Instant::now();
    tokio::task::spawn_blocking(move || {
        let resolved_user = config.user.clone();
        let resolved_port = config.port;

        let control_path = crate::ssh_config::control_path_for_connection(&config);
        crate::ssh_config::prepare_control_directory(&control_path)?;

        // Foreground CLI has a TTY → allow ssh to prompt for a password or
        // passphrase while establishing the shared master. MCP has no TTY, so
        // it runs non-interactively and reuses the master (or key/agent auth).
        let interactive = std::io::stdin().is_terminal();
        let connection_budget=timeout.checked_sub(started.elapsed()).filter(|d|!d.is_zero()).ok_or_else(||CoreError::Timeout("SSH worker deadline exceeded".into()))?;
        ensure_ssh_master(
            &config,
            &resolved_user,
            resolved_port,
            &control_path,
            connection_budget,
            interactive,
        )?;

        let mut cmd = crate::openssh::ssh_command_for(&config);
        cmd.arg("-o")
            .arg(crate::local_auth::host_key_mode(&config))
            .arg("-o")
            .arg(format!("ConnectTimeout={}", timeout.as_secs().max(1)));
        #[cfg(not(windows))]
        cmd.arg("-o")
            .arg("ControlMaster=auto")
            .arg("-o")
            .arg("ControlPersist=10m")
            .arg("-o")
            .arg(format!("ControlPath={}", control_path.display()));
        cmd.arg("-o")
            .arg("ServerAliveInterval=60")
            .arg("-o")
            .arg("ServerAliveCountMax=3")
            // MCP has no interactive prompt; key or agent auth must succeed.
            .arg("-o")
            .arg(crate::local_auth::batch_mode(&config))
            .arg("-l")
            .arg(&resolved_user);
        cmd.arg("-p").arg(resolved_port.to_string());
        if let AuthMethod::Key { path, .. } = &config.auth {
            cmd.arg("-i").arg(path);
        }
        cmd.args(crate::local_auth::ssh_arguments(&config,false)?);
        crate::local_auth::apply(&config,&mut cmd,false)?;
        cmd.arg(&config.host);

        if login_shell {
            cmd.arg("remote_shell=${SHELL:-/bin/sh}; case \"$remote_shell\" in */zsh|*/bash|*/ksh) exec \"$remote_shell\" -lic 'exec bash -s' ;; *) exec /bin/sh -s ;; esac");
        } else {
            cmd.arg("exec /bin/sh -s");
        }
        cmd.stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        configure_local_child_lifetime(&mut cmd);

        let remaining=timeout.checked_sub(started.elapsed()).filter(|d|!d.is_zero())
            .ok_or_else(||CoreError::Timeout("SSH connection exceeded its deadline".into()))?;
        let wrapped=remote_lifetime_wrapper(&command,login_shell,remaining);
        let (output,truncated)=crate::process::capture_input(&mut cmd,wrapped.as_bytes(),remaining,max_output,"ssh command")?;
        let text=|bytes:Vec<u8>| {let mut output=crate::stream::OutputBuffer::new(max_output);output.push(&bytes);output.into_text()};
        let (stdout,out_cut)=text(output.stdout);let (stderr,err_cut)=text(output.stderr);
        Ok((output.status.code().unwrap_or(-1),stdout,stderr,truncated||out_cut||err_cut))

    })
    .await
    .map_err(|e| CoreError::Connection(format!("Join error: {}", e)))?
}

#[cfg(not(windows))]
fn remote_lifetime_wrapper(command: &str, prefer_bash: bool, timeout: Duration) -> String {
    let encoded = crate::b64::encode(command.as_bytes());
    let shell_probe = if prefer_bash {
        r#"if command -v bash >/dev/null 2>&1; then lrw_shell="$(command -v bash)"; else lrw_shell="/bin/sh"; fi"#
    } else {
        r#"lrw_shell="/bin/sh""#
    };

    format!(
        r#"lrw_command=$(printf '%s' '{encoded}' | base64 -d)
{shell_probe}
lrw_child=""
lrw_cleanup() {{
    lrw_status=$?
    trap - INT TERM HUP EXIT
    if [ -n "$lrw_child" ]; then
        kill -TERM "-$lrw_child" 2>/dev/null || kill -TERM "$lrw_child" 2>/dev/null || true
        sleep 0.2 2>/dev/null || true
        kill -KILL "-$lrw_child" 2>/dev/null || kill -KILL "$lrw_child" 2>/dev/null || true
        wait "$lrw_child" 2>/dev/null || true
    fi
    exit "$lrw_status"
}}
trap lrw_cleanup INT TERM HUP EXIT
if command -v setsid >/dev/null 2>&1 && command -v timeout >/dev/null 2>&1; then
    setsid timeout -k 2s {timeout_secs}s "$lrw_shell" -c "$lrw_command" &
elif command -v setsid >/dev/null 2>&1; then
    setsid "$lrw_shell" -c "$lrw_command" &
elif command -v timeout >/dev/null 2>&1; then
    timeout -k 2s {timeout_secs}s "$lrw_shell" -c "$lrw_command" &
else
    "$lrw_shell" -c "$lrw_command" &
fi
lrw_child=$!
wait "$lrw_child"
lrw_status=$?
trap - INT TERM HUP EXIT
exit "$lrw_status"
"#,
        timeout_secs = timeout.as_secs().saturating_sub(1).max(1),
    )
}

#[cfg(unix)]
fn configure_local_child_lifetime(cmd: &mut std::process::Command) {
    use std::os::unix::process::CommandExt;
    unsafe {
        cmd.pre_exec(|| {
            // Put ssh in its own process group so timeout cleanup does not
            // signal the LRW parent, and ask the kernel to terminate ssh if
            // the parent process dies abruptly.
            libc::setpgid(0, 0);
            #[cfg(target_os = "linux")]
            {
                libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_local_child_lifetime(_cmd: &mut std::process::Command) {}

/// Whether the system `ssh` binary is available on `PATH`.
///
/// System ssh is the primary transport (ControlMaster reuse on Unix).
/// Only when it is missing do we fall back to the in-process ssh2 client.
pub(crate) fn system_ssh_available() -> bool {
    crate::openssh::openssh_client_available()
}

#[cfg(not(windows))]
fn authenticate_session(
    session: &mut ssh2::Session,
    username: &str,
    auth: &AuthMethod,
) -> CoreResult<()> {
    match auth {
        AuthMethod::Agent => {
            session.userauth_agent(username).map_err(|e|CoreError::Connection(format!("Agent auth for '{}': {}",username,e)))?;
        }
        AuthMethod::Key { path, passphrase } => {
            session
                .userauth_pubkey_file(username, None, path, passphrase.as_ref().map(|value|value.expose()))
                .map_err(|e| {
                    CoreError::Connection(format!(
                        "Key auth for '{}' with {}: {}",
                        username,
                        path.display(),
                        e
                    ))
                })?;
        }
        AuthMethod::Password { password } => {
            session.userauth_password(username, password.expose()).map_err(|_| {
                CoreError::Connection(format!("Password auth failed for '{}'", username))
            })?;
        }
        AuthMethod::LocalPassword{..}=>return Err(CoreError::PermissionDenied("Private password sessions require native OpenSSH".into())),
    }

    if !session.authenticated() {
        return Err(CoreError::Connection(
            "Not authenticated after auth attempt".into(),
        ));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helper: read from stream with size limit
// ---------------------------------------------------------------------------

/// Read from a channel stream (stdout or stderr) up to `max_bytes`.
/// Returns `(content, was_truncated)`.
#[cfg(not(windows))]
fn read_stream_with_limit(
    channel: &mut ssh2::Channel,
    stderr: bool,
    max_bytes: usize,
) -> CoreResult<(String, bool)> {
    let mut stream = if stderr {
        channel.stream(1) // extended data (stderr)
    } else {
        channel.stream(0) // stdout
    };

    let mut buf = vec![0u8; 8192];
    let mut output = Vec::with_capacity(8192);
    let mut truncated = false;

    loop {
        match stream.read(&mut buf) {
            Ok(0) => break, // EOF — stream is done
            Ok(n) => {
                if output.len() + n > max_bytes {
                    let remaining = max_bytes - output.len();
                    output.extend_from_slice(&buf[..remaining]);
                    truncated = true;
                    // Drain the rest
                    let mut drain = [0u8; 4096];
                    while stream.read(&mut drain).unwrap_or(0) > 0 {}
                    break;
                }
                output.extend_from_slice(&buf[..n]);
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                // No data right now — sleep briefly then retry.
                // WouldBlock is expected on slow commands; do NOT break.
                std::thread::sleep(std::time::Duration::from_millis(50));
                continue;
            }
            Err(e) => {
                return Err(CoreError::Connection(format!(
                    "Failed to read {}: {}",
                    if stderr { "stderr" } else { "stdout" },
                    e
                )));
            }
        }
    }

    let content = String::from_utf8_lossy(&output).into_owned();

    if truncated {
        warn!(
            stream = if stderr { "stderr" } else { "stdout" },
            max_bytes, "Output truncated"
        );
    }

    Ok((content, truncated))
}

// ---------------------------------------------------------------------------
// Helper: parse environment snapshot from captured output
// ---------------------------------------------------------------------------

fn parse_env_snapshot(workspace_id: WorkspaceId, output: &str) -> CoreResult<EnvSnapshot> {
    let mut os = String::new();
    let mut os_version = String::new();
    let mut arch = String::new();
    let mut shell = String::new();
    let mut home_dir = PathBuf::from("/home");
    let mut working_dir = PathBuf::from(".");
    let mut env_vars: HashMap<String, String> = HashMap::new();
    let mut programs: Vec<ProgramInfo> = Vec::new();
    let runtimes: Vec<RuntimeInfo> = Vec::new();

    let mut section = "";

    for line in output.lines() {
        match line.trim() {
            "---OS---" => section = "os",
            "---OS_VERSION---" => section = "os_version",
            "---ARCH---" => section = "arch",
            "---SHELL---" => section = "shell",
            "---HOME---" => section = "home",
            "---PWD---" => section = "pwd",
            "---PATH---" => section = "path",
            "---ENV_VARS---" => section = "env_vars",
            "---PROGRAMS---" => section = "programs",
            "---END---" => section = "",
            line => match section {
                "os" => os = line.to_string(),
                "os_version" => os_version = line.to_string(),
                "arch" => arch = line.to_string(),
                "shell" => {
                    shell = line.to_string();
                }
                "home" => home_dir = PathBuf::from(line),
                "pwd" => working_dir = PathBuf::from(line),
                "path" => {
                    env_vars.insert("PATH".into(), line.to_string());
                }
                "env_vars" => {
                    if let Some((key, value)) = line.split_once('=') {
                        env_vars.insert(key.to_string(), value.to_string());
                    }
                }
                "programs" => {
                    if let Some(rest) = line.strip_prefix("PROG:") {
                        let parts: Vec<&str> = rest.splitn(3, ':').collect();
                        if parts.len() >= 3 {
                            let name = parts[0].to_string();
                            let path = PathBuf::from(parts[1]);
                            let version = if parts[2].is_empty() {
                                None
                            } else {
                                Some(parts[2].to_string())
                            };

                            programs.push(ProgramInfo {
                                name,
                                path,
                                version,
                            });
                        }
                    }
                }
                _ => {}
            },
        }
    }

    Ok(EnvSnapshot {
        workspace_id,
        captured_at: chrono::Utc::now(),
        os: if os.is_empty() { "Linux".into() } else { os },
        os_version,
        arch,
        shell,
        working_dir,
        home_dir,
        env_vars,
        programs,
        runtimes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_workspace(host: &str) -> Workspace {
        Workspace {
            id: Uuid::new_v4(),
            name: "test".into(),
            display_name: "test".into(),
            connection: ConnectionConfig {
                host: host.into(),
                port: 22,
                user: "alice".into(),
                auth: AuthMethod::Agent,
                jump_hosts: vec![],
            },
            remote_work_dir: PathBuf::from("/tmp/project"),

            remote_data_dirs: vec![PathBuf::from("/data/reference")],
            node_execution_mode: NodeExecutionMode::SubmitOnly,
            authorized: true,
            created_at: chrono::Utc::now(),
            last_accessed_at: chrono::Utc::now(),
            file_threshold_bytes: 1_048_576,
            config: HashMap::new(),
        }
    }


    #[cfg(windows)]
    #[test]
    fn waiting_for_a_busy_session_consumes_the_deadline() {
        let mutex=Arc::new(Mutex::new(()));let other=mutex.clone();let (ready,receiver)=mpsc::channel();
        let worker=std::thread::spawn(move||{let _guard=other.lock().unwrap();ready.send(()).unwrap();std::thread::sleep(Duration::from_millis(300));});
        receiver.recv().unwrap();let start=std::time::Instant::now();
        assert!(matches!(lock_before(&mutex,start+Duration::from_millis(50)),Err(CoreError::Timeout(_))));
        assert!(start.elapsed()<Duration::from_millis(200));worker.join().unwrap();
    }

    #[test]
    fn mcp_command_enforces_working_dir_inside_work_root() {
        let workspace = test_workspace("example");
        let outside = RemoteExecutorImpl::build_mcp_command(
            &workspace,
            "echo hi",
            Some(Path::new("/data/reference")),
            &HashMap::new(),
        );
        assert!(matches!(outside, Err(CoreError::PermissionDenied(_))));

        let script = RemoteExecutorImpl::build_mcp_command(
            &workspace,
            "echo hi",
            Some(Path::new("/tmp/project/sub")),
            &HashMap::new(),
        )
        .unwrap();
        assert!(script.contains("cd '/tmp/project/sub'"));
        assert!(script.contains("echo hi"));
        assert!(!script.contains("bwrap"));
    }

    #[test]
    fn persistent_frame_is_delimited_without_remote_temp_files() {
        let frame = windows_remote_frame("printf hello", false, "token", Duration::from_secs(7));
        assert!(frame.contains("LRW_BEGIN:token"));
        assert!(frame.contains("LRW_END:token"));
        assert!(frame.contains("trap lrw_cleanup INT TERM HUP EXIT"));
        assert!(frame.contains("kill -KILL"));
        assert!(frame.contains("timeout -k 2s 6s"));
        assert!(!frame.contains("/tmp"));
        assert_eq!(find_bytes(b"abcXYZdef", b"XYZ"), Some(3));
    }

    #[test]
    fn login_node_manifest_uses_path_globs_and_grep_only() {
        assert!(BIN_PATH_REFRESH_SCRIPT.contains("$HOME/.bin_path_file"));
        assert!(!BIN_PATH_REFRESH_SCRIPT.contains("find "));
        assert!(BIN_PATH_REFRESH_SCRIPT
            .contains("'/^# lrworkspace:bin_path$/ { print; next } !seen[$1]++'"));
        assert!(ENV_CAPTURE_SCRIPT.contains("grep -v '^$' \"$HOME/.bin_path_file\""));
        assert!(!ENV_CAPTURE_SCRIPT.contains("--version"));

        #[cfg(not(windows))]
        {
            let wrapper = remote_lifetime_wrapper("sleep 300", false, Duration::from_secs(9));
            assert!(wrapper.contains("timeout -k 2s 8s"));
            assert!(wrapper.contains("kill -KILL"));
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn remote_watchdog_reaps_the_command_process() {
        let marker = std::env::temp_dir().join(format!("lrw-watchdog-{}.pid", Uuid::new_v4()));
        let command = format!("echo $$ > {}; exec sleep 300", marker.display());
        let wrapper = remote_lifetime_wrapper(&command, false, Duration::from_secs(2));
        let started = std::time::Instant::now();
        let status = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(wrapper)
            .status()
            .unwrap();
        assert!(!status.success());
        assert!(started.elapsed() < Duration::from_secs(5));

        let pid = std::fs::read_to_string(&marker).unwrap();
        let alive = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("kill -0 {} 2>/dev/null", pid.trim()))
            .status()
            .unwrap()
            .success();
        let _ = std::fs::remove_file(marker);
        assert!(!alive, "timed-out remote child was left running");
    }

    #[cfg(not(windows))]
    #[test]
    fn persistent_frame_watchdog_reaps_the_command_process() {
        let marker = std::env::temp_dir().join(format!("lrw-frame-{}.pid", Uuid::new_v4()));
        let command = format!("echo $$ > {}; exec sleep 300", marker.display());
        let frame = windows_remote_frame(&command, false, "watchdog", Duration::from_secs(2));
        let started = std::time::Instant::now();
        let output = std::process::Command::new("/bin/bash")
            .arg("-c")
            .arg(frame)
            .output()
            .unwrap();
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(String::from_utf8_lossy(&output.stdout).contains("LRW_END:watchdog:124"));

        let pid = std::fs::read_to_string(&marker).unwrap();
        let alive = std::process::Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("kill -0 {} 2>/dev/null", pid.trim()))
            .status()
            .unwrap()
            .success();
        let _ = std::fs::remove_file(marker);
        assert!(!alive, "persistent-frame child was left running");
    }
}
