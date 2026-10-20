//! Core data types for the LrWorkspace Remote system.
//!
//! These types model workspaces, project indices, file entries,
//! environment snapshots, execution records, and agent identities.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use uuid::Uuid;

/// Unique identifier for a workspace.
pub type WorkspaceId = Uuid;

/// Unique identifier for an audit record.
pub type AuditId = Uuid;

/// Unique identifier for an execution run.
pub type ExecutionId = Uuid;

/// Identifies which AI agent triggered an operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AgentKind {
    #[serde(rename = "claude-code")]
    ClaudeCode,
    #[serde(rename = "codex")]
    Codex,
    #[serde(rename = "cursor")]
    Cursor,
    #[serde(rename = "reasonix")]
    Reasonix,
    #[serde(rename = "cli-agent")]
    CliAgent,
    #[serde(rename = "web-agent")]
    WebAgent,
    #[serde(rename = "other")]
    Other(String),
}

impl std::fmt::Display for AgentKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AgentKind::ClaudeCode => write!(f, "claude-code"),
            AgentKind::Codex => write!(f, "codex"),
            AgentKind::Cursor => write!(f, "cursor"),
            AgentKind::Reasonix => write!(f, "reasonix"),
            AgentKind::CliAgent => write!(f, "cli-agent"),
            AgentKind::WebAgent => write!(f, "web-agent"),
            AgentKind::Other(s) => write!(f, "{}", s),
        }
    }
}

// ---------------------------------------------------------------------------
// Workspace
// ---------------------------------------------------------------------------

/// Whether workload commands may run on the SSH endpoint itself.
///
/// Control-plane operations such as file access, scheduler queries, and job
/// submission still use the SSH endpoint in every mode.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionMode {
    /// No workload may run until the user confirms this node's role.
    #[default]
    Unconfirmed,
    /// The SSH endpoint is a login/submission node. Workloads are submitted to
    /// the scheduler unless this request explicitly names an authorized compute
    /// SSH target. Control-plane queries and file transfers still use this endpoint.
    SubmitOnly,
    /// Standalone-server mode (legacy storage name). The user confirmed computation on
    /// the SSH endpoint.
    #[serde(alias = "single_server")]
    DirectCompute,
}

/// A workspace represents one remote project that an agent can interact with.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Workspace {
    /// Unique workspace ID.
    pub id: WorkspaceId,
    /// Human-readable name.
    pub name: String,
    /// Display alias for shell prompt (defaults to name).
    #[serde(default)]
    pub display_name: String,
    /// Remote connection configuration.
    pub connection: ConnectionConfig,
    /// Writable remote working directory. Serialized with the new explicit
    /// name while accepting legacy `remote_root` workspace files.
    #[serde(rename = "remote_work_dir", alias = "remote_root")]
    pub remote_work_dir: PathBuf,
    /// Remote data roots readable through the MCP file API. MCP file writes
    /// are rejected for these roots and agents are instructed not to modify
    /// them; the SSH user's real permissions are never changed.
    #[serde(default)]
    pub remote_data_dirs: Vec<PathBuf>,
    /// Persistent user-confirmed behavior for workload execution on this SSH
    /// endpoint. Legacy workspaces fail closed as `unconfirmed`.
    #[serde(default)]
    pub node_execution_mode: NodeExecutionMode,
    /// Whether the AI agent is authorized by the user.
    #[serde(default)]
    pub authorized: bool,
    /// Workspace creation time.
    pub created_at: DateTime<Utc>,
    /// Last time the workspace was accessed.
    pub last_accessed_at: DateTime<Utc>,
    /// Maximum file size returned by one MCP read (default 1 MB).
    pub file_threshold_bytes: u64,
    /// Project-level configuration overrides.
    pub config: HashMap<String, String>,
}

/// Which configured remote root a file operation addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteRootKind {
    Work,
    Data(usize),
}

impl Workspace {
    pub fn remote_work_dir(&self) -> &std::path::Path {
        &self.remote_work_dir
    }

    pub fn remote_root_for(&self, root: RemoteRootKind) -> Option<&std::path::Path> {
        match root {
            RemoteRootKind::Work => Some(&self.remote_work_dir),
            RemoteRootKind::Data(index) => self.remote_data_dirs.get(index).map(PathBuf::as_path),
        }
    }
}

/// Result of validating the remote work/data layout.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceLayoutReport {
    pub remote_work_dir: PathBuf,
    pub remote_data_dirs: Vec<PathBuf>,
    pub work_exists: bool,
    pub work_readable: bool,
    pub work_writable: bool,
    pub data_dirs_readable: Vec<bool>,
    pub valid: bool,
    pub errors: Vec<String>,
}

/// How to connect to the remote host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionConfig {
    /// SSH hostname or IP.
    pub host: String,
    /// SSH port (default 22).
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    /// SSH username.
    pub user: String,
    /// Authentication method.
    pub auth: AuthMethod,
    /// Optional jump host(s) for multi-hop connections.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub jump_hosts: Vec<JumpHost>,
}

fn default_ssh_port() -> u16 {
    22
}

#[derive(Clone, Serialize, Deserialize)]
pub enum AuthMethod {
    /// SSH key file path.
    #[serde(rename = "key")]
    Key {
        path: PathBuf,
        passphrase: Option<String>,
    },
    /// SSH agent forwarding.
    #[serde(rename = "agent")]
    Agent,
    /// Legacy in-process library authentication. Never serialized or accepted
    /// through MCP arguments; use LocalPassword for the native private flow.
    #[serde(rename = "password", skip_serializing)]
    Password { password: String },
    /// Opaque reference to process-local credentials; never contains a password.
    #[serde(rename="local_password")]
    LocalPassword { session_id: Uuid, #[serde(default,skip_serializing_if="Option::is_none")] socks_proxy: Option<String> },
}

impl std::fmt::Debug for AuthMethod {
    fn fmt(&self,f:&mut std::fmt::Formatter<'_>)->std::fmt::Result {
        match self {
            Self::Agent=>f.write_str("Agent"),
            Self::Key{path,..}=>f.debug_struct("Key").field("path",path).field("passphrase",&crate::privacy::REDACTED).finish(),
            Self::Password{..}=>f.write_str("Password([REDACTED])"),
            Self::LocalPassword{session_id,..}=>f.debug_struct("LocalPassword").field("session_id",session_id).finish(),
        }
    }
}

/// An intermediate jump host.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JumpHost {
    pub host: String,
    #[serde(default = "default_ssh_port")]
    pub port: u16,
    pub user: String,
    pub auth: AuthMethod,
}

// ---------------------------------------------------------------------------
// Project Index
// ---------------------------------------------------------------------------

/// A snapshot of the remote project's file structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectIndex {
    pub workspace_id: WorkspaceId,
    /// The indexed remote root.
    pub root: PathBuf,
    /// Whether the indexed root is a configured read-only data directory.
    pub read_only: bool,
    /// When the index was created.
    pub indexed_at: DateTime<Utc>,
    /// Root entries (top-level directory contents).
    pub root_entries: Vec<FileEntry>,
    /// Total file count (recursive).
    pub total_files: usize,
    /// Total directory count.
    pub total_dirs: usize,
    /// Sensitive files excluded from the listing.
    pub excluded_files: Vec<String>,
    /// Total size of all files (bytes).
    pub total_size_bytes: u64,
}

/// A single file or directory entry in the project index.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileEntry {
    /// Relative path from the project root.
    pub relative_path: PathBuf,
    /// File type.
    pub entry_type: EntryType,
    /// File size in bytes (0 for directories).
    pub size_bytes: u64,
    /// Modification time on the remote.
    pub mtime: Option<DateTime<Utc>>,
    /// File classification.
    pub classification: FileClassification,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum EntryType {
    File,
    Directory,
    Symlink { target: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FileClassification {
    /// Source code, scripts, and configs in the remote work tree.
    Source,
    /// Data files, models, and large archives in a remote data tree.
    Data,
    /// Log files, output files.
    Output,
    /// Sensitive files excluded from indexes and searches (.env, keys, etc.).
    Sensitive,
    /// Unknown / other.
    Other,
}

// ---------------------------------------------------------------------------
// Environment Snapshot
// ---------------------------------------------------------------------------

/// A point-in-time snapshot of the remote environment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvSnapshot {
    pub workspace_id: WorkspaceId,
    pub captured_at: DateTime<Utc>,
    /// OS name (e.g. "Linux", "Darwin").
    pub os: String,
    /// OS version / release.
    pub os_version: String,
    /// Architecture (e.g. "x86_64", "aarch64").
    pub arch: String,
    /// Default shell on remote.
    pub shell: String,
    /// Working directory at capture time.
    pub working_dir: PathBuf,
    /// Home directory.
    pub home_dir: PathBuf,
    /// Key environment variables (PATH, CONDA_PREFIX, etc.).
    pub env_vars: HashMap<String, String>,
    /// Programs found in PATH with versions.
    pub programs: Vec<ProgramInfo>,
    /// Detected language runtimes / package managers.
    pub runtimes: Vec<RuntimeInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProgramInfo {
    pub name: String,
    pub path: PathBuf,
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeInfo {
    pub kind: RuntimeKind,
    pub version: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RuntimeKind {
    Python,
    Node,
    Rust,
    Go,
    Java,
    Cpp,
    Conda,
    Other(String),
}

// ---------------------------------------------------------------------------
// Execution Record
// ---------------------------------------------------------------------------

/// Record of a single remote command execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub id: ExecutionId,
    pub workspace_id: WorkspaceId,
    /// The agent that triggered this execution.
    pub triggered_by: AgentKind,
    /// The command that was run.
    pub command: String,
    /// Working directory on remote.
    pub working_dir: PathBuf,
    /// Environment variables set for this run.
    pub env: HashMap<String, String>,
    /// When execution started.
    pub started_at: DateTime<Utc>,
    /// When execution finished.
    pub finished_at: Option<DateTime<Utc>>,
    /// Exit code (None if still running or killed).
    pub exit_code: Option<i32>,
    /// Standard output (truncated if too large).
    pub stdout: String,
    /// Standard error (truncated if too large).
    pub stderr: String,
    /// Whether output was truncated.
    pub output_truncated: bool,
    /// Full remote paths to output files for large outputs.
    pub remote_stdout_path: Option<PathBuf>,
    pub remote_stderr_path: Option<PathBuf>,
}

// ---------------------------------------------------------------------------
// Slurm Job
// ---------------------------------------------------------------------------

/// A Slurm job record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlurmJob {
    pub workspace_id: WorkspaceId,
    pub triggered_by: AgentKind,
    pub job_id: String,
    /// The sbatch command used.
    pub submit_command: String,
    /// Job name.
    pub job_name: String,
    /// When the job was submitted.
    pub submitted_at: DateTime<Utc>,
    /// Current Slurm state (PENDING, RUNNING, COMPLETED, FAILED, etc.).
    pub state: SlurmJobState,
    /// Remote path to stdout log.
    pub stdout_log_path: Option<PathBuf>,
    /// Remote path to stderr log.
    pub stderr_log_path: Option<PathBuf>,
    /// Exit code when finished.
    pub exit_code: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SlurmJobState {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
    Timeout,
    Unknown(String),
}

// ---------------------------------------------------------------------------
// Connection Status
// ---------------------------------------------------------------------------

/// Snapshot of the current connection health to a remote workspace.
/// Written to a status file for shell prompt integration, and exposed
/// as an MCP tool for AI agents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionStatus {
    /// Whether the SSH connection is currently healthy.
    pub connected: bool,
    /// Friendly display name (e.g. "my-cluster" or workspace name).
    pub display_name: String,
    /// Remote hostname or IP (for diagnostics).
    pub remote_host: String,
    /// Round-trip latency in milliseconds (None if not measured).
    pub latency_ms: Option<f64>,
    /// When the status was last checked.
    pub checked_at: chrono::DateTime<chrono::Utc>,
    /// Human-readable error if disconnected.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Writable remote working directory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote_work_dir: Option<String>,
    /// Configured read-only remote data directories.
    #[serde(default)]
    pub remote_data_dirs: Vec<String>,
    /// Whether all configured path permission checks passed.
    #[serde(default)]
    pub layout_valid: bool,
    /// Permission or path-layout validation error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub layout_error: Option<String>,
}

impl ConnectionStatus {
    /// Build a status snapshot for shell prompt integration.
    pub fn from_workspace_health(
        workspace: &Workspace,
        connected: bool,
        latency_ms: Option<f64>,
        error: Option<String>,
        connection_label: String,
    ) -> Self {
        Self {
            connected,
            display_name: if !workspace.display_name.is_empty() {
                workspace.display_name.clone()
            } else {
                workspace.name.clone()
            },
            remote_host: connection_label,
            latency_ms,
            checked_at: chrono::Utc::now(),
            error,
            remote_work_dir: Some(workspace.remote_work_dir.display().to_string()),
            remote_data_dirs: workspace
                .remote_data_dirs
                .iter()
                .map(|path| path.display().to_string())
                .collect(),
            layout_valid: false,
            layout_error: None,
        }
    }

    /// Compact one-line summary for shell prompts.
    pub fn prompt_label(&self) -> String {
        if self.connected {
            let latency = self
                .latency_ms
                .map(|ms| format!(" {:.0}ms", ms))
                .unwrap_or_default();
            let guard = if self.layout_valid {
                " ro✓"
            } else {
                " guard!"
            };
            format!("● {}{}{}", self.display_name, latency, guard)
        } else {
            format!("○ {} offline", self.display_name)
        }
    }
}

// ---------------------------------------------------------------------------
// Cluster Configuration
// ---------------------------------------------------------------------------

/// Supported cluster schedulers / workload managers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ClusterScheduler {
    /// Slurm (`sbatch`, `squeue`, `scancel`, `sacct`).
    #[serde(rename = "slurm")]
    Slurm,
    /// PBS / Torque (`qsub`, `qstat`, `qdel`, `pbsnodes`).
    #[serde(rename = "pbs")]
    Pbs,
    /// Grid Engine (`qsub` variant, `qstat`, `qdel`).
    #[serde(rename = "sge")]
    Sge,
    /// LSF (`bsub`, `bjobs`, `bkill`).
    #[serde(rename = "lsf")]
    Lsf,
    /// No scheduler — direct execution only.
    #[serde(rename = "none")]
    None,
    /// Unrecognized scheduler.
    #[serde(rename = "other")]
    Other(String),
}

impl ClusterScheduler {
    pub fn detect_from_command(cmd: &str) -> Self {
        match cmd {
            "sbatch" => Self::Slurm,
            "qsub" => Self::Pbs,
            "qsub-sge" => Self::Sge,
            "bsub" => Self::Lsf,
            _ => Self::None,
        }
    }
}

/// A single compute node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node hostname.
    pub hostname: String,
    /// CPU cores available.
    pub cpu_cores: u32,
    /// Total memory in MB.
    pub memory_mb: u64,
    /// GPU count (0 if none).
    pub gpu_count: u32,
    /// GPU model name if available.
    pub gpu_model: Option<String>,
    /// Node state (idle, mix, alloc, down, offline).
    pub state: String,
    /// Partitions / queues this node belongs to.
    pub partitions: Vec<String>,
    /// Architecture (e.g. "x86_64", "aarch64").
    pub arch: Option<String>,
}

/// A Slurm partition or PBS queue.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    /// Partition / queue name.
    pub name: String,
    /// Total nodes in this partition.
    pub total_nodes: u32,
    /// Max walltime per job (e.g. "7-00:00:00").
    pub max_walltime: Option<String>,
    /// Default walltime.
    pub default_walltime: Option<String>,
    /// Max jobs per user.
    pub max_jobs_per_user: Option<u32>,
    /// Node names in this partition.
    pub nodes: Vec<String>,
    /// Whether this is the default partition.
    pub is_default: bool,
}

/// User-imposed constraints — AI agents read these to plan submissions.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ClusterConstraints {
    /// Max concurrent jobs across all partitions.
    pub max_total_jobs: Option<u32>,
    /// Max CPU cores across all running jobs.
    pub max_total_cores: Option<u32>,
    /// Max walltime per job in minutes (None = no limit).
    pub max_walltime_minutes: Option<u64>,
    /// Max memory per job in MB.
    pub max_memory_mb_per_job: Option<u64>,
    /// Only submit to these partitions (empty = all allowed).
    pub allowed_partitions: Vec<String>,
    /// Additional notes from the user.
    pub user_notes: Option<String>,
}

/// Complete snapshot of the remote cluster topology + user constraints.
/// Stored in `<data_dir>/workspaces/<id>.cluster.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// Which scheduler is detected.
    pub scheduler: ClusterScheduler,
    /// All discovered nodes.
    pub nodes: Vec<NodeInfo>,
    /// All discovered partitions / queues.
    pub partitions: Vec<PartitionInfo>,
    /// User-imposed constraints for safe AI operation.
    pub constraints: ClusterConstraints,
    /// When this info was last refreshed.
    pub refreshed_at: chrono::DateTime<chrono::Utc>,
}

impl ClusterConfig {
    /// Build an sbatch-style resource header from constraints (for AI agents).
    pub fn resource_flags(&self) -> String {
        let mut flags = Vec::new();
        if let Some(cores) = self.constraints.max_total_cores {
            flags.push(format!("--cpus-per-task={}", cores.min(64)));
        }
        if let Some(mem) = self.constraints.max_memory_mb_per_job {
            flags.push(format!("--mem={}M", mem));
        }
        if let Some(wt) = self.constraints.max_walltime_minutes {
            let h = wt / 60;
            let m = wt % 60;
            flags.push(format!("--time={:02}:{:02}:00", h, m));
        }
        if !self.constraints.allowed_partitions.is_empty() {
            flags.push(format!(
                "--partition={}",
                self.constraints.allowed_partitions.join(",")
            ));
        }
        flags.join(" ")
    }

    /// Human-readable summary for init output and MCP response.
    pub fn summary(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!("Scheduler: {:?}", self.scheduler));
        lines.push(format!("Nodes: {}", self.nodes.len()));
        let idle: Vec<_> = self.nodes.iter().filter(|n| n.state == "idle").collect();
        let busy: Vec<_> = self
            .nodes
            .iter()
            .filter(|n| n.state != "idle" && n.state != "down" && n.state != "offline")
            .collect();
        let down: Vec<_> = self
            .nodes
            .iter()
            .filter(|n| n.state == "down" || n.state == "offline")
            .collect();
        lines.push(format!(
            "  idle: {}, busy: {}, down/offline: {}",
            idle.len(),
            busy.len(),
            down.len()
        ));

        // Node detail (first 3 as sample)
        for n in self.nodes.iter().take(3) {
            let gpu = if n.gpu_count > 0 {
                format!(
                    ", GPU: {}×{}",
                    n.gpu_count,
                    n.gpu_model.as_deref().unwrap_or("?")
                )
            } else {
                String::new()
            };
            lines.push(format!(
                "  {} — {} cores, {} MB{} ({})",
                n.hostname, n.cpu_cores, n.memory_mb, gpu, n.state
            ));
        }
        if self.nodes.len() > 3 {
            lines.push(format!("  ... and {} more", self.nodes.len() - 3));
        }

        lines.push(format!("Partitions: {}", self.partitions.len()));
        for p in &self.partitions {
            let def = if p.is_default { " (default)" } else { "" };
            lines.push(format!(
                "  {} — {} nodes, max walltime: {}{}{}",
                p.name,
                p.total_nodes,
                p.max_walltime.as_deref().unwrap_or("unlimited"),
                def,
                p.max_jobs_per_user
                    .map(|n| format!(", max {n} jobs/user"))
                    .unwrap_or_default(),
            ));
        }

        if self.constraints.max_total_jobs.is_some()
            || self.constraints.max_total_cores.is_some()
            || self.constraints.max_walltime_minutes.is_some()
        {
            lines.push("Constraints:".to_string());
            if let Some(j) = self.constraints.max_total_jobs {
                lines.push(format!("  max total jobs: {}", j));
            }
            if let Some(c) = self.constraints.max_total_cores {
                lines.push(format!("  max total cores: {}", c));
            }
            if let Some(w) = self.constraints.max_walltime_minutes {
                lines.push(format!("  max walltime: {} min", w));
            }
            if let Some(m) = self.constraints.max_memory_mb_per_job {
                lines.push(format!("  max memory/job: {} MB", m));
            }
            if !self.constraints.allowed_partitions.is_empty() {
                lines.push(format!(
                    "  allowed partitions: {}",
                    self.constraints.allowed_partitions.join(", ")
                ));
            }
        }

        if let Some(ref notes) = self.constraints.user_notes {
            lines.push(format!("Notes: {}", notes));
        }

        lines.join("\n")
    }
}
