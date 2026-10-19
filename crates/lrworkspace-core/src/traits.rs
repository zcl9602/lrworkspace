//! Capability contracts for the remote-first LRW architecture.
//!
//! There is deliberately no local cache or synchronization capability. File
//! reads target the configured remote work/data roots; file writes target only
//! the remote work root.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::CoreResult;
use crate::permission::RiskLevel;
use crate::types::*;

#[async_trait]
pub trait ProjectIndexer: Send + Sync {
    async fn index_root(
        &self,
        workspace: &Workspace,
        root: RemoteRootKind,
    ) -> CoreResult<ProjectIndex>;

    async fn search_files(
        &self,
        workspace: &Workspace,
        root: RemoteRootKind,
        pattern: &str,
    ) -> CoreResult<Vec<FileEntry>>;

    async fn stat_file(
        &self,
        workspace: &Workspace,
        root: RemoteRootKind,
        relative_path: &Path,
    ) -> CoreResult<FileEntry>;
}

#[async_trait]
pub trait RemoteFileAccess: Send + Sync {
    /// Read a file from the writable work root or a read-only data root.
    async fn read_remote_file(
        &self,
        workspace: &Workspace,
        root: RemoteRootKind,
        relative_path: &Path,
    ) -> CoreResult<Vec<u8>>;

    /// Atomically write one file under `remote_work_dir` only.
    async fn write_work_file(
        &self,
        workspace: &Workspace,
        relative_path: &Path,
        content: &[u8],
    ) -> CoreResult<()>;
}

#[async_trait]
pub trait RemoteExecutor: Send + Sync {
    async fn execute(
        &self,
        workspace: &Workspace,
        command: &str,
        working_dir: Option<&Path>,
        env: &std::collections::HashMap<String, String>,
    ) -> CoreResult<ExecutionRecord>;

    async fn check_connection(&self, workspace: &Workspace) -> CoreResult<bool>;

    async fn capture_env(&self, workspace: &Workspace) -> CoreResult<EnvSnapshot>;
}

#[async_trait]
pub trait SlurmManager: Send + Sync {
    async fn submit(
        &self,
        workspace: &Workspace,
        script_path: &Path,
        job_name: &str,
    ) -> CoreResult<SlurmJob>;

    async fn status(&self, workspace: &Workspace, job_id: &str) -> CoreResult<SlurmJobState>;

    async fn cancel(&self, workspace: &Workspace, job_id: &str) -> CoreResult<()>;

    async fn fetch_logs(&self, workspace: &Workspace, job_id: &str)
        -> CoreResult<(String, String)>;
}

#[async_trait]
pub trait ClusterDetector: Send + Sync {
    async fn detect_scheduler(&self, workspace: &Workspace) -> CoreResult<ClusterScheduler>;
    async fn collect_nodes(&self, workspace: &Workspace) -> CoreResult<Vec<NodeInfo>>;
    async fn collect_partitions(&self, workspace: &Workspace) -> CoreResult<Vec<PartitionInfo>>;
    async fn discover_cluster(&self, workspace: &Workspace) -> CoreResult<ClusterConfig>;
}

#[async_trait]
pub trait AuditLogger: Send + Sync {
    async fn record(
        &self,
        workspace_id: WorkspaceId,
        agent: &AgentKind,
        operation: &str,
        risk_level: RiskLevel,
        details: &str,
        outcome: &str,
    ) -> CoreResult<AuditRecord>;

    async fn query(
        &self,
        workspace_id: WorkspaceId,
        limit: usize,
        offset: usize,
    ) -> CoreResult<Vec<AuditRecord>>;

    async fn storage_size(&self) -> CoreResult<u64>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub id: AuditId,
    pub workspace_id: WorkspaceId,
    pub agent: AgentKind,
    pub operation: String,
    pub risk_level: RiskLevel,
    pub details: String,
    pub outcome: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}
