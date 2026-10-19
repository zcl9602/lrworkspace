//! LrWorkspace Audit — persistent audit logging.
//!
//! Stores audit records as newline-delimited JSON (JSON Lines) in a
//! rotating log file under the data directory.
//!
//! ## Storage Layout
//!
//! ```text
//! <data_dir>/audit/
//!   └── <workspace_id>/
//!       ├── audit-2025-01.jsonl
//!       ├── audit-2025-02.jsonl
//!       └── ...
//! ```
//!
//! Records are partitioned by workspace and year-month for easy cleanup.

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use tokio::sync::Mutex;
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use lrworkspace_core::config::LrWorkspaceConfig;
use lrworkspace_core::error::CoreResult;
use lrworkspace_core::permission::RiskLevel;
use lrworkspace_core::traits::{AuditLogger, AuditRecord};
use lrworkspace_core::types::*;

// ---------------------------------------------------------------------------
// Audit Logger Implementation
// ---------------------------------------------------------------------------

/// Production-grade implementation of `AuditLogger`.
///
/// Records are written to date-partitioned JSON Lines files.
/// Queries scan the most recent partition(s) in reverse.
pub struct AuditLoggerImpl {
    audit_dir: PathBuf,
    max_bytes: u64,
    /// Per-file write lock to prevent interleaving
    write_lock: Mutex<()>,
}

impl AuditLoggerImpl {
    /// Create a new audit logger.
    pub fn new(config: &LrWorkspaceConfig) -> Self {
        let audit_dir = config.data_dir.join("audit");
        Self {
            audit_dir,
            max_bytes: config.audit_max_bytes,
            write_lock: Mutex::new(()),
        }
    }

    /// Get the partition file path for a workspace in the current month.
    fn partition_path(&self, workspace_id: WorkspaceId) -> PathBuf {
        let now = Utc::now();
        let month_key = now.format("%Y-%m").to_string();
        self.audit_dir
            .join(workspace_id.to_string())
            .join(format!("audit-{}.jsonl", month_key))
    }

    /// Ensure the parent directory exists.
    fn ensure_dir(path: &Path) -> CoreResult<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        Ok(())
    }

    /// Append a single record to a JSONL file.
    fn append_record(path: &Path, record: &SerializableAuditRecord) -> CoreResult<()> {
        Self::ensure_dir(path)?;
        let mut file = OpenOptions::new().create(true).append(true).open(path)?;
        let line = serde_json::to_string(record)?;
        writeln!(file, "{}", line)?;
        Ok(())
    }

    /// Compute total size of all audit files.
    fn compute_storage_size(audit_dir: &Path) -> u64 {
        let mut total = 0u64;
        if let Ok(entries) = fs::read_dir(audit_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    total += Self::compute_storage_size(&path);
                } else if let Ok(meta) = path.metadata() {
                    total += meta.len();
                }
            }
        }
        total
    }

    /// List audit partition files for a workspace, newest first.
    fn list_partitions(&self, workspace_id: WorkspaceId) -> Vec<PathBuf> {
        let dir = self.audit_dir.join(workspace_id.to_string());
        let mut paths: Vec<PathBuf> = Vec::new();

        if let Ok(entries) = fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    paths.push(path);
                }
            }
        }

        // Sort newest first by filename
        paths.sort_by(|a, b| b.file_name().cmp(&a.file_name()));
        paths
    }
}

#[async_trait]
impl AuditLogger for AuditLoggerImpl {
    #[instrument(skip(self))]
    async fn record(
        &self,
        workspace_id: WorkspaceId,
        agent: &AgentKind,
        operation: &str,
        risk_level: RiskLevel,
        details: &str,
        outcome: &str,
    ) -> CoreResult<AuditRecord> {
        let record = AuditRecord {
            id: Uuid::new_v4(),
            workspace_id,
            agent: agent.clone(),
            operation: operation.to_string(),
            risk_level,
            details: details.to_string(),
            outcome: outcome.to_string(),
            timestamp: Utc::now(),
        };

        let path = self.partition_path(workspace_id);
        let serializable = SerializableAuditRecord::from(&record);

        // Lock to prevent interleaving writes from concurrent tasks
        let _guard = self.write_lock.lock().await;
        Self::append_record(&path, &serializable)?;

        debug!(
            op = %operation,
            risk = ?risk_level,
            agent = %agent,
            "Audit record written"
        );

        Ok(record)
    }

    #[instrument(skip(self))]
    async fn query(
        &self,
        workspace_id: WorkspaceId,
        limit: usize,
        offset: usize,
    ) -> CoreResult<Vec<AuditRecord>> {
        if limit==0 {return Ok(vec![]);}
        if limit>1000 {return Err(lrworkspace_core::CoreError::Validation("Audit query limit must be <= 1000".into()));}
        let partitions=self.list_partitions(workspace_id);
        let mut records=Vec::new();
        let mut skipped=0usize;
        for path in &partitions {
            if records.len()>=limit {break;}
            let mut file=File::open(path)?;
            lrworkspace_core::jsonl::visit_reverse_lines(&mut file,|line| {
                if let Ok(record)=serde_json::from_slice::<SerializableAuditRecord>(line) {
                    if skipped<offset {skipped+=1;} else {records.push(record.into());}
                }
                records.len()<limit
            })?;
        }
        Ok(records)
    }

    #[instrument(skip(self))]
    async fn storage_size(&self) -> CoreResult<u64> {
        let size = Self::compute_storage_size(&self.audit_dir);
        if size > self.max_bytes {
            warn!(
                total_bytes = size,
                max_bytes = self.max_bytes,
                "Audit storage exceeds threshold — user should be prompted for cleanup"
            );
        }
        Ok(size)
    }
}

// ---------------------------------------------------------------------------
// Serializable audit record (for JSONL persistence)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableAuditRecord {
    pub id: String,
    pub workspace_id: String,
    pub agent: String,
    pub operation: String,
    pub risk_level: String,
    pub details: String,
    pub outcome: String,
    pub timestamp: String,
}

impl From<&AuditRecord> for SerializableAuditRecord {
    fn from(r: &AuditRecord) -> Self {
        Self {
            id: r.id.to_string(),
            workspace_id: r.workspace_id.to_string(),
            agent: r.agent.to_string(),
            operation: r.operation.clone(),
            risk_level: format!("{:?}", r.risk_level),
            details: r.details.clone(),
            outcome: r.outcome.clone(),
            timestamp: r.timestamp.to_rfc3339(),
        }
    }
}

impl From<SerializableAuditRecord> for AuditRecord {
    fn from(s: SerializableAuditRecord) -> Self {
        Self {
            id: s.id.parse().unwrap_or_else(|_| Uuid::new_v4()),
            workspace_id: s.workspace_id.parse().unwrap_or_else(|_| Uuid::new_v4()),
            agent: match s.agent.as_str() {
                "claude-code" => AgentKind::ClaudeCode,
                "codex" => AgentKind::Codex,
                "cursor" => AgentKind::Cursor,
                "reasonix" => AgentKind::Reasonix,
                "cli-agent" => AgentKind::CliAgent,
                "web-agent" => AgentKind::WebAgent,
                other => AgentKind::Other(other.to_string()),
            },
            operation: s.operation,
            risk_level: match s.risk_level.as_str() {
                "ReadOnly" => RiskLevel::ReadOnly,
                "WorkspaceWrite" | "LocalModify" | "NormalSync" => RiskLevel::WorkspaceWrite,
                "RemoteExecute" => RiskLevel::RemoteExecute,
                "WorkspaceInstall" => RiskLevel::WorkspaceInstall,
                "HighRisk" => RiskLevel::HighRisk,
                "SystemLevel" => RiskLevel::SystemLevel,
                _ => RiskLevel::RemoteExecute,
            },
            details: s.details,
            outcome: s.outcome,
            timestamp: s.timestamp.parse().unwrap_or_else(|_| Utc::now()),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;


    #[tokio::test]
    async fn newest_records_and_offsets_cross_partition_boundaries() {
        let root=std::env::temp_dir().join(format!("lrw-audit-tail-{}",Uuid::new_v4()));
        let config=LrWorkspaceConfig{data_dir:root.clone(),..Default::default()};
        let logger=AuditLoggerImpl::new(&config);let id=Uuid::new_v4();
        for index in 0..1200 {logger.record(id,&AgentKind::CliAgent,"test",RiskLevel::ReadOnly,&index.to_string(),"ok").await.unwrap();}
        let newest=logger.query(id,1,0).await.unwrap();assert_eq!(newest[0].details,"1199");
        let old=logger.audit_dir.join(id.to_string()).join("audit-0000-01.jsonl");
        let mut earlier=SerializableAuditRecord::from(&newest[0]);earlier.details="older".into();
        AuditLoggerImpl::append_record(&old,&earlier).unwrap();
        let page=logger.query(id,2,1199).await.unwrap();assert_eq!(page.len(),2);assert_eq!(page[0].details,"0");assert_eq!(page[1].details,"older");
        assert!(logger.query(id,0,0).await.unwrap().is_empty());assert!(logger.query(id,1001,0).await.is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn test_serde_roundtrip() {
        let record = AuditRecord {
            id: Uuid::new_v4(),
            workspace_id: Uuid::new_v4(),
            agent: AgentKind::ClaudeCode,
            operation: "read_file".into(),
            risk_level: RiskLevel::ReadOnly,
            details: "Read src/main.py".into(),
            outcome: "success".into(),
            timestamp: Utc::now(),
        };

        let ser = SerializableAuditRecord::from(&record);
        let json = serde_json::to_string(&ser).unwrap();
        let deser: SerializableAuditRecord = serde_json::from_str(&json).unwrap();
        let recovered: AuditRecord = deser.into();

        assert_eq!(recovered.operation, "read_file");
        assert_eq!(recovered.agent, AgentKind::ClaudeCode);
        assert_eq!(recovered.risk_level, RiskLevel::ReadOnly);
    }
}
