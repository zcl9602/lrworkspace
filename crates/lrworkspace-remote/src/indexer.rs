//! Remote-only project and data-root indexing.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use regex::{Regex, RegexSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::{debug, info, instrument};

use lrworkspace_core::config::LrWorkspaceConfig;
use lrworkspace_core::error::{CoreError, CoreResult};
use lrworkspace_core::safety::{safe_join, shell_quote};
use lrworkspace_core::traits::ProjectIndexer;
use lrworkspace_core::types::*;

use crate::RemoteExecutorImpl;

pub struct ProjectIndexerImpl {
    executor: Arc<RemoteExecutorImpl>,
    sensitive: RegexSet,
    max_depth: usize,
}

impl ProjectIndexerImpl {
    pub fn new(executor: Arc<RemoteExecutorImpl>, config: LrWorkspaceConfig) -> Self {
        let patterns: Vec<String> = config
            .sensitive_patterns
            .iter()
            .map(|pattern| glob_to_regex(pattern))
            .collect();
        let sensitive = RegexSet::new(patterns).unwrap_or_else(|_| RegexSet::empty());
        Self {
            executor,
            sensitive,
            max_depth: 20,
        }
    }

    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth;
        self
    }

    fn root<'a>(&self, workspace: &'a Workspace, root: RemoteRootKind) -> CoreResult<&'a Path> {
        workspace
            .remote_root_for(root)
            .ok_or_else(|| CoreError::Validation(format!("Unknown remote root: {root:?}")))
    }

    fn classification(&self, path: &str, root: RemoteRootKind) -> FileClassification {
        if self.sensitive.is_match(path) {
            return FileClassification::Sensitive;
        }
        if matches!(root, RemoteRootKind::Data(_)) {
            return FileClassification::Data;
        }
        let lower = path.to_ascii_lowercase();
        if lower.contains("/output")
            || lower.starts_with("output")
            || lower.ends_with(".log")
            || lower.ends_with(".out")
            || lower.ends_with(".err")
        {
            FileClassification::Output
        } else {
            FileClassification::Source
        }
    }

    fn file_list_command(mode: NodeExecutionMode) -> &'static str {
        const GIT_ONLY: &str = "if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then git -c core.quotepath=false ls-files -c -o --exclude-standard -z -- .; else echo 'LRW refuses recursive login-node scans: this root needs a Git index' >&2; exit 64; fi";
        const COMPUTE_NODE: &str = "if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then git -c core.quotepath=false ls-files -c -o --exclude-standard -z -- .; elif command -v rg >/dev/null 2>&1; then rg --files -0 --hidden -g '!.git'; else echo 'File listing requires Git or rg; recursive scanning is disabled' >&2; exit 64; fi";
        match mode {
            NodeExecutionMode::DirectCompute => COMPUTE_NODE,
            NodeExecutionMode::Unconfirmed | NodeExecutionMode::SubmitOnly => GIT_ONLY,
        }
    }

    async fn list_remote_paths(
        &self,
        workspace: &Workspace,
        root: &Path,
    ) -> CoreResult<Vec<PathBuf>> {
        let command = Self::file_list_command(workspace.node_execution_mode);
        let record = self
            .executor
            .execute_fast(workspace, command, Some(root), &Default::default())
            .await?;
        if record.exit_code != Some(0) {
            return Err(CoreError::PermissionDenied(
                record.stderr.trim().to_string(),
            ));
        }
        Ok(record
            .stdout
            .split('\0')
            .filter(|path| !path.is_empty())
            .map(PathBuf::from)
            .filter(|path| path.components().count() <= self.max_depth)
            .collect())
    }
}

#[async_trait]
impl ProjectIndexer for ProjectIndexerImpl {
    #[instrument(skip(self, workspace), fields(ws = %workspace.id))]
    async fn index_root(
        &self,
        workspace: &Workspace,
        root_kind: RemoteRootKind,
    ) -> CoreResult<ProjectIndex> {
        let root = self.root(workspace, root_kind)?.to_path_buf();
        info!(root = %root.display(), "Indexing remote root");
        let mut entries = Vec::new();
        let mut total_files = 0usize;
        let mut excluded_files = Vec::new();
        for relative_path in self.list_remote_paths(workspace, &root).await? {
            let classification = self.classification(&relative_path.to_string_lossy(), root_kind);
            if classification == FileClassification::Sensitive {
                excluded_files.push(relative_path.display().to_string());
                continue;
            }
            total_files += 1;
            entries.push(FileEntry {
                relative_path,
                entry_type: EntryType::File,
                size_bytes: 0,
                mtime: None,
                classification,
            });
        }

        Ok(ProjectIndex {
            workspace_id: workspace.id,
            root,
            read_only: matches!(root_kind, RemoteRootKind::Data(_)),
            indexed_at: Utc::now(),
            root_entries: entries,
            total_files,
            total_dirs: 0,
            excluded_files,
            total_size_bytes: 0,
        })
    }

    async fn search_files(
        &self,
        workspace: &Workspace,
        root_kind: RemoteRootKind,
        pattern: &str,
    ) -> CoreResult<Vec<FileEntry>> {
        let root = self.root(workspace, root_kind)?;
        debug!(root = %root.display(), pattern, "Searching remote files");
        let matcher = Regex::new(&glob_to_regex(pattern))
            .map_err(|error| CoreError::Validation(format!("Invalid search pattern: {error}")))?;
        let mut results = Vec::new();
        for relative_path in self.list_remote_paths(workspace, root).await? {
            if !matcher.is_match(&relative_path.to_string_lossy()) {
                continue;
            }
            let classification = self.classification(&relative_path.to_string_lossy(), root_kind);
            if classification == FileClassification::Sensitive {
                continue;
            }
            results.push(FileEntry {
                relative_path,
                entry_type: EntryType::File,
                size_bytes: 0,
                mtime: None,
                classification,
            });
        }
        Ok(results)
    }

    async fn stat_file(
        &self,
        workspace: &Workspace,
        root_kind: RemoteRootKind,
        relative_path: &Path,
    ) -> CoreResult<FileEntry> {
        let root = self.root(workspace, root_kind)?;
        let target = safe_join(root, relative_path)?;
        let command = format!(
            "stat --format='%F\\t%s\\t%Y' {} 2>/dev/null",
            shell_quote(&target.to_string_lossy())
        );
        let record = self
            .executor
            .execute_fast(workspace, &command, Some(root), &Default::default())
            .await?;
        let parts: Vec<&str> = record.stdout.trim().splitn(3, '\t').collect();
        if parts.len() != 3 {
            return Err(CoreError::NotFound(format!(
                "Remote file not found: {}",
                relative_path.display()
            )));
        }
        let entry_type = match parts[0] {
            "regular file" | "regular empty file" => EntryType::File,
            "directory" => EntryType::Directory,
            "symbolic link" => EntryType::Symlink {
                target: String::new(),
            },
            other => {
                return Err(CoreError::InvalidState(format!(
                    "Unsupported remote file type: {other}"
                )))
            }
        };
        Ok(FileEntry {
            relative_path: relative_path.to_path_buf(),
            entry_type,
            size_bytes: parts[1].parse().unwrap_or(0),
            mtime: parts[2]
                .parse::<i64>()
                .ok()
                .and_then(|seconds| DateTime::from_timestamp(seconds, 0)),
            classification: self.classification(&relative_path.to_string_lossy(), root_kind),
        })
    }
}

fn glob_to_regex(pattern: &str) -> String {
    let mut regex = String::from("(?i)(^|.*/)");
    for character in pattern.chars() {
        match character {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '.' => regex.push_str("\\."),
            other if "()[]{}+^$|\\".contains(other) => {
                regex.push('\\');
                regex.push(other);
            }
            other => regex.push(other),
        }
    }
    regex.push('$');
    regex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_roots_are_classified_as_data() {
        let config = LrWorkspaceConfig::default();
        let indexer = ProjectIndexerImpl::new(
            Arc::new(RemoteExecutorImpl::new(std::time::Duration::from_secs(1))),
            config,
        );
        assert_eq!(
            indexer.classification("reads/sample.fastq", RemoteRootKind::Data(0)),
            FileClassification::Data
        );
    }

    #[test]
    fn login_node_file_listing_never_uses_find_or_rg() {
        let command = ProjectIndexerImpl::file_list_command(NodeExecutionMode::SubmitOnly);
        assert!(command.contains("git") && !command.contains("find "));
        assert!(!command.contains("rg --files"));

        let command = ProjectIndexerImpl::file_list_command(NodeExecutionMode::DirectCompute);
        assert!(command.contains("rg --files") && !command.contains("find "));
    }
}
