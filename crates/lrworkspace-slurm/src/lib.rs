//! LrWorkspace Slurm — Slurm cluster job management.
//!
//! Implements `SlurmManager` by wrapping Slurm CLI commands
//! (`sbatch`, `squeue`, `scancel`, `sacct`) executed over SSH.
//!
//! ## Job Lifecycle
//!
//! ```text
//! submit ──→ Pending ──→ Running ──→ Completed
//!                │            │
//!                │            ├── Failed
//!                │            └── Cancelled
//!                └── (cancelled via scancel)
//! ```
//!
//! ## Polling
//!
//! Status checks are poll-based (no long-lived SSH channels).
//! The polling interval is configurable via `LrWorkspaceConfig`.

use async_trait::async_trait;
use chrono::Utc;
use std::path::Path;
use std::sync::Arc;
use tracing::{debug, info, instrument};

use lrworkspace_core::error::{CoreError, CoreResult};
use lrworkspace_core::safety::{normalize_remote_absolute, safe_join, shell_quote};
use lrworkspace_core::traits::SlurmManager;
use lrworkspace_core::types::*;

use lrworkspace_remote::RemoteExecutorImpl;

mod pbs;
pub use pbs::PbsManagerImpl;
pub mod batch;

/// Production-grade implementation of `SlurmManager`.
pub struct SlurmManagerImpl {
    executor: Arc<RemoteExecutorImpl>,
    max_output_bytes: usize,
}

impl SlurmManagerImpl {
    /// Create a new Slurm manager.
    pub fn new(executor: Arc<RemoteExecutorImpl>) -> Self {
        Self {
            executor,
            max_output_bytes: 10 * 1024 * 1024, // 10 MB
        }
    }

    /// Parse `squeue` output to extract job state.
    /// Expected format from `squeue -j <job_id> -h -o %T`
    fn parse_job_state(raw: &str) -> SlurmJobState {
        let state = raw.trim().to_uppercase();
        // sacct may append a cancelling user ("CANCELLED by 1234") or a
        // truncation marker. Request a wide field, but tolerate both forms.
        let state = state.split_whitespace().next().unwrap_or("").trim_end_matches('+');
        match state {
            "PENDING" | "PD" | "CONFIGURING" | "CF" | "REQUEUED" | "RQ" => SlurmJobState::Pending,
            "RUNNING" | "R" | "COMPLETING" | "CG" | "SUSPENDED" | "S" => SlurmJobState::Running,
            "COMPLETED" | "CD" => SlurmJobState::Completed,
            "FAILED" | "F" | "OUT_OF_MEMORY" | "OOM" | "NODE_FAIL" | "NF"
            | "BOOT_FAIL" | "BF" | "DEADLINE" | "DL" => SlurmJobState::Failed,
            "CANCELLED" | "CA" => SlurmJobState::Cancelled,
            "TIMEOUT" | "TO" => SlurmJobState::Timeout,
            "" => SlurmJobState::Unknown("no output".into()),
            other => SlurmJobState::Unknown(other.to_string()),
        }
    }

    /// Parse `sacct` exit code field.
    fn parse_exit_code(raw: &str) -> Option<i32> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return None;
        }
        // sacct returns "0:0" format (exit_code:signal)
        if let Some((ec, signal)) = trimmed.split_once(':') {
            let code: i32 = ec.parse().ok()?;
            let signal: i32 = signal.parse().ok()?;
            Some(if code == 0 && signal != 0 { 128 + signal } else { code })
        } else {
            trimmed.parse().ok()
        }
    }

    /// Array jobs remain active while any element is active; only an entirely
    /// successful set of records is reported as completed.
    fn combine_states(states: impl IntoIterator<Item = SlurmJobState>) -> SlurmJobState {
        states.into_iter().max_by_key(|state| match state {
            SlurmJobState::Running => 7,
            SlurmJobState::Pending => 6,
            SlurmJobState::Unknown(_) => 5,
            SlurmJobState::Failed => 4,
            SlurmJobState::Timeout => 3,
            SlurmJobState::Cancelled => 2,
            SlurmJobState::Completed => 1,
        }).unwrap_or_else(|| SlurmJobState::Unknown("no scheduler record".into()))
    }

    fn parse_accounting_state(raw: &str) -> SlurmJobState {
        Self::combine_states(raw.lines().filter(|line| !line.trim().is_empty()).map(|line| {
            let mut fields = line.split('|');
            let state = Self::parse_job_state(fields.next().unwrap_or(""));
            let exit_code = fields.next().and_then(Self::parse_exit_code);
            if state == SlurmJobState::Completed && exit_code.is_some_and(|code| code != 0) {
                SlurmJobState::Failed
            } else {
                state
            }
        }))
    }

    /// scontrol values can contain spaces, so splitting the record into words
    /// truncates paths. Stop only at the next key=value field boundary.
    fn control_field(raw: &str, key: &str) -> Option<String> {
        let marker = format!("{key}=");
        let start = raw.match_indices(&marker).find_map(|(index, _)| {
            (index == 0 || raw[..index].ends_with(char::is_whitespace))
                .then_some(index + marker.len())
        })?;
        let remainder = &raw[start..];
        let end = remainder.char_indices().find_map(|(index, ch)| {
            if !ch.is_whitespace() {
                return None;
            }
            let next = remainder[index..].trim_start();
            let (candidate, _) = next.split_once('=')?;
            (!candidate.is_empty()
                && candidate.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
                .then_some(index)
        }).unwrap_or(remainder.len());
        Self::log_path(&remainder[..end])
    }

    fn log_path(raw: &str) -> Option<String> {
        let value = raw.trim();
        if value.is_empty() || matches!(value, "(null)" | "Unknown" | "N/A" | "None") {
            None
        } else {
            Some(value.to_string())
        }
    }

    fn parse_accounting_paths(raw: &str) -> (Option<String>, Option<String>) {
        let mut fields = raw.lines().find(|line| !line.trim().is_empty()).unwrap_or("").split('|');
        (fields.next().and_then(Self::log_path), fields.next().and_then(Self::log_path))
    }

    fn parse_accounting_job_name(raw: &str) -> Option<String> {
        raw.lines().find(|line| !line.trim().is_empty())
            .and_then(|line| line.split('|').next())
            .and_then(Self::log_path)
    }

    fn parse_submitted_job_id(raw: &str) -> Option<String> {
        raw.lines().find_map(|line| {
            let id = line.trim().strip_prefix("Submitted batch job ")?.trim();
            (!id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())).then(|| id.to_string())
        }).or_else(|| {
            let id = raw.trim();
            (!id.is_empty() && id.bytes().all(|b| b.is_ascii_digit())).then(|| id.to_string())
        })
    }

    fn is_generated_job_name(name: &str) -> bool {
        name.rsplit_once('.').is_some_and(|(base, suffix)| {
            !base.is_empty() && base.chars().all(|c| c.is_alphanumeric() || c == '_')
                && suffix.len() == 8 && suffix.bytes().all(|b| b.is_ascii_hexdigit())
        })
    }

    fn validate_job_id(job_id: &str) -> CoreResult<()> {
        if !job_id.is_empty()
            && job_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.' | '[' | ']'))
        {
            return Ok(());
        }
        Err(CoreError::Validation(format!(
            "Invalid Slurm job id: {}",
            job_id
        )))
    }

    /// Prefer controller paths, which work without Slurm accounting and on
    /// older releases where sacct does not expose StdOut/StdErr.
    async fn resolve_log_paths(
        executor: &RemoteExecutorImpl,
        workspace: &Workspace,
        job_id: &str,
    ) -> CoreResult<(Option<String>, Option<String>)> {
        Self::validate_job_id(job_id)?;
        let control_cmd = format!("scontrol show job {}", shell_quote(job_id));
        let control = executor.execute_scheduler_control(workspace, &control_cmd, None, &Default::default()).await?;
        let mut stdout_path = None;
        let mut stderr_path = None;
        let mut job_name = None;
        if control.exit_code == Some(0) {
            stdout_path = Self::control_field(&control.stdout, "StdOut");
            stderr_path = Self::control_field(&control.stdout, "StdErr");
            job_name = Self::control_field(&control.stdout, "JobName");
        }
        if stdout_path.is_none() || stderr_path.is_none() {
            let cmd = format!(
                "sacct -X -j {} --format=StdOut%4096,StdErr%4096 -n -P",
                shell_quote(job_id)
            );
            let record = executor.execute_scheduler_control(workspace, &cmd, None, &Default::default()).await?;
            if record.exit_code == Some(0) {
                let (out, err) = Self::parse_accounting_paths(&record.stdout);
                stdout_path = stdout_path.or(out);
                stderr_path = stderr_path.or(err);
            }
        }
        // Completed jobs can leave scontrol before their logs are fetched.
        // Older sacct can still retrieve our unique generated job name.
        if stdout_path.is_none() || stderr_path.is_none() {
            if job_name.is_none() {
                let cmd = format!("sacct -X -j {} --format=JobName%128 -n -P", shell_quote(job_id));
                let record = executor.execute_scheduler_control(workspace, &cmd, None, &Default::default()).await?;
                if record.exit_code == Some(0) {
                    job_name = Self::parse_accounting_job_name(&record.stdout);
                }
            }
            if let Some(name) = job_name.filter(|name| Self::is_generated_job_name(name)) {
                let base = normalize_remote_absolute(&workspace.remote_work_dir)?;
                stdout_path = stdout_path.or_else(|| Some(format!("{}/._lrworkspace_jobs/{}.out", base.trim_end_matches('/'), name)));
                stderr_path = stderr_path.or_else(|| Some(format!("{}/._lrworkspace_jobs/{}.err", base.trim_end_matches('/'), name)));
            }
        }

        Ok((stdout_path, stderr_path))
    }
}

#[async_trait]
impl SlurmManager for SlurmManagerImpl {
    #[instrument(skip(self, workspace))]
    async fn submit(
        &self,
        workspace: &Workspace,
        script_path: &Path,
        job_name: &str,
    ) -> CoreResult<SlurmJob> {
        info!(
            "Submitting Slurm job: {} ({})",
            job_name,
            script_path.display()
        );

        let remote_script = safe_join(&workspace.remote_work_dir, script_path)?;
        let remote_work_dir = normalize_remote_absolute(&workspace.remote_work_dir)?;
        let cmd = format!(
            "cd {} && sbatch --job-name={} {} 2>&1",
            shell_quote(&remote_work_dir),
            shell_quote(job_name),
            shell_quote(&remote_script.to_string_lossy())
        );

        let record = self
            .executor
            .execute_scheduler_control(workspace, &cmd, None, &Default::default())
            .await?;

        if record.exit_code != Some(0) {
            return Err(CoreError::Connection(format!(
                "sbatch failed: stdout={} stderr={}",
                record.stdout.trim(),
                record.stderr.trim()
            )));
        }

        // Parse job ID from sbatch output: "Submitted batch job <id>"
        let job_id = Self::parse_submitted_job_id(&record.stdout).ok_or_else(|| {
            CoreError::InvalidState(format!(
                "sbatch succeeded but its job ID could not be parsed. The job may already be submitted; check the scheduler before retrying to avoid duplicate jobs. Output: {}",
                record.stdout.trim()
            ))
        })?;

        let now = Utc::now();

        info!(job_id = %job_id, "Slurm job submitted");

        Ok(SlurmJob {
            workspace_id: workspace.id,
            triggered_by: AgentKind::CliAgent,
            job_id,
            submit_command: cmd,
            job_name: job_name.to_string(),
            submitted_at: now,
            state: SlurmJobState::Pending,
            stdout_log_path: None,
            stderr_log_path: None,
            exit_code: None,
        })
    }

    #[instrument(skip(self, workspace))]
    async fn status(&self, workspace: &Workspace, job_id: &str) -> CoreResult<SlurmJobState> {
        debug!("Checking status for job: {}", job_id);
        Self::validate_job_id(job_id)?;

        // Use squeue first (for active jobs), then sacct (for finished jobs)
        let cmd = format!("squeue -j {} -h -o '%T'", shell_quote(job_id));

        let record = self
            .executor
            .execute_scheduler_control(workspace, &cmd, None, &Default::default())
            .await?;

        let state = Self::combine_states(record.stdout.lines().filter(|line| !line.trim().is_empty()).map(Self::parse_job_state));
        if record.exit_code == Some(0) && !matches!(state, SlurmJobState::Unknown(_)) {
            return Ok(state);
        }
        // Do not pipe to head: that masks sacct failures as successful empty
        // output. -X excludes job steps, whose state is not the allocation's.
        let sacct_cmd = format!(
            "sacct -X -j {} --format=State%64,ExitCode -n -P",
            shell_quote(job_id)
        );
        let sacct_record = self.executor.execute_scheduler_control(workspace, &sacct_cmd, None, &Default::default()).await?;
        if sacct_record.exit_code != Some(0) {
            return Err(CoreError::Connection(format!(
                "Cannot determine Slurm job {} status: squeue={} {}; sacct={} {}",
                job_id, record.stdout.trim(), record.stderr.trim(),
                sacct_record.stdout.trim(), sacct_record.stderr.trim()
            )));
        }
        Ok(Self::parse_accounting_state(&sacct_record.stdout))
    }

    #[instrument(skip(self, workspace))]
    async fn cancel(&self, workspace: &Workspace, job_id: &str) -> CoreResult<()> {
        info!("Cancelling Slurm job: {}", job_id);
        Self::validate_job_id(job_id)?;

        let cmd = format!("scancel {} 2>&1", shell_quote(job_id));
        let record = self
            .executor
            .execute_scheduler_control(workspace, &cmd, None, &Default::default())
            .await?;

        if record.exit_code != Some(0) {
            return Err(CoreError::Connection(format!(
                "Failed to cancel job {}: {}",
                job_id, record.stderr
            )));
        }

        info!("Slurm job {} cancelled", job_id);
        Ok(())
    }

    #[instrument(skip(self, workspace))]
    async fn fetch_logs(
        &self,
        workspace: &Workspace,
        job_id: &str,
    ) -> CoreResult<(String, String)> {
        info!("Fetching logs for job: {}", job_id);
        Self::validate_job_id(job_id)?;

        // Resolve log file paths
        let (stdout_path, stderr_path) =
            Self::resolve_log_paths(&self.executor, workspace, job_id).await?;

        let stdout = if let Some(ref path) = stdout_path {
            let cmd = format!(
                "tail -c {} -- {} 2>/dev/null",
                self.max_output_bytes,
                shell_quote(path)
            );
            let record = self
                .executor
                .execute_scheduler_control(workspace, &cmd, None, &Default::default())
                .await?;
            record.stdout
        } else {
            // Try default slurm-<job_id>.out in submission directory
            let cmd = format!(
                "tail -c {} -- {} 2>/dev/null",
                self.max_output_bytes,
                shell_quote(
                    &safe_join(
                        &workspace.remote_work_dir,
                        Path::new(&format!("slurm-{}.out", job_id))
                    )?
                    .to_string_lossy()
                )
            );
            let record = self
                .executor
                .execute_scheduler_control(workspace, &cmd, None, &Default::default())
                .await?;
            record.stdout
        };

        let stderr = if let Some(ref path) = stderr_path {
            let cmd = format!(
                "tail -c {} -- {} 2>/dev/null",
                self.max_output_bytes,
                shell_quote(path)
            );
            let record = self
                .executor
                .execute_scheduler_control(workspace, &cmd, None, &Default::default())
                .await?;
            record.stdout // tail's stdout is log content; stderr is file errors
        } else {
            String::new()
        };

        Ok((stdout, stderr))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_job_state() {
        assert!(matches!(
            SlurmManagerImpl::parse_job_state("PENDING"),
            SlurmJobState::Pending
        ));
        assert!(matches!(
            SlurmManagerImpl::parse_job_state("RUNNING"),
            SlurmJobState::Running
        ));
        assert!(matches!(
            SlurmManagerImpl::parse_job_state("COMPLETED"),
            SlurmJobState::Completed
        ));
        assert!(matches!(
            SlurmManagerImpl::parse_job_state("FAILED"),
            SlurmJobState::Failed
        ));
        assert!(matches!(
            SlurmManagerImpl::parse_job_state("CANCELLED"),
            SlurmJobState::Cancelled
        ));
    }

    #[test]
    fn test_parse_exit_code() {
        assert_eq!(SlurmManagerImpl::parse_exit_code("0:0"), Some(0));
        assert_eq!(SlurmManagerImpl::parse_exit_code("1:0"), Some(1));
        assert_eq!(SlurmManagerImpl::parse_exit_code(""), None);
        assert_eq!(SlurmManagerImpl::parse_exit_code("137:9"), Some(137));
        assert_eq!(SlurmManagerImpl::parse_exit_code("0:9"), Some(137));
    }

    #[test]
    fn missing_accounting_records_are_not_completed() {
        assert!(matches!(SlurmManagerImpl::parse_accounting_state(" \n"), SlurmJobState::Unknown(_)));
        assert!(matches!(SlurmManagerImpl::parse_accounting_state("UNKNOWN|0:0"), SlurmJobState::Unknown(_)));
    }

    #[test]
    fn accounting_state_checks_failure_and_signal() {
        assert_eq!(SlurmManagerImpl::parse_accounting_state("COMPLETED|0:0\n"), SlurmJobState::Completed);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("COMPLETED|2:0\n"), SlurmJobState::Failed);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("COMPLETED|0:9\n"), SlurmJobState::Failed);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("OUT_OF_MEMORY|0:125\n"), SlurmJobState::Failed);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("CANCELLED by 1001|0:15\n"), SlurmJobState::Cancelled);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("TIMEOUT|0:15\n"), SlurmJobState::Timeout);
    }

    #[test]
    fn array_status_does_not_use_only_the_first_record() {
        assert_eq!(SlurmManagerImpl::parse_accounting_state("COMPLETED|0:0\nRUNNING|0:0\n"), SlurmJobState::Running);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("COMPLETED|0:0\nPENDING|0:0\n"), SlurmJobState::Pending);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("COMPLETED|0:0\nFAILED|1:0\n"), SlurmJobState::Failed);
        assert_eq!(SlurmManagerImpl::parse_accounting_state("COMPLETED|0:0\nCOMPLETED|0:0\n"), SlurmJobState::Completed);
        assert!(matches!(SlurmManagerImpl::parse_accounting_state("COMPLETED|0:0\nUNKNOWN|\n"), SlurmJobState::Unknown(_)));
        assert_eq!(SlurmManagerImpl::parse_job_state("COMPLETING"), SlurmJobState::Running);
    }

    #[test]
    fn control_log_paths_preserve_spaces_in_multiline_and_oneline_output() {
        let multiline = "JobId=123 JobName=cd_project.0123abcd\n   WorkDir=/work/my project\n   StdErr=/work/my project/log file.err\n   StdIn=/dev/null\n   StdOut=/work/my project/log file.out\n   Power=\n";
        assert_eq!(SlurmManagerImpl::control_field(multiline, "StdOut"), Some("/work/my project/log file.out".into()));
        assert_eq!(SlurmManagerImpl::control_field(multiline, "StdErr"), Some("/work/my project/log file.err".into()));
        assert_eq!(SlurmManagerImpl::control_field(multiline, "JobName"), Some("cd_project.0123abcd".into()));
        let oneline = "JobId=123 StdErr=/work/project a/error log StdOut=/work/project a/output log TresPerNode=cpu:1";
        assert_eq!(SlurmManagerImpl::control_field(oneline, "StdOut"), Some("/work/project a/output log".into()));
        assert_eq!(SlurmManagerImpl::control_field(oneline, "StdErr"), Some("/work/project a/error log".into()));
        assert_eq!(SlurmManagerImpl::control_field("StdErr=(null) StdOut=", "StdErr"), None);
        assert_eq!(SlurmManagerImpl::control_field("StdErr=(null) StdOut=", "StdOut"), None);
    }

    #[test]
    fn accounting_paths_and_generated_name_fallback() {
        assert_eq!(SlurmManagerImpl::parse_accounting_paths("/work/project a/out|/work/project a/err\n"), (Some("/work/project a/out".into()), Some("/work/project a/err".into())));
        assert_eq!(SlurmManagerImpl::parse_accounting_paths("(null)|\n"), (None, None));
        assert!(SlurmManagerImpl::is_generated_job_name("cd_project.0123abcd"));
        assert!(!SlurmManagerImpl::is_generated_job_name("manual_job"));
        assert!(!SlurmManagerImpl::is_generated_job_name("../other.0123abcd"));
        assert!(!SlurmManagerImpl::is_generated_job_name("cd_project.0123abc+"));
        assert_eq!(SlurmManagerImpl::parse_accounting_job_name("\ncd_project.0123abcd|\n"), Some("cd_project.0123abcd".into()));
        assert_eq!(SlurmManagerImpl::parse_accounting_job_name("cd_project.0123abcd\n"), Some("cd_project.0123abcd".into()));
        assert_eq!(SlurmManagerImpl::parse_accounting_job_name("|\n"), None);
    }

    #[test]
    fn submission_requires_an_actual_job_id() {
        assert_eq!(SlurmManagerImpl::parse_submitted_job_id("Submitted batch job 12345\n"), Some("12345".into()));
        assert_eq!(SlurmManagerImpl::parse_submitted_job_id("warning: account 42\nSubmitted batch job 12345\n"), Some("12345".into()));
        assert_eq!(SlurmManagerImpl::parse_submitted_job_id("12345\n"), Some("12345".into()));
        assert_eq!(SlurmManagerImpl::parse_submitted_job_id("Submitted batch job unknown\n"), None);
        assert_eq!(SlurmManagerImpl::parse_submitted_job_id(""), None);
        assert_eq!(SlurmManagerImpl::parse_submitted_job_id("warning: account 42"), None);
    }

    #[test]
    fn test_validate_job_id_rejects_shell_syntax() {
        assert!(SlurmManagerImpl::validate_job_id("12345").is_ok());
        assert!(SlurmManagerImpl::validate_job_id("12345_7").is_ok());
        assert!(SlurmManagerImpl::validate_job_id("12345;rm -rf /").is_err());
        assert!(SlurmManagerImpl::validate_job_id("$(id)").is_err());
    }
}
