//! Shared CLI/MCP array deployment and submission. Existing runs are retained.
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use lrworkspace_core::{CoreError,CoreResult,LrWorkspaceConfig};
use lrworkspace_core::types::*;
use lrworkspace_core::safety::{normalize_remote_absolute,shell_quote};
use lrworkspace_core::traits::SlurmManager;
use std::io::Write;
use lrworkspace_remote::TransferPreference;
use lrworkspace_core::workspace_store;
use lrworkspace_remote::RemoteExecutorImpl;
use lrworkspace_remote::batch::{BatchOptions,BatchCommand,BatchPlan,BatchSummary,PbsFlavor,new_run_relative,pbs_log_paths};
use serde::{Deserialize,Serialize};
use crate::{SlurmManagerImpl,PbsManagerImpl};

#[derive(Debug,Clone,Serialize,Deserialize)]
pub struct BatchSubmission {
    pub status:String,
    pub batch:BatchSummary,
    pub job:Option<SlurmJob>,
    pub job_id:Option<String>,
    pub warning:Option<String>,
}

pub async fn submit_batch(executor:Arc<RemoteExecutorImpl>, config:&LrWorkspaceConfig,
    workspace:&Workspace, cluster:&ClusterConfig, commands:&[BatchCommand], options:&BatchOptions,
    working_dir:Option<&Path>, submit:bool, agent:&AgentKind) -> CoreResult<BatchSubmission> {
    if !matches!(cluster.scheduler,ClusterScheduler::Slurm|ClusterScheduler::Pbs) {
        return Err(CoreError::InvalidState("Batch submission requires Slurm or PBS; login-node execution is forbidden".into()));
    }
    if submit && workspace.node_execution_mode==NodeExecutionMode::Unconfirmed {
        return Err(CoreError::PermissionDenied("Confirm SSH node mode before submitting workloads".into()));
    }
    let started=Instant::now();
    let remaining=||executor.timeout().checked_sub(started.elapsed()).filter(|budget|!budget.is_zero())
        .ok_or_else(||CoreError::Timeout("Batch preparation/submission exceeded its operation budget; preserved run files must be inspected before retrying".into()));
    let mut options=options.clone();
    if cluster.scheduler==ClusterScheduler::Pbs && options.pbs_flavor.is_none() {
        let version=executor.with_timeout(remaining()?).execute_scheduler_control(workspace,"qsub --version 2>&1",None,&Default::default()).await?;
        options.pbs_flavor=PbsFlavor::detect(&format!("{}\n{}",version.stdout,version.stderr));
        if options.pbs_flavor.is_none() {
            return Err(CoreError::Validation("Cannot distinguish PBS Pro/OpenPBS from Torque; specify pbs_flavor=pbs-pro or torque".into()));
        }
    }
    // Validate user input before any permanent run directory is created.
    let run_relative=new_run_relative(&options)?;
    let provisional=lrworkspace_core::safety::safe_join(&workspace.remote_work_dir,&run_relative)?;
    let _=BatchPlan::prepare(commands,&options,cluster.scheduler.clone(),run_relative.clone(),&provisional)?;
    let prefix=RemoteExecutorImpl::build_mcp_command(workspace,"",working_dir,&Default::default())?;
    let target=executor.with_timeout(remaining()?).validate_mcp_write_target(workspace,&run_relative.join("bundle.tar")).await?;
    let target=normalize_remote_absolute(&target)?;
    let run_directory=target.rsplit_once('/').map(|(parent,_)|parent).ok_or_else(||CoreError::InvalidState("Invalid run root".into()))?;
    let parent=run_directory.rsplit_once('/').map(|(parent,_)|if parent.is_empty(){"/"}else{parent}).unwrap_or("/");
    let summary=BatchPlan::prepare(commands,&options,cluster.scheduler.clone(),run_relative,Path::new(run_directory))?;
    let archive_path=std::env::temp_dir().join(format!("lrw-batch-{}.tar",uuid::Uuid::new_v4().simple()));
    let file=std::fs::OpenOptions::new().write(true).create_new(true).open(&archive_path)?;
    let archive=TemporaryArchive(archive_path);
    {
        let mut writer=std::io::BufWriter::new(file);
        BatchPlan::write_archive(commands,&summary,&prefix,&mut writer)?;
        writer.flush()?;
    }
    // mkdir without -p reserves the unique root. Never clear, reuse, or replace a run.
    let prepare=format!("umask 077\nmkdir -p -- {} && mkdir -- {} && mkdir -p -- {} {}",
        shell_quote(parent),shell_quote(run_directory),shell_quote(&summary.stdout_directory),shell_quote(&summary.stderr_directory));
    let record=executor.with_timeout(remaining()?).execute_control(workspace,&prepare,None,&Default::default()).await?;
    if record.exit_code!=Some(0) {return Err(CoreError::Connection(format!("Cannot reserve fresh batch run {}: {}",run_directory,record.stderr)));}
    executor.with_timeout(remaining()?).upload_local_file(workspace,&summary.run_relative.join("bundle.tar"),&archive.0,TransferPreference::Auto).await?;
    let extract=format!("tar --keep-old-files --no-same-owner --no-same-permissions -xf {} -C {}",
        shell_quote(&target),shell_quote(run_directory));
    let record=executor.with_timeout(remaining()?).execute_control(workspace,&extract,None,&Default::default()).await?;
    if record.exit_code!=Some(0) {return Err(CoreError::Connection(format!("Batch extraction failed; files retained at {}: {}",run_directory,record.stderr)));}
    let mut result=BatchSubmission {status:"generated".into(),batch:summary,job:None,job_id:None,warning:None};
    if !submit {return Ok(result);}
    let executor=Arc::new(executor.with_timeout(remaining()?));
    let mut job=match cluster.scheduler {
        ClusterScheduler::Slurm=>SlurmManagerImpl::new(executor).submit(workspace,&result.batch.script_relative,&options.jobname).await,
        ClusterScheduler::Pbs=>{
            let (stdout,stderr)=pbs_log_paths(&result.batch);
            PbsManagerImpl::new(executor).submit_with_logs(workspace,&result.batch.script_relative,&options.jobname,Path::new(&stdout),Path::new(&stderr)).await
        },
        _=>unreachable!(),
    }.map_err(|error|CoreError::Connection(format!("Submission outcome must be checked before retrying; batch files retained at {}: {error}",result.batch.run_directory)))?;
    job.triggered_by=agent.clone();
    result.status="submitted".into();result.job_id=Some(job.job_id.clone());result.job=Some(job);
    // Once a job ID is known, a local bookkeeping failure must not hide it.
    if let Err(error)=workspace_store::save_job_record(config,workspace.id,result.job_id.as_deref().expect("job ID"),&serde_json::to_value(&result)?) {
        result.warning=Some(format!("Job submitted, but local submission record failed: {error}; do not resubmit"));
    }
    Ok(result)
}

struct TemporaryArchive(std::path::PathBuf);
impl Drop for TemporaryArchive {fn drop(&mut self){let _=std::fs::remove_file(&self.0);}}

fn parent_job_id(job_id:&str)->(String,Option<u32>) {
    if let Some((base,rest))=job_id.split_once('[') {
        if let Some((index,suffix))=rest.split_once(']') {
            return (format!("{base}[]{suffix}"),index.parse().ok());
        }
    }
    if let Some((base,index))=job_id.split_once('_') {
        if base.bytes().all(|byte|byte.is_ascii_digit()) {
            return (base.into(),index.split('.').next().and_then(|index|index.parse().ok()));
        }
    }
    (job_id.into(),None)
}

/// Return None for older/untracked jobs so callers can use legacy log discovery.
pub async fn tracked_batch_logs(executor:&RemoteExecutorImpl, config:&LrWorkspaceConfig,
    workspace:&Workspace, job_id:&str)->CoreResult<Option<(String,String)>> {
    let (parent,task)=parent_job_id(job_id);
    let record=workspace_store::load_job_record(config,workspace.id,job_id)?
        .or(workspace_store::load_job_record(config,workspace.id,&parent)?);
    let Some(record)=record else{return Ok(None)};
    let record:BatchSubmission=serde_json::from_value(record)?;
    if task.is_some_and(|task|task==0||task as usize>record.batch.array_tasks) {return Err(CoreError::Validation("Array task ID is outside this submission".into()));}
    let mut streams=Vec::new();
    for directory in ["out","err"] {
        let relative=record.batch.run_relative.join("array.0001").join(directory);
        let path=executor.validate_mcp_read_target(workspace,RemoteRootKind::Work,&relative).await?;
        // Include scheduler startup/spool files as well as per-attempt logs.
        // PBS spool filenames differ by implementation; suffix-only matching
        // can hide a failure that occurred before the dispatcher started.
        let pattern="*";
        let command=format!("count=0\nfor file in {}/{pattern}; do\n[ -L \"$file\" ] && continue\n[ -f \"$file\" ] || continue\ncount=$((count + 1))\n[ \"$count\" -le 2048 ] || {{ printf '\\n[LRW: more than 2048 log files; inspect the run directory]\\n'; break; }}\nprintf '\\n--- %s ---\\n' \"$file\"\ntail -c 2048 -- \"$file\"\ndone",shell_quote(&normalize_remote_absolute(&path)?));
        let output=executor.execute_control(workspace,&command,None,&Default::default()).await?;
        if output.exit_code!=Some(0) {return Err(CoreError::Connection(format!("Cannot read batch log directory: {}",output.stderr)));}
        let mut content=output.stdout;
        if content.is_empty(){content="[LRW: no log files yet; this is not completion evidence]\n".into();}
        if let Some(task)=task {content=format!("[LRW: array element {task} requested; showing the parent run's log directory]\n{content}");}
        if output.output_truncated {content.push_str("\n[LRW: output truncated; inspect individual log files]\n");}
        streams.push(content);
    }
    Ok(Some((streams.remove(0),streams.remove(0))))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn array_element_ids_resolve_to_the_parent_record() {
        assert_eq!(parent_job_id("123_7"),("123".into(),Some(7)));
        assert_eq!(parent_job_id("123[7].server"),("123[].server".into(),Some(7)));
        assert_eq!(parent_job_id("123[].server"),("123[].server".into(),None));
        assert_eq!(parent_job_id("123"),("123".into(),None));
    }
}
