//! Detached SSH workloads with durable, workspace-scoped handles and bounded log reads.
use crate::RemoteExecutorImpl;
use lrworkspace_core::{
    config::LrWorkspaceConfig,
    safety::{normalize_remote_absolute, shell_quote},
    types::*,
    workspace_store, CoreError, CoreResult,
};
use serde_json::{json, Value};
use std::{collections::BTreeSet, path::Path};
use uuid::Uuid;

fn key(id: Uuid) -> String {
    format!("exec-{id}")
}

fn terminal(task:&Value)->bool {
    match (task["status"].as_str(),task["exit_code"].as_i64()) {
        (Some("succeeded"),Some(0))=>true,
        (Some("failed"),Some(1..=255))=>true,
        _=>false,
    }
}

fn observation_time(task:&Value)->Option<chrono::DateTime<chrono::FixedOffset>> {
    ["observation_started_at","last_checked_at","created_at"].iter().find_map(|field|
        task[*field].as_str().and_then(|value|chrono::DateTime::parse_from_rfc3339(value).ok()))
}

fn merge_observation(previous:Option<Value>,next:Value)->CoreResult<Value> {
    let Some(previous)=previous else{return Ok(next)};
    for field in ["kind","task_id","workspace_id","remote_host","remote_user","remote_port","work_root","compute_node"] {
        if previous[field]!=next[field] {
            return Err(CoreError::InvalidState("Task identity changed while its state was being updated".into()));
        }
    }
    if terminal(&previous) {
        // Refresh metadata for the same terminal evidence, but never regress it
        // or let an older reply replace a newer observation.
        if terminal(&next) && previous["status"]==next["status"] && previous["exit_code"]==next["exit_code"]
            && observation_time(&next)>=observation_time(&previous) { return Ok(next); }
        return Ok(previous);
    }
    if terminal(&next) { return Ok(next); }
    if matches!((observation_time(&previous),observation_time(&next)),(Some(old),Some(new)) if old>new) {
        return Ok(previous);
    }
    Ok(next)
}

async fn commit_observation(config:&LrWorkspaceConfig,workspace:&Workspace,id:Uuid,next:Value,
    timeout:std::time::Duration)->CoreResult<Value> {
    let config=config.clone();let workspace_id=workspace.id;
    tokio::task::spawn_blocking(move||workspace_store::update_job_record(&config,workspace_id,&key(id),
        timeout.min(std::time::Duration::from_secs(5)),|previous|merge_observation(previous,next)))
        .await.map_err(|error|CoreError::InvalidState(format!("Task state worker failed: {error}")))?
}

fn load(config: &LrWorkspaceConfig, workspace: &Workspace, id: Uuid) -> CoreResult<Value> {
    let task = workspace_store::load_job_record(config, workspace.id, &key(id))?
        .ok_or_else(|| CoreError::NotFound("Unknown task in this workspace".into()))?;
    if task["kind"] != "ssh-task"
        || task["workspace_id"] != workspace.id.to_string()
        || task["remote_host"] != workspace.connection.host
        || task["remote_user"] != workspace.connection.user
        || task["remote_port"] != workspace.connection.port
        || task["work_root"] != workspace.remote_work_dir.to_string_lossy().as_ref()
    {
        return Err(CoreError::InvalidState(
            "Task endpoint or workspace root changed; refusing to query a different location"
                .into(),
        ));
    }
    Ok(task)
}


pub fn list(config: &LrWorkspaceConfig, workspace: &Workspace, limit: usize) -> CoreResult<Value> {
    if !(1..=100).contains(&limit) {
        return Err(CoreError::Validation("limit must be 1-100".into()));
    }
    let directory = config.data_dir.join("jobs").join(workspace.id.to_string());
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(json!({"tasks":[]}))
        }
        Err(error) => return Err(error.into()),
    };
    let mut newest = BTreeSet::new();
    for entry in entries {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("657865632d")
            || !name.ends_with(".json")
            || !entry.file_type()?.is_file()
        {
            continue;
        }
        newest.insert((entry.metadata()?.modified()?, entry.path()));
        if newest.len() > limit {
            newest.pop_first();
        }
    }
    let mut tasks = Vec::new();
    for (_, path) in newest.into_iter().rev() {
        let value: Value = serde_json::from_slice(&std::fs::read(path)?)?;
        tasks.push(value);
    }
    Ok(
        json!({"tasks":tasks,"status_source":"local last observation; query each task_id for live state"}),
    )
}

async fn execute(
    executor: &RemoteExecutorImpl,
    workspace: &Workspace,
    node: Option<&str>,
    script: &str,
    control: bool,
) -> CoreResult<ExecutionRecord> {
    if let Some(node) = node {
        if control { return executor.execute_compute_control(workspace,node,script).await; }
        executor
            .execute_compute_ssh(workspace, node, script, None)
            .await
    } else {
        executor
            .execute_control(workspace, script, Some(Path::new("/")), &Default::default())
            .await
    }
}

pub async fn start(
    executor: &RemoteExecutorImpl,
    config: &LrWorkspaceConfig,
    workspace: &Workspace,
    node: Option<&str>,
    command: &str,
    working_dir: Option<&Path>,
) -> CoreResult<Value> {
    if node.is_none() && workspace.node_execution_mode != NodeExecutionMode::DirectCompute {
        return Err(CoreError::PermissionDenied(
            "Background workloads cannot execute on a login node".into(),
        ));
    }
    let cwd = normalize_remote_absolute(&RemoteExecutorImpl::resolve_mcp_working_dir(
        workspace,
        working_dir,
    )?)?;
    let id = Uuid::new_v4();
    let work = normalize_remote_absolute(&workspace.remote_work_dir)?;
    let relative = format!("._lrworkspace_tasks/{id}");
    let directory = format!("{}/{}", work.trim_end_matches('/'), relative);
    let temporary = format!("{directory}/exit_code.tmp");
    let complete = format!("{directory}/exit_code");
    let on_exit = format!(
        "rc=$?; printf '%s\\n' \"$rc\" > {}; mv -- {} {}",
        shell_quote(&temporary),
        shell_quote(&temporary),
        shell_quote(&complete)
    );
    let runner = format!(
        r#"#!/bin/bash
umask 077
cd -- {directory} || exit 23
trap {on_exit} EXIT
hostname -s > hostname
if [ -r "/proc/$$/stat" ]; then awk '{{print $22}}' "/proc/$$/stat" > process_start;
else ps -p "$$" -o lstart= > process_start; fi
printf '%s\n' "$$" > pid
cd -- {cwd} || exit 23
/bin/bash -lc {command}
"#,
        directory = shell_quote(&directory),
        on_exit = shell_quote(&on_exit),
        cwd = shell_quote(&cwd),
        command = shell_quote(command)
    );
    let script = format!(
        r#"umask 077
command -v setsid >/dev/null 2>&1 || {{ echo 'LRW: setsid is required for independent background tasks' >&2; exit 25; }}
work=$(realpath -e -- {work}) || exit 20
parent="$work/._lrworkspace_tasks"
mkdir -p -- "$parent" || exit 20
parent=$(realpath -e -- "$parent") || exit 20
case "$parent/" in "${{work%/}}/"*) ;; *) exit 22;; esac
task="$parent/{id}"
mkdir -- "$task" || exit 20
printf '%s' {runner} > "$task/runner.sh" || exit 20
nohup setsid /bin/bash "$task/runner.sh" </dev/null >"$task/stdout" 2>"$task/stderr" &
printf 'LRW_TASK_STARTED:{id}\n'
"#,
        work = shell_quote(&work),
        runner = shell_quote(&runner)
    );
    let mut task = json!({"kind":"ssh-task","task_id":id,"workspace_id":workspace.id,
        "mode":if node.is_some(){"cluster"}else{"direct"},"execution_path":if node.is_some(){"ssh"}else{"direct"},
        "compute_node":node,"remote_host":workspace.connection.host,"remote_user":workspace.connection.user,
        "remote_port":workspace.connection.port,"work_root":workspace.remote_work_dir,"relative_directory":relative,
        "status":"starting","created_at":chrono::Utc::now(),"working_dir":cwd,
        "stdout_path":format!("{directory}/stdout"),"stderr_path":format!("{directory}/stderr")});
    // Persist the handle before dispatch so an uncertain launch can be inspected without replaying it.
    task["observation_started_at"]=task["created_at"].clone();
    task=commit_observation(config,workspace,id,task,executor.timeout()).await?;
    let result = execute(executor, workspace, node, &script, false).await;
    match result {
        Ok(record)
            if record.exit_code == Some(0)
                && record.stdout.contains(&format!("LRW_TASK_STARTED:{id}")) =>
        {
            task["status"] = json!("started")
        }
        Ok(record) => {
            task["status"] = json!("unknown");
            task["start_error"] = json!(record.stderr.chars().take(4096).collect::<String>());
        }
        Err(error) => {
            task["status"] = json!("unknown");
            task["start_error"] = json!(error.to_string().chars().take(4096).collect::<String>());
        }
    }
    task["instruction"]=json!("Query this task_id for live state and logs. Do not replay an unknown launch. Task survives MCP/SSH disconnects.");
    lrworkspace_core::privacy::redact_value(&mut task);
    match commit_observation(config,workspace,id,task.clone(),executor.timeout()).await {
        Ok(latest)=>task=latest,
        Err(error)=>task["warning"]=json!(format!("Task handle was created but status update failed: {error}; do not resubmit")),
    }
    Ok(task)
}

pub async fn poll(
    executor: &RemoteExecutorImpl,
    config: &LrWorkspaceConfig,
    workspace: &Workspace,
    id: Uuid,
    log_bytes: Option<usize>,
) -> CoreResult<Value> {
    let mut task = load(config, workspace, id)?;
    let observed_at=chrono::Utc::now();
    let node = task["compute_node"].as_str();
    let directory = format!(
        "{}/._lrworkspace_tasks/{id}",
        normalize_remote_absolute(&workspace.remote_work_dir)?.trim_end_matches('/')
    );
    let directory = shell_quote(&directory);
    let mut script = format!(
        r#"task={directory}
if [ ! -d "$task" ]; then printf 'STATE=unknown\n'; exit 0; fi
task=$(realpath -e -- "$task") || exit 22
if [ -f "$task/exit_code" ]; then
  code=$(cat -- "$task/exit_code")
  case "$code" in ''|*[!0-9]*) printf 'STATE=unknown\n';; *) printf 'STATE=finished\nCODE=%s\n' "$code";; esac
elif [ -f "$task/pid" ]; then
  pid=$(cat -- "$task/pid")
  case "$pid" in ''|*[!0-9]*) printf 'STATE=unknown\n';; *)
    birth=$(cat -- "$task/process_start" 2>/dev/null)
    if [ -r "/proc/$pid/stat" ]; then current=$(awk '{{print $22}}' "/proc/$pid/stat");
    else current=$(ps -p "$pid" -o lstart= 2>/dev/null); fi
    if [ -n "$birth" ] && [ "$birth" = "$current" ] && kill -0 "$pid" 2>/dev/null;
    then printf 'STATE=running\n'; else printf 'STATE=unknown\n'; fi;; esac
else printf 'STATE=starting\n'; fi
if [ -f "$task/hostname" ]; then printf 'HOST='; head -n 1 -- "$task/hostname"; fi
"#
    );
    if let Some(limit) = log_bytes {
        if !(1..=65536).contains(&limit) {
            return Err(CoreError::Validation(
                "max_bytes must be 1-65536 per stream".into(),
            ));
        }
        for (field, file) in [("OUT", "stdout"), ("ERR", "stderr")] {
            script.push_str(&format!("printf '{field}='; if [ -f \"$task/{file}\" ]; then tail -c {limit} -- \"$task/{file}\" | base64 | tr -d '\\n'; fi; printf '\\n'\nprintf '{field}_SIZE='; if [ -f \"$task/{file}\" ]; then wc -c < \"$task/{file}\"; else printf '0\\n'; fi\n"));
        }
    }
    let record = execute(executor, workspace, node, &script, true).await?;
    if record.exit_code != Some(0) {
        return Err(CoreError::Connection(
            "Task monitoring failed; task outcome remains unknown".into(),
        ));
    }
    let fields = record
        .stdout
        .lines()
        .filter_map(|line| line.split_once('='))
        .collect::<std::collections::HashMap<_, _>>();
    let code = fields.get("CODE").and_then(|v| v.parse::<i32>().ok());
    task["status"] = json!(match fields.get("STATE").copied() {
        Some("finished") if code.is_some_and(|code|(0..=255).contains(&code)) =>
            if code == Some(0) {
                "succeeded"
            } else {
                "failed"
            },
        Some("running") => "running",
        Some("starting") => "starting",
        _ => "unknown",
    });
    if task["status"] == "starting"
        && task["created_at"]
            .as_str()
            .and_then(|text| chrono::DateTime::parse_from_rfc3339(text).ok())
            .is_some_and(|time| {
                (chrono::Utc::now() - time.with_timezone(&chrono::Utc)).num_seconds() > 30
            })
    {
        task["status"] = json!("unknown");
    }
    task["exit_code"] = json!(code);
    task["hostname"] = json!(fields.get("HOST"));
    task["last_checked_at"] = json!(chrono::Utc::now());
    task["observation_started_at"]=json!(observed_at);
    match commit_observation(config,workspace,id,task.clone(),executor.timeout()).await {
        Ok(latest)=>task=latest,
        Err(error)=>task["warning"]=json!(format!("Live state retrieved, but local state update failed: {error}")),
    }
    if let Some(limit) = log_bytes {
        if task["observation_started_at"]!=json!(observed_at) {
            task["warning"]=json!("Retained a confirmed task state from another observation; request logs again if final output is needed");
        }
        task["logs_observation_started_at"]=json!(observed_at);
        for (field, name) in [("OUT", "stdout"), ("ERR", "stderr")] {
            let bytes = crate::b64::decode(fields.get(field).copied().unwrap_or(""))
                .map_err(CoreError::Validation)?;
            task[name] = json!(String::from_utf8_lossy(&bytes));
            task[format!("{name}_truncated")] = json!(fields
                .get(format!("{field}_SIZE").as_str())
                .and_then(|n| n.trim().parse::<usize>().ok())
                .is_some_and(|n| n > limit));
        }
    }
    Ok(task)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn observation(status:&str,code:Value,seconds:i64)->Value {
        json!({"kind":"ssh-task","task_id":"fixture","workspace_id":"fixture","remote_host":"fixture.invalid",
            "remote_user":"fixture","remote_port":22,"work_root":"/work","compute_node":null,"status":status,"exit_code":code,
            "observation_started_at":chrono::DateTime::from_timestamp(seconds,0).unwrap()})
    }
    #[test]
    fn valid_terminal_evidence_survives_older_and_newer_nonterminal_updates() {
        for (state,code) in [("succeeded",0),("failed",9)] {
            let terminal=observation(state,json!(code),20);
            for time in [10,30] {
                assert_eq!(merge_observation(Some(terminal.clone()),observation("starting",Value::Null,time)).unwrap(),terminal);
            }
            assert_eq!(merge_observation(Some(observation("running",Value::Null,30)),terminal.clone()).unwrap(),terminal);
            let refreshed=observation(state,json!(code),30);
            assert_eq!(merge_observation(Some(terminal.clone()),refreshed.clone()).unwrap(),refreshed);
            assert_eq!(merge_observation(Some(refreshed.clone()),terminal).unwrap(),refreshed);
        }
    }
    #[test]
    fn observation_order_prevents_late_start_callbacks_from_regressing_live_state() {
        let latest=observation("running",Value::Null,20);
        assert_eq!(merge_observation(Some(latest.clone()),observation("started",Value::Null,10)).unwrap(),latest);
        let mut different=latest.clone();different["remote_host"]=json!("other.invalid");
        assert!(merge_observation(Some(latest),different).is_err());
    }
    #[test]
    fn invalid_exit_evidence_cannot_freeze_a_terminal_state() {
        let invalid=observation("failed",Value::Null,30);
        assert!(!terminal(&invalid));
        let success=observation("succeeded",json!(0),20);
        assert_eq!(merge_observation(Some(invalid),success.clone()).unwrap(),success);
        assert!(!terminal(&observation("failed",json!(9999),20)));
    }
}
