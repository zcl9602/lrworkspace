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
    format!("task-{id}")
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
        if !name.starts_with("7461736b2d")
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
) -> CoreResult<ExecutionRecord> {
    if let Some(node) = node {
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
    workspace_store::save_job_record(config, workspace.id, &key(id), &task)?;
    let result = execute(executor, workspace, node, &script).await;
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
    if let Err(error) = workspace_store::save_job_record(config, workspace.id, &key(id), &task) {
        task["warning"] = json!(format!(
            "Task handle was created but status update failed: {error}; do not resubmit"
        ));
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
    let record = execute(executor, workspace, node, &script).await?;
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
        Some("finished") =>
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
    if let Err(error) = workspace_store::save_job_record(config, workspace.id, &key(id), &task) {
        task["warning"] = json!(format!(
            "Live state retrieved, but local state update failed: {error}"
        ));
    }
    if let Some(limit) = log_bytes {
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
