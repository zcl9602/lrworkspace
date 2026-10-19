//! Persistent storage for remote-first workspaces.
//!
//! Workspace state contains connection metadata, one writable work root and zero or
//! more remote data roots readable through the MCP file API (writes rejected).
//! No cache, hydration, sync baseline, or daemon state is persisted.

use crate::config::LrWorkspaceConfig;
use crate::error::{CoreError, CoreResult};
use crate::safety::{normalize_remote_absolute, remote_paths_overlap};
use crate::types::*;
use std::path::{Path, PathBuf};
use uuid::Uuid;

fn workspaces_dir(config: &LrWorkspaceConfig) -> PathBuf {
    config.data_dir.join("workspaces")
}

fn workspace_path(config: &LrWorkspaceConfig, id: WorkspaceId) -> PathBuf {
    workspaces_dir(config).join(format!("{id}.json"))
}

fn cluster_path(config: &LrWorkspaceConfig, id: WorkspaceId) -> PathBuf {
    workspaces_dir(config).join(format!("{id}.cluster.json"))
}

fn env_path(config: &LrWorkspaceConfig, id: WorkspaceId) -> PathBuf {
    workspaces_dir(config).join(format!("{id}.env.json"))
}

fn status_path(config: &LrWorkspaceConfig, id: WorkspaceId) -> PathBuf {
    workspaces_dir(config).join(format!("{id}.status.json"))
}

fn validate_definition(workspace: &Workspace) -> CoreResult<()> {
    normalize_remote_absolute(&workspace.remote_work_dir)?;
    for data_dir in &workspace.remote_data_dirs {
        normalize_remote_absolute(data_dir)?;
        if remote_paths_overlap(data_dir, &workspace.remote_work_dir)? {
            return Err(CoreError::Validation(format!(
                "remote_work_dir and remote_data_dir must not overlap: {} <-> {}",
                workspace.remote_work_dir.display(),
                data_dir.display()
            )));
        }
    }
    Ok(())
}

pub fn save_workspace(config: &LrWorkspaceConfig, workspace: &Workspace) -> CoreResult<()> {
    validate_definition(workspace)?;
    write_json_atomic(&workspace_path(config, workspace.id), workspace)
}

/// Port changes are explicit and backed up; never silently reinterpret 22.
pub fn update_connection_port(config:&LrWorkspaceConfig,workspace:&mut Workspace,port:u16)->CoreResult<Option<PathBuf>> {
    use std::io::Write;
    if port==0{return Err(CoreError::Validation("SSH port must be 1-65535".into()));}
    if workspace.connection.port==port{return Ok(None);}
    let source=workspace_path(config,workspace.id);let bytes=std::fs::read(&source)?;
    let directory=config.data_dir.join("registration-backups");std::fs::create_dir_all(&directory)?;
    let backup=directory.join(format!("{}.{}.json",workspace.id,Uuid::new_v4()));
    let mut options=std::fs::OpenOptions::new();options.write(true).create_new(true);
    #[cfg(unix)] {use std::os::unix::fs::OpenOptionsExt;options.mode(0o600);}
    let mut file=options.open(&backup)?;file.write_all(&bytes)?;file.sync_all()?;
    let old=workspace.connection.port;workspace.connection.port=port;
    if let Err(error)=save_workspace(config,workspace){workspace.connection.port=old;return Err(error);}
    Ok(Some(backup))
}

/// A failed write must not truncate an existing registration or cached state.
fn write_json_atomic(path: &Path, value: &impl serde::Serialize) -> CoreResult<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
    let temporary = path.with_extension(format!("{}.tmp", Uuid::new_v4()));
    let result = (|| -> CoreResult<()> {
        let mut file = std::fs::OpenOptions::new().write(true).create_new(true).open(&temporary)?;
        file.write_all(serde_json::to_string_pretty(value)?.as_bytes())?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() { let _ = std::fs::remove_file(&temporary); }
    result
}

pub fn load_workspace(config: &LrWorkspaceConfig, id: WorkspaceId) -> CoreResult<Workspace> {
    let path = workspace_path(config, id);
    let payload = std::fs::read_to_string(&path).map_err(|error| {
        CoreError::NotFound(format!(
            "Workspace {id} not found at {}: {error}",
            path.display()
        ))
    })?;
    let workspace: Workspace = serde_json::from_str(&payload).map_err(|error| {
        CoreError::Config(format!("Invalid workspace registration {id} at {}: {error}", path.display()))
    })?;
    validate_definition(&workspace)?;
    Ok(workspace)
}

pub fn list_workspace_ids(config: &LrWorkspaceConfig) -> CoreResult<Vec<WorkspaceId>> {
    let dir = workspaces_dir(config);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let Some(name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        if name.matches('.').count() != 1 || !name.ends_with(".json") {
            continue;
        }
        if let Ok(id) = Uuid::parse_str(name.trim_end_matches(".json")) {
            ids.push(id);
        }
    }
    ids.sort();
    Ok(ids)
}

pub fn list_workspaces(config: &LrWorkspaceConfig) -> CoreResult<Vec<Workspace>> {
    let mut result = Vec::new();
    for id in list_workspace_ids(config)? {
        result.push(load_workspace(config, id)?);
    }
    result.sort_by(|a, b| a.display_name.cmp(&b.display_name));
    Ok(result)
}

pub fn delete_workspace(config: &LrWorkspaceConfig, id: WorkspaceId) -> CoreResult<()> {
    for path in [
        workspace_path(config, id),
        cluster_path(config, id),
        env_path(config, id),
        status_path(config, id),
    ] {
        if path.exists() {
            std::fs::remove_file(path)?;
        }
    }
    Ok(())
}

/// Select a registration explicitly by UUID/name, or use the only registration.
/// The agent's current project directory never selects a remote workspace.
pub fn resolve_workspace(config: &LrWorkspaceConfig, selector: Option<&str>) -> CoreResult<Workspace> {
    if let Some(selector) = selector.filter(|value| !value.is_empty()) {
        if let Ok(id) = Uuid::parse_str(selector) { return load_workspace(config, id); }
        let matches: Vec<_> = list_workspaces(config)?.into_iter()
            .filter(|workspace| workspace.name == selector || workspace.display_name == selector).collect();
        return match matches.len() {
            1 => Ok(matches.into_iter().next().expect("one workspace")),
            0 => Err(CoreError::NotFound(format!("No workspace named {selector}"))),
            _ => Err(CoreError::Validation(format!("Ambiguous workspace name {selector}; use its UUID"))),
        };
    }
    let workspaces = list_workspaces(config)?;
    match workspaces.len() {
        1 => Ok(workspaces.into_iter().next().expect("one workspace")),
        0 => Err(CoreError::NotFound("No workspace registered; run rw init".into())),
        _ => Err(CoreError::Validation("Multiple workspaces registered; pass --workspace <UUID or name>".into())),
    }
}

pub fn write_status_file(
    config: &LrWorkspaceConfig,
    workspace_id: WorkspaceId,
    status: &ConnectionStatus,
) -> CoreResult<PathBuf> {
    std::fs::create_dir_all(workspaces_dir(config))?;
    let path = status_path(config, workspace_id);
    write_json_atomic(&path, status)?;
    Ok(path)
}

pub fn load_cluster_config(
    config: &LrWorkspaceConfig,
    id: WorkspaceId,
) -> CoreResult<ClusterConfig> {
    let payload = std::fs::read_to_string(cluster_path(config, id))?;
    Ok(serde_json::from_str(&payload)?)
}

pub fn save_cluster_config(
    config: &LrWorkspaceConfig,
    cluster: &ClusterConfig,
    id: WorkspaceId,
) -> CoreResult<()> {
    std::fs::create_dir_all(workspaces_dir(config))?;
    write_json_atomic(&cluster_path(config, id), cluster)?;
    Ok(())
}

pub fn save_env_snapshot(config: &LrWorkspaceConfig, snapshot: &EnvSnapshot) -> CoreResult<()> {
    std::fs::create_dir_all(workspaces_dir(config))?;
    write_json_atomic(&env_path(config, snapshot.workspace_id), snapshot)?;
    Ok(())
}

pub fn load_env_snapshot(config: &LrWorkspaceConfig, id: WorkspaceId) -> CoreResult<EnvSnapshot> {
    let payload = std::fs::read_to_string(env_path(config, id))?;
    Ok(serde_json::from_str(&payload)?)
}

fn job_record_path(config: &LrWorkspaceConfig, workspace: WorkspaceId, job_id: &str) -> CoreResult<PathBuf> {
    if job_id.is_empty() || job_id.len()>100 || !job_id.bytes().all(|byte|byte.is_ascii_alphanumeric()||b"_.-[]:".contains(&byte)) {
        return Err(CoreError::Validation("Invalid job ID for local submission record".into()));
    }
    let key=job_id.bytes().map(|byte|format!("{byte:02x}")).collect::<String>();
    Ok(config.data_dir.join("jobs").join(workspace.to_string()).join(format!("{key}.json")))
}

pub fn save_job_record(config:&LrWorkspaceConfig, workspace:WorkspaceId, job_id:&str, record:&serde_json::Value) -> CoreResult<()> {
    write_json_atomic(&job_record_path(config,workspace,job_id)?,record)
}

pub fn load_job_record(config:&LrWorkspaceConfig, workspace:WorkspaceId, job_id:&str) -> CoreResult<Option<serde_json::Value>> {
    match std::fs::read_to_string(job_record_path(config,workspace,job_id)?) {
        Ok(payload)=>Ok(Some(serde_json::from_str(&payload)?)),
        Err(error) if error.kind()==std::io::ErrorKind::NotFound=>Ok(None),
        Err(error)=>Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn array_job_records_are_workspace_scoped_and_path_safe() {
        let root=std::env::temp_dir().join(format!("lrw-job-record-test-{}",Uuid::new_v4()));
        let config=LrWorkspaceConfig {data_dir:root.clone(),..Default::default()};
        let first=Uuid::new_v4();let second=Uuid::new_v4();
        let record=serde_json::json!({"run":"retained"});
        save_job_record(&config,first,"123[].server",&record).unwrap();
        assert_eq!(load_job_record(&config,first,"123[].server").unwrap(),Some(record));
        assert!(load_job_record(&config,second,"123[].server").unwrap().is_none());
        assert!(load_job_record(&config,first,"../../file").is_err());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn legacy_local_directories_are_ignored_and_selection_is_explicit() {
        let root = std::env::temp_dir().join(format!("lrw-store-test-{}", Uuid::new_v4()));
        let config = LrWorkspaceConfig { data_dir: root.join("state"), ..Default::default() };
        let mut value = serde_json::to_value(workspace()).unwrap();
        let old_local = root.join("old-local-project");
        value["local_temp_dir"] = serde_json::json!(old_local);
        let mut first: Workspace = serde_json::from_value(value).unwrap();
        save_workspace(&config, &first).unwrap();
        assert!(!old_local.exists(), "legacy local directories must not be created");
        assert_eq!(resolve_workspace(&config, None).unwrap().id, first.id);
        assert_eq!(resolve_workspace(&config, Some("test")).unwrap().id, first.id);
        // Updating an existing registration must atomically replace it.
        first.display_name = "renamed".into();
        save_workspace(&config, &first).unwrap();
        assert_eq!(load_workspace(&config, first.id).unwrap().display_name, "renamed");
        let mut second = first.clone(); second.id = Uuid::new_v4();
        save_workspace(&config, &second).unwrap();
        assert!(resolve_workspace(&config, None).is_err());
        assert!(resolve_workspace(&config, Some("test")).is_err());
        assert_eq!(resolve_workspace(&config, Some(&second.id.to_string())).unwrap().id, second.id);
        std::fs::write(workspace_path(&config, second.id), "invalid json").unwrap();
        assert!(list_workspaces(&config).is_err(), "corrupt records must not silently select another workspace");
        delete_workspace(&config, first.id).unwrap();
        assert!(!workspace_path(&config, first.id).exists());
        std::fs::remove_dir_all(root).unwrap();
    }

    fn workspace() -> Workspace {
        Workspace {
            id: Uuid::new_v4(),
            name: "test".into(),
            display_name: "test".into(),
            connection: ConnectionConfig {
                host: "example".into(),
                port: 22,
                user: "user".into(),
                auth: AuthMethod::Agent,
                jump_hosts: Vec::new(),
            },
            remote_work_dir: PathBuf::from("/remote/work"),

            remote_data_dirs: vec![PathBuf::from("/remote/data")],
            node_execution_mode: NodeExecutionMode::SubmitOnly,
            authorized: true,
            created_at: chrono::Utc::now(),
            last_accessed_at: chrono::Utc::now(),
            file_threshold_bytes: 1_048_576,
            config: Default::default(),
        }
    }

    #[test]
    fn rejects_overlapping_work_and_data_roots() {
        let mut workspace = workspace();
        workspace.remote_data_dirs = vec![PathBuf::from("/remote/work/data")];
        assert!(validate_definition(&workspace).is_err());
    }

    #[test]
    fn new_field_names_are_serialized() {
        let value = serde_json::to_value(workspace()).unwrap();
        assert!(value.get("remote_work_dir").is_some());
        assert!(value.get("local_temp_dir").is_none());
        assert!(value.get("remote_root").is_none());
        assert!(value.get("local_cache_dir").is_none());
    }
}
