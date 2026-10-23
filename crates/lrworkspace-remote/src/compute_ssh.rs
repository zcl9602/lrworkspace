//! Explicit, per-request SSH execution through the configured login node.
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use lrworkspace_core::{CoreError, CoreResult, safety::{normalize_remote_absolute, shell_quote}, types::*};
use crate::{ExecutionRoute, RemoteExecutorImpl};
use lrworkspace_core::{config::LrWorkspaceConfig, traits::ClusterDetector, workspace_store};

async fn fresh_inventory(executor: &RemoteExecutorImpl, config: &LrWorkspaceConfig,
    workspace: &Workspace) -> CoreResult<ClusterConfig> {
    let probe = executor.with_timeout(executor.timeout().min(Duration::from_secs(10)));
    let mut cluster = crate::ClusterDetectorImpl::new(Arc::new(probe)).discover_cluster(workspace).await?;
    if let Ok(previous) = workspace_store::load_cluster_config(config, workspace.id) {
        cluster.constraints = previous.constraints;
    }
    workspace_store::save_cluster_config(config, &cluster, workspace.id)?;
    Ok(cluster)
}

/// Explicit targets still need scheduler evidence that they are compute hosts.
/// Always return the inventory hostname, never an unverified caller-supplied alias.
pub async fn verify_compute_node(executor: &RemoteExecutorImpl, config: &LrWorkspaceConfig,
    workspace: &Workspace, requested: &str) -> CoreResult<String> {
    validate_compute_node(ExecutionRoute::Cluster, Some(requested), false)?;
    let cluster = fresh_inventory(executor, config, workspace).await?;
    resolve_inventory_node(workspace, &cluster, requested)
}

fn resolve_inventory_node(workspace: &Workspace, cluster: &ClusterConfig, requested: &str) -> CoreResult<String> {
    if !matches!(cluster.scheduler, ClusterScheduler::Slurm | ClusterScheduler::Pbs) {
        return Err(CoreError::PermissionDenied("Compute SSH requires a fresh Slurm/PBS node inventory; target identity is unverified".into()));
    }
    let exact: Vec<_> = cluster.nodes.iter().filter(|node| node.hostname.eq_ignore_ascii_case(requested)).collect();
    let candidates = if exact.is_empty() {
        // Short names are accepted only when unambiguous; a caller's arbitrary FQDN is not.
        cluster.nodes.iter().filter(|node| !requested.contains('.') && node.hostname.split('.').next().is_some_and(|host| host.eq_ignore_ascii_case(requested))).collect::<Vec<_>>()
    } else { exact };
    let hosts = candidates.iter().map(|node| node.hostname.to_ascii_lowercase()).collect::<std::collections::BTreeSet<_>>();
    if hosts.len() != 1 {
        return Err(CoreError::PermissionDenied("Requested compute node is absent or ambiguous in the fresh scheduler inventory; workload was not sent".into()));
    }
    let node = candidates.iter().find(|node| partition_allowed(cluster, node))
        .ok_or_else(|| CoreError::PermissionDenied("Compute node is outside the configured allowed partitions".into()))?;
    validate_compute_node(ExecutionRoute::Cluster, Some(&node.hostname), false)?;
    if node.hostname.eq_ignore_ascii_case(&workspace.connection.host) {
        return Err(CoreError::PermissionDenied("Compute target is the login endpoint".into()));
    }
    Ok(node.hostname.clone())
}

/// Probe fresh scheduler-advertised idle nodes before sending any workload.
/// A failed probe is safe to retry; an attempted workload is never replayed.
pub async fn choose_compute_node(executor: &RemoteExecutorImpl, config: &LrWorkspaceConfig,
    workspace: &Workspace) -> CoreResult<Option<String>> {
    let cluster = fresh_inventory(executor, config, workspace).await?;
    let mut nodes = eligible_compute_nodes(workspace, &cluster);
    // Spread simultaneous requests over the eligible nodes instead of always choosing the first.
    if !nodes.is_empty() {
        let offset = (uuid::Uuid::new_v4().as_u128() % nodes.len() as u128) as usize;
        nodes.rotate_left(offset);
    }
    let probe = executor.with_timeout(executor.timeout().min(Duration::from_secs(5)));
    for node in nodes.into_iter().take(4) {
        if probe.execute_compute_ssh(workspace, &node, ":", None).await.is_ok_and(|r|r.exit_code==Some(0)) {
            return Ok(Some(node));
        }
    }
    Ok(None)
}

fn partition_allowed(cluster: &ClusterConfig, node: &NodeInfo) -> bool {
    cluster.constraints.allowed_partitions.is_empty()
        || node.partitions.iter().any(|p| cluster.constraints.allowed_partitions.contains(p))
        || cluster.partitions.iter().any(|p| cluster.constraints.allowed_partitions.contains(&p.name) && p.nodes.contains(&node.hostname))
}

fn eligible_compute_nodes(workspace: &Workspace, cluster: &ClusterConfig) -> Vec<String> {
    if !matches!(cluster.scheduler, ClusterScheduler::Slurm|ClusterScheduler::Pbs) { return Vec::new(); }
    let mut nodes = cluster.nodes.iter().filter(|node| {
        node.state == "idle" && node.cpu_cores > 0
        && !node.hostname.eq_ignore_ascii_case(&workspace.connection.host)
        && validate_compute_node(ExecutionRoute::Cluster,Some(&node.hostname),false).is_ok()
        && partition_allowed(cluster, node)
    }).map(|node|node.hostname.clone()).collect::<Vec<_>>();
    nodes.sort(); nodes.dedup(); nodes
}

/// Validate syntax only. Explicit workload targets additionally need verify_compute_node.
pub fn validate_compute_node(route: ExecutionRoute, node: Option<&str>, has_batch: bool) -> CoreResult<()> {
    let Some(node) = node else { return Ok(()); };
    if route != ExecutionRoute::Cluster {
        return Err(CoreError::Validation("compute-node requires cluster execution".into()));
    }
    if has_batch {
        return Err(CoreError::Validation("compute-node SSH cannot be combined with scheduler batch resources".into()));
    }
    if node.len() > 253 || node.split('.').any(|label| label.is_empty() || label.len() > 63
        || !label.as_bytes()[0].is_ascii_alphanumeric()
        || !label.as_bytes()[label.len()-1].is_ascii_alphanumeric()
        || !label.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-')) {
        return Err(CoreError::Validation("compute-node must be an explicit hostname (letters, digits, hyphens and dots); not SSH options or user@host".into()));
    }
    Ok(())
}

impl RemoteExecutorImpl {
    pub fn build_compute_ssh_command(workspace: &Workspace, node: &str, command: &str, working_dir: Option<&Path>, timeout_secs: u64) -> CoreResult<String> {
        Self::build_compute_command(workspace,node,command,working_dir,timeout_secs,true)
    }

    fn build_compute_command(workspace: &Workspace, node: &str, command: &str, working_dir: Option<&Path>, timeout_secs: u64, verify_identity: bool) -> CoreResult<String> {
        validate_compute_node(ExecutionRoute::Cluster, Some(node), false)?;
        if node.eq_ignore_ascii_case(&workspace.connection.host) {
            return Err(CoreError::PermissionDenied("compute-node must differ from the login endpoint".into()));
        }
        let cwd = Self::resolve_mcp_working_dir(workspace, working_dir)?;
        let work = normalize_remote_absolute(&workspace.remote_work_dir)?;
        let cwd = normalize_remote_absolute(&cwd)?;
        // Resolve the actual node and directory on the compute host, before the workload.
        let expected = node.split('.').next().expect("validated hostname").to_ascii_lowercase();
        let identity_guard=if verify_identity {format!("[ \"$here\" = {} ] || {{ echo 'LRW: SSH target identity differs from the verified compute node' >&2; exit 24; }}\n",shell_quote(&expected))}else{String::new()};
        let inner = format!("here=$(hostname -s | tr '[:upper:]' '[:lower:]') || exit 24\n[ -n \"$here\" ] && [ \"$here\" != \"$1\" ] || {{ echo 'LRW: refusing workload on login endpoint' >&2; exit 24; }}\n{identity_guard}work=$(realpath -e -- {}) || exit 20\ncwd=$(realpath -e -- {}) || exit 21\ncase \"$cwd/\" in \"${{work%/}}/\"*) ;; *) echo 'LRW: compute working_dir escapes work root' >&2; exit 22;; esac\ncd -- \"$cwd\" || exit 23\n{}",
            shell_quote(&work), shell_quote(&cwd), command);
        // No passwords, interactive shell, agent forwarding or host-key bypass.
        // The login user's own SSH configuration/keys must permit the second hop.
        let remote = format!("bash -lc {} lrw-compute \"$lrw_login_node\"", shell_quote(&inner));
        Ok(format!("lrw_login_node=$(hostname -s | tr '[:upper:]' '[:lower:]') || exit 24\ncase \"$lrw_login_node\" in \"\"|*[!a-zA-Z0-9._-]*) exit 24;; esac\nssh -T -n -o BatchMode=yes -o ForwardAgent=no -o ClearAllForwardings=yes -o ConnectTimeout={} {} {}",
            timeout_secs.clamp(1,30), shell_quote(node), remote_command_argument(&remote)))
    }

    pub async fn execute_compute_ssh(&self, workspace: &Workspace, node: &str, command: &str, working_dir: Option<&Path>) -> CoreResult<ExecutionRecord> {
        self.execute_on_compute(workspace,node,command,working_dir,true).await
    }

    /// Only for the fixed status/log scripts of an existing durable task.
    /// Legacy task records may name an IP/SSH alias rather than the canonical host.
    pub(crate) async fn execute_compute_control(&self, workspace: &Workspace, node: &str, command: &str) -> CoreResult<ExecutionRecord> {
        self.execute_on_compute(workspace,node,command,None,false).await
    }

    async fn execute_on_compute(&self, workspace: &Workspace, node: &str, command: &str, working_dir: Option<&Path>, verify_identity:bool) -> CoreResult<ExecutionRecord> {
        let script = Self::build_compute_command(workspace,node,command,working_dir,self.timeout().as_secs(),verify_identity)?;
        let mut record = self.execute_control(workspace,&script,Some(Path::new("/")),&HashMap::new()).await?;
        record.command = command.into();
        record.working_dir = Self::resolve_mcp_working_dir(workspace,working_dir)?;
        Ok(record)
    }
}

// Expand only the trusted local variable, leaving the inner script literal.
fn remote_command_argument(remote: &str) -> String {
    let suffix = "\"$lrw_login_node\"";
    format!("{}\"$lrw_login_node\"",shell_quote(remote.strip_suffix(suffix).expect("trusted remote template")))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn explicit_targets_require_inventory_membership_and_partition_authorization() {
        let workspace:Workspace=serde_json::from_value(serde_json::json!({
            "id":uuid::Uuid::new_v4(),"name":"fixture","display_name":"fixture",
            "connection":{"host":"login.example.invalid","user":"fixture","port":22,"auth":"agent","jump_hosts":[]},
            "remote_work_dir":"/work","remote_data_dirs":[],"node_execution_mode":"submit_only","authorized":true,
            "created_at":chrono::Utc::now(),"last_accessed_at":chrono::Utc::now(),"file_threshold_bytes":1048576,"config":{}
        })).unwrap();
        let node=NodeInfo{hostname:"compute01.example.invalid".into(),cpu_cores:8,memory_mb:16000,
            gpu_count:0,gpu_model:None,state:"alloc".into(),partitions:vec!["cpu".into()],arch:None};
        for scheduler in [ClusterScheduler::Slurm,ClusterScheduler::Pbs] {
            let mut cluster=ClusterConfig{scheduler,nodes:vec![node.clone()],partitions:vec![],constraints:Default::default(),refreshed_at:chrono::Utc::now()};
            assert_eq!(resolve_inventory_node(&workspace,&cluster,"compute01").unwrap(),node.hostname);
            assert!(resolve_inventory_node(&workspace,&cluster,"login02").is_err());
            assert!(resolve_inventory_node(&workspace,&cluster,"compute01.other.invalid").is_err());
            cluster.constraints.allowed_partitions=vec!["gpu".into()];
            assert!(resolve_inventory_node(&workspace,&cluster,"compute01").is_err());
            cluster.constraints.allowed_partitions.clear();
            let mut duplicate=node.clone();duplicate.hostname="compute01.other.invalid".into();cluster.nodes.push(duplicate);
            assert!(resolve_inventory_node(&workspace,&cluster,"compute01").is_err());
            cluster.nodes.clear();assert!(resolve_inventory_node(&workspace,&cluster,"compute01").is_err());
        }
    }
    #[test]
    fn explicit_nodes_cannot_inject_ssh_options_or_shell_commands() {
        for node in ["compute01","compute-1.cluster.example","10.0.0.8"] {
            assert!(validate_compute_node(ExecutionRoute::Cluster,Some(node),false).is_ok());
        }
        for node in ["","-oProxyCommand=x","user@node","node;id","node\nfoo","node/xx","a..b","node."] {
            assert!(validate_compute_node(ExecutionRoute::Cluster,Some(node),false).is_err(),"{node}");
        }
        assert!(validate_compute_node(ExecutionRoute::Direct,Some("compute01"),false).is_err());
        assert!(validate_compute_node(ExecutionRoute::ReadOnly,Some("compute01"),false).is_err());
        assert!(validate_compute_node(ExecutionRoute::Cluster,Some("compute01"),true).is_err());
    }
}
