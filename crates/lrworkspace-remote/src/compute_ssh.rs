//! Explicit, per-request SSH execution through the configured login node.
use std::{collections::HashMap, path::Path, sync::Arc, time::Duration};
use lrworkspace_core::{CoreError, CoreResult, safety::{normalize_remote_absolute, shell_quote}, types::*};
use crate::{ExecutionRoute, RemoteExecutorImpl};
use lrworkspace_core::{config::LrWorkspaceConfig, traits::ClusterDetector, workspace_store};

/// Probe fresh scheduler-advertised idle nodes before sending any workload.
/// A failed probe is safe to retry; an attempted workload is never replayed.
pub async fn choose_compute_node(executor: &RemoteExecutorImpl, config: &LrWorkspaceConfig,
    workspace: &Workspace) -> CoreResult<Option<String>> {
    let probe = executor.with_timeout(executor.timeout().min(Duration::from_secs(10)));
    let mut cluster = crate::ClusterDetectorImpl::new(Arc::new(probe)).discover_cluster(workspace).await?;
    if let Ok(previous) = workspace_store::load_cluster_config(config, workspace.id) {
        cluster.constraints = previous.constraints;
    }
    workspace_store::save_cluster_config(config, &cluster, workspace.id)?;
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

fn eligible_compute_nodes(workspace: &Workspace, cluster: &ClusterConfig) -> Vec<String> {
    if !matches!(cluster.scheduler, ClusterScheduler::Slurm|ClusterScheduler::Pbs) { return Vec::new(); }
    let mut nodes = cluster.nodes.iter().filter(|node| {
        node.state == "idle" && node.cpu_cores > 0
        && !node.hostname.eq_ignore_ascii_case(&workspace.connection.host)
        && validate_compute_node(ExecutionRoute::Cluster,Some(&node.hostname),false).is_ok()
        && (cluster.constraints.allowed_partitions.is_empty()
            || node.partitions.iter().any(|p|cluster.constraints.allowed_partitions.contains(p))
            || cluster.partitions.iter().any(|p|cluster.constraints.allowed_partitions.contains(&p.name) && p.nodes.contains(&node.hostname)))
    }).map(|node|node.hostname.clone()).collect::<Vec<_>>();
    nodes.sort(); nodes.dedup(); nodes
}

/// Supplying a named node is the caller's explicit authorization for this call.
/// Never interpret flags, usernames, shell syntax or arbitrary SSH arguments.
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
        validate_compute_node(ExecutionRoute::Cluster, Some(node), false)?;
        if node.eq_ignore_ascii_case(&workspace.connection.host) {
            return Err(CoreError::PermissionDenied("compute-node must differ from the login endpoint".into()));
        }
        let cwd = Self::resolve_mcp_working_dir(workspace, working_dir)?;
        let work = normalize_remote_absolute(&workspace.remote_work_dir)?;
        let cwd = normalize_remote_absolute(&cwd)?;
        // Resolve the actual node and directory on the compute host, before the workload.
        let inner = format!("here=$(hostname -s) || exit 24\n[ -n \"$here\" ] && [ \"$here\" != \"$1\" ] || {{ echo 'LRW: refusing workload on login endpoint' >&2; exit 24; }}\nwork=$(realpath -e -- {}) || exit 20\ncwd=$(realpath -e -- {}) || exit 21\ncase \"$cwd/\" in \"${{work%/}}/\"*) ;; *) echo 'LRW: compute working_dir escapes work root' >&2; exit 22;; esac\ncd -- \"$cwd\" || exit 23\n{}",
            shell_quote(&work), shell_quote(&cwd), command);
        // No passwords, interactive shell, agent forwarding or host-key bypass.
        // The login user's own SSH configuration/keys must permit the second hop.
        let remote = format!("bash -lc {} lrw-compute \"$lrw_login_node\"", shell_quote(&inner));
        Ok(format!("lrw_login_node=$(hostname -s) || exit 24\ncase \"$lrw_login_node\" in \"\"|*[!a-zA-Z0-9._-]*) exit 24;; esac\nssh -T -n -o BatchMode=yes -o ForwardAgent=no -o ClearAllForwardings=yes -o ConnectTimeout={} {} {}",
            timeout_secs.clamp(1,30), shell_quote(node), remote_command_argument(&remote)))
    }

    pub async fn execute_compute_ssh(&self, workspace: &Workspace, node: &str, command: &str, working_dir: Option<&Path>) -> CoreResult<ExecutionRecord> {
        let script = Self::build_compute_ssh_command(workspace,node,command,working_dir,self.timeout().as_secs())?;
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
