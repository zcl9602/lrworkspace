//! Shared CLI/MCP route selection. All scheduler submissions use batch arrays.
use lrworkspace_core::error::CoreResult;

/// One shared routing policy for both CLI and MCP. Legacy compute-node is
/// accepted only as an alias for scheduler-backed cluster execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRoute { ReadOnly, Direct, Cluster }

/// Normalize path hints without changing the already-authorized execution mode.
/// In cluster mode even a legacy "direct" hint can only mean compute-node SSH.
pub fn normalize_execution_path(route:ExecutionRoute,value:&str)->CoreResult<&'static str> {
    use lrworkspace_core::CoreError;
    match value.trim().to_ascii_lowercase().as_str() {
        ""|"auto"=>Ok("auto"),
        "ssh"=>Ok("ssh"),
        "submit"|"submit-only" if route==ExecutionRoute::Cluster=>Ok("submit"),
        "cluster" if route==ExecutionRoute::Cluster=>Ok("auto"),
        "compute-node"|"compute_node" if route==ExecutionRoute::Cluster=>Ok("ssh"),
        "direct" if route==ExecutionRoute::Cluster=>Ok("ssh"),
        "direct" if route==ExecutionRoute::Direct=>Ok("auto"),
        "read-only"|"read_only" if route==ExecutionRoute::ReadOnly=>Ok("auto"),
        _=>Err(CoreError::Validation("execution_path selects transport: auto, ssh, or submit (cluster only). force selects mode: cluster, direct (confirmed standalone only), or read-only. Omit both for automatic compute-node selection".into())),
    }
}

pub fn resolve_execution_route(mode: lrworkspace_core::types::NodeExecutionMode, force: Option<&str>) -> CoreResult<ExecutionRoute> {
    use lrworkspace_core::{CoreError, types::NodeExecutionMode};
    if force == Some("read-only") { return Ok(ExecutionRoute::ReadOnly); }
    if force.is_some_and(|value| !["direct", "cluster", "compute-node"].contains(&value)) {
        return Err(CoreError::Validation("force must be direct, cluster, or read-only".into()));
    }
    match mode {
        NodeExecutionMode::Unconfirmed => Err(CoreError::PermissionDenied("Confirm SSH node mode before running workloads".into())),
        NodeExecutionMode::SubmitOnly if force == Some("direct") => Err(CoreError::PermissionDenied("Direct execution is forbidden on a submit-only login node".into())),
        NodeExecutionMode::SubmitOnly => Ok(ExecutionRoute::Cluster),
        NodeExecutionMode::DirectCompute if force == Some("direct") => Ok(ExecutionRoute::Direct),
        // Legacy registrations may have called a cluster login host direct_compute.
        // Default to compute-node routing; endpoint execution is always explicit.
        NodeExecutionMode::DirectCompute => Ok(ExecutionRoute::Cluster),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn path_compatibility_never_changes_the_authorized_mode() {
        use ExecutionRoute::*;
        for (route,input,expected) in [(Cluster,"auto","auto"),(Cluster," cluster ","auto"),
            (Cluster,"compute-node","ssh"),(Cluster,"compute_node","ssh"),(Cluster,"direct","ssh"),
            (Cluster,"SSH","ssh"),(Cluster,"submit-only","submit"),(Direct,"ssh","ssh"),
            (Direct,"direct","auto"),(ReadOnly,"ssh","ssh"),(ReadOnly,"read-only","auto")] {
            assert_eq!(normalize_execution_path(route,input).unwrap(),expected);
        }
        for (route,input) in [(Direct,"submit"),(ReadOnly,"submit"),(Direct,"cluster"),(Cluster,"invalid"),
                              (ReadOnly,"direct"),(Cluster,"read-only")] {
            assert!(normalize_execution_path(route,input).is_err());
        }
    }
    #[test]
    fn routes_never_fall_back_from_cluster_to_login_execution() {
        use lrworkspace_core::types::NodeExecutionMode::*;
        for mode in [SubmitOnly, DirectCompute] {
            assert_eq!(resolve_execution_route(mode, Some("cluster")).unwrap(), ExecutionRoute::Cluster);
            assert_eq!(resolve_execution_route(mode, Some("compute-node")).unwrap(), ExecutionRoute::Cluster);
        }
        assert_eq!(resolve_execution_route(SubmitOnly, None).unwrap(), ExecutionRoute::Cluster);
        assert!(resolve_execution_route(SubmitOnly, Some("direct")).is_err());
        assert!(resolve_execution_route(Unconfirmed, Some("cluster")).is_err());
        assert_eq!(resolve_execution_route(DirectCompute, None).unwrap(), ExecutionRoute::Cluster);
        assert_eq!(resolve_execution_route(DirectCompute, Some("direct")).unwrap(), ExecutionRoute::Direct);
        assert_eq!(resolve_execution_route(Unconfirmed, Some("read-only")).unwrap(), ExecutionRoute::ReadOnly);
        assert!(resolve_execution_route(DirectCompute, Some("unknown")).is_err());
    }
}
