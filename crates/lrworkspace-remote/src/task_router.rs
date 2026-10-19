//! Shared CLI/MCP route selection. All scheduler submissions use batch arrays.
use lrworkspace_core::error::CoreResult;

/// One shared routing policy for both CLI and MCP. Legacy compute-node is
/// accepted only as an alias for scheduler-backed cluster execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionRoute { ReadOnly, Direct, Cluster }

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
        NodeExecutionMode::DirectCompute if matches!(force, Some("cluster" | "compute-node")) => Ok(ExecutionRoute::Cluster),
        NodeExecutionMode::DirectCompute => Ok(ExecutionRoute::Direct),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert_eq!(resolve_execution_route(DirectCompute, None).unwrap(), ExecutionRoute::Direct);
        assert_eq!(resolve_execution_route(Unconfirmed, Some("read-only")).unwrap(), ExecutionRoute::ReadOnly);
        assert!(resolve_execution_route(DirectCompute, Some("unknown")).is_err());
    }
}
