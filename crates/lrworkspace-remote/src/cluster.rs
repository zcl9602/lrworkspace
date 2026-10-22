//! Cluster detection — auto-discovers the scheduler type, node topology,
//! and partition/queue configuration by running remote commands.
//!
//! ## Detection Script
//!
//! A single shell script is executed on the remote. It detects which
//! scheduler is present (slurm/pbs/sge/lsf/none), then calls the
//! corresponding introspection commands to collect node and partition data.
//!
//! ## Parsing Strategy
//!
//! Output is structured with section markers (`---NODES---`, `---PARTITIONS---`)
//! and parsed locally, avoiding fragile per-command SSH round-trips.

use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{info, instrument, warn};

use lrworkspace_core::error::CoreResult;
use lrworkspace_core::traits::ClusterDetector;
use lrworkspace_core::types::*;

use crate::executor::RemoteExecutorImpl;

/// The combined remote script that detects scheduler + collects node/partition info.
///
/// Output sections:
/// ```text
/// ---SCHEDULER---
/// slurm
/// ---NODES---
/// node001 64 256000 gpu:A100:4 idle partition1,partition2
/// node002 32 128000 (null) alloc partition1
/// ---PARTITIONS---
/// partition1* 128 7-00:00:00 1-00:00:00 node[001-128]
/// partition2 16 infinite 4:00:00 node[201-216]
/// ---END---
/// ```
const CLUSTER_DETECT_SCRIPT: &str = r#"
# Scheduler fallback paths are supplied by execute_scheduler_control.
SCHED="none"
if command -v sbatch >/dev/null 2>&1; then SCHED="slurm"
elif command -v qsub >/dev/null 2>&1; then
    if qstat --version 2>/dev/null | grep -Eqi 'grid[[:space:]_-]*engine|(^|[^a-z])sge([^a-z]|$)'; then SCHED="sge"
    else SCHED="pbs"
    fi
elif command -v bsub >/dev/null 2>&1; then SCHED="lsf"
fi
echo "---SCHEDULER---"
echo "$SCHED"

echo "---NODES---"
case "$SCHED" in
    slurm)
        sinfo -N -h -o '%n %c %m %G %t %P' 2>/dev/null | while read -r line; do
            echo "$line"
        done
        ;;
    pbs)
        pbsnodes -a 2>/dev/null | awk '
            /^[a-zA-Z]/ { if (name != "") { printf "%s %s %s %s %s %s\n", name, cores, mem, gpus, (busy ? "alloc" : state), queue } name=$1; busy=0; explicit_state=0; cores="1"; mem="0"; gpus="0"; state="?"; queue="default" }
            /^[[:space:]]*np[[:space:]]*=/ { n=$3; gsub(/[^0-9]/,"",n); cores=n }
            /^[[:space:]]*state[[:space:]]*=/ && $0 !~ /status/ { s=$3; gsub(/^[[:space:]]+|[[:space:]]+$/,"",s); state=s; explicit_state=1 }
            /^[[:space:]]*jobs[[:space:]]*=/ && NF>=3 { busy=1 }
            /status[[:space:]]*=/ {
                # Parse comma-separated key=value pairs
                rest=$0; gsub(/^[[:space:]]*status[[:space:]]*=[[:space:]]*/,"",rest);
                n=split(rest, kvs, ",");
                for (i=1; i<=n; i++) {
                    kv=kvs[i]; gsub(/^[[:space:]]+|[[:space:]]+$/,"",kv);
                    if (kv ~ /^physmem=/) { v=kv; gsub(/[^0-9]/,"",v); mem=v }
                    if (!explicit_state && kv ~ /^state=/) { v=kv; sub(/^state=/,"",v); gsub(/^[[:space:]]+|[[:space:]]+$/,"",v); state=v }
                    if (kv ~ /^ncpus=/) { v=kv; gsub(/[^0-9]/,"",v); cores=v }
                    if (kv ~ /^gres=/ && kv !~ /gres=\s*,/) { v=kv; gsub(/[^a-zA-Z0-9:]/,"",v); sub(/^gres=/,"",v); gpus=v }
                }
            }
            /^[[:space:]]*physmem[[:space:]]*=/ && $0 !~ /status/ { m=$3; gsub(/[^0-9]/,"",m); mem=m }
            END { if (name != "") printf "%s %s %s %s %s %s\n", name, cores, mem, gpus, (busy ? "alloc" : state), queue }
        ' 2>/dev/null
        ;;
    sge)
        qhost 2>/dev/null | awk 'NR>3 && $1!="" { printf "%s %s ? ? %s %s ?\n", $1, $2, $5, $1 }'
        ;;
    lsf)
        bhosts 2>/dev/null | awk 'NR>1 && $1!="" { printf "%s %s ? ? %s %s ?\n", $1, $4, $2, $1 }'
        ;;
    *)
        echo "${HOSTNAME:-localhost} $(nproc 2>/dev/null || echo 1) $(free -m 2>/dev/null | awk '/Mem/{print $2}' || echo 0) 0 - idle default"
        ;;
esac
# Add architecture hint
echo "ARCH:$(uname -m 2>/dev/null || echo x86_64)"

echo "---PARTITIONS---"
case "$SCHED" in
    slurm)
        # %P carries the default-partition '*'; %N is the node list.
        # %F is an allocated/idle/other/total count, not a default flag.
        sinfo -h -o '%P %D %l %L %N' 2>/dev/null | while read -r line; do
            echo "$line"
        done
        ;;
    pbs)
        # Queue names from qstat -Q
        qstat -Q 2>/dev/null | awk 'NR>2 && NF>0 && $1 !~ /^-/ { print $1, "?", "?", "?", "?", "false", "-" }'
        ;;
    *)
        echo "default 1 infinite 1:00:00 100 true localhost"
        ;;
esac
# Additional queue node lists for PBS
if [ "$SCHED" = "pbs" ]; then
    echo "---QUEUE_NODES---"
    # Use grep+sed per queue: extract acl_hosts lines precisely,
    # then clean whitespace. Much simpler than the shell loop.
    qstat -Q 2>/dev/null | awk 'NR>2 && NF>0 && $1 !~ /^-/ { print $1 }' | while read -r qname; do
        # Extract the acl_hosts block for this queue from qstat -Qf
        BLOCK=$(qstat -Qf "$qname" 2>/dev/null || true)
        HOSTS=$(echo "$BLOCK" | grep '^[[:space:]]*acl_hosts[[:space:]]=' | head -1 | sed 's/.*= *//' | tr -d '[:space:]')
        if [ -n "$HOSTS" ]; then
            echo "$qname $HOSTS"
        else
            echo "$qname ALL_NODES"
        fi
    done
fi
echo "---END---"
"#;

/// Implementation of `ClusterDetector` using SSH remote execution.
pub struct ClusterDetectorImpl {
    executor: Arc<RemoteExecutorImpl>,
}

impl ClusterDetectorImpl {
    pub fn new(executor: Arc<RemoteExecutorImpl>) -> Self {
        Self { executor }
    }

    /// Parse scheduler line: "slurm", "pbs", "sge", "lsf", "none".
    fn parse_scheduler(raw: &str) -> ClusterScheduler {
        match raw.trim().to_lowercase().as_str() {
            "slurm" => ClusterScheduler::Slurm,
            "pbs" => ClusterScheduler::Pbs,
            "sge" => ClusterScheduler::Sge,
            "lsf" => ClusterScheduler::Lsf,
            "none" => ClusterScheduler::None,
            other => ClusterScheduler::Other(other.to_string()),
        }
    }

    /// Parse a node line from sinfo -N -h -o '%n %c %m %G %t %P' or pbsnodes output:
    ///   `hostname cpu_cores memory_mb gres state partition1,partition2,...`
    ///
    /// Minimum 3 fields (hostname, cores, mem). GPU, state, partition optional.
    fn parse_node_line(line: &str) -> Option<NodeInfo> {
        let parts: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        if parts.len() < 3 {
            return None;
        }
        let hostname = parts[0].clone();
        let cpu_cores: u32 = parts[1].parse().unwrap_or(1);
        let memory_mb: u64 = parts[2].parse().unwrap_or(0);
        // Raw value could be bytes, KB or MB — normalize to MB
        let memory_mb = if memory_mb > 1_000_000_000 {
            memory_mb / 1_048_576 // bytes → MB
        } else if memory_mb > 10_000_000 {
            memory_mb / 1024 // KB → MB (e.g. 263793212KB → 257610MB)
        } else {
            memory_mb // already MB
        };

        // GPU field (parts[3]) if present
        let (gpu_count, gpu_model) = if parts.len() > 3 {
            Self::parse_gres(&parts[3])
        } else {
            (0, None)
        };

        // State field (parts[4]) if present
        let state_raw = parts.get(4).cloned().unwrap_or_else(|| "?".to_string());

        // Partitions (remaining fields)
        let partitions: Vec<String> = parts
            .iter()
            .skip(5)
            .flat_map(|s| s.split(','))
            .map(|s| Self::clean_partition_name(s.trim()))
            .filter(|s| !s.is_empty())
            .collect();

        let state = match state_raw.as_str() {
            "idle" | "free" | "ok" => "idle".into(),
            "mix" | "mixed" | "alloc" | "allocated" | "busy" | "job-exclusive" => "alloc".into(),
            "down" | "drain" | "drained" | "offline" | "down*" | "drain*" => "down".into(),
            "resv" | "maint" | "reserved" => "reserved".into(),
            "?" | "unknown" | "(null)" => "unknown".into(),
            other => other.to_string(),
        };

        Some(NodeInfo {
            hostname,
            cpu_cores,
            memory_mb,
            gpu_count,
            gpu_model,
            state,
            partitions,
            arch: None,
        })
    }

    /// Slurm marks the default partition with a trailing `*` in `sinfo`
    /// output (e.g. `cpu64*`); strip it so `sbatch --partition=cpu64` works.
    fn clean_partition_name(name: &str) -> String {
        name.trim_end_matches('*').to_string()
    }

    /// Parse Slurm GRES field: `(null)` → (0, None), `gpu:4` → (4, None), `gpu:A100:4` → (4, Some("A100"))
    pub fn parse_gres(gres: &str) -> (u32, Option<String>) {
        if gres == "(null)" || gres == "0" || gres == "-" || gres == "?" || gres.is_empty() {
            return (0, None);
        }
        // Expected format: "gpu:MODEL:COUNT" or "gpu:COUNT"
        let parts: Vec<&str> = gres.split(':').collect();
        if parts.len() == 1 {
            // Just a number?
            return (parts[0].parse().unwrap_or(0), None);
        }
        // parts[0] should be "gpu"
        match parts.len() {
            2 => (parts[1].parse().unwrap_or(0), None), // gpu:4
            3 => (parts[2].parse().unwrap_or(0), Some(parts[1].to_string())), // gpu:A100:4
            _ => (0, None),
        }
    }

    /// Parse a partition line: `name total_nodes max_walltime default_walltime max_jobs is_default nodes`
    /// Fields 2+ are space-separated.
    fn parse_partition_line(line: &str) -> Option<PartitionInfo> {
        let parts: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        if parts.len() < 2 {
            return None;
        }
        let name = Self::clean_partition_name(&parts[0]);
        let total_nodes: u32 = parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0);
        let max_walltime = parts
            .get(2)
            .map(|s| s.to_string())
            .filter(|s| s != "?" && s != "infinite");
        let default_walltime = parts
            .get(3)
            .map(|s| s.to_string())
            .filter(|s| s != "?" && s != "infinite");
        let max_jobs_per_user: Option<u32> = parts.get(4).and_then(|s| s.parse().ok());
        let is_default = parts
            .get(5)
            .map(|s| s.as_str() == "true" || s.as_str() == "default")
            .unwrap_or(false);
        let nodes: Vec<String> = parts.iter().skip(6).map(|s| s.to_string()).collect();

        Some(PartitionInfo {
            name,
            total_nodes,
            max_walltime,
            default_walltime,
            max_jobs_per_user,
            nodes,
            is_default,
        })
    }

    /// Native sinfo format: `%P %D %l %L %N`. Slurm does not report
    /// a per-user job limit in these fields, so leave that limit unknown.
    fn parse_slurm_partition_line(line: &str) -> Option<PartitionInfo> {
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 5 {
            return None;
        }
        let walltime = |value: &str| {
            if matches!(value.to_ascii_lowercase().as_str(), "?" | "infinite" | "unlimited" | "n/a" | "none") {
                None
            } else {
                Some(value.to_string())
            }
        };
        Some(PartitionInfo {
            name: Self::clean_partition_name(parts[0]),
            total_nodes: parts[1].parse().ok()?,
            max_walltime: walltime(parts[2]),
            default_walltime: walltime(parts[3]),
            max_jobs_per_user: None,
            nodes: vec![parts[4].to_string()],
            is_default: parts[0].ends_with('*'),
        })
    }

    /// Parse the full output from `CLUSTER_DETECT_SCRIPT`.
    fn parse_cluster_output(workspace: &Workspace, output: &str) -> ClusterConfig {
        let mut scheduler = ClusterScheduler::None;
        let mut nodes: Vec<NodeInfo> = Vec::new();
        let mut partitions: Vec<PartitionInfo> = Vec::new();
        let mut queue_nodes: HashMap<String, Vec<String>> = HashMap::new(); // queue name → node list
        let mut section = "";
        let mut arch_hint: Option<String> = None;

        for line in output.lines() {
            let line = line.trim();
            match line {
                "---SCHEDULER---" => section = "scheduler",
                "---NODES---" => section = "nodes",
                "---PARTITIONS---" => section = "partitions",
                "---QUEUE_NODES---" => section = "queue_nodes",
                "---END---" => section = "",
                _ if line.starts_with("ARCH:") => {
                    arch_hint = Some(line[5..].to_string());
                }
                _ => {
                    if line.is_empty() {
                        continue;
                    }
                    match section {
                        "scheduler" => {
                            scheduler = Self::parse_scheduler(line);
                        }
                        "nodes" => {
                            if let Some(node) = Self::parse_node_line(line) {
                                nodes.push(node);
                            }
                        }
                        "partitions" => {
                            let part = if scheduler == ClusterScheduler::Slurm {
                                Self::parse_slurm_partition_line(line)
                            } else {
                                Self::parse_partition_line(line)
                            };
                            if let Some(part) = part {
                                partitions.push(part);
                            }
                        }
                        "queue_nodes" => {
                            let parts: Vec<&str> = line.splitn(2, ' ').collect();
                            if parts.len() == 2 {
                                let raw_nodes = parts[1].trim();
                                let nodes_in_queue: Vec<String> = if raw_nodes == "ALL_NODES" {
                                    // No acl_hosts → all nodes can submit to this queue
                                    nodes.iter().map(|n| n.hostname.clone()).collect()
                                } else {
                                    // Strip all whitespace (newlines, tabs, spaces) then split
                                    let cleaned: String =
                                        raw_nodes.chars().filter(|c| !c.is_whitespace()).collect();
                                    if cleaned.is_empty() {
                                        vec!["(unknown)".into()]
                                    } else {
                                        cleaned
                                            .split(',')
                                            .map(|s| s.trim().to_string())
                                            .filter(|s| !s.is_empty())
                                            .collect()
                                    }
                                };
                                queue_nodes.insert(parts[0].to_string(), nodes_in_queue);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        // Enrich partitions with node lists from queue_nodes
        for part in &mut partitions {
            if let Some(nodes_list) = queue_nodes.get(&part.name) {
                part.nodes = nodes_list.clone();
                part.total_nodes = nodes_list.len() as u32;
            }
        }

        // Apply arch hint to nodes
        if let Some(ref arch) = arch_hint {
            for node in &mut nodes {
                if node.arch.is_none() {
                    node.arch = Some(arch.clone());
                }
            }
        }

        // If no nodes discovered, create a single local node entry
        if nodes.is_empty() {
            nodes.push(NodeInfo {
                hostname: workspace.connection.host.clone(),
                cpu_cores: 1,
                memory_mb: 1024,
                gpu_count: 0,
                gpu_model: None,
                state: "unknown".into(),
                partitions: partitions.iter().map(|p| p.name.clone()).collect(),
                arch: arch_hint,
            });
        }

        ClusterConfig {
            scheduler,
            nodes,
            partitions,
            constraints: ClusterConstraints::default(),
            refreshed_at: chrono::Utc::now(),
        }
    }
}

#[async_trait]
impl ClusterDetector for ClusterDetectorImpl {
    #[instrument(skip(self, workspace))]
    async fn detect_scheduler(&self, workspace: &Workspace) -> CoreResult<ClusterScheduler> {
        let cmd = "if command -v sbatch >/dev/null 2>&1; then echo slurm; \
                   elif command -v qsub >/dev/null 2>&1; then echo pbs; \
                   elif command -v bsub >/dev/null 2>&1; then echo lsf; \
                   else echo none; fi";
        let record = self
            .executor
            .execute_scheduler_control(workspace, cmd, None, &Default::default())
            .await?;
        Ok(Self::parse_scheduler(&record.stdout))
    }

    #[instrument(skip(self, workspace))]
    async fn collect_nodes(&self, workspace: &Workspace) -> CoreResult<Vec<NodeInfo>> {
        let config = self.discover_cluster(workspace).await?;
        Ok(config.nodes)
    }

    #[instrument(skip(self, workspace))]
    async fn collect_partitions(&self, workspace: &Workspace) -> CoreResult<Vec<PartitionInfo>> {
        let config = self.discover_cluster(workspace).await?;
        Ok(config.partitions)
    }

    #[instrument(skip(self, workspace))]
    async fn discover_cluster(&self, workspace: &Workspace) -> CoreResult<ClusterConfig> {
        info!(
            "Discovering cluster topology on {}",
            workspace.connection.host
        );

        // Use a heredoc piped to /bin/sh to avoid zsh compatibility issues.
        let cmd = format!("cat <<'LREOF' | /bin/sh\n{}\nLREOF", CLUSTER_DETECT_SCRIPT);

        let record = self
            .executor
            .execute_scheduler_control(workspace, &cmd, None, &Default::default())
            .await?;

        if record.exit_code != Some(0) && record.stdout.is_empty() {
            warn!(
                "Cluster detection script returned non-zero: {}",
                record.stderr
            );
        }

        let config = Self::parse_cluster_output(workspace, &record.stdout);

        info!(
            scheduler = ?config.scheduler,
            nodes = config.nodes.len(),
            partitions = config.partitions.len(),
            "Cluster discovery complete"
        );

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gridview_pbs_is_not_misdetected_as_grid_engine() {
        #[cfg(windows)] let bash=r"C:\Program Files\Git\bin\bash.exe";
        #[cfg(unix)] let bash="/bin/bash";
        // Shell functions replace every scheduler program: no real server query.
        let stubs=r#"
command() { case "$1:$2" in -v:qsub) return 0;; *) return 1;; esac; }
qstat() { if [ "${1:-}" = --version ]; then printf '%s\n' "$LRW_TEST_VERSION"; fi; }
pbsnodes() { :; }
qhost() { :; }
qconf() { :; }
"#;
        for (version,expected) in [("GridView PBS Professional 23.0","pbs"),("SGE 8.1.9","sge"),("Grid Engine 8.0","sge")] {
            let output=std::process::Command::new(bash).args(["-c",&format!("{stubs}\n{CLUSTER_DETECT_SCRIPT}")]).env("LRW_TEST_VERSION",version).output().unwrap();
            assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
            assert!(String::from_utf8_lossy(&output.stdout).contains(&format!("---SCHEDULER---\n{expected}\n")),"{}",String::from_utf8_lossy(&output.stdout));
        }
    }

    #[test]
    fn pbs_node_type_never_overrides_offline_or_allocated_state() {
        #[cfg(windows)] let bash=r"C:\Program Files\Git\bin\bash.exe";
        #[cfg(unix)] let bash="/bin/bash";
        let stubs=r#"
command() { case "$1:$2" in -v:qsub) return 0;; *) return 1;; esac; }
qstat() { :; }
pbsnodes() { printf '%s\n' 'node-offline' '    state = offline' '    ntype = cluster' '    status = state=free' 'node-busy' '    state = free' '    ntype = cluster' '    jobs = 0/123.server' '    status = state=free,ncpus=32' 'node-idle' '    state = free' '    ntype = cluster'; }
"#;
        let output=std::process::Command::new(bash).args(["-c",&format!("{stubs}\n{CLUSTER_DETECT_SCRIPT}")]).output().unwrap();
        assert!(output.status.success());let text=String::from_utf8(output.stdout).unwrap();
        assert!(text.contains("node-offline 1 0 0 offline default"),"{text}");
        assert!(text.contains("node-busy 32 0 0 alloc default"),"{text}");
        assert!(text.contains("node-idle 1 0 0 free default"),"{text}");
    }

    #[test]
    fn test_parse_scheduler() {
        assert_eq!(
            ClusterDetectorImpl::parse_scheduler("slurm"),
            ClusterScheduler::Slurm
        );
        assert_eq!(
            ClusterDetectorImpl::parse_scheduler("pbs"),
            ClusterScheduler::Pbs
        );
        assert_eq!(
            ClusterDetectorImpl::parse_scheduler("none"),
            ClusterScheduler::None
        );
        assert!(matches!(
            ClusterDetectorImpl::parse_scheduler("moab"),
            ClusterScheduler::Other(_)
        ));
    }

    #[test]
    fn test_parse_node_line() {
        // Slurm sinfo format: hostname cpu_cores memory_mb gres state partitions
        let node =
            ClusterDetectorImpl::parse_node_line("node001 64 256000 gpu:A100:4 idle gpu,cpu")
                .unwrap();
        assert_eq!(node.hostname, "node001");
        assert_eq!(node.cpu_cores, 64);
        assert_eq!(node.gpu_count, 4);
        assert_eq!(node.gpu_model.as_deref(), Some("A100"));
        assert_eq!(node.state, "idle");
        assert_eq!(node.partitions, vec!["gpu", "cpu"]);

        // No GPU
        let node2 =
            ClusterDetectorImpl::parse_node_line("node002 32 64000 (null) alloc cpu").unwrap();
        assert_eq!(node2.gpu_count, 0);
        assert_eq!(node2.state, "alloc");
        assert_eq!(node2.partitions, vec!["cpu"]);

        // GPU with count only
        let node3 =
            ClusterDetectorImpl::parse_node_line("node003 128 512000 gpu:8 mix gpu").unwrap();
        assert_eq!(node3.gpu_count, 8);
        assert_eq!(node3.gpu_model, None);
        assert_eq!(node3.state, "alloc"); // mix → alloc
    }

    #[test]
    fn test_parse_gres() {
        assert_eq!(ClusterDetectorImpl::parse_gres("(null)"), (0, None));
        assert_eq!(ClusterDetectorImpl::parse_gres("gpu:4"), (4, None));
        assert_eq!(
            ClusterDetectorImpl::parse_gres("gpu:A100:4"),
            (4, Some("A100".into()))
        );
        assert_eq!(
            ClusterDetectorImpl::parse_gres("gpu:V100:8"),
            (8, Some("V100".into()))
        );
        assert_eq!(ClusterDetectorImpl::parse_gres("0"), (0, None));
    }

    #[test]
    fn test_parse_partition_line() {
        let part = ClusterDetectorImpl::parse_partition_line(
            "gpu 128 7-00:00:00 1-00:00:00 100 true node[001-128]",
        )
        .unwrap();
        assert_eq!(part.name, "gpu");
        assert_eq!(part.total_nodes, 128);
        assert_eq!(part.max_walltime.as_deref(), Some("7-00:00:00"));
        assert!(part.is_default);
    }

    #[test]
    fn parse_actual_sinfo_partition_fields() {
        let part = ClusterDetectorImpl::parse_slurm_partition_line(
            "cpu64* 128 7-00:00:00 1-00:00:00 cu[001-128]",
        ).unwrap();
        assert_eq!(part.name, "cpu64");
        assert!(part.is_default);
        assert_eq!(part.total_nodes, 128);
        assert_eq!(part.max_walltime.as_deref(), Some("7-00:00:00"));
        assert_eq!(part.default_walltime.as_deref(), Some("1-00:00:00"));
        assert_eq!(part.nodes, vec!["cu[001-128]"]);
        assert_eq!(part.max_jobs_per_user, None);

        let unlimited = ClusterDetectorImpl::parse_slurm_partition_line(
            "gpu 2 infinite n/a gpu[01-02]",
        ).unwrap();
        assert!(!unlimited.is_default);
        assert_eq!(unlimited.max_walltime, None);
        assert_eq!(unlimited.default_walltime, None);
        assert_eq!(unlimited.nodes, vec!["gpu[01-02]"]);
        assert!(ClusterDetectorImpl::parse_slurm_partition_line("cpu* 2 7-00:00:00 1-00:00:00 %Q 0/2/0/2").is_none());
    }
}
