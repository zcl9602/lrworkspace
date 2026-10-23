use std::path::{Path,PathBuf};
use std::process::{Command,Output};
use lrworkspace_core::{LrWorkspaceConfig,types::*,workspace_store};

struct Fixture {root:PathBuf,bash:PathBuf,config:LrWorkspaceConfig,workspace:Workspace}
impl Fixture {
    fn new(scheduler:ClusterScheduler)->Option<Self> {
        #[cfg(windows)] let bash=PathBuf::from(r"C:\Program Files\Git\bin\bash.exe");
        #[cfg(unix)] let bash=PathBuf::from("bash");
        if !Command::new(&bash).args(["-c","exit 0"]).status().is_ok_and(|status|status.success()){eprintln!("Bash unavailable; local scheduler fixture skipped");return None;}
        let root=PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("lrw-native-batch-{}",uuid::Uuid::new_v4()));
        std::fs::create_dir_all(root.join("bin")).unwrap();std::fs::create_dir_all(root.join("work")).unwrap();
        let executable=root.join("helper.exe");
        let source=Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake_scheduler.rs");
        let compiled=Command::new("rustc").arg(source).arg("-o").arg(&executable).output().unwrap();
        assert!(compiled.status.success(),"{}",String::from_utf8_lossy(&compiled.stderr));
        for program in ["ssh","scp","rsync","sbatch","qsub","qstat","hostname","sinfo"] {
            let name=if cfg!(windows){format!("{program}.exe")}else{program.into()};
            std::fs::copy(&executable,root.join("bin").join(name)).unwrap();
        }
        let config=LrWorkspaceConfig {data_dir:root.join("state"),..Default::default()};
        let workspace=Workspace {id:uuid::Uuid::new_v4(),name:"fixture".into(),display_name:"fixture".into(),
            connection:ConnectionConfig {host:"only-a-local-fixture".into(),user:"test".into(),port:22,auth:AuthMethod::Agent,jump_hosts:Vec::new()},
            remote_work_dir:posix(&root.join("work")),remote_data_dirs:Vec::new(),node_execution_mode:NodeExecutionMode::SubmitOnly,
            authorized:true,created_at:chrono::Utc::now(),last_accessed_at:chrono::Utc::now(),file_threshold_bytes:1048576,config:Default::default()};
        workspace_store::save_workspace(&config,&workspace).unwrap();
        let cluster=ClusterConfig {scheduler,nodes:Vec::new(),partitions:Vec::new(),
            constraints:ClusterConstraints {max_total_cores:Some(256),max_memory_mb_per_job:Some(1024000),max_walltime_minutes:Some(10080),..Default::default()},refreshed_at:chrono::Utc::now()};
        workspace_store::save_cluster_config(&config,&cluster,workspace.id).unwrap();
        std::fs::write(root.join("config.toml"),"").unwrap();
        std::fs::write(root.join("commands.sh"),"# comment\necho first\nfalse\necho last\n").unwrap();
        std::fs::write(root.join("work/previous-result"),b"keep").unwrap();
        Some(Self {root,bash,config,workspace})
    }
    fn enable_background_fixture(&self) {
        std::fs::copy(self.root.join("helper.exe"),self.root.join("bin").join(if cfg!(windows){"setsid.exe"}else{"setsid"})).unwrap();
    }
    fn command(&self)->Command {
        self.command_for(Path::new(env!("CARGO_BIN_EXE_rw")))
    }
    fn command_for(&self,program:&Path)->Command {
        let mut command=Command::new(program);
        let mut paths=vec![self.root.join("bin")];
        #[cfg(windows)] paths.push(PathBuf::from(r"C:\Program Files\Git\usr\bin"));
        if let Some(path)=std::env::var_os("PATH"){paths.extend(std::env::split_paths(&path));}
        command.env("PATH",std::env::join_paths(paths).unwrap())
            .env("LRW_TEST_TRANSFER_LOG",self.root.join("transfers.log"))
            .env("LRW_TEST_POLL_ROOT",&self.root)
            .env("LRW_TEST_PBS_PROBE_LOG",self.root.join("pbs-probes.log")).env_remove("LRW_TEST_PBS_VERSION")
            .env("LRWORKSPACE_SSH",self.root.join("bin").join(if cfg!(windows){"ssh.exe"}else{"ssh"})).env("LRWORKSPACE_SCP",self.root.join("bin").join(if cfg!(windows){"scp.exe"}else{"scp"}))
            .env("LRWORKSPACE_CONFIG",self.root.join("config.toml")).env("LRWORKSPACE_DATA_DIR",&self.config.data_dir)
            .env("LRWORKSPACE_RSYNC",self.root.join("bin").join(if cfg!(windows){"rsync.exe"}else{"rsync"}))
            .env("MSYS2_ENV_CONV_EXCL","LRW_TEST_BIN_POSIX").env("LRW_TEST_BIN_POSIX",posix(&self.root.join("bin"))).env("LRW_TEST_SSH_LOG",self.root.join("compute-ssh.log")).env_remove("LRW_TEST_COMPUTE_FAILURE").env_remove("LRW_TEST_ON_COMPUTE").env_remove("LRW_TEST_SAME_HOST").env_remove("LRW_TEST_IDLE_COMPUTE")
            .env("LRW_TEST_BASH",&self.bash).env("LRW_TEST_SUBMIT_LOG",self.root.join("submissions.log"))
            .env_remove("LRWORKSPACE_FILE_THRESHOLD_MB").env_remove("LRWORKSPACE_SSH_TIMEOUT")
            .current_dir(&self.root);
        command
    }
}
impl Drop for Fixture {fn drop(&mut self){let _=std::fs::remove_dir_all(&self.root);}}
fn posix(path:&Path)->PathBuf {
    let value=path.to_string_lossy().replace('\\',"/");
    #[cfg(windows)] let value=format!("/{}{}",value[..1].to_ascii_lowercase(),&value[2..]);
    value.into()
}
fn success(output:Output)->serde_json::Value {
    assert!(output.status.success(),"stdout={} stderr={}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));
    serde_json::from_slice(&output.stdout).unwrap()
}

#[test]
fn slurm_cli_generates_then_submits_a_fresh_array_without_deleting_previous_outputs() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let first=success(f.command().args(["submit","--workspace","fixture","--in","commands.sh","--outdir","JOBS","--cpu","4","--maxcpu","8","--shuffle-seed","42","--timeout-secs","30"]).output().unwrap());
    assert_eq!(first["status"],"generated");assert!(!f.root.join("submissions.log").exists());
    let result=success(f.command().args(["submit","--workspace","fixture","--in","commands.sh","--outdir","JOBS","--cpu","4","--maxcpu","8","--shuffle-seed","42","--timeout-secs","30","--submit"]).output().unwrap());
    assert_eq!(result["job_id"],"4242");assert_eq!(result["batch"]["array_parallel"],2);
    assert_ne!(first["batch"]["run_id"],result["batch"]["run_id"]);
    let relative=result["batch"]["script_relative"].as_str().unwrap();
    let script=std::fs::read_to_string(f.root.join("work").join(relative)).unwrap();
    assert!(script.contains("#SBATCH --array=1-3%2"));assert!(!script.contains("#SBATCH --mem="));
    assert!(std::fs::read_to_string(f.root.join("submissions.log")).unwrap().contains("sbatch --job-name=job"));
    assert_eq!(std::fs::read(f.root.join("work/previous-result")).unwrap(),b"keep");
    assert!(workspace_store::load_job_record(&f.config,f.workspace.id,"4242").unwrap().is_some());
    let logs=f.command().args(["slurm","logs","--workspace","fixture","--job-id","4242"]).output().unwrap();
    assert!(logs.status.success(),"{}",String::from_utf8_lossy(&logs.stderr));
    assert!(String::from_utf8_lossy(&logs.stdout).contains("not completion evidence"));
}

#[test]
fn pbs_cli_detects_openpbs_and_keeps_server_and_array_job_id() {
    let Some(f)=Fixture::new(ClusterScheduler::Pbs) else{return};
    let result=success(f.command().args(["submit","--workspace","fixture","--in","commands.sh","--outdir","JOBS","--cpu","2","--maxcpu","4","--timeout-secs","30","--submit"]).output().unwrap());
    assert_eq!(result["job_id"],"4243[].server");assert_eq!(result["batch"]["pbs_flavor"],"pbs-pro");
    let script=std::fs::read_to_string(f.root.join("work").join(result["batch"]["script_relative"].as_str().unwrap())).unwrap();
    assert!(script.contains("#PBS -J 1-3%2"));assert!(!script.contains("#PBS -l mem="));
    assert!(workspace_store::load_job_record(&f.config,f.workspace.id,"4243[].server").unwrap().is_some());
}

#[test]
fn pbs_detection_handles_torque_stderr_usage_and_server_fallback_without_retrying_a_job() {
    for (mode,flavor,expected_probe) in [("torque-bare","torque","qsub --version"),
        ("fallback-torque","torque","qstat --version"),("help-pro","pbs-pro","qsub --help"),
        ("help-torque","torque","qsub --help"),("server-pro","pbs-pro","qstat -Bf")] {
        let Some(f)=Fixture::new(ClusterScheduler::Pbs) else{return};
        let result=success(f.command().env("LRW_TEST_PBS_VERSION",mode).args(["submit","--workspace","fixture","--in","commands.sh","--timeout-secs","30","--submit"]).output().unwrap());
        assert_eq!(result["batch"]["pbs_flavor"],flavor,"{mode}");
        assert_eq!(std::fs::read_to_string(f.root.join("submissions.log")).unwrap().lines().count(),1,"{mode}");
        let probes=std::fs::read_to_string(f.root.join("pbs-probes.log")).unwrap();assert!(probes.contains(expected_probe),"{mode}: {probes}");
        // A new client process reuses the workspace choice even if the wrapper
        // subsequently hides every version/help response.
        let second=success(f.command().env("LRW_TEST_PBS_VERSION","unknown").args(["submit","--workspace","fixture","--in","commands.sh","--timeout-secs","30"]).output().unwrap());
        assert_eq!(second["batch"]["pbs_flavor"],flavor);
        assert_eq!(std::fs::read_to_string(f.root.join("pbs-probes.log")).unwrap(),probes);
    }
}

#[test]
fn pbs_unknown_is_safe_and_an_explicit_choice_is_remembered_but_not_for_another_endpoint() {
    let Some(f)=Fixture::new(ClusterScheduler::Pbs) else{return};
    let output=f.command().env("LRW_TEST_PBS_VERSION","unknown").args(["submit","--workspace","fixture","--in","commands.sh","--submit"]).output().unwrap();
    assert!(!output.status.success());assert!(!f.root.join("submissions.log").exists());
    assert!(!f.root.join("work/._lrworkspace_jobs").exists());
    let result=success(f.command().env("LRW_TEST_PBS_VERSION","unknown").args(["submit","--workspace","fixture","--in","commands.sh","--pbs-flavor","torque","--submit"]).output().unwrap());
    assert_eq!(result["batch"]["pbs_flavor"],"torque");
    let probes=std::fs::read_to_string(f.root.join("pbs-probes.log")).unwrap();
    let cached=success(f.command().env("LRW_TEST_PBS_VERSION","unknown").args(["submit","--workspace","fixture","--in","commands.sh"]).output().unwrap());
    assert_eq!(cached["batch"]["pbs_flavor"],"torque");
    assert_eq!(std::fs::read_to_string(f.root.join("pbs-probes.log")).unwrap(),probes);
    let mut workspace=f.workspace.clone();workspace.connection.port=2222;workspace_store::save_workspace(&f.config,&workspace).unwrap();
    let changed=f.command().env("LRW_TEST_PBS_VERSION","unknown").args(["submit","--workspace","fixture","--in","commands.sh"]).output().unwrap();
    assert!(!changed.status.success());
    assert_eq!(std::fs::read_to_string(f.root.join("submissions.log")).unwrap().lines().count(),1);
}

#[test]
fn ordinary_cluster_run_uses_the_same_array_mode() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let result=success(f.command().args(["run","--workspace","fixture","--force","cluster","--cpu","4","--maxcpu","8","--timeout-secs","30","--","echo","one-command"]).output().unwrap());
    assert_eq!(result["mode"],"cluster");assert_eq!(result["batch"]["command_count"],1);
    assert_eq!(result["batch"]["array_tasks"],1);assert_eq!(result["batch"]["options"]["cpu"],4);
    assert_eq!(result["job_id"],"4242");
}

#[test]
fn submitted_job_id_is_preserved_when_audit_storage_fails() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    std::fs::write(f.config.data_dir.join("audit"),b"fixture blocks audit directory creation").unwrap();
    let result=success(f.command().args(["submit","--workspace","fixture","--in","commands.sh","--timeout-secs","30","--submit"]).output().unwrap());
    assert_eq!(result["status"],"submitted");assert_eq!(result["job_id"],"4242");
    assert!(result["warning"].as_str().unwrap().contains("audit"));
}

#[test]
#[ignore="requires the MCP binary built alongside the CLI; uses only local fake SSH/schedulers"]
fn mcp_command_array_submission_uses_the_same_pbs_backend() {
    use std::io::Write;
    use std::process::Stdio;
    let Some(f)=Fixture::new(ClusterScheduler::Pbs) else{return};
    let executable=Path::new(env!("CARGO_BIN_EXE_rw")).parent().unwrap().join(if cfg!(windows){"lrworkspace-mcp.exe"}else{"lrworkspace-mcp"});
    assert!(executable.is_file(),"Build lrworkspace-mcp before this explicit integration test");
    let mut child=f.command_for(&executable).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut input=child.stdin.take().unwrap();
    for request in [
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"batch-test"}}}),
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_submit","arguments":{"workspace_id":"fixture","commands_text":"echo first\nfalse\necho last","batch":{"outdir":"JOBS","cpu":2,"maxcpu":4,"pbs_flavor":"torque"},"submit":true,"timeout_sec":30}}}),
    ] {writeln!(input,"{request}").unwrap();}
    drop(input);let output=child.wait_with_output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    let mut responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect(); responses.sort_by_key(|v|v["id"].as_u64());
    assert_eq!(responses.len(),2);assert!(responses[1].get("error").is_none(),"{}",responses[1]);
    let result:serde_json::Value=serde_json::from_str(responses[1]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(result["job_id"],"4243[].server");assert_eq!(result["batch"]["array_parallel"],2);
    let script=std::fs::read_to_string(f.root.join("work").join(result["batch"]["script_relative"].as_str().unwrap())).unwrap();
    assert!(script.contains("#PBS -t 1-3%2"));
    assert_eq!(std::fs::read(f.root.join("work/previous-result")).unwrap(),b"keep");
}

#[test]
fn cluster_ssh_uses_login_hop_and_preserves_quotes_workdir_and_exit_status() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    std::fs::create_dir(f.root.join("work/space dir")).unwrap();
    let script="printf '%s\\n' \"quoted 'value' and spaces\"; pwd; hostname -s; exit 7";
    let output=f.command().args(["run","--workspace","fixture","--force","cluster","--compute-node","compute01","--working-dir","space dir","--",script]).output().unwrap();
    assert_eq!(output.status.code(),Some(7),"stdout={} stderr={}",String::from_utf8_lossy(&output.stdout),String::from_utf8_lossy(&output.stderr));
    let result:serde_json::Value=serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["mode"],"cluster");assert_eq!(result["execution_path"],"ssh");assert_eq!(result["compute_node"],"compute01");
    let stdout=result["result"]["stdout"].as_str().unwrap();
    assert!(stdout.contains("quoted 'value' and spaces"),"{stdout}");assert!(stdout.contains("/space dir"));assert!(stdout.contains("compute01"));
    assert!(!f.root.join("submissions.log").exists());
    let log=std::fs::read_to_string(f.root.join("compute-ssh.log")).unwrap();
    assert!(log.contains("compute01") && log.contains("BatchMode=yes") && log.contains("ForwardAgent=no"));
    assert_eq!(std::fs::read(f.root.join("work/previous-result")).unwrap(),b"keep");
}

#[test]
fn cluster_ssh_never_falls_back_when_unreachable_or_same_as_login() {
    let Some(f)=Fixture::new(ClusterScheduler::Pbs) else{return};
    for flag in ["LRW_TEST_COMPUTE_FAILURE","LRW_TEST_SAME_HOST"] {
        let output=f.command().env(flag,"1").args(["run","--workspace","fixture","--compute-node","compute01","--","touch should-not-run"]).output().unwrap();
        assert!(!output.status.success(),"{}",String::from_utf8_lossy(&output.stdout));
        assert!(!f.root.join("work/should-not-run").exists());
        assert!(!f.root.join("submissions.log").exists());
    }
}

#[test]
fn cluster_ssh_rejects_conflicting_flags_and_missing_compute_workdir() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    for options in [vec!["--force","direct"],vec!["--force","read-only"],vec!["--cpu","4"],vec!["--working-dir","../outside"],vec!["--working-dir","missing"]] {
        let mut c=f.command();c.args(["run","--workspace","fixture","--compute-node","compute01"]);c.args(options).args(["--","touch should-not-run"]);
        assert!(!c.output().unwrap().status.success());
    }
    assert!(!f.root.join("work/should-not-run").exists());assert!(!f.root.join("submissions.log").exists());
}

#[test]
#[ignore="requires the MCP binary built alongside the CLI; local fake SSH only"]
fn mcp_cluster_ssh_uses_the_same_explicit_node_route() {
    use std::io::Write;
    use std::process::Stdio;
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let exe=Path::new(env!("CARGO_BIN_EXE_rw")).parent().unwrap().join(if cfg!(windows){"lrworkspace-mcp.exe"}else{"lrworkspace-mcp"});
    let mut child=f.command_for(&exe).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut input=child.stdin.take().unwrap();
    for request in [
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"ssh-test"}}}),
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_run","arguments":{"background":false,"workspace_id":"fixture","command":"hostname -s; exit 9","force":"cluster","compute_node":"compute01","timeout_sec":30}}}),
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workspace_run","arguments":{"background":false,"workspace_id":"fixture","command":"touch should-not-run","force":"cluster","compute_node":"compute01","batch":{"cpu":1}}}}),
    ] {writeln!(input,"{request}").unwrap();}
    drop(input);let output=child.wait_with_output().unwrap();assert!(output.status.success());
    let mut responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect(); responses.sort_by_key(|v|v["id"].as_u64());
    let result:serde_json::Value=serde_json::from_str(responses[1]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(result["execution_path"],"ssh");assert_eq!(result["result"]["exit_code"],9);
    assert!(result["result"]["stdout"].as_str().unwrap().contains("compute01"));
    assert!(responses[2].get("error").is_some() || responses[2]["result"]["isError"]==true,"{}",responses[2]);
    assert!(!f.root.join("submissions.log").exists());assert!(!f.root.join("work/should-not-run").exists());
}

#[test]
fn cluster_ssh_requires_confirmed_role_and_cannot_target_login_host() {
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    for node in ["only-a-local-fixture","-oProxyCommand=x","node;id",""] {
        let output=f.command().args(["run","--workspace","fixture","--force","cluster","--compute-node",node,"--","touch should-not-run"]).output().unwrap();
        assert!(!output.status.success());
    }
    f.workspace.node_execution_mode=NodeExecutionMode::Unconfirmed;
    workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    assert!(!f.command().args(["run","--workspace","fixture","--force","cluster","--compute-node","compute01","--","touch should-not-run"]).output().unwrap().status.success());
    assert!(!f.root.join("compute-ssh.log").exists());assert!(!f.root.join("work/should-not-run").exists());
}

#[test]
fn output_limits_preserve_frames_and_exit_codes() {
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    f.workspace.node_execution_mode=NodeExecutionMode::DirectCompute;workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    let output=f.command().args(["run","--workspace","fixture","--force","direct","--timeout-secs","30","--","head -c 12582912 /dev/zero | tr '\\0' x; head -c 12582912 /dev/zero | tr '\\0' y >&2; exit 7"]).output().unwrap();
    assert_eq!(output.status.code(),Some(7),"{}",String::from_utf8_lossy(&output.stderr));
    let result:serde_json::Value=serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["result"]["stdout"].as_str().unwrap().len(),10*1024*1024);
    assert_eq!(result["result"]["stderr"].as_str().unwrap().len(),10*1024*1024);assert_eq!(result["result"]["output_truncated"],true);
}

#[test]
fn ssh_stdin_deadline_does_not_wait_for_a_nonreading_peer() {
    use std::io::Write;use std::process::Stdio;
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    f.workspace.node_execution_mode=NodeExecutionMode::DirectCompute;workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    let script=format!("# {}\nprintf done","x".repeat(512*1024));let start=std::time::Instant::now();
    let mut child=f.command().env("LRW_TEST_BLOCK_STDIN","1").args(["run","--workspace","fixture","--force","direct","--timeout-secs","1","--","-"]).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    child.stdin.take().unwrap().write_all(script.as_bytes()).unwrap();let output=child.wait_with_output().unwrap();
    let error=String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(),"stdout={}",String::from_utf8_lossy(&output.stdout));
    // Both transports must report the typed timeout and return promptly;
    // Unix process capture and Windows persistent SSH use different wording.
    assert!(error.contains("Timeout:"),"{error}");
    assert!(start.elapsed()<std::time::Duration::from_secs(3),"{error}");
}

#[test]
#[ignore="requires the MCP binary; exercises truncation and same-session reuse through local fake SSH"]
fn mcp_truncated_output_preserves_the_next_request() {
    use std::io::Write;use std::process::Stdio;
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    f.workspace.node_execution_mode=NodeExecutionMode::DirectCompute;workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    let exe=Path::new(env!("CARGO_BIN_EXE_rw")).parent().unwrap().join(if cfg!(windows){"lrworkspace-mcp.exe"}else{"lrworkspace-mcp"});
    let mut child=f.command_for(&exe).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();let mut input=child.stdin.take().unwrap();
    for request in [
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"bounded-output-test"}}}),
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_run","arguments":{"background":false,"workspace_id":"fixture","force":"direct","command":"head -c 12582912 /dev/zero | tr '\\0' x; exit 7","timeout_sec":30}}}),
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workspace_run","arguments":{"background":false,"workspace_id":"fixture","force":"direct","command":"printf after","timeout_sec":30}}}),
    ] {writeln!(input,"{request}").unwrap();}
    drop(input);let output=child.wait_with_output().unwrap();assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    let mut responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect(); responses.sort_by_key(|v|v["id"].as_u64());
    let first:serde_json::Value=serde_json::from_str(responses[1]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    let second:serde_json::Value=serde_json::from_str(responses[2]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(first["result"]["output_truncated"],true);assert_eq!(first["result"]["exit_code"],7);assert_eq!(first["result"]["stdout"].as_str().unwrap().len(),10*1024*1024);
    assert_eq!(second["result"]["stdout"],"after");assert_eq!(second["result"]["output_truncated"],false);
}

struct McpClient {
    child: std::process::Child,
    input: Option<std::process::ChildStdin>,
    output: std::sync::mpsc::Receiver<serde_json::Value>,
}
impl McpClient {
    fn new(f:&Fixture, automatic_nodes:bool)->Self {
        Self::with_pbs_version(f,automatic_nodes,None)
    }
    fn with_pbs_version(f:&Fixture, automatic_nodes:bool,pbs_version:Option<&str>)->Self {
        use std::io::BufRead;
        use std::process::Stdio;
        let binary=Path::new(env!("CARGO_BIN_EXE_rw")).parent().unwrap()
            .join(if cfg!(windows){"lrworkspace-mcp.exe"}else{"lrworkspace-mcp"});
        assert!(binary.is_file(),"Build the matching MCP binary first");
        let mut command=f.command_for(&binary);
        if automatic_nodes {command.env("LRW_TEST_IDLE_COMPUTE","1");}
        if let Some(version)=pbs_version {command.env("LRW_TEST_PBS_VERSION",version);}
        let mut child=command.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let input=child.stdin.take();let stdout=child.stdout.take().unwrap();
        let (sender,output)=std::sync::mpsc::channel();
        std::thread::spawn(move||{
            for line in std::io::BufReader::new(stdout).lines() {
                let Ok(line)=line else{break};
                let value=serde_json::from_str(&line).unwrap();
                if sender.send(value).is_err(){break;}
            }
        });
        let mut client=Self{child,input,output};
        client.send(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"concurrency-test"}}}));
        assert_eq!(client.reply()["id"],1);client
    }
    fn send(&mut self,value:serde_json::Value) {
        use std::io::Write;
        let input=self.input.as_mut().unwrap();writeln!(input,"{value}").unwrap();input.flush().unwrap();
    }
    fn call(&mut self,id:u64,name:&str,arguments:serde_json::Value) {
        self.send(serde_json::json!({"jsonrpc":"2.0","id":id,"method":"tools/call","params":{"name":name,"arguments":arguments}}));
    }
    #[track_caller]
    fn reply(&self)->serde_json::Value {self.output.recv_timeout(std::time::Duration::from_secs(15)).expect("MCP response blocked behind another request")}
    #[track_caller]
    fn result(&self)->serde_json::Value {
        let response=self.reply();assert!(response.get("error").is_none(),"{response}");
        serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
    }
}
impl Drop for McpClient {
    fn drop(&mut self){self.input.take();let _=self.child.kill();let _=self.child.wait();}
}

#[test]
#[ignore="requires matching MCP binary; verifies compatibility paths on local fake compute hosts"]
fn mcp_path_hints_do_not_require_a_failed_first_call_or_allow_login_workloads() {
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let mut client=McpClient::new(&f,true);
    for (index,path) in [serde_json::Value::Null,serde_json::json!("auto"),serde_json::json!("cluster"),
        serde_json::json!("ssh"),serde_json::json!("compute-node"),serde_json::json!("direct")].into_iter().enumerate() {
        client.call(2+index as u64,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"cluster",
            "execution_path":path,"background":false,"command":"hostname -s","timeout_sec":30}));
        let result=client.result();assert_eq!(result["mode"],"cluster");assert_eq!(result["execution_path"],"ssh");
        assert_eq!(result["result"]["stdout"],"compute01\n");
    }
    for batch in [serde_json::Value::Null,serde_json::json!({})] {
        client.call(18,"workspace_run",serde_json::json!({"workspace_id":"fixture","batch":batch,
            "background":false,"command":"hostname -s","timeout_sec":30}));
        assert_eq!(client.result()["result"]["stdout"],"compute01\n");
    }
    assert!(!f.root.join("submissions.log").exists());
    client.call(20,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"direct",
        "execution_path":"ssh","background":false,"command":"touch forbidden-login-workload"}));
    assert!(client.reply().get("error").is_some());assert!(!f.root.join("work/forbidden-login-workload").exists());
    client.call(21,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"read-only",
        "execution_path":"ssh","background":false,"command":"pwd"}));
    assert_eq!(client.result()["result"]["stdout"],format!("{}\n",f.workspace.remote_work_dir.display()));
    client.call(22,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"read-only",
        "execution_path":"ssh","background":false,"command":"touch forbidden-login-workload"}));
    assert!(client.reply().get("error").is_some());assert!(!f.root.join("work/forbidden-login-workload").exists());
    drop(client);
    f.workspace.node_execution_mode=NodeExecutionMode::DirectCompute;workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    let mut client=McpClient::new(&f,false);
    for path in ["ssh","direct"] {
        client.call(2,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"direct",
            "execution_path":path,"background":false,"command":"printf standalone"}));
        assert_eq!(client.result()["result"]["stdout"],"standalone");
    }
}

#[test]
#[ignore="requires matching MCP binary; checks PBS auto detection and persistence without real submission"]
fn mcp_pbs_run_identifies_torque_on_the_first_call_and_reuses_it_after_restart() {
    let Some(f)=Fixture::new(ClusterScheduler::Pbs) else{return};
    let mut client=McpClient::with_pbs_version(&f,false,Some("fallback-torque"));
    client.call(2,"workspace_run",serde_json::json!({"workspace_id":"fixture","execution_path":"submit",
        "command":"printf first-command","timeout_sec":30}));
    let first=client.result();assert_eq!(first["batch"]["pbs_flavor"],"torque");assert_eq!(first["status"],"submitted");
    drop(client);
    let probes=std::fs::read_to_string(f.root.join("pbs-probes.log")).unwrap();
    let mut client=McpClient::with_pbs_version(&f,false,Some("unknown"));
    client.call(2,"workspace_submit",serde_json::json!({"workspace_id":"fixture","commands_text":"printf second-command",
        "submit":true,"timeout_sec":30}));
    assert_eq!(client.result()["batch"]["pbs_flavor"],"torque");
    assert_eq!(std::fs::read_to_string(f.root.join("pbs-probes.log")).unwrap(),probes);
    assert_eq!(std::fs::read_to_string(f.root.join("submissions.log")).unwrap().lines().count(),2);
}

#[test]
#[ignore="requires matching MCP binary; barrier-controlled fake SSH, no remote server"]
fn mcp_concurrent_same_host_commands_ping_and_timeout_are_independent() {
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    f.workspace.node_execution_mode=NodeExecutionMode::DirectCompute;
    workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    let mut client=McpClient::new(&f,false);
    client.call(2,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"direct","background":false,
        "command":"printf started > a-started; for i in $(seq 1 400); do test -f gate-a && break; sleep 0.05; done; printf alpha; exit 7","timeout_sec":30}));
    let deadline=std::time::Instant::now()+std::time::Duration::from_secs(10);
    while !f.root.join("work/a-started").exists(){assert!(std::time::Instant::now()<deadline);std::thread::sleep(std::time::Duration::from_millis(10));}
    client.call(3,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"direct","background":false,"command":"printf beta","timeout_sec":10}));
    let reply=client.reply();assert_eq!(reply["id"],3,"second command must finish while first is blocked: {reply}");
    let beta:serde_json::Value=serde_json::from_str(reply["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(beta["result"]["stdout"],"beta");
    client.send(serde_json::json!({"jsonrpc":"2.0","id":4,"method":"ping"}));assert_eq!(client.reply()["id"],4);
    client.call(5,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"direct","background":false,"command":"sleep 3","timeout_sec":1}));
    let timeout=client.reply();assert_eq!(timeout["id"],5);assert!(timeout.get("error").is_some(),"{timeout}");
    client.call(6,"workspace_run",serde_json::json!({"workspace_id":"fixture","force":"direct","background":false,"command":"touch gate-a; printf after-timeout","timeout_sec":10}));
    let mut replies=vec![client.reply(),client.reply()];replies.sort_by_key(|v|v["id"].as_u64());
    assert_eq!(replies[0]["id"],2);assert_eq!(replies[1]["id"],6);
    let values:Vec<serde_json::Value>=replies.iter().map(|r|{
        assert!(r.get("error").is_none(),"{r}");serde_json::from_str(r["result"]["content"][0]["text"].as_str().unwrap()).unwrap()
    }).collect();
    assert_eq!(values[0]["result"]["exit_code"],7);assert_eq!(values[0]["result"]["stdout"],"alpha");
    assert_eq!(values[1]["result"]["stdout"],"after-timeout");
}

#[test]
fn cluster_automatically_selects_idle_compute_nodes_and_can_explicitly_submit() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let result=success(f.command().env("LRW_TEST_IDLE_COMPUTE","1").args(["run","--workspace","fixture","--","hostname -s"]).output().unwrap());
    assert_eq!(result["execution_path"],"ssh");assert_eq!(result["compute_node"],"compute01");
    assert!(result["result"]["stdout"].as_str().unwrap().contains("compute01"));
    let result=success(f.command().env("LRW_TEST_IDLE_COMPUTE","1").args(["run","--workspace","fixture","--execution-path","submit","--","touch should-not-run-on-login"]).output().unwrap());
    assert_eq!(result["execution_path"],"submit");assert_eq!(result["job_id"],"4242");
    let rejected=f.command().env("LRW_TEST_IDLE_COMPUTE","1").env("LRW_TEST_SAME_HOST","1")
        .args(["run","--workspace","fixture","--execution-path","ssh","--","touch should-not-run-on-login"]).output().unwrap();
    assert!(!rejected.status.success());assert!(!f.root.join("work/should-not-run-on-login").exists());
}

#[test]
fn independent_cli_task_handles_preserve_quoted_paths_exit_codes_and_other_tasks() {
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    f.enable_background_fixture();
    let work=f.root.join("work space ' quoted");std::fs::create_dir(&work).unwrap();
    f.workspace.remote_work_dir=posix(&work);
    workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    let launched=success(f.command().args(["run","--workspace","fixture","--compute-node","compute01","--background","--","printf 'quoted path payload'; exit 7"]).output().unwrap());
    assert_eq!(launched["status"],"started","{launched}");
    let id=launched["task_id"].as_str().unwrap();
    let deadline=std::time::Instant::now()+std::time::Duration::from_secs(10);
    let outcome=loop {
        let state=success(f.command().args(["tasks","--workspace","fixture","--id",id,"--logs"]).output().unwrap());
        if state["status"]=="failed" {break state;}
        assert!(std::time::Instant::now()<deadline,"{state}");
        std::thread::sleep(std::time::Duration::from_millis(50));
    };
    assert_eq!(outcome["exit_code"],7);assert_eq!(outcome["stdout"],"quoted path payload");
    // A concurrent atomic write must not make the task-list reader parse a partial temporary file.
    let jobs=f.config.data_dir.join("jobs").join(f.workspace.id.to_string());
    std::fs::write(jobs.join("657865632d.pending.tmp"),b"{partial").unwrap();
    let listing=success(f.command().args(["tasks","--workspace","fixture"]).output().unwrap());
    assert_eq!(listing["tasks"].as_array().unwrap().len(),1);
    assert_eq!(std::fs::read(f.root.join("work/previous-result")).unwrap(),b"keep");
}

#[test]
#[ignore="requires matching MCP binary; independent detached tasks on a local fake compute host"]
fn mcp_concurrent_background_tasks_have_live_logs_and_survive_restart() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    f.enable_background_fixture();
    let mut client=McpClient::new(&f,true);
    for (id,label,exit) in [(2,"c",9),(3,"d",0)] {
        let command=format!("printf '{label}-start\\n'; printf '{label}-stderr\\n' >&2; for i in $(seq 1 600); do test -f gate-{label} && break; sleep 0.05; done; printf '{label}-end'; exit {exit}");
        client.call(id,"workspace_run",serde_json::json!({"workspace_id":"fixture","command":command}));
    }
    let first=client.result();let second=client.result();
    assert_eq!(first["status"],"started","{first}");assert_eq!(second["status"],"started","{second}");
    let ids=[first["task_id"].as_str().unwrap().to_string(),second["task_id"].as_str().unwrap().to_string()];
    assert_ne!(ids[0],ids[1]);
    let ready=std::time::Instant::now()+std::time::Duration::from_secs(10);
    let logs=loop {
        client.call(4,"workspace_task_logs",serde_json::json!({"workspace_id":"fixture","task_id":ids[0],"max_bytes":65536}));
        let value=client.result();
        if value["status"]=="running" && value["stdout"].as_str().is_some_and(|text|text.contains("-start"))
            && value["stderr"].as_str().is_some_and(|text|text.contains("-stderr")) {break value;}
        assert!(std::time::Instant::now()<ready,"{value}");
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert_eq!(logs["hostname"],"compute01");assert!(logs["stdout"].as_str().unwrap().contains("-start"));
    assert!(logs["stderr"].as_str().unwrap().contains("-stderr"));
    drop(client);
    let mut client=McpClient::new(&f,true);
    client.call(2,"workspace_tasks",serde_json::json!({"workspace_id":"fixture"}));
    assert_eq!(client.result()["tasks"].as_array().unwrap().len(),2);
    // Release both previously detached runners from the new MCP process.
    client.call(3,"workspace_run",serde_json::json!({"workspace_id":"fixture","compute_node":"compute01","background":false,"command":"touch gate-c gate-d"}));
    assert_eq!(client.result()["result"]["exit_code"],0);
    let mut outcomes=Vec::new();
    for (index,id) in ids.iter().enumerate() {
        for _ in 0..20 {
            client.call(10+index as u64,"workspace_task_logs",serde_json::json!({"workspace_id":"fixture","task_id":id,"max_bytes":4}));
            let status=client.result();
            if status["status"]=="succeeded" || status["status"]=="failed" {
                assert!(status["stdout_truncated"].as_bool().unwrap());outcomes.push(status);break;
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }
    assert_eq!(outcomes.len(),2);outcomes.sort_by_key(|v|v["exit_code"].as_i64());
    assert_eq!(outcomes[0]["status"],"succeeded");assert_eq!(outcomes[0]["exit_code"],0);
    assert_eq!(outcomes[1]["status"],"failed");assert_eq!(outcomes[1]["exit_code"],9);
    assert!(!f.root.join("submissions.log").exists(),"automatic SSH must not also submit a duplicate job");
    assert_eq!(std::fs::read(f.root.join("work/previous-result")).unwrap(),b"keep");
}

#[test]
fn file_download_prefers_rsync_reports_scp_and_never_clobbers() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let bytes:Vec<u8>=(0..=255).cycle().take(65536).collect();
    std::fs::write(f.root.join("work/binary ' 中文.bin"),&bytes).unwrap();
    let remote="binary ' 中文.bin";
    let first=f.root.join("first ' 中文.bin");
    let receipt=success(f.command().args(["download","--workspace","fixture","--file",remote,"--to"]).arg(&first).output().unwrap());
    assert_eq!(receipt["transport"],"rsync"); assert_eq!(std::fs::read(&first).unwrap(),bytes);
    let second=f.root.join("second.bin");
    let receipt=success(f.command().env("LRW_TEST_FAIL_RSYNC","1").args(["download","--workspace","fixture","--file",remote,"--to"]).arg(&second).output().unwrap());
    assert_eq!(receipt["transport"],"scp"); assert_eq!(receipt["fallback_reasons"].as_array().unwrap().len(),1);
    assert_eq!(std::fs::read(second).unwrap(),bytes);
    let output=f.command().args(["download","--workspace","fixture","--file",remote,"--to"]).arg(&first).output().unwrap();
    assert!(!output.status.success()); assert_eq!(std::fs::read(first).unwrap(),bytes);
    for strict in [false,true] {
        let target=f.root.join(if strict {"strict.bin"}else{"failed.bin"});
        let mut command=f.command();command.env("LRW_TEST_FAIL_RSYNC","1").env("LRW_TEST_FAIL_SCP","1").args(["download","--workspace","fixture","--file",remote,"--to"]).arg(&target);
        if strict {command.args(["--transport","rsync"]);}
        assert!(!command.output().unwrap().status.success()); assert!(!target.exists());
    }
    let target=f.root.join("partial.bin");
    assert!(!f.command().env("LRW_TEST_TRANSFER_TRUNCATED","1").args(["download","--workspace","fixture","--file",remote,"--to"]).arg(&target).output().unwrap().status.success());
    assert!(!target.exists());
    let shared=f.root.join("concurrent.bin");
    let mut children=Vec::new();
    for _ in 0..2 {children.push(f.command().args(["download","--workspace","fixture","--file",remote,"--to"]).arg(&shared).stdout(std::process::Stdio::null()).stderr(std::process::Stdio::null()).spawn().unwrap());}
    assert_eq!(children.into_iter().filter_map(|mut child|child.wait().ok()).filter(|status|status.success()).count(),1);
    assert_eq!(std::fs::read(shared).unwrap(),bytes);
    std::fs::write(f.root.join("work/large.txt"),vec![b'x';1048577]).unwrap();
    assert!(!f.command().args(["read","--workspace","fixture","--file","large.txt"]).output().unwrap().status.success());
    let full=f.root.join("large.txt");
    let receipt=success(f.command().args(["download","--workspace","fixture","--file","large.txt","--to"]).arg(&full).output().unwrap());
    assert_eq!(receipt["bytes"],1048577);
    assert!(!f.command().args(["read","--workspace","fixture","--file",remote]).output().unwrap().status.success());
    assert!(!f.command().args(["write","--workspace","fixture","--file","keep.txt","--content","new"]).env("LRW_TEST_FAIL_RSYNC","1").env("LRW_TEST_FAIL_SCP","1").output().unwrap().status.success());
    assert!(!f.root.join("work/keep.txt").exists());
}

#[test]
#[ignore="requires matching MCP binary; local synthetic transports only"]
fn mcp_file_images_and_downloads_use_binary_transport() {
    use std::io::Write; use std::process::Stdio;
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let bytes=b"\x89PNG\r\n\x1a\nsynthetic preview";
    std::fs::write(f.root.join("work/image.png"),bytes).unwrap();
    let exe=Path::new(env!("CARGO_BIN_EXE_rw")).parent().unwrap().join(if cfg!(windows){"lrworkspace-mcp.exe"}else{"lrworkspace-mcp"});
    let mut child=f.command_for(&exe).stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();
    let mut input=child.stdin.take().unwrap();
    let destination=f.root.join("image-local.png");
    for request in [
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"file-test"}}}),
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_read_file","arguments":{"workspace_id":"fixture","relative_path":"image.png"}}}),
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workspace_download_file","arguments":{"workspace_id":"fixture","relative_path":"image.png","local_path":destination}}}),
    ] {writeln!(input,"{request}").unwrap();}
    drop(input); let output=child.wait_with_output().unwrap(); assert!(output.status.success());
    let mut responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect();responses.sort_by_key(|v|v["id"].as_u64());
    for response in &responses {assert!(response.get("error").is_none(),"{response}");}
    let preview=&responses[1]["result"]["content"];
    assert_eq!(preview[1]["type"],"image"); assert_eq!(preview[1]["mimeType"],"image/png");
    assert_eq!(lrworkspace_remote::b64::decode(preview[1]["data"].as_str().unwrap()).unwrap(),bytes);
    let metadata:serde_json::Value=serde_json::from_str(preview[0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(metadata["transport"],"rsync"); assert!(metadata.get("content").is_none());
    assert_eq!(std::fs::read(destination).unwrap(),bytes);
}

#[test]
fn explicit_cluster_targets_are_verified_before_any_workload_is_sent() {
    let Some(f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    let rejected=f.command().args(["run","--workspace","fixture","--compute-node","login02","--","touch should-not-run"]).output().unwrap();
    assert!(!rejected.status.success());assert!(!f.root.join("work/should-not-run").exists());
    assert!(!f.root.join("compute-ssh.log").exists());
    let rejected=f.command().env("LRW_TEST_TARGET_HOST","other-login").args(["run","--workspace","fixture","--compute-node","compute01","--","touch should-not-run"]).output().unwrap();
    assert!(!rejected.status.success());assert!(!f.root.join("work/should-not-run").exists());
    let absent=f.command().env("LRW_TEST_NO_COMPUTE","1").args(["run","--workspace","fixture","--compute-node","compute01","--","touch should-not-run"]).output().unwrap();
    assert!(!absent.status.success());assert!(!f.root.join("work/should-not-run").exists());
}

#[test]
#[ignore="requires matching MCP binary; two isolated local clients and a deterministic poll gate"]
fn mcp_concurrent_status_never_regresses_across_processes() {
    let Some(mut f)=Fixture::new(ClusterScheduler::Slurm) else{return};
    f.workspace.node_execution_mode=NodeExecutionMode::DirectCompute;
    workspace_store::save_workspace(&f.config,&f.workspace).unwrap();
    std::fs::copy(f.root.join("helper.exe"),f.root.join("bin").join(if cfg!(windows){"head.exe"}else{"head"})).unwrap();
    let id=uuid::Uuid::new_v4();let directory=f.root.join("work/._lrworkspace_tasks").join(id.to_string());
    std::fs::create_dir_all(&directory).unwrap();std::fs::write(directory.join("hostname"),b"poll-fixture\n").unwrap();
    let key=format!("exec-{id}");
    workspace_store::save_job_record(&f.config,f.workspace.id,&key,&serde_json::json!({
        "kind":"ssh-task","task_id":id,"workspace_id":f.workspace.id,"compute_node":null,
        "remote_host":f.workspace.connection.host,"remote_user":f.workspace.connection.user,
        "remote_port":f.workspace.connection.port,"work_root":f.workspace.remote_work_dir,
        "created_at":chrono::Utc::now(),"status":"started"
    })).unwrap();
    let mut first=McpClient::new(&f,false);let mut second=McpClient::new(&f,false);
    first.call(2,"workspace_task_status",serde_json::json!({"workspace_id":"fixture","task_id":id}));
    let deadline=std::time::Instant::now()+std::time::Duration::from_secs(10);
    while !f.root.join("first-poll-head").exists() {assert!(std::time::Instant::now()<deadline,"first poll did not reach gate");std::thread::sleep(std::time::Duration::from_millis(5));}
    std::fs::write(directory.join("exit_code"),b"0\n").unwrap();
    second.call(2,"workspace_task_status",serde_json::json!({"workspace_id":"fixture","task_id":id}));
    assert_eq!(second.result()["status"],"succeeded");
    std::fs::write(f.root.join("release-poll-head"),b"go").unwrap();
    let older=first.result();assert_eq!(older["status"],"succeeded");assert_eq!(older["exit_code"],0);
    assert_eq!(workspace_store::load_job_record(&f.config,f.workspace.id,&key).unwrap().unwrap()["status"],"succeeded");
    // Read-only monitoring keeps old IP/alias task handles usable after the upgrade.
    let legacy_id=uuid::Uuid::new_v4();let legacy_dir=f.root.join("work/._lrworkspace_tasks").join(legacy_id.to_string());
    std::fs::create_dir_all(&legacy_dir).unwrap();std::fs::write(legacy_dir.join("hostname"),b"poll-fixture\n").unwrap();std::fs::write(legacy_dir.join("exit_code"),b"0\n").unwrap();
    let mut legacy=workspace_store::load_job_record(&f.config,f.workspace.id,&key).unwrap().unwrap();
    legacy["task_id"]=serde_json::json!(legacy_id);legacy["compute_node"]=serde_json::json!("10.0.0.8");legacy["status"]=serde_json::json!("started");legacy["exit_code"]=serde_json::Value::Null;
    workspace_store::save_job_record(&f.config,f.workspace.id,&format!("exec-{legacy_id}"),&legacy).unwrap();
    second.call(3,"workspace_task_status",serde_json::json!({"workspace_id":"fixture","task_id":legacy_id}));
    assert_eq!(second.result()["status"],"succeeded");
}
