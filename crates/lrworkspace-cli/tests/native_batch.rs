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
        for program in ["ssh","scp","rsync","sbatch","qsub","hostname"] {
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
    fn command(&self)->Command {
        self.command_for(Path::new(env!("CARGO_BIN_EXE_rw")))
    }
    fn command_for(&self,program:&Path)->Command {
        let mut command=Command::new(program);
        let mut paths=vec![self.root.join("bin")];
        #[cfg(windows)] paths.push(PathBuf::from(r"C:\Program Files\Git\usr\bin"));
        if let Some(path)=std::env::var_os("PATH"){paths.extend(std::env::split_paths(&path));}
        command.env("PATH",std::env::join_paths(paths).unwrap())
            .env("LRWORKSPACE_SSH",self.root.join("bin").join(if cfg!(windows){"ssh.exe"}else{"ssh"})).env("LRWORKSPACE_SCP",self.root.join("bin").join(if cfg!(windows){"scp.exe"}else{"scp"}))
            .env("LRWORKSPACE_CONFIG",self.root.join("config.toml")).env("LRWORKSPACE_DATA_DIR",&self.config.data_dir)
            .env("LRWORKSPACE_RSYNC",self.root.join("bin").join(if cfg!(windows){"rsync.exe"}else{"rsync"}))
            .env("MSYS2_ENV_CONV_EXCL","LRW_TEST_BIN_POSIX").env("LRW_TEST_BIN_POSIX",posix(&self.root.join("bin"))).env("LRW_TEST_SSH_LOG",self.root.join("compute-ssh.log")).env_remove("LRW_TEST_COMPUTE_FAILURE").env_remove("LRW_TEST_ON_COMPUTE").env_remove("LRW_TEST_SAME_HOST")
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
    let responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect();
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
    assert!(stdout.contains("quoted 'value' and spaces"),"{stdout}");assert!(stdout.contains("/space dir"));assert!(stdout.contains("compute-fixture"));
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
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_run","arguments":{"workspace_id":"fixture","command":"hostname -s; exit 9","force":"cluster","compute_node":"compute01","timeout_sec":30}}}),
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workspace_run","arguments":{"workspace_id":"fixture","command":"touch should-not-run","force":"cluster","compute_node":"compute01","batch":{}}}}),
    ] {writeln!(input,"{request}").unwrap();}
    drop(input);let output=child.wait_with_output().unwrap();assert!(output.status.success());
    let responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect();
    let result:serde_json::Value=serde_json::from_str(responses[1]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(result["execution_path"],"ssh");assert_eq!(result["result"]["exit_code"],9);
    assert!(result["result"]["stdout"].as_str().unwrap().contains("compute-fixture"));
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
    assert!(!output.status.success());assert!(String::from_utf8_lossy(&output.stderr).contains("deadline"));assert!(start.elapsed()<std::time::Duration::from_secs(3));
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
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_run","arguments":{"workspace_id":"fixture","force":"direct","command":"head -c 12582912 /dev/zero | tr '\\0' x; exit 7","timeout_sec":30}}}),
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workspace_run","arguments":{"workspace_id":"fixture","force":"direct","command":"printf after","timeout_sec":30}}}),
    ] {writeln!(input,"{request}").unwrap();}
    drop(input);let output=child.wait_with_output().unwrap();assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    let responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect();
    let first:serde_json::Value=serde_json::from_str(responses[1]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    let second:serde_json::Value=serde_json::from_str(responses[2]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(first["result"]["output_truncated"],true);assert_eq!(first["result"]["exit_code"],7);assert_eq!(first["result"]["stdout"].as_str().unwrap().len(),10*1024*1024);
    assert_eq!(second["result"]["stdout"],"after");assert_eq!(second["result"]["output_truncated"],false);
}
