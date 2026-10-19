//! Isolated user profiles. Never reads/writes the developer's client settings.
use std::{path::{Path,PathBuf},process::{Command,Stdio},io::{Read,Write}};
use lrworkspace_core::{LrWorkspaceConfig,types::*,workspace_store};
struct Fixture {root:PathBuf,home:PathBuf,data:PathBuf}
impl Fixture {
    fn new()->Self {
        let root=PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!("lrw-portable-{}",uuid::Uuid::new_v4()));
        let home=root.join("用户 home");let data=home.join("state");std::fs::create_dir_all(&home).unwrap();std::fs::write(root.join("config.toml"),"").unwrap();
        Self {root,home,data}
    }
    fn command(&self)->Command {
        let mut command=Command::new(env!("CARGO_BIN_EXE_rw"));
        command.env("LRWORKSPACE_HOME",&self.home).env("CODEX_HOME",self.home.join(".codex"))
            .env("LRWORKSPACE_DATA_DIR",&self.data).env("LRWORKSPACE_CONFIG",self.root.join("config.toml"))
            .env_remove("LRWORKSPACE_SSH").env_remove("LRWORKSPACE_SCP").env_remove("LRWORKSPACE_SSH_CONFIG").env_remove("LRWORKSPACE_SSH_TIMEOUT");
        command.current_dir(&self.root);command
    }
}
impl Drop for Fixture {fn drop(&mut self){let _=std::fs::remove_dir_all(&self.root);}}

#[test]
fn socks5_cli_is_binary_clean_and_independent_of_workspace_configuration() {
    use std::net::TcpListener;
    let fixture=Fixture::new();let listener=TcpListener::bind("127.0.0.1:0").unwrap();let endpoint=listener.local_addr().unwrap();
    let worker=std::thread::spawn(move||{let (mut stream,_)=listener.accept().unwrap();stream.set_read_timeout(Some(std::time::Duration::from_secs(3))).unwrap();let mut greeting=[0;3];stream.read_exact(&mut greeting).unwrap();stream.write_all(&[5,0]).unwrap();let mut header=[0;5];stream.read_exact(&mut header).unwrap();let mut name_and_port=vec![0;header[4] as usize+2];stream.read_exact(&mut name_and_port).unwrap();stream.write_all(&[5,0,0,1,127,0,0,1,0,22]).unwrap();stream.write_all(b"SSH-2.0-fixture\r\n\0\xff").unwrap();});
    let output=fixture.command().env("LRWORKSPACE_CONFIG",fixture.root.join("missing.toml")).args(["proxy","--proxy",&endpoint.to_string(),"--host","fixture.test","--port","2048"]).stdin(Stdio::null()).output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));assert_eq!(output.stdout,b"SSH-2.0-fixture\r\n\0\xff");assert!(!fixture.data.exists());worker.join().unwrap();
}

#[test]
fn socks5_relay_flushes_binary_packets_before_peer_eof() {
    use std::net::TcpListener;
    let fixture=Fixture::new();let listener=TcpListener::bind("127.0.0.1:0").unwrap();let endpoint=listener.local_addr().unwrap();
    let worker=std::thread::spawn(move||{let(mut socket,_)=listener.accept().unwrap();socket.set_read_timeout(Some(std::time::Duration::from_secs(3))).unwrap();let mut greeting=[0;3];socket.read_exact(&mut greeting).unwrap();socket.write_all(&[5,0]).unwrap();let mut request=[0;10];socket.read_exact(&mut request).unwrap();socket.write_all(&[5,0,0,1,127,0,0,1,0,22]).unwrap();socket.write_all(&[0,255,1,254]).unwrap();let mut ack=[0;4];socket.read_exact(&mut ack).unwrap();assert_eq!(&ack,b"ACK!");socket.write_all(b"DONE").unwrap();});
    let mut child=fixture.command().args(["proxy","--proxy",&endpoint.to_string(),"--host","10.0.0.1","--port","2048"]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn().unwrap();
    let mut output=child.stdout.take().unwrap();let mut packet=[0;4];output.read_exact(&mut packet).unwrap();assert_eq!(packet,[0,255,1,254]);child.stdin.as_mut().unwrap().write_all(b"ACK!").unwrap();let mut tail=Vec::new();output.read_to_end(&mut tail).unwrap();assert_eq!(tail,b"DONE");assert!(child.wait().unwrap().success());worker.join().unwrap();
}

#[test]
fn native_ssh_include_resolution_detects_and_repairs_a_stale_port() {
    let fixture=Fixture::new();let include=fixture.root.join("ssh included config");let config=fixture.root.join("ssh config");
    std::fs::write(&include,"Host fixture-alias\n HostName 127.0.0.1\n User fixture\n Port 2048\n").unwrap();
    std::fs::write(&config,format!("Include \"{}\"\nHost *\n Port 22\n",include.to_string_lossy().replace('\\',"/"))).unwrap();
    // OpenSSH checks Include ACLs even with -F. Test roots may inherit a
    // sandbox group unknown to OpenSSH; restrict only these new fixtures.
    #[cfg(windows)] {
        let status=Command::new(PathBuf::from(std::env::var_os("WINDIR").unwrap()).join("System32/WindowsPowerShell/v1.0/powershell.exe"))
            .env("LRW_TEST_SSH_INCLUDE",&include).args(["-NoProfile","-NonInteractive","-Command",
                "$identity=[Security.Principal.WindowsIdentity]::GetCurrent().Name; & ($env:WINDIR+'/System32/icacls.exe') $env:LRW_TEST_SSH_INCLUDE /inheritance:r /grant:r ($identity+':(F)'); exit $LASTEXITCODE"])
            .status().unwrap();assert!(status.success());
    }
    let store=LrWorkspaceConfig{data_dir:fixture.data.clone(),..Default::default()};
    let workspace=Workspace{id:uuid::Uuid::new_v4(),name:"fixture".into(),display_name:"fixture".into(),connection:ConnectionConfig{host:"fixture-alias".into(),user:"fixture".into(),port:22,auth:AuthMethod::Agent,jump_hosts:vec![]},remote_work_dir:"/work".into(),remote_data_dirs:vec![],node_execution_mode:NodeExecutionMode::SubmitOnly,authorized:true,created_at:chrono::Utc::now(),last_accessed_at:chrono::Utc::now(),file_threshold_bytes:1048576,config:Default::default()};workspace_store::save_workspace(&store,&workspace).unwrap();
    let output=fixture.command().env("LRWORKSPACE_SSH_CONFIG",&config).env("LRWORKSPACE_RSYNC",fixture.root.join("missing-rsync")).args(["doctor","--workspace","fixture"]).output().unwrap();
    assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));let report:serde_json::Value=serde_json::from_slice(&output.stdout).unwrap();assert_eq!(report["connection"]["ssh_config_port"],2048,"{report}");assert_eq!(report["connection"]["port_mismatch"],true);
    let output=fixture.command().args(["workspace","--id",&workspace.id.to_string(),"--port","2048"]).output().unwrap();assert!(output.status.success());assert_eq!(workspace_store::load_workspace(&store,workspace.id).unwrap().connection.port,2048);
    let backup=std::fs::read_dir(store.data_dir.join("registration-backups")).unwrap().next().unwrap().unwrap();let old:serde_json::Value=serde_json::from_slice(&std::fs::read(backup.path()).unwrap()).unwrap();assert_eq!(old["connection"]["port"],22);
}

#[test]
#[ignore="requires the matching MCP binary built alongside CLI; isolated profiles only"]
fn installing_from_unicode_paths_updates_clients_and_starts_without_rust_or_python_path() {
    let fixture=Fixture::new();let source=Path::new(env!("CARGO_BIN_EXE_rw")).parent().unwrap();let bundle=fixture.root.join("release 中文 path");std::fs::create_dir_all(&bundle).unwrap();
    for name in ["rw","lrworkspace-mcp"] {let file=format!("{name}{}",std::env::consts::EXE_SUFFIX);std::fs::copy(source.join(&file),bundle.join(&file)).unwrap();}
    #[cfg(unix)] {use std::os::unix::fs::PermissionsExt;for file in std::fs::read_dir(&bundle).unwrap(){std::fs::set_permissions(file.unwrap().path(),std::fs::Permissions::from_mode(0o755)).unwrap();}}
    std::fs::create_dir_all(fixture.home.join(".codex")).unwrap();std::fs::write(fixture.home.join(".codex/config.toml"),"# retain\n[mcp_servers.other]\ncommand='keep'\n[mcp_servers.lrworkspace]\ncommand='old-tokenfix'\n").unwrap();
    let executable=bundle.join(format!("rw{}",std::env::consts::EXE_SUFFIX));let template=fixture.command();let mut install=Command::new(executable);
    for (key,value) in template.get_envs(){if let Some(value)=value{install.env(key,value);}else{install.env_remove(key);}}
    let output=install.args(["mcp","install","--client","all"]).output().unwrap();assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));
    let text=std::fs::read_to_string(fixture.home.join(".codex/config.toml")).unwrap();let config:toml::Value=toml::from_str(&text).unwrap();assert!(text.contains("# retain"));assert_eq!(config["mcp_servers"]["other"]["command"].as_str(),Some("keep"));
    let server=&config["mcp_servers"]["lrworkspace"];let command=server["command"].as_str().unwrap();assert!(Path::new(command).is_absolute());assert!(!command.contains("old-tokenfix"));assert_eq!(server["env"]["LRWORKSPACE_DATA_DIR"].as_str(),fixture.data.to_str());
    let mut mcp=Command::new(command);for(key,value)in template.get_envs(){if let Some(value)=value{mcp.env(key,value);}else{mcp.env_remove(key);}}
    #[cfg(windows)] mcp.env("PATH",std::env::var_os("WINDIR").map(PathBuf::from).unwrap().join("System32"));
    #[cfg(unix)] mcp.env("PATH","/usr/bin:/bin");
    mcp.env("LRWORKSPACE_RSYNC",fixture.root.join("not-installed"));
    let mut child=mcp.stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped()).spawn().unwrap();let mut input=child.stdin.take().unwrap();
    for request in [serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{},"clientInfo":{"name":"portable-profile"}}}),serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_doctor","arguments":{}}})]{writeln!(input,"{request}").unwrap();}drop(input);
    let output=child.wait_with_output().unwrap();assert!(output.status.success(),"{}",String::from_utf8_lossy(&output.stderr));let responses:Vec<serde_json::Value>=String::from_utf8(output.stdout).unwrap().lines().map(|line|serde_json::from_str(line).unwrap()).collect();assert_eq!(responses[0]["result"]["serverInfo"]["version"],env!("CARGO_PKG_VERSION"));let doctor:serde_json::Value=serde_json::from_str(responses[1]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();assert_eq!(doctor["base_ready"],true);assert_eq!(doctor["rsync"]["compatible"],false);
}
