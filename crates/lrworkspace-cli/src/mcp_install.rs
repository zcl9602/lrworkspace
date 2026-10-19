//! Immutable version directories and backed-up, atomic user configuration.
use lrworkspace_core::{CoreError,CoreResult};
use std::{fs,path::{Path,PathBuf},io::{Read,Write}};

pub fn install_mcp(timeout:u64,client:&str,data_dir:&Path,install_dir:Option<&Path>)->CoreResult<()> {
    let source=std::env::current_exe()?;
    let directory=source.parent().ok_or_else(||CoreError::InvalidState("Cannot locate release directory".into()))?;
    let mcp=directory.join(executable_name("lrworkspace-mcp"));
    let version=lrworkspace_remote::doctor::mcp_binary_version(&mcp)?;
    if version!=env!("CARGO_PKG_VERSION"){return Err(CoreError::Validation(format!("CLI {} and MCP {version} do not match; extract one complete release",env!("CARGO_PKG_VERSION"))));}
    let home=lrworkspace_core::config::user_home();
    let destination=install_dir.map(Path::to_path_buf).unwrap_or_else(||home.join(".local/share/lrworkspace/versions").join(format!("{}-{}-{}",version,std::env::consts::OS,std::env::consts::ARCH)));
    if !destination.is_absolute()||!data_dir.is_absolute(){return Err(CoreError::Validation("Install and data directories must be absolute paths".into()));}
    let installed_mcp=destination.join(executable_name("lrworkspace-mcp"));
    let mut configurations=Vec::new();
    if matches!(client,"codex"|"all") {
        let base=std::env::var_os("CODEX_HOME").map(PathBuf::from).unwrap_or_else(||home.join(".codex"));
        let path=base.join("config.toml");let existing=read_optional(&path)?;
        configurations.push((path,render_codex_config(&existing,&installed_mcp,data_dir,timeout)?));
    }
    if matches!(client,"cursor"|"all") {
        let path=home.join(".cursor/mcp.json");let existing=read_optional(&path)?;
        configurations.push((path,render_mcp_json(&existing,&installed_mcp,data_dir)?));
    }
    if configurations.is_empty(){return Err(CoreError::Validation("client must be codex, cursor or all".into()));}
    fs::create_dir_all(&destination)?;
    for (name,src) in [("lrworkspace",source.as_path()),("rw",source.as_path()),("lrworkspace-mcp",mcp.as_path())] {install_binary(src,&destination.join(executable_name(name)))?;}
    for (path,text) in configurations {write_backed_up(&path,text.as_bytes())?;println!("Configured {}",path.display());}
    println!("Installed LRW {version}: {}",destination.display());
    println!("CLI: {}",destination.join(executable_name("rw")).display());
    println!("Reconnect the MCP client when current calls finish. Workspaces and old release directories are retained.");Ok(())
}

pub fn show_mcp_config() {
    let home=lrworkspace_core::config::user_home();
    let command=std::env::current_exe().ok().and_then(|p|p.parent().map(|d|d.join(executable_name("lrworkspace-mcp")))).unwrap_or_else(||home.join("lrworkspace-mcp"));
    let data=std::env::var_os("LRWORKSPACE_DATA_DIR").map(PathBuf::from).unwrap_or_else(||home.join(".lrworkspace"));
    println!("{}",render_mcp_json("",&command,&data).expect("example serializes"));
    println!("\n# Codex\n{}",render_codex_config("",&command,&data,120).expect("example serializes"));
}
fn read_optional(path:&Path)->CoreResult<String>{match fs::read_to_string(path){Ok(s)=>Ok(s),Err(e) if e.kind()==std::io::ErrorKind::NotFound=>Ok(String::new()),Err(e)=>Err(e.into())}}

fn render_codex_config(existing:&str,command:&Path,data:&Path,timeout:u64)->CoreResult<String> {
    let value:toml::Value=toml::from_str(existing)?;
    for entry in [value.get("mcp_servers"),value.get("mcp_servers").and_then(|s|s.get("lrworkspace")),value.get("mcp_servers").and_then(|s|s.get("lrworkspace")).and_then(|s|s.get("env"))].into_iter().flatten(){
        if !entry.is_table(){return Err(CoreError::Validation("Codex MCP/server/env entries must be tables; existing configuration preserved".into()));}
    }
    let server=value.get("mcp_servers").and_then(|s|s.get("lrworkspace"));
    if let Some(args)=server.and_then(|s|s.get("args")){if args.as_array().is_none_or(|a|!a.is_empty()&&!(a.len()==1&&a[0].as_str()==Some("--stdio"))){return Err(CoreError::Validation("Existing LRW MCP has custom arguments; review them before updating the native stdio executable".into()));}}
    let mut document=existing.parse::<toml_edit::DocumentMut>().map_err(|e|CoreError::Validation(e.to_string()))?;
    document["mcp_servers"]["lrworkspace"]["command"]=toml_edit::value(command.to_string_lossy().as_ref());
    let tool_timeout=server.and_then(|s|s.get("tool_timeout_sec")).and_then(toml::Value::as_integer).unwrap_or(0).max(timeout.saturating_add(30).min(i64::MAX as u64) as i64);
    document["mcp_servers"]["lrworkspace"]["tool_timeout_sec"]=toml_edit::value(tool_timeout);
    if server.and_then(|s|s.get("startup_timeout_ms")).is_none(){let startup=server.and_then(|s|s.get("startup_timeout_sec")).and_then(toml::Value::as_integer).unwrap_or(10).max(10);document["mcp_servers"]["lrworkspace"]["startup_timeout_sec"]=toml_edit::value(startup);}
    document["mcp_servers"]["lrworkspace"]["env"]["LRWORKSPACE_DATA_DIR"]=toml_edit::value(data.to_string_lossy().as_ref());
    let text=document.to_string();let _:toml::Value=toml::from_str(&text)?;Ok(text)
}
fn render_mcp_json(existing:&str,command:&Path,data:&Path)->CoreResult<String> {
    let mut root:serde_json::Value=if existing.trim().is_empty(){serde_json::json!({})}else{serde_json::from_str(existing)?};
    let object=root.as_object_mut().ok_or_else(||CoreError::Validation("MCP configuration must be an object".into()))?;
    let servers=object.entry("mcpServers").or_insert_with(||serde_json::json!({})).as_object_mut().ok_or_else(||CoreError::Validation("mcpServers must be an object".into()))?;
    let server=servers.entry("lrworkspace").or_insert_with(||serde_json::json!({})).as_object_mut().ok_or_else(||CoreError::Validation("lrworkspace MCP must be an object".into()))?;
    if let Some(args)=server.get("args"){if args.as_array().is_none_or(|a|!a.is_empty()&&a!=&[serde_json::json!("--stdio")]){return Err(CoreError::Validation("Review custom LRW MCP arguments before updating".into()));}}
    server.insert("command".into(),serde_json::json!(command));
    let env=server.entry("env").or_insert_with(||serde_json::json!({})).as_object_mut().ok_or_else(||CoreError::Validation("LRW env must be an object".into()))?;
    env.insert("LRWORKSPACE_DATA_DIR".into(),serde_json::json!(data));Ok(serde_json::to_string_pretty(&root)?+"\n")
}
fn write_backed_up(path:&Path,bytes:&[u8])->CoreResult<()> {
    let previous=match fs::read(path){Ok(b)=>Some(b),Err(e) if e.kind()==std::io::ErrorKind::NotFound=>None,Err(e)=>return Err(e.into())};
    if previous.as_deref()==Some(bytes){return Ok(());}if let Some(parent)=path.parent(){fs::create_dir_all(parent)?;}
    if let Some(previous)=previous{let backup=path.with_extension(format!("{}.bak",uuid::Uuid::new_v4()));write_private_new(&backup,&previous)?;}
    let temporary=path.with_extension(format!("{}.new",uuid::Uuid::new_v4()));write_private_new(&temporary,bytes)?;
    if let Err(e)=fs::rename(&temporary,path){let _=fs::remove_file(&temporary);return Err(e.into());}Ok(())
}
fn write_private_new(path:&Path,bytes:&[u8])->CoreResult<()> {
    let mut options=fs::OpenOptions::new();options.write(true).create_new(true);
    #[cfg(unix)]{use std::os::unix::fs::OpenOptionsExt;options.mode(0o600);}
    let mut file=options.open(path)?;file.write_all(bytes)?;file.sync_all()?;Ok(())
}
fn same_file_contents(a:&Path,b:&Path)->CoreResult<bool> {
    if fs::metadata(a)?.len()!=fs::metadata(b)?.len(){return Ok(false);}
    let mut a=fs::File::open(a)?;let mut b=fs::File::open(b)?;let mut left=[0;8192];let mut right=[0;8192];
    loop{let n=a.read(&mut left)?;let m=b.read(&mut right)?;if n!=m||left[..n]!=right[..m]{return Ok(false);}if n==0{return Ok(true);}}
}
fn install_binary(source:&Path,destination:&Path)->CoreResult<()> {
    if destination.exists(){if same_file_contents(source,destination)?{return Ok(());}return Err(CoreError::Validation(format!("{} contains a different build; use a new --install-dir instead of overwriting a running release",destination.display())));}
    let temporary=destination.with_extension(format!("{}.new",uuid::Uuid::new_v4()));fs::copy(source,&temporary)?;
    #[cfg(unix)]{use std::os::unix::fs::PermissionsExt;fs::set_permissions(&temporary,fs::Permissions::from_mode(0o755))?;}
    fs::rename(&temporary,destination)?;Ok(())
}
fn executable_name(base:&str)->String{format!("{base}{}",std::env::consts::EXE_SUFFIX)}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn updates_existing_codex_preserving_comments_env_and_longer_timeouts(){
        let old="# retain\nmodel='user-model'\n[mcp_servers.other]\ncommand='keep'\n[mcp_servers.lrworkspace]\ncommand='old-tokenfix'\nargs=['--stdio']\ntool_timeout_sec=600\n[mcp_servers.lrworkspace.env]\nCUSTOM='keep'\n";
        let text=render_codex_config(old,Path::new("/new path/mcp"),Path::new("/user data"),120).unwrap();assert!(text.contains("# retain"));let value:toml::Value=toml::from_str(&text).unwrap();
        assert_eq!(value["mcp_servers"]["lrworkspace"]["command"].as_str(),Some("/new path/mcp"));assert_eq!(value["mcp_servers"]["lrworkspace"]["env"]["CUSTOM"].as_str(),Some("keep"));assert_eq!(value["mcp_servers"]["lrworkspace"]["tool_timeout_sec"].as_integer(),Some(600));assert_eq!(render_codex_config(&text,Path::new("/new path/mcp"),Path::new("/user data"),120).unwrap(),text);
    }
    #[test]
    fn new_config_pins_command_data_and_timeout_margin(){
        let text=render_codex_config("",Path::new("C:/用户 name/mcp.exe"),Path::new("C:/用户 name/state"),120).unwrap();let value:toml::Value=toml::from_str(&text).unwrap();assert_eq!(value["mcp_servers"]["lrworkspace"]["tool_timeout_sec"].as_integer(),Some(150));assert_eq!(value["mcp_servers"]["lrworkspace"]["env"]["LRWORKSPACE_DATA_DIR"].as_str(),Some("C:/用户 name/state"));
    }
    #[test]
    fn invalid_configs_and_custom_arguments_fail_before_writing(){
        for text in ["broken!","mcp_servers='bad'","[mcp_servers.lrworkspace]\nargs=['--unknown']"]{assert!(render_codex_config(text,Path::new("/mcp"),Path::new("/data"),120).is_err());}
        for text in ["{broken","[]","{\"mcpServers\":[]}"]{assert!(render_mcp_json(text,Path::new("/mcp"),Path::new("/data")).is_err());}
    }
    #[test]
    fn json_update_retains_other_servers_and_custom_env(){
        let text=render_mcp_json(r#"{"mcpServers":{"other":{"command":"keep"},"lrworkspace":{"command":"old","env":{"CUSTOM":"keep"}}}}"#,Path::new("/new"),Path::new("/data")).unwrap();let value:serde_json::Value=serde_json::from_str(&text).unwrap();assert_eq!(value["mcpServers"]["other"]["command"],"keep");assert_eq!(value["mcpServers"]["lrworkspace"]["env"]["CUSTOM"],"keep");
    }
    #[test]
    fn backed_up_update_is_idempotent(){
        let root=std::env::temp_dir().join(format!("lrw-install-{}",uuid::Uuid::new_v4()));fs::create_dir_all(&root).unwrap();let path=root.join("config");fs::write(&path,b"original").unwrap();write_backed_up(&path,b"new").unwrap();write_backed_up(&path,b"new").unwrap();let copies:Vec<_>=fs::read_dir(&root).unwrap().map(Result::unwrap).filter(|e|e.path().extension().is_some_and(|s|s=="bak")).collect();assert_eq!(copies.len(),1);assert_eq!(fs::read(copies[0].path()).unwrap(),b"original");fs::remove_dir_all(root).unwrap();
    }
}
