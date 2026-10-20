//! Local-only environment report. Never connects to a server or reads key data.
use std::path::Path;
use std::process::Command;
use std::time::Duration;
use lrworkspace_core::{LrWorkspaceConfig,types::Workspace};
use serde_json::{Value,json};

fn program_report(program:std::ffi::OsString,version_args:&[&str])->Value {
    let located=Path::new(&program).is_file()||crate::openssh::find_on_path(&program.to_string_lossy()).is_some();
    let probe=crate::process::capture(Command::new(&program).args(version_args),Duration::from_secs(3),"local tool probe");
    let usable=located&&probe.as_ref().is_ok_and(|output|output.status.success());
    let version=probe.ok().map(|out|format!("{}{}",String::from_utf8_lossy(&out.stdout),String::from_utf8_lossy(&out.stderr)).lines().next().unwrap_or("").to_string());
    json!({"program":program.to_string_lossy(),"available":usable,"version":version})
}

pub fn environment_report(config:&LrWorkspaceConfig,workspace:Option<&Workspace>)->Value {
    let ssh=program_report(crate::openssh::ssh_program(),&["-V"]);
    // scp has no portable version option; its presence is enough here, as the
    // actual authentication and transfer require a separate user-chosen test.
    let scp=crate::openssh::scp_program();
    let scp_available=Path::new(&scp).is_file()||crate::openssh::find_on_path(&scp.to_string_lossy()).is_some();
    let mut warnings=Vec::new();
    let connection=workspace.map(|ws| {
        if crate::local_auth::is_local_password(&ws.connection) {
            return json!({"workspace_id":ws.id,"host":ws.connection.host,"user":ws.connection.user,"registered_port":ws.connection.port,"ssh_config_used":false,"port_mismatch":false,"auth_mode":"local_password","authentication_ready":crate::local_auth::connection_ready(&ws.connection)});
        }
        match crate::SshConfig::effective(&ws.connection.host,Some(&ws.connection.user),None) {
            Ok(effective)=> {
                let mismatch=effective.port!=Some(ws.connection.port);
                if mismatch {warnings.push("Registered port differs from OpenSSH configuration. An explicit registered port takes precedence; confirm and use workspace_update_connection / rw workspace --port to correct a stale registration.");}
                json!({"workspace_id":ws.id,"host":ws.connection.host,"registered_port":ws.connection.port,"ssh_config_port":effective.port,"user":ws.connection.user,"proxy_command_configured":effective.proxy_command.is_some(),"proxy_jump_configured":effective.proxy_jump.is_some(),"port_mismatch":mismatch})
            }
            Err(error)=>json!({"workspace_id":ws.id,"host":ws.connection.host,"registered_port":ws.connection.port,"configuration_error":error.to_string()}),
        }
    });
    let rsync=crate::file_transfer::local_rsync_report();
    if rsync["compatible"]!=true {warnings.push("rsync is optional: native SCP/base64 remain available. macOS system rsync 2.6.9 lacks protected arguments; use modern rsync >=3 when rsync is required.");}
    if rsync["cygwin_ssh"]==true && rsync["proxy_shell"].is_null(){warnings.push("Cygwin SSH ProxyCommand requires a POSIX shell. Install a matching sh/bash or Git Bash, set LRWORKSPACE_RSYNC_SHELL, or use native SCP.");}
    json!({"version":env!("CARGO_PKG_VERSION"),"platform":std::env::consts::OS,"arch":std::env::consts::ARCH,
        "home":lrworkspace_core::config::user_home(),"data_dir":config.data_dir,"base_ready":ssh["available"]==true&&scp_available,
        "ssh":ssh,"scp":{"program":scp.to_string_lossy(),"available":scp_available},"rsync":rsync,
        "ssh_config_override":std::env::var_os("LRWORKSPACE_SSH_CONFIG").map(std::path::PathBuf::from),"connection":connection,"warnings":warnings,
        "scope":"local checks only; authentication, network reachability and remote scheduler behavior are not implied"})
}

pub fn mcp_binary_version(path:&Path)->lrworkspace_core::CoreResult<String> {
    let output=crate::process::checked(Command::new(path).arg("--version"),Duration::from_secs(5),"MCP executable verification")?;
    let text=String::from_utf8_lossy(&output.stdout);text.trim().strip_prefix("lrworkspace-mcp ").map(str::to_owned)
        .ok_or_else(||lrworkspace_core::CoreError::Validation("MCP executable did not report its version; keep matching release binaries together".into()))
}
