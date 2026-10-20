//! Native tools are resolved even when GUI clients have a minimal PATH.
use std::path::{Path,PathBuf};
use std::ffi::OsString;

pub fn find_on_path(name:&str)->Option<PathBuf> {
    let path=Path::new(name);
    if path.components().count()>1 && path.is_file(){return Some(path.to_path_buf());}
    std::env::var_os("PATH").and_then(|paths|std::env::split_paths(&paths).find_map(|directory|{
        let plain=directory.join(name);if plain.is_file(){return Some(plain);}
        let native=directory.join(format!("{name}{}",std::env::consts::EXE_SUFFIX));native.is_file().then_some(native)
    }))
}
fn resolve(name:&str,override_name:&str)->OsString {
    if let Some(program)=std::env::var_os(override_name).filter(|s|!s.is_empty()){return program;}
    #[cfg(windows)] if let Some(windows)=std::env::var_os("WINDIR") {
        let program=PathBuf::from(windows).join("System32/OpenSSH").join(format!("{name}.exe"));if program.is_file(){return program.into_os_string();}
    }
    #[cfg(unix)] {let program=PathBuf::from("/usr/bin").join(name);if program.is_file(){return program.into_os_string();}}
    if let Some(program)=find_on_path(name){return program.into_os_string();}
    name.into()
}
pub fn ssh_program()->OsString {resolve("ssh","LRWORKSPACE_SSH")}
pub fn scp_program()->OsString {resolve("scp","LRWORKSPACE_SCP")}
pub fn ssh_command()->std::process::Command {let mut command=std::process::Command::new(ssh_program());apply_config(&mut command);command}
pub fn scp_command()->std::process::Command {let mut command=std::process::Command::new(scp_program());apply_config(&mut command);command}
pub fn ssh_command_for(config:&lrworkspace_core::ConnectionConfig)->std::process::Command {
    if crate::local_auth::is_local_password(config){let mut command=std::process::Command::new(ssh_program());command.args(["-F","none"]);command}else{ssh_command()}
}
pub fn scp_command_for(config:&lrworkspace_core::ConnectionConfig)->std::process::Command {
    if crate::local_auth::is_local_password(config){let mut command=std::process::Command::new(scp_program());command.args(["-F","none"]);command}else{scp_command()}
}
fn apply_config(command:&mut std::process::Command) {if let Some(path)=std::env::var_os("LRWORKSPACE_SSH_CONFIG"){command.arg("-F").arg(path);}}
fn available(program:OsString)->bool {Path::new(&program).is_file() || find_on_path(&program.to_string_lossy()).is_some()}
pub fn openssh_client_available()->bool {available(ssh_program())&&available(scp_program())}
pub fn ensure_openssh_client()->Result<(),String> {
    if openssh_client_available(){Ok(())}else{Err("Native OpenSSH ssh/scp are required. Install Windows OpenSSH Client or use macOS /usr/bin/ssh; optional overrides: LRWORKSPACE_SSH and LRWORKSPACE_SCP".into())}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn openssh_availability_is_boolean() {
        let _ = openssh_client_available();
    }
}
