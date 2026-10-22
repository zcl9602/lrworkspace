//! Remote file transfer — rsync first, SCP fallback, base64 only by explicit request.
//! Reads are bounded; full downloads stream to a new local file. All blocking
//! subprocess waits run outside the asynchronous I/O workers.

use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};
use crate::process;

use async_trait::async_trait;
use tracing::warn;
use uuid::Uuid;

use lrworkspace_core::error::{CoreError, CoreResult};
use lrworkspace_core::safety::{normalize_remote_absolute, shell_quote};
use lrworkspace_core::traits::RemoteFileAccess;
use lrworkspace_core::types::{AuthMethod, ConnectionConfig, RemoteRootKind, Workspace};

use crate::b64;
use crate::executor::RemoteExecutorImpl;
#[cfg(not(windows))]
use crate::ssh_config::control_path_for_connection;

/// Which transport succeeded (for logging / diagnostics).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferMethod {
    Rsync,
    SystemScp,
    Ssh2Sftp,
    Base64Exec,
}

/// A requested transport is strict: only Auto may fall back after failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferPreference { Auto, Rsync, Scp, Base64 }
impl TransferPreference {
    pub fn parse(value: &str) -> CoreResult<Self> {
        match value {
            "auto" => Ok(Self::Auto), "rsync" => Ok(Self::Rsync),
            "scp" => Ok(Self::Scp), "base64" => Ok(Self::Base64),
            _ => Err(CoreError::Validation("transport must be auto, rsync, scp, or base64".into())),
        }
    }
}
impl TransferMethod {
    pub fn as_str(self) -> &'static str {
        match self { Self::Rsync => "rsync", Self::SystemScp => "scp", Self::Ssh2Sftp => "sftp", Self::Base64Exec => "base64" }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct DownloadReceipt {
    #[serde(serialize_with = "serialize_transport")]
    pub transport: TransferMethod,
    pub bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_path: Option<PathBuf>,
    pub fallback_reasons: Vec<String>,
}
fn serialize_transport<S: serde::Serializer>(method: &TransferMethod, serializer: S) -> Result<S::Ok, S::Error> { serializer.serialize_str(method.as_str()) }

struct DownloadTemporary(PathBuf);
impl DownloadTemporary {
    fn new(parent: &Path) -> CoreResult<Self> {
        let path = parent.join(format!(".lrw-download-{}", Uuid::new_v4()));
        #[allow(unused_mut)]
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)] { use std::os::unix::fs::DirBuilderExt; builder.mode(0o700); }
        builder.create(&path)?;
        Ok(Self(path))
    }
    fn payload(&self) -> PathBuf { self.0.join("payload") }
}
impl Drop for DownloadTemporary {
    fn drop(&mut self) { let _ = std::fs::remove_file(self.payload()); let _ = std::fs::remove_dir(&self.0); }
}

struct LocalTemporary(PathBuf);
impl Drop for LocalTemporary {
    fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); }
}

struct OpenSshTarget {
    host: String,
    user: String,
    port: u16,
    #[cfg(not(windows))]
    control_path: PathBuf,
}

fn resolve_openssh_target(config: &ConnectionConfig) -> OpenSshTarget {
    OpenSshTarget {
        host: config.host.clone(),
        user: config.user.clone(),
        port: config.port,
        #[cfg(not(windows))]
        control_path: control_path_for_connection(config),
    }
}

/// `target` is only consumed by the Unix ControlMaster block.
#[allow(unused_variables)]
fn apply_openssh_options(
    cmd: &mut Command,
    config: &ConnectionConfig,
    target: &OpenSshTarget,
    timeout: Duration,
) -> CoreResult<()> {
    #[cfg(not(windows))]
    crate::ssh_config::prepare_control_directory(&target.control_path)?;
    cmd.arg("-o")
        .arg(crate::local_auth::host_key_mode(config))
        .arg("-o")
        .arg(format!("ConnectTimeout={}", timeout.as_secs().max(1)));
    #[cfg(not(windows))]
    cmd.arg("-o")
        .arg("ControlMaster=auto")
        .arg("-o")
        .arg("ControlPersist=10m")
        .arg("-o")
        .arg(format!("ControlPath={}", target.control_path.display()));
    cmd.arg("-o")
        .arg("ServerAliveInterval=60")
        .arg("-o")
        .arg("ServerAliveCountMax=3")
        .arg("-o")
        .arg(crate::local_auth::batch_mode(config));
    if let AuthMethod::Key { path, .. } = &config.auth {
        cmd.arg("-i").arg(path);
    }
    cmd.args(crate::local_auth::ssh_arguments(config,false)?);
    crate::local_auth::apply(config,cmd,false)?;
    Ok(())
}

/// scp uses `-P` while ssh uses `-p`; callers append the matching flag.
fn append_port_flag(cmd: &mut Command, target: &OpenSshTarget, scp_style: bool) {
    cmd.arg(if scp_style { "-P" } else { "-p" }).arg(target.port.to_string());
}

fn scp_remote_spec(target: &OpenSshTarget, remote_abs: &Path) -> CoreResult<String> {
    let remote_abs = normalize_remote_absolute(remote_abs)?;
    Ok(rsync_remote_spec(target, &remote_abs))
}

fn temp_transfer_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "lrw-{prefix}-{}-{}",
        std::process::id(),
        Uuid::new_v4()
    ))
}

fn upload_directory(output: &str, parent: &str) -> CoreResult<PathBuf> {
    let output = output.trim();
    let base = format!("{}/", parent.trim_end_matches('/'));
    let suffix = output.strip_prefix(&base).unwrap_or("");
    if !suffix.strip_prefix(".lrw-transfer-")
        .is_some_and(|random| random.len() >= 6 && random.bytes().all(|byte| byte.is_ascii_alphanumeric())) {
        return Err(CoreError::InvalidState("Invalid upload staging directory returned by SSH".into()));
    }
    Ok(PathBuf::from(output))
}

fn finalize_upload_command(staging: &Path, target: &str, size: u64) -> CoreResult<String> {
    Ok(format!(
        "set -e\n[ -f {source} ] && [ ! -d {target} ] || exit 26\n[ \"$(wc -c < {source})\" -eq {size} ] || exit 27\nif [ -f {target} ]; then chmod --reference={target} -- {source}; fi\nmv -T -- {source} {target}",
        source=shell_quote(&normalize_remote_absolute(staging)?), target=shell_quote(target)))
}

fn try_system_scp_download(config: &ConnectionConfig, remote_abs: &Path, local: &Path, timeout: Duration) -> CoreResult<()> {
    let target = resolve_openssh_target(config);
    let mut command = crate::openssh::scp_command_for(config);
    apply_openssh_options(&mut command, config, &target, timeout)?;
    append_port_flag(&mut command, &target, true);
    command.arg("--").arg(scp_remote_spec(&target, remote_abs)?).arg(local);
    process::checked(&mut command, timeout, "scp download")?;
    Ok(())
}

fn try_system_scp_upload(config: &ConnectionConfig, remote_abs: &Path, local_path: &Path, timeout: Duration) -> CoreResult<()> {
    let target = resolve_openssh_target(config);
    let mut command = crate::openssh::scp_command_for(&config);
    apply_openssh_options(&mut command, config, &target, timeout)?;
    append_port_flag(&mut command, &target, true);
    command.arg("--").arg(local_path).arg(scp_remote_spec(&target, remote_abs)?);
    process::checked(&mut command, timeout, "scp upload")?;
    Ok(())
}


fn parse_rsync_major(text:&str)->Option<u32> {
    text.lines().find_map(|line|line.split_once("version ").and_then(|(_,tail)|tail.split('.').next()?.trim().parse().ok()))
}
fn ensure_rsync_compatible(timeout:Duration)->CoreResult<()> {
    let output=process::checked(Command::new(rsync_program()).arg("--version"),timeout.min(Duration::from_secs(5)),"rsync version")?;
    let version=String::from_utf8_lossy(&output.stdout);
    if parse_rsync_major(&version).is_some_and(|major|major>=3){Ok(())}else{Err(CoreError::Validation("rsync >= 3 is required for protected arguments. macOS system rsync 2.6.9 is too old; install a modern rsync or use transport=scp/base64".into()))}
}

pub fn local_rsync_report()->serde_json::Value {
    let program=rsync_program();let probe=process::capture(Command::new(&program).arg("--version"),Duration::from_secs(3),"rsync version");
    let version=probe.as_ref().ok().map(|out|String::from_utf8_lossy(&out.stdout).lines().next().unwrap_or("").to_string());
    let compatible=probe.as_ref().is_ok_and(|out|out.status.success()&&parse_rsync_major(&String::from_utf8_lossy(&out.stdout)).is_some_and(|v|v>=3));
    let runtime=rsync_ssh_program();
    let shell=if runtime.1 {PathBuf::from(&program).parent().and_then(rsync_proxy_shell)}else{None};
    serde_json::json!({"program":program.to_string_lossy(),"compatible":compatible,"version":version,"optional":true,"cygwin_ssh":runtime.1,"proxy_shell":shell,"hint":if compatible {"rsync >=3 available; remote rsync and authentication still need verification"}else{"Base MCP works via native SCP; optionally install rsync >=3 or set LRWORKSPACE_RSYNC"}})
}

fn rsync_program() -> std::ffi::OsString {
    if let Some(program) = std::env::var_os("LRWORKSPACE_RSYNC") { return program; }
    // Prefer a native binary over a PATH shim: wrappers may buffer protocol
    // stdin/stdout and deadlock server mode. Do not install or modify tools.
    #[cfg(windows)]
    if let Some(home) = std::env::var_os("USERPROFILE") {
        let native = PathBuf::from(home).join(".local/opt/cwrsync/bin/rsync.exe");
        if native.is_file() { return native.into_os_string(); }
    }
    #[cfg(target_os="macos")]
    for path in ["/opt/homebrew/bin/rsync","/usr/local/bin/rsync"] {if Path::new(path).is_file(){return path.into();}}
    crate::openssh::find_on_path("rsync").map(PathBuf::into_os_string).unwrap_or_else(||"rsync".into())
}

fn posix_windows_path(path: &Path) -> String {
    let value = path.to_string_lossy().replace('\\', "/");
    let value = value.strip_prefix("//?/").unwrap_or(&value);
    if value.as_bytes().get(1) == Some(&b':') {
        format!("/cygdrive/{}{}", value[..1].to_ascii_lowercase(), &value[2..])
    } else { value.to_string() }
}

fn rsync_ssh_program() -> (String, bool) {
    #[cfg(windows)]
    {
        let program = PathBuf::from(rsync_program());
        if let Some(parent) = program.parent() {
            let ssh = parent.join("ssh.exe");
            if parent.join("cygwin1.dll").is_file() && ssh.is_file() {
                return (posix_windows_path(&ssh), true);
            }
        }
    }
    (crate::openssh::ssh_program().to_string_lossy().into_owned(), false)
}

/// Cygwin OpenSSH prepends `exec` to ProxyCommand, so it needs a POSIX
/// shell even when the LRW parent was launched by PowerShell. Scope this to
/// rsync's child environment; never change the user's global shell/config.
#[cfg(any(windows,test))]
fn find_rsync_proxy_shell(install:&Path,git_roots:&[PathBuf])->Option<PathBuf> {
    let local=[install.join("sh.exe"),install.join("bash.exe")];
    local.into_iter().chain(git_roots.iter().flat_map(|root|[root.join("Git/bin/bash.exe"),root.join("Git/usr/bin/bash.exe")])).find(|path|path.is_file())
}

#[cfg(any(windows,test))]
fn rsync_proxy_shell(install:&Path)->Option<PathBuf> {
    std::env::var_os("LRWORKSPACE_RSYNC_SHELL").map(PathBuf::from).or_else(|| {
        let roots:Vec<PathBuf>=["ProgramFiles","ProgramFiles(x86)"].iter().filter_map(std::env::var_os).map(PathBuf::from).collect();find_rsync_proxy_shell(install,&roots)
    })
}
#[cfg(not(any(windows,test)))]
fn rsync_proxy_shell(_install:&Path)->Option<PathBuf> {None}

// rsync -e parses quotes itself; shell backslash escaping is NOT supported.
fn rsync_quote(value: &str) -> String { format!("'{}'", value.replace('\'', "''")) }

fn rsync_remote_spec(target: &OpenSshTarget, path: &str) -> String {
    let host = if target.host.contains(':') && !target.host.starts_with('[') {
        format!("[{}]", target.host)
    } else { target.host.clone() };
    format!("{}@{}:{}", target.user, host, path)
}

/// Use a relative local operand from its own directory: a Windows drive colon
/// otherwise makes rsync interpret BOTH operands as remote hosts.
fn rsync_command(config: &ConnectionConfig, remote: &Path, local: &Path, timeout: Duration, upload: bool) -> CoreResult<Command> {
    rsync_command_limited(config, remote, local, timeout, upload, None)
}

fn rsync_command_limited(config: &ConnectionConfig, remote: &Path, local: &Path, timeout: Duration, upload: bool, limit: Option<u64>) -> CoreResult<Command> {
    let remote = normalize_remote_absolute(remote)?;
    let parent = local.parent().ok_or_else(|| CoreError::Validation("Local file has no parent directory".into()))?;
    let name = local.file_name().ok_or_else(|| CoreError::Validation("Local file has no name".into()))?;
    let target = resolve_openssh_target(config);
    let mut command = Command::new(rsync_program());
    #[cfg(windows)]
    if rsync_ssh_program().1 {
        if let Some(home) = std::env::var_os("USERPROFILE") {
            command.env("HOME", posix_windows_path(Path::new(&home)));
        }
        if let Some(parent) = PathBuf::from(rsync_program()).parent() {
            let shell=rsync_proxy_shell(parent);
            if let Some(shell)=shell {command.env("SHELL",posix_windows_path(&shell));}
            let mut paths = vec![parent.to_path_buf()];
            if let Some(existing) = std::env::var_os("PATH") { paths.extend(std::env::split_paths(&existing)); }
            if let Ok(path) = std::env::join_paths(paths) { command.env("PATH", path); }
        }
    }
    if let Some(limit) = limit { command.arg(format!("--max-size={limit}")); }
    command.current_dir(parent).args(["-tzs", "--blocking-io", "--partial", "--checksum", "--no-perms", "--no-owner", "--no-group"])
        .arg(format!("--timeout={}", timeout.as_secs().max(1)))
        .arg("-e").arg(rsync_ssh_command(config, &target, timeout)?).arg("--");
    crate::local_auth::apply(config,&mut command,rsync_ssh_program().1)?;
    let local_operand = Path::new(".").join(name);
    let remote_operand = rsync_remote_spec(&target, &remote);
    if upload { command.arg(local_operand).arg(remote_operand); }
    else { command.arg(remote_operand).arg(local_operand); }
    Ok(command)
}

fn rsync_ssh_command(
    config: &ConnectionConfig,
    target: &OpenSshTarget,
    timeout: Duration,
) -> CoreResult<String> {
    let (ssh, cygwin) = rsync_ssh_program();
    let config_path=if crate::local_auth::is_local_password(config){None}else{std::env::var_os("LRWORKSPACE_SSH_CONFIG").map(PathBuf::from).or_else(|| {
        if cygwin {std::env::var_os("USERPROFILE").map(|home|PathBuf::from(home).join(".ssh/config")).filter(|p|p.is_file())}else{None}
    })};
    let mut command=rsync_ssh_command_with_config(config,target,timeout,ssh,cygwin,config_path.as_deref());
    for argument in crate::local_auth::ssh_arguments(config,cygwin)? {command.push(' ');command.push_str(&rsync_quote(&argument));}
    Ok(command)
}

fn rsync_ssh_command_with_config(config:&ConnectionConfig,target:&OpenSshTarget,timeout:Duration,ssh:String,cygwin:bool,config_path:Option<&Path>)->String {
    let mut parts = vec![
        ssh,
        "-o".into(),
        crate::local_auth::host_key_mode(config).into(),
        "-o".into(),
        format!("ConnectTimeout={}", timeout.as_secs().max(1)),
        "-o".into(),
        "ServerAliveInterval=60".into(),
        "-o".into(),
        "ServerAliveCountMax=3".into(),
        "-o".into(),
        crate::local_auth::batch_mode(config).into(),
        "-l".into(),
        target.user.clone(),
    ];
    parts.push("-p".into());
    parts.push(target.port.to_string());
    if let AuthMethod::Key { path, .. } = &config.auth {
        parts.push("-i".into());
        parts.push(if cygwin { posix_windows_path(path) } else { path.to_string_lossy().to_string() });
    }
    if let Some(path)=config_path {
        parts.push("-F".into());parts.push(if cygwin {posix_windows_path(path)}else{path.to_string_lossy().into_owned()});
    }
    parts.iter().map(|part| rsync_quote(part)).collect::<Vec<_>>().join(" ")
}

fn try_rsync_download(config: &ConnectionConfig, remote_abs: &Path, local: &Path, timeout: Duration, limit: Option<u64>) -> CoreResult<()> {
    let started = Instant::now();
    ensure_rsync_compatible(timeout)?;
    let budget = timeout.checked_sub(started.elapsed()).filter(|d| !d.is_zero())
        .ok_or_else(|| CoreError::Timeout("rsync version check exhausted transfer budget".into()))?;
    let mut command = rsync_command_limited(config, remote_abs, local, budget, false, limit)?;
    process::checked(&mut command, budget, "rsync download")?;
    Ok(())
}

fn try_rsync_upload(config: &ConnectionConfig, remote_abs: &Path, local_path: &Path, timeout: Duration) -> CoreResult<()> {
    let started=Instant::now();ensure_rsync_compatible(timeout)?;
    let timeout=timeout.checked_sub(started.elapsed()).filter(|d|!d.is_zero()).ok_or_else(||CoreError::Timeout("rsync version check exhausted transfer budget".into()))?;
    let mut command = rsync_command(config, remote_abs, local_path, timeout, true)?;
    process::checked(&mut command, timeout, "rsync upload")?;
    Ok(())
}

async fn upload_via_base64_exec(
    executor: &RemoteExecutorImpl,
    workspace: &Workspace,
    remote_abs: &Path,
    data: &[u8],
) -> CoreResult<()> {
    let upload_cmd = base64_upload_command(remote_abs, data)?;
    let record = executor
        .execute_control(workspace, &upload_cmd, None, &Default::default())
        .await?;
    if record.exit_code != Some(0) {
        return Err(CoreError::Connection(format!(
            "base64 upload failed for {}: {}",
            remote_abs.display(), record.stderr
        )));
    }
    Ok(())
}

fn base64_upload_command(remote_abs: &Path, data: &[u8]) -> CoreResult<String> {
    let remote_full_path = normalize_remote_absolute(remote_abs)?;
    let remote_dir = remote_full_path
        .rsplit_once('/')
        .map(|(parent, _)| if parent.is_empty() { "/" } else { parent })
        .unwrap_or("/");
    let encoded = b64::encode(data);
    // mktemp creates the sibling exclusively. Never truncate a predictable
    // <target>.tmp, which might be a user file or a symlink outside the root.
    let template = format!("{remote_full_path}.lrw-upload-XXXXXXXXXX");
    Ok(format!(
        "set -e\n[ -d {parent} ] || mkdir -p -- {parent}\ntmp=$(mktemp -- {template})\n\
         trap 'rm -f -- \"$tmp\"' EXIT\n\
         trap 'exit 129' HUP\ntrap 'exit 130' INT\ntrap 'exit 143' TERM\n\
         base64 -d > \"$tmp\" <<'ENDOFBASE64'\n{encoded}\nENDOFBASE64\n\
         mv -T -- \"$tmp\" {target}\ntrap - EXIT\n",
        parent = shell_quote(remote_dir),
        template = shell_quote(&template),
        target = shell_quote(&remote_full_path),
    ))
}

fn bounded_read_command(remote_abs: &Path, limit: u64) -> CoreResult<String> {
    let path = normalize_remote_absolute(remote_abs)?;
    let read_bytes = limit.checked_add(1).ok_or_else(|| {
        CoreError::Validation("Remote file read limit is too large".into())
    })?;
    // The extra byte detects growth after stat without transferring the full
    // file. pipefail preserves head/read errors instead of accepting an empty
    // successful base64 result. Non-regular inputs (e.g. FIFOs) are rejected.
    let script = format!(
        "file={}\n[ -f \"$file\" ] || exit 23\n\
         size=$(stat -Lc '%s' -- \"$file\") || exit 24\n\
         [ \"$size\" -le {limit} ] || exit 25\n\
         printf 'LRW_FILE_SIZE:%s\\n' \"$size\"\n\
         head -c {read_bytes} -- \"$file\" | base64",
        shell_quote(&path),
    );
    Ok(format!("bash -o pipefail -c {}", shell_quote(&script)))
}

fn decode_bounded_read(output: &str, limit: u64) -> CoreResult<Vec<u8>> {
    let (header, encoded) = output.split_once('\n').ok_or_else(|| {
        CoreError::InvalidState("Remote file read is missing its size header".into())
    })?;
    let expected = header
        .trim_end_matches('\r')
        .strip_prefix("LRW_FILE_SIZE:")
        .and_then(|size| size.parse::<u64>().ok())
        .ok_or_else(|| CoreError::InvalidState("Invalid remote file size header".into()))?;
    if expected > limit {
        return Err(CoreError::Validation(format!(
            "Remote file exceeds the read limit of {limit} bytes"
        )));
    }
    let bytes = b64::decode(encoded)
        .map_err(|error| CoreError::InvalidState(format!("Corrupt remote file data: {error}")))?;
    if bytes.len() as u64 > limit {
        return Err(CoreError::Validation(format!(
            "Remote file grew beyond the read limit of {limit} bytes"
        )));
    }
    if bytes.len() as u64 != expected {
        return Err(CoreError::InvalidState(
            "Remote file changed size during the read or its output was truncated; retry the read"
                .into(),
        ));
    }
    Ok(bytes)
}

impl RemoteExecutorImpl {
    /// Download directly to a new file in the agent's project; never overwrite an existing path.
    pub async fn download_to_local_file(&self, workspace: &Workspace, root: RemoteRootKind, relative: &Path, local: &Path, preference: TransferPreference) -> CoreResult<DownloadReceipt> {
        if !local.is_absolute() { return Err(CoreError::Validation("local_path must be absolute".into())); }
        if std::fs::symlink_metadata(local).is_ok() { return Err(CoreError::Validation("Download destination already exists; choose a new local_path".into())); }
        let parent = local.parent().ok_or_else(|| CoreError::Validation("Missing destination parent".into()))?;
        let parent = std::fs::canonicalize(parent)?;
        let destination = parent.join(local.file_name().ok_or_else(|| CoreError::Validation("Missing file name".into()))?);
        let temporary = DownloadTemporary::new(&parent)?;
        let mut receipt = self.transfer_download(workspace, root, relative, &temporary.payload(), preference, None).await?;
        // Atomic create without clobbering a file created by another concurrent caller.
        std::fs::hard_link(temporary.payload(), &destination)?;
        receipt.local_path = Some(destination);
        Ok(receipt)
    }

    /// Bounded preview. Binary data is never returned as shell stdout unless explicitly requested.
    pub async fn read_remote_file_with_transport(&self, workspace: &Workspace, root: RemoteRootKind, relative: &Path, preference: TransferPreference) -> CoreResult<(Vec<u8>, DownloadReceipt)> {
        let temporary = DownloadTemporary::new(&std::env::temp_dir())?;
        let receipt = self.transfer_download(workspace, root, relative, &temporary.payload(), preference, Some(workspace.file_threshold_bytes)).await?;
        let path = temporary.payload();
        let limit = workspace.file_threshold_bytes;
        let bytes = tokio::task::spawn_blocking(move || {
            use std::io::Read;
            let file = std::fs::File::open(path)?;
            let mut bytes = Vec::new();
            file.take(limit.saturating_add(1)).read_to_end(&mut bytes)?;
            if bytes.len() as u64 > limit { return Err(CoreError::Validation("File grew beyond preview limit".into())); }
            Ok(bytes)
        }).await.map_err(|e| CoreError::Connection(format!("Preview worker failed: {e}")))??;
        Ok((bytes, receipt))
    }

    /// Compatibility API: bounded by the workspace preview limit; large files should stream to disk.
    pub async fn download_remote_file(&self, workspace: &Workspace, root: RemoteRootKind, relative: &Path) -> CoreResult<(Vec<u8>, TransferMethod)> {
        let (bytes, receipt) = self.read_remote_file_with_transport(workspace, root, relative, TransferPreference::Auto).await?;
        Ok((bytes, receipt.transport))
    }

    async fn transfer_download(&self, workspace: &Workspace, root: RemoteRootKind, relative: &Path, local: &Path, preference: TransferPreference, limit: Option<u64>) -> CoreResult<DownloadReceipt> {
        let started = Instant::now();
        let remaining = || self.timeout().checked_sub(started.elapsed()).filter(|d| !d.is_zero())
            .ok_or_else(|| CoreError::Timeout("File download exceeded its timeout budget".into()));
        let remote = self.with_timeout(remaining()?).validate_mcp_read_target(workspace, root, relative).await?;
        let stat = format!("[ -f {path} ] || exit 23; stat -Lc '%s' -- {path}", path=shell_quote(&normalize_remote_absolute(&remote)?));
        let record = self.with_timeout(remaining()?).execute_control(workspace, &stat, None, &Default::default()).await?;
        if record.exit_code != Some(0) { return Err(CoreError::Validation("Download source must be a readable regular file".into())); }
        let expected = record.stdout.trim().parse::<u64>().map_err(|_| CoreError::InvalidState("Invalid remote file size".into()))?;
        if limit.is_some_and(|limit| expected > limit) { return Err(CoreError::Validation("Remote file exceeds preview limit; use workspace_download_file or rw download".into())); }
        let mut fallbacks = Vec::new();
        for method in [TransferMethod::Rsync, TransferMethod::SystemScp, TransferMethod::Base64Exec] {
            if !matches!((preference, method), (TransferPreference::Auto, TransferMethod::Rsync | TransferMethod::SystemScp) | (TransferPreference::Rsync, TransferMethod::Rsync) | (TransferPreference::Scp, TransferMethod::SystemScp) | (TransferPreference::Base64, TransferMethod::Base64Exec)) { continue; }
            let result = if method == TransferMethod::Base64Exec {
                let cap = limit.unwrap_or(16 * 1024 * 1024).min(16 * 1024 * 1024);
                if expected > cap { return Err(CoreError::Validation("Explicit base64 read is limited to 16 MiB; use rsync/scp".into())); }
                let record = self.with_timeout(remaining()?).execute_control(workspace, &bounded_read_command(&remote, cap)?, None, &Default::default()).await?;
                if record.exit_code != Some(0) || record.output_truncated { Err(CoreError::Connection("Bounded base64 read failed or was truncated".into())) }
                else { std::fs::write(local, decode_bounded_read(&record.stdout, cap)?).map_err(CoreError::from) }
            } else {
                let config = workspace.connection.clone(); let remote = remote.clone(); let local = local.to_path_buf(); let budget = remaining()?;
                tokio::task::spawn_blocking(move || match method {
                    TransferMethod::Rsync => try_rsync_download(&config, &remote, &local, budget, limit),
                    _ => try_system_scp_download(&config, &remote, &local, budget),
                }).await.map_err(|e| CoreError::Connection(format!("Download worker failed: {e}")))?
            };
            match result {
                Ok(()) => {
                    let bytes = std::fs::metadata(local)?.len();
                    if bytes != expected || limit.is_some_and(|limit| bytes > limit) { return Err(CoreError::InvalidState("Remote file changed size during transfer; destination was not created".into())); }
                    return Ok(DownloadReceipt { transport: method, bytes, local_path: None, fallback_reasons: fallbacks });
                }
                Err(error) if preference != TransferPreference::Auto => return Err(error),
                Err(error) => { fallbacks.push(format!("{}: {error}", method.as_str())); }
            }
        }
        Err(CoreError::Connection(format!("Binary download failed; no automatic base64 fallback. {}", fallbacks.join("; "))))
    }

    /// Upload bytes without a local project mirror.
    pub async fn upload_work_file_bytes(&self, workspace: &Workspace, relative_path: &Path, data: &[u8]) -> CoreResult<TransferMethod> {
        self.upload_work_file_bytes_with_transport(workspace, relative_path, data, TransferPreference::Auto).await
    }

    pub async fn upload_work_file_bytes_with_transport(&self, workspace: &Workspace, relative_path: &Path, data: &[u8], preference: TransferPreference) -> CoreResult<TransferMethod> {
        let temporary = LocalTemporary(temp_transfer_path("ul"));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)] { use std::os::unix::fs::OpenOptionsExt; options.mode(0o600); }
        { use std::io::Write; options.open(&temporary.0)?.write_all(data)?; }
        self.upload_local_file(workspace, relative_path, &temporary.0, preference).await
    }

    /// Upload a file directly from the agent's own project. rsync/scp stream
    /// it from disk; no registration, mirror, or whole-file memory copy needed.
    pub async fn upload_local_file(&self, workspace: &Workspace, relative_path: &Path, local_path: &Path, preference: TransferPreference) -> CoreResult<TransferMethod> {
        let started = Instant::now();
        let remaining = || self.timeout().checked_sub(started.elapsed())
            .filter(|budget| !budget.is_zero())
            .ok_or_else(|| CoreError::Timeout("File upload exceeded its timeout budget".into()));
        let local = std::fs::canonicalize(local_path)?;
        let metadata = std::fs::metadata(&local)?;
        if !metadata.is_file() { return Err(CoreError::Validation("Upload source must be a regular file".into())); }
        let target = self.validate_mcp_write_target(workspace, relative_path).await?;
        let target_string = normalize_remote_absolute(&target)?;
        let parent = target_string.rsplit_once('/').map(|(parent, _)| if parent.is_empty() { "/" } else { parent }).unwrap_or("/");
        let template = format!("{}/.lrw-transfer-XXXXXXXXXX", parent.trim_end_matches('/'));
        let preparation = format!("mkdir -p -- {} && mktemp -d -- {}", shell_quote(parent), shell_quote(&template));
        let record = self.with_timeout(remaining()?).execute_control(workspace, &preparation, None, &Default::default()).await?;
        if record.exit_code != Some(0) { return Err(CoreError::Connection(format!("Cannot prepare upload directory: {}", record.stderr))); }
        let directory = record.stdout.trim();
        let directory_path = upload_directory(directory, parent)?;
        let staging = directory_path.join("payload");
        let result: CoreResult<TransferMethod> = async {
            let mut failures = Vec::new();
            let mut transferred = None;
            if matches!(preference, TransferPreference::Auto | TransferPreference::Rsync) {
                let config = workspace.connection.clone(); let path = staging.clone(); let source = local.clone(); let budget = remaining()?;
                let transfer = tokio::task::spawn_blocking(move || try_rsync_upload(&config, &path, &source, budget)).await
                    .map_err(|error| CoreError::Connection(format!("rsync worker failed: {error}")))?;
                match transfer {
                    Ok(()) => transferred = Some(TransferMethod::Rsync),
                    Err(error) if preference == TransferPreference::Rsync => return Err(error),
                    Err(error) => { warn!(error=%error, "rsync failed; trying scp"); failures.push(error.to_string()); }
                }
            }
            if transferred.is_none() && matches!(preference, TransferPreference::Auto | TransferPreference::Scp) {
                let config = workspace.connection.clone(); let path = staging.clone(); let source = local.clone(); let budget = remaining()?;
                let transfer = tokio::task::spawn_blocking(move || try_system_scp_upload(&config, &path, &source, budget)).await
                    .map_err(|error| CoreError::Connection(format!("scp worker failed: {error}")))?;
                match transfer {
                    Ok(()) => transferred = Some(TransferMethod::SystemScp),
                    Err(error) if preference == TransferPreference::Scp => return Err(error),
                    Err(error) => { failures.push(error.to_string()); }
                }
            }
            if transferred.is_none() && preference != TransferPreference::Base64 {
                return Err(CoreError::Connection(format!("Binary upload failed; no automatic base64 fallback. {}", failures.join("; "))));
            }
            if transferred.is_none() {
                // Avoid loading arbitrarily large project files into memory if
                // binary transports are unavailable. Explicit rsync/scp never fall back.
                if metadata.len() > 16 * 1024 * 1024 {
                    return Err(CoreError::Connection(format!("Binary transfer unavailable; base64 fallback is limited to 16 MiB. {}", failures.join("; "))));
                }
                use std::io::Read;
                let mut bytes = Vec::new();
                std::fs::File::open(&local)?.take(16 * 1024 * 1024 + 1).read_to_end(&mut bytes)?;
                if bytes.len() as u64 != metadata.len() || bytes.len() > 16 * 1024 * 1024 {
                    return Err(CoreError::Validation("Upload source changed or exceeded the explicit base64 limit".into()));
                }
                upload_via_base64_exec(&self.with_timeout(remaining()?), workspace, &staging, &bytes).await
                    .map_err(|error| CoreError::Connection(format!("{}; {}", failures.join("; "), error)))?;
                transferred = Some(TransferMethod::Base64Exec);
            }
            let finalize = finalize_upload_command(&staging, &target_string, metadata.len())?;
            let record = self.with_timeout(remaining()?).execute_control(workspace, &finalize, None, &Default::default()).await?;
            if record.exit_code != Some(0) { return Err(CoreError::Connection(format!("Upload finalization or size verification failed (exit {:?}): {}", record.exit_code, record.stderr))); }
            Ok(transferred.expect("transport succeeded"))
        }.await;
        let cleanup = format!("rm -f -- {} ; rmdir -- {}", shell_quote(&normalize_remote_absolute(&staging)?), shell_quote(directory));
        let cleanup_result = self.with_timeout(Duration::from_secs(5)).execute_control(workspace, &cleanup, None, &Default::default()).await;
        if !matches!(cleanup_result, Ok(ref record) if record.exit_code == Some(0)) {
            warn!("Upload staging cleanup failed; no automatic transfer replay");
        }
        result
    }
}

#[async_trait]
impl RemoteFileAccess for RemoteExecutorImpl {
    async fn read_remote_file(
        &self,
        workspace: &Workspace,
        root: RemoteRootKind,
        relative_path: &Path,
    ) -> CoreResult<Vec<u8>> {
        Ok(self.read_remote_file_with_transport(workspace, root, relative_path, TransferPreference::Auto).await?.0)
    }

    async fn write_work_file(
        &self,
        workspace: &Workspace,
        relative_path: &Path,
        content: &[u8],
    ) -> CoreResult<()> {
        self.upload_work_file_bytes(workspace, relative_path, content)
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestDir(PathBuf);

    impl TestDir {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!("lrw-file-test-{}", Uuid::new_v4()));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }

        fn remote_path(&self, name: &str) -> PathBuf {
            let path = self.0.join(name).to_string_lossy().replace('\\', "/");
            #[cfg(windows)]
            let path = format!("/{}{}", path[..1].to_ascii_lowercase(), &path[2..]);
            PathBuf::from(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            if let (Ok(path), Ok(root)) = (
                self.0.canonicalize(),
                std::env::temp_dir().canonicalize(),
            ) {
                if path.starts_with(&root) && path != root {
                    let _ = std::fs::remove_dir_all(path);
                }
            }
        }
    }

    fn local_bash() -> Option<PathBuf> {
        let candidates = [
            PathBuf::from("bash"),
            #[cfg(windows)]
            PathBuf::from(r"C:\Program Files\Git\bin\bash.exe"),
        ];
        for path in candidates {
            if Command::new(&path)
                .args(["-c", "exit 0"])
                .stdout(std::process::Stdio::null())
                .stderr(std::process::Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
            {
                return Some(path);
            }
        }
        eprintln!("Skipping local POSIX integration test: bash is unavailable");
        None
    }


    #[test]
    fn proxy_shell_uses_installed_posix_shell_and_never_powershell() {
        let directory=TestDir::new();let install=directory.0.join("cwrsync");let apps=directory.0.join("Program Files");
        std::fs::create_dir_all(&install).unwrap();std::fs::create_dir_all(apps.join("Git/bin")).unwrap();
        assert!(find_rsync_proxy_shell(&install,&[apps.clone()]).is_none());
        std::fs::write(apps.join("Git/bin/bash.exe"),b"fixture").unwrap();
        assert_eq!(find_rsync_proxy_shell(&install,&[apps.clone()]),Some(apps.join("Git/bin/bash.exe")));
        std::fs::write(install.join("sh.exe"),b"fixture").unwrap();
        assert_eq!(find_rsync_proxy_shell(&install,&[apps]),Some(install.join("sh.exe")));
    }

    #[test]
    fn bounded_payload_checks_exact_size_and_limit() {
        assert_eq!(decode_bounded_read("LRW_FILE_SIZE:0\n", 0).unwrap(), b"");
        assert_eq!(
            decode_bounded_read("LRW_FILE_SIZE:3\nZm9v\n", 3).unwrap(),
            b"foo"
        );
        assert!(decode_bounded_read("LRW_FILE_SIZE:3\nZm8=\n", 3).is_err());
        assert!(decode_bounded_read("LRW_FILE_SIZE:3\nZm9vYg==\n", 3).is_err());
        assert!(decode_bounded_read("LRW_FILE_SIZE:4\nZm9vYg==\n", 3).is_err());
        assert!(decode_bounded_read("Zm9v", 3).is_err());
        assert!(bounded_read_command(Path::new("/tmp/file"), u64::MAX).is_err());
    }

    #[test]
    fn staging_directory_validation_rejects_escape_banners_and_nested_paths() {
        assert_eq!(upload_directory("/work/new folder/.lrw-transfer-abc123XYZ0\n", "/work/new folder").unwrap(), PathBuf::from("/work/new folder/.lrw-transfer-abc123XYZ0"));
        assert!(upload_directory("/work/.lrw-transfer-abc123", "/").is_err());
        assert!(upload_directory("/work-other/.lrw-transfer-abc123", "/work").is_err());
        assert!(upload_directory("/work/.lrw-transfer-abc123/nested", "/work").is_err());
        assert!(upload_directory("banner\n/work/.lrw-transfer-abc123", "/work").is_err());
        assert!(upload_directory("/work/../.lrw-transfer-abc123", "/work").is_err());
    }

    #[test]
    fn finalization_rejects_partial_files_and_preserves_existing_target() {
        let Some(bash) = local_bash() else { return };
        let dir = TestDir::new();
        let staged = dir.remote_path("staged ' file");
        let target = normalize_remote_absolute(&dir.remote_path("target ' file")).unwrap();
        std::fs::write(dir.0.join("staged ' file"), b"short").unwrap();
        std::fs::write(dir.0.join("target ' file"), b"previous").unwrap();
        let command = finalize_upload_command(&staged, &target, 9).unwrap();
        let result = Command::new(&bash).args(["-c", &command]).output().unwrap();
        assert_eq!(result.status.code(), Some(27));
        assert_eq!(std::fs::read(dir.0.join("target ' file")).unwrap(), b"previous");
        let command = finalize_upload_command(&staged, &target, 5).unwrap();
        let result = Command::new(&bash).args(["-c", &command]).output().unwrap();
        assert!(result.status.success(), "{}", String::from_utf8_lossy(&result.stderr));
        assert_eq!(std::fs::read(dir.0.join("target ' file")).unwrap(), b"short");
        assert!(!dir.0.join("staged ' file").exists());
    }

    fn connection() -> ConnectionConfig {
        ConnectionConfig { host: "lrw-local-peer".into(), user: "alice".into(), port: 2222,
            auth: AuthMethod::Agent, jump_hosts: Vec::new() }
    }

    #[test]
    fn rsync_versions_and_explicit_ssh_config_are_portable() {
        assert_eq!(parse_rsync_major("rsync  version 2.6.9  protocol version 29"),Some(2));
        assert_eq!(parse_rsync_major("rsync  version 3.4.1  protocol version 32"),Some(3));
        assert_eq!(parse_rsync_major("not rsync"),None);
        let config=connection();let target=resolve_openssh_target(&config);
        let path=Path::new("C:/user path/ssh config");
        for cygwin in [false,true] {
            let command=rsync_ssh_command_with_config(&config,&target,Duration::from_secs(120),"ssh".into(),cygwin,Some(path));
            assert!(command.contains("'-F'"));
            assert!(command.contains(if cygwin {"'/cygdrive/c/user path/ssh config'"}else{"'C:/user path/ssh config'"}));
        }
    }

    #[test]
    fn rsync_operands_are_relative_locally_and_protected_remotely() {
        let dir = TestDir::new();
        let mut config = connection();
        config.auth = AuthMethod::Key { path: PathBuf::from(r"C:\keys\O'Brien key"), passphrase: None };
        let command = rsync_command(&config, Path::new("/work/space ' 中文.txt"), &dir.0.join("-source file"), Duration::from_secs(240), true).unwrap();
        let args: Vec<_> = command.get_args().map(|arg| arg.to_string_lossy().to_string()).collect();
        assert!(args.iter().any(|arg| arg == "-tzs"));
        assert!(args.iter().any(|arg| arg == "--timeout=240"));
        assert!(!args[args.len()-2].contains(':'));
        assert!(args[args.len()-2].starts_with('.'));
        let remote_shell = &args[args.iter().position(|arg| arg == "-e").unwrap()+1];
        assert!(remote_shell.contains("O''Brien key"));
        assert!(remote_shell.contains("'2222'"));
        config.host = "2001:db8::1".into();
        assert_eq!(rsync_remote_spec(&resolve_openssh_target(&config), "/work/file"), "alice@[2001:db8::1]:/work/file");
    }

    #[test]
    #[ignore = "requires a working local rsync and rustc; no server connection"]
    fn real_rsync_roundtrip_preserves_binary_and_unicode_paths() {
        let dir = TestDir::new();
        #[cfg(unix)]
        let peer = dir.0.join(if cfg!(windows) { "test peer.exe" } else { "test peer" });
        #[cfg(unix)]
        let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/rsync_peer.rs");
        #[cfg(unix)]
        process::checked(Command::new("rustc").arg(source).arg("-o").arg(&peer), Duration::from_secs(30), "compile local rsync peer").unwrap();
        let original = dir.0.join("source ' 中文.bin");
        let remote = dir.0.join("remote ' 中文.bin");
        let downloaded = dir.0.join("download ' 中文.bin");
        let payload: Vec<u8> = (0..1024*1024+17).map(|index| (index % 251) as u8).collect();
        std::fs::write(&original, &payload).unwrap();
        let remote_string = remote.to_string_lossy().replace('\\', "/");
        #[cfg(windows)]
        let remote_string = format!("/cygdrive/{}{}", remote_string[..1].to_ascii_lowercase(), &remote_string[2..]);
        let remote_path = PathBuf::from(remote_string);
        for (local, upload) in [(&original, true), (&downloaded, false)] {
            eprintln!("local-peer transfer upload={upload}");
            let command = rsync_command(&connection(), &remote_path, local, Duration::from_secs(30), upload).unwrap();
            let args: Vec<_> = command.get_args().map(|arg| arg.to_os_string()).collect();
                let mut actual = Command::new(rsync_program());
                actual.current_dir(command.get_current_dir().unwrap()).env("LRW_TEST_RSYNC", rsync_program());
            for (key, value) in command.get_envs() {
                match value { Some(value) => { actual.env(key, value); }, None => { actual.env_remove(key); } }
            }
            let mut remote_shell = false;
            for arg in args {
                if remote_shell {
                    #[cfg(unix)]
                    actual.arg(rsync_quote(&peer.to_string_lossy()));
                    remote_shell = false;
                } else if cfg!(windows) && arg.to_string_lossy().starts_with("alice@lrw-local-peer:") {
                    // Cygwin socket descriptors are not portable through a
                    // Win32 fake-SSH helper. Test real local-mode transfers on
                    // Windows; the Unix variant exercises remote server mode.
                    actual.arg(&remote_path);
                } else {
                    remote_shell = arg == "-e";
                    if !cfg!(windows) || !remote_shell { actual.arg(arg); }
                }
            }
            process::checked(&mut actual, Duration::from_secs(15), "real local-peer rsync").unwrap();
        }
        assert_eq!(std::fs::read(&remote).unwrap(), payload);
        assert_eq!(std::fs::read(&downloaded).unwrap(), payload);
    }

    #[test]
    fn bounded_read_rejects_oversize_and_non_files_before_payload() {
        let Some(bash) = local_bash() else { return };
        let dir = TestDir::new();
        std::fs::write(dir.0.join("payload"), b"12345").unwrap();
        let path = dir.remote_path("payload");
        let exact = Command::new(&bash)
            .args(["-c", &bounded_read_command(&path, 5).unwrap()])
            .output()
            .unwrap();
        assert!(exact.status.success(), "{}", String::from_utf8_lossy(&exact.stderr));
        assert_eq!(
            decode_bounded_read(&String::from_utf8(exact.stdout).unwrap(), 5).unwrap(),
            b"12345"
        );
        let oversized = Command::new(&bash)
            .args(["-c", &bounded_read_command(&path, 4).unwrap()])
            .output()
            .unwrap();
        assert_eq!(oversized.status.code(), Some(25));
        assert!(oversized.stdout.is_empty());
        let directory = Command::new(&bash)
            .args(["-c", &bounded_read_command(&dir.remote_path(""), 4).unwrap()])
            .output()
            .unwrap();
        assert_eq!(directory.status.code(), Some(23));
        assert!(directory.stdout.is_empty());
    }

    #[test]
    fn base64_upload_preserves_existing_tmp_and_replaces_target() {
        let Some(bash) = local_bash() else { return };
        let dir = TestDir::new();
        std::fs::write(dir.0.join("a'b.txt"), b"old").unwrap();
        std::fs::write(dir.0.join("a'b.txt.tmp"), b"user data").unwrap();
        let command = base64_upload_command(&dir.remote_path("a'b.txt"), b"new\0bytes").unwrap();
        let output = Command::new(bash).args(["-c", &command]).output().unwrap();
        assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
        assert_eq!(std::fs::read(dir.0.join("a'b.txt")).unwrap(), b"new\0bytes");
        assert_eq!(std::fs::read(dir.0.join("a'b.txt.tmp")).unwrap(), b"user data");
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 2);
    }

    #[test]
    fn base64_upload_directory_target_fails_and_cleans_temporary_file() {
        let Some(bash) = local_bash() else { return };
        let dir = TestDir::new();
        std::fs::create_dir(dir.0.join("target")).unwrap();
        let command = base64_upload_command(&dir.remote_path("target"), b"payload").unwrap();
        let output = Command::new(bash).args(["-c", &command]).output().unwrap();
        assert!(!output.status.success());
        assert_eq!(std::fs::read_dir(&dir.0).unwrap().count(), 1);
        assert_eq!(std::fs::read_dir(dir.0.join("target")).unwrap().count(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn base64_upload_does_not_follow_predictable_tmp_symlink() {
        let Some(bash) = local_bash() else { return };
        let dir = TestDir::new();
        std::fs::write(dir.0.join("unrelated"), b"untouched").unwrap();
        std::os::unix::fs::symlink(dir.0.join("unrelated"), dir.0.join("target.tmp")).unwrap();
        let command = base64_upload_command(&dir.remote_path("target"), b"payload").unwrap();
        let output = Command::new(bash).args(["-c", &command]).output().unwrap();
        assert!(output.status.success());
        assert_eq!(std::fs::read(dir.0.join("unrelated")).unwrap(), b"untouched");
        assert!(std::fs::symlink_metadata(dir.0.join("target.tmp")).unwrap().is_symlink());
    }
}
