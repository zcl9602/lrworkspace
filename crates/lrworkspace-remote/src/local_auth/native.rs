use super::*;
use lrworkspace_core::safety::normalize_remote_absolute;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::time::Instant;

#[derive(Clone, Serialize)]
pub struct LoginForm {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub name: String,
    pub remote_work_dir: String,
    pub node_mode: String,
    pub socks_proxy: String,
    pub locked: bool,
    pub node_mode_locked: bool,
    callback_url: String,
    callback_certificate: String,
    callback_helper: String,
    parent_pid: u32,
    validate_only: bool,
    confirm_prompt: Option<String>,
}
impl Default for LoginForm {
    fn default() -> Self {
        Self {
            host: String::new(),
            port: 22,
            user: String::new(),
            name: "remote".into(),
            remote_work_dir: String::new(),
            node_mode: "submit_only".into(),
            socks_proxy: String::new(),
            locked: false,
            node_mode_locked: false,
            callback_url: String::new(),
            callback_certificate: String::new(),
            callback_helper: String::new(),
            parent_pid: std::process::id(),
            validate_only: false,
            confirm_prompt: None,
        }
    }
}
#[derive(Debug, Deserialize)]
pub struct LoginInput {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub name: String,
    pub remote_work_dir: String,
    pub node_mode: String,
    #[serde(default)]
    pub socks_proxy: String,
    pub authorized: bool,
    pub password: SensitiveString,
}
impl LoginInput {
    pub fn validate(&self) -> CoreResult<()> {
        if self.host.is_empty()
            || self.host.len() > 253
            || !self
                .host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-_:[]".contains(&b))
            || self.host.starts_with('-')
        {
            return Err(CoreError::Validation(
                "Enter an IP or DNS hostname in the local window".into(),
            ));
        }
        if self.port == 0
            || self.user.is_empty()
            || self.user.len() > 128
            || self.user.starts_with('-')
            || !self
                .user
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        {
            return Err(CoreError::Validation(
                "Enter a valid SSH user and port in the local window".into(),
            ));
        }
        if !self.authorized || !matches!(self.node_mode.as_str(), "submit_only" | "single_server") {
            return Err(CoreError::PermissionDenied(
                "Local access consent and node role are required".into(),
            ));
        }
        if self.name.is_empty() || self.name.len() > 100 || self.name.chars().any(char::is_control)
        {
            return Err(CoreError::Validation("Enter a short workspace name".into()));
        }
        normalize_remote_absolute(Path::new(&self.remote_work_dir))?;
        if !self.socks_proxy.is_empty() {
            validate_proxy(&self.socks_proxy)?;
        }
        if self.password.is_empty()
            || self.password.expose().len() > 1023
            || self.password.expose().contains(['\r', '\n', '\0'])
        {
            return Err(CoreError::Validation(
                "Enter a nonempty single-line password in the local window".into(),
            ));
        }
        Ok(())
    }
}

struct UiChannel {
    url: String,
    receiver: mpsc::Receiver<LoginInput>,
    stop: Arc<AtomicBool>,
    tls: Arc<tls::Endpoint>,
}
impl Drop for UiChannel {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}
fn reply(stream: &mut tls::ServerStream, success: bool) -> std::io::Result<()> {
    stream
        .sock
        .set_write_timeout(Some(Duration::from_secs(3)))?;
    let body = if success {
        "{\"ok\":true}"
    } else {
        "{\"ok\":false}"
    };
    write!(stream,"HTTP/1.1 {}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n{}",if success{"200 OK"}else{"403 Forbidden"},body.len(),body)?;
    stream.flush()?;
    stream.conn.send_close_notify();
    stream.flush()
}
fn receive_form(stream: &mut tls::ServerStream, path: &str) -> CoreResult<LoginInput> {
    let mut reader = std::io::BufReader::new(&mut *stream);
    let mut header = Vec::with_capacity(1024);
    while !header.ends_with(b"\r\n\r\n") {
        if header.len() >= 8192 {
            return Err(CoreError::Validation("Invalid local form request".into()));
        }
        let mut byte = [0];
        reader.read_exact(&mut byte)?;
        header.push(byte[0]);
    }
    let header = std::str::from_utf8(&header)
        .map_err(|_| CoreError::Validation("Invalid local form request".into()))?;
    let mut lines = header.lines();
    if lines.next() != Some(&format!("POST {path} HTTP/1.1")) {
        return Err(CoreError::PermissionDenied(
            "Invalid local form capability".into(),
        ));
    }
    let mut size = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("origin") {
            return Err(CoreError::PermissionDenied(
                "Browser form submissions are not accepted".into(),
            ));
        }
        if name.eq_ignore_ascii_case("content-length") {
            if size.is_some() {
                return Err(CoreError::Validation("Invalid local form request".into()));
            }
            size = Some(
                value
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| CoreError::Validation("Invalid local form request".into()))?,
            );
        }
    }
    let size = size
        .filter(|size| *size <= 16384)
        .ok_or_else(|| CoreError::Validation("Invalid local form size".into()))?;
    let mut bytes = Zeroizing::new(vec![0; size]);
    reader.read_exact(&mut bytes)?;
    serde_json::from_slice(&bytes)
        .map_err(|_| CoreError::Validation("Invalid local form contents".into()))
}
impl UiChannel {
    fn new() -> CoreResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let tls = Arc::new(tls::Endpoint::new()?);
        let encrypted = tls.clone();
        let capability = format!(
            "/login/{}{}",
            Uuid::new_v4().simple(),
            Uuid::new_v4().simple()
        );
        let url = format!("https://{}{capability}", listener.local_addr()?);
        let stop = Arc::new(AtomicBool::new(false));
        let closed = stop.clone();
        let (sender, receiver) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(305);
            while !closed.load(Ordering::Relaxed) && Instant::now() < deadline {
                match listener.accept() {
                    Ok((stream, _)) => {
                        if let Ok(mut stream) = encrypted.accept(stream) {
                            match receive_form(&mut stream, &capability) {
                                Ok(input) => {
                                    let _ = reply(&mut stream, true);
                                    let _ = sender.send(input);
                                    break;
                                }
                                Err(_) => {
                                    let _ = reply(&mut stream, false);
                                }
                            }
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(20))
                    }
                    Err(_) => break,
                }
            }
        });
        Ok(Self {
            url,
            receiver,
            stop,
            tls,
        })
    }
}

pub fn ui_available() -> bool {
    cfg!(any(windows, target_os = "macos"))
}
fn command() -> CoreResult<Command> {
    #[cfg(windows)]
    {
        let program = std::env::var_os("WINDIR")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("C:/Windows"))
            .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let mut command = Command::new(program);
        let bootstrap="[Console]::InputEncoding=New-Object Text.UTF8Encoding($false);[Console]::OutputEncoding=New-Object Text.UTF8Encoding($false);$bundle=[Console]::In.ReadToEnd()|ConvertFrom-Json;& ([ScriptBlock]::Create($bundle.script)) $bundle.form";
        let code = bootstrap
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect::<Vec<_>>();
        command.args([
            "-NoProfile",
            "-NonInteractive",
            "-STA",
            "-EncodedCommand",
            &crate::b64::encode(&code),
        ]);
        return Ok(command);
    }
    #[cfg(target_os = "macos")]
    {
        let mut command = Command::new("/usr/bin/osascript");
        command.args(["-l", "JavaScript", "-e", include_str!("native/macos.js")]);
        return Ok(command);
    }
    #[cfg(not(any(windows,target_os="macos")))]
    Err(CoreError::Validation("The private login window needs a Windows or macOS desktop; use key/agent authentication on headless hosts".into()))
}
fn run_form(form: &LoginForm) -> CoreResult<String> {
    #[cfg(windows)]
    let input = serde_json::to_vec(
        &serde_json::json!({"form":form,"script":include_str!("native/windows.ps1")}),
    )?;
    #[cfg(not(windows))]
    let input = serde_json::to_vec(form)?;
    let budget = Duration::from_secs(if form.confirm_prompt.is_some() {
        60
    } else {
        300
    });
    let mut native = command()?;
    native
        .env_remove("SSLKEYLOGFILE")
        .env_remove("LD_PRELOAD")
        .env_remove("DYLD_INSERT_LIBRARIES");
    let (output, _) = crate::process::capture_input(
        &mut native,
        &input,
        budget,
        8192,
        "local authentication window",
    )
    .map_err(|_| CoreError::Connection("Local authentication window closed or timed out".into()))?;
    if !output.status.success() {
        if form.validate_only {
            return Err(CoreError::Connection(format!(
                "Native dialog self-test failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        return Err(CoreError::Connection(
            "Local authentication window was unavailable or cancelled".into(),
        ));
    }
    // The UI outputs status words only. Credential data uses the separate
    // authenticated callback and is never written to the UI's stdout/stderr.
    let status = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !matches!(
        status.as_str(),
        "submitted" | "cancelled" | "validated" | "yes" | "no"
    ) {
        return Err(CoreError::Connection(
            "Unexpected local window result".into(),
        ));
    }
    Ok(status)
}
pub fn collect_login(mut form: LoginForm) -> CoreResult<Option<LoginInput>> {
    let channel = UiChannel::new()?;
    form.callback_url = channel.url.clone();
    form.callback_certificate = channel.tls.certificate_b64();
    form.callback_helper = std::env::current_exe()?.to_string_lossy().into_owned();
    if run_form(&form)? != "submitted" {
        return Ok(None);
    }
    channel
        .receiver
        .recv_timeout(Duration::from_secs(3))
        .map(Some)
        .map_err(|_| CoreError::Connection("Local credential handoff did not complete".into()))
}
pub(super) fn confirm_host(prompt: &str) -> CoreResult<bool> {
    let form = LoginForm {
        confirm_prompt: Some(prompt.into()),
        ..Default::default()
    };
    Ok(run_form(&form)? == "yes")
}
pub fn validate_native_dialog() -> CoreResult<()> {
    if run_form(&LoginForm {
        validate_only: true,
        ..Default::default()
    })? == "validated"
    {
        Ok(())
    } else {
        Err(CoreError::Validation(
            "Native dialog validation failed".into(),
        ))
    }
}
pub fn validate_native_dialog_for_helper(helper: &Path) -> CoreResult<()> {
    let channel = UiChannel::new()?;
    let form = LoginForm {
        validate_only: true,
        callback_url: channel.url.clone(),
        callback_certificate: channel.tls.certificate_b64(),
        callback_helper: helper.to_string_lossy().into_owned(),
        ..Default::default()
    };
    if run_form(&form)? != "validated" {
        return Err(CoreError::Validation(
            "Native dialog validation failed".into(),
        ));
    }
    channel
        .receiver
        .recv_timeout(Duration::from_secs(3))
        .map(|_| ())
        .map_err(|_| CoreError::Validation("Native pinned TLS handoff failed".into()))
}

/// Private native-UI child: accepts the body on stdin, verifies the ephemeral
/// TLS peer first, then forwards it. Its stdout is a status word only.
pub(super) fn submit_form() -> CoreResult<()> {
    use std::io::IsTerminal;
    if std::io::stdin().is_terminal() {
        return Err(CoreError::PermissionDenied(
            "Private form handoff requires a pipe".into(),
        ));
    }
    let mut bytes = Zeroizing::new(Vec::new());
    std::io::stdin().take(32769).read_to_end(&mut bytes)?;
    if bytes.len() > 32768 {
        return Err(CoreError::Validation(
            "Private form handoff is too large".into(),
        ));
    }
    #[derive(Deserialize)]
    struct Packet {
        url: String,
        certificate: String,
        body: SensitiveString,
    }
    let packet: Packet = serde_json::from_slice(&bytes)
        .map_err(|_| CoreError::Validation("Invalid private handoff".into()))?;
    let payload: LoginInput = serde_json::from_str(packet.body.expose())
        .map_err(|_| CoreError::Validation("Invalid local form".into()))?;
    payload.validate()?;
    let url = packet
        .url
        .strip_prefix("https://")
        .ok_or_else(|| CoreError::Validation("Pinned HTTPS required".into()))?;
    let (address, path) = url
        .split_once('/')
        .ok_or_else(|| CoreError::Validation("Invalid local endpoint".into()))?;
    let address: SocketAddr = address
        .parse()
        .map_err(|_| CoreError::Validation("Numeric loopback endpoint required".into()))?;
    if !address.ip().is_loopback()
        || !path
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"/-".contains(&b))
        || !path.starts_with("login/")
    {
        return Err(CoreError::Validation("Invalid local endpoint".into()));
    }
    let certificate = crate::b64::decode(&packet.certificate)
        .map_err(|_| CoreError::Validation("Invalid local certificate".into()))?;
    let mut stream = tls::connect(
        address,
        rustls::pki_types::CertificateDer::from(certificate),
    )?;
    stream.sock.set_read_timeout(Some(Duration::from_secs(8)))?;
    write!(stream,"POST /{path} HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",packet.body.expose().len())?;
    stream.write_all(packet.body.expose().as_bytes())?;
    stream.flush()?;
    let mut response = Vec::new();
    std::io::Read::take(&mut stream, 4097).read_to_end(&mut response)?;
    if response.len() > 4096 || !response.starts_with(b"HTTP/1.1 200 OK\r\n") {
        return Err(CoreError::Connection(
            "Private form was not accepted".into(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn connection_fields_cannot_reinterpret_transport_operands() {
        let mut form = serde_json::json!({"host":"192.0.2.1","port":2222,"user":"fixture_01","name":"fixture","remote_work_dir":"/work","node_mode":"submit_only","authorized":true,"password":"validation-canary-8431"});
        assert!(serde_json::from_value::<LoginInput>(form.clone())
            .unwrap()
            .validate()
            .is_ok());
        for user in ["-option", "user@other-host", "user:other/path", "user name"] {
            form["user"] = serde_json::json!(user);
            assert!(serde_json::from_value::<LoginInput>(form.clone())
                .unwrap()
                .validate()
                .is_err());
        }
        form["user"] = serde_json::json!("fixture");
        form["password"] = serde_json::json!("x".repeat(1024));
        assert!(serde_json::from_value::<LoginInput>(form)
            .unwrap()
            .validate()
            .is_err());
    }
    #[test]
    fn callback_rejects_browser_origin_and_wrong_capability() {
        let channel = UiChannel::new().unwrap();
        let endpoint = channel
            .url
            .strip_prefix("https://")
            .unwrap()
            .split('/')
            .next()
            .unwrap()
            .parse()
            .unwrap();
        for request in [
            "POST /wrong HTTP/1.1\r\nContent-Length: 0\r\n\r\n".to_string(),
            format!(
                "POST /{} HTTP/1.1\r\nOrigin: https://example.com\r\nContent-Length: 0\r\n\r\n",
                channel.url.splitn(4, '/').nth(3).unwrap()
            ),
        ] {
            let mut stream = channel.tls.connect(endpoint).unwrap();
            stream.write_all(request.as_bytes()).unwrap();
            stream.flush().unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).unwrap();
            assert!(response.starts_with("HTTP/1.1 403"));
        }
    }
    #[test]
    fn credential_handoff_is_private_and_status_reply_contains_no_credentials() {
        let channel = UiChannel::new().unwrap();
        let url = channel.url.strip_prefix("https://").unwrap();
        let (endpoint, path) = url.split_once('/').unwrap();
        let body = r#"{"host":"192.0.2.1","port":2222,"user":"fixture","name":"fixture","remote_work_dir":"/work","node_mode":"submit_only","authorized":true,"password":"local-form-canary-5839"}"#;
        let mut stream = channel.tls.connect(endpoint.parse().unwrap()).unwrap();
        write!(
            stream,
            "POST /{path} HTTP/1.1\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .unwrap();
        stream.flush().unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).unwrap();
        assert!(!response.contains("canary"));
        assert!(response.contains("200 OK"));
        let input = channel
            .receiver
            .recv_timeout(Duration::from_secs(2))
            .unwrap();
        input.validate().unwrap();
        assert!(!format!("{input:?}").contains("local-form-canary"));
    }
    #[test]
    fn native_dialog_validates_without_displaying_or_entering_secrets() {
        if ui_available() {
            validate_native_dialog().unwrap();
        }
    }
}
