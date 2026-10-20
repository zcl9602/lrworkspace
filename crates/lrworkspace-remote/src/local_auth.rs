//! Process-local password sessions. Model-facing calls never accept a password.
//! Native UI and OpenSSH askpass use separate loopback capability channels.
use lrworkspace_core::privacy::SensitiveString;
use lrworkspace_core::{
    types::{AuthMethod, ConnectionConfig},
    CoreError, CoreResult,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};
mod native;
mod tls;
pub use native::{
    collect_login, ui_available, validate_native_dialog, validate_native_dialog_for_helper,
    LoginForm, LoginInput,
};
pub fn submit_private_form() -> CoreResult<()> {
    native::submit_form()
}

#[derive(Clone, Debug)]
pub struct SessionTarget {
    pub host: String,
    pub user: String,
    pub port: u16,
    pub socks_proxy: Option<String>,
    pub known_hosts: PathBuf,
}
struct Session {
    password: SensitiveString,
    capability: SensitiveString,
    helper: PathBuf,
    target: SessionTarget,
    generation: Uuid,
}
struct Broker {
    address: SocketAddr,
    sessions: Arc<Mutex<HashMap<Uuid, Arc<Session>>>>,
    tls: Arc<tls::Endpoint>,
}
static BROKER: OnceLock<Broker> = OnceLock::new();
static START: Mutex<()> = Mutex::new(());

fn required_login() -> CoreError {
    CoreError::PermissionDenied("Local password session is unavailable. Call workspace_login; never send a password through chat or MCP arguments.".into())
}
fn broker() -> CoreResult<&'static Broker> {
    if let Some(broker) = BROKER.get() {
        return Ok(broker);
    }
    let _lock = START.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(broker) = BROKER.get() {
        return Ok(broker);
    }
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let tls = Arc::new(tls::Endpoint::new()?);
    let private_tls = tls.clone();
    let sessions = Arc::new(Mutex::new(HashMap::<Uuid, Arc<Session>>::new()));
    let shared = sessions.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            // Bounded serial handler: an invalid local peer cannot allocate an
            // unbounded number of authentication threads or request buffers.
            if let Ok(stream) = private_tls.accept(stream) {
                let _ = handle_askpass(stream, &shared);
            }
        }
    });
    let _ = BROKER.set(Broker {
        address,
        sessions,
        tls,
    });
    Ok(BROKER.get().expect("initialized broker"))
}

pub fn create_session(
    password: SensitiveString,
    helper: PathBuf,
    target: SessionTarget,
) -> CoreResult<Uuid> {
    let id = Uuid::new_v4();
    create_session_at(id, password, helper, target)?;
    Ok(id)
}
pub fn create_session_at(
    id: Uuid,
    password: SensitiveString,
    helper: PathBuf,
    target: SessionTarget,
) -> CoreResult<()> {
    if password.is_empty()
        || password.expose().len() > 1024
        || password.expose().contains(['\r', '\n', '\0'])
    {
        return Err(CoreError::Validation(
            "Enter a nonempty single-line password of at most 1024 UTF-8 bytes in the local window"
                .into(),
        ));
    }
    let service = broker()?;
    let mut sessions = service.sessions.lock().unwrap_or_else(|e| e.into_inner());
    if sessions.len() >= 32 {
        return Err(CoreError::InvalidState(
            "Too many local password sessions; log out of an unused workspace".into(),
        ));
    }
    if sessions.contains_key(&id) {
        return Err(CoreError::InvalidState(
            "Log out of the existing local session before replacing it".into(),
        ));
    }
    if !password.retain_for_redaction() {
        return Err(CoreError::InvalidState(
            "Private login redaction capacity reached; restart the MCP connection".into(),
        ));
    }
    let capability = SensitiveString::new(format!(
        "{}{}",
        Uuid::new_v4().simple(),
        Uuid::new_v4().simple()
    ));
    sessions.insert(
        id,
        Arc::new(Session {
            password,
            capability,
            helper,
            target,
            generation: Uuid::new_v4(),
        }),
    );
    Ok(())
}
pub fn session_ready(auth: &AuthMethod) -> bool {
    match auth {
        AuthMethod::LocalPassword { session_id, .. } => BROKER.get().is_some_and(|broker| {
            broker
                .sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains_key(session_id)
        }),
        _ => true,
    }
}
pub fn remove_session(id: Uuid) {
    if let Some(broker) = BROKER.get() {
        broker
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&id);
    }
}
pub fn control_tag(id: Uuid) -> String {
    BROKER
        .get()
        .and_then(|broker| {
            broker
                .sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(&id)
                .map(|value| format!("{}:{}", std::process::id(), value.generation))
        })
        .unwrap_or_else(|| "unavailable".into())
}
fn session(config: &ConnectionConfig) -> CoreResult<Option<Arc<Session>>> {
    let AuthMethod::LocalPassword {
        session_id,
        socks_proxy,
    } = &config.auth
    else {
        return Ok(None);
    };
    let value = BROKER
        .get()
        .and_then(|b| {
            b.sessions
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .get(session_id)
                .cloned()
        })
        .ok_or_else(required_login)?;
    if value.target.host != config.host
        || value.target.port != config.port
        || value.target.user != config.user
        || &value.target.socks_proxy != socks_proxy
    {
        return Err(required_login());
    }
    Ok(Some(value))
}
pub fn session_connection(id: Uuid) -> Option<ConnectionConfig> {
    BROKER.get().and_then(|broker| {
        broker
            .sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&id)
            .map(|value| ConnectionConfig {
                host: value.target.host.clone(),
                user: value.target.user.clone(),
                port: value.target.port,
                auth: AuthMethod::LocalPassword {
                    session_id: id,
                    socks_proxy: value.target.socks_proxy.clone(),
                },
                jump_hosts: vec![],
            })
    })
}
pub fn is_local_password(config: &ConnectionConfig) -> bool {
    matches!(config.auth, AuthMethod::LocalPassword { .. })
}
pub fn check_session(config: &ConnectionConfig) -> CoreResult<()> {
    session(config).map(|_| ())
}
pub fn connection_ready(config: &ConnectionConfig) -> bool {
    check_session(config).is_ok()
}
pub fn batch_mode(config: &ConnectionConfig) -> &'static str {
    if is_local_password(config) {
        "BatchMode=no"
    } else {
        "BatchMode=yes"
    }
}
pub fn host_key_mode(config: &ConnectionConfig) -> &'static str {
    if is_local_password(config) {
        "StrictHostKeyChecking=ask"
    } else {
        "StrictHostKeyChecking=accept-new"
    }
}

fn helper_path(path: &Path, cygwin: bool) -> String {
    #[cfg(windows)]
    let path = path.to_string_lossy().replace('\\', "/");
    #[cfg(not(windows))]
    let path = path.to_string_lossy().into_owned();
    if cygwin && path.as_bytes().get(1) == Some(&b':') {
        format!("/cygdrive/{}{}", path[..1].to_ascii_lowercase(), &path[2..])
    } else {
        path
    }
}
/// Additional SSH arguments precede host operands. -F none excludes config
/// Match/ProxyCommand hooks from this explicitly configured login path.
pub fn ssh_arguments(config: &ConnectionConfig, cygwin: bool) -> CoreResult<Vec<String>> {
    let Some(session) = session(config)? else {
        return Ok(vec![]);
    };
    let known_hosts = helper_path(&session.target.known_hosts, cygwin)
        .replace('\\', "\\\\")
        .replace('"', "\\\"");
    let mut args = vec![
        "-F".into(),
        "none".into(),
        "-o".into(),
        "PreferredAuthentications=password".into(),
        "-o".into(),
        "PubkeyAuthentication=no".into(),
        "-o".into(),
        "KbdInteractiveAuthentication=no".into(),
        "-o".into(),
        "NumberOfPasswordPrompts=1".into(),
        "-o".into(),
        format!("UserKnownHostsFile=\"{known_hosts}\""),
        "-o".into(),
        "GlobalKnownHostsFile=none".into(),
    ];
    if let AuthMethod::LocalPassword {
        socks_proxy: Some(proxy),
        ..
    } = &config.auth
    {
        validate_proxy(proxy)?;
        let helper = helper_path(&session.helper, cygwin);
        #[cfg(windows)]
        let quoted = if cygwin {
            lrworkspace_core::safety::shell_quote(&helper)
        } else {
            format!("\"{}\"", helper.replace('"', ""))
        };
        #[cfg(not(windows))]
        let quoted = lrworkspace_core::safety::shell_quote(&helper);
        #[cfg(windows)]
        let proxy = if cygwin {
            lrworkspace_core::safety::shell_quote(proxy)
        } else {
            format!("\"{proxy}\"")
        };
        #[cfg(not(windows))]
        let proxy = lrworkspace_core::safety::shell_quote(proxy);
        args.extend([
            "-o".into(),
            format!("ProxyCommand={quoted} --auth-proxy {proxy} %h %p"),
        ]);
    }
    Ok(args)
}
pub fn apply(config: &ConnectionConfig, command: &mut Command, cygwin: bool) -> CoreResult<()> {
    let Some(session) = session(config)? else {
        return Ok(());
    };
    let service = broker()?;
    command
        .env("SSH_ASKPASS", helper_path(&session.helper, cygwin))
        .env("SSH_ASKPASS_REQUIRE", "force")
        .env("LRW_ASKPASS_ENDPOINT", service.address.to_string())
        .env("LRW_ASKPASS_CAPABILITY", session.capability.expose())
        .env("LRW_ASKPASS_CERT", service.tls.certificate_b64())
        .env_remove("LRWORKSPACE_SSH_PASSWORD")
        .env_remove("SSLKEYLOGFILE")
        .env_remove("LD_PRELOAD")
        .env_remove("DYLD_INSERT_LIBRARIES");
    // Older compatible OpenSSH builds also need DISPLAY to select askpass.
    command.env("DISPLAY", "lrworkspace-local-auth");
    Ok(())
}
pub fn validate_proxy(value: &str) -> CoreResult<()> {
    let address: SocketAddr = value.parse().map_err(|_| {
        CoreError::Validation("SOCKS5 proxy must be a numeric IP:port (IPv6 in brackets)".into())
    })?;
    if address.port() == 0 {
        return Err(CoreError::Validation("Proxy port must be positive".into()));
    }
    Ok(())
}
#[derive(Deserialize, Serialize)]
struct AskpassRequest {
    capability: String,
    prompt: String,
    confirm: bool,
}
impl Drop for AskpassRequest {
    fn drop(&mut self) {
        self.capability.zeroize();
    }
}
fn constant_time_equal(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b))
        == 0
}
fn handle_askpass(
    mut stream: tls::ServerStream,
    sessions: &Mutex<HashMap<Uuid, Arc<Session>>>,
) -> CoreResult<()> {
    let bytes = tls::read_line(&mut stream, 8192)?;
    let request: AskpassRequest = serde_json::from_slice(&bytes)
        .map_err(|_| CoreError::Validation("Invalid local authentication request".into()))?;
    let found = sessions
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .values()
        .find(|session| {
            constant_time_equal(
                session.capability.expose().as_bytes(),
                request.capability.as_bytes(),
            )
        })
        .cloned();
    #[derive(Serialize)]
    struct Reply<'a> {
        answer: Option<&'a str>,
    }
    let mut answer = None;
    if let Some(session) = &found {
        if request.confirm {
            answer = Some(if native::confirm_host(&request.prompt)? {
                "yes"
            } else {
                "no"
            });
        } else if request.prompt.to_ascii_lowercase().contains("password") {
            answer = Some(session.password.expose());
        }
    }
    stream
        .sock
        .set_write_timeout(Some(Duration::from_secs(3)))?;
    let response = Zeroizing::new(serde_json::to_vec(&Reply { answer })?);
    stream.write_all(&response)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    stream.conn.send_close_notify();
    let _ = stream.flush();
    Ok(())
}

/// Called only by OpenSSH's private askpass child. With no scoped capability
/// it refuses to disclose anything. Its stdout belongs to OpenSSH, not MCP.
pub fn askpass(prompt: &str) -> CoreResult<bool> {
    let endpoint = std::env::var("LRW_ASKPASS_ENDPOINT").map_err(|_| required_login())?;
    let capability =
        Zeroizing::new(std::env::var("LRW_ASKPASS_CAPABILITY").map_err(|_| required_login())?);
    let address: SocketAddr = endpoint.parse().map_err(|_| required_login())?;
    if !address.ip().is_loopback() || prompt.len() > 6000 {
        return Err(required_login());
    }
    let certificate = std::env::var("LRW_ASKPASS_CERT")
        .ok()
        .filter(|value| value.len() < 16384)
        .ok_or_else(required_login)?;
    let certificate = crate::b64::decode(&certificate).map_err(|_| required_login())?;
    let mut stream = tls::connect(
        address,
        rustls::pki_types::CertificateDer::from(certificate),
    )?;
    let confirm = std::env::var("SSH_ASKPASS_PROMPT").is_ok_and(|value| value == "confirm")
        || prompt.contains("yes/no");
    let request = Zeroizing::new(serde_json::to_vec(&AskpassRequest {
        capability: capability.to_string(),
        prompt: prompt.into(),
        confirm,
    })?);
    stream.write_all(&request)?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let bytes = tls::read_line(&mut stream, 8192)?;
    #[derive(Deserialize)]
    struct Reply {
        answer: Option<SensitiveString>,
    }
    let response: Reply = serde_json::from_slice(&bytes).map_err(|_| required_login())?;
    let Some(answer) = response.answer else {
        return Ok(false);
    };
    let mut output = std::io::stdout().lock();
    output.write_all(answer.expose().as_bytes())?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sessions_store_only_opaque_references_and_scope_askpass_environment() {
        let password = SensitiveString::new("auth-broker-fixture-canary-9462".into());
        let target = SessionTarget {
            host: "192.0.2.1".into(),
            user: "fixture".into(),
            port: 2222,
            socks_proxy: None,
            known_hosts: "/test/known_hosts".into(),
        };
        let id =
            create_session(password.clone(), PathBuf::from("/test/mcp-helper"), target).unwrap();
        let config = ConnectionConfig {
            host: "192.0.2.1".into(),
            port: 2222,
            user: "fixture".into(),
            auth: AuthMethod::LocalPassword {
                session_id: id,
                socks_proxy: None,
            },
            jump_hosts: vec![],
        };
        let encoded = serde_json::to_string(&config).unwrap();
        assert!(!encoded.contains(password.expose()));
        assert!(!format!("{config:?}").contains(password.expose()));
        let mut command = Command::new("ssh");
        apply(&config, &mut command, false).unwrap();
        assert!(!command
            .get_envs()
            .any(|(_, value)| value.is_some_and(|value| value == password.expose())));
        assert!(session_ready(&config.auth));
        let mut wrong = config.clone();
        wrong.host = "192.0.2.2".into();
        assert!(apply(&wrong, &mut command, false).is_err());
        remove_session(id);
        assert!(!session_ready(&config.auth));
        assert!(apply(&config, &mut command, false).is_err());
    }
    #[test]
    fn broker_denies_wrong_capabilities_and_unexpected_prompts() {
        let password = SensitiveString::new("auth-broker-second-canary-9462".into());
        let id = create_session(
            password,
            PathBuf::from("/test/mcp-helper"),
            SessionTarget {
                host: "192.0.2.1".into(),
                user: "fixture".into(),
                port: 2222,
                socks_proxy: None,
                known_hosts: "/test/known_hosts".into(),
            },
        )
        .unwrap();
        let service = broker().unwrap();
        let capability = service
            .sessions
            .lock()
            .unwrap()
            .get(&id)
            .unwrap()
            .capability
            .expose()
            .to_string();
        for (capability, prompt) in [
            ("invalid".to_string(), "Password:"),
            (capability, "One-time code:"),
        ] {
            let mut stream = service.tls.connect(service.address).unwrap();
            stream
                .write_all(
                    &serde_json::to_vec(&AskpassRequest {
                        capability,
                        prompt: prompt.into(),
                        confirm: false,
                    })
                    .unwrap(),
                )
                .unwrap();
            stream.write_all(b"\n").unwrap();
            stream.flush().unwrap();
            let reply = tls::read_line(&mut stream, 8192).unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&reply).unwrap()["answer"],
                serde_json::Value::Null
            );
        }
        remove_session(id);
    }
}
