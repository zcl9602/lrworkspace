//! Asynchronous local-user authentication. Requests/status contain no credentials.
use lrworkspace_core::traits::RemoteExecutor;
use lrworkspace_core::{types::*, workspace_store, CoreError, CoreResult, LrWorkspaceConfig};
use lrworkspace_remote::{
    local_auth::{self, LoginForm, LoginInput, SessionTarget},
    RemoteExecutorImpl,
};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use uuid::Uuid;

type Input = Arc<dyn Fn(LoginForm) -> CoreResult<Option<LoginInput>> + Send + Sync>;
type Verify = Arc<dyn Fn(&Workspace, &RemoteExecutorImpl) -> CoreResult<()> + Send + Sync>;
struct Request {
    created: Instant,
    value: Value,
    cancelled: Arc<AtomicBool>,
    workspace_id: Option<Uuid>,
    finished: Arc<AtomicBool>,
}
struct FinishRequest(Arc<AtomicBool>);
impl Drop for FinishRequest {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}
#[derive(Default)]
struct State {
    requests: HashMap<Uuid, Request>,
    sessions: HashSet<Uuid>,
}
struct PendingSession<'a> {
    executor: &'a RemoteExecutorImpl,
    id: Uuid,
    committed: bool,
}
impl Drop for PendingSession<'_> {
    fn drop(&mut self) {
        if !self.committed {
            self.executor.close_local_session(self.id);
        }
    }
}
pub(crate) struct LocalLogins {
    state: Arc<Mutex<State>>,
    closed: Arc<AtomicBool>,
    config: LrWorkspaceConfig,
    executor: Arc<RemoteExecutorImpl>,
    input: Input,
    verify: Verify,
    available: bool,
}
fn verify(workspace: &Workspace, executor: &RemoteExecutorImpl) -> CoreResult<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        if !executor.check_connection(workspace).await? {
            return Err(CoreError::Connection("Local authentication failed".into()));
        }
        let report = executor.inspect_workspace_layout(workspace).await?;
        if !report.valid {
            return Err(CoreError::Validation(
                "Remote work/data layout must be valid".into(),
            ));
        }
        Ok(())
    })
}
impl LocalLogins {
    pub fn new(config: LrWorkspaceConfig, executor: Arc<RemoteExecutorImpl>) -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
            closed: Arc::new(AtomicBool::new(false)),
            config,
            executor,
            input: Arc::new(local_auth::collect_login),
            verify: Arc::new(verify),
            available: local_auth::ui_available(),
        }
    }
    pub fn start(&self, existing: Option<Workspace>) -> CoreResult<Value> {
        if !self.available {
            return Err(CoreError::Validation("A local Windows or macOS desktop window is required; use key/agent auth on headless clients".into()));
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.requests.retain(|_, request| {
            !request.finished.load(Ordering::Acquire)
                || request.created.elapsed() < Duration::from_secs(3600)
        });
        if let Some((id, _)) = state
            .requests
            .iter()
            .find(|(_, request)| !request.finished.load(Ordering::Acquire))
        {
            return Ok(
                json!({"request_id":id,"status":"awaiting_user","instruction":"A local login window is already open; the user must complete or cancel it. Never request the password in chat."}),
            );
        }
        if state.requests.len() >= 64 {
            return Err(CoreError::InvalidState(
                "Too many local login requests".into(),
            ));
        }
        let id = Uuid::new_v4();
        let result = json!({"request_id":id,"status":"awaiting_user","credentials":"[REDACTED]","instruction":"Complete the separate local window. Do not inspect, screenshot, or automate credential entry. Poll workspace_login_status for the result."});
        let cancelled = Arc::new(AtomicBool::new(false));
        let finished = Arc::new(AtomicBool::new(false));
        state.requests.insert(
            id,
            Request {
                created: Instant::now(),
                value: result.clone(),
                cancelled: cancelled.clone(),
                workspace_id: existing.as_ref().map(|workspace| workspace.id),
                finished: finished.clone(),
            },
        );
        drop(state);
        let shared = self.state.clone();
        let closed = self.closed.clone();
        let config = self.config.clone();
        let executor = self.executor.clone();
        let input = self.input.clone();
        let verifier = self.verify.clone();
        std::thread::spawn(move || {
            let _finished = FinishRequest(finished);
            let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
                || -> CoreResult<Option<Workspace>> {
                    let mut form = LoginForm::default();
                    if let Some(workspace) = &existing {
                        form.host = workspace.connection.host.clone();
                        form.user = workspace.connection.user.clone();
                        form.port = workspace.connection.port;
                        form.name = workspace.name.clone();
                        form.remote_work_dir =
                            workspace.remote_work_dir.to_string_lossy().into_owned();
                        form.locked = true;
                        form.node_mode_locked =
                            workspace.node_execution_mode != NodeExecutionMode::Unconfirmed;
                        form.node_mode =
                            if workspace.node_execution_mode == NodeExecutionMode::DirectCompute {
                                "single_server"
                            } else {
                                "submit_only"
                            }
                            .into();
                        if let AuthMethod::LocalPassword {
                            socks_proxy: Some(proxy),
                            ..
                        } = &workspace.connection.auth
                        {
                            form.socks_proxy = proxy.clone();
                        }
                    }
                    let Some(data) = input(form)? else {
                        return Ok(None);
                    };
                    data.validate()?;
                    if closed.load(Ordering::Relaxed) || cancelled.load(Ordering::Relaxed) {
                        return Ok(None);
                    }
                    if let Some(workspace) = &existing {
                        if workspace.connection.host != data.host
                            || workspace.connection.user != data.user
                            || workspace.connection.port != data.port
                            || workspace.remote_work_dir.to_string_lossy() != data.remote_work_dir
                            || workspace.name != data.name
                        {
                            return Err(CoreError::Validation(
                                "Re-login cannot change the registered endpoint or work root"
                                    .into(),
                            ));
                        }
                        if workspace.node_execution_mode != NodeExecutionMode::Unconfirmed {
                            let selected = if data.node_mode == "single_server" {
                                NodeExecutionMode::DirectCompute
                            } else {
                                NodeExecutionMode::SubmitOnly
                            };
                            if workspace.node_execution_mode != selected {
                                return Err(CoreError::Validation(
                                    "Re-login cannot change an already confirmed node role".into(),
                                ));
                            }
                        }
                    }
                    let workspace_id = existing
                        .as_ref()
                        .map(|workspace| workspace.id)
                        .unwrap_or_else(Uuid::new_v4);
                    // Stable opaque registration reference, independent in each MCP
                    // process; SSH masters additionally use a per-login generation.
                    let session_id = workspace_id;
                    let proxy = (!data.socks_proxy.is_empty()).then(|| data.socks_proxy.clone());
                    let known_hosts = config.data_dir.join("private-auth/known_hosts");
                    std::fs::create_dir_all(known_hosts.parent().expect("auth directory"))?;
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::PermissionsExt;
                        std::fs::set_permissions(
                            known_hosts.parent().unwrap(),
                            std::fs::Permissions::from_mode(0o700),
                        )?;
                    }
                    {
                        let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
                        if closed.load(Ordering::Relaxed) || cancelled.load(Ordering::Relaxed) {
                            return Ok(None);
                        }
                        executor.close_local_session(session_id);
                        local_auth::create_session_at(
                            session_id,
                            data.password.clone(),
                            std::env::current_exe()?,
                            SessionTarget {
                                host: data.host.clone(),
                                user: data.user.clone(),
                                port: data.port,
                                socks_proxy: proxy.clone(),
                                known_hosts,
                            },
                        )?;
                        state.sessions.insert(session_id);
                    }
                    let mut pending = PendingSession {
                        executor: &executor,
                        id: session_id,
                        committed: false,
                    };
                    let now = chrono::Utc::now();
                    let mut workspace = existing.clone().unwrap_or_else(|| Workspace {
                        id: workspace_id,
                        name: data.name.clone(),
                        display_name: data.name.clone(),
                        connection: ConnectionConfig {
                            host: data.host.clone(),
                            user: data.user.clone(),
                            port: data.port,
                            auth: AuthMethod::Agent,
                            jump_hosts: vec![],
                        },
                        remote_work_dir: data.remote_work_dir.clone().into(),
                        remote_data_dirs: vec![],
                        node_execution_mode: NodeExecutionMode::Unconfirmed,
                        authorized: false,
                        created_at: now,
                        last_accessed_at: now,
                        file_threshold_bytes: config.file_threshold_bytes,
                        config: Default::default(),
                    });
                    workspace.connection.auth = AuthMethod::LocalPassword {
                        session_id,
                        socks_proxy: proxy,
                    };
                    workspace.authorized = true;
                    workspace.node_execution_mode = if data.node_mode == "single_server" {
                        NodeExecutionMode::DirectCompute
                    } else {
                        NodeExecutionMode::SubmitOnly
                    };
                    shared
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .requests
                        .get_mut(&id)
                        .expect("pending request")
                        .value =
                        json!({"request_id":id,"status":"connecting","credentials":"[REDACTED]"});
                    if let Err(error) = verifier(&workspace, &executor) {
                        shared
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .sessions
                            .remove(&session_id);
                        return Err(error);
                    }
                    let mut state = shared.lock().unwrap_or_else(|e| e.into_inner());
                    if closed.load(Ordering::Relaxed) || cancelled.load(Ordering::Relaxed) {
                        state.sessions.remove(&session_id);
                        return Ok(None);
                    }
                    workspace_store::save_workspace(&config, &workspace)?;
                    pending.committed = true;
                    Ok(Some(workspace))
                },
            ));
            let status = match outcome {
                Ok(Ok(Some(workspace))) => {
                    json!({"request_id":id,"status":"authenticated","workspace_id":workspace.id,"credentials":"[REDACTED]","scope":"this MCP process; re-login after restart"})
                }
                Ok(Ok(None)) => json!({"request_id":id,"status":"cancelled"}),
                _ => {
                    json!({"request_id":id,"status":"failed","reason":"local_input_authentication_or_work_directory_check_failed","instruction":"Retry in the local window. Never send the password in chat."})
                }
            };
            if let Some(request) = shared
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .requests
                .get_mut(&id)
            {
                if !request.cancelled.load(Ordering::Relaxed) {
                    request.value = status;
                }
            }
        });
        Ok(result)
    }
    pub fn status(&self, id: Uuid) -> CoreResult<Value> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .requests
            .get(&id)
            .map(|request| request.value.clone())
            .ok_or_else(|| CoreError::NotFound("Unknown local login request".into()))
    }
    pub fn logout(&self, workspace: &Workspace) -> Value {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let AuthMethod::LocalPassword { session_id, .. } = &workspace.connection.auth {
            self.executor.close_local_session(*session_id);
            state.sessions.remove(session_id);
        }
        for request in state.requests.values_mut() {
            if request.workspace_id == Some(workspace.id)
                || request.value["workspace_id"].as_str() == Some(&workspace.id.to_string())
            {
                request.cancelled.store(true, Ordering::Relaxed);
                request.value["status"] = json!("logged_out");
            }
        }
        json!({"status":"logged_out","workspace_id":workspace.id,"registration_retained":true})
    }
    pub fn shutdown(&self) {
        self.closed.store(true, Ordering::Relaxed);
        let sessions = std::mem::take(
            &mut self
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .sessions,
        );
        for id in sessions {
            self.executor.close_local_session(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn local_form_never_puts_credentials_in_status_or_registration_and_logout_revokes() {
        let root = std::env::temp_dir().join(format!("lrw-login-flow-{}", Uuid::new_v4()));
        let config = LrWorkspaceConfig {
            data_dir: root.clone(),
            ..Default::default()
        };
        let mut logins = LocalLogins::new(
            config.clone(),
            Arc::new(RemoteExecutorImpl::new(Duration::from_secs(1))),
        );
        let canary = "native-login-flow-canary-8439";
        logins.available = true;
        logins.input = Arc::new(move |_| {
            Ok(Some(LoginInput {
                host: "192.0.2.1".into(),
                user: "fixture".into(),
                port: 2222,
                name: "private-fixture".into(),
                remote_work_dir: "/work".into(),
                node_mode: "submit_only".into(),
                socks_proxy: String::new(),
                authorized: true,
                password: lrworkspace_core::privacy::SensitiveString::new(canary.into()),
            }))
        });
        logins.verify = Arc::new(|_, _| Ok(()));
        let response = logins.start(None).unwrap();
        let request = Uuid::parse_str(response["request_id"].as_str().unwrap()).unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        let state = loop {
            let state = logins.status(request).unwrap();
            if state["status"] == "authenticated" {
                break state;
            }
            assert!(Instant::now() < end, "{state}");
            std::thread::sleep(Duration::from_millis(10));
        };
        assert!(!state.to_string().contains(canary));
        let workspace = workspace_store::resolve_workspace(&config, None).unwrap();
        assert!(local_auth::session_ready(&workspace.connection.auth));
        let record = std::fs::read_to_string(
            root.join("workspaces")
                .join(format!("{}.json", workspace.id)),
        )
        .unwrap();
        assert!(!record.contains(canary));
        logins.logout(&workspace);
        assert!(!local_auth::session_ready(&workspace.connection.auth));
        assert_eq!(workspace.node_execution_mode, NodeExecutionMode::SubmitOnly);
        // Logout while a re-login verifier is blocked must not resurrect its
        // capability, persist a result, or let stale cleanup revoke a new login.
        let (entered_tx, entered_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        logins.verify = Arc::new(move |_, _| {
            entered_tx.send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            Ok(())
        });
        let response = logins.start(Some(workspace.clone())).unwrap();
        let pending = Uuid::parse_str(response["request_id"].as_str().unwrap()).unwrap();
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        logins.logout(&workspace);
        assert_eq!(
            logins.start(Some(workspace.clone())).unwrap()["request_id"],
            response["request_id"]
        );
        release_tx.send(()).unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        while !logins.state.lock().unwrap().requests[&pending]
            .finished
            .load(Ordering::Acquire)
        {
            assert!(Instant::now() < end);
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(logins.status(pending).unwrap()["status"], "logged_out");
        assert!(!local_auth::session_ready(&workspace.connection.auth));
        // Authentication failure never writes a new registration or exposes
        // the verifier's potentially sensitive diagnostic.
        logins.verify = Arc::new(move |_, _| Err(CoreError::Connection(canary.into())));
        let failed = logins.start(None).unwrap();
        let failed = Uuid::parse_str(failed["request_id"].as_str().unwrap()).unwrap();
        let end = Instant::now() + Duration::from_secs(5);
        while !logins.state.lock().unwrap().requests[&failed]
            .finished
            .load(Ordering::Acquire)
        {
            assert!(Instant::now() < end);
            std::thread::sleep(Duration::from_millis(10));
        }
        let failed = logins.status(failed).unwrap();
        assert_eq!(failed["status"], "failed");
        assert!(!failed.to_string().contains(canary));
        assert_eq!(
            std::fs::read_dir(root.join("workspaces")).unwrap().count(),
            1
        );
        logins.shutdown();
        std::fs::remove_dir_all(root).unwrap();
    }
}
