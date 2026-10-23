//! MCP server for the remote-first LRW architecture.
//!
//! There is no local project mirror and no synchronization API. MCP reads can
//! address the writable work root or configured data roots. MCP writes always
//! address the work root. Commands run with the SSH user's permissions;
//! working_dir is validated, but there is no OS-level filesystem sandbox.

use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use std::collections::VecDeque;
#[cfg(test)]
use std::io::{BufRead,Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

use lrworkspace_audit::AuditLoggerImpl;
use lrworkspace_core::config::LrWorkspaceConfig;
use lrworkspace_core::error::{CoreError, CoreResult};
use lrworkspace_core::permission::{classify_command_risk, DefaultPolicy, RiskLevel};
use lrworkspace_core::safety::normalize_remote_absolute;
use lrworkspace_core::traits::{
    AuditLogger, ClusterDetector, ProjectIndexer, RemoteExecutor, SlurmManager,
};
use lrworkspace_core::types::*;
use lrworkspace_core::workspace_store;
use lrworkspace_remote::{
    resolve_execution_route, ClusterDetectorImpl, ExecutionRoute, ProjectIndexerImpl,
    RemoteExecutorImpl, SshConfig, TransferPreference,
};
use lrworkspace_slurm::{PbsManagerImpl, SlurmManagerImpl};
use lrworkspace_slurm::batch::{submit_batch,tracked_batch_logs};
use lrworkspace_remote::batch::{BatchOptions,BatchCommand,read_command_file,parse_command_file};
use regex::Regex;
use std::fs;
use std::sync::LazyLock;
mod local_login;
mod transport;

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct JsonRpcRequest {
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ToolDefinition {
    name: String,
    description: String,
    #[serde(rename = "inputSchema")]
    input_schema: Value,
}

#[derive(Debug, Serialize)]
struct ServerInfo {
    name: String,
    version: String,
}

#[derive(Debug, Serialize)]
struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    protocol_version: String,
    #[serde(rename = "serverInfo")]
    server_info: ServerInfo,
    capabilities: Value,
    instructions: String,
}

const MCP_VERSION: &str = env!("CARGO_PKG_VERSION");
const AGENT_INSTRUCTIONS: &str = "workspace_discover first. Cluster auto-picks compute; no login workloads. Files read/download/write; no shell base64. No delete.";

fn image_mime(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") { Some("image/png") }
    else if bytes.starts_with(b"\xff\xd8\xff") { Some("image/jpeg") }
    else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") { Some("image/gif") }
    else if bytes.starts_with(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") { Some("image/webp") }
    else { None }
}

fn tool_result(name: &str, value: Value) -> Value {
    let mut safe = lrworkspace_core::privacy::redacted_value(value);
    let mut content = Vec::new();
    if name == "workspace_read_file" && safe["mime_type"].as_str().is_some_and(|mime| matches!(mime, "image/png"|"image/jpeg"|"image/gif"|"image/webp")) {
        if let (Some(data), Some(mime)) = (safe["content"].as_str(), safe["mime_type"].as_str()) {
            if data != lrworkspace_core::privacy::REDACTED {
                content.push(serde_json::json!({"type":"image","mimeType":mime,"data":data}));
            }
        }
        if let Some(metadata) = safe.as_object_mut() { metadata.remove("content"); metadata.remove("encoding"); }
    }
    content.insert(0, serde_json::json!({"type":"text","text":serde_json::to_string_pretty(&safe).unwrap_or_else(|error|error.to_string())}));
    serde_json::json!({"content":content})
}

fn batch_options(args:&serde_json::Map<String,Value>)->CoreResult<BatchOptions> {
    match args.get("batch") {None|Some(Value::Null)=>Ok(BatchOptions::default()),Some(Value::Object(value))=>Ok(serde_json::from_value(Value::Object(value.clone()))?),_=>Err(CoreError::Validation("batch must be an object".into()))}
}

fn has_batch_options(args:&serde_json::Map<String,Value>)->CoreResult<bool> {
    match args.get("batch") {None|Some(Value::Null)=>Ok(false),Some(Value::Object(value))=>Ok(!value.is_empty()),
        _=>Err(CoreError::Validation("batch must be an object".into()))}
}

fn batch_schema()->Value {
    serde_json::json!({"type":["object","null"],"description":"Optional scheduler resources. Omit, null or an empty object keeps automatic compute routing; a nonempty object requests scheduler submission.","additionalProperties":false,"properties":{
        "outdir":{"type":"string","default":"._lrworkspace_jobs","description":"Relative remote output parent; every request creates a fresh run directory"},
        "partition":{"type":"string","description":"Slurm default cpu64,cpu128"},"queue":{"type":"string","description":"PBS queue; omitted uses scheduler default"},
        "nodes":{"type":"integer","minimum":1,"default":1},"ntasks":{"type":"integer","minimum":1,"default":1},"cpu":{"type":"integer","minimum":1,"default":1},
        "maxnum":{"type":"integer","minimum":1,"default":1000},"maxcpu":{"type":"integer","minimum":1,"default":2000},"jobname":{"type":"string","default":"job"},
        "time":{"type":"string"},"mem":{"type":"string"},"shuffle_seed":{"type":"integer","minimum":0},"pbs_flavor":{"type":"string","enum":["pbs-pro","torque"],"description":"Optional. Reuses the saved workspace choice or probes PBS version/usage. An explicit value overrides and is remembered for this endpoint."}}})
}

fn request_timeout(args: &serde_json::Map<String, Value>, default: u64) -> CoreResult<Duration> {
    let seconds = match args.get("timeout_sec") {
        None => default,
        Some(value) => value.as_u64().filter(|value| *value > 0)
            .ok_or_else(|| CoreError::Validation("timeout_sec must be a positive integer".into()))?,
    };
    Ok(Duration::from_secs(seconds))
}

enum UploadSource<'a> { Content(&'a str), LocalPath(&'a str) }

fn upload_source(args: &serde_json::Map<String, Value>) -> CoreResult<UploadSource<'_>> {
    match (args.get("content"), args.get("local_path")) {
        (Some(Value::String(content)), None) => Ok(UploadSource::Content(content)),
        (None, Some(Value::String(path))) if !path.is_empty() => Ok(UploadSource::LocalPath(path)),
        _ => Err(CoreError::Validation("Pass exactly one of content or local_path, as a string".into())),
    }
}

fn log_tail(text: &str, limit: usize) -> String {
    let mut lines = text.lines().rev().take(limit).collect::<Vec<_>>();
    lines.reverse();
    lines.join("\n")
}

fn job_is_terminal(state: &SlurmJobState) -> bool {
    matches!(state, SlurmJobState::Completed | SlurmJobState::Failed
        | SlurmJobState::Cancelled | SlurmJobState::Timeout)
}

// --- Diagnostic tool helpers (integrated from lrw-diagnose) ---

/// Log classification signatures (from lrw-diagnose)
#[derive(Debug, Clone, Serialize)]
struct LogSignature {
    category: String,
    pattern: String,
    description: String,
}

static LOG_SIGNATURES: LazyLock<Vec<LogSignature>> = LazyLock::new(|| {
    vec![
        LogSignature {
            category: "OOM".into(),
            pattern: r"(?i)(out of memory|memory limit exceeded|oom_kill|cannot allocate memory)".into(),
            description: "Out of memory kill".into(),
        },
        LogSignature {
            category: "ModuleMissing".into(),
            pattern: r"(?i)(module.*not found|no module named|import error|modulenotfounderror)".into(),
            description: "Missing Python/environment module".into(),
        },
        LogSignature {
            category: "Walltime".into(),
            pattern: r"(?i)(due to time limit|time limit exceeded|walltime (exceeded|limit)|job killed: walltime)".into(),
            description: "Job exceeded walltime limit".into(),
        },
        LogSignature {
            category: "SignalKill".into(),
            pattern: r"(?i)(signal \d+|sigkill|sigterm|sigsegv|sigbus|segmentation fault|killed)".into(),
            description: "Process killed by signal".into(),
        },
        LogSignature {
            category: "DiskQuota".into(),
            pattern: r"(?i)(disk quota|no space left|quota exceeded|write.*failed.*space)".into(),
            description: "Disk quota or space exhausted".into(),
        },
        LogSignature {
            category: "PathPermission".into(),
            pattern: r"(?i)(permission denied|no such file|not a directory|access denied)".into(),
            description: "Path or permission error".into(),
        },
        LogSignature {
            category: "MPIOrNetwork".into(),
            pattern: r"(?i)(mpi_|rank \d+|connection refused|network unreachable|tcp.*failed)".into(),
            description: "MPI or network communication failure".into(),
        },
    ]
});

fn classify_log_text(text: &str) -> Vec<String> {
    let matched: Vec<&LogSignature> = LOG_SIGNATURES
        .iter()
        .filter(|sig| {
            Regex::new(&sig.pattern)
                .map(|re| re.is_match(text))
                .unwrap_or(false)
        })
        .collect();
    // An OOM kill is delivered as SIGKILL; report it once, as OOM.
    let oom = matched.iter().any(|sig| sig.category == "OOM");
    matched
        .into_iter()
        .filter(|sig| !(oom && sig.category == "SignalKill"))
        .map(|sig| format!("{}: {}", sig.category, sig.description))
        .collect()
}

/// Journal file path for a workspace
fn journal_path(config: &LrWorkspaceConfig, workspace_id: Uuid) -> PathBuf {
    config.data_dir.join("journals").join(format!("{}.jsonl", workspace_id))
}

/// Read journal entries for a workspace
fn read_journal(config: &LrWorkspaceConfig, workspace_id: Uuid, limit: usize) -> CoreResult<Vec<Value>> {
    let path = journal_path(config, workspace_id);
    if !path.exists() {
        return Ok(vec![]);
    }
    if limit==0 {return Ok(vec![]);}
    if limit>1000 {return Err(CoreError::Validation("Journal query limit must be <= 1000".into()));}
    let mut entries=Vec::new();
    let mut file=fs::File::open(path)?;
    lrworkspace_core::jsonl::visit_reverse_lines(&mut file, |line| {
        if let Ok(entry)=serde_json::from_slice(line) {entries.push(entry);}
        entries.len()<limit
    })?;
    Ok(entries)
}

/// Write a journal entry
fn write_journal(config: &LrWorkspaceConfig, workspace_id: Uuid, entry: Value) -> CoreResult<()> {
    let path = journal_path(config, workspace_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::OpenOptions::new().create(true).append(true).open(path)?;
    use std::io::Write;
    writeln!(file, "{}", serde_json::to_string(&lrworkspace_core::privacy::redacted_value(entry))?)?;
    Ok(())
}

pub struct McpServer {
    executor: Arc<RemoteExecutorImpl>,
    cluster_detector: Arc<ClusterDetectorImpl>,
    indexer: Arc<ProjectIndexerImpl>,
    slurm: Arc<SlurmManagerImpl>,
    pbs: Arc<PbsManagerImpl>,
    audit: Arc<AuditLoggerImpl>,
    config: LrWorkspaceConfig,
    agent: std::sync::Mutex<AgentKind>,
    client_supports_elicitation: std::sync::atomic::AtomicBool,
    local_logins: local_login::LocalLogins,
}

impl Drop for McpServer {fn drop(&mut self){self.local_logins.shutdown();}}

impl McpServer {
    pub fn new(config: LrWorkspaceConfig, agent: AgentKind) -> CoreResult<Self> {
        let executor = Arc::new(RemoteExecutorImpl::new(Duration::from_secs(
            config.ssh_timeout_secs,
        )));
        Ok(Self {
            cluster_detector: Arc::new(ClusterDetectorImpl::new(executor.clone())),
            indexer: Arc::new(ProjectIndexerImpl::new(executor.clone(), config.clone())),
            slurm: Arc::new(SlurmManagerImpl::new(executor.clone())),
            pbs: Arc::new(PbsManagerImpl::new(executor.clone())),
            audit: Arc::new(AuditLoggerImpl::new(&config)),
            local_logins: local_login::LocalLogins::new(config.clone(),executor.clone()),
            executor,
            config,
            agent: std::sync::Mutex::new(agent),
            client_supports_elicitation: std::sync::atomic::AtomicBool::new(false),
        })
    }

    pub async fn run(self) -> CoreResult<()> { transport::run(self).await }

    async fn handle_request(&self, request: &JsonRpcRequest) -> JsonRpcResponse {
        match request.method.as_str() {
            "initialize" => self.initialize(request),
            "ping" => success(request, serde_json::json!({})),
            "tools/list" => success(request, serde_json::json!({"tools": tool_definitions()})),
            "tools/call" => self.call_request(request).await,
            _ => failure(
                request,
                -32601,
                format!("Method not found: {}", request.method),
            ),
        }
    }

    fn initialize(&self, request: &JsonRpcRequest) -> JsonRpcResponse {
        self.client_supports_elicitation.store(request
            .params
            .as_ref()
            .and_then(|value| value.get("capabilities"))
            .and_then(|value| value.get("elicitation"))
            .is_some(), std::sync::atomic::Ordering::Relaxed);
        if let Some(name) = request
            .params
            .as_ref()
            .and_then(|value| value.get("clientInfo"))
            .and_then(|value| value.get("name"))
            .and_then(Value::as_str)
        {
            let agent = if name.to_ascii_lowercase().contains("codex") {
                AgentKind::Codex
            } else if name.to_ascii_lowercase().contains("cursor") {
                AgentKind::Cursor
            } else if name.to_ascii_lowercase().contains("claude") {
                AgentKind::ClaudeCode
            } else {
                AgentKind::Other(name.to_string())
            };
            if let Ok(mut current) = self.agent.lock() {
                *current = agent;
            }
        }
        success(
            request,
            serde_json::to_value(InitializeResult {
                protocol_version: if self.client_supports_elicitation.load(std::sync::atomic::Ordering::Relaxed) {
                    "2025-06-18".into()
                } else {
                    "2024-11-05".into()
                },
                server_info: ServerInfo {
                    name: "lrworkspace-mcp".into(),
                    version: MCP_VERSION.into(),
                },
                capabilities: serde_json::json!({"tools": {}}),
                instructions: AGENT_INSTRUCTIONS.into(),
            })
            .expect("initialize result serializes"),
        )
    }

    async fn call_request(&self, request: &JsonRpcRequest) -> JsonRpcResponse {
        let params = request.params.as_ref().and_then(Value::as_object);
        let Some(params) = params else {
            return failure(request, -32602, "Invalid params");
        };
        let name = params.get("name").and_then(Value::as_str).unwrap_or("");
        let args = params
            .get("arguments")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        match self.call_tool(name, &args).await {
            Ok(value) => success(request, tool_result(name, value)),
            Err(error) => JsonRpcResponse {
                jsonrpc: "2.0".into(),
                id: request.id.clone(),
                result: None,
                error: Some(tool_rpc_error(&error)),
            },
        }
    }

    async fn call_tool(
        &self,
        name: &str,
        args: &serde_json::Map<String, Value>,
    ) -> CoreResult<Value> {
        if args.keys().any(|key|matches!(key.to_ascii_lowercase().as_str(),"password"|"passphrase"|"ssh_password")) {
            return Err(CoreError::Validation("Passwords are not accepted in MCP arguments. Use workspace_login and the separate local window.".into()));
        }
        if name=="workspace_login" {
            if args.keys().any(|key|key!="workspace_id"){return Err(CoreError::Validation("workspace_login accepts only an optional workspace_id; enter connection details in the local window".into()));}
            let existing=if args.contains_key("workspace_id"){Some(resolve_workspace(&self.config,args)?)}else{None};
            return self.local_logins.start(existing);
        }
        if name=="workspace_login_status" {
            let id=Uuid::parse_str(required_string(args,"request_id")?).map_err(|_|CoreError::Validation("Invalid local login request ID".into()))?;
            return self.local_logins.status(id);
        }
        if name=="workspace_logout" {let workspace=resolve_workspace(&self.config,args)?;return Ok(self.local_logins.logout(&workspace));}
        if name == "workspace_discover" {
            return self.discover(args).await;
        }
        if name=="workspace_doctor" {
            let workspace=if args.contains_key("workspace_id"){Some(resolve_workspace(&self.config,args)?)}else{None};
            return Ok(lrworkspace_remote::doctor::environment_report(&self.config,workspace.as_ref()));
        }
        if name == "workspace_configure" {
            return self.configure_workspace(args).await;
        }
        if name == "workspace_add_data_dir" {
            return self.add_data_dir(args).await;
        }
        if name == "workspace_diagnose_classify" {
            let log_text = required_string(args, "log_text")?;
            return Ok(serde_json::json!({ "signatures": classify_log_text(log_text) }));
        }
        if name == "workspace_diagnose_list" {
            return self.diagnose_list();
        }
        if name == "workspace_diagnose_journal" {
            return self.diagnose_journal(args);
        }
        let mut workspace = resolve_workspace(&self.config, args)?;
        if name == "workspace_set_node_mode" {
            return self.set_node_mode(&workspace, args);
        }
        if name == "workspace_authorize" {
            if !args
                .get("confirmed")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Ok(serde_json::json!({
                    "status": "preview",
                    "workspace_id": workspace.id,
                    "remote_work_dir": workspace.remote_work_dir,
                    "remote_data_dirs": workspace.remote_data_dirs,
                    "instruction": "Call again with confirmed:true after user review"
                }));
            }
            workspace.authorized = true;
            workspace_store::save_workspace(&self.config, &workspace)?;
            return Ok(serde_json::json!({"status": "authorized", "workspace_id": workspace.id}));
        }
        if !workspace.authorized {
            return Err(CoreError::PermissionDenied(
                "Workspace is not authorized; call workspace_authorize with confirmed:true".into(),
            ));
        }
        let agent = self.agent.lock().unwrap().clone();
        match name {
            "workspace_tasks"=>lrworkspace_remote::tasks::list(&self.config,&workspace,
                args.get("limit").and_then(Value::as_u64).unwrap_or(20) as usize),
            "workspace_task_status"|"workspace_task_logs"=> {
                let id=Uuid::parse_str(required_string(args,"task_id")?).map_err(|_|CoreError::Validation("Invalid task_id".into()))?;
                let bytes=if name=="workspace_task_logs" {Some(args.get("max_bytes").and_then(Value::as_u64).unwrap_or(16384) as usize)}else{None};
                let executor=self.executor.with_timeout(request_timeout(args,self.config.ssh_timeout_secs)?);
                lrworkspace_remote::tasks::poll(&executor,&self.config,&workspace,id,bytes).await
            }
            "workspace_update_connection"=> {
                let port=args.get("port").and_then(Value::as_u64).and_then(|p|u16::try_from(p).ok()).filter(|p|*p>0).ok_or_else(||CoreError::Validation("port must be 1-65535".into()))?;
                if args.get("confirmed").and_then(Value::as_bool)!=Some(true){return Ok(serde_json::json!({"status":"preview","host":workspace.connection.host,"current_port":workspace.connection.port,"requested_port":port,"instruction":"Confirm this endpoint with the user before calling confirmed:true"}));}
                let mut workspace=workspace.clone();let backup=workspace_store::update_connection_port(&self.config,&mut workspace,port)?;
                Ok(serde_json::json!({"status":"updated","workspace_id":workspace.id,"host":workspace.connection.host,"port":port,"backup":backup}))
            }
            "workspace_layout" => {
                let report = self.executor.inspect_workspace_layout(&workspace).await?;
                Ok(serde_json::to_value(report)?)
            }
            "workspace_status" => {
                let mut status = self.executor.check_health(&workspace).await?;
                if status.connected {
                    match self.executor.inspect_workspace_layout(&workspace).await {
                        Ok(report) => {
                            status.layout_valid = report.valid;
                            status.layout_error = (!report.valid).then(|| report.errors.join("; "));
                        }
                        Err(error) => status.layout_error = Some(error.to_string()),
                    }
                }
                workspace_store::write_status_file(&self.config, workspace.id, &status)?;
                Ok(serde_json::to_value(status)?)
            }
            "workspace_env" => {
                use lrworkspace_core::traits::RemoteExecutor;
                let snapshot = self.executor.capture_env(&workspace).await?;
                workspace_store::save_env_snapshot(&self.config, &snapshot)?;
                Ok(serde_json::to_value(snapshot)?)
            }
            "workspace_index" => {
                let root = parse_root(args)?;
                Ok(serde_json::to_value(
                    self.indexer.index_root(&workspace, root).await?,
                )?)
            }
            "workspace_search_files" => {
                let pattern = required_string(args, "pattern")?;
                let root = parse_root(args)?;
                Ok(serde_json::to_value(
                    self.indexer.search_files(&workspace, root, pattern).await?,
                )?)
            }
            "workspace_read_file" => {
                let root = parse_root(args)?;
                let relative = Path::new(required_string(args, "relative_path")?);
                let mut workspace = workspace.clone();
                workspace.file_threshold_bytes = workspace.file_threshold_bytes
                    .min(self.config.file_threshold_bytes);
                let executor = self.executor.with_timeout(request_timeout(args, self.config.ssh_timeout_secs)?);
                let preference = TransferPreference::parse(optional_string(args, "transport").unwrap_or("auto"))?;
                let (bytes, receipt) = executor.read_remote_file_with_transport(&workspace, root, relative, preference).await?;
                let mut result = serde_json::json!({"root":root_label(root), "relative_path":relative,
                    "transport":receipt.transport.as_str(), "bytes":receipt.bytes, "fallback_reasons":receipt.fallback_reasons});
                if let Some(mime) = image_mime(&bytes) {
                    result["mime_type"] = mime.into();
                    result["encoding"] = "base64".into();
                    result["content"] = lrworkspace_remote::b64::encode(&bytes).into();
                } else {
                    let text = String::from_utf8(bytes).map_err(|_| CoreError::Validation("Binary file: use workspace_download_file with an absolute local_path; do not run shell base64".into()))?;
                    result["encoding"] = "utf-8".into(); result["content"] = text.into();
                }
                Ok(result)
            }
            "workspace_download_file" => {
                let root = parse_root(args)?;
                let relative = Path::new(required_string(args, "relative_path")?);
                let local = Path::new(required_string(args, "local_path")?);
                let preference = TransferPreference::parse(optional_string(args, "transport").unwrap_or("auto"))?;
                if preference == TransferPreference::Base64 { return Err(CoreError::Validation("Full downloads use auto, rsync or scp".into())); }
                let executor = self.executor.with_timeout(request_timeout(args, self.config.ssh_timeout_secs)?);
                Ok(serde_json::to_value(executor.download_to_local_file(&workspace, root, relative, local, preference).await?)?)
            }
            "workspace_write_file" => {
                if args
                    .get("root")
                    .and_then(Value::as_str)
                    .is_some_and(|root| root != "work")
                {
                    return Err(CoreError::PermissionDenied(
                        "MCP writes are allowed only with root=work".into(),
                    ));
                }
                let relative = Path::new(required_string(args, "relative_path")?);
                let executor = self.executor.with_timeout(request_timeout(args, self.config.ssh_timeout_secs)?);
                let preference = TransferPreference::parse(optional_string(args, "transport").unwrap_or("auto"))?;
                let (method, bytes) = match upload_source(args)? {
                    UploadSource::Content(content) => (executor.upload_work_file_bytes_with_transport(&workspace, relative, content.as_bytes(), preference).await?, content.len() as u64),
                    UploadSource::LocalPath(local) => {
                        let bytes = fs::metadata(local)?.len();
                        (executor.upload_local_file(&workspace, relative, Path::new(local), preference).await?, bytes)
                    }
                };
                self.audit
                    .record(
                        workspace.id,
                        &agent,
                        "write_work_file",
                        RiskLevel::WorkspaceWrite,
                        &relative.display().to_string(),
                        "written directly to remote_work_dir",
                    )
                    .await?;
                Ok(serde_json::json!({
                    "status": "written",
                    "root": "work",
                    "relative_path": relative,
                    "bytes": bytes,
                    "transport": method.as_str(),
                    "fallback": preference == TransferPreference::Auto && method != lrworkspace_remote::TransferMethod::Rsync,
                }))
            }
            "workspace_submit" => {
                let commands=match (args.get("local_path"),args.get("commands_text")) {
                    (Some(Value::String(path)),None)=>read_command_file(Path::new(path))?,
                    (None,Some(Value::String(text)))=>parse_command_file(text)?,
                    _=>return Err(CoreError::Validation("Pass exactly one of local_path or commands_text".into())),
                };
                let options=batch_options(args)?;
                let submit=match args.get("submit") {None=>false,Some(Value::Bool(value))=>*value,_=>return Err(CoreError::Validation("submit must be a boolean".into()))};
                let executor=Arc::new(self.executor.with_timeout(request_timeout(args,self.config.ssh_timeout_secs)?));
                let cluster=self.load_or_discover_cluster(&workspace).await?;
                let working_dir=optional_string(args,"working_dir").map(Path::new);
                let mut result=submit_batch(executor,&self.config,&workspace,&cluster,&commands,&options,working_dir,submit,&agent).await?;
                if let Err(error)=self.audit.record(workspace.id,&agent,"mcp_batch_submit",RiskLevel::RemoteExecute,&result.batch.run_directory,&result.status).await {
                    result.warning=Some(format!("Operation completed; audit failed: {error}. {:?}",result.warning));
                }
                Ok(serde_json::to_value(result)?)
            }
            "workspace_run" => self.run_command(&workspace, args, &agent).await,
            "workspace_cluster_info" => {
                let refresh=match args.get("refresh") {
                    None=>false,Some(Value::Bool(value))=>*value,
                    _=>return Err(CoreError::Validation("refresh must be a boolean".into())),
                };
                let previous=workspace_store::load_cluster_config(&self.config,workspace.id).ok();
                let cluster=if !refresh && previous.is_some() { previous.unwrap() } else {
                    let mut cluster=self.cluster_detector.discover_cluster(&workspace).await?;
                    if let Some(previous)=previous {cluster.constraints=previous.constraints;}
                    workspace_store::save_cluster_config(&self.config,&cluster,workspace.id)?;
                    cluster
                };
                Ok(serde_json::to_value(cluster)?)
            }
            "workspace_slurm_status" => {
                let job_id = required_string(args, "job_id")?;
                let cluster = self.load_or_discover_cluster(&workspace).await?;
                let state = match cluster.scheduler {
                    ClusterScheduler::Slurm => self.slurm.status(&workspace, job_id).await?,
                    ClusterScheduler::Pbs => self.pbs.status(&workspace, job_id).await?,
                    other => {
                        return Err(CoreError::InvalidState(format!(
                            "job status is implemented only for Slurm and PBS, got {:?}",
                            other
                        )))
                    }
                };
                Ok(serde_json::to_value(state)?)
            }
            "workspace_slurm_logs" => {
                let job_id = required_string(args, "job_id")?;
                let cluster = self.load_or_discover_cluster(&workspace).await?;
                let (stdout, stderr) = match cluster.scheduler {
                    ClusterScheduler::Slurm => self.job_logs(&workspace,job_id,ClusterScheduler::Slurm).await?,
                    ClusterScheduler::Pbs => self.job_logs(&workspace,job_id,ClusterScheduler::Pbs).await?,
                    other => {
                        return Err(CoreError::InvalidState(format!(
                            "job logs are implemented only for Slurm and PBS, got {:?}",
                            other
                        )))
                    }
                };
                Ok(serde_json::json!({"stdout": stdout, "stderr": stderr}))
            }
            "workspace_slurm_cancel" => {
                let job_id = required_string(args, "job_id")?;
                let cluster = self.load_or_discover_cluster(&workspace).await?;
                match cluster.scheduler {
                    ClusterScheduler::Slurm => self.slurm.cancel(&workspace, job_id).await?,
                    ClusterScheduler::Pbs => self.pbs.cancel(&workspace, job_id).await?,
                    other => {
                        return Err(CoreError::InvalidState(format!(
                            "job cancel is implemented only for Slurm and PBS, got {:?}",
                            other
                        )))
                    }
                }
                Ok(serde_json::json!({"status": "cancelled", "job_id": job_id}))
            }
            "workspace_audit" => {
                let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize;
                Ok(serde_json::to_value(
                    self.audit.query(workspace.id, limit, 0).await?,
                )?)
            }
            // --- Diagnostic tools (integrated from lrw-diagnose) ---
            "workspace_diagnose_job" => {
                let job_id = required_string(args, "job_id")?;
                let cluster = self.load_or_discover_cluster(&workspace).await?;
                let (state, stdout, stderr) = match cluster.scheduler {
                    ClusterScheduler::Slurm => {
                        let state = self.slurm.status(&workspace, job_id).await?;
                        let (stdout, stderr) = self.job_logs(&workspace,job_id,ClusterScheduler::Slurm).await?;
                        (state, stdout, stderr)
                    }
                    ClusterScheduler::Pbs => {
                        let state = self.pbs.status(&workspace, job_id).await?;
                        let (stdout, stderr) = self.job_logs(&workspace,job_id,ClusterScheduler::Pbs).await?;
                        (state, stdout, stderr)
                    }
                    other => {
                        return Err(CoreError::InvalidState(format!(
                            "job diagnosis is implemented only for Slurm and PBS, got {:?}",
                            other
                        )))
                    }
                };
                let log_text = format!("{}\n{}", stdout, stderr);
                let signatures = classify_log_text(&log_text);
                self.journal_diagnosis(&workspace, job_id, &state, &signatures)?;
                Ok(serde_json::json!({
                    "job_id": job_id,
                    "state": state,
                    "signatures": signatures,
                    "stdout_tail": log_tail(&stdout, 50),
                    "stderr_tail": log_tail(&stderr, 50),
                }))
            }
            "workspace_diagnose_wait" => {
                let job_id = required_string(args, "job_id")?;
                // Bounded on purpose: this server handles one request at a time,
                // so a long poll would stall every other tool call.
                let timeout_sec = request_timeout(args, self.config.ssh_timeout_secs)?.as_secs();
                let interval_sec = args.get("interval_sec").and_then(Value::as_u64).unwrap_or(15).clamp(5, 60);
                let cluster = self.load_or_discover_cluster(&workspace).await?;

                let start = std::time::Instant::now();
                loop {
                    let state = match cluster.scheduler {
                        ClusterScheduler::Slurm => self.slurm.status(&workspace, job_id).await?,
                        ClusterScheduler::Pbs => self.pbs.status(&workspace, job_id).await?,
                        other => {
                            return Err(CoreError::InvalidState(format!(
                                "job wait is implemented only for Slurm and PBS, got {:?}",
                                other
                            )))
                        }
                    };
                    if matches!(state, SlurmJobState::Unknown(_)) {
                        return Ok(serde_json::json!({
                            "job_id": job_id,
                            "status": "unknown",
                            "state": state,
                            "waited_seconds": start.elapsed().as_secs(),
                            "hint": "Scheduler evidence is unavailable or unrecognized; completion is not confirmed",
                        }));
                    }
                    if job_is_terminal(&state) {
                        let (stdout, stderr) = match cluster.scheduler {
                            ClusterScheduler::Slurm => self.job_logs(&workspace,job_id,ClusterScheduler::Slurm).await?,
                            _ => self.job_logs(&workspace,job_id,ClusterScheduler::Pbs).await?,
                        };
                        let log_text = format!("{}\n{}", stdout, stderr);
                        let signatures = classify_log_text(&log_text);
                        self.journal_diagnosis(&workspace, job_id, &state, &signatures)?;
                        return Ok(serde_json::json!({
                            "job_id": job_id,
                            "final_state": state,
                            "signatures": signatures,
                            "waited_seconds": start.elapsed().as_secs(),
                            "stdout_tail": log_tail(&stdout, 50),
                            "stderr_tail": log_tail(&stderr, 50),
                        }));
                    }
                    if start.elapsed().as_secs() >= timeout_sec {
                        return Ok(serde_json::json!({
                            "job_id": job_id,
                            "status": "still_running",
                            "state": state,
                            "waited_seconds": start.elapsed().as_secs(),
                            "hint": "call workspace_diagnose_wait again to keep polling",
                        }));
                    }
                    let remaining = Duration::from_secs(timeout_sec).saturating_sub(start.elapsed());
                    tokio::time::sleep(Duration::from_secs(interval_sec).min(remaining)).await;
                }
            }
            "workspace_diagnose_topo" => {
                let partition = required_string(args, "partition")?;
                let cluster = self.load_or_discover_cluster(&workspace).await?;
                if cluster.scheduler != ClusterScheduler::Slurm && cluster.scheduler != ClusterScheduler::Pbs {
                    return Err(CoreError::InvalidState("Topology diagnosis requires Slurm or PBS".into()));
                }
                let nodes: Vec<_> = cluster.nodes
                    .iter()
                    .filter(|n| n.partitions.iter().any(|p| p == partition))
                    .collect();
                if nodes.is_empty() {
                    return Err(CoreError::NotFound(format!("No nodes found for partition {}", partition)));
                }
                let total_cores: u32 = nodes.iter().map(|n| n.cpu_cores).sum();
                let total_mem_mb: u64 = nodes.iter().map(|n| n.memory_mb).sum();
                let gpu_nodes: Vec<_> = nodes.iter().filter(|n| n.gpu_count > 0).collect();
                let gpu_models: Vec<String> = gpu_nodes.iter().filter_map(|n| n.gpu_model.clone()).collect();
                Ok(serde_json::json!({
                    "partition": partition,
                    "scheduler": cluster.scheduler,
                    "node_count": nodes.len(),
                    "total_cores": total_cores,
                    "total_memory_mb": total_mem_mb,
                    "avg_cores_per_node": if nodes.is_empty() { 0 } else { total_cores / nodes.len() as u32 },
                    "avg_memory_mb_per_node": if nodes.is_empty() { 0 } else { total_mem_mb / nodes.len() as u64 },
                    "gpu_nodes": gpu_nodes.len(),
                    "gpu_models": gpu_models,
                    "sample_nodes": nodes.iter().take(5).map(|n| serde_json::json!({
                        "hostname": n.hostname,
                        "cores": n.cpu_cores,
                        "memory_mb": n.memory_mb,
                        "gpu_count": n.gpu_count,
                        "gpu_model": n.gpu_model,
                        "state": n.state,
                    })).collect::<Vec<_>>(),
                }))
            }
            "workspace_diagnose_node" => {
                let node = required_string(args, "node")?;
                let cluster = self.load_or_discover_cluster(&workspace).await?;
                let node_info = cluster.nodes.iter().find(|n| n.hostname == node)
                    .ok_or_else(|| CoreError::NotFound(format!("Node {} not found", node)))?;
                Ok(serde_json::json!({
                    "hostname": node_info.hostname,
                    "cpu_cores": node_info.cpu_cores,
                    "memory_mb": node_info.memory_mb,
                    "gpu_count": node_info.gpu_count,
                    "gpu_model": node_info.gpu_model,
                    "state": node_info.state,
                    "partitions": node_info.partitions,
                    "arch": node_info.arch,
                }))
            }
            "workspace_diagnose_suggest" => {
                let partition = required_string(args, "partition")?;
                let workload = args.get("workload").and_then(Value::as_str).unwrap_or("generic");
                let cluster = self.load_or_discover_cluster(&workspace).await?;
                let nodes: Vec<_> = cluster.nodes
                    .iter()
                    .filter(|n| n.partitions.iter().any(|p| p == partition))
                    .collect();
                if nodes.is_empty() {
                    return Err(CoreError::NotFound(format!("No nodes found for partition {}", partition)));
                }
                let cores_per_node = nodes.iter().map(|node| node.cpu_cores).min().unwrap_or(0);
                let mem_per_node_mb = nodes.iter().map(|node| node.memory_mb).min().unwrap_or(0);
                if cores_per_node == 0 || mem_per_node_mb == 0 {
                    return Err(CoreError::InvalidState("Discovered CPU/memory metadata is incomplete; refresh topology before requesting recommendations".into()));
                }
                let (threads_per_task, mem_per_core_mb) = match workload {
                    "aligner" | "assembler" => (cores_per_node.min(32), (mem_per_node_mb / cores_per_node as u64).min(8000)),
                    "variant" => (cores_per_node.min(16), (mem_per_node_mb / cores_per_node as u64).min(4000)),
                    "python" | "r" => (cores_per_node.min(8), (mem_per_node_mb / cores_per_node as u64).min(2000)),
                    "mpi" => (cores_per_node, (mem_per_node_mb / cores_per_node as u64).min(4000)),
                    _ => (cores_per_node.min(8), (mem_per_node_mb / cores_per_node as u64).min(2000)),
                };
                let known_cores = nodes.iter().fold(0u32, |sum,node| sum.saturating_add(node.cpu_cores));
                let max_cores = cluster.constraints.max_total_cores.unwrap_or(known_cores).min(known_cores);
                let max_jobs = cluster.constraints.max_total_jobs.unwrap_or(max_cores / threads_per_task.max(1));
                Ok(serde_json::json!({
                    "partition": partition,
                    "workload": workload,
                    "recommended": {
                        "threads_per_task": threads_per_task,
                        "memory_per_core_mb": mem_per_core_mb,
                        "max_concurrent_jobs": max_jobs.min(max_cores / threads_per_task.max(1)),
                        "sbatch_flags": format!("--cpus-per-task={} --mem={}M", threads_per_task, (threads_per_task as u64) * mem_per_core_mb),
                    },
                    "recommendation_basis": "Heuristic from discovered topology and optional user constraints; not a scheduler allocation or site-policy limit",
                    "partition_limits": {
                        "nodes_available": nodes.iter().filter(|node| node.state == "idle").count(),
                        "nodes_discovered": nodes.len(),
                        "cores_per_node": cores_per_node,
                        "memory_per_node_mb": mem_per_node_mb,
                    },
                }))
            }
            _ => Err(CoreError::NotFound(format!("Unknown MCP tool: {name}"))),
        }
    }

    /// Workspace list from local metadata only: listing must not open SSH sessions.
    fn diagnose_list(&self) -> CoreResult<Value> {
        let mut values = Vec::new();
        for ws in workspace_store::list_workspaces(&self.config)? {
            let scheduler = workspace_store::load_cluster_config(&self.config, ws.id)
                .ok()
                .map(|cluster| cluster.scheduler);
            values.push(serde_json::json!({
                "workspace_id": ws.id,
                "name": ws.name,
                "authorized": ws.authorized,
                "remote_host": ws.connection.host,
                "remote_user": ws.connection.user,
                "remote_work_dir": ws.remote_work_dir,
                "node_execution_mode": ws.node_execution_mode,
                "scheduler": scheduler,
            }));
        }
        Ok(serde_json::json!({ "workspaces": values }))
    }

    fn diagnose_journal(&self, args: &serde_json::Map<String, Value>) -> CoreResult<Value> {
        let limit = args.get("limit").and_then(Value::as_u64).unwrap_or(50) as usize;
        let entries = match optional_string(args, "workspace_id") {
            Some(id) => {
                let id = Uuid::parse_str(id)
                    .map_err(|error| CoreError::Validation(format!("Invalid workspace UUID: {error}")))?;
                read_journal(&self.config, id, limit)?
            }
            None => {
                let mut all = Vec::new();
                for ws in workspace_store::list_workspaces(&self.config)? {
                    all.extend(read_journal(&self.config, ws.id, limit)?);
                }
                let timestamp = |v: &Value| v.get("timestamp").and_then(Value::as_str).unwrap_or("").to_string();
                all.sort_by(|a, b| timestamp(b).cmp(&timestamp(a)));
                all.truncate(limit);
                all
            }
        };
        Ok(serde_json::json!({ "entries": entries }))
    }

    fn journal_diagnosis(
        &self,
        workspace: &Workspace,
        job_id: &str,
        state: &SlurmJobState,
        signatures: &[String],
    ) -> CoreResult<()> {
        write_journal(
            &self.config,
            workspace.id,
            serde_json::json!({
                "timestamp": chrono::Utc::now().to_rfc3339(),
                "job_id": job_id,
                "state": state,
                "signatures": signatures,
            }),
        )
    }

    async fn configure_workspace(
        &self,
        args: &serde_json::Map<String, Value>,
    ) -> CoreResult<Value> {
        let host = optional_string(args, "host");
        let remote_work_dir = optional_string(args, "remote_work_dir");
        if host.is_none() || remote_work_dir.is_none() {
            let mut missing = Vec::new();
            if host.is_none() {
                missing.push("host");
            }
            if remote_work_dir.is_none() {
                missing.push("remote_work_dir");
            }
            return Ok(input_required(&missing));
        }
        let host = host.expect("checked above");
        let remote_work_dir = remote_work_dir.expect("checked above");
        let remote_work_dir = PathBuf::from(normalize_remote_absolute(Path::new(remote_work_dir))?);
        let port=match args.get("port") {
            None=>None,
            Some(value)=>Some(value.as_u64().and_then(|p|u16::try_from(p).ok()).filter(|p|*p>0).ok_or_else(||CoreError::Validation("port must be an integer between 1 and 65535".into()))?),
        };
        let effective=SshConfig::effective(host,optional_string(args,"user"),port)?;
        let port=effective.port.expect("resolved port");let user=effective.user.expect("resolved user");
        let default_name = remote_work_dir
            .to_string_lossy()
            .trim_end_matches('/')
            .rsplit('/')
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("remote-workspace")
            .to_string();
        let name = optional_string(args, "name")
            .unwrap_or(&default_name)
            .to_string();
        let workspace_id = Uuid::new_v4();
        let connection = ConnectionConfig {
            host: host.to_string(),
            port,
            user,
            auth: AuthMethod::Agent,
            jump_hosts: Vec::new(),
        };
        let confirmed_mode = if let Some(value) = optional_string(args, "node_execution_mode") {
            if !args
                .get("node_mode_confirmed")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return Err(CoreError::PermissionDenied(
                    "node_execution_mode requires node_mode_confirmed:true from the user".into(),
                ));
            }
            Some(parse_node_execution_mode(value)?)
        } else {
            None
        };
        let known_mode = workspace_store::list_workspaces(&self.config)?
            .into_iter()
            .find(|existing| {
                same_node(&existing.connection, &connection)
                    && existing.node_execution_mode != NodeExecutionMode::Unconfirmed
            })
            .map(|existing| existing.node_execution_mode);
        let now = chrono::Utc::now();
        let workspace = Workspace {
            id: workspace_id,
            name: name.clone(),
            display_name: name,
            connection,
            remote_work_dir,

            remote_data_dirs: remote_data_dirs(args)?,
            node_execution_mode: confirmed_mode
                .or(known_mode)
                .unwrap_or(NodeExecutionMode::Unconfirmed),
            authorized: args
                .get("authorize")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            created_at: now,
            last_accessed_at: now,
            file_threshold_bytes: self.config.file_threshold_bytes,
            config: Default::default(),
        };
        if !self.executor.check_connection(&workspace).await? {
            return Err(CoreError::Connection(format!(
                "SSH connection failed for {host}"
            )));
        }
        let layout = self.executor.inspect_workspace_layout(&workspace).await?;
        if !layout.valid {
            return Err(CoreError::Validation(format!(
                "Invalid workspace directory layout: {}",
                layout.errors.join("; ")
            )));
        }
        workspace_store::save_workspace(&self.config, &workspace)?;
        let cluster = self
            .cluster_detector
            .discover_cluster(&workspace)
            .await
            .ok();
        if let Some(cluster) = &cluster {
            workspace_store::save_cluster_config(&self.config, cluster, workspace.id)?;
        }
        Ok(serde_json::json!({
            "status": "configured",
            "workspace_id": workspace.id,
            "local_platform": std::env::consts::OS,
            "remote_work_dir": workspace.remote_work_dir,
            "remote_data_dirs": workspace.remote_data_dirs,
            "node_execution_mode": workspace.node_execution_mode,
            "node_mode_confirmation_required": workspace.node_execution_mode == NodeExecutionMode::Unconfirmed,
            "detected_scheduler": cluster.as_ref().map(|value| &value.scheduler),
            "authorized": workspace.authorized,
        }))
    }

    fn set_node_mode(
        &self,
        workspace: &Workspace,
        args: &serde_json::Map<String, Value>,
    ) -> CoreResult<Value> {
        let requested = optional_string(args, "node_execution_mode");
        let confirmed = args
            .get("confirmed")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if requested.is_none() || !confirmed {
            return Ok(serde_json::json!({
                "status": "confirmation_required",
                "workspace_id": workspace.id,
                "remote_host": workspace.connection.host,
                "current_mode": workspace.node_execution_mode,
                "choices": ["submit_only", "direct_compute"],
                "safe_default": "submit_only",
                "instruction": "Call again with node_execution_mode and confirmed:true after user confirmation"
            }));
        }
        let mode = parse_node_execution_mode(requested.expect("checked above"))?;
        if mode == NodeExecutionMode::Unconfirmed {
            return Err(CoreError::Validation(
                "node_execution_mode must be submit_only or single_server (legacy: direct_compute)".into(),
            ));
        }
        let mut updated = Vec::new();
        for mut candidate in workspace_store::list_workspaces(&self.config)? {
            if same_node(&candidate.connection, &workspace.connection) {
                candidate.node_execution_mode = mode;
                workspace_store::save_workspace(&self.config, &candidate)?;
                updated.push(candidate.id);
            }
        }
        Ok(serde_json::json!({
            "status": "confirmed",
            "remote_host": workspace.connection.host,
            "node_execution_mode": mode,
            "updated_workspace_ids": updated,
        }))
    }

    async fn add_data_dir(&self, args: &serde_json::Map<String, Value>) -> CoreResult<Value> {
        let Some(data_dir) = optional_string(args, "remote_data_dir") else {
            return Ok(input_required(&["remote_data_dir"]));
        };
        let data_dir = PathBuf::from(normalize_remote_absolute(Path::new(data_dir))?);
        let mut workspace = resolve_workspace(&self.config, args)?;
        if !workspace.authorized {
            return Err(CoreError::PermissionDenied(
                "Workspace is not authorized; call workspace_authorize first".into(),
            ));
        }
        if workspace.remote_data_dirs.iter().any(|existing| {
            normalize_remote_absolute(existing).ok() == normalize_remote_absolute(&data_dir).ok()
        }) {
            return Ok(serde_json::json!({
                "status": "already_configured",
                "workspace_id": workspace.id,
                "remote_data_dirs": workspace.remote_data_dirs,
            }));
        }
        workspace.remote_data_dirs.push(data_dir);
        let layout = self.executor.inspect_workspace_layout(&workspace).await?;
        if !layout.valid {
            return Err(CoreError::Validation(format!(
                "Invalid remote data directory: {}",
                layout.errors.join("; ")
            )));
        }
        workspace_store::save_workspace(&self.config, &workspace)?;
        Ok(serde_json::json!({
            "status": "added",
            "workspace_id": workspace.id,
            "remote_data_dirs": workspace.remote_data_dirs,
        }))
    }

    async fn discover(&self, _args: &serde_json::Map<String, Value>) -> CoreResult<Value> {
        let workspaces = workspace_store::list_workspaces(&self.config)?;
        let values: Vec<Value> = workspaces
            .iter()
            .map(|workspace| {
                serde_json::json!({
                    "workspace_id": workspace.id,
                    "name": workspace.name,
                    "authorized": workspace.authorized,
                    "remote_host": workspace.connection.host,
                    "remote_work_dir": workspace.remote_work_dir,
                    "remote_data_dirs": workspace.remote_data_dirs,
                    "node_execution_mode": workspace.node_execution_mode,
                    "authentication_ready": lrworkspace_remote::local_auth::connection_ready(&workspace.connection),
                })
            })
            .collect();
        Ok(serde_json::json!({
            "architecture": "remote-first-no-sync",
            "local_platform": std::env::consts::OS,
            "local_arch": std::env::consts::ARCH,
            "local_data_dir": self.config.data_dir,
            "elicitation_supported": self.client_supports_elicitation.load(std::sync::atomic::Ordering::Relaxed),
            "workspaces": values,
            "next_action": if values.is_empty() { "Use workspace_login for private local password entry, or workspace_configure for key/agent authentication. Never request passwords in chat." } else { "If authentication_ready is false, call workspace_login with workspace_id. Otherwise call workspace_layout, then workspace_env/index/read/write/run." },
        }))
    }

    async fn job_logs(&self,workspace:&Workspace,job_id:&str,scheduler:ClusterScheduler)->CoreResult<(String,String)> {
        if let Some(logs)=tracked_batch_logs(&self.executor,&self.config,workspace,job_id).await? {return Ok(logs);}
        match scheduler {
            ClusterScheduler::Slurm=>self.slurm.fetch_logs(workspace,job_id).await,
            ClusterScheduler::Pbs=>self.pbs.fetch_logs(workspace,job_id).await,
            _=>Err(CoreError::InvalidState("Logs require Slurm/PBS".into())),
        }
    }

    async fn load_or_discover_cluster(&self, workspace: &Workspace) -> CoreResult<ClusterConfig> {
        match workspace_store::load_cluster_config(&self.config, workspace.id) {
            Ok(cluster) => Ok(cluster),
            Err(_) => {
                let cluster = self.cluster_detector.discover_cluster(workspace).await?;
                workspace_store::save_cluster_config(&self.config, &cluster, workspace.id)?;
                Ok(cluster)
            }
        }
    }

    async fn run_command(
        &self,
        workspace: &Workspace,
        args: &serde_json::Map<String, Value>,
        agent: &AgentKind,
    ) -> CoreResult<Value> {
        let command = required_string(args, "command")?;
        let working_dir = args
            .get("working_dir")
            .and_then(Value::as_str)
            .map(PathBuf::from);
        lrworkspace_remote::batch::validate_commands(&[BatchCommand {source_line:1,command:command.into()}])?;
        let risk = classify_command_risk(command);
        match risk.default_policy() {
            DefaultPolicy::Deny | DefaultPolicy::Confirm => {
                return Err(CoreError::PermissionDenied(format!(
                    "MCP command {} is {} and was refused",
                    command,
                    risk.label()
                )))
            }
            DefaultPolicy::Allow | DefaultPolicy::AllowLog => {}
        }
        let executor = self.executor.with_timeout(request_timeout(args, self.config.ssh_timeout_secs)?);
        let force = match args.get("force") {
            None => None,
            Some(value) => Some(value.as_str().ok_or_else(|| CoreError::Validation("force must be a string".into()))?),
        };
        let route = resolve_execution_route(workspace.node_execution_mode, force)?;
        if args.get("background").is_some_and(|value| !value.is_boolean()) {
            return Err(CoreError::Validation("background must be a boolean".into()));
        }
        let execution_path=match args.get("execution_path") {
            Some(Value::Null)=>"auto",
            Some(value)=>value.as_str().ok_or_else(||CoreError::Validation("execution_path must be a string".into()))?,
            None=>"auto",
        };
        let execution_path=lrworkspace_remote::normalize_execution_path(route,execution_path)?;
        let has_batch=has_batch_options(args)?;
        if route!=ExecutionRoute::Cluster && has_batch {
            return Err(CoreError::Validation("batch options require force=cluster or a submit-only workspace".into()));
        }
        let compute_node=match args.get("compute_node") {
            None=>None,
            Some(value)=>Some(value.as_str().ok_or_else(||CoreError::Validation("compute_node must be a hostname string".into()))?),
        };
        lrworkspace_remote::compute_ssh::validate_compute_node(route,compute_node,has_batch)?;
        let mut result = match route {
            ExecutionRoute::ReadOnly => {
                if risk != RiskLevel::ReadOnly {
                    return Err(CoreError::PermissionDenied("Read-only execution requires an allowlisted command".into()));
                }
                let record = executor.execute_read_only(workspace, command, working_dir.as_deref(), &Default::default()).await?;
                serde_json::json!({"mode":"read-only", "result":record})
            }
            ExecutionRoute::Cluster => {
                if compute_node.is_some() && execution_path=="submit" {
                    return Err(CoreError::Validation("compute_node conflicts with execution_path=submit".into()));
                }
                if execution_path=="ssh" && has_batch {
                    return Err(CoreError::Validation("SSH cannot be combined with scheduler batch resources".into()));
                }
                let selected=if let Some(node)=compute_node {Some(lrworkspace_remote::compute_ssh::verify_compute_node(&executor,&self.config,workspace,node).await?)}
                    else if execution_path!="submit" && !has_batch {
                        lrworkspace_remote::compute_ssh::choose_compute_node(&executor,&self.config,workspace).await?
                    } else {None};
                if selected.is_none() && execution_path=="ssh" {
                    return Err(CoreError::NotFound("No reachable idle compute node; workload was not started".into()));
                }
                if let Some(node)=selected {
                    if args.get("background").and_then(Value::as_bool).unwrap_or(true) {
                        lrworkspace_remote::tasks::start(&executor,&self.config,workspace,Some(&node),command,working_dir.as_deref()).await?
                    } else {
                        let record=executor.execute_compute_ssh(workspace,&node,command,working_dir.as_deref()).await?;
                        serde_json::json!({"mode":"cluster","execution_path":"ssh","compute_node":node,"result":record})
                    }
                } else {
                    let cluster = match workspace_store::load_cluster_config(&self.config, workspace.id) {
                        Ok(cluster) => cluster,
                        Err(_) => {
                            let detector = ClusterDetectorImpl::new(Arc::new(executor.with_timeout(executor.timeout())));
                            let cluster = detector.discover_cluster(workspace).await?;
                            workspace_store::save_cluster_config(&self.config, &cluster, workspace.id)?;
                            cluster
                        }
                    };
                    let commands=[BatchCommand {source_line:1,command:command.into()}];
                    let result=submit_batch(Arc::new(executor),&self.config,workspace,&cluster,&commands,&batch_options(args)?,working_dir.as_deref(),true,agent).await?;
                    let mut value=serde_json::to_value(result)?;value["mode"]=serde_json::json!("cluster");value["execution_path"]=serde_json::json!("submit");value
                }
            }
            ExecutionRoute::Direct => {
                if args.get("background").and_then(Value::as_bool).unwrap_or(true) {
                    lrworkspace_remote::tasks::start(&executor,&self.config,workspace,None,command,working_dir.as_deref()).await?
                } else {
                    let record = executor.execute_mcp_guarded(workspace, command, working_dir.as_deref(), &Default::default()).await?;
                    serde_json::json!({"mode":"direct", "result":record})
                }
            }
        };
        let audit_command = command.chars().take(500).collect::<String>();
        let audit=self.audit
            .record(
                workspace.id,
                agent,
                "mcp_workload_run",
                risk,
                &audit_command,
                &format!(
                    "mode={}; node_execution_mode={:?}; execution_path={}; compute_node={}; real SSH permissions; data roots are read-only by file API and agent instructions",
                    result.get("mode").and_then(Value::as_str).unwrap_or_default(),
                    workspace.node_execution_mode, result["execution_path"], result["compute_node"]
                ),
            )
            .await;
        if let Err(error)=audit {result["audit_warning"]=serde_json::json!(format!("Operation completed, but audit failed: {error}; do not blindly replay"));}
        Ok(result)
    }
}

fn elicitation_for_request(request: &JsonRpcRequest) -> Option<Value> {
    if request.method != "tools/call" {
        return None;
    }
    let params = request.params.as_ref()?.as_object()?;
    let name = params.get("name")?.as_str()?;
    let args = params
        .get("arguments")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let missing = |key: &str| {
        args.get(key)
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
    };

    match name {
        "workspace_configure" if missing("host") || missing("remote_work_dir") => {
            let mut required = Vec::new();
            if missing("host") {
                required.push("host");
            }
            if missing("remote_work_dir") {
                required.push("remote_work_dir");
            }
            Some(serde_json::json!({
                "message": "Configure remote work/data roots. The agent uses its own project; no local LRW project directory is required.",
                "requestedSchema": {
                    "type": "object",
                    "properties": {
                        "host": {"type": "string", "title": "SSH host", "description": "Host or alias from ~/.ssh/config"},
                        "user": {"type": "string", "title": "SSH user", "description": "Optional when the SSH config already defines User"},
                        "port": {"type": "integer", "title": "SSH port (optional)", "minimum": 1, "maximum": 65535, "description":"Leave blank to inherit this computer's OpenSSH configuration"},
                        "remote_work_dir": {"type": "string", "title": "Remote work directory", "description": "Writable remote POSIX path, for example /scratch/user/project"},
                        "remote_data_dirs": {"type": "string", "title": "Remote data directories", "description": "Optional remote POSIX data paths (newline/comma separated); readable through MCP file tools, writes rejected"},
                        "name": {"type": "string", "title": "Workspace name", "description": "Optional; defaults to the remote work directory name"},
                        "node_execution_mode": {"type": "string", "title": "SSH node behavior", "description": "Optional. Leave blank to reuse a saved decision for the same host. Choose submit_only for login/bastion nodes; choose single_server for a standalone server (direct_compute is a legacy alias).", "enum": ["submit_only", "single_server", "direct_compute"]},
                        "node_mode_confirmed": {"type": "boolean", "title": "Confirm SSH node behavior", "description": "Required only when selecting a new node_execution_mode", "default": false},
                        "authorize": {"type": "boolean", "title": "Authorize MCP access", "default": true}
                    },
                    "required": required
                }
            }))
        }
        "workspace_add_data_dir" if missing("remote_data_dir") => Some(serde_json::json!({
            "message": "Add a remote data directory. MCP file reads may use it and writes are rejected; agents must not modify it. The SSH user's permissions are never changed.",
            "requestedSchema": {
                "type": "object",
                "properties": {
                    "remote_data_dir": {"type": "string", "title": "Remote data directory", "description": "Absolute remote POSIX path"}
                },
                "required": ["remote_data_dir"]
            }
        })),
        "workspace_set_node_mode"
            if missing("node_execution_mode")
                || !args
                    .get("confirmed")
                    .and_then(Value::as_bool)
                    .unwrap_or(false) =>
        {
            Some(serde_json::json!({
                "message": "Confirm how workload commands may use this SSH node. submit_only is the safe default for login, bastion, and scheduler submission nodes.",
                "requestedSchema": {
                    "type": "object",
                    "properties": {
                        "node_execution_mode": {"type": "string", "title": "SSH node behavior", "enum": ["submit_only", "single_server", "direct_compute"], "description": "submit_only never runs workloads on the login endpoint; cluster automatically selects idle compute SSH nodes, or submits to the scheduler. single_server permits explicit force=direct on a standalone server; default execution remains cluster; direct_compute is its legacy alias"},
                        "confirmed": {"type": "boolean", "title": "I confirm this node behavior", "default": false}
                    },
                    "required": ["node_execution_mode", "confirmed"]
                }
            }))
        }
        _ => None,
    }
}

#[cfg(test)]
fn next_protocol_line<R: BufRead>(
    reader: &mut R,
    pending: &mut VecDeque<String>,
) -> CoreResult<Option<String>> {
    if let Some(line) = pending.pop_front() {
        return Ok(Some(line));
    }
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        Ok(None)
    } else {
        Ok(Some(line))
    }
}

#[cfg(test)]
fn wait_for_elicitation_response<R: BufRead>(
    reader: &mut R,
    pending: &mut VecDeque<String>,
    id: &str,
) -> CoreResult<Option<serde_json::Map<String, Value>>> {
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line)? == 0 {
            return Ok(None);
        }
        let response: Value = match serde_json::from_str(line.trim()) {
            Ok(value) => value,
            Err(_) => {
                pending.push_back(line);
                continue;
            }
        };
        // The client can send requests/notifications while a form is open.
        // Preserve them in arrival order for the main dispatch loop; only
        // consume the matching server-request response here. Requests and
        // responses have separate ID namespaces, hence the method check.
        if response.get("id").and_then(Value::as_str) != Some(id)
            || response.get("method").is_some()
        {
            pending.push_back(line);
            continue;
        }
        if response.get("error").is_some() {
            return Ok(None);
        }
        let result = response.get("result").and_then(Value::as_object);
        if result
            .and_then(|value| value.get("action"))
            .and_then(Value::as_str)
            != Some("accept")
        {
            return Ok(None);
        }
        return Ok(result
            .and_then(|value| value.get("content"))
            .and_then(Value::as_object)
            .cloned());
    }
}

fn merge_tool_arguments(request: &mut JsonRpcRequest, content: serde_json::Map<String, Value>) {
    let Some(params) = request.params.as_mut().and_then(Value::as_object_mut) else {
        return;
    };
    let arguments = params
        .entry("arguments")
        .or_insert_with(|| Value::Object(serde_json::Map::new()));
    let Some(arguments) = arguments.as_object_mut() else {
        return;
    };
    for (key, value) in content {
        arguments.insert(key, value);
    }
}

fn tool_definitions() -> Vec<ToolDefinition> {
    let workspace = serde_json::json!({"workspace_id": {"type": "string"}});
    let rooted_path = serde_json::json!({
        "workspace_id": {"type": "string"},
        "root": {"type": "string", "description": "work or data:N", "default": "work"},
        "relative_path": {"type": "string"},
        "transport": {"type":"string", "enum":["auto","rsync","scp","base64"], "default":"auto"},
        "timeout_sec": {"type":"integer", "minimum":1, "default":120}
    });
    vec![
        tool("workspace_discover", "List registered remote workspaces and their work/data roots.", serde_json::json!({}), &[]),
        tool("workspace_login","Open a separate native local login window for IP/port/user/password without SSH config. Never send passwords in chat or tool arguments; do not inspect, screenshot or automate credential entry. Returns a request ID immediately; poll workspace_login_status. Credentials stay in this MCP process and require re-login after restart. Optional workspace_id re-authenticates an existing workspace without changing its endpoint.",serde_json::json!({"workspace_id":{"type":"string"}}),&[]),
        tool("workspace_login_status","Read local login progress and authentication result; contains no password or form values.",serde_json::json!({"request_id":{"type":"string"}}),&["request_id"]),
        tool("workspace_logout","Revoke this MCP process's local password session. Keeps registration and remote files; submitted jobs and in-flight operations may continue. Password redaction filters remain in memory until process exit.",serde_json::json!({"workspace_id":{"type":"string"}}),&[]),
        tool("workspace_doctor","Check local SSH/SCP/optional rsync and this computer's OpenSSH configuration. Does not contact servers or read private keys. With workspace_id, reports stale port mismatches without changing them.",serde_json::json!({"workspace_id":{"type":"string"}}),&[]),
        tool("workspace_update_connection","Explicitly repair a registered SSH port after the user confirms the endpoint. Default previews; confirmed:true backs up the original registration before updating. Does not change SSH config or cancel jobs.",serde_json::json!({"workspace_id":{"type":"string"},"port":{"type":"integer","minimum":1,"maximum":65535},"confirmed":{"type":"boolean","default":false}}),&["port"]),
        tool("workspace_configure", "Configure a workspace. Call without arguments to request a client form when elicitation is supported; No local project directory is required; data roots may be added later. A saved node mode is reused for the same host/user/port.", serde_json::json!({"host": {"type": "string"}, "user": {"type": "string"}, "port": {"type": "integer", "minimum":1,"maximum":65535,"description":"Optional; omit to inherit this computer's OpenSSH config"}, "remote_work_dir": {"type": "string"}, "remote_data_dirs": {"type": "string", "description": "Newline- or comma-separated remote POSIX paths"}, "name": {"type": "string"}, "node_execution_mode": {"type": "string", "enum": ["submit_only", "single_server", "direct_compute"]}, "node_mode_confirmed": {"type": "boolean", "default": false}, "authorize": {"type": "boolean", "default": false}}), &[]),
        tool("workspace_add_data_dir", "Add one remote data root at any time. Call without remote_data_dir to request a client input form.", serde_json::json!({"workspace_id": {"type": "string"}, "remote_data_dir": {"type": "string"}}), &[]),
        tool("workspace_set_node_mode", "Confirm once per SSH host/user/port whether this is a cluster login node (submit_only) or a standalone server (single_server; legacy direct_compute). The decision is persisted and reused by all workspaces on the same node.", serde_json::json!({"workspace_id": {"type": "string"}, "node_execution_mode": {"type": "string", "enum": ["submit_only", "single_server", "direct_compute"]}, "confirmed": {"type": "boolean", "default": false}}), &[]),
        tool("workspace_authorize", "Authorize a workspace after review.", serde_json::json!({"workspace_id": {"type": "string"}, "confirmed": {"type": "boolean"}}), &["confirmed"]),
        tool("workspace_layout", "Validate the remote work/data layout (existence, readability, canonical non-overlap) without changing user permissions.", workspace.clone(), &[]),
        tool("workspace_status", "Check SSH and directory-layout status.", workspace.clone(), &[]),
        tool("workspace_env", "Refresh ~/.bin_path_file from the login-shell PATH without recursive scans or version probes, then capture the remote environment using grep.", workspace.clone(), &[]),
        tool("workspace_index", "Index root=work or data:N. On unconfirmed/submit_only nodes this lists Git tracked and unignored paths and refuses non-Git recursive scans.", serde_json::json!({"workspace_id": {"type": "string"}, "root": {"type": "string", "default": "work"}}), &[]),
        tool("workspace_search_files", "Search file names in work or data:N. On unconfirmed/submit_only nodes this lists Git tracked and unignored paths and never runs find/rg recursively.", serde_json::json!({"workspace_id": {"type": "string"}, "root": {"type": "string", "default": "work"}, "pattern": {"type": "string"}}), &["pattern"]),
        tool("workspace_read_file", "Preview bounded UTF-8 text or PNG/JPEG/GIF/WebP images from work/data roots. Images return native MCP image content. auto tries rsync then SCP; base64 SSH is explicit only. For full/binary files use workspace_download_file, never workspace_run base64. No persistent local copy.", rooted_path, &["relative_path"]),
        tool("workspace_download_file", "Download any file from work/data roots to a NEW absolute local_path in the agent project. Streams through rsync, falls back to SCP; no automatic base64. Never overwrites an existing file. Returns path, size, actual transport and fallback reasons, without file contents. Parent directory must exist. No project registration required.", serde_json::json!({"workspace_id":{"type":"string"},"root":{"type":"string","default":"work"},"relative_path":{"type":"string"},"local_path":{"type":"string"},"transport":{"type":"string","enum":["auto","rsync","scp"],"default":"auto"},"timeout_sec":{"type":"integer","minimum":1,"default":120}}), &["relative_path","local_path"]),
        tool("workspace_write_file", "Upload a file from the agent's own project via local_path, or supply UTF-8 content. Exactly one input is required. Writes stay under the remote work root; no local project registration. transport=rsync/scp/base64 is strict; auto tries rsync then SCP without base64 fallback. Returns the actual transport.", serde_json::json!({"workspace_id":{"type":"string"},"root":{"type":"string","enum":["work"],"default":"work"},"relative_path":{"type":"string"},"content":{"type":"string"},"local_path":{"type":"string","description":"Existing file in the agent's own local project"},"transport":{"type":"string","enum":["auto","rsync","scp","base64"],"default":"auto"},"timeout_sec":{"type":"integer","minimum":1,"default":120}}), &["relative_path"]),
        tool("workspace_run", "For file/image transfer use workspace_read_file/download_file/write_file; do not run base64 to move files. Run independent commands under real SSH permissions. Cluster auto discovers reachable idle compute nodes and uses SSH; if none is available, submit to Slurm/PBS. execution_path=submit or batch resources force scheduler submission. Never execute workloads on the login endpoint. SSH workloads default to background=true and return a durable task_id; monitor via workspace_task_status/logs and workspace_tasks. background=false waits for output. direct is only for explicitly configured standalone servers. timeout_sec budgets SSH operations, not the lifetime of detached tasks. No automatic replay after an uncertain launch.", serde_json::json!({"workspace_id":{"type":"string"},"command":{"type":"string"},"working_dir":{"type":"string"},"compute_node":{"type":"string","description":"Optional explicit compute hostname, verified against fresh scheduler inventory and allowed partitions; omitted means automatic discovery."},"execution_path":{"type":["string","null"],"enum":["auto","ssh","submit","cluster","compute-node","compute_node","direct","submit-only","read-only","read_only",null],"default":"auto","description":"Usually omit. Transport hint: auto chooses compute SSH then scheduler; ssh requires compute SSH in cluster mode; submit forces the scheduler. force selects the execution mode. Legacy cluster/compute-node/direct path hints are normalized without allowing login-node workloads."},"background":{"type":"boolean","default":true},"batch":batch_schema(),"force":{"type":"string","enum":["direct","cluster","read-only"],"default":"cluster"},"timeout_sec":{"type":"integer","minimum":1,"default":120}}), &["command"]),
        tool("workspace_tasks", "List independent SSH task handles in this workspace without contacting a server. Returned states are last observations; query task_id for live state. Never deletes tasks or logs.", serde_json::json!({"workspace_id":{"type":"string"},"limit":{"type":"integer","minimum":1,"maximum":100,"default":20}}), &[]),
        tool("workspace_task_status", "Read one SSH task's live state, exit code and actual hostname without waiting for other commands. Unknown is not success. Handles survive MCP restart.", serde_json::json!({"workspace_id":{"type":"string"},"task_id":{"type":"string"},"timeout_sec":{"type":"integer","minimum":1,"default":120}}), &["task_id"]),
        tool("workspace_task_logs", "Read independent bounded stdout/stderr tails and live task state. Output is read on the recorded compute node; empty output is not completion evidence.", serde_json::json!({"workspace_id":{"type":"string"},"task_id":{"type":"string"},"max_bytes":{"type":"integer","minimum":1,"maximum":65536,"default":16384},"timeout_sec":{"type":"integer","minimum":1,"default":120}}), &["task_id"]),
        tool("workspace_submit", "Build the supplied command-file array mode: filter blank/comments, shuffle, balance at most 1000 task scripts, bound concurrency by maxnum and maxcpu/(ntasks*cpu), continue after individual failures, and report aggregate failure. Provide exactly one of local_path or commands_text. Every run gets a fresh directory; prior files are retained. Default generates only; submit=true launches. PBS flavor is reused per workspace or detected by read-only version/usage probes; an explicit pbs-pro/torque choice is remembered.", serde_json::json!({"workspace_id":{"type":"string"},"local_path":{"type":"string"},"commands_text":{"type":"string"},"batch":batch_schema(),"working_dir":{"type":"string"},"submit":{"type":"boolean","default":false},"timeout_sec":{"type":"integer","minimum":1,"default":120}}), &[]),
        tool("workspace_cluster_info", "Read the cached scheduler/topology snapshot. refresh=true rediscovers topology, preserves user constraints, and replaces the cache. Missing resources stay unknown; cached data retains its original refreshed_at timestamp.", serde_json::json!({"workspace_id":{"type":"string"},"refresh":{"type":"boolean","default":false}}), &[]),
        tool("workspace_slurm_status", "Read scheduler job status (Slurm/PBS).", serde_json::json!({"workspace_id": {"type": "string"}, "job_id": {"type": "string"}}), &["job_id"]),
        tool("workspace_slurm_logs", "Read scheduler job logs (Slurm/PBS).", serde_json::json!({"workspace_id": {"type": "string"}, "job_id": {"type": "string"}}), &["job_id"]),
        tool("workspace_slurm_cancel", "Cancel a submitted scheduler job (Slurm/PBS).", serde_json::json!({"workspace_id": {"type": "string"}, "job_id": {"type": "string"}}), &["job_id"]),
        tool("workspace_audit", "Read the newest MCP operation audit records, at most 1000 per query.", serde_json::json!({"workspace_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 0, "maximum": 1000}}), &[]),
        // --- Diagnostic tools (integrated from lrw-diagnose) ---
        tool("workspace_diagnose_list", "List workspaces from local metadata (SSH host, user, cached scheduler, node mode). No SSH. Read-only.", serde_json::json!({}), &[]),
        tool("workspace_diagnose_job", "Diagnose a finished or running Slurm/PBS job: state, exit code, log tail, classified error signatures. Read-only.", serde_json::json!({"workspace_id": {"type": "string"}, "job_id": {"type": "string"}}), &["job_id"]),
        tool("workspace_diagnose_wait", "Poll a Slurm/PBS job with a configurable polling budget (default 120 s), plus in-flight SSH queries and diagnosis. Only known terminal states produce final_state; otherwise return still_running or unknown. Read-only.", serde_json::json!({"workspace_id": {"type": "string"}, "job_id": {"type": "string"}, "timeout_sec": {"type": "integer", "default": 120, "minimum": 1}, "interval_sec": {"type": "integer", "default": 15}}), &["job_id"]),
        tool("workspace_diagnose_classify", "Classify HPC job log text into structured failure signatures (OOM, module missing, walltime, signal kill, disk quota, path/permission, MPI/network). Pure local regex, no SSH. Read-only.", serde_json::json!({"log_text": {"type": "string"}}), &["log_text"]),
        tool("workspace_diagnose_topo", "Discover compute-node hardware topology for a partition: core counts, memory, GPU models. Feeds parallel parameter advice. Read-only.", serde_json::json!({"workspace_id": {"type": "string"}, "partition": {"type": "string"}}), &["partition"]),
        tool("workspace_diagnose_node", "Inspect one compute node in detail (cores, memory, GPU, state, arch) via cached cluster info. Read-only.", serde_json::json!({"workspace_id": {"type": "string"}, "node": {"type": "string"}}), &["node"]),
        tool("workspace_diagnose_suggest", "Suggest thread counts, memory per core, and partition choice for a workload, based on live partition topology. Read-only advice only.", serde_json::json!({"workspace_id": {"type": "string"}, "partition": {"type": "string"}, "workload": {"type": "string", "enum": ["aligner", "assembler", "variant", "python", "r", "mpi", "generic"], "default": "generic"}}), &["partition"]),
        tool("workspace_diagnose_journal", "Read the local diagnostic journal: which jobs were observed, when, and the diagnosis summary. Useful to recover context across sessions.", serde_json::json!({"workspace_id": {"type": "string"}, "limit": {"type": "integer", "minimum": 0, "maximum": 1000, "default": 50}}), &[]),
    ]
}

fn tool(name: &str, description: &str, properties: Value, required: &[&str]) -> ToolDefinition {
    let mut input_schema=serde_json::json!({"type":"object","properties":properties,"required":required});
    if matches!(name,"workspace_login"|"workspace_login_status"|"workspace_logout"){input_schema["additionalProperties"]=Value::Bool(false);}
    ToolDefinition {
        name: name.into(),
        description: description.into(),
        input_schema,
    }
}

fn parse_root(args: &serde_json::Map<String, Value>) -> CoreResult<RemoteRootKind> {
    let value = args.get("root").and_then(Value::as_str).unwrap_or("work");
    if value == "work" {
        return Ok(RemoteRootKind::Work);
    }
    let Some(index) = value.strip_prefix("data:") else {
        return Err(CoreError::Validation(
            "root must be `work` or `data:N`".into(),
        ));
    };
    let index = index
        .parse::<usize>()
        .map_err(|_| CoreError::Validation("data root index must be an integer".into()))?;
    Ok(RemoteRootKind::Data(index))
}

fn root_label(root: RemoteRootKind) -> String {
    match root {
        RemoteRootKind::Work => "work".into(),
        RemoteRootKind::Data(index) => format!("data:{index}"),
    }
}

fn required_string<'a>(args: &'a serde_json::Map<String, Value>, key: &str) -> CoreResult<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| CoreError::Validation(format!("{key} is required")))
}

fn optional_string<'a>(args: &'a serde_json::Map<String, Value>, key: &str) -> Option<&'a str> {
    args.get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn parse_node_execution_mode(value: &str) -> CoreResult<NodeExecutionMode> {
    match value {
        "submit_only" => Ok(NodeExecutionMode::SubmitOnly),
        "direct_compute" | "single_server" => Ok(NodeExecutionMode::DirectCompute),
        "unconfirmed" => Ok(NodeExecutionMode::Unconfirmed),
        _ => Err(CoreError::Validation(
            "node_execution_mode must be submit_only or single_server (legacy: direct_compute)".into(),
        )),
    }
}

fn same_node(left: &ConnectionConfig, right: &ConnectionConfig) -> bool {
    left.host.eq_ignore_ascii_case(&right.host)
        && left.port == right.port
        && left.user == right.user
}

fn remote_data_dirs(args: &serde_json::Map<String, Value>) -> CoreResult<Vec<PathBuf>> {
    let values: Vec<&str> = match args.get("remote_data_dirs") {
        Some(Value::Array(values)) => values.iter().filter_map(Value::as_str).collect(),
        Some(Value::String(value)) => value
            .split(['\n', ',', ';'])
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .collect(),
        _ => Vec::new(),
    };
    let mut result = Vec::new();
    for value in values {
        let normalized = PathBuf::from(normalize_remote_absolute(Path::new(value))?);
        if !result.contains(&normalized) {
            result.push(normalized);
        }
    }
    Ok(result)
}

fn input_required(fields: &[&str]) -> Value {
    serde_json::json!({
        "status": "input_required",
        "local_platform": std::env::consts::OS,
        "requested_fields": fields,
        "instruction": "Ask the user for these values and call the same tool again; clients with MCP elicitation enabled show a form automatically"
    })
}

fn resolve_workspace(
    config: &LrWorkspaceConfig,
    args: &serde_json::Map<String, Value>,
) -> CoreResult<Workspace> {
    if let Some(id) = args
        .get("workspace_id")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
    {
        return workspace_store::resolve_workspace(config, Some(id));
    }
    workspace_store::resolve_workspace(config, None)
}

fn success(request: &JsonRpcRequest, value: Value) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id: request.id.clone(),
        result: Some(lrworkspace_core::privacy::redacted_value(value)),
        error: None,
    }
}

fn failure(request: &JsonRpcRequest, code: i32, message: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse {
        jsonrpc: "2.0".into(),
        id: request.id.clone(),
        result: None,
        error: Some(JsonRpcError {
            code,
            message: lrworkspace_core::privacy::redact_text(&message.into()),
            data: None,
        }),
    }
}

fn tool_rpc_error(error: &CoreError) -> JsonRpcError {
    let code = match error {
        CoreError::PermissionDenied(_) => -32003,
        CoreError::NotFound(_) => -32004,
        CoreError::Timeout(_) => -32008,
        _ => -32000,
    };
    JsonRpcError {
        code,
        message: lrworkspace_core::privacy::redact_text(&error.to_string()),
        data: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn passwords_are_rejected_as_tool_arguments_without_echo_or_persistence() {
        let root=std::env::temp_dir().join(format!("lrw-no-secret-args-{}",Uuid::new_v4()));
        let config=LrWorkspaceConfig{data_dir:root.clone(),..Default::default()};let server=McpServer::new(config,AgentKind::CliAgent).unwrap();
        let request:JsonRpcRequest=serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workspace_login","arguments":{"password":"unregistered-input-canary-8357"}}})).unwrap();
        let response=server.handle_request(&request).await;let wire=serde_json::to_string(&response).unwrap();assert!(!wire.contains("unregistered-input-canary"));assert!(response.error.is_some());
        assert!(!root.exists()||std::fs::read_dir(&root).unwrap().next().is_none());
        for name in ["workspace_login","workspace_login_status","workspace_logout"] {
            let tool=tool_definitions().into_iter().find(|tool|tool.name==name).unwrap();assert_eq!(tool.input_schema["additionalProperties"],false);assert!(tool.input_schema["properties"].get("password").is_none());
        }
        let _=std::fs::remove_dir_all(root);
    }
    #[tokio::test]
    async fn resource_advice_rejects_unknown_metadata_and_uses_observed_capacity() {
        let root=std::env::temp_dir().join(format!("lrw-resource-advice-{}",Uuid::new_v4()));
        let config=LrWorkspaceConfig{data_dir:root.clone(),..Default::default()};
        let workspace:Workspace=serde_json::from_value(serde_json::json!({
            "id":Uuid::new_v4(),"name":"fixture","display_name":"fixture",
            "connection":{"host":"fixture.invalid","user":"fixture","port":22,"auth":"agent","jump_hosts":[]},
            "remote_work_dir":"/work","remote_data_dirs":[],"node_execution_mode":"submit_only","authorized":true,
            "created_at":chrono::Utc::now(),"last_accessed_at":chrono::Utc::now(),"file_threshold_bytes":1048576,"config":{}
        })).unwrap();
        workspace_store::save_workspace(&config,&workspace).unwrap();
        let mut cluster=ClusterConfig{scheduler:ClusterScheduler::Slurm,nodes:vec![NodeInfo{hostname:"compute01".into(),cpu_cores:0,memory_mb:16000,gpu_count:0,gpu_model:None,state:"idle".into(),partitions:vec!["cpu".into()],arch:None}],partitions:vec![],constraints:Default::default(),refreshed_at:chrono::Utc::now()};
        workspace_store::save_cluster_config(&config,&cluster,workspace.id).unwrap();
        let server=McpServer::new(config.clone(),AgentKind::CliAgent).unwrap();
        let args=serde_json::json!({"workspace_id":workspace.id,"partition":"cpu"});
        assert!(matches!(server.call_tool("workspace_cluster_info",serde_json::json!({"workspace_id":workspace.id,"refresh":"true"}).as_object().unwrap()).await,Err(CoreError::Validation(_))));
        assert!(matches!(server.call_tool("workspace_diagnose_suggest",args.as_object().unwrap()).await,Err(CoreError::InvalidState(_))));
        cluster.nodes[0].cpu_cores=8;
        workspace_store::save_cluster_config(&config,&cluster,workspace.id).unwrap();
        let result=server.call_tool("workspace_diagnose_suggest",args.as_object().unwrap()).await.unwrap();
        assert_eq!(result["recommended"]["max_concurrent_jobs"],1);
        assert_eq!(result["partition_limits"]["cores_per_node"],8);
        cluster.nodes[0].state="down".into();
        workspace_store::save_cluster_config(&config,&cluster,workspace.id).unwrap();
        let result=server.call_tool("workspace_diagnose_suggest",args.as_object().unwrap()).await.unwrap();
        assert_eq!(result["partition_limits"]["nodes_available"],0);
        assert_eq!(result["partition_limits"]["nodes_discovered"],1);
        drop(server);std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn native_images_do_not_duplicate_base64_into_text_and_keep_redaction() {
        let bytes=b"\xff\xd8\xffsynthetic JPEG";
        assert_eq!(image_mime(bytes),Some("image/jpeg"));
        let encoded=lrworkspace_remote::b64::encode(bytes);
        let value=serde_json::json!({"mime_type":"image/jpeg","encoding":"base64","content":encoded});
        let result=tool_result("workspace_read_file",value.clone());
        assert_eq!(result["content"][1]["data"],encoded);
        assert!(!result["content"][0]["text"].as_str().unwrap().contains(&encoded));
        let _secret=lrworkspace_core::privacy::SensitiveString::new(encoded);
        let safe=tool_result("workspace_read_file",value);
        assert_eq!(safe["content"].as_array().unwrap().len(),1);
    }

    #[test]
    fn model_payloads_errors_and_diagnostic_journals_suppress_known_credentials() {
        let secret=lrworkspace_core::privacy::SensitiveString::new("model-output-canary-3451".into());
        let request:JsonRpcRequest=serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"ping"})).unwrap();
        let response=success(&request,serde_json::json!({"stdout":secret.expose(),"exit_code":0}));let wire=serde_json::to_string(&response).unwrap();assert!(!wire.contains(secret.expose()));assert!(wire.contains("REDACTED"));
        let error=tool_rpc_error(&CoreError::Connection(secret.expose().into()));assert!(!error.message.contains(secret.expose()));
        let root=std::env::temp_dir().join(format!("lrw-private-journal-{}",Uuid::new_v4()));let config=LrWorkspaceConfig{data_dir:root.clone(),..Default::default()};let id=Uuid::new_v4();
        write_journal(&config,id,serde_json::json!({"text":secret.expose()})).unwrap();assert!(!std::fs::read_to_string(journal_path(&config,id)).unwrap().contains(secret.expose()));std::fs::remove_dir_all(root).unwrap();
    }


    #[test]
    fn omitted_port_is_not_filled_with_22_by_the_client_schema() {
        let tool=tool_definitions().into_iter().find(|t|t.name=="workspace_configure").unwrap();assert!(tool.input_schema["properties"]["port"].get("default").is_none());
        let request:JsonRpcRequest=serde_json::from_value(serde_json::json!({"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"workspace_configure","arguments":{}}})).unwrap();
        let form=elicitation_for_request(&request).unwrap();assert!(form["requestedSchema"]["properties"]["port"].get("default").is_none());
    }

    #[test]
    fn journal_tail_skips_partial_rows_and_does_not_load_earlier_huge_records() {
        let root=std::env::temp_dir().join(format!("lrw-journal-tail-{}",Uuid::new_v4()));
        let config=LrWorkspaceConfig{data_dir:root.clone(),..Default::default()};let id=Uuid::new_v4();
        let path=journal_path(&config,id);std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut file=std::fs::File::create(&path).unwrap();
        file.write_all(&vec![b'x';17*1024*1024]).unwrap();
        file.write_all(b"\n{\"value\":1}\r\n{\"value\":2}\n{partial").unwrap();drop(file);
        let entries=read_journal(&config,id,2).unwrap();assert_eq!(entries,vec![serde_json::json!({"value":2}),serde_json::json!({"value":1})]);
        assert!(read_journal(&config,id,1001).is_err());std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn command_array_tool_and_resource_options_use_the_user_supplied_contract() {
        let options=batch_options(&serde_json::Map::new()).unwrap();
        assert_eq!((options.nodes,options.ntasks,options.cpu,options.maxnum,options.maxcpu),(1,1,1,1000,2000));
        assert!(options.mem.is_none() && options.time.is_none());
        assert!(batch_options(serde_json::json!({"batch":{"unrecognized":1}}).as_object().unwrap()).is_err());
        for value in [serde_json::json!({}),serde_json::json!({"batch":null}),serde_json::json!({"batch":{}})] {
            assert!(!has_batch_options(value.as_object().unwrap()).unwrap());
            assert!(batch_options(value.as_object().unwrap()).is_ok());
        }
        assert!(has_batch_options(serde_json::json!({"batch":{"cpu":1}}).as_object().unwrap()).unwrap());
        assert!(has_batch_options(serde_json::json!({"batch":false}).as_object().unwrap()).is_err());
        let tools=tool_definitions();let tool=tools.iter().find(|tool|tool.name=="workspace_submit").unwrap();
        assert_eq!(tool.input_schema["properties"]["submit"]["default"],false);
        assert_eq!(tool.input_schema["properties"]["batch"]["properties"]["cpu"]["default"],1);
        let run=tools.iter().find(|tool|tool.name=="workspace_run").unwrap();
        assert!(run.input_schema["properties"].get("batch").is_some());
    }

    #[test]
    fn upload_requires_one_typed_source_and_preserves_empty_content() {
        assert!(matches!(upload_source(serde_json::json!({"content":""}).as_object().unwrap()).unwrap(), UploadSource::Content("")));
        for input in [serde_json::json!({}), serde_json::json!({"content":"x","local_path":"file"}), serde_json::json!({"content":true}), serde_json::json!({"local_path":""})] {
            assert!(upload_source(input.as_object().unwrap()).is_err());
        }
        assert!(matches!(upload_source(serde_json::json!({"local_path":"file with spaces"}).as_object().unwrap()).unwrap(), UploadSource::LocalPath("file with spaces")));
    }

    #[test]
    fn elicitation_queues_interleaved_calls_and_notifications_in_order() {
        let messages = [
            serde_json::json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}),
            serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
            serde_json::json!({"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"workspace_diagnose_list","arguments":{}}}),
            // The client's request namespace may use the same ID as our form.
            serde_json::json!({"jsonrpc":"2.0","id":"form","method":"tools/list"}),
            serde_json::json!({"jsonrpc":"2.0","id":"form","result":{"action":"accept","content":{"confirmed":false}}}),
            serde_json::json!({"jsonrpc":"2.0","id":9,"method":"tools/list"}),
        ];
        let input = messages.iter().map(|value| format!("{value}\n")).collect::<String>();
        let mut reader = std::io::Cursor::new(input.as_bytes());
        let mut pending = VecDeque::new();
        let content = wait_for_elicitation_response(&mut reader, &mut pending, "form")
            .unwrap().unwrap();
        assert_eq!(content.get("confirmed"), Some(&Value::Bool(false)));
        assert_eq!(pending.len(), 4);
        // The actual dispatch reader drains the queue before reading the next
        // physical stdin line. The form response itself is consumed once.
        for expected in messages[..4].iter().chain(messages[5..].iter()) {
            let line = next_protocol_line(&mut reader, &mut pending).unwrap().unwrap();
            assert_eq!(serde_json::from_str::<Value>(&line).unwrap(), *expected);
        }
        assert!(next_protocol_line(&mut reader, &mut pending).unwrap().is_none());
    }

    #[test]
    fn declined_or_missing_elicitation_keeps_pending_requests_without_approval() {
        for response in [
            Some(serde_json::json!({"jsonrpc":"2.0","id":"form","result":{"action":"decline","content":{"confirmed":true}}})),
            Some(serde_json::json!({"jsonrpc":"2.0","id":"form","result":{"action":"cancel"}})),
            Some(serde_json::json!({"jsonrpc":"2.0","id":"form","error":{"code":-32603,"message":"unavailable"}})),
            None,
        ] {
            let request = serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list"});
            let mut input = format!("{request}\n");
            if let Some(response) = response {
                input.push_str(&format!("{response}\n"));
            }
            let mut reader = std::io::Cursor::new(input.as_bytes());
            let mut pending = VecDeque::new();
            assert!(wait_for_elicitation_response(&mut reader, &mut pending, "form")
                .unwrap().is_none());
            let queued = next_protocol_line(&mut reader, &mut pending).unwrap().unwrap();
            assert_eq!(serde_json::from_str::<Value>(&queued).unwrap(), request);
        }
    }

    #[test]
    fn diagnostic_log_tail_preserves_chronological_order() {
        assert_eq!(log_tail("one\ntwo\nthree\n", 2), "two\nthree");
        assert_eq!(log_tail("one\n", 50), "one");
        assert_eq!(log_tail("", 50), "");
    }

    #[test]
    fn absent_scheduler_evidence_is_not_terminal() {
        assert!(!job_is_terminal(&SlurmJobState::Unknown("not found".into())));
        assert!(!job_is_terminal(&SlurmJobState::Pending));
        assert!(!job_is_terminal(&SlurmJobState::Running));
        assert!(job_is_terminal(&SlurmJobState::Completed));
        assert!(job_is_terminal(&SlurmJobState::Failed));
    }

    #[test]
    fn parses_explicit_remote_roots() {
        let mut args = serde_json::Map::new();
        assert_eq!(parse_root(&args).unwrap(), RemoteRootKind::Work);
        args.insert("root".into(), Value::String("data:2".into()));
        assert_eq!(parse_root(&args).unwrap(), RemoteRootKind::Data(2));
        args.insert("root".into(), Value::String("/etc".into()));
        assert!(parse_root(&args).is_err());
    }

    #[test]
    fn removed_sync_tools_are_not_exposed() {
        let names: Vec<String> = tool_definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect();
        for removed in [
            "workspace_sync",
            "workspace_cache_files",
            "workspace_apply_cache_selection",
            "workspace_diff",
        ] {
            assert!(!names.iter().any(|name| name == removed));
        }
    }

    #[test]
    fn shared_agent_instructions_stay_small() {
        assert!(AGENT_INSTRUCTIONS.len() <= 128);
    }

    #[test]
    fn write_tool_accepts_agent_project_files_without_local_registration() {
        let write = tool_definitions()
            .into_iter()
            .find(|tool| tool.name == "workspace_write_file")
            .unwrap();
        assert!(write.input_schema["properties"].get("local_path").is_some());
        assert!(write.input_schema["properties"].get("transport").is_some());
        assert_eq!(write.input_schema["required"], serde_json::json!(["relative_path"]));
        let run = tool_definitions().into_iter().find(|tool| tool.name == "workspace_run").unwrap();
        assert_eq!(run.input_schema["properties"]["force"]["enum"], serde_json::json!(["direct", "cluster", "read-only"]));
        assert_eq!(run.input_schema["properties"]["timeout_sec"]["default"], 120);
        assert_eq!(run.input_schema["properties"]["compute_node"]["type"], "string");
    }

    #[test]
    fn per_request_timeout_can_exceed_the_old_limit_and_rejects_invalid_values() {
        assert_eq!(request_timeout(&serde_json::Map::new(), 120).unwrap(), Duration::from_secs(120));
        assert_eq!(request_timeout(serde_json::json!({"timeout_sec":600}).as_object().unwrap(), 120).unwrap(), Duration::from_secs(600));
        for value in [serde_json::json!(0), serde_json::json!(-1), serde_json::json!("120")] {
            assert!(request_timeout(serde_json::json!({"timeout_sec":value}).as_object().unwrap(), 120).is_err());
        }
    }

    #[test]
    fn setup_tools_support_missing_input_workflows() {
        let names: Vec<String> = tool_definitions()
            .into_iter()
            .map(|tool| tool.name)
            .collect();
        assert!(names.contains(&"workspace_configure".to_string()));
        assert!(names.contains(&"workspace_add_data_dir".to_string()));
        assert!(names.contains(&"workspace_set_node_mode".to_string()));

        let request: JsonRpcRequest = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "workspace_configure", "arguments": {}}
        }))
        .unwrap();
        let elicitation = elicitation_for_request(&request).unwrap();
        assert_eq!(
            elicitation["requestedSchema"]["required"],
            serde_json::json!(["host", "remote_work_dir"])
        );

        let request: JsonRpcRequest = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "workspace_set_node_mode", "arguments": {}}
        }))
        .unwrap();
        let elicitation = elicitation_for_request(&request).unwrap();
        assert_eq!(
            elicitation["requestedSchema"]["properties"]["node_execution_mode"]["enum"],
            serde_json::json!(["submit_only", "single_server", "direct_compute"])
        );
    }

    #[test]
    fn node_execution_mode_is_fail_closed() {
        assert_eq!(
            parse_node_execution_mode("submit_only").unwrap(),
            NodeExecutionMode::SubmitOnly
        );
        assert_eq!(
            parse_node_execution_mode("direct_compute").unwrap(),
            NodeExecutionMode::DirectCompute
        );
        assert!(parse_node_execution_mode("direct").is_err());
    }
}
