// LrWorkspace CLI — remote-first workspace metadata, access, and execution.

use clap::{Parser, Subcommand};
use dialoguer::Confirm;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use lrworkspace_audit::AuditLoggerImpl;
use lrworkspace_core::config::LrWorkspaceConfig;
use lrworkspace_core::error::{CoreError, CoreResult};
use lrworkspace_core::permission::{classify_command_risk, DefaultPolicy, RiskLevel};
use lrworkspace_core::traits::{
    AuditLogger, ClusterDetector, ProjectIndexer, RemoteExecutor, RemoteFileAccess, SlurmManager,
};
use lrworkspace_core::types::*;
use lrworkspace_core::workspace_store;
use lrworkspace_remote::{
    resolve_execution_route, ClusterDetectorImpl, ExecutionRoute, ProjectIndexerImpl,
    RemoteExecutorImpl, SshConfig, TransferPreference,
};
use lrworkspace_slurm::{PbsManagerImpl, SlurmManagerImpl};
use lrworkspace_slurm::batch::{submit_batch,tracked_batch_logs};
use lrworkspace_remote::batch::{BatchOptions,BatchCommand,PbsFlavor,read_command_file,parse_command_file,MAX_INPUT_BYTES};
use uuid::Uuid;

mod mcp_install;

#[derive(Parser)]
#[command(version, about = "Remote-first AI workspace with no cache or sync")]
struct Cli {
    #[arg(short, long, env = "LRWORKSPACE_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, env = "LRWORKSPACE_DATA_DIR")]
    data_dir: Option<PathBuf>,
    /// Command and transfer timeout in seconds (default 120; overrides config/env).
    #[arg(long, global = true)]
    timeout_secs: Option<u64>,
    #[arg(short, long)]
    verbose: bool,
    #[command(subcommand)]
    command: Commands,
}

#[derive(clap::Args, Default)]
struct BatchArgs {
    #[arg(long)] outdir: Option<PathBuf>,
    #[arg(long)] partition: Option<String>,
    #[arg(long)] queue: Option<String>,
    #[arg(long)] nodes: Option<u32>,
    #[arg(long)] ntasks: Option<u32>,
    #[arg(long)] cpu: Option<u32>,
    #[arg(long)] maxnum: Option<u32>,
    #[arg(long)] maxcpu: Option<u64>,
    #[arg(long)] jobname: Option<String>,
    #[arg(long)] time: Option<String>,
    #[arg(long)] mem: Option<String>,
    #[arg(long)] shuffle_seed: Option<u64>,
    #[arg(long, value_parser=["pbs-pro","torque"])] pbs_flavor: Option<String>,
}
impl BatchArgs {
    fn options(&self)->CoreResult<BatchOptions> {
        let mut result=BatchOptions::default();
        if let Some(value)=&self.outdir {result.outdir=value.clone();}
        result.partition=self.partition.clone();result.queue=self.queue.clone();
        if let Some(value)=self.nodes {result.nodes=value;}
        if let Some(value)=self.ntasks {result.ntasks=value;}
        if let Some(value)=self.cpu {result.cpu=value;}
        if let Some(value)=self.maxnum {result.maxnum=value;}
        if let Some(value)=self.maxcpu {result.maxcpu=value;}
        if let Some(value)=&self.jobname {result.jobname=value.clone();}
        result.time=self.time.clone();result.mem=self.mem.clone();result.shuffle_seed=self.shuffle_seed;
        result.pbs_flavor=self.pbs_flavor.as_deref().map(PbsFlavor::parse).transpose()?;
        Ok(result)
    }
    fn configured(&self)->bool {
        self.outdir.is_some()||self.partition.is_some()||self.queue.is_some()||self.nodes.is_some()||self.ntasks.is_some()||self.cpu.is_some()||self.maxnum.is_some()||self.maxcpu.is_some()||self.jobname.is_some()||self.time.is_some()||self.mem.is_some()||self.shuffle_seed.is_some()||self.pbs_flavor.is_some()
    }
}

#[derive(Subcommand)]
enum Commands {
    /// Check this computer's tools/configuration without contacting servers.
    Doctor { #[arg(long)] workspace: Option<String> },
    /// Native no-auth SOCKS5 ProxyCommand; binary data only on stdout.
    Proxy {
        #[arg(long)] proxy: String,
        #[arg(long)] host: String,
        #[arg(long)] port: u16,
        #[arg(long,default_value_t=10)] connect_timeout: u64,
    },
    /// Register a remote work root and optional read-only data roots.
    #[command(alias = "i")]
    Init {
        #[arg(long)]
        host: String,
        #[arg(long)]
        port: Option<u16>,
        #[arg(long)]
        user: Option<String>,
        #[arg(long = "remote-work-dir", alias = "remote-root")]
        remote_work_dir: PathBuf,
        #[arg(long = "remote-data-dir")]
        remote_data_dirs: Vec<PathBuf>,
        #[arg(long)]
        key: Option<PathBuf>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        display_name: Option<String>,
        #[arg(long, default_value_t = false)]
        yes: bool,
        #[arg(long, default_value_t = false)]
        skip_cluster: bool,
        #[arg(
            long = "node-mode",
            value_parser = ["submit-only", "single-server", "direct-compute"],
            help = "Persist workload behavior for this SSH node; omitted values reuse an existing decision or prompt once"
        )]
        node_mode: Option<String>,
    },
    /// List or inspect registered workspaces.
    #[command(alias = "ws")]
    Workspace {
        #[arg(long)]
        id: Option<String>,
        /// Explicitly repair the saved SSH port; original registration is backed up.
        #[arg(long)]
        port: Option<u16>,
        #[arg(long, default_value_t = false)]
        authorize: bool,
        #[arg(
            long = "node-mode",
            value_parser = ["submit-only", "single-server", "direct-compute"]
        )]
        node_mode: Option<String>,
    },
    /// Validate connection and workspace directory roles.
    #[command(alias = "re")]
    Resume {
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Index a remote work or data root.
    #[command(alias = "ix")]
    Index {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long, default_value = "work")]
        root: String,
    },
    /// Read a file directly from a remote work or data root.
    Read {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long, default_value = "work")]
        root: String,
        #[arg(long)]
        file: PathBuf,
    },
    /// Write a file directly under remote_work_dir.
    Write {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        file: PathBuf,
        #[arg(long, conflicts_with = "from")]
        content: Option<String>,
        #[arg(long, conflicts_with = "content")]
        from: Option<PathBuf>,
        #[arg(long, default_value="auto", value_parser=["auto", "rsync", "scp", "base64"])]
        transport: String,
    },
    /// Run under SSH user permissions with working_dir inside remote_work_dir.
    /// Pass `-` as the command to read it from stdin byte-exact (avoids
    /// Windows PowerShell stripping double quotes from argv).
    #[command(alias = "ru")]
    Run {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long, help = "Execution route: cluster, direct, or read-only", value_parser = clap::builder::PossibleValuesParser::new([
            clap::builder::PossibleValue::new("direct"), clap::builder::PossibleValue::new("cluster"),
            clap::builder::PossibleValue::new("read-only"), clap::builder::PossibleValue::new("compute-node").hide(true),
        ]))]
        force: Option<String>,
        /// Explicit permission to SSH from the login host to this compute node.
        #[arg(long)]
        compute_node: Option<String>,
        #[arg(long)]
        working_dir: Option<PathBuf>,
        #[command(flatten)]
        batch: BatchArgs,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Generate a shuffled Slurm/PBS command array; --submit launches it.
    Submit {
        #[arg(long)] workspace: Option<String>,
        #[arg(long="in")] input: PathBuf,
        #[arg(long)] working_dir: Option<PathBuf>,
        #[command(flatten)] batch: BatchArgs,
        #[arg(long)] submit: bool,
    },
    /// Run a read-only command on the login node (index, search, logs, env).
    /// Never runs on compute nodes; safe for submit_only login nodes.
    #[command(alias = "ro")]
    ReadOnly {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        working_dir: Option<PathBuf>,
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        command: Vec<String>,
    },
    /// Capture remote environment information.
    #[command(alias = "ev")]
    Env {
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Check SSH and workspace layout.
    #[command(alias = "ck")]
    Check {
        #[arg(long)]
        workspace: Option<String>,
    },
    /// Show all registered workspaces.
    #[command(alias = "ss")]
    Status {
        #[arg(long, default_value_t = false)]
        live: bool,
        #[arg(long, default_value_t = false)]
        json: bool,
    },
    #[command(subcommand, alias = "cl")]
    Cluster(ClusterCommand),
    #[command(subcommand, alias = "sl")]
    Slurm(SlurmCommand),
    /// Read audit records.
    #[command(alias = "ad")]
    Audit {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long, default_value = "50")]
        limit: usize,
    },
    #[command(subcommand, alias = "mc")]
    Mcp(McpCommand),
    /// Remove LRW metadata; remote and local directories are preserved.
    #[command(alias = "del")]
    Remove {
        #[arg(long)]
        workspace: Option<String>,
    },
}

#[derive(Subcommand)]
enum ClusterCommand {
    Info {
        #[arg(long)]
        workspace: Option<String>,
    },
    Refresh {
        #[arg(long)]
        workspace: Option<String>,
    },
}

#[derive(Subcommand)]
enum SlurmCommand {
    Status {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        job_id: String,
    },
    Logs {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        job_id: String,
    },
    Cancel {
        #[arg(long)]
        workspace: Option<String>,
        #[arg(long)]
        job_id: String,
    },
}

#[derive(Subcommand)]
enum McpCommand {
    Install {
        // Legacy spelling is accepted; installation is always user-scoped.
        #[arg(long, hide=true)]
        global: bool,
        #[arg(long,default_value="codex",value_parser=["codex","cursor","all"])]
        client: String,
        #[arg(long)] install_dir: Option<PathBuf>,
    },
    Show,
}

struct App {
    config: LrWorkspaceConfig,
    executor: Arc<RemoteExecutorImpl>,
    cluster_detector: Arc<ClusterDetectorImpl>,
    indexer: Arc<ProjectIndexerImpl>,
    slurm: Arc<SlurmManagerImpl>,
    pbs: Arc<PbsManagerImpl>,
    audit: Arc<AuditLoggerImpl>,
}

impl App {
    fn new(config: LrWorkspaceConfig) -> Self {
        let executor = Arc::new(RemoteExecutorImpl::new(Duration::from_secs(
            config.ssh_timeout_secs,
        )));
        Self {
            cluster_detector: Arc::new(ClusterDetectorImpl::new(executor.clone())),
            indexer: Arc::new(ProjectIndexerImpl::new(executor.clone(), config.clone())),
            slurm: Arc::new(SlurmManagerImpl::new(executor.clone())),
            pbs: Arc::new(PbsManagerImpl::new(executor.clone())),
            audit: Arc::new(AuditLoggerImpl::new(&config)),
            executor,
            config,
        }
    }

    async fn job_logs(&self,workspace:&Workspace,job_id:&str,scheduler:ClusterScheduler)->CoreResult<(String,String)> {
        if let Some(logs)=tracked_batch_logs(&self.executor,&self.config,workspace,job_id).await? {return Ok(logs);}
        match scheduler {
            ClusterScheduler::Slurm=>self.slurm.fetch_logs(workspace,job_id).await,
            ClusterScheduler::Pbs=>self.pbs.fetch_logs(workspace,job_id).await,
            _=>Err(CoreError::InvalidState("Logs require Slurm/PBS".into())),
        }
    }

    fn workspace(&self, id: Option<&str>) -> CoreResult<Workspace> {
        workspace_store::resolve_workspace(&self.config, id)
    }
}

#[tokio::main]
async fn main() {
    lrworkspace_core::privacy::install_panic_redaction();
    let code = match run().await {
        Ok(code) => code,
        Err(error) => {
            eprintln!("Error: {}",lrworkspace_core::privacy::redact_text(&error.to_string()));
            1
        }
    };
    std::process::exit(code);
}

async fn run() -> CoreResult<i32> {
    let cli = Cli::parse();
    if let Commands::Proxy{proxy,host,port,connect_timeout}=&cli.command {
        lrworkspace_remote::proxy::relay_stdio(proxy,host,*port,std::time::Duration::from_secs(*connect_timeout))?;
        return Ok(0);
    }
    tracing_subscriber::fmt()
        .with_env_filter(if cli.verbose { "debug" } else { "warn" })
        .with_writer(lrworkspace_core::privacy::RedactingStderr::default)
        .init();
    let mut config =
        LrWorkspaceConfig::load_with_overrides(cli.config.as_deref(), cli.data_dir.as_deref())?;
    if let Some(timeout) = cli.timeout_secs {
        if timeout == 0 { return Err(CoreError::Validation("--timeout-secs must be positive".into())); }
        config.ssh_timeout_secs = timeout;
    }
    let app = App::new(config);

    match cli.command {
        Commands::Doctor{workspace}=> {
            let workspace=workspace.as_deref().map(|id|app.workspace(Some(id))).transpose()?;
            let report=lrworkspace_remote::doctor::environment_report(&app.config,workspace.as_ref());
            println!("{}",serde_json::to_string_pretty(&report)?);
            return Ok(if report["base_ready"]==true {0}else{1});
        }
        Commands::Proxy{..}=>unreachable!("proxy handled before configuration loading"),
        Commands::Init {
            host,
            port,
            user,
            remote_work_dir,
            remote_data_dirs,
            key,
            name,
            display_name,
            yes,
            skip_cluster,
            node_mode,
        } => {
            let effective=SshConfig::effective(&host,user.as_deref(),port)?;
            let port=effective.port.expect("resolved port");
            let user=effective.user.expect("resolved user");
            let name = name.unwrap_or_else(|| {
                remote_work_dir
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("remote-workspace")
                    .to_string()
            });
            let workspace_id = Uuid::new_v4();
            let authorized = yes
                || Confirm::new()
                    .with_prompt("Authorize MCP access to this remote workspace?")
                    .default(false)
                    .interact()
                    .unwrap_or(false);
            let mut workspace = Workspace {
                id: workspace_id,
                name: name.clone(),
                display_name: display_name.unwrap_or(name),
                connection: ConnectionConfig {
                    host,
                    port,
                    user,
                    auth: key
                        .map(|path| AuthMethod::Key {
                            path,
                            passphrase: None,
                        })
                        .unwrap_or(AuthMethod::Agent),
                    jump_hosts: Vec::new(),
                },
                remote_work_dir,
                    remote_data_dirs,
                node_execution_mode: NodeExecutionMode::Unconfirmed,
                authorized,
                created_at: chrono::Utc::now(),
                last_accessed_at: chrono::Utc::now(),
                file_threshold_bytes: app.config.file_threshold_bytes,
                config: Default::default(),
            };
            if !app.executor.check_connection(&workspace).await? {
                return Err(CoreError::Connection("SSH connection failed".into()));
            }
            let report = app.executor.inspect_workspace_layout(&workspace).await?;
            if !report.valid {
                return Err(CoreError::Validation(format!(
                    "Invalid workspace directory layout: {}",
                    report.errors.join("; ")
                )));
            }
            let cluster = if skip_cluster {
                None
            } else {
                app.cluster_detector.discover_cluster(&workspace).await.ok()
            };
            let known_mode = workspace_store::list_workspaces(&app.config)?
                .into_iter()
                .find(|existing| {
                    same_node(&existing.connection, &workspace.connection)
                        && existing.node_execution_mode != NodeExecutionMode::Unconfirmed
                })
                .map(|existing| existing.node_execution_mode);
            workspace.node_execution_mode = if let Some(value) = node_mode.as_deref() {
                parse_cli_node_mode(value)?
            } else if let Some(mode) = known_mode {
                mode
            } else if yes {
                NodeExecutionMode::SubmitOnly
            } else {
                let scheduler = cluster
                    .as_ref()
                    .map(|value| format!("{:?}", value.scheduler))
                    .unwrap_or_else(|| "not detected".into());
                let direct = Confirm::new()
                    .with_prompt(format!(
                        "Scheduler: {scheduler}. Is this a standalone server where workloads may run directly (not a cluster login node)?"
                    ))
                    .default(false)
                    .interact()
                    .unwrap_or(false);
                if direct {
                    NodeExecutionMode::DirectCompute
                } else {
                    NodeExecutionMode::SubmitOnly
                }
            };
            workspace_store::save_workspace(&app.config, &workspace)?;
            if let Some(cluster) = &cluster {
                workspace_store::save_cluster_config(&app.config, cluster, workspace.id)?;
            }
            println!("Workspace registered: {}", workspace.id);
            println!("  remote_work_dir: {}", workspace.remote_work_dir.display());
            println!("  node_execution_mode: {:?}", workspace.node_execution_mode);
            for (index, data) in workspace.remote_data_dirs.iter().enumerate() {
                println!(
                    "  remote_data_dir[{index}]: {} (data root; MCP file writes rejected)",
                    data.display()
                );
            }
        }
        Commands::Workspace {
            id,
            port,
            authorize,
            node_mode,
        } => {
            if let Some(id) = id {
                let mut workspace = app.workspace(Some(&id))?;
                if let Some(port)=port {let backup=workspace_store::update_connection_port(&app.config,&mut workspace,port)?;println!("Connection port updated; backup={}",backup.map(|p|p.display().to_string()).unwrap_or_else(||"unchanged".into()));}
                if authorize {
                    workspace.authorized = true;
                }
                if let Some(value) = node_mode.as_deref() {
                    let mode = parse_cli_node_mode(value)?;
                    for mut candidate in workspace_store::list_workspaces(&app.config)? {
                        if same_node(&candidate.connection, &workspace.connection) {
                            candidate.node_execution_mode = mode;
                            workspace_store::save_workspace(&app.config, &candidate)?;
                        }
                    }
                    workspace.node_execution_mode = mode;
                }
                workspace_store::save_workspace(&app.config, &workspace)?;
                print_workspace(&workspace);
            } else {
                if node_mode.is_some() || authorize || port.is_some() {
                    return Err(CoreError::Validation("--node-mode, --port and --authorize require --id".into()));
                }
                for workspace in workspace_store::list_workspaces(&app.config)? {
                    print_workspace(&workspace);
                }
            }
        }
        Commands::Resume { workspace } | Commands::Check { workspace } => {
            let workspace = app.workspace(workspace.as_deref())?;
            let mut status = app.executor.check_health(&workspace).await?;
            if status.connected {
                let report = app.executor.inspect_workspace_layout(&workspace).await?;
                status.layout_valid = report.valid;
                status.layout_error = (!report.valid).then(|| report.errors.join("; "));
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
            workspace_store::write_status_file(&app.config, workspace.id, &status)?;
            return Ok(if status.connected && status.layout_valid { 0 } else { 1 });
        }
        Commands::Index { workspace, root } => {
            let workspace = app.workspace(workspace.as_deref())?;
            let index = app
                .indexer
                .index_root(&workspace, parse_root(&root)?)
                .await?;
            println!("{}", serde_json::to_string_pretty(&index)?);
        }
        Commands::Read {
            workspace,
            root,
            file,
        } => {
            let mut workspace = app.workspace(workspace.as_deref())?;
            workspace.file_threshold_bytes = workspace.file_threshold_bytes
                .min(app.config.file_threshold_bytes);
            let bytes = app
                .executor
                .read_remote_file(&workspace, parse_root(&root)?, &file)
                .await?;
            if bytes.len() as u64 > app.config.file_threshold_bytes {
                return Err(CoreError::Validation(format!(
                    "File exceeds read limit: {} bytes",
                    bytes.len()
                )));
            }
            match String::from_utf8(bytes) {
                Ok(text) => print!("{text}"),
                Err(error) => println!("{}", lrworkspace_remote::b64::encode(error.as_bytes())),
            }
        }
        Commands::Write {
            workspace,
            file,
            content,
            from,
            transport,
        } => {
            let workspace = app.workspace(workspace.as_deref())?;
            let preference = TransferPreference::parse(&transport)?;
            let method = match (content, from) {
                (Some(content), None) => app.executor.upload_work_file_bytes_with_transport(&workspace, &file, content.as_bytes(), preference).await?,
                (None, Some(path)) => app.executor.upload_local_file(&workspace, &file, &path, preference).await?,
                _ => return Err(CoreError::Validation("Pass exactly one of --content or --from".into())),
            };
            app.audit
                .record(
                    workspace.id,
                    &AgentKind::CliAgent,
                    "write_work_file",
                    RiskLevel::WorkspaceWrite,
                    &file.display().to_string(),
                    "written directly to remote_work_dir",
                )
                .await?;
            println!("Written to remote_work_dir: {} (transport={})", file.display(), method.as_str());
        }
        Commands::Run {
            workspace,
            force,
            compute_node,
            working_dir,
            batch,
            command,
        } => {
            let workspace = app.workspace(workspace.as_deref())?;
            let mut command = command.join(" ");
            if command == "-" {
                let mut stdin_buf = String::new();
                std::io::stdin().read_to_string(&mut stdin_buf)?;
                command = stdin_buf;
            }
            // Strip a leading UTF-8 BOM (PowerShell pipes add one), then
            // normalize Windows CRLF; bash scripts must be LF.
            let command = command
                .trim_start_matches('\u{feff}')
                .replace("\r\n", "\n")
                .replace('\r', "\n");
            if command.trim().is_empty() {
                return Err(CoreError::Validation("Command is required".into()));
            }
            let risk = classify_command_risk(&command);
            if matches!(
                risk.default_policy(),
                DefaultPolicy::Confirm | DefaultPolicy::Deny
            ) {
                return Err(CoreError::PermissionDenied(format!(
                    "Workload run refused {} command",
                    risk.label()
                )));
            }
            if batch.configured() && resolve_execution_route(workspace.node_execution_mode,force.as_deref())?!=ExecutionRoute::Cluster {
                return Err(CoreError::Validation("Batch resources require --force cluster or a submit-only workspace".into()));
            }
            lrworkspace_remote::batch::validate_commands(&[BatchCommand {source_line:1,command:command.clone()}])?;
            lrworkspace_remote::compute_ssh::validate_compute_node(resolve_execution_route(workspace.node_execution_mode,force.as_deref())?,compute_node.as_deref(),batch.configured())?;
            let options=batch.options()?;
            let mut value = run_guarded(
                &app,
                &workspace,
                &command,
                working_dir.as_deref(),
                force.as_deref(),
                compute_node.as_deref(),
                &options,
            )
            .await?;
            let audit_command = command.chars().take(500).collect::<String>();
            let audit=app.audit
                .record(
                    workspace.id,
                    &AgentKind::CliAgent,
                    "cli_workload_run",
                    risk,
                    &audit_command,
                    &format!("executed via rw run; execution_path={}; compute_node={}",value["execution_path"],value["compute_node"]),
                )
                .await;
            if let Err(error)=audit {value["audit_warning"]=serde_json::json!(format!("Operation completed, but audit recording failed: {error}; do not blindly replay"));}
            println!("{}", serde_json::to_string_pretty(&value)?);
            return Ok(run_exit_code(&value));
        }
        Commands::Submit {workspace,input,working_dir,batch,submit} => {
            let workspace=app.workspace(workspace.as_deref())?;
            let commands=if input==Path::new("-") {
                let mut text=String::new();std::io::stdin().take(MAX_INPUT_BYTES+1).read_to_string(&mut text)?;
                parse_command_file(&text)?
            } else {read_command_file(&input)?};
            let cluster=match workspace_store::load_cluster_config(&app.config,workspace.id) {
                Ok(cluster)=>cluster,
                Err(_)=>{let cluster=app.cluster_detector.discover_cluster(&workspace).await?;workspace_store::save_cluster_config(&app.config,&cluster,workspace.id)?;cluster},
            };
            let mut result=submit_batch(app.executor.clone(),&app.config,&workspace,&cluster,&commands,&batch.options()?,working_dir.as_deref(),submit,&AgentKind::CliAgent).await?;
            if let Err(error)=app.audit.record(workspace.id,&AgentKind::CliAgent,"cli_batch_submit",RiskLevel::RemoteExecute,&result.batch.run_directory,&result.status).await {
                result.warning=Some(format!("Operation completed; audit failed: {error}. {:?}",result.warning));
            }
            println!("{}",serde_json::to_string_pretty(&result)?);
        }
        Commands::ReadOnly { workspace, working_dir, command } => {
            let workspace = app.workspace(workspace.as_deref())?;
            let mut command = command.join(" ");
            if command == "-" {
                let mut stdin_buf = String::new();
                std::io::stdin().read_to_string(&mut stdin_buf)?;
                command = stdin_buf;
            }
            let command = command
                .trim_start_matches('\u{feff}')
                .replace("\r\n", "\n")
                .replace('\r', "\n");
            if command.trim().is_empty() {
                return Err(CoreError::Validation("Command is required".into()));
            }
            let risk = classify_command_risk(&command);
            if risk != RiskLevel::ReadOnly {
                return Err(CoreError::PermissionDenied(format!(
                    "ReadOnly command refused: {} is {} (only L0 ReadOnly allowed)",
                    command,
                    risk.label()
                )));
            }
            let record = app
                .executor
                .execute_read_only(&workspace, &command, working_dir.as_deref(), &Default::default())
                .await?;
            let audit_command = command.chars().take(500).collect::<String>();
            app.audit
                .record(
                    workspace.id,
                    &AgentKind::CliAgent,
                    "cli_readonly_run",
                    risk,
                    &audit_command,
                    "executed via rw ro (read-only on login node)",
                )
                .await?;
            println!("{}", serde_json::to_string_pretty(&serde_json::json!({
                "mode": "read-only",
                "result": record
            }))?);
            return Ok(remote_exit_code(record.exit_code));
        }
        Commands::Env { workspace } => {
            let workspace = app.workspace(workspace.as_deref())?;
            let snapshot = app.executor.capture_env(&workspace).await?;
            workspace_store::save_env_snapshot(&app.config, &snapshot)?;
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        }
        Commands::Status { live, json } => {
            let mut values = Vec::new();
            for workspace in workspace_store::list_workspaces(&app.config)? {
                let value = if live {
                    serde_json::to_value(app.executor.check_health(&workspace).await?)?
                } else {
                    serde_json::json!({
                        "workspace_id": workspace.id,
                        "name": workspace.display_name,
                        "remote_host": workspace.connection.host,
                        "remote_work_dir": workspace.remote_work_dir,
                        "remote_data_dirs": workspace.remote_data_dirs,
                        "node_execution_mode": workspace.node_execution_mode,
                        "authorized": workspace.authorized,
                    })
                };
                values.push(value);
            }
            if json {
                println!("{}", serde_json::to_string_pretty(&values)?);
            } else {
                for value in values {
                    println!("{}", serde_json::to_string_pretty(&value)?);
                }
            }
        }
        Commands::Cluster(command) => match command {
            ClusterCommand::Info { workspace } => {
                let workspace = app.workspace(workspace.as_deref())?;
                let cluster = workspace_store::load_cluster_config(&app.config, workspace.id)?;
                println!("{}", serde_json::to_string_pretty(&cluster)?);
            }
            ClusterCommand::Refresh { workspace } => {
                let workspace = app.workspace(workspace.as_deref())?;
                let cluster = app.cluster_detector.discover_cluster(&workspace).await?;
                workspace_store::save_cluster_config(&app.config, &cluster, workspace.id)?;
                println!("{}", serde_json::to_string_pretty(&cluster)?);
            }
        },
        Commands::Slurm(command) => match command {
            SlurmCommand::Status { workspace, job_id } => {
                let workspace = app.workspace(workspace.as_deref())?;
                let cluster = workspace_store::load_cluster_config(&app.config, workspace.id)?;
                let state = match cluster.scheduler {
                    ClusterScheduler::Slurm => app.slurm.status(&workspace, &job_id).await?,
                    ClusterScheduler::Pbs => app.pbs.status(&workspace, &job_id).await?,
                    other => {
                        return Err(CoreError::InvalidState(format!(
                            "job status is implemented only for Slurm and PBS, got {:?}",
                            other
                        )))
                    }
                };
                println!("{:?}", state);
            }
            SlurmCommand::Logs { workspace, job_id } => {
                let workspace = app.workspace(workspace.as_deref())?;
                let cluster = workspace_store::load_cluster_config(&app.config, workspace.id)?;
                let (stdout, stderr) = match cluster.scheduler {
                    ClusterScheduler::Slurm => app.job_logs(&workspace,&job_id,ClusterScheduler::Slurm).await?,
                    ClusterScheduler::Pbs => app.job_logs(&workspace,&job_id,ClusterScheduler::Pbs).await?,
                    other => {
                        return Err(CoreError::InvalidState(format!(
                            "job logs are implemented only for Slurm and PBS, got {:?}",
                            other
                        )))
                    }
                };
                println!("--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}");
            }
            SlurmCommand::Cancel { workspace, job_id } => {
                let workspace = app.workspace(workspace.as_deref())?;
                let cluster = workspace_store::load_cluster_config(&app.config, workspace.id)?;
                match cluster.scheduler {
                    ClusterScheduler::Slurm => app.slurm.cancel(&workspace, &job_id).await?,
                    ClusterScheduler::Pbs => app.pbs.cancel(&workspace, &job_id).await?,
                    other => {
                        return Err(CoreError::InvalidState(format!(
                            "job cancel is implemented only for Slurm and PBS, got {:?}",
                            other
                        )))
                    }
                }
                println!("Cancelled {job_id}");
            }
        },
        Commands::Audit { workspace, limit } => {
            let workspace = app.workspace(workspace.as_deref())?;
            let records = app.audit.query(workspace.id, limit, 0).await?;
            println!("{}", serde_json::to_string_pretty(&records)?);
        }
        Commands::Mcp(command) => match command {
            McpCommand::Install { global: _,client,install_dir } => {
                mcp_install::install_mcp(app.config.ssh_timeout_secs,&client,&app.config.data_dir,install_dir.as_deref())?;
            }
            McpCommand::Show => mcp_install::show_mcp_config(),
        },
        Commands::Remove { workspace } => {
            let workspace = app.workspace(workspace.as_deref())?;
            workspace_store::delete_workspace(&app.config, workspace.id)?;
            println!("Removed LRW metadata for {}", workspace.id);
            println!("Remote and local directories were not deleted.");
        }
    }
    Ok(0)
}

async fn run_guarded(
    app: &App,
    workspace: &Workspace,
    command: &str,
    working_dir: Option<&Path>,
    force: Option<&str>,
    compute_node: Option<&str>,
    options: &BatchOptions,
) -> CoreResult<serde_json::Value> {
    let route = resolve_execution_route(workspace.node_execution_mode, force)?;
    if route == ExecutionRoute::ReadOnly {
        if classify_command_risk(command) != RiskLevel::ReadOnly {
            return Err(CoreError::PermissionDenied("Read-only execution requires an allowlisted command".into()));
        }
        let record = app.executor.execute_read_only(workspace, command, working_dir, &Default::default()).await?;
        return Ok(serde_json::json!({"mode":"read-only", "result":record}));
    }
    if route == ExecutionRoute::Cluster {
        if let Some(node)=compute_node {
            let record=app.executor.execute_compute_ssh(workspace,node,command,working_dir).await?;
            return Ok(serde_json::json!({"mode":"cluster","execution_path":"ssh","compute_node":node,"result":record}));
        }
        let cluster = match workspace_store::load_cluster_config(&app.config, workspace.id) {
            Ok(cluster) => cluster,
            Err(_) => {
                let cluster = app.cluster_detector.discover_cluster(workspace).await?;
                workspace_store::save_cluster_config(&app.config, &cluster, workspace.id)?;
                cluster
            }
        };
        let commands=[BatchCommand {source_line:1,command:command.into()}];
        let result=submit_batch(app.executor.clone(),&app.config,workspace,&cluster,&commands,options,working_dir,true,&AgentKind::CliAgent).await?;
        let mut result=serde_json::to_value(result)?;result["mode"]=serde_json::json!("cluster");result["execution_path"]=serde_json::json!("submit");Ok(result)
    } else {
        let record = app
            .executor
            .execute_mcp_guarded(workspace, command, working_dir, &Default::default())
            .await?;
        Ok(serde_json::json!({"mode": "direct", "result": record}))
    }
}

fn remote_exit_code(code: Option<i32>) -> i32 {
    code.filter(|code| (0..=255).contains(code)).unwrap_or(1)
}

fn run_exit_code(value: &serde_json::Value) -> i32 {
    if value.get("mode").and_then(serde_json::Value::as_str) == Some("cluster") && value.get("execution_path").and_then(serde_json::Value::as_str) != Some("ssh") {
        // Submission success says nothing about the eventual job outcome.
        0
    } else {
        remote_exit_code(value.pointer("/result/exit_code")
            .and_then(serde_json::Value::as_i64)
            .and_then(|code| i32::try_from(code).ok()))
    }
}

fn parse_root(value: &str) -> CoreResult<RemoteRootKind> {
    if value == "work" {
        return Ok(RemoteRootKind::Work);
    }
    let index = value
        .strip_prefix("data:")
        .ok_or_else(|| CoreError::Validation("root must be work or data:N".into()))?
        .parse::<usize>()
        .map_err(|_| CoreError::Validation("data root index must be an integer".into()))?;
    Ok(RemoteRootKind::Data(index))
}

fn parse_cli_node_mode(value: &str) -> CoreResult<NodeExecutionMode> {
    match value {
        "submit-only" => Ok(NodeExecutionMode::SubmitOnly),
        "direct-compute" | "single-server" => Ok(NodeExecutionMode::DirectCompute),
        _ => Err(CoreError::Validation(
            "--node-mode must be submit-only or single-server (legacy: direct-compute)".into(),
        )),
    }
}

fn same_node(left: &ConnectionConfig, right: &ConnectionConfig) -> bool {
    left.host.eq_ignore_ascii_case(&right.host)
        && left.port == right.port
        && left.user == right.user
}

fn print_workspace(workspace: &Workspace) {
    println!("{}  {}", workspace.id, workspace.display_name);
    println!(
        "  remote: {}@{}",
        workspace.connection.user, workspace.connection.host
    );
    println!("  remote_work_dir: {}", workspace.remote_work_dir.display());
    println!("  node_execution_mode: {:?}", workspace.node_execution_mode);
    for (index, data) in workspace.remote_data_dirs.iter().enumerate() {
        println!(
            "  remote_data_dir[{index}]: {} (data root; MCP file writes rejected)",
            data.display()
        );
    }
    println!("  authorized: {}", workspace.authorized);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_accepts_a_per_operation_timeout_before_the_command() {
        let cli = Cli::try_parse_from(["rw", "run", "--force", "cluster", "--timeout-secs", "300", "--", "python", "job.py"]).unwrap();
        assert_eq!(cli.timeout_secs, Some(300));
        match cli.command {
            Commands::Run { force, command, .. } => {
                assert_eq!(force.as_deref(), Some("cluster"));
                assert_eq!(command, vec!["python", "job.py"]);
            }
            _ => panic!("unexpected command"),
        }
    }

    #[test]
    fn remote_failures_are_not_successful_cli_exits() {
        for mode in ["direct", "read-only", "compute-node"] {
            assert_eq!(run_exit_code(&serde_json::json!({"mode": mode, "result": {"exit_code": 7}})), 7);
            assert_eq!(run_exit_code(&serde_json::json!({"mode": mode, "result": {"exit_code": null}})), 1);
            assert_eq!(run_exit_code(&serde_json::json!({"mode": mode, "result": {"exit_code": 0}})), 0);
        }
        assert_eq!(run_exit_code(&serde_json::json!({"mode": "cluster", "job": {}})), 0);
        assert_eq!(remote_exit_code(Some(256)), 1);
        assert_eq!(remote_exit_code(Some(-1)), 1);
    }

    #[test]
    fn explicit_root_parser_has_no_arbitrary_path_mode() {
        assert_eq!(parse_root("work").unwrap(), RemoteRootKind::Work);
        assert_eq!(parse_root("data:0").unwrap(), RemoteRootKind::Data(0));
        assert!(parse_root("/remote/other").is_err());
    }

    #[test]
    fn parses_cli_node_modes() {
        assert_eq!(
            parse_cli_node_mode("submit-only").unwrap(),
            NodeExecutionMode::SubmitOnly
        );
        assert_eq!(
            parse_cli_node_mode("direct-compute").unwrap(),
            NodeExecutionMode::DirectCompute
        );
        assert!(parse_cli_node_mode("direct").is_err());
    }
}
