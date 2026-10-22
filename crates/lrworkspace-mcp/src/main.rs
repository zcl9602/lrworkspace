//! LrWorkspace MCP server binary — stdio JSON-RPC endpoint for AI agents.
//!
//! ## Usage
//!
//! This binary is **not** run by the user directly. It is registered as an
//! MCP server in the AI agent's config (`.mcp.json` for Claude Code, Cursor
//! MCP settings, etc.). The agent launches it as a subprocess and communicates
//! over stdin/stdout via JSON-RPC 2.0.
//!
//! ## Installation
//!
//! ### Claude Code
//! Add to your project's `.mcp.json`:
//! ```json
//! {
//!   "mcpServers": {
//!     "lrworkspace": {
//!       "command": "/path/to/lrworkspace-mcp",
//!       "env": {
//!         "LRWORKSPACE_DATA_DIR": "/absolute/path/to/state"
//!       }
//!     }
//!   }
//! }
//! ```
//!
//! ### Cursor
//! Settings → MCP → Add Server → Command: `/path/to/lrworkspace-mcp`
//!
//! After installation, the agent automatically discovers these tools:
//! - `workspace_discover` — find the active workspace and next action
//! - `workspace_index` — get project structure
//! - `workspace_read_file` — read the work root or a configured data root
//! - `workspace_write_file` — write directly under the remote work root only
//! - `workspace_run` — execute with a work-root working directory under SSH user permissions
//! - `workspace_submit` — generate/submit shuffled command-file arrays for Slurm/PBS
//! - `workspace_slurm_*` — manage Slurm jobs
//! - `workspace_env` — capture remote environment

use lrworkspace_core::config::LrWorkspaceConfig;
use lrworkspace_core::types::AgentKind;
use lrworkspace_mcp::McpServer;

fn main() {
    lrworkspace_core::privacy::install_panic_redaction();
    let arguments: Vec<_> = std::env::args().skip(1).collect();
    if arguments == ["--version"] {
        println!("lrworkspace-mcp {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if arguments == ["--help"] {
        println!("lrworkspace-mcp [--stdio | --version]\nDefault transport: stdio JSON-RPC");
        return;
    }
    if arguments == ["--private-submit"] {
        let success = lrworkspace_remote::local_auth::submit_private_form().is_ok();
        if success {
            println!("submitted");
        }
        std::process::exit(if success { 0 } else { 1 });
    }
    if arguments.len() == 4 && arguments[0] == "--auth-proxy" {
        let result = arguments[3]
            .parse::<u16>()
            .ok()
            .filter(|port| *port > 0)
            .ok_or(())
            .and_then(|port| {
                lrworkspace_remote::proxy::relay_stdio(
                    &arguments[1],
                    &arguments[2],
                    port,
                    std::time::Duration::from_secs(10),
                )
                .map_err(|_| ())
            });
        std::process::exit(if result.is_ok() { 0 } else { 1 });
    }
    if std::env::var_os("LRW_ASKPASS_ENDPOINT").is_some()
        && arguments.len() == 1
        && !arguments[0].starts_with("--")
    {
        let success = lrworkspace_remote::local_auth::askpass(&arguments[0]).unwrap_or(false);
        std::process::exit(if success { 0 } else { 1 });
    }
    if !arguments.is_empty() && arguments != ["--stdio"] {
        eprintln!("Unknown MCP arguments; use --stdio or no arguments");
        std::process::exit(2);
    }
    run_server();
}

#[tokio::main(worker_threads = 2)]
async fn run_server() {
    tracing_subscriber::fmt()
        .with_writer(lrworkspace_core::privacy::RedactingStderr::default)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .init();

    let config = match LrWorkspaceConfig::load() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("Failed to load config: {}", e);
            std::process::exit(1);
        }
    };

    // Agent identity is set during MCP initialize from clientInfo
    let agent = AgentKind::CliAgent; // default, overridden by initialize

    let server = match McpServer::new(config, agent) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to initialize MCP server: {}", e);
            std::process::exit(1);
        }
    };

    if let Err(e) = server.run().await {
        eprintln!("MCP server error: {}", e);
        std::process::exit(1);
    }
}
