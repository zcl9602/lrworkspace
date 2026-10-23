//! LrWorkspace Remote — SSH connectivity and remote command execution.
//!
//! This crate implements the `RemoteExecutor` trait from `lrworkspace-core`,
//! providing:
//!
//! - SSH session pooling with automatic reconnect
//! - Direct and jump-host (multi-hop) connections
//! - Key, agent, and password authentication
//! - Remote command execution with output truncation
//! - Environment snapshot capture (OS, shell, programs, runtimes)
//! - Connection health checks
//!
//! ## Example
//!
//! ```rust,no_run
//! # use std::time::Duration;
//! # use lrworkspace_remote::RemoteExecutorImpl;
//! # use lrworkspace_core::traits::RemoteExecutor;
//! #
//! # async fn example() -> lrworkspace_core::error::CoreResult<()> {
//! let executor = RemoteExecutorImpl::new(Duration::from_secs(60));
//!
//! // check_connection, execute, and capture_env are available via the trait
//! # Ok(())
//! # }
//! ```

pub mod b64;
pub mod batch;
pub mod compute_ssh;
mod cluster;
mod executor;
mod file_transfer;
mod process;
mod stream;
mod indexer;
pub mod openssh;
pub mod proxy;
pub mod doctor;
pub mod local_auth;
pub mod ssh_config;
pub mod task_router;
pub mod tasks;

pub use cluster::ClusterDetectorImpl;
pub use executor::RemoteExecutorImpl;
pub use file_transfer::{DownloadReceipt, TransferMethod, TransferPreference};
pub use indexer::ProjectIndexerImpl;
pub use openssh::{ensure_openssh_client, openssh_client_available};
pub use ssh_config::SshConfig;
pub use task_router::{
    resolve_execution_route, normalize_execution_path, ExecutionRoute,
};
