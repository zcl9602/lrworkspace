//! LrWorkspace Core — fundamental types, configuration, and traits for the
//! Universal AI Remote Workspace Capability System.
//!
//! This crate defines the shared data model used by all other crates:
//! workspace identity, project index, file entries, environment snapshots,
//! audit records, permission levels, and the configuration system.

pub mod config;
pub mod jsonl;
pub mod error;
pub mod permission;
pub mod privacy;
pub mod safety;
pub mod traits;
pub mod types;
pub mod workspace_store;

pub use config::LrWorkspaceConfig;
pub use error::{CoreError, CoreResult};
pub use permission::RiskLevel;
pub use types::*;
