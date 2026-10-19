//! Child processes isolate environment overrides from parallel Rust tests.
use std::path::PathBuf;
use std::process::{Command, Output};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        let root = std::env::temp_dir().join(format!("lrw-cli-test-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }

    fn command(&self) -> Command {
        let config = self.0.join("config.toml");
        std::fs::write(&config, "").unwrap();
        let mut command = Command::new(env!("CARGO_BIN_EXE_rw"));
        command.env("LRWORKSPACE_CONFIG", config)
            .env("LRWORKSPACE_DATA_DIR", self.0.join("env-data"))
            .env_remove("LRWORKSPACE_FILE_THRESHOLD_MB")
            .env_remove("LRWORKSPACE_SSH_TIMEOUT")
            .env_remove("LRWORKSPACE_PROJECT_ROOT")
            .current_dir(&self.0);
        command
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Unique directory created by this test, never a user configuration root.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn assert_success(output: Output) {
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
}

#[test]
fn cli_data_directory_wins_over_environment() {
    let fixture = Fixture::new();
    let explicit = fixture.0.join("explicit-data");
    assert_success(fixture.command().arg("--data-dir").arg(&explicit)
        .args(["status", "--json"]).output().unwrap());
    assert!(explicit.is_dir());
    assert!(!fixture.0.join("env-data").exists());
}

#[test]
fn missing_explicit_config_fails_without_creating_a_store() {
    let fixture = Fixture::new();
    let output = fixture.command().arg("--config").arg(fixture.0.join("missing.toml"))
        .args(["status", "--json"]).output().unwrap();
    assert!(!output.status.success());
    assert!(!fixture.0.join("env-data").exists());
}

#[test]
fn invalid_environment_limits_fail_without_panicking() {
    let fixture = Fixture::new();
    for (name, value) in [
        ("LRWORKSPACE_FILE_THRESHOLD_MB", "18446744073709551615"),
        ("LRWORKSPACE_SSH_TIMEOUT", "zero"),
        ("LRWORKSPACE_SSH_TIMEOUT", "0"),
    ] {
        let output = fixture.command().env(name, value)
            .args(["status", "--json"]).output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr).contains(name));
    }
}

#[test]
fn authorization_flag_requires_workspace_selection() {
    let fixture = Fixture::new();
    let output = fixture.command().args(["workspace", "--authorize"]).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("require --id"));
}

#[test]
fn timeout_override_is_global_and_zero_is_rejected() {
    let fixture = Fixture::new();
    assert_success(fixture.command().args(["status", "--json", "--timeout-secs", "600"]).output().unwrap());
    let output = fixture.command().args(["status", "--timeout-secs", "0"]).output().unwrap();
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("--timeout-secs must be positive"));
    assert!(!fixture.0.join(".lrworkspace").exists());
    assert!(!fixture.0.join(".cursor").exists());
    assert!(!fixture.0.join(".codex").exists());
}
