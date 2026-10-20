//! Real native OpenSSH -> private askpass -> synthetic localhost SSH server.
//! Python/Paramiko are test-only; installed LRW has no Python dependency.
use lrworkspace_core::{privacy::SensitiveString, types::*};
use lrworkspace_remote::{
    local_auth::{self, SessionTarget},
    openssh,
};
use std::{
    fs,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, Command, Stdio},
};

struct Fixture {
    root: PathBuf,
    server: Child,
    input: Option<ChildStdin>,
    sessions: Vec<uuid::Uuid>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        for id in &self.sessions {
            local_auth::remove_session(*id);
        }
        self.input.take();
        let _ = self.server.wait();
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
#[ignore = "requires test-only Paramiko and a matching MCP helper binary"]
fn native_openssh_password_is_delivered_only_to_the_private_authentication_pipe() {
    let python = std::env::var_os("LRW_TEST_AUTH_PYTHON")
        .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into());
    let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/password_server.py");
    let mut server = Command::new(python)
        .arg(script)
        .env("PYTHONIOENCODING", "utf-8")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let password = SensitiveString::new(format!("Fixture-{}!测试", uuid::Uuid::new_v4()));
    let mut input = server.stdin.take().unwrap();
    // Synthetic test value goes over a private pipe, never argv or env.
    writeln!(
        input,
        "{}",
        serde_json::json!({"password":password.expose()})
    )
    .unwrap();
    input.flush().unwrap();
    let mut line = String::new();
    BufReader::new(server.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let ready: serde_json::Value = serde_json::from_str(&line)
        .expect("Paramiko fixture must start; install the test dependency");
    let port = ready["port"].as_u64().unwrap() as u16;
    let root = std::env::temp_dir().join(format!("lrw-native-auth-{}", uuid::Uuid::new_v4()));
    fs::create_dir(&root).unwrap();
    let mut fixture = Fixture {
        root: root.clone(),
        server,
        input: Some(input),
        sessions: vec![],
    };
    let known_hosts = root.join("known hosts");
    fs::write(
        &known_hosts,
        format!(
            "[127.0.0.1]:{port} {}\n",
            ready["host_key"].as_str().unwrap()
        ),
    )
    .unwrap();
    let source_helper = Path::new(env!("CARGO_BIN_EXE_rw"))
        .parent()
        .unwrap()
        .join(format!("lrworkspace-mcp{}", std::env::consts::EXE_SUFFIX));
    assert!(source_helper.is_file());
    // Real installations may contain Unicode and spaces in the user path.
    let helper_dir = root.join("helper 中文 space");
    fs::create_dir(&helper_dir).unwrap();
    let helper = helper_dir.join(source_helper.file_name().unwrap());
    fs::copy(&source_helper, &helper).unwrap();
    if local_auth::ui_available() {
        local_auth::validate_native_dialog_for_helper(&helper).unwrap();
    }
    let target = SessionTarget {
        host: "127.0.0.1".into(),
        port,
        user: "fixture".into(),
        socks_proxy: None,
        known_hosts,
    };
    for (credential, accepted) in [
        (password.clone(), true),
        (
            SensitiveString::new(format!("Wrong-{}", uuid::Uuid::new_v4())),
            false,
        ),
    ] {
        let id = local_auth::create_session(credential, helper.clone(), target.clone()).unwrap();
        fixture.sessions.push(id);
        let config = ConnectionConfig {
            host: target.host.clone(),
            port,
            user: target.user.clone(),
            auth: AuthMethod::LocalPassword {
                session_id: id,
                socks_proxy: None,
            },
            jump_hosts: vec![],
        };
        let mut command = openssh::ssh_command_for(&config);
        command.args([
            "-T",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            "BatchMode=no",
            "-o",
            "ConnectTimeout=5",
            "-l",
            "fixture",
            "-p",
            &port.to_string(),
        ]);
        command.args(local_auth::ssh_arguments(&config, false).unwrap());
        local_auth::apply(&config, &mut command, false).unwrap();
        command.args(["127.0.0.1", "true"]).stdin(Stdio::null());
        assert!(!command
            .get_args()
            .any(|arg| arg.to_string_lossy().contains(password.expose())));
        assert!(!command.get_envs().any(|(_, value)| value
            .is_some_and(|value| value.to_string_lossy().contains(password.expose()))));
        let output = command.output().unwrap();
        assert_eq!(
            output.status.success(),
            accepted,
            "native SSH authentication result mismatch: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains(password.expose()));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(password.expose()));
        if accepted {
            assert_eq!(output.stdout, b"AUTHENTICATED\n");
        }
        local_auth::remove_session(id);
    }
}
