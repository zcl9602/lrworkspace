//! Disposable Linux CI only: real OpenSSH password authentication, SCP and
//! rsync with synthetic credentials. Never runs against a user's SSH server.
#![cfg(target_os = "linux")]
use lrworkspace_core::{
    privacy::SensitiveString,
    traits::{RemoteExecutor, RemoteFileAccess},
    types::*,
};
use lrworkspace_remote::{
    local_auth::{self, SessionTarget},
    RemoteExecutorImpl, TransferPreference,
};
use std::{
    collections::HashMap,
    fs,
    io::Write,
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

fn checked(command: &mut Command) {
    let output = command.output().unwrap();
    assert!(
        output.status.success(),
        "fixture command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
struct Fixture {
    root: PathBuf,
    user: String,
    created_user: bool,
    child: Option<Child>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Ok(pid) = fs::read_to_string(self.root.join("sshd.pid")) {
            if let Ok(pid) = pid.trim().parse::<u32>() {
                let _ = Command::new("sudo")
                    .args(["-n", "kill", "--", &pid.to_string()])
                    .output();
            }
        }
        if let Some(child) = self.child.as_mut() {
            let _ = child.wait();
        }
        if self.created_user {
            let _ = Command::new("sudo")
                .args(["-n", "userdel", "--", &self.user])
                .output();
        }
        // Only reclaim ownership of this freshly created /tmp fixture tree.
        if self.root.parent() == Some(std::env::temp_dir().as_path())
            && self
                .root
                .file_name()
                .is_some_and(|s| s.to_string_lossy().starts_with("lrw-private-ssh-"))
        {
            let uid = Command::new("id").arg("-u").output().unwrap();
            let gid = Command::new("id").arg("-g").output().unwrap();
            let owner = format!(
                "{}:{}",
                String::from_utf8_lossy(&uid.stdout).trim(),
                String::from_utf8_lossy(&gid.stdout).trim()
            );
            let _ = Command::new("sudo")
                .args(["-n", "chown", "-R", "--no-dereference", &owner])
                .arg(&self.root)
                .output();
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}
struct Login {
    executor: RemoteExecutorImpl,
    id: uuid::Uuid,
}
impl Drop for Login {
    fn drop(&mut self) {
        self.executor.close_local_session(self.id);
    }
}

#[test]
#[ignore = "creates a temporary OS account/sshd; explicitly enable only in disposable Linux CI"]
fn isolated_sshd_password_authentication_and_binary_transports() {
    assert_eq!(
        std::env::var("LRW_RUN_ISOLATED_SSHD_TEST").as_deref(),
        Ok("1"),
        "Run only in a disposable Linux runner/container"
    );
    assert!(
        Path::new("/usr/sbin/sshd").is_file(),
        "Install openssh-server in the disposable runner"
    );
    let root = std::env::temp_dir().join(format!("lrw-private-ssh-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(root.join("work")).unwrap();
    let user = format!("lrwtst{}", &uuid::Uuid::new_v4().simple().to_string()[..12]);
    let mut fixture = Fixture {
        root: root.clone(),
        user: user.clone(),
        created_user: false,
        child: None,
    };
    checked(
        Command::new("sudo")
            .args([
                "-n",
                "useradd",
                "--no-create-home",
                "--no-log-init",
                "--shell",
                "/bin/sh",
                "--home-dir",
            ])
            .arg(root.join("work"))
            .arg(&user),
    );
    fixture.created_user = true;
    let password = SensitiveString::new(format!("Fixture-{}!", uuid::Uuid::new_v4()));
    let mut change = Command::new("sudo")
        .args(["-n", "chpasswd"])
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    {
        let mut input = change.stdin.take().unwrap();
        writeln!(input, "{}:{}", user, password.expose()).unwrap();
    }
    assert!(change.wait().unwrap().success());
    checked(
        Command::new("sudo")
            .args(["-n", "chown", &user])
            .arg(root.join("work")),
    );
    checked(
        Command::new("ssh-keygen")
            .args(["-q", "-t", "ed25519", "-N", "", "-f"])
            .arg(root.join("host_key")),
    );
    checked(Command::new("sudo").args(["-n", "mkdir", "-p", "/run/sshd"]));
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let config=format!("ListenAddress 127.0.0.1\nPort {port}\nHostKey {}\nPidFile {}\nPasswordAuthentication yes\nAuthenticationMethods password\nKbdInteractiveAuthentication no\nUsePAM no\nPermitRootLogin no\nPermitEmptyPasswords no\nAllowUsers {user}\nAuthorizedKeysFile none\nStrictModes no\nLogLevel ERROR\nSubsystem sftp internal-sftp\n",root.join("host_key").display(),root.join("sshd.pid").display());
    fs::write(root.join("sshd_config"), config).unwrap();
    fixture.child = Some(
        Command::new("sudo")
            .args(["-n", "/usr/sbin/sshd", "-D", "-e", "-f"])
            .arg(root.join("sshd_config"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + Duration::from_secs(10);
    while TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "isolated sshd failed to start");
        std::thread::sleep(Duration::from_millis(30));
    }
    let public = fs::read_to_string(root.join("host_key.pub")).unwrap();
    let key = public
        .split_whitespace()
        .take(2)
        .collect::<Vec<_>>()
        .join(" ");
    let known_hosts = root.join("known hosts");
    fs::write(&known_hosts, format!("[127.0.0.1]:{port} {key}\n")).unwrap();
    let helper = Path::new(env!("CARGO_BIN_EXE_rw"))
        .parent()
        .unwrap()
        .join("lrworkspace-mcp");
    assert!(helper.is_file(), "Build matching MCP first");
    let target = SessionTarget {
        host: "127.0.0.1".into(),
        user: user.clone(),
        port,
        socks_proxy: None,
        known_hosts,
    };
    let id = local_auth::create_session(password.clone(), helper.clone(), target.clone()).unwrap();
    let executor = RemoteExecutorImpl::new(Duration::from_secs(20));
    let _login = Login {
        executor: executor.with_timeout(Duration::from_secs(20)),
        id,
    };
    let workspace = Workspace {
        id: uuid::Uuid::new_v4(),
        name: "isolated-fixture".into(),
        display_name: "isolated-fixture".into(),
        connection: ConnectionConfig {
            host: target.host.clone(),
            user: target.user.clone(),
            port,
            auth: AuthMethod::LocalPassword {
                session_id: id,
                socks_proxy: None,
            },
            jump_hosts: vec![],
        },
        remote_work_dir: root.join("work"),
        remote_data_dirs: vec![],
        node_execution_mode: NodeExecutionMode::SubmitOnly,
        authorized: true,
        created_at: chrono::Utc::now(),
        last_accessed_at: chrono::Utc::now(),
        file_threshold_bytes: 1048576,
        config: HashMap::new(),
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        eprintln!("checking isolated password authentication");
        assert!(executor.check_connection(&workspace).await.unwrap());
        let payload = (0..65553).map(|n| (n % 251) as u8).collect::<Vec<_>>();
        let source = root.join("source 中文.bin");
        fs::write(&source, &payload).unwrap();
        for transport in [TransferPreference::Scp, TransferPreference::Rsync] {
            let name = if transport == TransferPreference::Scp {
                "scp ' 中文.bin"
            } else {
                "rsync ' 中文.bin"
            };
            let method = executor
                .upload_local_file(&workspace, Path::new(name), &source, transport)
                .await
                .unwrap();
            assert_eq!(
                method.as_str(),
                if transport == TransferPreference::Scp {
                    "scp"
                } else {
                    "rsync"
                }
            );
            assert_eq!(
                executor
                    .read_remote_file(&workspace, RemoteRootKind::Work, Path::new(name))
                    .await
                    .unwrap(),
                payload
            );
        }
        let wrong = SensitiveString::new(format!("Wrong-{}", uuid::Uuid::new_v4()));
        let wrong_id = local_auth::create_session(wrong, helper.clone(), target.clone()).unwrap();
        let mut invalid = workspace.clone();
        invalid.connection.auth = AuthMethod::LocalPassword {
            session_id: wrong_id,
            socks_proxy: None,
        };
        assert!(!executor.check_connection(&invalid).await.unwrap_or(false));
        executor.close_local_session(wrong_id);
        let mut untrusted = target.clone();
        untrusted.known_hosts = root.join("untrusted hosts");
        fs::write(&untrusted.known_hosts, "").unwrap();
        let untrusted_id = local_auth::create_session(password, helper, untrusted.clone()).unwrap();
        invalid.connection.auth = AuthMethod::LocalPassword {
            session_id: untrusted_id,
            socks_proxy: None,
        };
        assert!(!executor.check_connection(&invalid).await.unwrap_or(false));
        assert!(fs::read(&untrusted.known_hosts).unwrap().is_empty());
        executor.close_local_session(untrusted_id);
        executor.close_local_session(id);
        assert!(!local_auth::session_ready(&workspace.connection.auth));
        assert!(
            executor.check_connection(&workspace).await.is_err()
                || !executor.check_connection(&workspace).await.unwrap_or(false)
        );
    });
}
