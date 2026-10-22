//! Exercise the actual stdio binary without accessing a remote workspace.
use std::io::Write;
use std::process::{Command, Stdio};

#[test]
fn local_stdio_initialize_tools_and_classification() {
    let root = std::env::temp_dir().join(format!("lrw-mcp-test-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    let config = root.join("config.toml");
    std::fs::write(&config, "").unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_lrworkspace-mcp"))
        .env("LRWORKSPACE_CONFIG", &config)
        .env("LRWORKSPACE_DATA_DIR", root.join("data"))
        .env_remove("LRWORKSPACE_FILE_THRESHOLD_MB")
        .env_remove("LRWORKSPACE_SSH_TIMEOUT")
        .env_remove("LRWORKSPACE_PROJECT_ROOT")
        .current_dir(&root)
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::piped())
        .spawn().unwrap();
    let mut input = child.stdin.take().unwrap();
    for request in [
        serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"clientInfo":{"name":"test"},"capabilities":{}}}),
        serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}),
        serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"workspace_diagnose_classify","arguments":{"log_text":"before\nOut of memory\nafter"}}}),
        serde_json::json!({"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"workspace_diagnose_list","arguments":{}}}),
        serde_json::json!({"jsonrpc":"2.0","id":5,"method":"ping"}),
    ] {
        writeln!(input, "{request}").unwrap();
    }
    drop(input);
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let mut responses = String::from_utf8(output.stdout).unwrap().lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap()).collect::<Vec<_>>();
    responses.sort_by_key(|value|value["id"].as_u64());
    assert_eq!(responses.len(), 5, "notifications must not produce responses");
    for (index, response) in responses.iter().enumerate() {
        assert_eq!(response["id"], index + 1);
        assert!(response.get("error").is_none(), "{response}");
    }
    assert_eq!(responses[0]["result"]["serverInfo"]["name"], "lrworkspace-mcp");
    assert!(responses[1]["result"]["tools"].as_array().unwrap().iter()
        .any(|tool| tool["name"] == "workspace_read_file"));
    let classification: serde_json::Value = serde_json::from_str(
        responses[2]["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert!(classification["signatures"].as_array().unwrap().iter()
        .any(|signature| signature.as_str().is_some_and(|text| text.starts_with("OOM:"))));
    // Unique, test-created configuration root only.
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn a_pending_client_form_does_not_block_ping_or_other_requests() {
    use std::io::BufRead;
    let root=std::env::temp_dir().join(format!("lrw-stdio-form-{}",uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(root.join("config.toml"),"").unwrap();
    let mut child=Command::new(env!("CARGO_BIN_EXE_lrworkspace-mcp"))
        .env("LRWORKSPACE_CONFIG",root.join("config.toml"))
        .env("LRWORKSPACE_DATA_DIR",root.join("data"))
        .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
    let mut input=child.stdin.take().unwrap();let stdout=child.stdout.take().unwrap();
    let (sender,receiver)=std::sync::mpsc::channel::<serde_json::Value>();
    std::thread::spawn(move||{for line in std::io::BufReader::new(stdout).lines(){
        let Ok(line)=line else{break};if sender.send(serde_json::from_str(&line).unwrap()).is_err(){break;}
    }});
    let receive=||receiver.recv_timeout(std::time::Duration::from_secs(5)).unwrap();
    writeln!(input,"{}",serde_json::json!({"jsonrpc":"2.0","id":1,"method":"initialize","params":{"capabilities":{"elicitation":{}},"clientInfo":{"name":"test"}}})).unwrap();input.flush().unwrap();
    assert_eq!(receive()["id"],1);
    writeln!(input,"{}",serde_json::json!({"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"workspace_configure","arguments":{}}})).unwrap();input.flush().unwrap();
    let form=receive();assert_eq!(form["method"],"elicitation/create");
    // Client request IDs may overlap server request IDs; do not consume this as a form response.
    writeln!(input,"{}",serde_json::json!({"jsonrpc":"2.0","id":form["id"],"method":"ping"})).unwrap();input.flush().unwrap();
    assert_eq!(receive()["id"],form["id"]);
    writeln!(input,"{}",serde_json::json!({"jsonrpc":"2.0","id":3,"method":"tools/list"})).unwrap();input.flush().unwrap();
    assert_eq!(receive()["id"],3);
    writeln!(input,"{}",serde_json::json!({"jsonrpc":"2.0","id":form["id"],"result":{"action":"decline"}})).unwrap();input.flush().unwrap();
    assert_eq!(receive()["id"],2);
    drop(input);assert!(child.wait().unwrap().success());
    std::fs::remove_dir_all(root).unwrap();
}
