//! One reader and one writer own stdio; independent requests may finish out of order.
use crate::{
    elicitation_for_request, failure, merge_tool_arguments, JsonRpcError, JsonRpcRequest,
    JsonRpcResponse, McpServer,
};
use lrworkspace_core::{CoreError, CoreResult};
use serde_json::Value;
use std::{
    collections::HashMap,
    io::{self, BufRead, Write},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, Semaphore};

type Forms = Arc<Mutex<HashMap<String, oneshot::Sender<Value>>>>;

fn send(output: &std::sync::mpsc::SyncSender<Value>, mut value: Value) -> CoreResult<()> {
    for field in ["result", "error", "params"] {
        if let Some(value) = value.get_mut(field) {
            lrworkspace_core::privacy::redact_value(value);
        }
    }
    output
        .send(value)
        .map_err(|_| CoreError::InvalidState("MCP output closed".into()))
}

async fn dispatch(
    server: Arc<McpServer>,
    mut request: JsonRpcRequest,
    output: std::sync::mpsc::SyncSender<Value>,
    forms: Forms,
    closed: Arc<AtomicBool>,
) -> CoreResult<()> {
    if server
        .client_supports_elicitation
        .load(std::sync::atomic::Ordering::Relaxed)
    {
        if let Some(params) = elicitation_for_request(&request) {
            let id = format!("lrw-elicitation-{}", uuid::Uuid::new_v4());
            let (sender, receiver) = oneshot::channel();
            let accepted = {
                let mut pending = forms.lock().unwrap_or_else(|e| e.into_inner());
                if closed.load(Ordering::Acquire) {
                    false
                } else {
                    pending.insert(id.clone(), sender);
                    true
                }
            };
            if !accepted {
                return send(
                    &output,
                    serde_json::to_value(server.handle_request(&request).await)?,
                );
            }
            send(
                &output,
                serde_json::json!({"jsonrpc":"2.0","id":id,"method":"elicitation/create","params":params}),
            )?;
            let response = tokio::time::timeout(Duration::from_secs(300), receiver).await;
            forms.lock().unwrap_or_else(|e| e.into_inner()).remove(&id);
            if let Ok(Ok(value)) = response {
                if value["result"]["action"] == "accept" {
                    if let Some(content) = value["result"]["content"].as_object() {
                        merge_tool_arguments(&mut request, content.clone());
                    }
                }
            }
        }
    }
    let response = server.handle_request(&request).await;
    send(&output, serde_json::to_value(response)?)
}

pub(super) async fn run(server: McpServer) -> CoreResult<()> {
    let server = Arc::new(server);
    let forms: Forms = Arc::new(Mutex::new(HashMap::new()));
    let closed = Arc::new(AtomicBool::new(false));
    let (input_tx, mut input) = mpsc::channel::<io::Result<String>>(32);
    std::thread::spawn(move || {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let failed = line.is_err();
            if input_tx.blocking_send(line).is_err() || failed {
                break;
            }
        }
    });
    let (output, receive) = std::sync::mpsc::sync_channel::<Value>(32);
    let writer = std::thread::spawn(move || -> CoreResult<()> {
        let stdout = io::stdout();
        let mut writer = stdout.lock();
        while let Ok(value) = receive.recv() {
            writeln!(writer, "{}", serde_json::to_string(&value)?)?;
            writer.flush()?;
        }
        Ok(())
    });
    let work = Arc::new(Semaphore::new(16));
    let control = Arc::new(Semaphore::new(16));
    let mut calls = tokio::task::JoinSet::new();
    while let Some(line) = input.recv().await {
        // Reap completed workers instead of retaining one allocation per historical call.
        while let Some(result) = calls.try_join_next() {
            result.map_err(|_| CoreError::InvalidState("MCP worker panicked".into()))??;
        }
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let value = match serde_json::from_str::<Value>(&line) {
            Ok(value) => value,
            Err(_) => {
                let response = JsonRpcResponse {
                    jsonrpc: "2.0".into(),
                    id: Some(Value::Null),
                    result: None,
                    error: Some(JsonRpcError {
                        code: -32700,
                        message: "Parse error".into(),
                        data: None,
                    }),
                };
                send(&output, serde_json::to_value(response)?)?;
                continue;
            }
        };
        if value.get("method").is_none() {
            if let Some(id) = value.get("id").and_then(Value::as_str) {
                if let Some(sender) = forms.lock().unwrap_or_else(|e| e.into_inner()).remove(id) {
                    let _ = sender.send(value);
                }
            }
            continue;
        }
        let request: JsonRpcRequest = match serde_json::from_value(value) {
            Ok(request) => request,
            Err(_) => continue,
        };
        if request.id.is_none() {
            continue;
        }
        // Initialize is a barrier so pipelined calls see the correct client identity/capabilities.
        if request.method == "initialize"
            || request.method == "ping"
            || request.method == "tools/list"
        {
            send(
                &output,
                serde_json::to_value(server.handle_request(&request).await)?,
            )?;
            continue;
        }
        let tool = request
            .params
            .as_ref()
            .and_then(|p| p.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let slots = if matches!(
            tool,
            "workspace_run" | "workspace_submit" | "workspace_write_file" | "workspace_read_file" | "workspace_download_file"
        ) {
            &work
        } else {
            &control
        };
        let permit = match slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                send(
                    &output,
                    serde_json::to_value(failure(
                        &request,
                        -32000,
                        "Too many concurrent requests; retry after checking running tasks",
                    ))?,
                )?;
                continue;
            }
        };
        let (server, output, forms, closed) = (
            server.clone(),
            output.clone(),
            forms.clone(),
            closed.clone(),
        );
        calls.spawn(async move {
            let _permit = permit;
            dispatch(server, request, output, forms, closed).await
        });
    }
    // Closing the input cancels outstanding forms, but accepted operations finish normally.
    {
        let mut pending = forms.lock().unwrap_or_else(|e| e.into_inner());
        closed.store(true, Ordering::Release);
        pending.clear();
    }
    while let Some(result) = calls.join_next().await {
        result.map_err(|_| CoreError::InvalidState("MCP worker panicked".into()))??;
    }
    drop(output);
    writer
        .join()
        .map_err(|_| CoreError::InvalidState("MCP writer panicked".into()))??;
    Ok(())
}
