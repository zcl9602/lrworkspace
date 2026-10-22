# Cluster 执行与独立任务（0.6.0）

`cluster` 在集群计算节点运行任务；登录节点只负责轻量查询、文件访问、中转和提交。`direct` 仅用于已确认的单服务器。所有 workload 默认 cluster，包括旧 `direct_compute` 注册。单服务器需显式选择 direct，未确认角色的端点仍拒绝 workload。

| 请求 | 实际路径 |
|---|---|
| `run --force cluster` | 重新发现 Slurm/PBS 空闲节点，探测第二跳 SSH，选择可达节点运行；没有合适节点时提交调度器 |
| `run --force cluster --execution-path submit` | 登录节点提交 Slurm/PBS，任务在分配的计算节点运行 |
| `run --force cluster --compute-node compute01` | 经登录节点 SSH 到指定计算节点，不自动重提 |
| `run --force direct` | 已确认的单服务器执行 |

自动选择依据本次调度器发现结果，只选择 `idle/free` 节点，遵守已保存的允许队列限制，排除未知、占用、离线和登录端点。多个请求分散选择候选节点；最多探测四个节点。探测只核对主机与工作目录，不运行用户 workload。第二跳在执行前对比实际主机名，别名回到登录节点也会拒绝。

自动选择无需每次由用户提供计算节点名称。可用 `execution_path=ssh` / `--execution-path ssh` 要求必须 SSH，找不到节点就报错。显式 `batch` / CLI batch 资源参数强制调度提交；SSH 与 batch 资源不能混用。`workspace_submit` / `rw submit` 始终保留原有 Slurm、PBS Pro/OpenPBS、Torque 命令数组模式。

SSH 使用登录账户已有的非交互认证，不转发本机 agent、不跳过主机密钥验证、不自动输入第二跳密码。它不申请调度器配额；要求资源分配的站点应使用 `execution_path=submit`。计算节点须能访问同一 work 路径，并提供 bash、hostname、realpath。后台任务另需 Linux setsid、nohup、tail 和 base64。

## 并发与监控

MCP 同时接收独立请求，响应按请求 ID 匹配，允许先返回后提交但较快的请求。Windows 同一端点使用多个独立 SSH 会话，不再让一条长命令锁住该主机。前台任务超时只影响自己的会话；ping、客户端表单与其他任务互不等待。

MCP `workspace_run` 的 SSH/单服务器 workload 默认 `background:true`：

```json
{"workspace_id":"research","force":"cluster","command":"bash scripts/a.sh"}
{"workspace_id":"research","force":"cluster","command":"bash scripts/b.sh"}
```

每个请求返回独立 `task_id`。使用 `workspace_task_status(task_id)` 查实时状态、实际 hostname 和退出码，`workspace_task_logs(task_id)` 查两路日志尾部，`workspace_tasks` 列出句柄。列表中的状态是本机最近一次观测，实时状态应单独查询。调度提交仍返回 `job_id`，使用原有 scheduler status/logs 工具监控。

```text
rw run --workspace research --force cluster --background -- bash scripts/a.sh
rw run --workspace research --force cluster --background -- bash scripts/b.sh
rw tasks --workspace research
rw tasks --workspace research --id <UUID> --logs --max-bytes 16384
```

CLI 默认保持前台输出，`--background` 启用后台任务；MCP 可设 `background:false` 等待输出。后台脚本和日志各自存放在远端 work 根下 `._lrworkspace_tasks/<task_id>/`，句柄保存在本机工作区元数据中。任务与日志不会因 MCP 重启或断开而自动删除。重连后重新认证即可用原句柄查询。

日志默认每路最多读取最近 16 KiB，可设 `max_bytes` / `--max-bytes`，上限为每路 64 KiB；报告截断，不把整份日志载入内存。状态区分 starting、running、succeeded、failed、unknown；只有明确的退出码文件证明完成，空日志、PID 消失或连接失败不算成功。

默认 SSH 操作预算仍为 120 秒，CLI `--timeout-secs` / MCP `timeout_sec` 可修改。后台任务寿命不受这次 SSH 等待预算限制；调度作业的时间限制由调度器资源配置决定。

启动前先落盘句柄。发送 workload 后发生连接异常时返回可查询的 `task_id` 和 unknown 状态，不重新启动、不切换节点重跑、不回落登录节点。只有尚未发送 workload 的自动探测失败，才可换候选节点或选择调度提交。

节点配置继续使用 CLI `--node-mode submit-only` / MCP `submit_only` 表示集群登录端点；这是保留的存储名称，不代表任务必须仅用调度器。单服务器使用 `single-server` / `single_server`，兼容旧 `direct-compute` / `direct_compute`。
