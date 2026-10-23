# Cluster 执行与独立任务（0.6.6）

`cluster` 在集群计算节点运行任务；登录节点只负责轻量查询、文件访问、中转和提交。`direct` 仅用于已确认的单服务器。所有 workload 默认 cluster，包括旧 `direct_compute` 注册。单服务器需显式选择 direct，未确认角色的端点仍拒绝 workload。

| 请求 | 实际路径 |
|---|---|
| `run --force cluster` | 重新发现 Slurm/PBS 节点，经登录节点测量实际资源，选择符合门槛的计算节点；没有合适节点时提交调度器 |
| `run --force cluster --execution-path submit` | 登录节点提交 Slurm/PBS，任务在分配的计算节点运行 |
| `run --force cluster --compute-node compute01` | 验证指定节点属于本次清单后，经登录节点 SSH 运行，不自动重提 |
| `run --force direct` | 已确认的单服务器执行 |

集群连接链路始终是 **本机 → 已注册登录节点 → 计算节点**。`compute_node` 是从登录节点访问的目标；agent 不应在本机直接 SSH 该节点，也不能通过重注册为单服务器绕过此链路。返回的 `connection_chain` 显示实际两跳端点。

自动选择依据新鲜调度器清单和计算节点上的实际资源测量，不再以 `idle/free` 作为唯一门槛。默认要求两项同时满足：实际空闲 CPU 能力折算至少 3 核；实际可用内存严格超过总内存的 20%。按用户要求，`alloc/mixed` 节点只要实际资源足够也可进入，不把调度器已预留的核数直接当成实际 CPU 占用。

CPU 通过计算节点 `/proc/stat` 两次相隔 200 ms 的观测计算，空闲能力包含 idle/iowait；这是主机实际空闲能力折算，不是未分配配额。内存优先读取 `/proc/meminfo` 的 `MemAvailable`，旧内核缺少该字段时保守使用 `MemFree`，返回数据中标明口径。未知、无效或低于门槛的测量不算可用。指定节点同样需要符合资源门槛；已启动任务的状态/日志查询不受该门槛影响。

仍遵守允许队列限制，排除 down/offline/drain/maintenance/reserved 和登录端点；同时请求分散候选节点，最多探测 16 个并受操作预算约束。固定探测只读取身份、工作目录和资源，不运行用户 workload。第二跳核对主机身份，别名回到登录节点也会拒绝。返回 `compute_selection.selected_resources` 和失败原因；这些是当次快照，不预留 CPU/内存，不保证后续负载不变。

自动选择无需每次由用户提供计算节点名称。可用 `execution_path=ssh` / `--execution-path ssh` 要求必须 SSH，找不到节点就报错。非空 `batch` / CLI batch 资源参数强制调度提交；SSH 与 batch 资源不能混用。`workspace_submit` / `rw submit` 始终保留原有 Slurm、PBS Pro/OpenPBS、Torque 命令数组模式。

0.6.4 起，MCP 中省略 `execution_path`、设为 null，或省略/清空 `batch` 均保留默认自动路径，不会因为空资源对象意外强制 PBS 提交。`force` 决定模式，`execution_path` 仅选择传输路径；通常两者都省略即可。兼容旧路径提示 `cluster` → auto、`compute-node` → ssh、`submit-only` → submit；`execution_path=direct` 在 cluster 模式只能进入已验证计算节点 SSH，不能绕过登录节点限制。单服务器仍必须明确 `force=direct` 且已有单服务器确认；其 `execution_path=ssh/direct` 均可用。只读模式的 SSH 仍只允许白名单命令。

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

0.6.3 起，显式节点不能跳过新鲜节点清单与允许分区校验；目标实际主机名不匹配时拒绝执行。节点清单缺失或名字不明确时使用 submit，不把其它可 SSH 主机当成计算节点。既有任务只读监控继续沿用原节点记录。

0.6.5 支持调度器节点名与系统主机名不同的 DNS 别名。登录节点对清单中的目标做实时 `getent ahosts` 查询；若短主机名不直接相同，实际主机名必须匹配该查询的规范名称，且 SSH 服务端连接地址必须出现在同一查询中。缺少任一依据仍拒绝执行；不会仅凭 SSH 可连接、客户端别名或目标自报名称放行。登录节点拒绝规则和 OpenSSH 主机密钥校验继续生效，合法别名不需要用户关闭校验或在源码添加节点白名单。

自动选择失败会报告本次发现的节点数、候选计算节点数与实际资源测量和每个候选的探测失败原因；回退提交失败时保留这些原因，不能把身份不匹配误报成“节点全满”。`workspace_discover` 返回运行进程的 `server_version`。升级后应核对该字段，而非仅检查磁盘上的安装包；旧版本进程和客户端恢复的旧配置可能仍然存在。
