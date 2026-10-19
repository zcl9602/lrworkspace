# Cluster 的两条执行路径（0.3.1）

`cluster` 表示在集群计算节点工作，`direct` 表示单服务器执行。登录节点仍负责查询、文件访问和中转，不运行 workload。

| 请求 | 实际路径 | 结果 |
|---|---|---|
| `run --force cluster` | 登录节点 → Slurm/PBS → 分配的计算节点 | job_id，后续查状态和日志 |
| `run --force cluster --compute-node compute01` | 登录节点 → SSH compute01 → 执行命令 | stdout、stderr、退出码 |
| `run --force direct` | 已确认的单服务器 → 执行命令 | stdout、stderr、退出码 |

```text
rw run --workspace research --force cluster --cpu 4 -- bash scripts/run.sh
rw run --workspace research --force cluster --compute-node compute01 --timeout-secs 300 -- bash scripts/run.sh
```

MCP 调用 `workspace_run`：

```json
{"workspace_id":"research","force":"cluster","compute_node":"compute01","command":"bash scripts/run.sh","timeout_sec":300}
```

只有用户明确允许 SSH 进入某计算节点时，agent 才可填写 `compute_node`；参数是这次请求的显式选择，不持久授权，不扫描或自动选空闲节点。省略则走原有数组提交。`workspace_submit` / `rw submit` 始终生成/提交数组，继续支持 Slurm、PBS Pro/OpenPBS 和 Torque。

SSH 路径不向调度器申请 CPU/内存，不接受任何 batch 资源参数（MCP 连空 `batch:{}` 也拒绝）。目标节点应是用户允许访问的节点；此参数不能证明拥有调度器资源分配。

登录节点需要 OpenSSH client，且其账户自己的 SSH 配置/密钥已允许到目标节点的非交互认证；不转发本机 agent，不自动跳过主机密钥验证，不提示密码。使用登录节点的 SSH 默认用户、端口或其 Host 配置；参数只收 hostname/IPv4，不收 user@host、IPv6 字面地址或 SSH options。远端需要 bash、hostname、realpath。

工作目录仍须位于 work 根，计算节点上会重新解析真实路径并检查边界，目录缺失或指向根外时拒绝运行；两端需能访问同一 work 路径。显式目标不能等于登录端点，并在第二跳比对实际短主机名，防止别名误连回登录节点。短名相同但域名不同也会保守拒绝。

SSH 失败、路径错误或命令失败不会回落登录节点，也不会改走调度器。响应标明 `mode:"cluster"` 和 `execution_path:"ssh"` / `"submit"`；SSH 响应带 `compute_node` 和原始命令的执行记录，CLI 返回实际命令退出码。审计保留路径和目标节点。

默认等待 120 秒，可通过上述参数修改。这是命令执行接口，不提供交互式终端。连接超时/中断不应被当作任务从未执行；重试前核对远端输出，避免重复执行。

节点角色配置：集群登录节点继续用 CLI `--node-mode submit-only` / MCP `submit_only`；单服务器新增 CLI `--node-mode single-server` / MCP `single_server`，兼容旧 `direct-compute` / `direct_compute`，落盘名称不变，不需迁移已有注册。
