# Local Remote Workspace（LRW）

LRW 通过本机 CLI/MCP 和 SSH 访问远程工作区：查看文件、上传 agent 已完成的文件，以及执行命令或提交计算作业。当前版本为 **0.5.1**。

```text
agent 自己的项目 → rw / lrworkspace-mcp → SSH → 远程 work / data 根
                                           → Slurm / PBS 计算节点
```

不需要本地 LRW 项目目录、marker、项目镜像或同步。agent 在自己的项目中完成编辑，使用 `rw write --from` 或 MCP 的 `local_path` 上传；LRW 只保存连接注册、缓存和审计，传输临时文件自动清理。

没有 SSH config 时，Windows / macOS 的 MCP 可调用 `workspace_login`，由用户在本机独立窗口填写 IP、端口、用户名和密码。密码不进入聊天、MCP 参数或持久化配置；工具只返回登录状态。Windows 可在系统保护可用时显示明文预览，macOS 保持遮蔽。会话在重启后失效；详情及保护边界见[本机私密登录](docs/private-login.md)。

0.5.1 修复发布二进制中的个人构建路径和旧私钥口令的明文保存：统一映射 Rust/C 路径、剥离符号，并在打包前检查程序；注册和新建备份不再保存私钥口令。旧字段只作本次进程内兼容，后续使用 SSH agent。详见[隐私修复与验收](docs/release-privacy.md)。

## 执行方式

| 入口 | 行为 |
|---|---|
| `cluster` | 默认 Slurm/PBS 提交；显式 `--compute-node HOST` 时从登录节点 SSH 到指定计算节点执行 |
| `direct` | 单服务器模式，在已确认的服务器上直接执行 |
| `read-only` / `rw ro` | 在 SSH endpoint 执行白名单查询 |

`--force compute-node` 仍仅是 `cluster` 的旧别名；新参数 `--compute-node HOST` 表示本次明确允许进入该节点。不会自动选择缓存空闲节点。`submit_only` 工作区默认 cluster 并拒绝 direct；`--node-mode single-server` 默认 direct（兼容旧 `direct-compute`，存储值仍为 `direct_compute`）；未确认角色时拒绝 workload。

所有 Slurm/PBS 提交现统一使用用户提供代码的命令数组模式：过滤空行/注释、随机打乱、最多 1000 个命令文件、按 CPU 和任务数限制并发、逐条记录状态并汇总失败。默认 1 节点、1 task、每 task 1 CPU，`maxnum=1000`、`maxcpu=2000`；不主动申请内存或时间上限。

```text
rw submit --workspace <UUID或名称> --in commands.sh --outdir JOBS --cpu 1 --maxnum 500 --submit
```

`rw submit` 不加 `--submit` 只生成；未指定计算节点时，`run --force cluster` 和 MCP 的 `workspace_run` 自动提交单命令分组，`workspace_submit` 接收命令列表。每次使用新 run 目录，保留旧脚本和日志。详见 [统一提交模式](docs/batch-submit.md)。

默认命令/传输等待为 **120 秒**，CLI 用 `--timeout-secs`，MCP run/write/submit/wait 用 `timeout_sec` 覆盖。排队任务的作业时限由调度器控制，和本机请求超时是两个概念。

```text
# 明确允许 SSH 到 compute01，跳过调度器
rw run --workspace research --force cluster --compute-node compute01 --timeout-secs 300 -- bash scripts/run.sh
```

不带 `--compute-node` 仍然提交；SSH 失败不回落登录节点、不改走提交。此路径不申请调度资源，不能与 `--cpu` 等 batch 参数混用。登录节点需已具备到目标节点的非交互 SSH 认证，计算节点需能访问工作目录。详见 [cluster 双路径](docs/cluster-execution.md)。

0.3.2 修复 SSH 输出无限缓存、写入/收尾超时失效、长历史查询的内存增长和审计记录顺序；批量归档改为临时文件流式写入。默认 stdout/stderr 各保留最多 10 MiB，超出通过 `output_truncated` 标记，仍返回退出码。详见 [内存与稳定性维护](docs/maintenance-2026-10-19-memory-stability.md)。

0.3.3 经旧版 PBS 集群实机验收修复了 Windows rsync 代理 shell 和旧 Torque 日志路径；PBS 只有一个命令组时使用普通作业，仍保留相同分组脚本和状态记录。该站点的数组启动器存在 zsh 方括号解析错误，多元素数组尚未通过。详见 [匿名化集群验收记录](docs/maintenance-2026-10-19-proxy-pbs.md)。

## 上传

```text
rw write --workspace <UUID或名称> --file scripts/run.sh --from ./run.sh --transport rsync --timeout-secs 300
rw run --workspace <UUID或名称> --force cluster -- bash scripts/run.sh
```

`auto` 可从 rsync 回退到 scp/base64；显式指定 `rsync`/`scp`/`base64` 时失败即报错，返回实际使用的传输方式。上传先准备目标父目录和独占临时目录，传输后核对大小，再替换目标。

**文件 API 有 work/data 根边界；命令没有 OS 级只读沙箱。** 所有命令仍按 SSH 账户真实权限运行。未查到作业或日志不代表成功，需核对明确终态和实际输出。

## 构建与文档

发布程序运行时不需要 Rust/Python/Git。Windows 与 macOS 用户先按[跨平台安装](docs/portable-setup.md)执行 `rw doctor`、`rw mcp install --client codex`；安装采用用户版本目录、绝对路径和配置备份，并更新旧版 MCP 路径。内置 `rw proxy` 可直接提供 SOCKS5 转发，无需私人 Python 脚本。Windows、Apple Silicon Mac 和 Intel Mac 已通过 GitHub 托管 runner 的完整测试与隔离安装验收；用户 Mac 到实际集群的连接仍需按自己的配置验证。

```text
cargo test --workspace --locked
python scripts/build_release.py --verify-install
```

Windows 产物为原生 `.exe`；Linux/macOS 使用同一数据模型。运行时不依赖 WSL，远端不需要安装 LRW。依赖缓存完整时可加 `--offline`。

- [使用方法](usage.md)：注册、上传、执行、超时与迁移。
- [Windows / macOS 安装](docs/portable-setup.md)：独立用户环境、原生代理、依赖检查。
- [架构与边界](docs/architecture.md)：模块和数据流。
- [开发与验收](docs/development.md)：本地测试及真实服务器验收。
- [0.3.0 维护记录](docs/maintenance-2026-10-15-native-arrays.md)：本轮实现、验证和限制。
