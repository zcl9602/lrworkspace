# 使用方法

## 1. 构建与配置

发布程序运行时只需本机 OpenSSH，rsync 是可选能力；不需要 Rust/Python/Git/WSL。先按[跨平台安装](docs/portable-setup.md)配置。仅从源码构建时需要 Rust 工具链和对应链接器；非 Windows 构建的 vendored OpenSSL 还需要 C 编译器、Make 和 Perl。远端主要面向 Linux，需要 bash、git、GNU stat/realpath/head/mv、mktemp、base64 等工具；建议提供 setsid/timeout。rsync 传输需要两端都可运行 rsync，作业提交需要 Slurm/PBS。

```text
cargo test --workspace --locked
python scripts/build_release.py --verify-install
```

产物默认在 `target/release/`，Windows 使用 `.exe` 后缀。以下示例假定 `rw` 已在 PATH；否则使用完整路径。`rw` 与 `lrworkspace` 是相同实现。

`rw mcp install --client codex` 将相邻、同版本的 CLI/MCP 安装到用户 `.local/share/lrworkspace/versions/<版本>-<平台>-<架构>`，写用户级 Codex 配置。Cursor 用 `--client cursor`，同时配置用 `--client all`。它不写当前项目配置、规则或 cwd。其他客户端可手动注册 `lrworkspace-mcp` 的绝对路径，`rw mcp show` 提供示例。

安装前备份配置，并更新已有 lrworkspace 的旧程序路径，保留其他服务、自定义环境变量和更长超时。默认 Codex 工具预算至少 150 秒，为 120 秒操作预留余量；如果单次请求使用更长超时，客户端的 tool timeout 也需大于该预算，例如 300 秒操作可将客户端设为 330 秒。更换程序后重新连接 MCP。

## 2. 注册与选择远程工作区

没有 SSH config、希望使用密码时，在桌面 MCP 中调用 `workspace_login`，由用户完成独立本机窗口；再调用 `workspace_login_status` 查询结果。密码禁止作为工具参数或聊天内容传入。重新连接 MCP 后，用 `workspace_login {workspace_id: ...}` 重新认证；该会话不能由独立 CLI 进程复用。详见[本机私密登录](docs/private-login.md)。以下 CLI 注册方式适用于已有 SSH config / 密钥 / agent。

```powershell
rw init `
  --host login.cluster.example `
  --user alice `
  --remote-work-dir /scratch/alice/project `
  --remote-data-dir /data/reference `
  --name research `
  --node-mode submit-only `
  --yes
```

远程路径使用 POSIX 绝对路径。work 必须可读写，data 必须可读，canonical 根不能重叠。省略 user/port 时注册会参考 SSH config；注册后以保存的用户和端口为准，避免执行和传输连接到不同账户。`--yes` 授权 MCP 访问。

工作区按 UUID 或唯一名称选择，不再根据本地 cwd/marker 选择。只注册一个工作区时可以省略选择；多个工作区必须明确指定，重名时使用 UUID。

```text
rw workspace
rw check --workspace research
rw workspace --id <uuid> --authorize
rw workspace --id <uuid> --node-mode submit-only
```

MCP 工作流：`workspace_discover` → `workspace_configure`（必要时表单）→ `workspace_authorize` → `workspace_set_node_mode` → `workspace_layout`。数据根可随后通过 `workspace_add_data_dir` 增加。确认参数应体现用户已经确认的选择。

## 3. 上传 agent 项目中的文件

agent 使用自己的项目编辑和检查文件，再逐文件上传，不需要另外创建 LRW 本地项目：

```text
rw write --workspace research --file scripts/run.sh --from ./run.sh
rw write --workspace research --file scripts/run.sh --from ./run.sh --transport rsync --timeout-secs 300
```

`--file` 是 work 根内的相对目标；`--from` 可以指向 agent 自己项目中的文件。rsync/scp 从文件流式传输，不先把整个文件读入内存。短文本可使用 `--content`，与 `--from` 二选一。

MCP 可直接读取本机文件上传：

```json
{
  "workspace_id": "<uuid或唯一名称>",
  "relative_path": "scripts/run.sh",
  "local_path": "D:\\agent-project\\run.sh",
  "transport": "rsync",
  "timeout_sec": 300
}
```

调用 `workspace_write_file`，`local_path` 与 `content` 必须且只能提供一个；空字符串 content 可创建空文件。

| transport | 行为 |
|---|---|
| `auto`（默认） | rsync → scp → base64，返回实际 transport；base64 回退最多 16 MiB |
| `rsync` | 只用 rsync，失败返回退出码和诊断，不用其他后端冒充成功 |
| `scp` | 只用 scp |
| `base64` | 只用有界内存的 base64 上传，最多 16 MiB |

上传流程为：校验远程目标 → 创建父目录与独占 staging 目录 → 传输 → 校验长度 → 替换目标 → 清理 staging。失败不会用不完整文件覆盖已有目标。并发写同一目标仍是最后一次成功替换生效，不能代替版本管理。

Windows 的 rsync 本地操作数使用当前传输目录中的相对文件名，避免盘符冒号被识别为远程端点；远程参数使用 `-s` 保护。rsync `-e` 有自己的引号语法，见[官方参数说明](https://download.samba.org/pub/rsync/rsync.1)。Windows 优先使用 `.local/opt/cwrsync/bin` 原生 rsync 和配套 SSH，避免不兼容的 PATH 包装器。可用 `LRWORKSPACE_RSYNC` 指定其他原生 rsync 路径；该设置不会安装软件或修改全局 PATH。

Cygwin SSH 和 Win32 SSH agent 不保证互通。配套 SSH 读取用户 SSH config/密钥；无法使用其认证时会明确失败，auto 可回退到系统 scp。不要为排障把密码或私钥内容写入参数。

## 4. 执行与等待

| 工作区节点模式 | 默认执行 | 限制 |
|---|---|---|
| `submit_only` | cluster | 自动发现可用计算节点并 SSH；不可用时调度提交；拒绝登录节点 direct |
| `single_server`（存储兼容 `direct_compute`） | cluster | 允许显式 `--force direct`；CLI 配置用 `--node-mode single-server` |
| `unconfirmed` | 拒绝 workload | 先确认节点角色；仍可用只读查询 |

单次请求只保留三种入口：

```text
rw run --workspace research --force cluster -- bash scripts/run.sh
rw run --workspace research --force direct --timeout-secs 300 -- bash scripts/test.sh
rw ro --workspace research -- squeue -u alice
```

`cluster` 默认重新发现空闲计算节点并探测 SSH，自动选择可用节点，无需用户指定；没有可用节点时提交 Slurm/PBS。可用 `--compute-node compute01` 指定节点，`--execution-path submit` 强制提交。MCP 对应 `compute_node` 和 `execution_path`；发送实际命令后发生 SSH 异常不会回落登录节点或重复提交。batch 资源参数强制提交。`direct` 是单服务器执行，`read-only` / `rw ro` 是白名单轻量查询。详见 [执行与独立任务](docs/cluster-execution.md)。

`working_dir` 必须在 work 根内且存在。命令仍有 SSH 账户真实权限，没有 OS 沙箱，调用方必须限制持久写入在 work 根内。

默认等待为 **120 秒**：

- CLI：`--timeout-secs 300`，可放在子命令参数区，必须位于远程命令分隔符 `--` 之前。
- MCP run/write/submit/wait：`"timeout_sec": 300`。
- 用户 config：`ssh_timeout_secs = 300`；环境变量：`LRWORKSPACE_SSH_TIMEOUT=300`。CLI 显式参数优先。

已有配置中显式写的 60 秒仍被尊重。SSH keepalive 周期不是操作超时。作业提交成功只返回 job ID；调度器作业时限、排队时间和本机请求等待分别管理，120 秒不会截断排队任务本身。

超时不自动重放命令。提交结果丢失时可能已经提交，先核对队列再决定重试。

### 统一 Slurm/PBS 提交模式（0.3.0）

```text
# commands.sh 每个有效行是一条独立命令，输入来自 agent 自己的项目
rw submit --workspace research --in ./commands.sh --outdir JOBS --cpu 1 --maxnum 500 --maxcpu 2000 --submit
# PBS Pro/OpenPBS 与 Torque 使用同一入口，队列可选；识别不出版本时显式指定
rw submit --workspace research --in ./commands.sh --outdir JOBS --queue cpu --pbs-flavor torque --submit
# 单条命令也使用同一生成、上传、提交实现
rw run --workspace research --force cluster --cpu 4 --maxnum 20 --maxcpu 80 -- bash scripts/run.sh
```

省略 `--submit` 只生成远端文件。默认参数来自用户提供代码：nodes=1、ntasks=1、cpu=1、maxnum=1000、maxcpu=2000；Slurm 默认 partition=cpu64,cpu128，PBS 不猜测队列；mem/time 默认省略。每次创建新 run，不删除或复用旧 cmd/out/err。

MCP 调用 `workspace_submit`，提供 `local_path` 或 `commands_text`，资源放在 `batch` 对象中，`submit:true` 才提交；`workspace_run(force=cluster)` 也接收相同的 batch 参数。不能在命令文件中再嵌套 sbatch/qsub。完整算法、参数和 PBS 差异见 [统一提交模式](docs/batch-submit.md)。

## 5. 读取、索引与诊断

```text
rw read --workspace research --root work --file scripts/run.sh
rw read --workspace research --root data:0 --file samples.tsv
rw index --workspace research --root work
rw cluster refresh --workspace research
rw slurm status --workspace research --job-id 12345
rw slurm logs --workspace research --job-id 12345
rw audit --workspace research --limit 100
```

work/data 读取默认限制 1 MiB，超限需在远端生成小型摘要。索引在登录节点使用 Git 已跟踪及未忽略路径；非 Git 根拒绝递归索引。已知路径读取不要求先索引。`find`、`rg`、`sort` 等不属于只读命令入口，需要时按 workload 运行。

诊断 wait 默认预算 120 秒，可通过 timeout_sec 增大，未结束返回 still_running，证据不足返回 unknown，只有已知终态产生 final_state。服务并发处理请求，单个 wait 不阻塞其他命令和监控；SSH 查询和诊断收尾仍可能超出轮询预算。未知、队列消失和空日志都不能代替完成证据。

`rw env` 会更新远端 `~/.bin_path_file`，是 work 根外的明确 LRW 元数据写入。远端 startup 脚本行为和 watchdog 降级需在具体服务器验证。

## 6. 状态、移除与迁移

本地仅保存 `data_dir` 中的注册、缓存、审计和 journal，默认用户 `.lrworkspace`。CLI/MCP 应使用相同 `LRWORKSPACE_DATA_DIR`；CLI 顶层 `--data-dir` 可覆盖环境变量。

`rw remove --workspace research` 删除该工作区的注册/cluster/env/status 元数据，不删除本地项目、远程数据，不取消作业。审计与 journal 保留。

从 0.1.x 迁移：旧 JSON 中 local_temp_dir/local_cache_dir 被忽略，新保存不再写这些字段；不再创建或查找 marker。旧目录及文件不会自动删除。移除 `--local-temp-dir` 和 LRWORKSPACE_PROJECT_ROOT 的使用；已有项目级客户端配置若固定了过时 cwd，需要在客户端取消该绑定。老注册若保存了错误的默认 user/port，应核对并更新或重新注册，0.2.0 严格使用注册值。

## 0.3.2 的输出与查询限制

命令 stdout/stderr 默认各保留最多 10 MiB；超限后返回 `output_truncated:true`，退出码保持真实。截断不是命令失败，完整日志应由任务写入 work 内的独立文件，避免直接输出超大内容。

诊断 journal 和审计查询从最新记录开始，单次 `limit` 为 0–1000。文件不会被清空；查询遇到超过 16 MiB 的单条历史记录时明确报错，建议将大日志保存在独立文件中。

超时预算包含系统 SSH 的锁等待、命令写入、输出读取和传输的管道收尾；强制清理可能再耗时约 1.2 秒。超时后的作业结果仍需核对，不自动重放。运行中的旧 MCP 不会自动升级，切换新版程序后重连。

## 旧版 PBS 集群兼容注意事项（0.3.3）

LRW 注册中的端口会作为显式 `-p`/`-P` 传给 SSH，优先于 SSH config；旧注册如果仍是 22，不会自动继承主机别名中的 2048。确认实际端点后，可用 rw workspace --id <UUID> --port <PORT> 备份并修复旧注册。

Windows 配套 Cygwin SSH 的 ProxyCommand 需要 POSIX shell。LRW 只为 rsync 子进程选择配套 sh/bash，或现有 Git Bash；可用 `LRWORKSPACE_RSYNC_SHELL` 指定路径，不修改全局 SHELL。没有相应 shell 时需自行提供，不自动安装。

旧版 PBS 集群 使用 Torque 4.2.7，简短版本输出无法唯一识别方言时需显式设置 `--pbs-flavor torque` / MCP `batch.pbs_flavor:"torque"`。当前站点的数组作业会在启动器中触发 `zsh: no matches found: JOBID[INDEX]`；单个命令组现在按普通 PBS 作业提交并已验证成功。暂将多个步骤写在同一个脚本中，通过以下入口提交；不要把多行命令列表的数组模式当作已在该站点验证通过。

```text
rw run --workspace research --force cluster --pbs-flavor torque --queue batch --cpu 1 --maxcpu 1 -- bash scripts/run.sh
```

PBS 日志路径通过真正的 qsub 参数传递；简单路径也保留无引号的脚本指令。Torque 使用 out/scheduler、err/scheduler 前缀，数组子任务附加的索引仍留在对应日志目录中。含空格/引号的复杂路径依赖 LRW 提交参数，手动 qsub 导出脚本时需自行传入 -o/-e。

## 独立后台命令（0.6.0）

MCP `workspace_run` 默认返回独立 `task_id`，可连续或并发启动多个任务。用 `workspace_task_status` 查实时状态与退出码，`workspace_task_logs` 查日志尾部，`workspace_tasks` 列出当前工作区句柄。需要同步输出时设 `background:false`。调度提交仍使用 `job_id` 及原有 scheduler 工具。

```text
rw run --workspace research --background -- bash scripts/a.sh
rw run --workspace research --background -- bash scripts/b.sh
rw tasks --workspace research
rw tasks --workspace research --id <UUID> --logs
```

CLI 保留默认前台输出。后台任务的 SSH 启动/监控请求仍默认等待 120 秒，但任务本身不会因此被截断。独立脚本与 stdout/stderr 保存在 work 根的 `._lrworkspace_tasks/<task_id>/`，MCP 重启后仍可查询；日志读取默认每路 16 KiB、最多 64 KiB。unknown 不算成功，不应盲目重跑。

0.6.0 起所有 workload 默认 cluster，包括旧 `direct_compute` 注册。确认的单服务器可显式用 `--force direct` / `force:"direct"`，登录节点禁止 direct。
