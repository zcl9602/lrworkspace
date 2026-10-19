# LRW 维护记录 — 2026-10-15

本次在现有 Git 仓库上进行全面维护，保留 remote-first、work/data 根边界和 submit-only 设计。没有连接真实服务器、提交集群作业或改动用户已安装的 MCP/CLI 配置。

- 维护分支：`maintenance/2026-10-15-tool-hardening`。
- 未修改依赖版本或 `Cargo.lock`。

## 修复内容

| 范围 | 修复后的行为 |
|---|---|
| Slurm/PBS 状态 | 空记录与未知状态不再当作成功；Slurm 数组合并状态，考虑退出信号；PBS E 保持运行中，C/F 根据 Exit_status 判断 |
| Slurm 日志 | 优先 scontrol，再尝试 sacct 和 LRW 唯一任务名；保留含空格路径；直接读取日志尾部，避免先扫描整个文件 |
| 提交结果 | sbatch 成功但无法解析作业 ID 时明确报错，并提示先查调度器，避免盲目重试重复提交 |
| 分区发现 | 按 sinfo 实际字段解析默认分区与节点列表；未提供的每用户作业上限保留未知 |
| 文件读取 | CLI/MCP 先检查文件类型与大小，并最多读取上限加一个字节；拒绝超限、读取期间大小变化或截断结果 |
| 文件写入 | 校验后的 canonical 路径贯穿传输后端；拒绝目录目标；base64 回退使用 mktemp 独占临时文件，保留已有同名 .tmp 文件 |
| 只读命令 | 保守移除有写入/执行副作用的工具，拒绝变量展开、重定向、进程替换和命令串联；常用查询与简单只读管道保留 |
| CLI | 同步执行失败返回非零退出状态；check 失败不再成功退出；只读执行无需先发现集群；authorize 必须选择工作区 |
| 配置 | CLI 参数优先于环境变量；MCP 也读取 LRWORKSPACE_CONFIG；显式配置路径缺失和非法数值明确失败 |
| 工作区元数据 | 默认临时目录按 UUID 隔离；阻止覆盖其他工作区 marker，删除前核对归属 |
| MCP | Unknown 不产生 final_state；日志尾部保持原始顺序；表单等待期间保存交错请求和通知，确认后按序处理 |
| 安装器 | JSON 配置损坏或结构错误时拒绝写入，保留其他 MCP server 和用户配置 |
| 文档/CI | 重写 README 和使用说明，补架构与验收文档，添加 Ubuntu/Windows 测试工作流 |

行为变化：`find`、`rg`、`sort`、`tree`、`file`、`date`、`hostname`、`blkid`、`lsof`、`zcat`、`zgrep` 不再属于只读白名单，`2>&1` 也不再例外放行。需要这些命令时使用正常 workload 路由。完整文件下载 API 保留；CLI/MCP 的小文件读取采用有界 SSH 输出。

## 已完成验证

环境：Windows，Rust/Cargo 1.98.1，`stable-x86_64-pc-windows-gnu`。最终命令在原始仓库目录执行，构建输出通过 CARGO_TARGET_DIR 隔离。

```text
cargo test --workspace --locked --offline
cargo build --release --locked --offline -p lrworkspace-cli -p lrworkspace-mcp
git diff --check
```

- 90 项测试执行通过：84 项单元测试（包含两个 CLI 名称的相同实现）、5 项入口集成测试、1 项文档测试；0 失败。
- 新增测试覆盖只读绕过、文件大小边界、临时文件保留、未知作业、日志路径、配置优先级、marker 归属、CLI 退出码、MCP stdio 和表单交错消息。
- 文件传输生成脚本通过本机 Git Bash 实际执行；没有连接远程。
- Release 构建成功；两个 CLI 入口可启动，Release MCP 初始化成功并返回 26 个工具定义。
- 调度器、文件边界和入口修改经分工审查与交叉复核。Git 差异检查通过。

原始验证日志与产物保留在本机私有记录中，不随源码发布。

## 验证限制与后续工作

1. 真实 SSH、Slurm/PBS 与 Linux/macOS 未在本次联调；新增 GitHub CI 仅已配置，未在远端运行。Unix 专属 symlink/watchdog 测试需在 Linux 环境执行。
2. 本机未安装 rustfmt/clippy，对应检查未运行，不能视为通过。
3. 日志文件不存在或不可读时，当前日志接口仍可能返回空内容；后续可增加明确的日志可用状态，避免与正常空日志混淆。
4. MCP 仍串行执行请求；表单交错请求不会丢失，但要等表单返回后处理。轮询预算不包含全部在途 SSH 耗时。
5. 文件路径校验不构成 OS 沙箱或对并发路径替换的完整隔离。用户命令仍按 SSH 账户真实权限运行。

真实服务器验收流程见 [开发与验收](development.md)。Slurm 分区字段参考 [官方 sinfo 文档](https://slurm.schedmd.com/sinfo.html)。
