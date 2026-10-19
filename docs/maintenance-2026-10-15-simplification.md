# LRW 0.2.0 维护与验收记录

日期：2026-10-15。代码基线：`f2bd915`。本轮按用户要求合并计算入口、修复 rsync、移除本地项目绑定、调整等待参数，并复核相关缺陷。

## 当前行为

1. **计算入口合并**：cluster 统一经 Slurm/PBS 分配计算节点；compute-node 是隐藏兼容别名，直接 SSH 到空闲节点的代码已移除。CLI/MCP 共用执行路由；submit_only 默认 cluster 并拒绝 direct，direct_compute 默认 direct。无调度器时不回落登录节点。
2. **agent 使用自己的项目**：删除 Workspace/local layout 中的 local_temp_dir、本地 marker 创建/查找、LRWORKSPACE_PROJECT_ROOT、项目级安装规则和 cwd 绑定。按 UUID/唯一名称选远程工作区。MCP local_path、CLI --from 可直接上传 agent 项目文件，不要求额外本地项目。
3. **rsync 传输**：本地操作数改相对文件名，避免 Windows 盘符冒号被当成远程端点；采用 -s 与正确的 rsync -e 引号语法；优先原生 cwRsync 与配套 SSH，支持 LRWORKSPACE_RSYNC 指定程序。子进程输出全部捕获，有总时间预算及进程树终止。
4. **上传闭环**：先创建父目录和独占 staging；rsync/scp 流式读取本地文件；核对长度后替换目标，保留已有目标模式，失败不以不完整文件覆盖旧目标；清理本次 staging。auto 可回退，显式 transport 失败即报错，并报告实际后端。base64 内存回退限制为 16 MiB。
5. **默认等待 120 秒**：CLI --timeout-secs、MCP run/write/wait 的 timeout_sec 可修改。用户配置及环境变量仍可设置默认值。移除旧控制命令硬上限和等待工具的 120 秒固定封顶。传输超时后清理最多额外使用 5 秒；诊断轮询另有在途查询/收尾时间。

## 同时修复的缺陷

- 损坏注册信息不再静默跳过，避免自动选错其他远程项目；错误包含记录 ID 和路径。
- 工作区和缓存状态改为写临时文件、sync、rename，避免普通写入失败直接截断旧状态。
- 注册时解析默认身份/端口；执行、传输、连接复用 key 和状态标签一致使用保存值，不被后续 SSH config 的 User/Port 再覆盖。
- PBS 作业 ID 保留服务器和数组后缀；不把普通 warning 数字误当作作业 ID。
- 显式上传参数互斥且校验类型，允许 content 为空；MCP 支持 ping。
- 完整下载的 base64 备用路径遇到输出截断会失败，避免返回损坏文件。
- 移除未使用 SessionPool 占位和命令文本路由猜测；移除运行时自动 sudo 安装 OpenSSH；不改动本机系统配置。

## 已完成验证

环境：Windows，Rust/Cargo 1.98.1，GNU 工具链。

```text
cargo check --workspace --all-targets --locked --offline
cargo test --workspace --locked --offline
cargo test -p lrworkspace-remote real_rsync_roundtrip --locked --offline -- --ignored --nocapture
cargo build --release --locked --offline -p lrworkspace-cli -p lrworkspace-mcp
```

- 常规测试 **99 项通过**，0 失败；其中本机 rsync 可选测试单列为 ignored。
- 显式运行真实 rsync 可选测试 **1 项通过**：1,048,593 字节二进制内容，空格/引号/中文源与目标路径，往返内容逐字节一致。
- Windows 的真实 rsync 验证使用本地模式。Unix 测试通过本机 fake-SSH helper 启动 rsync server 模式，已配置到 Linux CI，本次未在 Linux 执行。
- 本地 shell 测试验证不完整文件拒绝替换旧目标、上传目录/参数边界、临时文件保留与清理；子进程超时测试验证本次进程终止。
- 普通测试包含 CLI 超时参数位置/非法值、MCP schema/输入/ping、旧字段兼容、显式工作区选择及执行路由。
- Release 构建成功；依赖锁仅更新 6 个本地 crate 到 0.2.0，无第三方依赖升级。

原始验证日志与产物保留在本机私有记录中，不随源码发布。

## 已知限制与迁移

- **真实 SSH、远端 rsync 与 Slurm/PBS 未联调**；不能把本地 rsync 往返当成集群验收。Windows 原生模拟 SSH 的 Cygwin 管道测试出现不兼容，故 Windows 使用可靠的真实本地模式验证，记录该边界而不声称 SSH 通过。
- 配套 Cygwin SSH 与 Win32 ssh-agent 不保证互通。需要实际账户认证测试；auto 可回退系统 scp，显式 rsync 会保留失败诊断。
- 服务仍串行处理请求；等待参数不会改变客户端自身的 tool timeout，新生成安装配置留有余量，已有客户端配置需自行匹配。
- 无 OS 沙箱、并发 symlink 隔离或多写者冲突解决；日志缺失仍可能表现为空返回。状态明确不代表输出已经核验。
- 老配置中的 local_temp_dir/local_cache_dir 可读取但被忽略；旧目录/marker 不自动删除。旧项目级客户端配置如固定过时 cwd，需要移除绑定；旧注册若错误保存默认 user/port，需核对更新或重注册。
- rustfmt/clippy 组件当前未安装，未把这些检查视为通过。GitHub CI 未在本次远端运行。
- 没有运行安装器或替换用户当前加载的 CLI/MCP 进程；0.2.0 构建产物可在审阅后部署。

使用与迁移见 [usage.md](../usage.md)，服务器验收见 [开发与验收](development.md)。rsync 参数语义依据 [官方 manpage](https://download.samba.org/pub/rsync/rsync.1)，Windows 配套发行包参考 [cwRsync](https://itefix.net/cwrsync)。
