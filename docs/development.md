# 开发与验收

开发需 Rust 1.89 或更新版本（使用标准库跨进程文件锁），发布程序运行时不需要 Rust。

## 本地检查

```text
cargo check --workspace --all-targets --locked
cargo test --workspace --locked
python scripts/build_release.py --verify-install
```

缓存完整可加 `--offline`。有相应组件时再运行 `cargo fmt --all -- --check` 和 `cargo clippy --workspace --all-targets --locked`；缺失组件不能视为通过。

Mac 开发环境运行完整测试前，需要用 GNU 工具模拟 Linux endpoint。安装 `coreutils`、`gnu-tar`，并将它们的 `libexec/gnubin` 加到测试 PATH（CI 自动准备）：

```sh
brew install coreutils gnu-tar
export PATH="$(brew --prefix coreutils)/libexec/gnubin:$(brew --prefix gnu-tar)/libexec/gnubin:$PATH"
```

这是运行本机 fake-SSH/远端脚本测试的开发依赖；Mac 发布程序连接 Linux 服务器时仍使用服务器上的 GNU 工具，不要求本机安装这些包。超时回归同时检查 Timeout 错误类型和实际返回耗时，不依赖某个平台的报错措辞。

真实 rsync 本地回归测试需要可运行的 rsync：

```text
cargo test -p lrworkspace-remote real_rsync_roundtrip --locked -- --ignored --nocapture
```

Windows 该测试使用原生 rsync 的本地模式校验二进制和特殊文件名往返；Unix 通过一个只启动本机 rsync server 子进程的 fake-SSH helper 覆盖协议传输。两者都不连接远端服务器。Windows 模拟 SSH 的原生/Cygwin 管道不等价于真实配套 SSH，不将该测试宣称为服务器联调。

CI 配置在 Ubuntu、Windows、macOS Apple Silicon（macos-15）、macOS Intel（macos-15-intel）运行普通测试、显式 MCP 集成及隔离用户安装验收，并在 Ubuntu 显式运行 rsync 协议回归。release 工作流在这四个平台使用路径匿名化构建、发布版隔离安装与密码认证测试，再检查并打包原生程序、来源提交与 SHA256；Windows MSVC 使用静态 CRT。具体结果以对应提交的 CI 为准；托管 runner 的验证不替代用户 Mac 到真实集群的连接验收。

```text
cargo build -p lrworkspace-cli -p lrworkspace-mcp --locked
cargo test -p lrworkspace-cli --test portable --locked -- --include-ignored --nocapture
```

portable 测试使用全新中文/空格用户目录、独立 CODEX_HOME、旧客户端配置和精简 PATH，验证安装后的 MCP 初始化、环境检查以及无 rsync 时基本能力。另覆盖原生 OpenSSH Include 配置、端口冲突诊断及备份修复、二进制 SOCKS5 relay。Windows 只收紧新建 Include 测试文件的 ACL，不修改用户 SSH 配置。

## 回归契约

0.5.0 的普通测试覆盖本机 UI 控件、证书固定、能力令牌与端点绑定、状态/注册/审计/日志脱敏和注销。CI 四个平台另用 Paramiko 合成 localhost 服务验证实际 OpenSSH 正确/错误密码；Windows/macOS 验证原生窗口到 Rust helper 的私有表单交接。Python/Paramiko 仅是测试依赖，安装包不需要它们。Ubuntu 还显式启动一次性 sshd/临时系统账户验证密码模式的 SCP、rsync 二进制上传、未知主机拒绝和注销；此测试须设置 `LRW_RUN_ISOLATED_SSHD_TEST=1`，仅可在可丢弃 runner/container 执行。原生控件自检不代替人工 GUI 操作或用户真实集群认证验收。

- cluster 自动发现并探测空闲计算节点；没有可用节点或提供 batch 资源时调度提交。实际 workload 失败不回落/重提。旧 --force compute-node 仍是 cluster 别名。
- CLI/MCP 共用节点模式决策，不按命令文本猜测计算地点。
- 无本地项目目录/marker/cwd 绑定，旧字段可读取但不创建旧目录。
- 文件上传的子进程输出必须捕获，显式 rsync 不允许回退伪装成功。
- 新子目录先创建；短文件、超时和失败不覆盖旧目标；特殊文件名不能改变命令含义。
- 默认等待 120 秒，CLI/MCP 可覆盖并拒绝 0/非法类型。
- 工作区损坏要指出记录，不能跳过后自动选中其他项目。
- PBS 作业 ID 保留 server/array 后缀，不能把 warning 中的数字当 job ID。
- 保留完成证据边界和实际账号权限，不把文件 API 当 OS 沙箱。

## 真实服务器验收

用户指定服务器后，只使用独立的个人测试 work/data 根和测试作业。workload 通过调度器或用户明确允许的计算节点 SSH 执行，不在登录节点执行。

| 检查 | 预期 |
|---|---|
| rsync/scp 上传新子目录与覆盖测试文件 | 内容一致，报告实际传输方式，无残留 staging |
| 非 ASCII、空格、引号及二进制内容 | 内容与本机校验一致 |
| 显式 rsync 无法认证/远端缺 rsync | 明确报错；不回退伪装 rsync 成功 |
| 传输过程中超时/断连 | 返回失败、终止本次子进程；保留原目标 |
| 路径逃逸、data 写入口、根外 symlink | 文件 API 拒绝且测试 data 不变 |
| cluster + execution_path=submit | 调度器返回 job ID，实际 hostname 是分配的计算节点 |
| cluster + 显式 compute-node | 经登录节点第二跳，实际 hostname 为指定节点，保留命令退出码 |
| SSH 失败/别名回到登录节点 | 拒绝，不回落或重复提交 |
| submit_only + direct | 拒绝 |
| 无调度器的 cluster 提交路径 | 拒绝，不回落登录节点 |
| 作业未知、历史不可用或日志为空 | 不自动当完成 |
| 移除测试注册 | 项目文件和作业保留 |

记录提交版本、平台、身份/端口、节点模式、命令、退出码、作业 ID、实际 hostname 和文件校验。不要用真实共享数据试写；本地测试通过不替代具体集群联调。

## 命令数组回归

```text
cargo test -p lrworkspace-remote batch::tests --locked
cargo test -p lrworkspace-cli --test native_batch --locked
cargo build -p lrworkspace-cli -p lrworkspace-mcp --locked
cargo test -p lrworkspace-cli --test native_batch mcp_command_array --locked -- --ignored --nocapture
cargo test -p lrworkspace-cli --test native_batch mcp_cluster_ssh --locked -- --ignored --nocapture
```

测试使用实际 Bash/tar 和本机模拟 SSH/sbatch/qsub，不连接服务器或真实调度器。覆盖 2501 条命令分为 1000 组的完整性、CPU 限额、失败后继续执行、三种调度脚本、重复执行保留日志、CLI 只生成/提交、MCP Torque 提交和审计失败仍返回 job ID。普通集成测试使用 Cargo 的临时目标目录；只清理本次测试创建的文件。

真实服务器还需核对：Slurm MaxArraySize 是否允许索引 1000、站点的数组并发限制、PBS 方言/历史保留、GNU tar、共享文件系统和队列名。实际验收只使用独立测试根，不把本机模拟提交当成集群接受或任务运行证据。

## 0.3.2 内存与异常回归

普通测试增加输出限流/帧分块、锁与 stdin 超时、继承管道 EOF、历史尾读及跨月审计、流式归档一致性。显式 MCP 回归：

```text
cargo test -p lrworkspace-cli --test native_batch mcp_truncated --locked -- --ignored --nocapture
```

此测试在同一个 MCP 进程中先接收超过限制的输出，再执行下一条命令，校验真实退出码和会话帧未被破坏；不连接服务器。Windows 调用真实本机 rsync 往返的测试也应显式执行，不能把 fake SSH 测试视为真实认证验收。

## 0.6.0 并发回归

```text
cargo build -p lrworkspace-cli -p lrworkspace-mcp --locked
cargo test -p lrworkspace-cli --test native_batch mcp_concurrent --locked -- --ignored --nocapture
```

真实 stdio 进程与本机模拟 SSH 验证：同一主机长命令被门控等待时，第二命令和 ping 可返回；单次超时不终止其他命令；默认自动选择计算节点；两个后台任务日志和退出码独立，重启 MCP 后仍可查询。表单未完成时 ping 和工具列表也须响应。Mac 本地测试通过专用的 setsid helper 模拟 Linux 进程会话，不需要用户机器安装 util-linux；真实远端仍需 Linux setsid。CI 显式执行这些集成测试。
