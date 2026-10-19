# Windows / macOS 安装与环境检查（0.4.0）

使用自己平台的完整压缩包，三个程序保留在同一目录。Windows 使用 x86_64；Apple Silicon 使用 macOS aarch64；Intel Mac 使用 macOS x86_64，Mac 构建的最低系统目标为 macOS 12。运行发布程序不需要 Rust、Python、Git 或 WSL。基本功能需要本机 OpenSSH 的 ssh/scp；远端仍面向有 Bash、GNU 工具和可选 Slurm/PBS 的 Linux。

Windows 已实际验证；Apple Silicon 和 Intel Mac 已通过 GitHub macOS runner 上的构建、完整测试、MCP 集成及隔离安装验收。尚未在用户自己的 Mac 上完成真实集群连接测试；CI 结果不替代其 SSH 配置与认证验收。

## 安装

在解压目录执行，或使用程序的完整路径：

```powershell
# Windows PowerShell
.\rw.exe doctor
.\rw.exe mcp install --client codex
```

```sh
# macOS Terminal
./rw doctor
./rw mcp install --client codex
```

Cursor 用 `--client cursor`，两个客户端用 `--client all`。默认只配置 Codex。程序复制到当前用户 `.local/share/lrworkspace/versions/<版本>-<平台>-<架构>`，配置固定程序与数据目录的绝对路径，不依赖图形客户端的 PATH 或当前目录。安装输出 CLI 的完整路径；需要时自行加入 PATH。

已有 LRW MCP 的旧程序路径会更新，保留其他服务、环境变量、较长超时及 Codex 注释；修改前在原配置旁保存 `.bak`，损坏配置或自定义不兼容参数会停止安装。相同版本的不同构建使用新的 `--install-dir <绝对路径>`。旧程序、工作区和任务不删除。当前请求结束后重新连接 MCP。

Codex 使用 `$CODEX_HOME/config.toml`（未设时为用户 `.codex/config.toml`）；Cursor 使用用户 `.cursor/mcp.json`。其它 MCP 客户端用 `rw mcp show` 获取配置示例，并注册 `lrworkspace-mcp` 的绝对路径。

默认命令预算 120 秒，CLI `--timeout-secs`、MCP `timeout_sec` 可改。安装时 Codex `tool_timeout_sec` 至少为命令预算加 30 秒（默认 150 秒），保留更大值。客户端或电脑连接层自己的等待限制需独立配置；客户端等待超时不代表提交失败，先查询作业状态，避免重复提交。

## 每个用户配置自己的 SSH

以下账户、主机、节点、地址和路径均为虚构示例，需替换成自己的设置。

先保证自己的系统 SSH 能用密钥或 agent 非交互登录。LRW 使用各自系统的 SSH 配置、known_hosts 与认证，不分发任何人的注册、密钥或代理脚本。注册时省略端口/用户会用 `ssh -G` 解析 Include、Match 和 Host；显式参数优先。

```text
rw init --host my-cluster --remote-work-dir /scratch/alice/project --name research --node-mode submit-only --yes
rw doctor --workspace research
```

`doctor` 只检查本机，不连接服务器，也不读取私钥内容。`base_ready` 表示本机 ssh/scp 可用，不等于认证、网络和调度器通过。已有注册端口错误会显示 `port_mismatch`；确认正确端口后执行 `rw workspace --id <UUID> --port 2222`，自动备份旧注册。MCP `workspace_update_connection` 默认预览，`confirmed: true` 才保存。

需要本地无认证 SOCKS5 代理时，可用程序内置转发替换私人 Python 脚本。在自己的 SSH config 配置，例如：

```sshconfig
Host my-cluster
    HostName 192.0.2.20
    Port 2222
    User alice
    ProxyCommand "C:/Users/alice/.local/share/lrworkspace/versions/0.4.0-windows-x86_64/rw.exe" proxy --proxy 127.0.0.1:1080 --host %h --port %p
```

Mac 将程序位置替换为安装输出的绝对路径（没有 `.exe`）。代理地址/端口使用自己的设置；没有代理就省略 ProxyCommand。内置转发只支持无需用户名/密码的 SOCKS5；其它认证代理沿用合适的 ProxyCommand。代理只向 stdout 转发二进制，诊断走 stderr，握手默认 10 秒，可用 `--connect-timeout` 改。

## rsync 为可选能力

`auto` 可在缺少/不兼容 rsync 时使用原生 scp/base64；显式 `--transport rsync` 失败即报错。两端需有 rsync，本机要求 >=3，以支持受保护的特殊文件名。Mac 自带旧版 2.6.9 不适用；可自行安装新版（例如 Homebrew），LRW 自动检查 `/opt/homebrew/bin/rsync`、`/usr/local/bin/rsync`。

Windows rsync 使用匹配的原生发行版和配套 SSH。Cygwin SSH 的代理需要 sh/bash；LRW 优先配套 shell，其次 Git Bash，或显式 `LRWORKSPACE_RSYNC_SHELL`。基本 scp/MCP 功能没有 Bash/Git 依赖。Cygwin SSH 与 Windows agent 不保证互通，按其自身认证要求配置。不会自动安装依赖或修改系统 PATH/SHELL。

| 可选环境变量 | 用途 |
|---|---|
| `LRWORKSPACE_DATA_DIR` | 元数据与审计绝对路径，安装时固定到 MCP 配置 |
| `LRWORKSPACE_HOME` | LRW 元数据/安装的用户根；不改变系统 SSH 的 home |
| `LRWORKSPACE_CONFIG` | 显式 LRW 配置文件；该文件必须存在 |
| `LRWORKSPACE_SSH_CONFIG` | ssh/scp/rsync 共同使用的 SSH 配置文件（建议绝对路径） |
| `LRWORKSPACE_SSH` / `LRWORKSPACE_SCP` | 指定可执行程序；默认优先操作系统原生 OpenSSH |
| `LRWORKSPACE_RSYNC` | 指定 rsync 程序绝对路径 |
| `LRWORKSPACE_RSYNC_SHELL` | Windows rsync 子进程使用的 POSIX shell |
| `LRWORKSPACE_SSH_TIMEOUT` | 默认命令预算秒数，须大于 0 |

不要复制他人的 `.lrworkspace` 注册或 SSH 私钥。登录节点默认仍通过 Slurm/PBS 提交；只有本次明确指定 `--compute-node` 才经登录节点 SSH 到计算节点。`direct` 用于单服务器。
