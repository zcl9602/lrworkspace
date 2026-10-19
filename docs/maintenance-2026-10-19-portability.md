# 0.4.0 环境可移植性维护与验收

基线：0.3.3，维护分支 `maintenance/2026-10-19-portable-runtime`。不删除已有发布程序、注册或任务输出。

## 实现

- 安装使用用户自己的版本目录和绝对路径，不依赖项目目录/GUI PATH；CLI/MCP 版本匹配后再安装，修复旧 tokenfix 等命令路径。保留其他 MCP 服务与配置注释、环境变量、较长超时，备份后原子写入。当前进程不强制终止。
- Windows 原生系统 ssh/scp 优先于 PATH 中 Git/Cygwin 工具；macOS 可从 `/usr/bin` 找到系统 SSH。显式环境变量仍优先。注册调用原生 `ssh -G` 解析 Include/Match/Host；MCP 不再把省略端口默认填写为 22。
- `rw doctor` / `workspace_doctor` 提供本机依赖与配置诊断；端口冲突只提示，需显式确认修复并备份。SSH 配置覆盖同时传入命令、scp、rsync。
- 内置 `rw proxy` 无认证 SOCKS5 二进制转发，不依赖私人 Python 脚本；分片握手共用截止时间，二进制包即时刷新，避免 SSH 密钥交换卡住。
- 本机 rsync >=3 才启用受保护参数；旧版/缺失时 auto 可用 scp/base64，强制 rsync 则明确失败。Mac 检测两种 Homebrew 位置；Windows 保留配套 Cygwin SSH/shell 的可选依赖。
- Unix SSH 控制套接字使用短路径、按系统用户隔离的 0700 目录，避免 Mac 临时目录长度超限及用户间目录冲突；自定义 SSH 配置参与套接字区分。
- CI 覆盖 Windows、Ubuntu、Apple Silicon 与 Intel Mac；release 工作流为三个用户平台构建、隔离安装验收并输出来源提交与 SHA256。Mac 最低构建系统目标为 12.0；Windows MSVC 使用静态 CRT。

## 当前验证

执行端为 Windows x86_64。147 个常规测试（含文档测试）通过；6 个显式测试另行通过：3 个 MCP 集成、1 个隔离安装、1 个真实本机 rsync、1 个提供的 Perl 参考契约。优化构建的完整 portable 验收也通过。源码只在当前 Windows 构建；没有 fmt/clippy 组件，未宣称通过相应检查。

隔离用户验收包含中文/空格路径、独立 CODEX_HOME、旧 MCP 配置升级和最小 PATH。安装后的 MCP 无 Rust/Python/Git PATH、无 rsync 时完成初始化与 doctor；Windows 产物导入表只有系统 DLL，没有额外 Rust/Git/Python 运行库。

独立 SSH 配置和隔离注册下，原生 SOCKS5 代理的连接/目录检查通过。强制 rsync 上传131101字节的中文空格文件，实际返回 transport=rsync，远端只读 SHA256 与本地一致。发布版 MCP 进程完成初始化、discover、doctor及远端只读校验。

本次未额外提交作业，未改动已有用户任务。此前已验证的 PBS 普通作业与站点数组启动器问题仍见[匿名化验收记录](maintenance-2026-10-19-proxy-pbs.md)。本机原始证据、地址、校验值和测试文件标识仅保存在私有记录中。

## 尚未验证

当前未提供 Mac 实机，macOS 两种架构的 CI/打包配置已完成，远端 CI 尚未运行，不能称为 Mac 实机通过；也未覆盖所有 Windows 版本、代理认证方式、SSH agent 组合、服务器或大文件/断线重传。新用户应使用自己的 SSH 配置和认证，先 doctor，再对自己的工作区完成连接与小文件验收。
