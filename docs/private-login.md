# 本机私密密码登录（0.5.1）

没有 SSH config 时，在 Windows / macOS 桌面的 MCP 客户端中调用 `workspace_login`。LRW 弹出单独的本机窗口，由用户填写 IP/DNS、端口、SSH 用户名、密码、远端工作目录及可选 SOCKS5 代理。默认选择集群登录节点；单服务器需在窗口中明确选择。点击“连接并授权”后才验证连接和目录，成功后保存工作区注册。

```text
workspace_discover
workspace_login {}                       # 新建；工具立即返回 request_id
workspace_login_status {request_id: ...} # awaiting_user / connecting / authenticated / cancelled / failed
workspace_login {workspace_id: ...}      # 已有工作区重新登录
workspace_logout {workspace_id: ...}     # 撤销当前 MCP 进程的密码会话
```

以上是工具名及参数示意。密码不进入 MCP 参数、聊天或 elicitation 表单。agent 不应截图、读取窗口内容或代填凭据。首次连接未知服务器时，另一个本机窗口显示 OpenSSH 主机指纹，由用户核验并确认；不会自动接受未知或已变化的主机密钥。

## 用户可以看到什么

- Windows 默认遮蔽密码；系统确认窗口捕获保护可用时，可勾选“显示明文预览”。预览自行绘制，其辅助功能值固定为 `[REDACTED]`，实际输入框始终是密码框。保护不可用时禁用明文预览。
- macOS 使用 `NSSecureTextField` 并保持遮蔽。现有普通窗口接口不能可靠承诺阻止屏幕采集，因此不提供明文预览。
- MCP 只返回请求编号、状态和工作区编号。IP、用户名、目录等连接元数据会像已有注册一样保存并可供 agent 查询；密码不会保存到注册文件。

## 凭据与传输

原生窗口通过私有 stdin 管道将表单交给相同的 MCP 程序的短生命周期 helper，再经证书固定的临时 TLS 回环连接交给当前 MCP 进程。程序只信任该次启动生成的证书，握手验证后才发送表单；无需安装证书或修改系统信任库。入口使用一次性随机路径、长度上限和绝对 I/O 截止时间，并拒绝浏览器 Origin。

OpenSSH 使用 `SSH_ASKPASS_REQUIRE=force`，另一个受随机能力令牌和证书固定保护的本机 TLS 通道只向其私有 askpass 管道提供密码。密码不进入命令行、环境变量、临时文件或 MCP 标准输出；子进程环境中的能力令牌也不会返回给 agent。使用本机 OpenSSH，运行时无 Python、额外密码工具或自建 SSH 服务器依赖。

会话绑定主机、端口、用户名及代理。该登录路径使用 `-F none`，不会执行用户 SSH config 中的 Match/ProxyCommand；主机密钥保存在 LRW 数据目录的 `private-auth/known_hosts`。用户名接受字母、数字、点、下划线和连字符，密码最多 1023 UTF-8 字节，避免 OpenSSH askpass 截断。SOCKS5 仅支持数字 IP:port、无需认证的代理。支持原生 SSH、SCP 及兼容的 rsync；Windows Cygwin rsync 仍需配套 SSH/shell，失败遵循已有显式传输规则。Windows 非 ASCII 安装路径通过助手目录与相对程序名绕过原生 OpenSSH 环境变量的编码差异；如果原生 rsync 需要保留不同工作目录，则使用配套 Cygwin SSH 或 SCP，不能改动上传源目录。

## 会话与边界

- 密码只在创建它的 MCP 进程中可用。不同客户端不共享密码；重启、重新连接 MCP 或 CLI 独立进程均需重新登录，CLI 不能复用 MCP 密码会话。无桌面的 Linux/服务端仍使用密钥或 SSH agent。
- 注销立即撤销后续认证并移除连接缓存，不删除注册或远端文件，不取消作业；已在运行的请求可能按原预算结束。重新登录创建独立连接代次，不能借用旧的 SSH master 绕过密码验证。
- 工具输出、错误、审计及日志会过滤当前和此前登录密码的原文及若干常见编码；包含匹配项的整个字符串会被抑制。短密码可能误遮蔽正常输出。为过滤注销后的迟到输出，密码过滤模式保留在进程内存至退出，最多 128 个不同密码；并行会话最多 32 个。注销撤销使用权，不承诺立即清除所有内存副本。
- 仅支持 SSH `password` 认证，不自动处理键盘交互、OTP/MFA、密码到期修改或私钥口令。需要这些机制时沿用系统 SSH agent/密钥配置。
- **这保证的是正常 MCP 数据流不把密码发给模型，不是针对同权限程序的隔离。** 拥有任意本机执行、调试、内存读取或修改程序权限的 agent 可以绕过应用边界。Windows 捕获保护也是辅助措施，不能保证防摄像、管理员监控、所有录屏、崩溃转储或系统交换文件。需要这种强隔离时，应使用独立权限账户或外部认证代理。

集群任务默认自动选择可用计算节点走第二跳 SSH，没有可用节点或明确要求调度资源时使用 Slurm/PBS 提交；密码登录不会改变计算位置、文件 API 根边界或真实 SSH 账户权限。

0.5.1 的旧字段迁移规则见[隐私修复](release-privacy.md)：旧 `key.passphrase` 只在本次进程内兼容，保存注册或新建注册备份时省略；库后端不再读取密码环境变量。已有私人备份不会自动删除或改写。
