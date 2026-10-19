# 0.3.1：cluster 增加显式 SSH 计算节点路径

- 默认 cluster 继续使用 0.3.0 原生命令数组提交，保留 Slurm / PBS Pro / Torque。
- CLI `run --force cluster --compute-node HOST`、MCP `workspace_run(force=cluster,compute_node=HOST)` 从注册登录节点第二跳 SSH，不自动选节点。
- 每次显式节点参数作为本次选择，不持久授权；无参数仍提交，SSH 失败不回落/重提。
- 校验 hostname、防 SSH 参数注入，拒绝相同端点并比较第二跳真实短主机名，计算节点上校验 canonical work 根和 cwd。
- 不申请 scheduler 资源，混用 batch 参数明确拒绝。SSH 使用登录账户已有非交互认证，保持主机密钥验证，不转发本机 agent。
- 返回 execution_path、compute_node、stdout/stderr/退出码，审计记录执行路径，CLI 不将 SSH 命令失败误报为提交成功。
- direct 明确用于单服务器；新增 single-server/single_server 配置别名，兼容原有 direct-compute/direct_compute 注册。

验收：118 项普通测试通过，额外显式运行 2 项 MCP 本地模拟集成通过（PBS 数组提交及 cluster 第二跳）。全 workspace/all-targets 检查和 Windows release 构建通过。测试使用本机 fake SSH/scheduler，未连接真实服务器；不代表真实集群认证和挂载已验收。没有安装 rustfmt/clippy，未宣称这些检查通过。

新增测试覆盖第二跳命令引号与空格目录、退出码保留、SSH 不可达、登录节点别名保护、目录/参数冲突、未确认角色、MCP 资源冲突；既有默认 Slurm/PBS 数组测试继续通过。Windows 模拟层处理 Git Bash 的环境路径转换，生产执行不依赖该模拟层。

便携程序单独保存到 target/lrworkspace-0.3.1-windows-x86_64；旧版本和现有客户端配置保留。使用新 MCP 二进制并重连后才能获得 compute_node schema；当前已连接的旧进程不会自动更新。
