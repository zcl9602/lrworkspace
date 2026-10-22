# 架构与边界

## 模块

| Crate | 职责 |
|---|---|
| core | 工作区/连接类型、配置、原子状态写入、根目录和命令风险策略 |
| remote | SSH 会话、文件访问/传输、有界子进程、索引和环境、原生命令数组计划、归档和统一执行路由 |
| slurm | 共享数组部署/提交、Slurm/PBS 状态与日志、取消 |
| audit | 按工作区和月份保存 JSONL 审计 |
| cli | rw/lrworkspace 命令行，用户级 MCP 安装 |
| mcp | stdio 请求、工具 schema、表单、授权及诊断 journal |

`resolve_execution_route` 同时供 CLI/MCP 使用，统一处理 cluster/direct/read-only。旧 `--force compute-node` 只映射为 cluster；cluster 默认根据新发现的调度器状态自动选择可达的空闲计算节点；可指定 `--compute-node HOST` 或 `--execution-path submit`。没有可用节点或提供 batch 资源时提交调度器。所有 workload 默认 cluster；单服务器模式 `single-server`（存储兼容 direct_compute）允许显式 direct，代码不根据命令字符串猜测计算地点。

## 文件和状态

agent 在自己的项目中编辑，提供本机文件路径给 LRW 上传。远端一个 work 根、零到多个 data 根；没有第三个“本地项目根”。

```text
<data_dir>/workspaces/<uuid>.json
<data_dir>/workspaces/<uuid>.cluster.json
<data_dir>/workspaces/<uuid>.env.json
<data_dir>/workspaces/<uuid>.status.json
<data_dir>/audit/<uuid>/audit-YYYY-MM.jsonl
<data_dir>/journals/<uuid>.jsonl
<data_dir>/jobs/<uuid>/<hex-encoded-job-id>.json
```

通过 UUID/唯一名称选择，只有一个注册时可省略。状态文件用临时文件、sync 和 rename 替换；损坏的注册明确报错，不静默跳过后误选其他工作区。删除注册保留审计/journal 和旧版目录，不取消调度器任务。

## 上传路径

1. 校验相对目标及远端 canonical work 根边界。
2. 创建父目录，在目标同目录创建独占 `.lrw-transfer-*` 目录。
3. rsync/scp 流式上传；显式选择后端时失败即报错，auto 才可回退。
4. 核对传输文件大小，保留已有目标模式并替换，失败保留旧目标。
5. 清理本次 staging，失败不自动重放。

本机传输子进程捕获 stdout/stderr，避免破坏 MCP 协议；有时间预算，超时终止本次进程树。Windows rsync 使用相对本地操作数、受保护远程参数、配套 Cygwin SSH；用户可指定原生 rsync 路径。配套 SSH 与 Win32 agent 的认证能力需单独核对。

CLI/MCP 小文件读取使用有界 SSH，独立的库级完整下载 API 仍保留。传输临时文件不是项目镜像，不需要同步。

## 权限与完成证据

文件 API 限定 work/data 根，写入只接受 work；命令按 SSH 用户真实权限执行，没有 OS 级只读沙箱。目录校验也不构成对并发 symlink 替换的完整隔离。不得将数据根只读约定理解为 Unix 权限已改变。

submit_only 的 workload 默认自动选择空闲计算节点走 SSH 第二跳，不可用时经调度器；显式 compute_node 可固定节点。控制查询和文件访问仍在登录 endpoint；提交路径缺失调度器时失败，SSH 路径不依赖调度器。两条路径均不回落登录节点，未确认模式不执行 workload。

作业未知或队列无记录不等于完成。日志缺失/不可读在现有日志接口中仍可能表现为空内容，需结合明确终态及实际输出核对。诊断分类只是排查线索。

默认操作预算 120 秒，CLI/MCP 参数可覆盖。调度器作业时限独立；轮询预算也不包含所有在途 SSH 与收尾耗时。MCP 并发处理独立请求并按请求 ID 关联响应；表单等待不阻塞其他调用，也不会自动确认。Windows 端点使用有界 SSH 会话池，默认后台命令使用独立远端进程与持久句柄；监控日志读取有界尾部。

运行时不自动安装 OpenSSH、提升系统权限或创建本地项目绑定。Windows 使用原生 exe；Unix 系统 SSH 使用 ControlMaster，部分备用读取依赖 ssh2。

## 原生命令数组模式

`remote::batch` 原生实现用户 Perl 程序的命令读取、打乱、均衡分组、并发计算、脚本/manifest 生成和 ustar 归档，不执行外部 Perl。`slurm::batch::submit_batch` 供 CLI/MCP 共同调用：校验资源和工作目录 → 预留新 run → 上传单个 bundle.tar → 不覆盖解包 → sbatch/qsub → 保存作业记录。资源上限不再被当成默认申请量。

所有新的 cluster 提交经过这条路径。单条 run 命令作为一个命令组，不拆开其多行 shell 语义；命令文件由 submit 入口逐行解析。PBS Pro/OpenPBS 与 Torque 分别生成 -J/-t，PBS Pro 的单任务用普通 PBS 作业执行同一分组脚本。

新模式的脚本和日志位于 `<outdir>/run.<uuid>/array.0001/`。日志 API 根据保存的作业记录定位新目录，并兼容旧作业的原日志发现。数组子任务查询会明确展示父 run 的日志目录；每文件返回尾部，限制文件数。记录/审计失败不能隐藏已经获得的 job ID，避免盲目重复提交。

提交生成过程没有 remove_tree 或清空旧目录。命令入口保守拒绝可识别的删除指令，但任意子脚本仍按 SSH 账户权限运行；这不是 OS 级禁止删除保证。

## 0.3.2 内存与超时

系统 SSH 的 stdout/stderr 各有 10 MiB 默认保留上限（库的 with_max_output 可调整），超出仍排空管道并保留退出码，执行记录标明 output_truncated。Windows 使用容量 16 的 8 KiB 事件队列和增量帧解析；只保留标志长度的待解析尾部，不反复扫描全部输出。

Windows 同一次调用的会话锁等待、写入确认和结果读取共享截止时间，超时会失效并关闭该会话，不自动重放命令。Unix 系统 SSH 的主连接探测/认证与执行共享预算；stdin/stdout/stderr 并行处理。传输子进程的退出与管道 EOF 也必须在同一预算内完成，收尾不做无期限线程 join。Windows 优先使用 Job Object 清理后代进程；受限环境中无法加入时记录警告并回退到有界 taskkill。超时后的强制清理另有最多约 1.2 秒余量，无法保证远端工作负载随断线立即终止，重试前需核对输出。

诊断 journal 和审计都从文件尾部固定块读取，找到所需有效记录即停止，单行最大 16 MiB；单次 limit 为 0–1000。审计 offset 跨月份连续计数，不再只返回月初的一段记录。历史文件保留，不删除或改写。

生产数组提交只打乱索引，通过资源预检后逐个生成归档成员，将 TAR 写入独占临时文件并从磁盘上传；不复制整份命令列表、不为预检生成完整脚本、不同时保留全部脚本和整份 TAR。commands.tsv 目前按一个成员生成，其大小仍受计划预算限制。兼容库 API 的 BatchPlan::build/archive 仍可在内存中生成，生产 CLI/MCP 不使用该路径。临时归档在成功或失败后清理，远端新 run 和既有结果仍保留。

0.3.3 的 PBS 单命令组对两个方言都按普通作业提交；多组仍用各自数组参数。原生 PBS 提交将日志位置通过 qsub -o/-e 真实参数传递，避免旧 Torque 的指令引号被当作文件名。数组状态优先读取具体子任务，避免始终为 R 的模板掩盖终态；旧版 PBS 集群 的站点数组启动器问题单独记录在实机验收文档中。
