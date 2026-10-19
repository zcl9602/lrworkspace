# LRW 0.3.0：用户命令数组模式内置记录

开始：2026-10-15；交付：2026-10-16。代码基线：`b02455a`（0.2.0）。本轮按用户提供的 Perl 程序，将后续 Slurm/PBS 提交统一到原生命令数组实现。

## 来源与行为契约

原稿保存在 [reference/slurm_array_user.pl](reference/slurm_array_user.pl)，字节 SHA256 与用户附件一致：

```text
4b43114ce1a720ba1605770e93cfde62d19e74139cc975da5aa4695640840734
```

`.gitattributes` 将该原稿标为不做换行转换。原稿只用于追溯和测试，不被生产流程执行。内部 Rust 实现以下逻辑：

- 过滤空行、行首注释并打乱有效命令。
- 最多 1000 个命令脚本/数组元素，均衡分配，组内数量之差不超过 1。
- 默认 nodes=1、ntasks=1、cpu=1、maxnum=1000、maxcpu=2000、jobname=job。
- Slurm 默认 partition=cpu64,cpu128；time/mem 默认均省略，集群资源上限不再被当成请求值。
- 并发为 `min(array_tasks, maxnum, floor(maxcpu/(ntasks*cpu)))`；预算不足一个元素时拒绝。
- 命令逐条在子 shell 执行，失败后继续；组内存在失败时最终返回 1。
- 保存 cmd/out/err、manifest、原始行号对应关系、实际随机种子和每条命令的退出状态。

与原稿必须区分的调整：原稿 remove_tree 旧 cmd/out/err；新实现每次创建 `<outdir>/run.<uuid>/array.0001/`，不删除或复用旧 run。日志还按 attempt 保留，避免重复执行覆盖原日志。

## 接口与实现

- `rw submit --in commands.sh --outdir JOBS ... --submit`；省略 --submit 只生成。
- `rw run --force cluster` 使用同一实现，将整个命令字符串作为一个组。
- MCP 新增 `workspace_submit`，接收 local_path 或 commands_text、batch 资源对象和 submit 布尔值；`workspace_run(force=cluster)` 使用同一 batch 参数与后端。
- `remote::batch` 负责解析、打乱、分组、脚本和 ustar 归档；`slurm::batch::submit_batch` 共用预留目录、单归档上传、不覆盖解包、提交及记录流程。
- 本机作业记录保存到 `<data_dir>/jobs/<workspace-id>/<encoded-job-id>.json`，日志接口据此定位新目录，同时兼容旧作业。

任务命令是独立行；复杂流程用一行 bash 调用预先写好的脚本。不会自动插入 mpirun/srun。输入最多 64 MiB，生成计划有保守的 256 MiB 预算，以免本机 MCP 因海量短命令耗尽内存。

## PBS 实现

- PBS Pro/OpenPBS：-J、PBS_ARRAY_INDEX，使用 select/ncpus/mpiprocs/ompthreads。
- Torque：-t、PBS_ARRAYID，使用 nodes/ppn。
- PBS Pro 不支持只有一个索引的数组；单元素用普通 PBS 作业启动同一分组执行器。Torque 保留单元素数组。
- 多节点总 ntasks 均衡分配到各节点，要求 ntasks >= nodes；内存按 Slurm 的每节点语义映射，Torque 换算为全作业总量。
- PBS 队列默认交站点决定；不把 cpu64,cpu128 硬套成 PBS 队列。方言自动识别，识别不出时要求显式 pbs-pro/torque，不猜测。

## 相关缺陷修复

- 可识别的 rm/unlink/rmdir/find -delete/rsync --delete 等删除操作在工作负载入口拒绝；生成过程不清理旧目录。脚本内部仍没有 OS 沙箱，不承诺拦截所有间接删除。
- 已知 job ID 不因本机记录或审计失败而被隐藏；返回警告，避免误认为未提交而重复操作。
- cd 失败会停止该命令，不再继续在错误目录运行。
- PBS 先查通用文本，再兼容 PBS Pro 历史/Torque XML，合并多个数组记录，避免只看到第一个完成元素就报整体完成。
- Slurm 混合未知状态不自动成为已结束；未知和空日志不作为成功证据。
- 统一发现、提交、查询的常见 PBS/GridView/Slurm PATH 回退，保留用户 PATH 优先级。
- GridView PBS 不再因包含 grid 字样而误识别为 Grid Engine。

## 验证

Windows、Rust/Cargo 1.98.1、GNU 工具链；Bash/tar/Perl 来自本机开发环境。

```text
cargo check --workspace --all-targets --locked --offline
cargo test --workspace --locked --offline
cargo test -p lrworkspace-remote reference_perl --locked --offline -- --ignored --nocapture
cargo build -p lrworkspace-cli -p lrworkspace-mcp --locked --offline
cargo test -p lrworkspace-cli --test native_batch mcp_command_array --locked --offline -- --ignored --nocapture
cargo build --release -p lrworkspace-cli -p lrworkspace-mcp --locked --offline
```

- 常规测试 **113 项通过**，0 失败；可选专项在该次测试中单独标记 ignored。
- 与原 Perl 的专项对照 **1 项通过**：2501 条命令、1000 组、每组 2/3 条；ntasks=2、cpu=4、maxnum=300、maxcpu=2000 时并发 250；全部命令出现且无重复。只在新建临时目录运行原稿的临时 LF 副本，明确不提交任务，权威原稿未修改。
- MCP Torque 模拟提交专项 **1 项通过**，与 CLI 共用后端并返回含服务器/数组后缀的 job ID。
- 真实 Bash/tar 测试运行 Slurm/PBS Pro/Torque 三种分发器，验证失败后继续、保留重复 attempt 日志、源路径、逐命令状态和已有结果不变。
- CLI 端到端测试使用本机模拟 SSH/sbatch/qsub，覆盖只生成/提交、普通 cluster run、PBS 自动识别、日志定位和审计故障仍返回 job ID。
- Release 构建成功。没有新增第三方依赖，锁文件仅更新本地 6 个 crate 到 0.3.0。

原始验证日志与产物保留在本机私有记录中，不随源码发布。

## 验证边界与启用

- 没有连接真实服务器、提交真实 Slurm/PBS 作业或读取实际科研数据。本机模拟不是集群接受/运行证据；仍需指定独立测试根验收 MaxArraySize、队列权限、PBS 方言、历史保留、共享文件系统与 GNU tar。
- 本轮未重跑未改动的 rsync 专项；既有传输测试及模拟提交中的归档传输继续通过。
- Linux/macOS 本轮未执行；CI 已增加 MCP 和 Perl 对照步骤，但没有在远端运行。当前缺 rustfmt/clippy，没有视为检查通过。
- 只有新版程序的提交入口使用本实现；已经加载的旧 MCP 进程不会热更新，部署新二进制后需重新连接。没有自动修改客户端配置或替换现有安装。
- 新 run、bundle 和日志有意保留，不自动清理；工作负载输出仍应独立验收。

使用见 [统一提交模式](batch-submit.md)。语法依据：[PBS Professional 用户指南](https://help.altair.com/2024.1.0/PBS%20Professional/PBSUserGuide2024.1.pdf)、[Torque qsub](https://docs.adaptivecomputing.com/torque/6-1-1/adminGuide/Content/topics/torque/commands/qsub.htm)、[Torque XML 输出说明](https://docs.adaptivecomputing.com/torque/archive/3-0-2/pdf/TORQUE_Administrator%27s_Guide.pdf)。
