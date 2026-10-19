# 统一命令数组提交

0.3.0 将用户提供的 Perl 提交程序逻辑实现到 LRW 内部。Slurm、PBS Pro/OpenPBS 和 Torque 共用命令分组、限流、失败统计和生成目录；不需要在远端安装 Perl。

## 命令文件

```bash
# 每个有效行是一条独立命令
python scripts/analyse.py --sample sampleA
python scripts/analyse.py --sample sampleB
bash scripts/complex_workflow.sh
```

输入是本机 agent 项目中的 UTF-8 文件。空行和行首注释忽略，CRLF/BOM 兼容；每行独立执行，不能依赖上一行的 cd、source 或变量。复杂多行 shell 内容先保存成脚本，再用一行 bash 调用。普通 `run` 的多行命令则保留为一个完整组，不逐行打散。输入最多 64 MiB，生成计划按保守估算限制为 256 MiB，超出时应拆成更小的输入。

## CLI

```text
# 只生成远端文件
rw submit --workspace research --in commands.sh --outdir JOBS --cpu 1 --maxnum 500
# 生成新 run 并提交
rw submit --workspace research --in commands.sh --outdir JOBS --cpu 1 --maxnum 500 --submit
# PBS 队列/方言（不能识别版本时需要显式选择）
rw submit --workspace research --in commands.sh --outdir JOBS --queue cpu --pbs-flavor pbs-pro --submit
rw submit --workspace research --in commands.sh --outdir JOBS --queue cpu --pbs-flavor torque --submit
# 单命令也走同一模式
rw run --workspace research --force cluster --cpu 4 --maxcpu 80 -- bash scripts/run.sh
```

`--in -` 从 stdin 读取命令列表。`--outdir` 相对于远端 work 根；文件 API 的 work/data 边界仍生效。每次调用创建独立目录，不会删除、清空或复用旧 run。只生成后再次带 --submit 调用会创建新的 run；需要相同打乱顺序时使用返回的 shuffle_seed。

| 参数 | 默认/含义 |
|---|---|
| partition | Slurm 默认 cpu64,cpu128；可覆盖 |
| queue | PBS 使用站点默认队列，不填写 Slurm 分区列表 |
| nodes / ntasks / cpu | 1 / 1 / 1；ntasks 是每个数组元素的总 task 数，cpu 是每个 task 的 CPU 数 |
| maxnum | 1000，最多同时运行的数组元素数 |
| maxcpu | 2000，本批次所有同时运行元素的 CPU 预算 |
| jobname | job；Slurm 最多 64 个 ASCII 字符，PBS 为兼容起见最多 15 个且字母开头 |
| time / mem | 默认不写指令；显式提供时才申请 |
| shuffle-seed | 可选，用于复现打乱顺序；实际值写进 plan.json |
| pbs-flavor | 自动识别，或 pbs-pro / torque |
| submit | 默认只生成，显式 --submit 才提交 |
| timeout-secs | 默认 120 秒，控制生成/上传/提交请求；与调度器作业运行时间不同 |

```text
array_tasks = min(有效命令数, 1000)
cpu_per_array_task = ntasks * cpu
array_parallel = min(array_tasks, maxnum, floor(maxcpu / cpu_per_array_task))
```

maxcpu 不足以运行一个元素时直接拒绝。命令尽量均匀分到各组，数量之差至多 1。maxcpu/maxnum 只约束本批次，不是跨批次的全局资源锁。集群限制不是申请量，不会自动把可用上限写成内存或时间请求。

## MCP

调用 `workspace_submit`：

```json
{
  "workspace_id": "research",
  "local_path": "D:\\agent-project\\commands.sh",
  "batch": {
    "outdir": "JOBS",
    "cpu": 1,
    "maxnum": 500,
    "maxcpu": 2000
  },
  "submit": true,
  "timeout_sec": 300
}
```

local_path 可换成 commands_text，二选一。`workspace_run` 使用 `force: "cluster"` 时接收同样的 batch 对象并自动提交，不再使用旧的按集群上限生成脚本方式。命令列表不能再嵌套 sbatch/qsub；通过 LRW 的提交入口管理生成和提交过程。

## 目录、日志和失败处理

```text
<outdir>/run.<uuid>/
  bundle.tar
  array.0001/
    cmd/cmd.0001.sh ...
    out/
    err/
    manifest.tsv
    commands.tsv
    plan.json
    job.array.slurm 或 job.array.pbs
```

manifest.tsv 记录 task_id、命令区间、数量和脚本路径；commands.tsv 保存打乱后的索引、原始行号及 JSON 编码命令。plan.json 保存参数和实际随机种子。

每条命令在独立子 shell 运行，失败不跳过后面的命令；组内有任何失败，数组元素最终退出 1。每次执行保留独立 attempt 日志及逐命令状态 TSV，不覆盖此前 attempt。命令文件也可手动用 bash 运行，状态 TSV 环境变量可省略。

`rw slurm logs` / `workspace_slurm_logs` 会根据本机作业记录定位新目录，同时兼容旧作业。返回各文件尾部、最多 2048 个文件；数组子任务请求明确展示父 run 目录，完整内容需读取具体文件。未知作业、空日志和提交成功都不是结果验收凭证。

## PBS 映射与安全边界

- PBS Pro/OpenPBS 使用 -J 和 PBS_ARRAY_INDEX；Torque 使用 -t 和 PBS_ARRAYID。两者支持并发 slot 限额。
- PBS Pro 不接受只有一个索引的数组；单元素计划提交普通 PBS 作业，仍执行同一命令分组与统计逻辑。Torque 可使用单元素数组。
- 多节点要求 ntasks >= nodes；总 task 均衡映射到各节点。PBS Pro 使用 select/ncpus/mpiprocs/ompthreads；Torque 使用 nodes/ppn。不会自动插入 mpirun/srun。
- 调度器命令使用与发现相同的常见 PBS/GridView/Slurm 路径回退，不替换已有 PATH 的优先级；分析软件、conda 或 module 环境应在命令/脚本中明确准备。
- mem 按 Slurm 的每节点内存含义映射：PBS Pro 写到各 select chunk，Torque 换算为全作业总内存。time 转为 PBS walltime；默认二者均省略。
- 生成过程不用原 Perl 中的 remove_tree；归档解包禁止覆盖已有成员。可识别的删除命令会被拒绝，但任意子脚本没有 OS 级隔离，仍需遵守不删除用户文件的约定。
- 调度器接受后，记录/审计失败仍返回已知 job ID，不自动重提。网络断连导致结果未知时，先检查队列与保留的 run 目录。

原始程序原样保存在 [reference/slurm_array_user.pl](reference/slurm_array_user.pl)，仅用于核对来源，不被运行。语法参考 [PBS Professional 用户指南](https://help.altair.com/2024.1.0/PBS%20Professional/PBSUserGuide2024.1.pdf) 和 [Torque qsub 文档](https://docs.adaptivecomputing.com/torque/6-1-1/adminGuide/Content/topics/torque/commands/qsub.htm)。
