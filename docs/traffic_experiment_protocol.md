# 联合交通编排实验协议

两个脚本只依赖冻结的 `traffic_module_contracts.md` 和公开 JointScheduler / JointTrafficRuntime 接口。真实运行加载 B1_seed42_L3 的两个独立服务实例。该实验比较平台执行，不训练模型。输出目录通过 CLI 配置；默认定位工作区的研究点二联合编排 results，每次建立带时间和唯一后缀的新目录，以独占创建方式写文件，不覆盖旧实验。模型工件默认定位研究点一 TBA，可通过 `--artifact-root` 或 `TRAFFIC_EXPERIMENT_ROOT` 指定。

## 运行

使用安装了 NumPy、Torch 的虚拟环境，并设置 `PYTHONPATH=.`。psutil 可用时记录当前 RSS；不可用时明确记录进程历史峰值及平台单位。报告不写个人绝对路径，模型、数据、归一化、参考预测和源码以相对名称与 SHA256 标识。

```bash
PYTHONPATH=. python scripts/run_traffic_joint_experiments.py --mode all
PYTHONPATH=. python scripts/run_traffic_joint_experiments.py --artifact-root data/traffic_artifacts --mode all
PYTHONPATH=. python scripts/run_traffic_joint_experiments.py --quick
PYTHONPATH=. python scripts/run_traffic_joint_experiments.py --artifact-root "$TRAFFIC_ARTIFACT_ROOT" --output-root "$TRAFFIC_RESULT_ROOT"
PYTHONPATH=. python scripts/run_traffic_joint_simulation.py --profile-json "$REAL_PROFILE_RESPONSES"
PYTHONPATH=. python scripts/run_traffic_joint_simulation.py --quick
```

`--quick` 只作冒烟验证，不满足重复数量和结论显著性的要求。真实默认种子 17、29、42、53、71，混合业务每组 24 个工作流；模拟默认 0..19 共 20 种子，每组 24 个工作流，可用 `--workflows` 扩大规模。策略均为 fcfs/drc/edf/heft/joint，三个负载均使用相同共享资源、两个实例、同一组到达轨迹和目标帧。种子只生成请求次序、到达和模拟时长，不生成真实指标。

## 真实比较

先对三个模板和两个实例做真实串行 profile，再用每工作流端到端耗时的中位数，除以两个实例的名义容量，生成低、中、高的到达间隔；端到端 profile 包含平台调度、回调和序列化，单独保存的 operation profile 只代表操作时长。名义 offered load 分别为 0.25、0.9、1.8；它是 profile 校准量，实际竞争强度通过事件、峰值和完成时间核验。profile 本身保存全部真实操作及实例标识。初始化独立记录，不计入提交到完成的延迟。每组子进程重新加载服务，使缓存、历史 RSS 和生命周期独立；总进程启动时长不充当吞吐。

基线比较关闭跨请求复用。每个 seed/load 的 trace 完全相同，以 SHA256 校验；实际提交时刻和计划提交时刻分别保留，操作系统抖动不会被隐藏。flow 校验 flow；analysis 校验 state/flow 并执行拥堵规则；od_hotspots 校验 od 并执行热点规则。工作流 DAG 和真正实例选择来自 scheduler 快照。两服务真实 forward 使用 runtime 的模型 hook 计数，不能用 predict 任务数代替。

每次正式真实运行先在新输出目录创建 `runner_source`，只复制所需交通模块、包标识、脚本和冻结合同；`runner_source_manifest.json` 保存每个文件 SHA256。所有子进程从这个固定源码快照 import，避免并行 worker 在组间改文件导致代码版本混合。该过程不修改仓库中的模块文件。模拟若需与真实运行使用相同版本，可执行对应快照中的模拟脚本并显式传 `--output-root` 和真实 profile。真实性能测量与模拟顺序执行，避免自身模拟负载干扰真实延迟。未做主机 CPU 绑定，背景任务仍可能影响本机计时。

每组保存 config、初始化快照、submissions、responses、scheduler/resource events、任务 CSV、workflow CSV、profile CSV、RSS/CPU 采样 CSV、shutdown 后快照、metrics、invariants、provenance 和进程退出码。每个工作流记录 submit/start/finish，延迟为 finish-submit；P50/P95/P99 采用线性插值，仅统计成功工作流，失败另计。吞吐为成功工作流数 / (最后终态时间-最早提交时间)，其窗口单独记录为 workflow_measurement_span_seconds；按期率以全部已提交工作流为分母，失败不算按期。无截止期的成功请求算按期。资源峰值为声明账本峰值，RSS 为进程实测，两者不能混用。CPU 平均核占用为采样末行与首行的进程累计 CPU 差值 / 同两行实际 monotonic 时间差，保存 cpu_measurement_start/end、cpu_measurement_span（秒）及 cpu_measurement_span_seconds；该窗口包含边界内的并发工作和响应处理，与工作流吞吐窗口分开。主动 shutdown 组的采样可覆盖 shutdown，普通组在 shutdown 前停止采样；均以实际采样边界解释。operation profile 的 thread CPU 与包含并发的 process CPU 分开解释。

组间 summary 从各组 metrics.json 读取，给重复均值与样本标准差，并列出来源文件。平均 P95 表示各次运行 P95 的均值，不表示合并样本的 P95。少量工作流的 P99 受样本量约束。记录所有完成、失败和取消，不将异常组悄悄剔除。

## 消融

重复同窗口：相同混合 trace 使用帧 1264..1271，reuse on/off 独立运行。收益看真实 forward、cache/singleflight hits 和延迟；所有消费者仍执行自身校验。paired_checks.json 对照同序号工作流的 on/off 预测并要求最大差不超过 1e-5，同时核对策略比较的 trace、预算和源码 SHA。缓存命中本身不证明运行加速，不能预写收益。

recovery on/off：对真实 service.predict 的第一次调用注入一次明确的异常，发生在模型 forward 前；其余调用执行真实 B1。注入事件单独记录。它验证有限重试和备用实例选择，不能声称覆盖真实主机宕机、网络故障或所有故障模型。关闭恢复的失败和开启恢复的重试都保留。

时效 profile：高负载下仅改变截止期倍率 1/4/16，对应 tight/balanced/relaxed；保持请求、资源和到达过程一致。截止期是软目标，不将 deadline miss 当作模型超时。是否提升按期完成由 JSON 统计决定。

## 数值一致性

真实三个模板请求绝对帧 1264..1271。按工件保存的 test split 和同日历史规则重建参考行号，核对原 test_predictions.npz 的 pred_state/pred_flow/pred_od。严格要求原始单位预测最大绝对差不超过 1e-5，记录各模态差值与参考 SHA256；形状要求 (B=8,H=1,Y=6,X=6,C/D)。失败如实记录，不放宽容差来制造通过。

每个 t 使用 [t-3,t) 的 90 分钟观测历史预测 t，一个目标时间步为 30 分钟。batch 8 是八个有各自观测历史的独立单步样本，不能写成多步未来预测。复用范围需完整模型、数据、窗口、归一化身份相同。

## 模拟

离散事件执行真实 JointScheduler；执行耗时来自提供的真实 profile 中各 capability/instance 的中位数，并将任务时长的 0.8..1.2 倍扰动提前用种子生成、保存。profile 必须附带同目录的真实 B1 工件 provenance.json。缺少实例单元时可使用同 capability 的实测中位数，但必须记录补齐单元。缺少任何 capability 或缺少真实工件 SHA 证据时整组明确回退为 seeded synthetic durations，不能局部填随机数后称全实测。未提供 profile 也明确使用 synthetic。

所有策略共享已保存的 arrival、任务到实例时长表、估计 profile、DAG、CPU/内存/并发和 aging 参数。flow/analysis/od_hotspots 具有 prepare→predict→按需校验→规则分析→aggregate 依赖。模拟不运行模型，actual_model_forwards=0；其吞吐、分位数只解释为模拟时间指标。用实测 profile 校准也不等于真实并发测量。

## 不变量与退出

按保存的 JSON 检查每条事件单调非负、CPU/内存/并发不越预算、reserve/release 一一对应、依赖完成先于子任务开始、尝试数有限、所有任务终态。真实 async shutdown 后还必须 closed=true、reservations 为空、资源使用为零、active_futures=0、inflight=0、active_forwards=0。主动 shutdown 探针必须先观察到运行中的 future，再请求关闭并等待真实终态，不能只检测空闲实例退出。所有原始工件在组前后核 SHA256，禁止修改研究点一工件。

## 状态

父代理通知最终后端 freeze 后，正式全量已顺序执行并完成独立复核：真实 113 组（75 策略比较、35 消融、validate/profile/shutdown 各一组）和模拟 300 组，批次异常失败均为零。执行源码在 freeze_copy 后保持不变，UI 文件未纳入执行快照。旧 smoke、预验证及其 CPU 修正记录均保留，未进入下列正式汇总。

正式结果标识：真实 `real_20261109T170306_77bd8ac5`，模拟 `simulation_20261109T171751_c0cb5700`，独立复核及表格 `formal_review_20261109_f93f84`。以上目录位于指定 repo 外 results。`verification_report.json`、`verification_report.md`、`primary_real_summary.csv`、`simulation_summary.csv` 与 `formal_handoff.json` 保存全表、配对、数量和来源；真实及模拟各自保留全部配置、events、profiles、samples、snapshot、invariants 和 provenance。

冻结源码：runtime SHA256 `f93f84b6668f5df22b525cf2b309ac2944276838f91d690d36a2e90816e278d4`；Compiler SHA256 `b63a6b9c1c393fa7e9fa78dd01b66ec18d1f820c340b499a1d9a1874849878c8`。真实目录的 `runner_source` 包含 15 个执行文件，模拟使用同一快照，逐文件 SHA 一致。`runner_source_manifest.json` 的 SHA256 为 `8bae37cb94af7813be73295baa7282294a30f504e5539ee94c779360f3f4fad6`。模型、数据、训练归一化与模型源码前后 SHA 不变。

真实共提交 2,553 个工作流，成功 2,536 个，Torch hook 记录实际 forward 2,421 次。17 个非成功工作流为预期的 5 次 recovery-off 受控失败和 12 次主动 shutdown 取消。75 个策略组共 1,800 个工作流全部成功。帧 1264..1271 的 state/flow/od 最大绝对差为 2.8610e-6、3.8147e-6、1.4305e-6，满足 1e-5；batch8 为八个独立 H=1 单步样本。

复用消融在 5 个种子下均为每组 forward 24→1，总计 120→5，预测差为零；P95 的重复均值为 2.739183s→2.379249s，降低 13.14%。该比例只描述本次测量，不表示 CPU 总耗时减少 95.83%。恢复消融成功数为 55/60→60/60，故障范围仍仅为一次明确的 forward 前调用异常。时效 tight/balanced/relaxed 的按期数为 9/120、38/120、81/120。

完整策略结果同时包含收益与退化。joint 相对 FCFS 在低/中/高负载的 P95 变化为 +4.03%/+18.41%/+1.12%，吞吐变化为 +1.14%/-2.54%/+3.19%，按期率变化为 -1.67/+5.83/+6.67 个百分点。不能据此声称所有指标均有提升。比较值为重复均值之比，未做显著性检验。

模拟使用完整 16 个 capability/instance 实测 profile 单元和已保存的 seeded perturbation，无缺失单元补齐；20 种子、300 组、7,200 个工作流通过，实际模型 forward 为零。模拟相对指标仅说明离散事件模型中的行为，不等同于真实并发性能。

独立复核从 CSV 用 NumPy 重算 P50/P95/P99、吞吐、按期率、RSS 和 CPU；CPU 分母为采样首尾实际 monotonic elapsed，工作流吞吐保留独立 finish-submit 窗口。另核对真实/模拟源代码 SHA、每份输出 manifest、模型/数据来源、所有配对配置和时长表、事件时间、尝试上限、依赖顺序及实际 profile 完成后才 release。所有真实 shutdown 后 active futures/forwards、inflight 和 reservations 为零。任务内存预留峰值 64MB，动态额度为 1785.94..1790.22MB；总配置预算相同，实测常驻扣款导致余量浮动，该实验中任务内存约束未绑定。实测 RSS/CPU 为进程范围，不作为并发任务的独立消耗相加。
