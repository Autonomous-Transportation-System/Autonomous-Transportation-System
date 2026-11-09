# 交通联合平台验收记录

本分支的交通数值路径使用真实 B1_seed42_L3 检查点、NYC TLC Green 2024年1月网格制品和两个独立加载的服务实例。所有数量及指标来自实际执行；正式策略性能、模拟指标与功能探针分别记录。

## 功能与数值验收

| 验证项 | 实际结果 | 证据 |
| --- | --- | --- |
| 全交通回归 | 828 passed，0 failed，0 skipped，23.56秒 | final_traffic_tests_after_export_fix.xml |
| 真实TCP HTTP | 9个工作流完成，7种模板形式 | results/http_a4d43cd11359/http_manifest.json |
| 原生A2A实例绑定 | 两个已登记B1实例分别执行成功 | 同次HTTP原始工作流和实例选择 |
| 终端输出组合 | 默认三业务、自定义flow/OD、无aggregate、分工aggregate、混合终端均输出完整 | 同次HTTP请求与结果 |
| 与原保存预测一致性 | 帧1264..1271最大绝对差3.814697265625e-6，低于1e-5 | HTTP逐模态prediction_differences |
| 终态资源回收 | reservations为空，CPU/内存/并发账本为0，active_futures/inflight为0 | HTTP最终snapshot |
| 真实浏览器操作 | 三业务创建/运行/查询通过，五视图截图，页面及console无错误 | browser_20261109_172808/browser_manifest.json |
| 核心缺陷定向复核 | A2A完成映射、状态保存失败恢复、终端输出收集均经触发条件验证 | review_followup.md |

测试包括真实B1与原225测试目标帧的两次加载核对、DAG依赖、候选和契约、全局竞争、有限恢复、迟到回调、活线程/子进程、async取消、初始化和shutdown排空，以及应用持久化和QoS。测试时间是验证耗时，不作为平台吞吐或延迟结论。

单个时间步为30分钟。每个目标使用此前三个时间步的90分钟观测历史，预测下一时间步；batch8是八个独立样本，不是未来四小时的连续预测。B1实际forward计数通过真实模型hook核对。

## 正式策略与消融

113组正式真实验证、对比及消融均通过协议复核；共2,553次提交、2,536次成功、2,421次实际Torch forward。17个非成功工作流为5个关闭恢复时的受控失败和12个主动shutdown取消，均符合探针预期。随后完成300组、20种子、7,200个工作流的实测profile校准模拟。

主要比较为5个到达/负载种子 × 3档负载 × 5种策略，每组24个混合业务工作流，统一关闭复用。下表为重复均值；P95同时给出样本标准差，按期率以全部提交为分母。它们是描述性结果，不表示显著性检验。

| 负载 | 策略 | P95秒（均值±标准差） | 吞吐（工作流/秒） | 按期率 |
| --- | --- | ---: | ---: | ---: |
| 低 | FCFS | 0.482 ± 0.370 | 6.739 | 84.17% |
| 低 | DRC | 0.588 ± 0.365 | 6.618 | 78.33% |
| 低 | EDF | 0.465 ± 0.313 | 6.804 | 82.50% |
| 低 | HEFT | 0.574 ± 0.373 | 6.796 | 77.50% |
| 低 | JOINT | 0.501 ± 0.257 | 6.816 | 82.50% |
| 中 | FCFS | 1.445 ± 0.309 | 9.766 | 39.17% |
| 中 | DRC | 2.052 ± 0.175 | 9.033 | 41.67% |
| 中 | EDF | 1.427 ± 0.204 | 9.802 | 39.17% |
| 中 | HEFT | 2.463 ± 0.155 | 7.989 | 24.17% |
| 中 | JOINT | 1.711 ± 0.183 | 9.518 | 45.00% |
| 高 | FCFS | 1.958 ± 0.215 | 9.417 | 26.67% |
| 高 | DRC | 2.443 ± 0.201 | 8.977 | 14.17% |
| 高 | EDF | 1.928 ± 0.143 | 9.721 | 25.83% |
| 高 | HEFT | 2.553 ± 0.355 | 8.817 | 1.67% |
| 高 | JOINT | 1.980 ± 0.141 | 9.718 | 33.33% |

| 联合策略相对FCFS | P95变化 | 吞吐变化 | 按期率变化（百分点） |
| --- | ---: | ---: | ---: |
| 低负载 | +4.03% | +1.14% | -1.67 |
| 中负载 | +18.41% | -2.54% | +5.83 |
| 高负载 | +1.12% | +3.19% | +6.67 |

P95正变化表示退化，吞吐和按期率正变化表示收益。联合策略在中、高负载提高按期率，同时存在尾部延迟或吞吐退化；不将其描述成所有负载和指标均优于基线。

**复用消融**：五个种子的真实forward合计120→5；P95重复均值2.739→2.379秒，降低13.14%，配对预测最大差为0。每个消费者仍独立做输出校验。

**恢复消融**：关闭恢复55/60成功，开启恢复60/60成功；异常在真实forward前的一次调用中受控注入。

**时限敏感性**：tight/balanced/relaxed三档分别有9/120、38/120、81/120按期完成。deadline为软目标，执行超时与取消确认另行记录。

**资源复核**：113份CPU/RSS采样CSV、释放事件、依赖顺序、尝试数、配对配置及源码/输入SHA通过独立核验；shutdown后active futures、inflight及reservations均为0。真实任务内存峰值最多64MiB，动态额度约1785.94—1790.22MiB，任务内存约束未绑定。本批竞争主要由CPU/实例并发产生，内存紧张准入由压力单测验证。

模拟使用相同冻结调度源码、完整实测profile和已保存的seeded perturbation，实际模型forward为0；其秒数不解释为真实并发时间。完整P50/P95/P99、RSS、CPU、标准差、来源和模拟表见[机器可读汇总](traffic_validation_summary.json)。

## 源码与归档

当前runtime SHA256：`f93f84b6668f5df22b525cf2b309ac2944276838f91d690d36a2e90816e278d4`。最终编译器已允许多个汇总节点分工输出，并以全部终端导出的并集验证完整性；相关3项编译回归及真实HTTP两种组合已通过。

原始证据位于工作区 `实验/研究点二_平台/联合编排实施_20261109/`，不将模型、数据二进制、应用store、密钥或实验大目录提交Git。仓库保存实现、复运行脚本、协议和汇总。模型及数据SHA记录在真实制品和实验provenance中。

保护的RRDC/AW/ASD/ALCM文件、原traffic_scheduler/traffic_contracts及现有Word的指纹已核对保持一致。本轮没有改写第一研究点实验源。

## 验收环境与范围

交通数值验证使用Python3.9.6、Torch2.8.0及已有实验环境，完整项目仍声明Python>=3.12。独立交通入口不需要LLM密钥。原会话/爬虫集成测试在该数值环境缺少markdownify、python-dotenv等原项目依赖，部分无法收集；原QoS冷却测试还对monotonic初值作了假设。完整LLM应用路径未被本次交通回归代表。

资源账本为平台声明配额，进程RSS/CPU独立实测。CPU平均核占用以采样首尾实际时间作分母；吞吐使用独立的工作流测量窗口。模拟秒数不解释成真实并发秒数。注入一次forward前异常用于检验有限重试与备用实例，不代表主机或网络故障全覆盖。拥堵和OD分析是明确规则产生的交通代理指标，不声称实现交通控制闭环。

## 复运行

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python -m pytest tests/traffic -o addopts='' -p no:cacheprovider -q
PYTHONPATH=. python scripts/verify_traffic_platform_http.py --base-url http://127.0.0.1:18763 --artifact-root data/traffic_artifacts --output-root /path/to/new/results
PYTHONPATH=. python scripts/run_traffic_joint_experiments.py --artifact-root data/traffic_artifacts --mode all --output-root /path/to/new/results
PYTHONPATH=. python scripts/run_traffic_joint_simulation.py --profile-json /path/to/real/profile/responses.json --output-root /path/to/new/results
```

环境与制品导出见 [traffic_artifacts.md](traffic_artifacts.md)，完整参数与指标定义见 [traffic_experiment_protocol.md](traffic_experiment_protocol.md)。
