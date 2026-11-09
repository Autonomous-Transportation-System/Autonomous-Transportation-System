# 联合调度方法与实现口径

对应实现：`src/graph/traffic_joint_scheduler.py` 的 `_order_key`、`_candidate_options`、`tick`；契约候选来自 `traffic_composer.py`，profile来自实际实例执行。此方法是在线、确定性启发式，不声称求得全局最优调度。

## 变量与可行性

工作流为 DAG $G_w=(V_w,E_w)$，节点 $v$ 的候选集合 $I_v$ 已通过能力和时空契约过滤。实例 $j$ 的候选声明包含 CPU $c_{vj}$、增量内存 $m_{vj}$、执行估计 $e_{vj}$、冷启动 $b_{vj}$、适配 $a_{vj}$ 和实例并发上限。目录区分 capability、implementation 和 instance。

节点仅在全部依赖 `call_status=success` 且 `quality_status=valid` 后就绪。当前预留集合为 $R(t)$，准入必须同时满足：

$$
\sum_{(v,j)\in R(t)}c_{vj}+c_{uj}\le C,\qquad
\sum_{(v,j)\in R(t)}m_{vj}+m_{uj}\le M_{task},
$$

$$
|R(t)|+1\le K,\qquad k_j(t)+1\le K_j.
$$

其中 $M_{task}=M_{total}-M_{resident}-M_{cache\ allowance}$。CPU与内存额度采用声明配额；feasibility使用十进制Fraction求和，与原子ResourcePool预留一致。健康、已尝试实例、超预算候选同样参与过滤。正在执行或已预留的任务在真实结束前一直属于 $R(t)$。

## 任务与实例联合选择

任务 profile 总时长为 $d_{vj}=e_{vj}+b_{vj}+a_{vj}$。关键路径 $r_v$ 使用各节点最小候选时长向后递推，并记当前节点最小值 $d_v^{min}$。实例的预计完成时间及剩余工作流估计为：

$$
EFT_{vj}(t)=A_j(t)+d_{vj},\qquad
\widehat F_{wj}=EFT_{vj}(t)+\max(0,r_v-d_v^{min}).
$$

候选当前容量不足时，可以在诊断中根据活跃任务profile估计 $A_j(t)$，但其仍为不可准入候选。调度器只预留当前可执行候选，不提前预留未来槽位。

每个就绪任务先在可行实例中选择：联合策略优先预计工作流能在deadline内完成的实例，再取最小EFT；其他策略直接取最小EFT。相同估计以实例ID稳定排序。

等待老化为 $h_v(t)=\lfloor(t-t_v^{ready})/\Delta\rfloor$，有效优先级为 $p_v^{eff}=p_v+p_w+h_v$。联合策略对“任务、已选实例”按以下元组升序排序：

$$
(-p_v^{eff},\ D_w-(t+r_v),\ -r_v,\ EFT_{vj},\ n_w,\ t_w^{submit},\ order_w,\ order_v,\ id_v).
$$

无deadline时取 $D_w=+\infty$。$n_w$为当前工作流已分派数，用于同分时优先照顾分派更少的工作流。此规则提供确定的等待和同分处理，不等同于DRF，也不证明任何到达过程下的公平性界。每次预留后重新计算全部候选可行性和选择，直到没有当前可执行任务。

## 五种策略的公平比较

| 策略 | 当前实现的任务排序主键 |
| --- | --- |
| FCFS | 工作流提交时间与稳定顺序 |
| DRC | 有效优先级，包含等待老化 |
| EDF | 老化、业务deadline、有效优先级 |
| HEFT适配版 | 老化、剩余关键路径、有效优先级 |
| Joint | 有效优先级、截止期余量、关键路径、实例EFT |

五种策略使用相同DAG、候选、CPU/内存/并发、输入语义、取消和失败恢复规则。HEFT为在线到达与资源过滤的适配版，EDF也明确带有老化；不称为原论文算法的无修改复现。主策略比较统一关闭跨请求复用，以隔离排序和实例选择效果；复用另做on/off配对消融。

## 共享计算与执行反馈

同模型、数据、目标窗口、训练统计和完整输入契约/内容身份的预测请求可以通过运行时singleflight共享一个物理forward，或命中有界缓存。三个头全部合格才缓存；每个消费者仍有独立下游校验、deadline与工作流终态。共享推理由独立Future管理，不因单个消费者取消而提前停止。跟随者保留请求缓冲/等待配额，收益通过真实forward次数、延迟与结果一致性衡量。

目录记录实际实例耗时和执行成功情况。profile用于随后编译的计划；已编译计划的候选估计是不可变快照，健康变化通过调度器动态覆盖。QoS告警带冷却，将本平台拥有的实例隔离，后续编译与选择避开它。重试只由调度器实施，并受max_attempts和已尝试集合约束。

deadline是软时效目标，超过目标记录deadline_miss。节点执行timeout或取消先请求停止，真实线程/Future终态后才finish并释放；不把“发出取消”当成“已经停止”。任务调用、输出质量和调度状态分开保存。

## 结果解释

真实实验报告端到端延迟、吞吐、按期率、实际forward、进程RSS/CPU和资源不变量。profile校准的离散事件模拟使用同一调度器，但其耗时仍为模拟秒。哪种策略更好、哪些负载发生退化，以正式结果和重复统计为准。方法约束与每次选择理由都保存在原始事件中，可复核每次准入。

研究依据见 [traffic_related_work.md](traffic_related_work.md)，复运行协议见 [traffic_experiment_protocol.md](traffic_experiment_protocol.md)。
