# 联合交通编排的研究依据

本轮实现围绕交通能力组合、契约、实例与资源联合选择、共享计算和执行反馈展开。以下来源在前期调研中通过正式会议、原文或作者机构记录核对；原论文性能结果不作为本平台实验结论。

| 工作与出处 | 本平台对应机制 | 适用范围 |
| --- | --- | --- |
| [AutoGen，COLM 2024](https://openreview.net/forum?id=BAakY1hNKS) | 能力角色与可组合执行接口 | 会话协作不直接提供数值任务资源准入 |
| [Magentic-One，2024技术报告](https://arxiv.org/abs/2411.04468) | 进度跟踪与有限恢复 | 原 LangManus 会话入口保留；数值状态由独立执行内核管理 |
| [AIOS，COLM 2025](https://openreview.net/forum?id=L4HHkCDz2x) | 区分应用智能体与共享运行服务 | 不复制其完整 LLM 内核 |
| [LLMCompiler，ICML 2024](https://proceedings.mlr.press/v235/kim24y.html) | 请求到依赖 DAG 的编译 | 依赖可并行后还需交通语义和资源检查 |
| [HEFT/CPOP，IEEE TPDS 2002，DOI 10.1109/71.993206](https://experts.arizona.edu/en/publications/performance-effective-and-low-complexity-task-scheduling-for-hete) | 关键路径、预计完成时间和实例选择对照 | 本实现为在线到达及资源过滤的适配版 |
| [DRF，NSDI 2011](https://www.usenix.org/legacy/events/nsdi11/tech/full_papers/Ghodsi.pdf) | 多资源公平性研究依据 | 当前未实现 DRF，不将等待老化称为 DRF |
| [Ray，OSDI 2018](https://www.usenix.org/conference/osdi18/presentation/moritz) | 分离任务、服务实例和执行生命周期 | 使用现有线程/Future 后端，不引入 Ray 集群 |
| [Llama，SoCC 2021](https://dl.acm.org/doi/10.1145/3472883.3486972) | 端到端时效与实例性能 profile | 视频结果不能直接充当交通预测对照 |
| [LibCity，SIGSPATIAL 2021](https://dl.acm.org/doi/10.1145/3474717.3483923) | 统一交通数据、模型接口与评估口径 | 平台增加运行时组合、实例与资源竞争 |
| [Chameleon，SIGCOMM 2018](https://www.microsoft.com/en-us/research/publication/chameleon-video-analytics-scale-via-adaptive-configurations-cross-camera-correlations/) | 质量与计算成本折中的扩展方向 | 当前固定 B1 模型，不声称实现多模型质量自适应 |
| [Ekya，NSDI 2022](https://www.usenix.org/conference/nsdi22/presentation/bhardwaj) | 性能/收益 profile 的扩展依据 | 训练与推理联合调度尚未纳入本轮 |
| [CityFlow，WWW 2019 demo](https://dl.acm.org/doi/10.1145/3308558.3314139) | 后续交通控制验证环境 | 当前为离线交通认知服务，不声称实现信号控制闭环 |

实际实现见 [平台架构](traffic_platform.md)，验证见 [实验协议](traffic_experiment_protocol.md)。契约用于候选过滤并实际执行合法轴/单位/标准化适配；联合策略在全局配额内结合依赖、实例性能、时效和等待。共享计算通过真实 forward 次数、结果一致性及延迟对照验证。候选机制是否带来收益，以正式实验及消融结果解释，功能实现和测试数量本身不证明学术新颖性。
