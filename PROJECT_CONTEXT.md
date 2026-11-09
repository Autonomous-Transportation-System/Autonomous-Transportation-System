# 项目上下文快照

> 最后更新：2026-11-09

---

## 项目概述

**Autonomous Transportation System**：在 LangManus 分布式 Agent 编排基础上实现交通能力与资源联合编排，保留 Microsoft Magentic-One、自适应模式、周期调度及子工作流入口。

- 技术栈：完整项目声明 Python >=3.12；交通数值路径使用 FastAPI、httpx、asyncio、NumPy、Torch
- 项目根：`Autonomous-Transportation-System`
- 交通平台：`traffic_server.py`，默认端口 18763，页面 `/traffic/platform/ui`
- 主服务：`server.py`（FastAPI，端口 8000）
- Agent 节点：`agent_server.py`（可多实例，端口可配置，默认 9000）

---

## 当前进度

| 模块 | 状态 | 说明 |
|------|------|------|
| 交通能力/实现/实例目录与契约编译 | ✅ 已实现 | 三业务模板、自定义 DAG、schema 快照、显式输入绑定 |
| 交通全局联合调度 | ✅ 已实现 | 共用 ResourcePool，五策略、实例选择、有限重试及真实终态释放 |
| 真实 B1 联合运行时 | ✅ 已实现并验收 | 独立模型实例、实际 edge adapter、三头缓存准入、singleflight、取消排空 |
| 交通管理与五视图 UI | ✅ 已实现 | AppManager 持久化、注册、QoS 隔离/健康恢复、HTTP/A2A 接入 |
| 联合平台正式实验与前端验收 | ✅ 已完成 | 828项回归、113真实组、300模拟组、TCP/A2A和五视图截图 |
| 新分支交付 | 待推送 | 代码与验收汇总已就绪；分支feat/traffic-joint-orchestration-20261109 |
| §1 应用层（install/start/stop/display） | ✅ 已完成 | 含 Skills.md 在线编辑 |
| §2.1 单主体工作流编排 | ✅ 已完成 | ASD 用 subprocess，非 Docker |
| §2.2 跨主体工作流编排 | ✅ 已完成 | httpx POST /orchestration/dispatch |
| §2.3 跨主体重编排（Agent 失效 failover） | ✅ 已完成 | Rule 0 切换替代节点，Rule 1 跳过失败任务 |
| §2.4 跨主体工作流停止编排 | ✅ 已完成 | DELETE /orchestration/session/{id} 传播 |
| ARDC HTTP Gossip 智能体发现 | ✅ 已完成 | POST /registry/sync，PEER_AOE_URLS 环境变量，含子工作流同步 |
| ASD subprocess 进程管理 | ✅ 已完成 | agent_scheduler.py，自动端口分配 |
| QoS 监控框架 | ✅ 已完成 | record_call / check_threshold / get_alert_agents |
| QoS → ASD 反馈闭环 | ✅ 已完成 | register_alert_callback 注册回调，cooldown 60s 防 storm，redeploy 后 reset 指标 |
| 跨主体编排主动触发 | ✅ 已完成 | identify_cross_host_tasks() 在 Planner 出口调用，写入 cross_host_sessions |
| 子工作流即服务（Sub-Workflow as a Service） | ✅ 已完成 | config/sub_workflows.json 定义 + ARDC Gossip 发现 + Executor 三路径路由 |
| 周期性工作流调度 | ✅ 已完成 | WorkflowScheduler asyncio 循环 + 并行执行 + 历史持久化 + 自动恢复 |
| 项目交接文档 | ✅ 已完成 | `接口/项目交接文档.md` 11 章节全面交接 |

> 详细实现状态见 [`接口/当前进度.md`](接口/当前进度.md)

---

## 当前正在解决的问题

**交通计算平台联合编排验收与交付**（2026-11-09）

分支 `feat/traffic-joint-orchestration-20261109` 从远端 main `5fe1377` 建立。目录、契约编译、全局调度、真实模型运行、应用/注册/QoS桥接和五视图均已实现并验证。828项交通回归、9个真实TCP工作流、113组正式真实实验及300组模拟通过对应检查；实验与提交代码的14份执行文件SHA一致，最终前端截图再次核对当前源码。

共享计算消融真实forward合计120→5，P95降低13.14%，配对预测差为0。联合策略的中、高负载按期率提高，也存在部分延迟/吞吐退化；完整均值、标准差与边界见 [验收记录](docs/traffic_validation.md) 和 [机器可读汇总](docs/traffic_validation_summary.json)。本机当前页面为 `http://127.0.0.1:18764/traffic/platform/ui`，保留原应用store；仓库默认启动端口仍为18763。

研究点固定为“基于共享表征的多智能体交通态势认知方法”和“支持智能体与资源联合编排的交通计算平台”。本轮交付代码和工程文档，五个 Word 模块及第一研究点实验源保持原样。平台操作见 [traffic_platform.md](docs/traffic_platform.md)，实验口径见 [traffic_experiment_protocol.md](docs/traffic_experiment_protocol.md)。

以下为保留的原平台阶段记录。

**周期性工作流调度**（2026-05-07 完成）

支持将应用配置为周期性执行，按固定时间间隔自动触发工作流。每次触发启动独立实例（允许并行），执行历史持久化。

**实现要点**：
- `src/service/workflow_scheduler.py` — WorkflowScheduler 单例，asyncio 调度循环
- `GuidanceFile.constraints` 扩展：`schedule_interval_seconds` / `schedule_max_parallel` / `schedule_auto_restart`
- `AppStatus` 新增 `"scheduled"` 状态
- `ScheduleExecutionRecord` 数据类跟踪每次执行
- 4 个 API 端点：`schedule/start` / `schedule/stop` / `schedule/status` / `schedule/history`
- FastAPI startup 自动恢复 `schedule_auto_restart=true` 的应用
- Web UI 安装表单新增调度字段 + 调度控制按钮 + 历史面板

**子工作流即服务（Sub-Workflow as a Service）**（2026-05-07 完成）

支持节点定义子工作流（多 Agent 组成的命名 Pipeline），其他节点通过 ARDC Gossip 自动发现，直接调用子工作流整体而非单个 Agent。每次调用启动独立工作流实例。

**跨主体编排主动触发修复**（2026-05-07 完成）

修复 `identify_cross_host_tasks()` 从未被调用的问题 — 在 `distributed_planner_node()` 的 Pipeline 路径和 LLM 路径两个出口均注入调用，将识别结果写入 `cross_host_sessions`，使跨节点编排从被动（仅 Rule 0 故障切换）变为主动。

---

**编排可视化 H5 — 联动版**（2026-05-07 完成）

将可视化合并进主服务(端口 8000)。**真实分布式工作流自动出现在可视化页面,支持多工作流切换**。

**联动链路**:
```
POST /api/apps/install + /start  (或 任何调 run_distributed_workflow 的入口)
  → src/distributed_workflow.run_distributed_workflow()
    ├─ bus.register(title=user_input)              ← 注册新工作流
    ├─ bus.update_state(state, node="__init__")    ← 推首屏
    └─ async for chunk in graph.astream():
         → bus.update_state(state, node=node_name) ← 每节点推一帧
         → 触发 WS 推送给前端 (snapshot 含 3 场景全量数据)
```

**新增/改动文件**:
- `src/service/viz_bus.py` — VizBus 单例(多工作流注册/订阅/state 快照,asyncio-safe)
- `src/api/visualization_routes.py` — APIRouter(挂载到 src/api/app.py)
- `src/api/visualization.py` — 三场景数据提取层(纯只读,不侵入业务)
- `src/distributed_workflow.py` — `graph.ainvoke()` → `graph.astream()` + 注入 viz_bus
- `src/api/app.py` — `app.include_router(viz_router)`
- `static/visualization.html` — 顶部下拉选择器 + 双 WebSocket(全局列表 + 单工作流) + URL `?wf=<id>` 路由

**两种使用方式**:
- 真实工作流: `POST /api/apps/install` + `POST /api/apps/{id}/start` → 自动出现在 /viz
- Demo 演示: /viz 页面点 "▶ Demo 工作流" → 不依赖 LLM 的 7 任务流水线演示

**启动**: `python server.py` → 浏览器 http://127.0.0.1:8000/viz

**3 场景内容**:
- 场景 1 编排过程: Skills.md 原文 + 候选 Agent 按平台分组(本机/远端) + 已选 Agent 绿色高亮 + Pipeline 流图
- 场景 2 拓扑结果: Cytoscape + dagre 自动布局,平台 compound 父节点(🏠 本机 / ☁️ 远端),节点状态色 + 当前节点脉冲发光
- 场景 3 工作流执行: 总进度条 + 当前 Agent 卡(扫光动画) + MCP/A2A 工具调用气泡 + 时间线

> 旧的独立可视化服务 `visualization_server.py`(端口 8888)仍可用,作为离线演示模式保留。

---

**前期问题: 编排模式优化研究 — 基于 OpenClaw/Clawith Aware 系统的借鉴分析**（2026-03-30）

已完成对 OpenClaw/Clawith Aware 编排模式与 LangManus 现有三档编排的深度对比研究，产出技术方案报告，识别 5 个可借鉴方向：
1. P0: 结构化工作记忆（Focus List）— 解决 Magentic-One 长对话目标遗忘
2. P1: Monitor 主动超时检测 — 解决远端 hang 无法及时发现
3. P1: 跨主体异步 Dispatch — 解锁跨主体并行执行
4. P2: 事件触发层 — 支持 webhook/poll/cron 驱动编排
5. P2: 自适应控制参数 — 动态调整 max_round/max_stall

报告输出至 `接口/编排模式优化研究-基于OpenClaw借鉴分析.md`

---

## 关键约束与决策

- 交通计划与执行状态独立于 LangGraph 对话状态；保留原 Scheduler/TaskSpec API，通过新增 JointScheduler 共享 public ResourcePool。
- 业务 deadline 为软目标；节点 timeout/取消必须等待实际同步 Future 终态才回收，不由 HTTP waiter 管理工作生命周期。
- 缓存/请求合并按完整模型、数据、窗口、训练统计和输入契约身份，完整三头有效才缓存；调度器唯一负责重试。
- schema 契约不只生成图表说明，运行时按已选上下游实例执行适配并保留实际 trace。
- 应用管理通过 public backend hooks 和 update_state 接入；QoS 隔离仅影响后续调度，显式恢复受冷却保护。
- 配额账本与进程 RSS/CPU 分别报告；真实并发计时与 profile 校准模拟分别保存。保护 RRDC/AW/ASD/ALCM 骨架不改。

- `qwq-plus` 只支持 streaming=True，在 `src/agents/llm.py` 中特殊处理；节点中使用 `llm.astream()` 异步流式调用
- 所有 LangGraph 节点函数（planner/executor/monitor/reporter）均为 `async def`，所有 LLM 调用均使用异步方法（`astream`/`ainvoke`），确保多工作流并发执行
- LLM 模拟器（`llm_agent_simulator.py`）：executor 降级路径必须用 `await simulate_agent_call()` 而非同步版本，否则会冻结事件循环
- 跨主体 Rule 0 优先级高于 Rule 1：`apply_failure_rules` 先检查远端 failover，再检查本地重试
- `current_task_index` 必须 reset 到失败任务位置才能重新执行
- `find_alternative_remote_aoe` 跳过本地 IP（127/0.0.0/localhost）和已失败的远端 URL；仅匹配相同 capability
- `cross_host_sessions` 字段记录 task_id → remote_url 的映射，executor 路由用
- `failed_remote_aoe_urls: Dict[str, List[str]]` 按 task 维度记录已失败节点
- 子工作流（SubWorkflow）= 可被远端发现和调用的命名 Pipeline，复用 PipelineTopology 格式
- Executor 路由优先级：远端跨主体分发 > 本地子工作流 > 本地单 Agent
- 子工作流内部 Agent 生命周期由提供方节点自行管理，调用方只关心整体结果
- 不引入 Kafka/MQ，跨主体通信用 httpx 直连
- COMM 消息路由中间件（`src/service/message_router.py`）暂未实现
- 接口文档以“代码接口契约”为主体，HTTP API 作为子集纳入；必须覆盖 DistributedState、核心节点、共享配置契约

---

## 关键文件路径

| 文件 | 作用 |
|------|------|
| `src/graph/adaptive_orchestrator.py` | 所有工作流入口，自动选择编排模式 |
| `src/graph/distributed_types.py` | `DistributedState` 定义，所有状态字段在此 |
| `src/graph/distributed_nodes.py` | 核心节点：executor / monitor / apply_failure_rules |
| `src/graph/distributed_builder.py` | 构建 Planner→Executor→Monitor→Reporter 图 |
| `src/graph/magentic_nodes.py` | Magentic-One 节点（Progress Ledger 驱动） |
| `src/graph/progress_ledger.py` | 5 维 JSON 评估：满足/循环/进展/下一说话人/指令 |
| `接口/接口文档.md` | 统一接口文档：类、函数、状态对象、HTTP 接口契约 |
| `config/sub_workflows.json` | 子工作流定义（SubWorkflow as a Service） |
| `src/service/agent_registry.py` | AgentRegistryClient，ARDC 查询/注册（精确匹配，已移除 HNSW）+ 子工作流加载/Gossip 同步 |
| `src/service/agent_scheduler.py` | ASD subprocess 进程管理 |
| `agent_server.py` | FastAPI 节点服务，含 /orchestration/dispatch 等端点 |
| `src/app/app_logic_engine.py` | stop_app() / _cancel_remote_session() |
| `接口/系统接口与代码映射文档.md` | 统一接口文档：架构图+流程序列图+代码映射表+HTTP API+状态字段 |
| `接口/当前进度.md` | 实现状态档案（人工维护） |
| `接口/编排模式优化研究-基于OpenClaw借鉴分析.md` | 编排模式优化技术方案报告 |
| `src/api/visualization.py` | 可视化数据提取层(三场景 extract_*) |
| `visualization_server.py` | 独立可视化 FastAPI 服务(端口 8888) |
| `static/visualization.html` | H5 单页(Cytoscape + 原生 JS) |
| `scripts/run_visualization.sh` | 可视化启动脚本 |
| `src/service/workflow_scheduler.py` | 周期性工作流调度器(asyncio 循环 + 历史持久化) |
| `接口/项目交接文档.md` | 项目交接文档(11 章节，快速上手指南) |
