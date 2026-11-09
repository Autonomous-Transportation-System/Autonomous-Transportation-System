# TASKS.md — Sprint 任务看板

> 最后更新：2026-11-09

---

## Sprint 当前目标

支持智能体与资源联合编排的交通计算平台：实现、真实实验、验收与新分支交付

---

## 进行中

本次交通联合平台任务已完成；后续原平台扩展列于“待办”。


---

## 待办

- [ ] **P0** Focus List 结构化工作记忆：`distributed_types.py` 加字段 + `progress_ledger.py` 加第 6 维 + `magentic_nodes.py` 维护 focus_list
- [ ] **P1** Monitor 主动超时检测（Watchdog）：`distributed_nodes.py` Monitor 节点 + `agent_server.py` 新增进度查询端点
- [ ] **P1** 跨主体异步 Dispatch：Executor 非阻塞调用 + Monitor 轮询/回调 + TaskAssignment 增加 dispatched 状态
- [ ] **P2** 事件触发层：新增 `src/service/event_trigger.py` 模块 + `server.py` webhook 端点
- [ ] **P2** 自适应控制参数：Progress Ledger 增加 strategy_adjustment 维度
- [ ] COMM 消息路由中间件（`src/service/message_router.py`）：按 capability 路由，解耦 Agent 直连
- [ ] RRDC 跨节点资源分配（当前为内存 mock）
- [ ] ASD 接入 Docker SDK（当前为 subprocess 替代）
- [ ] 跨主体编排集成测试：两进程模拟 T_AOE + E_AOE 全链路

---

## 已完成（本 Sprint）

- [x] **新分支交付**：功能提交e3a7cb3已推送到feat/traffic-joint-orchestration-20261109，远端SHA核对一致；GitHub仓库已迁移至Autonomous-Transportation-System组织。完整平台在本机18764端口运行。

- [x] **交通联合平台工程实现与验收**（2026-11-09）：能力/实现/实例目录、三业务及自定义 DAG、实际契约适配、共享资源池五种调度、真实 B1 两实例、缓存/计算合并、有限恢复、应用/注册/QoS桥接、HTTP/A2A与五视图；828项回归通过，独立审查缺陷已修复。
- [x] **正式实验及文档**：113组真实、300组模拟完成并独立核验；保存原始JSON/CSV、事件、采样与SHA，汇总到docs/traffic_validation.md及traffic_validation_summary.json。五视图截图与报告关键区域已归档，保护源及Word指纹保持一致。
- [x] **独立源码仓库与制品隔离**：从远端 main 创建新分支，备份旧基础代码并记录保护源/Word 指纹；真实模型和数据原样导出至 gitignored 目录。
- [x] **复运行协议与 SSH 推送准备**：提供 standalone 依赖、导出/实验脚本与模块契约；专用 GitHub SSH 认证及新分支推送 dry-run 已通过。

- [x] **周期性工作流调度**（2026-05-07）：新增 `src/service/workflow_scheduler.py` WorkflowScheduler 单例。`models.py` 新增 `ScheduleExecutionRecord` + AppStatus `"scheduled"`。`app_manager.py` 新增 `start_schedule()/stop_schedule()/restore_schedules()`。`app_logic_engine.py` 暴露 `run_single_workflow()`。`app.py` 新增 4 个调度 API + startup 自动恢复。`ui.py` 前端新增调度配置字段 + 控制按钮 + 历史面板。32 个集成测试全通过。
- [x] **项目交接文档**（2026-05-07）：编写 `接口/项目交接文档.md`，11 章节覆盖项目简介、快速启动、架构概览、核心模块详解、目录结构、API 速查、骨架说明、关键约束、进度与待办、常见问题、文档索引。
- [x] **子工作流即服务（Sub-Workflow as a Service）**（2026-05-07）：新增 `config/sub_workflows.json` 定义子工作流（复用 PipelineTopology 格式）。`distributed_types.py` 新增 `SubWorkflowInfo` + `TaskAssignment.sub_workflow_id`。`agent_registry.py` 加载/查询/Gossip 同步子工作流。Planner LLM prompt 展示子工作流，Executor 新增三路径路由（远端→本地子工作流→本地 Agent）。`agent_server.py` E_AOE 支持 `sub_workflow_id` 查找本地 pipeline 执行。32 个集成测试全通过。
- [x] **跨主体编排主动触发修复**（2026-05-07）：修复 `identify_cross_host_tasks()` 从未被调用的缺陷。在 `distributed_planner_node()` Pipeline 路径和 LLM 路径两个出口注入调用，写入 `cross_host_sessions`。`distributed_workflow.py` 补充跨节点 state 字段初始化。
- [x] **可视化与主服务联动**（2026-05-07）：可视化合并进 `server.py`(端口 8000)。新增工作流总线 `src/service/viz_bus.py`(VizBus 单例,多工作流注册+订阅) + `src/api/visualization_routes.py`(APIRouter 挂载到 app)。改造 `distributed_workflow.py` 用 `graph.astream()` 每节点 publish state。前端加工作流下拉选择器 + 双 WebSocket(全局列表 + 单工作流) + URL `?wf=<id>` 路由。**真实分布式工作流自动出现在 /viz 页面**,支持多工作流并发切换。访问: `python server.py` → http://127.0.0.1:8000/viz
- [x] **编排过程动态可视化 H5**（2026-04-22）：独立 FastAPI 服务 `visualization_server.py`(默认端口 8888)，提供 3 场景:① 编排过程(Skills/平台候选 Agent/已选高亮)、② 拓扑结果(Cytoscape+dagre 平台分组拓扑图)、③ 工作流执行(进度条+当前 Agent+MCP/A2A 工具调用气泡+时间线)。HTTP API + WebSocket 实时推送 + Demo/Live 双模式。数据提取层 `src/api/visualization.py`。启动: `./scripts/run_visualization.sh`
- [x] 编排模式优化研究报告：对比 LangManus vs OpenClaw/Clawith Aware 编排模式，识别 5 个借鉴方向（Focus List / Monitor Watchdog / 异步 Dispatch / 事件触发层 / 自适应参数），输出至 `接口/编排模式优化研究-基于OpenClaw借鉴分析.md`（2026-03-30）
- [x] 修复多应用工作流并行执行（两阶段）：①节点 async 化 ②executor 降级路径 simulate_agent_call_sync→await simulate_agent_call + 模拟器内部 llm.stream→llm.astream，消除事件循环阻塞（2026-03-30）
- [x] HNSW 向量检索全量移除：删除 3 文件 + 修改 agent_registry.py + 移除 hnswlib/sentence-transformers 依赖 + 清理 10 个文档中 HNSW 引用（2026-03-29）
- [x] 创建 CLAUDE.md（项目规范入口，AI 自动读取）
- [x] 创建 PROJECT_CONTEXT.md（项目状态快照）
- [x] 创建 TASKS.md（Sprint 任务看板）
- [x] QoS → ASD 反馈闭环：register_alert_callback + cooldown + redeploy_agent + reset_metrics（2026-03-08）
- [x] 创建 `/接口/接口文档.md`：补齐应用层 / 编排层 / 运行层代码接口契约与对外接口说明（2026-03-10）
- [x] 创建 `接口/系统接口与代码映射文档.md`：统一接口文档 — 架构图+类关系图+流程序列图+代码映射表+HTTP API+DistributedState 全字段+设计模式+骨架区域说明（2026-03-10）

---

## 已完成（历史）

- [x] §1 应用层：install / start / stop / display + Skills.md 在线编辑
- [x] §2.1 单主体工作流：planner → executor → monitor → reporter
- [x] ARDC HTTP Gossip 智能体发现（PEER_AOE_URLS + /registry/sync）
- [x] ASD subprocess 进程管理（自动端口分配 + 就绪检测）
- [x] §2.2 跨主体编排：httpx dispatch + SessionRegistry + 引用计数
- [x] §2.3 跨主体重编排（Rule 0 failover + find_alternative_remote_aoe）
- [x] §2.4 跨主体工作流停止（DELETE /orchestration/session/{id} 传播）
