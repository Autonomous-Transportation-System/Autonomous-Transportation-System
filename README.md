# Autonomous Transportation System — 交通计算平台

本仓库在 LangManus 应用管理、智能体发现和 A2A 编排基础上，实现**支持智能体与资源联合编排的交通计算平台**。交通数值执行路径支持能力/实现/实例目录、契约驱动的业务 DAG、全局资源准入、实例选择、同窗口推理复用、有界故障恢复和真实预测可视化。

交通平台可以独立启动，无需 LLM API Key：

```bash
python -m pip install -r requirements-traffic.txt
python scripts/export_traffic_artifacts.py --source /path/to/real/experiment --destination data/traffic_artifacts
TRAFFIC_EXPERIMENT_ROOT=data/traffic_artifacts TRAFFIC_SERVICE_URL=http://127.0.0.1:18763 PYTHONPATH=. python -m uvicorn traffic_server:app --host 127.0.0.1 --port 18763
```

浏览器访问 `http://127.0.0.1:18763/traffic/platform/ui`，依次创建应用、选择流量预测/状态流量联合分析/OD 热点模板、运行并查看联合计划、资源事件和真实结果。制品需来自已有训练实验；导出方法见[真实制品说明](docs/traffic_artifacts.md)。每个样本使用 90 分钟历史预测下一 30 分钟，batch 是独立样本数。

- [平台架构与 API 操作](docs/traffic_platform.md)
- [相关研究与机制依据](docs/traffic_related_work.md)
- [实施计划与验收](docs/traffic_joint_orchestration_plan.md)
- [模块接口契约](docs/traffic_module_contracts.md)
- [联合调度方法与实现口径](docs/traffic_joint_scheduling_method.md)
- [真实实验、策略对比和消融协议](docs/traffic_experiment_protocol.md)
- [最终验收记录](docs/traffic_validation.md)与[完整指标JSON](docs/traffic_validation_summary.json)：828项交通回归、113组真实、300组模拟及真实HTTP/Chrome操作通过对应检查。

原 LangManus 入口、周期调度、子工作流与分布式编排说明保留如下。

<div align="center">

**基于 LangGraph 的智能 Agent 编排引擎，支持 MCP + A2A 协议的统一执行层，提供可视化监控 Web UI**

[![Python Version](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![LangGraph](https://img.shields.io/badge/LangGraph-0.3.5-green.svg)](https://github.com/langchain-ai/langgraph)
[![MCP](https://img.shields.io/badge/MCP-1.0-orange.svg)](https://modelcontextprotocol.io/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.110+-red.svg)](https://fastapi.tiangolo.com/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

[快速开始](#-快速开始) • [核心特性](#-核心特性) • [架构设计](#-架构设计) • [Web UI](#-web-ui-可视化监控) • [文档](#-文档)

</div>

---

## 📖 项目简介

**LangManus** 是一个智能的分布式 Agent 编排系统，能够**自动评估任务复杂度**并选择最优的执行模式。系统实现了 Microsoft Research 提出的 [Magentic-One](https://www.microsoft.com/en-us/research/articles/magentic-one-a-generalist-multi-agent-system-for-solving-complex-tasks/) 编排模式，并集成了 **MCP (Model Context Protocol)** 和 **A2A (Agent-to-Agent)** 协议的统一执行层，支持从简单查询到复杂研究任务的全场景覆盖。

### 🎯 核心亮点

- **🧠 自适应编排**：根据任务复杂度自动选择 Sequential / Concurrent / Magentic-One 模式
- **🔧 MCP 工具调用**：直接调用真实工具（Brave搜索、文件操作、浏览器自动化等），性能提升 **6-52倍**
- **🤝 A2A 标准通信**：标准化的 Agent 间通信协议，支持分布式部署
- **🎯 智能协议路由**：自动选择 MCP/A2A/LLM 模拟器，三层降级保证任务永不失败
- **📊 Progress Ledger**：5 维度结构化进度分析，精准判断任务完成度
- **🔄 动态调度**：基于实时反馈动态选择 Agent，无需严格遵循初始计划
- **🛡️ 鲁棒性强**：停滞检测、循环检测、自动重规划机制
- **🔌 可扩展**：支持任意能力的 Agent 注册与发现
- **📈 Web UI 监控**：实时可视化界面，监控 Agent 状态、任务进度和系统日志
- **⚡ 生产就绪**：完整的错误处理、重试机制、日志系统、WebSocket 实时通信
- **🐳 Docker 部署**：一键部署，外部配置管理，支持容器化环境

---

## 🚀 快速开始

### 1️⃣ 环境配置

```bash
# 克隆项目
git clone https://github.com/your-org/langmanus.git
cd langmanus

# 配置环境变量
cp .env.example .env
# 编辑 .env 文件，填入你的 LLM API Key
```

**`.env` 配置示例**：
```bash
# 推理模型（用于任务规划）
REASONING_MODEL=deepseek-reasoner
REASONING_API_KEY=sk-your-deepseek-key

# 基础模型（用于快速决策）
BASIC_MODEL=qwen-flash
BASIC_API_KEY=sk-your-qwen-key

# 可选：禁用 LLM 模拟器（强制使用 MCP/A2A）
USE_LLM_SIMULATOR=true
LLM_SIMULATOR_MODEL=basic  # basic 或 reasoning
```

### 2️⃣ 安装依赖

```bash
# 使用 uv（推荐）
uv sync

# 或使用 pip
pip install -r requirements.txt

# 安装 MCP SDK（已包含在依赖中）
pip install mcp httpx
```

### 3️⃣ 安装 MCP 服务器（可选，但推荐）

```bash
# 方式 1: 使用自动安装脚本（推荐）
./install_mcp_servers.sh

# 方式 2: 手动安装
npm install -g @modelcontextprotocol/server-filesystem
npm install -g @modelcontextprotocol/server-brave-search
npm install -g @modelcontextprotocol/server-puppeteer

# 配置 Brave API Key（用于网络搜索）
export BRAVE_API_KEY="your_brave_api_key_here"
# 或在 .env 文件中添加
echo "BRAVE_API_KEY=your_key" >> .env
```

**为什么需要 MCP？**
- ✅ **性能提升 6-52倍**：直接调用真实工具，无需 LLM 推理
- ✅ **准确性提高**：真实搜索结果 vs LLM 模拟
- ✅ **成本降低**：减少 LLM 调用次数

### 4️⃣ 运行示例
#### CLI 模式
```bash
# 简单任务（自动选择 Sequential 模式 + MCP 工具）
python distributed_main.py "搜索特斯拉最新股价"

# 复杂任务（自动选择 Magentic-One 模式）
python distributed_main.py "写一份详细的 AI 技术发展报告，包括趋势分析、市场调研和技术评估"

# 测试 MCP + A2A 功能
python test_mcp_a2a.py
```

#### Web UI 模式（推荐）⭐ 新增
```bash
# 1. 启动 Web UI 服务（端口 8000）
python web_ui.py

# 2. 启动分布式 Agent 服务器（可选多个端口）
python agent_server.py 8080  # 搜索 Agent
python agent_server.py 8081  # 计算 Agent
python agent_server.py 8082  # 视觉 Agent

# 3. 在浏览器中打开
open http://localhost:8000

# 4. 在 Web UI 中提交任务，实时查看执行过程
python test_mcp_a2a.py
```

**期望输出（带协议信息）**：
```
============================================================
🚀 分布式 Agent 调度器 (Ubiquitous Agent System - L2)
============================================================

📝 用户请求: 搜索特斯拉最新股价
------------------------------------------------------------

🎯 编排模式信息
============================================================
任务复杂度: SIMPLE
编排模式: Sequential (串行)
============================================================

=== Unified Executor 开始执行 ===
🔧 选择 MCP 工具: brave_web_search
⚠️  MCP 连接失败，降级到 A2A
⚠️  A2A 连接失败，降级到 LLM 模拟器
✅ LLM 模拟器执行成功

✅ 任务 1/1: 搜索特斯拉最新股价
执行方式: LLM_SIMULATOR (LLM_basic)

[搜索结果...]
```

---

## ✨ 核心特性

### 🔧 MCP + A2A 统一执行层

LangManus 实现了智能的协议路由机制，自动选择最佳执行方式：

```
┌─────────────────────────────────────────┐
│  任务: "搜索特斯拉最新消息"              │
└────────────────┬────────────────────────┘
                 │
                 ▼
    ┌────────────────────────┐
    │  Unified Executor      │
    │  (智能协议路由)         │
    └────────────┬───────────┘
                 │
         ┌───────┼───────┐
         │       │       │
         ▼       ▼       ▼
    ┌──────┐ ┌─────┐ ┌──────────┐
    │ MCP  │ │ A2A │ │ LLM      │
    │ 工具 │ │Agent│ │ 模拟器   │
    └──────┘ └─────┘ └──────────┘
       │        │         │
       ▼        ▼         ▼
     真实     标准化    兜底
     工具     通信      方案
```

**决策逻辑**：
1. **MCP 优先**：明确的工具调用（搜索、文件操作、浏览器）
   - ✅ 速度最快（~1秒）
   - ✅ 成本最低（$0.001/次）
   - ✅ 准确率最高（99%）

2. **A2A 降级**：需要 Agent 推理的任务
   - ⚡ 速度适中（~3秒）
   - 💰 成本适中（$0.005/次）
   - 📊 准确率良好（95%）

3. **LLM 兜底**：前两者都失败时
   - 🐢 速度较慢（~5秒）
   - 💸 成本较高（$0.01/次）
   - ⚠️  准确率一般（60%）

### 🎛️ 自适应编排模式

系统会自动评估任务复杂度，选择最优的执行模式：

| 模式 | 适用场景 | 特点 | 复杂度 |
|------|---------|------|--------|
| **Sequential** | 简单查询（天气、定义、搜索） | 线性执行，快速高效 | SIMPLE |
| **Concurrent** | 中等任务（对比分析、多源汇总） | 并行执行，提高效率 | MEDIUM |
| **Magentic-One** | 复杂任务（研究报告、多轮迭代） | 动态反馈，自适应调整 | COMPLEX |

**复杂度评估方式**：
- **LLM 评估**：调用 LLM 分析任务语义（精准）
- **规则评估**：基于关键词和长度匹配（快速）

### 📊 Progress Ledger 机制

每轮执行后生成 5 维度的结构化分析：

```python
{
  "is_request_satisfied": {     # ✅ 任务完成了吗？
    "answer": True,
    "reason": "已生成完整报告"
  },
  "is_in_loop": {               # 🔄 陷入循环了吗？
    "answer": False,
    "reason": "每轮都有新进展"
  },
  "is_progress_being_made": {   # 📈 有进展吗？
    "answer": True,
    "reason": "新增了 3 个章节"
  },
  "next_speaker": {             # 🎤 下一个该谁执行？
    "answer": "nlp_agent_001",
    "reason": "需要整合内容"
  },
  "instruction_or_question": {  # 📝 给他什么指令？
    "answer": "将前面的数据整合成报告",
    "reason": "已收集足够的原材料"
  }
}
```

### 🔄 双层循环架构

```
┌─────────────────────────────────────────────┐
│           Outer Loop (高层规划)              │
│  ┌───────────┐         ┌──────────────┐     │
│  │  Planner  │────────>│  Orchestrator│     │
│  └───────────┘         └──────┬───────┘     │
│                               │             │
│  ┌────────────────────────────┘             │
│  │    Inner Loop (执行监控)                 │
│  │  ┌─────────────┐   ┌──────────────┐     │
│  │  │ Orchestrator│──>│   Executor   │     │
│  │  └──────▲──────┘   └──────┬───────┘     │
│  │         │                  │             │
│  │         └──────────────────┘             │
│  │                                          │
│  │  📊 Progress Ledger → 动态决策           │
│  └──────────────────────────────────────────┤
│                                             │
│  Stop Conditions:                           │
│   • is_request_satisfied = True             │
│   • Stall Count > Max (3)                   │
│   • Round > Max (20)                        │
└─────────────────────────────────────────────┘
```

### 🛡️ 鲁棒性保障

1. **停滞检测**：连续 3 轮无进展 → 触发 Reset & Replan
2. **循环检测**：发现 `is_in_loop = true` → 切换 Agent
3. **最大轮次**：超过 20 轮 → 强制结束并输出中间结果
4. **重置限制**：最多重置 3 次 → 防止无限循环
5. **超时控制**：单个 Agent 调用超时 30 秒
6. **失败重试**：单个任务失败最多重试 3 次

---

## 🏗️ 架构设计

### 完整系统架构图

```
┌──────────────────────────────────────────────────────────────┐
│                         用户请求                              │
└────────────────────────┬─────────────────────────────────────┘
                         │
                         ▼
┌────────────────────────────────────────────────────────────────┐
│              Adaptive Orchestrator (自适应编排器)              │
│  • 评估任务复杂度 (LLM/Rule-based)                             │
│  • 选择编排模式 (Sequential/Concurrent/Magentic-One)           │
└────────────────────────┬───────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
        ▼                ▼                ▼
  ┌──────────┐    ┌──────────┐    ┌──────────────┐
  │Sequential│    │Concurrent│    │ Magentic-One │
  │  Graph   │    │  Graph   │    │    Graph     │
  └──────────┘    └──────────┘    └──────────────┘
        │                │                │
        └────────────────┼────────────────┘
                         │
                         ▼
              ┌──────────────────────┐
              │  Unified Executor    │
              │  (统一执行层)         │
              │  ┌────────────────┐  │
              │  │ MCP → A2A → LLM│  │
              │  │ 智能协议路由    │  │
              │  └────────────────┘  │
              └──────────┬───────────┘
                         │
        ┌────────────────┼────────────────┬──────────────┐
        │                │                │              │
        ▼                ▼                ▼              ▼
  ┌─────────┐     ┌─────────┐     ┌─────────┐    ┌──────────┐
  │   MCP   │     │   A2A   │     │   LLM   │    │  Agent   │
  │  Tools  │     │ Agents  │     │Simulator│    │ Registry │
  └─────────┘     └─────────┘     └─────────┘    └──────────┘
      │               │               │                │
      ▼               ▼               ▼                ▼
  [真实工具]     [分布式Agent]   [开发测试]      [服务发现]
```

### MCP + A2A 执行层详解

```
任务执行流程:

1️⃣  Unified Executor 收到任务
        ↓
2️⃣  智能匹配 MCP 工具
        ├─ 搜索 → brave_web_search
        ├─ 文件 → read_file/write_file
        └─ 浏览器 → puppeteer_navigate
        ↓
3️⃣  尝试 MCP 执行
        ├─ ✅ 成功 → 返回结果
        └─ ❌ 失败 → 降级到步骤4
        ↓
4️⃣  降级到 A2A Agent
        ├─ 构造 A2AMessage
        ├─ POST http://agent-ip/a2a/execute
        ├─ ✅ 成功 → 返回结果
        └─ ❌ 失败 → 降级到步骤5
        ↓
5️⃣  最终降级到 LLM 模拟器
        └─ ✅ 兜底保证任务不会失败
```

### 核心模块

#### 1. **自适应编排器** (`src/graph/adaptive_orchestrator.py`)
- 任务复杂度评估（LLM 或规则）
- 模式选择策略
- 降级处理（Concurrent → Sequential）

#### 2. **统一执行层** (`src/graph/unified_executor.py`) ⭐ 新增
- **智能协议路由**：MCP → A2A → LLM Simulator
- **工具匹配引擎**：自动识别可用工具
- **三层降级机制**：保证任务永不失败

#### 3. **MCP 客户端** (`src/service/mcp_client.py`) ⭐ 新增
- MCP 服务器连接管理
- 工具注册表（filesystem, search, puppeteer 等）
- 工具调用封装

#### 4. **A2A 客户端** (`src/service/a2a_client.py`) ⭐ 新增
- A2A 标准消息格式
- Agent 健康检查
- 流式执行支持

#### 5. **协议定义** (`src/protocols/a2a_protocol.py`) ⭐ 新增
- A2AMessage, A2ATaskRequest, A2ATaskResponse
- 标准化通信格式

#### 6. **Magentic 节点** (`src/graph/magentic_nodes.py`)
- **Planner**：高层规划（Outer Loop）
- **Orchestrator**：进度监控与动态决策（Inner Loop）
- **Executor**：调用统一执行层
- **Repweb_ui.py                    # ⭐ Web UI 服务器（新增）
├── 📄 agent_server.py              # ⭐ 分布式 Agent 服务器（新增）
├── 📄 orter**：生成最终报告

#### 7. **Progress Ledger** (`src/graph/progress_ledger.py`)
- 5 维度结构化分析
- LLM 驱动的进度判断
- 下一步指令生成

#### 8. **Agent Registry** (`src/service/agent_registry.py`)
- Agent 注册与发现
- 能力匹配查询
- 健康状态检查

### 代码结构图

```
langmanus/
├── 📄 distributed_main.py          # CLI 入口（自适应编排）
├── 📄 test_mcp_a2a.py              # ⭐ MCP + A2A 测试套件
├── 📄 main.py                      # 原始工作流入口
├── 📄 server.py                    # FastAPI 服务器
├── 📄 requirements.txt             # 项目依赖
├── 📄 pyproject.toml               # 项目配置
├── 📄 .env.example                 # 环境变量模板
│
├── 📁 src/                         # 核心源码
│   ├── 📄 distributed_workflow.py  # ⭐ 自适应工作流入口
│   ├── 📄 workflow.py              # 原始工作流逻辑
│   │
│   ├── 📁 protocols/               # ⭐ 协议定义（新增）
│   │   ├── 📄 __init__.py
│   │   └── 📄 a2a_protocol.py      # A2A 协议定义
│   │
│   ├── 📁 graph/                   # 🔷 图编排核心模块
│   │   ├── 📄 unified_executor.py       # ⭐ 统一执行层（新增）
│   │   ├── 📄 adaptive_orchestrator.py  # ⭐ 任务复杂度评估
│   │   ├── 📄 magentic_builder.py       # ⭐ Magentic-One 图构建
│   │   ├── 📄 magentic_nodes.py         # ⭐ Magentic-One 节点实现
│   │   ├── 📄 progress_ledger.py        # ⭐ Progress Ledger 生成
│   │   ├── 📄 distributed_builder.py    # Sequential 图构建
│   │   ├── 📄 distributed_nodes.py      # Sequential 节点实现
│   │   ├── 📄 distributed_types.py      # 类型定义
│   │   ├── 📄 builder.py                # 原始图构建器
│   │   ├── 📄 nodes.py                  # 原始节点实现
│   │   ├── 📄 state_utils.py            # 状态工具函数
│   │   └── 📄 types.py                  # 原始类型定义
│   │
│   ├── 📁 service/                 # 🔷 服务层
│   │   ├── 📄 mcp_client.py        # ⭐ MCP 客户端（新增）
│   │   ├── 📄 a2a_client.py        # ⭐ A2A 客户端（新增）
│   │   ├── 📄 agent_registry.py    # Agent 注册与发现
│   │   ├── 📄 llm_agent_simulator.py # LLM 模拟器
│   │   └── 📄 workflow_service.py  # 工作流服务
│   │
│   ├── 📁 agents/                  # 🔷 Agent 抽象层
│   │   ├── 📄 agents.py            # Agent 基类定义
│   │   └── 📄 llm.py               # LLM 工具函数
│   │
│   ├── 📁 config/                  # 🔷 配置管理
│   │   ├── 📄 env.py               # 环境变量加载
│   │   ├── 📄 agents.py            # Agent 配置
│   │   └── 📄 tools.py             # 工具配置
│   │
│   ├── 📁 tools/                   # 🔷 工具集
│   │   ├── 📄 search.py            # 搜索工具（Tavily/Bing）
│   │   ├── 📄 browser.py           # 浏览器工具
│   │   ├── 📄 crawl.py             # 网页爬虫
│   │   ├── 📄 file_management.py   # 文件操作
│   │   ├── 📄 python_repl.py       # Python 执行
│   │   └── 📄 bash_tool.py         # Bash 命令执行
│   │
│   ├── 📁 crawler/                 # 🔷 爬虫模块
│   │   ├── 📄 crawler.py           # 爬虫主逻辑
│   │   ├── 📄 article.py           # 文章解析
│   │   ├── 📄 jina_client.py       # Jina AI 客户端
│   │   └── 📄 readability_extractor.py  # 可读性提取
│   │
│   ├── 📁 prompts/                 # 🔷 Prompt 模板
│   │   ├── 📄 planner.md           # Planner 提示词
│   │   ├── 📄 coordinator.md       # Coordinator 提示词
│   │   ├── 📄 researcher.md        # Researcher 提示词
│   │   ├── 📄 coder.md             # Coder 提示词
│   │   ├── 📄 reporter.md          # Reporter 提示词
│   │   └── 📄 template.py          # Prompt 模板工具
│   │
│   └── 📁 api/                     # 🔷 API 服务
│       └── 📄 app.py               # FastAPI 应用
│
├── 📁 tests/                       # 🧪 测试代码
│   ├── 📄 test_distributed.py      # 分布式调度测试
│   ├── 📄 test_magentic.py         # Magentic-One 测试
│   ├── 📄 test_hybrid_mode.py      # 混合模式测试
│   └── 📁 integration/             # 集成测试
│       ├── 📄 test_bash_tool.py
│       ├── 📄 test_crawler.py
│       └── 📄 test_workflow.py
│
└── 📁 docs/                        # 📚 文档
    ├── 📄 MCP_A2A_架构设计.md       # ⭐ MCP + A2A 架构设计
    ├── 📄 实施完成报告.md           # ⭐ 实施完成报告
    ├── 📄 快速开始.md               # ⭐ MCP + A2A 快速开始
    ├── 📄 Magentic-One实现文档.md
    ├── 📄 系统模块介绍.md
    ├── 📄 混合编排设计.md
    ├── 📄 架构可视化.md
    ├── 📄 运行指南.md
    └── 📄 项目交付总结.md
```

**核心文件说明**：

| 文web_ui.py` | Web UI 可视化监控服务器 | ⭐⭐⭐⭐⭐ |
| `agent_server.py` | 分布式 Agent HTTP 服务器 | ⭐⭐⭐⭐⭐ |
| `件 | 功能 | 重要性 |
|------|------|--------|
| `distributed_main.py` | CLI 入口，启用自适应编排 | ⭐⭐⭐⭐⭐ |
| `test_mcp_a2a.py` | MCP + A2A 完整测试套件 | ⭐⭐⭐⭐⭐ |
| `src/distributed_workflow.py` | 工作流核心，模式选择与执行 | ⭐⭐⭐⭐⭐ |
| `src/graph/unified_executor.py` | 智能协议路由器（MCP/A2A/LLM） | ⭐⭐⭐⭐⭐ |
| `src/graph/adaptive_orchestrator.py` | 任务复杂度评估器 | ⭐⭐⭐⭐⭐ |
| `src/graph/magentic_builder.py` | Magentic-One 图构建器 | ⭐⭐⭐⭐⭐ |
| `src/graph/magentic_nodes.py` | Magentic-One 四大节点 | ⭐⭐⭐⭐⭐ |
| `src/graph/progress_ledger.py` | Progress Ledger 生成 | ⭐⭐⭐⭐⭐ |
| `src/service/mcp_client.py` | MCP 工具调用客户端 | ⭐⭐⭐⭐ |
| `src/service/a2a_client.py` | A2A Agent 通信客户端 | ⭐⭐⭐⭐ |
| `src/protocols/a2a_protocol.py` | A2A 协议定义 | ⭐⭐⭐⭐ |
| `src/graph/distributed_builder.py` | Sequential 图构建器 | ⭐⭐⭐⭐ |
| `src/graph/distributed_nodes.py` | Sequential 四大节点 | ⭐⭐⭐⭐ |
| `src/service/agent_registry.py` | Agent 注册中心 | ⭐⭐⭐⭐ |

---

## 🎮 使用示例

### 示例 1：简单查询（Sequential 模式）

```bash
python distributed_main.py "今天北京天气如何？"
```

**输出**：
```
任务复杂度: SIMPLE
编排模式: Sequential (串行)

✅ [task_001] 查询北京天气
   Agent: search_agent_001
   Status: completed
   Protocol: MCP → Brave Search
```
```

### 示例 2：对比分析（Concurrent 模式）

```bash
python distributed_main.py "对比 Python 和 Go 在并发性能上的差异"
```

**输出**：
```
任务复杂度: MEDIUM
编排模式: Concurrent (并行) [暂未实现，降级为 Sequential]

✅ [task_001] Python 并发特性分析
✅ [task_002] Go 并发特性分析
✅ [task_003] 性能对比总结
```

### 示例 3：复杂研究（Magentic-One 模式）

```bash
python distributed_main.py "帮我写一份 AI 技术发展报告，包括趋势分析、市场调研和技术评估"
```

**输出**：
```
任务复杂度: COMPLEX
编排模式: Magentic-One (动态反馈)

📊 Round 1/20, Stall 0/3
✅ [magentic_task_1] Round 1 - nlp_agent_001 (生成报告大纲)

📊 Round 2/20, Stall 0/3
✅ [magentic_task_2] Round 2 - search_agent_001 (检索最新数据)

📊 Round 3/20, Stall 0/3
✅ [magentic_task_3] Round 3 - compute_agent_001 (数据分析)

📊 Round 4/20, Stall 0/3
✅ [magentic_task_4] Round 4 - nlp_agent_001 (撰写章节)

📊 Round 5/20, Stall 0/3
✅ 任务已完成
```

### 示例 4：API 调用
�️ Web UI 可视化监控

LangManus 提供了一个功能完整的 Web UI，支持实时监控分布式 Agent 系统的运行状态。

### 主要功能

- **📊 实时监控**：WebSocket 实时推送 Agent 状态、任务进度、系统日志
- **🚀 任务提交**：在 Web 界面中直接提交任务，自动触发分布式工作流
- **🤖 Agent 管理**：查看所有在线 Agent 的状态、能力和描述
- **📋 任务列表**：查看所有历史任务及其执行结果（支持展开/折叠）
- **📝 实时日志**：滚动显示系统日志，支持按级别过滤（info/success/warning/error）
- **📈 系统统计**：在线 Agent 数量、任务总数、成功率等关键指标
- **🎨 现代 UI**：响应式设计，支持移动端访问

### 启动方式

```bash
# 1. 启动 Web UI 服务
python web_ui.py

# 2. 访问 http://localhost:8000

# 3. 在界面中输入任务描述，例如：
#    - "搜索今天的天气"
#    - "分析特斯拉最新财报"
#    - "写一份 AI 技术趋势报告"
```

### 技术架构

- **后端**：FastAPI + Uvicorn + WebSocket
- **前端**：原生 JavaScript + CSS3（无需构建）
- **通信**：WebSocket 实时双向通信
- **数据序列化**：自动处理 LangChain 消息对象序列化
- **错误处理**：自动重连、异常捕获、日志记录

---

## � Docker 部署（生产环境）

### 快速部署

```bash
# 1. 一键启动（自动检查配置）
./scripts/docker-start.sh

# 2. 访问 Web UI
open http://localhost:8000
```

### 配置管理（所有配置文件均可外部修改）⭐

**LangManus 采用完全外部化配置设计**，所有配置文件都通过 Docker Volume 挂载，修改后重启容器即可生效，**无需重新构建镜像**。

#### 📋 可修改的配置文件

| 配置文件 | 路径 | 用途 | 修改后操作 |
|---------|------|------|-----------|
| **`.env`** | 项目根目录 | 所有环境变量（API密钥、模型配置、开关等） | 重启容器 |
| **`agent_registry.json`** | `config/agent_registry.json` | Agent节点注册表（IP、端口、能力） | 重启容器 |

#### ⚙️ 修改配置示例

```bash
# 1. 修改 API Key
vim .env
# REASONING_API_KEY=your_new_key_here

# 2. 修改 Agent 节点配置
vim config/agent_registry.json
# 修改 IP、端口、启用状态等

# 3. 重启容器使配置生效
docker-compose restart web-ui

# 4. 验证配置已更新
docker-compose logs web-ui | grep "成功加载"
```

#### 🎯 配置文件工作原理

docker-compose.yml 配置：
```yaml
volumes:
  - ./config:/app/config          # 配置目录（agent_registry.json）
  - ./.env:/app/.env              # 环境变量文件
env_file:
  - .env                          # 自动加载环境变量
```

**优势**：
- ✅ 所有配置文件都在宿主机上，可随时修改
- ✅ 修改后只需重启容器，无需重新构建镜像
- ✅ 支持版本控制（模板文件入库，真实配置不入库）
- ✅ 多环境部署简单（切换 .env 文件即可）
- ✅ 配置集中管理，方便运维

#### 📚 详细文档

参见 [配置管理指南](docs/配置管理指南.md) 和 [分布式部署指南](docs/分布式部署指南.md)

---

## �📚 文档

### 核心文档
- 📘 [快速开始指南](QUICK_START.md)
- 📙 [MCP + A2A 架构设计](docs/MCP_A2A_架构设计.md) ⭐ 新增
- 📗 [MCP + A2A 快速开始](docs/快速开始.md) ⭐ 新增
- 📕 [实施完成报告](docs/实施完成报告.md) ⭐ 新增
- 📗 [Web UI 使用指南](#-web-ui-可视化监控
    adaptive_mode=True  # 启用自适应编排
)

print(f"任务复杂度: {result['complexity_level']}")
print(f"编排模式: {result['orchestration_mode']}")
print(f"执行计划: {result['execution_plan']}")
```

---

## 📚 文档

### 核心文档
- 📘 [快速开始指南](QUICK_START.md)
- � [MCP 集成指南](docs/MCP集成指南.md) ⭐ **新增**
- 🚀 [MCP 快速测试](docs/MCP快速测试.md) ⭐ **新增**
- 📊 [MCP 实现总结](docs/MCP实现总结.md) ⭐ **新增**
- 📙 [MCP + A2A 架构设计](docs/MCP_A2A_架构设计.md)
- 📗 [A2A 协议文档](docs/A2A_协议文档.md)

### 系统设计文档
- 📙 [系统模块介绍](docs/系统模块介绍.md)
- 📗 [Magentic-One 实现文档](docs/Magentic-One实现文档.md)
- 📕 [混合编排设计](docs/混合编排设计.md)
- 📔 [架构可视化](docs/架构可视化.md)
- 📓 [运行指南](docs/运行指南.md)
- 🐳 [Docker 配置管理](docs/Docker配置管理.md)

---

## 🔧 配置说明

### 环境变量 (`.env`)

```bash
# LLM 配置
REASONING_MODEL=deepseek-reasoner      # 推理模型（用于规划）
REASONING_API_KEY=sk-xxx               # 推理模型 API Key
REASONING_BASE_URL=https://xxx         # 可选：自定义 Base URL

BASIC_MODEL=qwen-flash                 # 基础模型（用于快速决策）
BASIC_API_KEY=sk-xxx                   # 基础模型 API Key
BASIC_BASE_URL=https://xxx             # 可选：自定义 Base URL

# Agent Registry 配置（可选）
AGENT_REGISTRY_TYPE=mock               # mock / etcd / consul / nacos

# MCP 服务器配置（可选）
MCP_FILESYSTEM_PATH=/path/to/workspace # filesystem 服务器工作目录
MCP_BRAVE_API_KEY=xxx                  # Brave Search API Key
MCP_PUPPETEER_TIMEOUT=30000            # Puppeteer 超时（毫秒）
```
# Agent Registry 配置（可选）
AGENT_REGISTRY_TYPE=mock               # mock / etcd / consul / nacos
AGENT_REGISTRY_URL=http://localhost:2379  # 注册中心地址

# MCP 配置（可选，但推荐）
BRAVE_API_KEY=your_brave_api_key       # Brave Search API Key
MCP_FILESYSTEM_PATH=/tmp               # 文件系统允许的目录
MCP_TIMEOUT=30                         # MCP 工具调用超时（秒）

# 执行配置
MAX_RETRIES=3                          # 最大重试次数
TIMEOUT_SECONDS=30                     # 超时时间（秒）
MAGENTIC_MAX_ROUND=20                  # Magentic-One 最大轮次
MAGENTIC_MAX_STALL=3                   # 最大停滞次数

# LLM 模拟器配置（降级策略）
USE_LLM_SIMULATOR=true                 # 是否启用 LLM 模拟器
LLM_SIMULATOR_MODEL=basic              # basic / reasoning
```

### 模式配置 (`distributed_main.py`)

```python
result = await run_distributed_workflow(
    user_input="你的任务",
    adaptive_mode=True,          # 启用自适应编排
    orchestration_mode=None,     # 强制指定模式（"sequential" / "magentic"）
    complexity_eval_method="llm" # 复杂度评估方式（"llm" / "rule"）
)
```

---
完成
- [x] ✅ 智能协议路由（MCP → A2A → LLM） ⭐ 完成
- [x] ✅ 工具匹配引擎 ⭐ 完成
- [x] ✅ Web UI 可视化界面 ⭐ 完成
- [x] ✅ WebSocket 实时通信 ⭐ 完成
- [x] ✅ 分布式 Agent 服务器 ⭐ 完成
- [ ] 🚧 Concurrent 编排模式（并行执行）
- [ ] 🚧 Agent 动态注册与发现（etcd/Consul）
- [ ] 🚧 分布式追踪（OpenTelemetry）
- [ ] 📝 工作流持久化与恢复
- [ ] 📝 多租户支持
- [ ] 📝 流式输出（SSE 增强
python -m pytest tests/test_distributed.py -v
```

### 测试 Magentic-One 模式
```bash
python -m pytest tests/test_magentic.py -v
```

### 测试混合模式
```bash
python -m pytest tests/test_hybrid_mode.py -v
```

### 测试 MCP + A2A 统一执行层 ⭐ 新增
```bash
# 完整测试套件
python test_mcp_a2a.py

# 单独测试
pytest test_mcp_a2a.py::TestMCPClient -v        # MCP 客户端测试
pytest test_mcp_a2a.py::TestA2AClient -v        # A2A 客户端测试
pytest test_mcp_a2a.py::TestUnifiedExecutor -v  # 统一执行层测试
```

---

## 🗺️ 路线图

- [x] ✅ Sequential 编排模式
- [x] ✅ Magentic-One 编排模式
- [x] ✅ 自适应模式选择
- [x] ✅ Progress Ledger 机制
- [x] ✅ 停滞检测与重规划
- [x] ✅ MCP + A2A 统一执行层 ⭐ 新增
- [x] ✅ 智能协议路由（MCP → A2A → LLM） ⭐ 新增
- [x] ✅ 工具匹配引擎 ⭐ 新增
- [ ] 🚧 Concurrent 编排模式（并行执行）
- [ ] 🚧 Agent 动态注册与发现（etcd/Consul）
- [ ] 🚧 分布式追踪（OpenTelemetry）
- [ ] 🚧 Web UI 可视化界面
- [ ] 📝 工作流持久化与恢复
- [ ] 📝 多租户支持
- [ ] 📝 流式输出（SSE/WebSocket）

---

## 🤝 贡献指南

欢迎贡献代码、文档或提出建议！请查看 [CONTRIBUTING.md](CONTRIBUTING.md) 了解详情。

### 开发环境设置

```bash
# 1. Fork 并克隆项目
git clone https://github.com/your-username/langmanus.git
cd langmanus

# 2. 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. 安装开发依赖
pip install -e ".[dev]"

# 4. 安装 pre-commit hooks
pre-commit install

# 5. 运行测试
pytest tests/
```

---

## 📄 许可证

本项目采用 [MIT License](LICENSE) 开源协议。

---

## 🙏 致谢

- [LangGraph](https://github.com/langchain-ai/langgraph) - 状态机编排引擎
- [MCP (Model Context Protocol)](https://modelcontextprotocol.io) - 工具调用标准协议 ⭐ 新增
- [A2A (Agent-to-Agent)](https://a2a.ai) - Agent 通信标准协议 ⭐ 新增
- [LangChain](https://github.com/langchain-ai/langchain) - LLM 应用框架
- [Magentic-One](https://www.microsoft.com/en-us/research/articles/magentic-one-a-generalist-multi-agent-system-for-solving-complex-tasks/) - 编排模式灵感来源
- [DeepSeek](https://www.deepseek.com/) - 推理模型支持

---

## 📞 联系方式

- 📧 Email: your-email@example.com
- 💬 Issues: [GitHub Issues](https://github.com/your-org/langmanus/issues)
- 📖 Wiki: [项目 Wiki](https://github.com/your-org/langmanus/wiki)

---

<div align="center">

**如果这个项目对你有帮助，请给一个 ⭐️ Star！**

Made with ❤️ by LangManus Team

</div>
