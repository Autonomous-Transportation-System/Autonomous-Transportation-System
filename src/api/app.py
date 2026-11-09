"""
FastAPI application for LangManus.
"""

import json
import logging
from typing import Dict, List, Any, Optional, Union

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sse_starlette.sse import EventSourceResponse
import asyncio
from typing import AsyncGenerator, Dict, List, Any

from src.graph import build_graph
from src.config import TEAM_MEMBERS
from src.service.workflow_service import run_agent_workflow
from src.api.visualization_routes import router as viz_router
from src.api.traffic import router as traffic_router, get_runtime as get_traffic_runtime
from src.api.traffic_platform import router as traffic_platform_router, shutdown_platform

# Configure logging
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="LangManus API",
    description="API for LangManus LangGraph-based agent workflow",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Create the graph
graph = build_graph()

# 挂载可视化路由 (端口 8000 → /viz, /api/viz/*, /ws/viz/*)
app.include_router(viz_router)
app.include_router(traffic_router)
app.include_router(traffic_platform_router)


@app.on_event("shutdown")
async def shutdown_traffic_services():
    await shutdown_platform()
    await get_traffic_runtime().shutdown()


# ======================================================================
# ARDC Gossip 端点（主 API 服务也支持 peer 同步）
# ======================================================================

class _RegistrySyncRequest(BaseModel):
    source_url: str
    agents: list
    sub_workflows: list = []  # 子工作流列表（可选，向后兼容）


@app.post("/registry/sync", summary="ARDC Gossip 接收 peer 推送")
async def registry_sync(req: _RegistrySyncRequest):
    """接收来自 peer 节点的 agent 列表和子工作流列表，合并到本节点注册表"""
    from src.service.agent_registry import get_registry_client
    registry = get_registry_client()
    merged_count = registry.sync_from_peer(
        req.source_url,
        req.agents,
        sub_workflows=req.sub_workflows or None,
    )
    return {
        "status": "ok",
        "source_url": req.source_url,
        "received_agents": len(req.agents),
        "received_sub_workflows": len(req.sub_workflows),
        "merged_count": merged_count,
    }


@app.on_event("startup")
async def _start_gossip():
    """
    应用启动时，若配置了 PEER_AOE_URLS 环境变量则启动 gossip 后台任务。
    PEER_AOE_URLS 格式：逗号分隔的 HTTP URL，如
        PEER_AOE_URLS=http://192.168.1.20:8000,http://192.168.1.21:8000
    LOCAL_AOE_URL 指定本节点地址（默认 http://localhost:8000）。
    """
    import os
    from src.service.agent_registry import get_registry_client

    peer_urls_raw = os.getenv("PEER_AOE_URLS", "")
    peer_urls = [u.strip() for u in peer_urls_raw.split(",") if u.strip()]
    local_url = os.getenv("LOCAL_AOE_URL", "http://localhost:8000")
    interval = int(os.getenv("GOSSIP_INTERVAL", "30"))

    registry = get_registry_client()
    await registry.start_gossip_background(
        peer_urls=peer_urls,
        local_url=local_url,
        interval=interval,
    )


@app.on_event("startup")
async def _restore_scheduled_apps():
    """
    服务启动时，自动恢复配置了 schedule_auto_restart: true 的周期调度应用。
    """
    try:
        from src.app.app_manager import get_app_manager
        manager = get_app_manager()
        await manager.restore_schedules()
    except Exception as e:
        logger.warning(f"[Startup] 恢复周期调度失败（非关键）: {e}")


class ContentItem(BaseModel):
    type: str = Field(..., description="The type of content (text, image, etc.)")
    text: Optional[str] = Field(None, description="The text content if type is 'text'")
    image_url: Optional[str] = Field(
        None, description="The image URL if type is 'image'"
    )


class ChatMessage(BaseModel):
    role: str = Field(
        ..., description="The role of the message sender (user or assistant)"
    )
    content: Union[str, List[ContentItem]] = Field(
        ...,
        description="The content of the message, either a string or a list of content items",
    )


class ChatRequest(BaseModel):
    messages: List[ChatMessage] = Field(..., description="The conversation history")
    debug: Optional[bool] = Field(False, description="Whether to enable debug logging")
    deep_thinking_mode: Optional[bool] = Field(
        False, description="Whether to enable deep thinking mode"
    )
    search_before_planning: Optional[bool] = Field(
        False, description="Whether to search before planning"
    )


@app.post("/api/chat/stream")
async def chat_endpoint(request: ChatRequest, req: Request):
    """
    Chat endpoint for LangGraph invoke.

    Args:
        request: The chat request
        req: The FastAPI request object for connection state checking

    Returns:
        The streamed response
    """
    try:
        # Convert Pydantic models to dictionaries and normalize content format
        messages = []
        for msg in request.messages:
            message_dict = {"role": msg.role}

            # Handle both string content and list of content items
            if isinstance(msg.content, str):
                message_dict["content"] = msg.content
            else:
                # For content as a list, convert to the format expected by the workflow
                content_items = []
                for item in msg.content:
                    if item.type == "text" and item.text:
                        content_items.append({"type": "text", "text": item.text})
                    elif item.type == "image" and item.image_url:
                        content_items.append(
                            {"type": "image", "image_url": item.image_url}
                        )

                message_dict["content"] = content_items

            messages.append(message_dict)

        async def event_generator():
            try:
                async for event in run_agent_workflow(
                    messages,
                    request.debug,
                    request.deep_thinking_mode,
                    request.search_before_planning,
                ):
                    # Check if client is still connected
                    if await req.is_disconnected():
                        logger.info("Client disconnected, stopping workflow")
                        break
                    yield {
                        "event": event["event"],
                        "data": json.dumps(event["data"], ensure_ascii=False),
                    }
            except asyncio.CancelledError:
                logger.info("Stream processing cancelled")
                raise

        return EventSourceResponse(
            event_generator(),
            media_type="text/event-stream",
            sep="\n",
        )
    except Exception as e:
        logger.error(f"Error in chat endpoint: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================================
# 应用管理层 API (APPM / AW / ALRE / DISP)
# ======================================================================

class InstallAppRequest(BaseModel):
    name: str = Field(..., description="应用名称")
    task_description: str = Field(..., description="任务总体描述（传递给编排引擎）")
    orchestration_mode: str = Field("adaptive", description="编排模式: adaptive|sequential|magentic")
    agents_required: List[str] = Field(default_factory=list, description="所需 Agent 能力列表")
    constraints: Dict[str, Any] = Field(default_factory=dict, description="约束条件（如 max_rounds, timeout_seconds）")
    skills_md: Optional[str] = Field(None, description="Skills.md 内容：应用专属技能指引")


class InstallAppResponse(BaseModel):
    app_id: str
    name: str
    status: str
    message: str


@app.get("/api/apps/", summary="获取所有应用列表")
async def list_apps():
    """
    DISP: 获取所有应用列表（含所有状态）
    对应接口文档 §4 交互呈现
    """
    try:
        from src.app.display import get_all_app_list
        return {"apps": get_all_app_list()}
    except Exception as e:
        logger.error(f"list_apps error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/apps/running", summary="获取运行中的应用列表")
async def list_running_apps():
    """
    DISP: 获取当前运行中的应用，返回应用接口信息
    对应接口文档 §4 交互呈现
    """
    try:
        from src.app.display import get_running_app_list
        return {"apps": get_running_app_list()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/apps/{app_id}", summary="获取单个应用信息")
async def get_app(app_id: str):
    """
    DISP: 获取应用详情和对外接口信息
    """
    try:
        from src.app.display import get_app_interface
        info = get_app_interface(app_id)
        if not info:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")
        return info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apps/install", response_model=InstallAppResponse, summary="安装应用")
async def install_app(request: InstallAppRequest):
    """
    APPM: 安装应用（存储指导文件 + 可选安装 Agent 镜像）
    对应接口文档 §1 安装/卸载应用
    """
    try:
        from src.app.app_manager import get_app_manager
        from src.app.models import GuidanceFile
        import uuid

        app_id = f"app_{uuid.uuid4().hex[:8]}"
        guidance = GuidanceFile(
            app_id=app_id,
            task_description=request.task_description,
            agents_required=request.agents_required,
            orchestration_mode=request.orchestration_mode,
            constraints=request.constraints,
            skills_content=request.skills_md,
        )
        manager = get_app_manager()
        app_info = manager.install(name=request.name, guidance_file=guidance)

        return InstallAppResponse(
            app_id=app_info.app_id,
            name=app_info.name,
            status=app_info.status,
            message="安装成功",
        )
    except Exception as e:
        logger.error(f"install_app error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apps/{app_id}/start", summary="启动应用")
async def start_app(app_id: str):
    """
    APPM: 启动应用，触发编排层工作流
    对应接口文档 §2 启动应用
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        handle = await manager.start(app_id)

        if handle is None:
            app = manager.get_app(app_id)
            if not app:
                raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")
            raise HTTPException(
                status_code=500,
                detail=f"启动失败: {app.error_message or '未知错误'}",
            )

        return {
            "app_id": app_id,
            "workflow_handle": handle,
            "status": "running",
            "app_interface_url": f"/api/apps/{app_id}/interface",
            "message": "启动成功",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"start_app error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apps/{app_id}/stop", summary="停止应用")
async def stop_app(app_id: str):
    """
    APPM: 停止应用，通知编排层停止工作流
    对应接口文档 §3 停止应用
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        success = await manager.stop(app_id)

        if not success:
            app = manager.get_app(app_id)
            if not app:
                raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")

        return {"app_id": app_id, "status": "stopped", "message": "停止成功"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================================
# 周期调度 API
# ======================================================================

@app.post("/api/apps/{app_id}/schedule/start", summary="启动周期调度")
async def start_schedule(app_id: str):
    """
    启动应用的周期调度。
    需在 GuidanceFile.constraints 中配置 schedule_interval_seconds。
    可选配置 schedule_max_parallel（默认 5）、schedule_max_history（默认 100）。
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        app = manager.get_app(app_id)
        if not app:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")

        success = await manager.start_schedule(app_id)
        if not success:
            raise HTTPException(
                status_code=400,
                detail="启动调度失败：应用可能已在调度中或未配置 schedule_interval_seconds",
            )

        return {
            "app_id": app_id,
            "status": "scheduled",
            "message": "周期调度已启动",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"start_schedule error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/apps/{app_id}/schedule/stop", summary="停止周期调度")
async def stop_schedule(app_id: str):
    """
    停止应用的周期调度。
    活跃的工作流实例继续运行直到完成。
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        app = manager.get_app(app_id)
        if not app:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")

        success = await manager.stop_schedule(app_id)
        if not success:
            raise HTTPException(
                status_code=400,
                detail="停止调度失败：应用当前未处于周期调度状态",
            )

        return {
            "app_id": app_id,
            "status": "stopped",
            "message": "周期调度已停止",
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"stop_schedule error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/apps/{app_id}/schedule/status", summary="获取调度状态")
async def get_schedule_status(app_id: str):
    """
    获取应用的周期调度状态。
    返回间隔、活跃实例数、累计执行次数等信息。
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        app = manager.get_app(app_id)
        if not app:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")

        from src.service.workflow_scheduler import get_workflow_scheduler
        scheduler = get_workflow_scheduler()
        status = scheduler.get_schedule_status(app_id)

        if not status:
            return {
                "app_id": app_id,
                "scheduled": False,
                "message": "应用未处于周期调度状态",
            }

        return {
            "app_id": app_id,
            "scheduled": True,
            **status,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/apps/{app_id}/schedule/history", summary="获取调度执行历史")
async def get_schedule_history(app_id: str, limit: int = 50):
    """
    获取应用的周期调度执行历史。
    返回最近 N 次执行记录，包含 run_id、开始时间、完成时间、状态等。
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        app = manager.get_app(app_id)
        if not app:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")

        from src.service.workflow_scheduler import get_workflow_scheduler
        scheduler = get_workflow_scheduler()
        history = scheduler.get_history(app_id, limit=limit)

        return {
            "app_id": app_id,
            "total": len(history),
            "records": history,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/api/apps/{app_id}", summary="卸载应用")
async def uninstall_app(app_id: str):
    """
    APPM: 卸载应用，删除指导文件和镜像注册
    对应接口文档 §1 安装/卸载应用
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        success = manager.uninstall(app_id)

        if not success:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")

        return {"app_id": app_id, "message": "卸载成功"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class UpdateAppRequest(BaseModel):
    name: Optional[str] = Field(None, description="新应用名称")
    task_description: Optional[str] = Field(None, description="新任务描述")
    skills_md: Optional[str] = Field(None, description="新 Skills.md 内容")
    orchestration_mode: Optional[str] = Field(None, description="新编排模式")
    constraints: Optional[Dict[str, Any]] = Field(None, description="要合并的约束条件")


@app.patch("/api/apps/{app_id}", summary="更新应用配置")
async def update_app(app_id: str, request: UpdateAppRequest):
    """
    APPM: 更新应用配置（无需重新安装）
    支持更新名称、任务描述、Skills.md、编排模式、约束条件。
    对正在运行的应用修改配置不会立即生效，需重启应用。
    """
    try:
        from src.app.app_manager import get_app_manager

        manager = get_app_manager()
        app = manager.update(
            app_id=app_id,
            name=request.name,
            task_description=request.task_description,
            skills_content=request.skills_md,
            orchestration_mode=request.orchestration_mode,
            constraints=request.constraints,
        )
        if app is None:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")

        return {"app_id": app_id, "status": app.status, "message": "更新成功"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"update_app error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================================
# 运行层 API (ALCM / QoS)
# ======================================================================

@app.get("/api/agents/instances", summary="获取 Agent 实例列表")
async def list_agent_instances():
    """
    ALCM: 获取运行层所有 Agent 实例
    """
    try:
        from src.runtime.lifecycle_manager import get_lifecycle_manager

        manager = get_lifecycle_manager()
        instances = manager.list_instances()
        return {"instances": [inst.to_dict() for inst in instances]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/agents/instances/{instance_id}", summary="获取单个 Agent 实例")
async def get_agent_instance(instance_id: str):
    """ALCM: 查询单个 Agent 实例详情"""
    try:
        from src.runtime.lifecycle_manager import get_lifecycle_manager

        manager = get_lifecycle_manager()
        inst = manager.get_instance(instance_id)
        if not inst:
            raise HTTPException(status_code=404, detail=f"实例 {instance_id} 不存在")
        return inst.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/qos/metrics", summary="获取所有 Agent QoS 指标")
async def get_qos_metrics():
    """
    QoS Monitor: 获取所有 Agent 的调用质量统计
    供资源 Agent 读取，用于通信/计算资源调度决策
    """
    try:
        from src.runtime.qos_monitor import get_qos_monitor

        monitor = get_qos_monitor()
        return {
            "summary": monitor.get_summary(),
            "metrics": monitor.get_metrics_dict(),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/qos/metrics/{agent_id}", summary="获取单个 Agent QoS 指标")
async def get_agent_qos_metrics(agent_id: str):
    """QoS Monitor: 查询单个 Agent 的 QoS 指标"""
    try:
        from src.runtime.qos_monitor import get_qos_monitor

        monitor = get_qos_monitor()
        m = monitor.get_metrics(agent_id)
        if not m:
            raise HTTPException(status_code=404, detail=f"Agent {agent_id} 暂无 QoS 数据")
        return m.to_dict()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================================
# 资源层 API (RRDC / ASD)
# ======================================================================

@app.get("/api/resources/", summary="查询可用资源")
async def query_resources(
    min_cpu: float = 0.5,
    min_mem_mb: int = 256,
    node_type: Optional[str] = None,
):
    """
    RRDC: 查询满足条件的节点资源列表
    """
    try:
        from src.service.resource_registry import get_resource_registry

        registry = get_resource_registry()
        nodes = registry.query_available_resources(
            min_cpu=min_cpu,
            min_mem_mb=min_mem_mb,
            node_type=node_type,
        )
        return {
            "summary": registry.get_summary(),
            "nodes": [n.to_dict() for n in nodes],
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class AppQueryRequest(BaseModel):
    query: str = Field(..., description="用户输入的查询内容")


@app.post("/api/apps/{app_id}/interface", summary="向应用发送查询")
async def query_app_interface(app_id: str, request: AppQueryRequest):
    """
    DISP: 应用运行时交互接口
    接收用户输入，使用应用配置的编排模式执行一次工作流，返回结果。
    对应接口文档 §4 交互呈现
    """
    try:
        from src.app.app_logic_engine import get_app_logic_engine

        engine = get_app_logic_engine()
        result = await engine.run_query(app_id=app_id, user_input=request.query)

        # 序列化 result 内部的 LangChain 消息对象，避免 JSON 序列化失败
        if isinstance(result.get("result"), dict):
            raw_state = result["result"]
            messages = raw_state.get("messages", [])
            serialized_msgs = []
            for m in messages:
                if hasattr(m, "type") and hasattr(m, "content"):
                    # LangChain BaseMessage
                    serialized_msgs.append({"type": m.type, "content": m.content})
                elif isinstance(m, dict):
                    serialized_msgs.append(m)
                else:
                    serialized_msgs.append({"type": "unknown", "content": str(m)})
            result["result"] = {
                **{k: v for k, v in raw_state.items() if k != "messages"},
                "messages": serialized_msgs,
            }

        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"query_app_interface error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/apps/{app_id}/interface", summary="获取应用接口说明")
async def get_app_interface_info(app_id: str):
    """
    返回应用的接口说明，包括如何调用 POST /interface 发送查询
    """
    try:
        from src.app.display import get_app_interface
        info = get_app_interface(app_id)
        if not info:
            raise HTTPException(status_code=404, detail=f"应用 {app_id} 不存在")
        info["usage"] = {
            "method": "POST",
            "url": f"/api/apps/{app_id}/interface",
            "body": {"query": "你的问题"},
            "description": "向该应用发送查询，使用应用配置的编排模式执行工作流",
        }
        return info
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/agents/deployments", summary="获取 Agent 部署记录")
async def list_agent_deployments():
    """
    ASD: 获取所有运行中的 Agent 部署记录
    """
    try:
        from src.service.agent_scheduler import get_agent_scheduler

        scheduler = get_agent_scheduler()
        running = scheduler.get_running_agents()
        return {"deployments": [r.to_dict() for r in running]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ======================================================================
# Web UI
# ======================================================================

@app.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def web_ui():
    """应用管理层 Web UI 控制台"""
    from src.api.ui import get_ui_html
    return HTMLResponse(content=get_ui_html())


# ======================================================================
# Demo: 车辆协同感知 — 可视化演示（DEMO_MODE=1 时激活事件总线）
# ======================================================================

class _DemoStartRequest(BaseModel):
    task_description: str


@app.get("/demo", response_class=HTMLResponse, include_in_schema=False)
async def demo_page():
    """车辆协同感知演示可视化页面"""
    from src.api.demo_ui import get_demo_html
    return HTMLResponse(content=get_demo_html())


@app.websocket("/demo/ws")
async def demo_ws(ws: WebSocket):
    """
    Demo WebSocket 端点 — 订阅 DemoEventBus，实时推送结构化事件到前端。
    每个浏览器连接对应一个独立的订阅队列（fan-out 广播）。
    """
    from src.service.demo_bus import get_demo_bus
    await ws.accept()
    try:
        async for event in get_demo_bus().subscribe():
            await ws.send_json(event)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"[Demo WS] 连接关闭: {e}")


@app.post("/demo/start", summary="启动协同感知演示工作流")
async def demo_start(request: _DemoStartRequest):
    """
    启动 app_vehicle_demo 工作流（后台异步执行）。
    Demo 事件通过 DemoEventBus 推送到 /demo/ws WebSocket 连接。
    """
    import uuid
    wf_id = f"demo_{uuid.uuid4().hex[:8]}"
    asyncio.create_task(_run_demo_workflow(request.task_description, wf_id))
    from src.service.demo_bus import get_demo_bus
    get_demo_bus().publish("log", {"level": "info", "message": f"工作流 {wf_id} 开始执行"})
    return {"status": "accepted", "workflow_id": wf_id}


async def _run_demo_workflow(task_desc: str, wf_id: str) -> None:
    """后台任务：运行 app_vehicle_demo 工作流并在完成时推送 demo:task_complete 事件"""
    from src.service.demo_bus import get_demo_bus
    bus = get_demo_bus()
    try:
        from src.app.app_logic_engine import get_app_logic_engine
        engine = get_app_logic_engine()
        result = await engine.run_query(app_id="app_vehicle_demo", user_input=task_desc)
        # 提取最终消息内容
        raw_state = result.get("result", {}) or {}
        messages = raw_state.get("messages", []) if isinstance(raw_state, dict) else []
        final_result = ""
        for msg in reversed(messages):
            content = (
                msg.content if hasattr(msg, "content")
                else msg.get("content") if isinstance(msg, dict)
                else None
            )
            if content and len(str(content)) > 50:
                final_result = str(content)[:1500]
                break
        bus.publish("demo:task_complete", {"result": final_result})
    except Exception as e:
        logger.error(f"[Demo] 工作流执行失败: {e}")
        bus.publish("log", {"level": "error", "message": f"工作流执行失败: {e}"})
        bus.publish("demo:task_complete", {"result": f"执行出错: {e}"})


@app.post("/demo/toggle-vehicleB-failure", summary="切换VehicleB故障模拟")
async def demo_toggle_vehicleB():
    """
    切换 VehicleB 感知节点故障模拟状态：
    - 开启时：下次执行 perception_vehicleB 任务时注入失败，触发 §2.3 failover 切换到 VehicleC
    - 关闭时：恢复正常执行
    """
    from src.service.demo_bus import set_vehicleB_failed, is_vehicleB_failed, get_demo_bus
    new_state = not is_vehicleB_failed()
    set_vehicleB_failed(new_state)
    get_demo_bus().publish("demo:status", {"vehicleB_failed": new_state})
    return {"vehicleB_failed": new_state, "message": f"VehicleB 故障模拟: {'开启' if new_state else '关闭'}"}

