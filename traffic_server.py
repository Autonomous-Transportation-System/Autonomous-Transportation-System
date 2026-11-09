"""Standalone traffic/A2A server without conversational graph or LLM imports.

Run: PYTHONPATH=. python traffic_server.py [port]
"""

import asyncio
import os
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import ValidationError

from src.api.traffic import TASKS, TrafficRequest, dispatch, get_runtime, router
from src.api.traffic_platform import (
    router as platform_router, shutdown_platform, get_platform_bridge,
)
from src.protocols.a2a_protocol import A2AMessage, A2ATaskRequest, A2ATaskResponse


@asynccontextmanager
async def lifespan(app):
    try:
        yield
    finally:
        await shutdown_platform()
        await get_runtime().shutdown()


app = FastAPI(title="Traffic API", version="0.1.0", lifespan=lifespan)
app.include_router(router)
app.include_router(platform_router)
app.post("/orchestration/dispatch", tags=["traffic-a2a"])(dispatch)


@app.get('/', include_in_schema=False)
async def index():
    return RedirectResponse('/traffic/platform/ui')


@app.get('/health', tags=['health'])
async def health():
    return {'status': 'ok', 'traffic_platform_url': '/traffic/platform/ui'}


@app.post("/a2a/execute", tags=["traffic-a2a"])
async def execute_a2a(message: A2AMessage):
    """Native A2A envelope; timeout cancels the waiter, never its lock owner."""
    started = time.monotonic()
    task_id = str(message.payload.get("task_id", "unknown"))
    platform_request = None

    def response(status, result=None, error=None, workflow_id=None):
        task_response = A2ATaskResponse(
            task_id=task_id, status=status, result=result, error_message=error,
            metadata={"execution_time": time.monotonic() - started,
                      "workflow_id": workflow_id})
        return A2AMessage(
            sender_id=message.receiver_id, receiver_id=message.sender_id,
            message_type="response" if status == "success" else "error",
            correlation_id=message.message_id, payload=task_response.model_dump())

    try:
        if message.message_type != "request":
            return response("error", error="message_type must be request")
        task = A2ATaskRequest.model_validate(message.payload)
        task_id = task.task_id
        if task.task_type not in ("traffic", "compute", "traffic.predict") + TASKS:
            return response("error", error="Unsupported traffic task_type: " + task.task_type)
        if task.timeout <= 0:
            return response("error", error="timeout must be positive")
        if task.require_stream:
            return response("error", error="Traffic A2A execution returns a complete response")
        context = dict(task.context)
        if "template" in context:
            from src.service.traffic_management_bridge import TrafficWorkflowRequest
            platform_request = TrafficWorkflowRequest.model_validate(context).model_dump(exclude_none=True)
        else:
            if task.task_type in TASKS:
                context["task"] = task.task_type
            elif task.task_type in ("compute", "traffic.predict") and "task" not in context:
                return response("error", error="compute requires context.task or a platform template")
            traffic_request = TrafficRequest.model_validate(context)
    except ValidationError as exc:
        return response("error", error="Invalid traffic A2A payload/context: " + str(exc))

    if platform_request is not None:
        workflow_id = None
        bridge = get_platform_bridge()
        try:
            await asyncio.wait_for(bridge.initialize(), timeout=task.timeout)
            try:
                target_instance = bridge.get_instance(message.receiver_id)
            except KeyError:
                target_instance = None
            if target_instance is not None:
                allowed = platform_request.get("allowed_instances")
                if allowed is not None and allowed != [message.receiver_id]:
                    return response("error", error="A2A receiver conflicts with allowed_instances")
                platform_request["allowed_instances"] = [message.receiver_id]
            run = await bridge.submit_workflow(platform_request)
            workflow_id = run["workflow_id"]
            remaining = task.timeout - (time.monotonic() - started)
            if remaining <= 0:
                raise asyncio.TimeoutError
            result = await asyncio.wait_for(bridge.runtime.wait(workflow_id), timeout=remaining)
        except asyncio.TimeoutError:
            if workflow_id is not None:
                bridge.runtime.cancel(workflow_id, reason="A2A caller timeout")
            return response("timeout", error="Execution cancellation requested; resources remain until actual completion",
                            workflow_id=workflow_id)
        except Exception as exc:
            return response("error", error=str(exc), workflow_id=workflow_id)
        status = {"completed": "success", "success": "success", "cancelled": "cancelled"}.get(result["status"], "error")
        error = None if status == "success" else (
            result.get("error") or f"Traffic workflow ended with status: {result['status']}")
        return response(status, result=result, error=error, workflow_id=workflow_id)

    runtime = get_runtime()
    context = {"a2a_message": message.model_dump(),
               "traffic_request": traffic_request.model_dump()}
    try:
        result = await asyncio.wait_for(
            runtime.run(traffic_request, context=context), timeout=task.timeout)
    except asyncio.TimeoutError:
        workflow_id = next((record.workflow_id for record in runtime.records.values()
                            if record.context == context), None)
        return response("timeout", error="Traffic request timed out; execution owner is draining workers",
                        workflow_id=workflow_id)
    except HTTPException as exc:
        return response("error", error=str(exc.detail))
    status = {"success": "success", "cancelled": "cancelled"}.get(result["status"], "error")
    return response(status, result=result, error=result.get("error"),
                    workflow_id=result["workflow_id"])


if __name__ == "__main__":
    import sys
    import uvicorn

    port = int(sys.argv[1]) if len(sys.argv) > 1 else 18763
    if not os.getenv('TRAFFIC_SERVICE_URL'):
        get_platform_bridge().service_url = f'http://127.0.0.1:{port}'
    uvicorn.run(app, host="127.0.0.1", port=port)
