"""Factory for the traffic platform router; no runtime or model is constructed."""

from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Body, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.service.traffic_management_bridge import (
    ManagementConflict, TrafficWorkflowRequest,
)


class CreateAppRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    name: str = Field(min_length=1, max_length=200)
    request: TrafficWorkflowRequest

    @field_validator("name")
    @classmethod
    def nonempty_name(cls, value):
        if not value.strip():
            raise ValueError("name must not be blank")
        return value.strip()


class CreateWorkflowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    request: TrafficWorkflowRequest
    appid: Optional[str] = Field(default=None, min_length=1)

    @field_validator("appid")
    @classmethod
    def nonempty_appid(cls, value):
        if value is not None and not value.strip():
            raise ValueError("appid must not be blank")
        return value


class EmptyCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class ExecuteInstanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    request: TrafficWorkflowRequest


class CreateTemplateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    template_id: str = Field(min_length=1)
    definition: dict

    @field_validator("template_id")
    @classmethod
    def nonblank_template_id(cls, value):
        if not value.strip():
            raise ValueError("template_id must not be blank")
        return value.strip()


class InstanceHealthRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    healthy: bool
    reason: Optional[str] = Field(default=None, min_length=1, max_length=500)

    @field_validator("reason")
    @classmethod
    def nonblank_reason(cls, value):
        if value is not None and not value.strip():
            raise ValueError("reason must not be blank")
        return value.strip() if value is not None else None


def _error(exc):
    if isinstance(exc, KeyError):
        return HTTPException(404, "Application, workflow or instance not found")
    if isinstance(exc, ManagementConflict):
        return HTTPException(409, str(exc))
    return HTTPException(422, str(exc))


async def _ready(bridge):
    try:
        await bridge.initialize()
    except Exception as exc:
        raise HTTPException(503, f"Traffic runtime is unavailable: {exc}") from exc


def create_management_router(bridge):
    """Bind one bridge to /traffic/platform, leaving legacy traffic routes intact."""
    router = APIRouter(prefix="/traffic/platform", tags=["traffic-management"])

    @router.get("/overview")
    async def overview():
        await _ready(bridge)
        return bridge.snapshot()

    @router.get("/catalog")
    async def catalog():
        await _ready(bridge)
        return bridge.catalog_snapshot()

    @router.get("/templates")
    async def templates():
        await _ready(bridge)
        return bridge.templates()

    @router.post("/templates", status_code=201)
    async def register_template(body: CreateTemplateRequest):
        await _ready(bridge)
        try:
            return await bridge.register_template(body.template_id, body.definition)
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.get("/apps")
    async def apps():
        return bridge.list_apps()

    @router.get("/apps/{app_id}")
    async def app(app_id: str):
        try:
            return bridge.get_app(app_id)
        except KeyError as exc:
            raise _error(exc) from exc

    @router.get("/workflows/{workflow_id}")
    async def workflow(workflow_id: str, wait: bool = Query(default=False)):
        try:
            if wait:
                return await bridge.wait_workflow(workflow_id)
            return bridge.get_workflow(workflow_id)
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.get("/instances/{instance_id}")
    async def instance(instance_id: str):
        await _ready(bridge)
        try:
            return bridge.get_instance(instance_id)
        except KeyError as exc:
            raise _error(exc) from exc

    @router.get("/resources")
    async def resources():
        await _ready(bridge)
        return bridge.resources_snapshot()

    @router.post("/instances/{instance_id}/execute", status_code=202)
    async def execute_instance(instance_id: str, body: ExecuteInstanceRequest):
        await _ready(bridge)
        try:
            return await bridge.execute_instance(instance_id, body.request.model_dump(exclude_none=True))
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.post("/instances/{instance_id}/health")
    async def instance_health(instance_id: str, body: InstanceHealthRequest):
        await _ready(bridge)
        try:
            return await bridge.set_instance_health(instance_id, body.healthy, body.reason)
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.get("/qos")
    async def qos():
        return bridge.qos.get_metrics_dict()

    @router.post("/apps", status_code=201)
    async def create_app(body: CreateAppRequest):
        try:
            return bridge.create(body.name, body.request.model_dump(exclude_none=True))
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.post("/apps/{app_id}/run", status_code=202)
    async def run_app(app_id: str, body: Optional[EmptyCommand] = Body(default=None)):
        try:
            bridge.get_app(app_id)
            await _ready(bridge)
            return await bridge.start(app_id)
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.post("/apps/{app_id}/stop")
    async def stop_app(app_id: str, body: Optional[EmptyCommand] = Body(default=None)):
        try:
            result = await bridge.stop(app_id)
            return JSONResponse(result, status_code=200 if result["done"] else 202)
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.delete("/apps/{app_id}")
    async def delete_app(app_id: str):
        try:
            result = await bridge.delete(app_id)
            return JSONResponse(result, status_code=200 if result["deleted"] else 202)
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    @router.post("/workflows", status_code=202)
    async def create_workflow(body: CreateWorkflowRequest):
        try:
            await _ready(bridge)
            return await bridge.submit_workflow(body.request.model_dump(exclude_none=True), body.appid)
        except (KeyError, ManagementConflict, ValueError) as exc:
            raise _error(exc) from exc

    return router
