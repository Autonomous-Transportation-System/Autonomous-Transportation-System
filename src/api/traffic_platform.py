"""Shared lazy platform router for standalone and conversational entry points."""
from __future__ import annotations

import os
from fastapi.responses import HTMLResponse

from src.api.traffic_management import create_management_router
from src.service.traffic_joint_runtime import JointTrafficRuntime
from src.service.traffic_management_bridge import TrafficManagementBridge

_bridge = None


def get_platform_bridge():
    global _bridge
    if _bridge is None:
        runtime = JointTrafficRuntime(
            cpu_budget=float(os.getenv('TRAFFIC_CPU_BUDGET', '4')),
            memory_budget=float(os.getenv('TRAFFIC_MEMORY_BUDGET_MB', '2048')),
            max_concurrency=int(os.getenv('TRAFFIC_MAX_CONCURRENCY', '4')),
            instance_count=int(os.getenv('TRAFFIC_INSTANCE_COUNT', '2')),
            policy=os.getenv('TRAFFIC_POLICY', 'joint'),
        )
        _bridge = TrafficManagementBridge(
            runtime, service_url=os.getenv('TRAFFIC_SERVICE_URL', 'http://127.0.0.1:18763'))
    return _bridge


router = create_management_router(get_platform_bridge())


@router.get('/ui', response_class=HTMLResponse, include_in_schema=False)
async def platform_ui():
    from src.api.traffic_platform_ui import get_traffic_platform_html
    return get_traffic_platform_html()


async def shutdown_platform():
    if _bridge is not None:
        await _bridge.shutdown()
