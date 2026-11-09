"""Standalone navigation and structured A2A routing/cancellation boundaries."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

import httpx
import pytest


class Runtime:
    def __init__(self, blocked=False):
        self.blocked = blocked
        self.cancelled = []

    async def wait(self, workflow_id):
        if self.blocked:
            await asyncio.Event().wait()
        return {'workflow_id': workflow_id, 'status': 'completed', 'done': True,
                'predictions': {'flow': {'shape': [1, 1, 6, 6, 2]}}}

    def cancel(self, workflow_id, reason=None):
        self.cancelled.append((workflow_id, reason))


class Bridge:
    def __init__(self, *, blocked=False, initialize_blocked=False):
        self.runtime = Runtime(blocked)
        self.initialize_blocked = initialize_blocked
        self.requests = []

    async def initialize(self):
        if self.initialize_blocked:
            await asyncio.Event().wait()

    def get_instance(self, instance_id):
        if instance_id != 'b1-2':
            raise KeyError(instance_id)
        return {'instance_id': instance_id}

    async def submit_workflow(self, request):
        self.requests.append(request)
        return {'app_id': 'app-test', 'workflow_id': 'joint-test'}


def message(context=None, receiver='traffic'):
    return {'sender_id': 'test-client', 'receiver_id': receiver,
            'message_type': 'request', 'payload': {
                'task_id': 'a2a-test', 'task_type': 'traffic.predict',
                'task_description': 'traffic workflow', 'timeout': 1,
                'context': context if context is not None else {
                    'template': 'flow', 'target_frames': [1264]}}}


@pytest.mark.asyncio
async def test_standalone_root_and_health_are_accessible():
    from traffic_server import app
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        root = await client.get('/')
        assert root.status_code == 307
        assert root.headers['location'] == '/traffic/platform/ui'
        assert (await client.get('/health')).json()['status'] == 'ok'


@pytest.mark.asyncio
async def test_structured_a2a_routes_to_platform_and_preserves_instance_binding(monkeypatch):
    import traffic_server
    bridge = Bridge()
    monkeypatch.setattr(traffic_server, 'get_platform_bridge', lambda: bridge)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=traffic_server.app), base_url='http://test') as client:
        data = (await client.post('/a2a/execute', json=message(receiver='b1-2'))).json()['payload']
    assert data['status'] == 'success'
    assert data['metadata']['workflow_id'] == 'joint-test'
    assert bridge.requests[0]['allowed_instances'] == ['b1-2']
    assert bridge.requests[0]['target_frames'] == [1264]


@pytest.mark.asyncio
async def test_structured_a2a_failed_workflow_has_an_error_reason(monkeypatch):
    import traffic_server
    bridge = Bridge()

    async def failed(workflow_id):
        return {'workflow_id': workflow_id, 'status': 'failed', 'done': True}

    bridge.runtime.wait = failed
    monkeypatch.setattr(traffic_server, 'get_platform_bridge', lambda: bridge)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=traffic_server.app), base_url='http://test') as client:
        data = (await client.post('/a2a/execute', json=message())).json()['payload']
    assert data['status'] == 'error'
    assert 'failed' in data['error_message']


@pytest.mark.asyncio
async def test_structured_a2a_with_real_b1_runtime_translates_completed(monkeypatch):
    import traffic_server
    from src.service.traffic_joint_runtime import JointTrafficRuntime
    repo = Path(__file__).resolve().parents[2]
    root = Path(os.getenv('TRAFFIC_EXPERIMENT_ROOT', str(repo / 'data/traffic_artifacts')))
    if not (root / 'results/B1_seed42_L3/best.pt').is_file():
        pytest.skip('Real B1 artifact is required for native A2A integration')
    runtime = JointTrafficRuntime(experiment_root=root, instance_count=1)

    class RealBridge:
        async def initialize(self):
            await runtime.initialize()

        def get_instance(self, instance_id):
            return runtime.instances[instance_id]

        async def submit_workflow(self, request):
            return {'workflow_id': runtime.submit(request)}

    bridge = RealBridge()
    bridge.runtime = runtime
    monkeypatch.setattr(traffic_server, 'get_platform_bridge', lambda: bridge)
    body = message({'template': 'analysis', 'target_frames': [1264], 'enable_reuse': False}, 'b1-1')
    body['payload']['timeout'] = 120
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=traffic_server.app), base_url='http://test') as client:
            envelope = (await client.post('/a2a/execute', json=body)).json()
        assert envelope['message_type'] == 'response'
        assert envelope['payload']['status'] == 'success'
        result = envelope['payload']['result']
        assert result['status'] == 'completed' and set(result['predictions']) == {'state', 'flow'}
        assert runtime.model_forward_count == 1
        assert not runtime.snapshot()['reservations']
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_a2a_rejects_receiver_and_candidate_conflict(monkeypatch):
    import traffic_server
    bridge = Bridge()
    monkeypatch.setattr(traffic_server, 'get_platform_bridge', lambda: bridge)
    request = message({'template': 'flow', 'target_frames': [1264], 'allowed_instances': ['b1-1']}, 'b1-2')
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=traffic_server.app), base_url='http://test') as client:
        data = (await client.post('/a2a/execute', json=request)).json()['payload']
    assert data['status'] == 'error' and not bridge.requests


@pytest.mark.asyncio
async def test_platform_a2a_timeout_requests_cancellation_of_identified_workflow(monkeypatch):
    import traffic_server
    bridge = Bridge(blocked=True)
    monkeypatch.setattr(traffic_server, 'get_platform_bridge', lambda: bridge)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=traffic_server.app), base_url='http://test') as client:
        data = (await client.post('/a2a/execute', json=message())).json()['payload']
    assert data['status'] == 'timeout'
    assert bridge.runtime.cancelled == [('joint-test', 'A2A caller timeout')]


@pytest.mark.asyncio
async def test_a2a_initialization_uses_same_request_timeout(monkeypatch):
    import traffic_server
    bridge = Bridge(initialize_blocked=True)
    monkeypatch.setattr(traffic_server, 'get_platform_bridge', lambda: bridge)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=traffic_server.app), base_url='http://test') as client:
        data = (await client.post('/a2a/execute', json=message())).json()['payload']
    assert data['status'] == 'timeout' and not bridge.requests


@pytest.mark.asyncio
async def test_invalid_frame_types_are_rejected_before_initialization(monkeypatch):
    import traffic_server
    bridge = Bridge()
    monkeypatch.setattr(traffic_server, 'get_platform_bridge', lambda: bridge)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=traffic_server.app), base_url='http://test') as client:
        data = (await client.post('/a2a/execute', json=message({'template': 'flow', 'target_frames': [True]}))).json()['payload']
    assert data['status'] == 'error' and not bridge.requests


def test_standalone_import_keeps_model_and_llm_loading_lazy():
    repo = Path(__file__).resolve().parents[2]
    script = 'import traffic_server,sys,json; print(json.dumps({name:name in sys.modules for name in ["torch","src.agents.llm","langchain_openai"]}))'
    result = subprocess.run([sys.executable, '-c', script], cwd=repo,
                            env=dict(os.environ, PYTHONPATH=str(repo), PYTHONDONTWRITEBYTECODE='1'),
                            capture_output=True, text=True, timeout=20, check=True)
    assert not any(json.loads(result.stdout.strip().splitlines()[-1]).values())
