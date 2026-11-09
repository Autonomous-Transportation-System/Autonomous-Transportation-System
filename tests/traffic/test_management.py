"""Real management components with an explicitly controlled public runtime fake."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import threading
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from src.api.traffic_management import create_management_router
from src.app.app_logic_engine import AppLogicEngine
from src.app.app_manager import AppManager
from src.app.models import GuidanceFile
from src.runtime.qos_monitor import QoSMonitor
from src.service.agent_registry import AgentRegistryClient
from src.service.traffic_management_bridge import (
    ManagementConflict, TrafficManagementBridge, TrafficWorkflowRequest,
)


REQUEST = {"template": "analysis", "target_frames": [1251]}


class FakeCatalog:
    def __init__(self):
        self.data = {
            "implementations": {"B1": {"implementation_id": "B1", "version": "test",
                                          "capabilities": {"traffic.predict": {}}}},
            "instances": {f"b1-{i}": {"instance_id": f"b1-{i}", "implementation_id": "B1",
                                      "endpoint": f"local://b1-{i}", "healthy": True,
                                      "max_concurrency": 1, "base_memory_mb": 4}
                          for i in (1, 2)},
        }

    def snapshot(self):
        return deepcopy(self.data)


class FakeRuntime:
    """Cancellation requests retain reservations until finish is explicitly called."""

    def __init__(self, *, nested=False):
        self.catalog = FakeCatalog()
        self.instances = self.catalog.snapshot()["instances"]
        self.records = {}
        self.waiters = {}
        self.cancelled = []
        self.initialize_calls = 0
        self.initialized = False
        self.closed = False
        self._shutdown_entered = None
        self._initialize_entered = None
        self.initialize_gate = None
        self.nested = nested
        self.submit_error = None
        self.health_calls = []
        self.health_threads = []
        self._health_changed = None
        self.wait_calls = []
        self._wait_entered = None
        self.snapshot_calls = 0

    @property
    def health_changed(self):
        if self._health_changed is None:
            self._health_changed = asyncio.Event()
        return self._health_changed

    @property
    def wait_entered(self):
        if self._wait_entered is None:
            self._wait_entered = asyncio.Event()
        return self._wait_entered

    def set_instance_health(self, instance_id, healthy, reason=None):
        self.health_calls.append((instance_id, healthy, reason))
        self.health_threads.append(threading.get_ident())
        self.instances[instance_id]["healthy"] = healthy
        self.catalog.data["instances"][instance_id]["healthy"] = healthy
        self.health_changed.set()

    @property
    def shutdown_entered(self):
        if self._shutdown_entered is None:
            self._shutdown_entered = asyncio.Event()
        return self._shutdown_entered

    @property
    def initialize_entered(self):
        if self._initialize_entered is None:
            self._initialize_entered = asyncio.Event()
        return self._initialize_entered

    async def initialize(self):
        self.initialize_calls += 1
        self.initialize_entered.set()
        if self.initialize_gate is not None:
            await self.initialize_gate.wait()
        self.initialized = True

    def submit(self, request, app_id=""):
        assert self.initialized
        if self.submit_error:
            raise self.submit_error
        workflow_id = f"wf_{len(self.records) + 1}"
        instance = f"b1-{1 + len(self.records) % 2}"
        self.records[workflow_id] = {
            "workflow_id": workflow_id, "app_id": app_id, "request": deepcopy(request),
            "status": "running", "done": False, "submitted_at": 10.0, "deadline": 130.0,
            "finished_at": None,
            "tasks": {f"{workflow_id}:predict": {"workflow_id": workflow_id, "node_id": "predict",
                       "instance_id": instance, "attempt": 1, "started_at": 10.0,
                       "finished_at": None, "call_status": "running", "quality_status": "not_evaluated",
                       "schedule_status": "running"}},
        }
        self.waiters[workflow_id] = asyncio.get_running_loop().create_future()
        return workflow_id

    async def wait(self, workflow_id):
        self.wait_calls.append(workflow_id)
        self.wait_entered.set()
        result = await asyncio.shield(self.waiters[workflow_id])
        if self.nested:
            # Joint runtime wait returns results, while snapshot owns done/ledger.
            return {"workflow_id": workflow_id, "status": self.records[workflow_id]["status"], "predictions": {"flow": [1]}}
        return result

    def cancel(self, workflow_id, reason=None):
        self.cancelled.append((workflow_id, reason))
        if not self.records[workflow_id]["done"]:
            self.records[workflow_id]["status"] = "cancelling"

    def finish(self, workflow_id, status="success"):
        record = self.records[workflow_id]
        record.update(status=status, done=True, finished_at=10.25)
        for task in record["tasks"].values():
            task.update(finished_at=10.25, call_status="success" if status in {"success", "completed"} else status,
                        quality_status="valid" if status in {"success", "completed"} else "not_evaluated",
                        schedule_status="completed" if status in {"success", "completed"} else "failed")
        if not self.waiters[workflow_id].done():
            self.waiters[workflow_id].set_result(self.snapshot(workflow_id))

    def snapshot(self, workflow_id=None):
        self.snapshot_calls += 1
        records = self.records if workflow_id is None else {workflow_id: self.records[workflow_id]}
        active = sum(not r["done"] for r in self.records.values())
        scheduler = {
            "workflows": {key: {k: deepcopy(v) for k, v in r.items() if k != "tasks"}
                          for key, r in records.items()},
            "tasks": {key: deepcopy(task) for r in records.values() for key, task in r["tasks"].items()},
            "resources": {"cpu_used": active, "memory_used_mb": active * 8, "concurrency": active},
            "reservations": {key: {"workflow_id": key} for key, r in self.records.items() if not r["done"]},
            "resource_events": [], "peaks": {"concurrency": len(self.records)},
        }
        if self.nested:
            return {"scheduler": scheduler, "resources": {"measurement": {"rss_mb": 12}},
                    "active_futures": active, "instances": self.catalog.snapshot()["instances"]}
        return scheduler

    async def shutdown(self):
        self.shutdown_entered.set()
        if self.waiters:
            await asyncio.gather(*(asyncio.shield(f) for f in self.waiters.values()))
        self.closed = True


@pytest.fixture
def components(tmp_path):
    engine = AppLogicEngine()
    manager = AppManager(tmp_path / "apps.json", execution_backend=engine)
    config = tmp_path / "agents.json"
    config.write_text('{"agents": []}')
    registry = AgentRegistryClient(config_file=str(config))
    runtime = FakeRuntime()
    qos = QoSMonitor()
    bridge = TrafficManagementBridge(runtime, manager, registry, qos,
                                     service_url="http://traffic.test:8008")
    return bridge, runtime, manager, engine, registry, qos


async def settle(bridge, workflow_id):
    await bridge._owners[workflow_id]


def test_management_import_is_lightweight():
    root = Path(__file__).resolve().parents[2]
    code = """
import json, sys
from src.api.traffic_management import create_management_router
from src.service.agent_registry import AgentRegistryClient
assert not [name for name in sys.modules if name == 'torch' or name.startswith(('langchain', 'langgraph'))]
"""
    subprocess.run([sys.executable, "-c", code], cwd=root,
                   env=dict(os.environ, PYTHONPATH=str(root), PYTHONDONTWRITEBYTECODE="1"),
                   capture_output=True, text=True, check=True)


@pytest.mark.parametrize("template,outputs", [("flow", ["flow"]), ("analysis", ["state", "flow"]),
                                                ("od_hotspots", ["od"])])
def test_create_persists_existing_guidance(components, tmp_path, template, outputs):
    bridge, runtime, manager, engine, _, _ = components
    app = bridge.create(" Traffic app ", {**REQUEST, "template": template})
    app_id = app["app_id"]
    assert app["name"] == "Traffic app"
    assert app["status"] == "idle" and app["last_run_status"] is None
    assert engine.get_guidance(app_id).metadata["traffic_request"]["template"] == template
    restored = AppManager(tmp_path / "apps.json", execution_backend=AppLogicEngine())
    assert restored.get_app(app_id).to_dict() == manager.get_app(app_id).to_dict()
    assert next(t for t in bridge.templates() if t["template"] == template)["outputs"] == outputs
    assert runtime.initialize_calls == 0


@pytest.mark.asyncio
async def test_success_persists_stopped_and_separate_outcome(components, tmp_path):
    bridge, runtime, manager, _, registry, qos = components
    app_id = bridge.create("Analysis", REQUEST)["app_id"]
    started = await bridge.start(app_id)
    workflow_id = started["workflow_id"]
    assert runtime.records[workflow_id]["app_id"] == app_id
    assert manager.get_app(app_id).workflow_handle == workflow_id
    assert json.loads((tmp_path / "apps.json").read_text())[app_id]["status"] == "running"
    assert qos.get_metrics_dict() == []
    assert {a["id"] for a in registry.query_agents()} == {"b1-1", "b1-2"}
    runtime.finish(workflow_id)
    await settle(bridge, workflow_id)
    app = bridge.get_app(app_id)
    assert app["status"] == "stopped" and app["last_run_status"] == "success"
    assert app["workflow_handle"] is None and app["workflow_id"] == workflow_id
    restored = AppManager(tmp_path / "apps.json", execution_backend=AppLogicEngine())
    assert restored.get_app(app_id).guidance_file.metadata["traffic_run"]["last_run_status"] == "success"
    assert qos.get_metrics("b1-1").total_calls == 1
    assert qos.get_metrics("b1-1").avg_latency_ms == 250
    bridge.get_workflow(workflow_id)
    bridge.snapshot()
    assert qos.get_metrics("b1-1").total_calls == 1
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_rerun_and_stale_handle_guard(components, tmp_path):
    bridge, runtime, manager, _, _, _ = components
    app_id = bridge.create("Rerun", REQUEST)["app_id"]
    first = (await bridge.start(app_id))["workflow_id"]
    # Simulate a delayed observer belonging to an older handle.
    manager.update_state(app_id, "stopped", expected_handle=first, workflow_handle=None)
    second = (await bridge.start(app_id))["workflow_id"]
    runtime.finish(first, "failed")
    await settle(bridge, first)
    assert bridge.get_app(app_id)["workflow_handle"] == second
    assert bridge.get_app(app_id)["status"] == "running"
    disk_before = (tmp_path / "apps.json").read_bytes()
    assert manager.update_state(app_id, "stopped", expected_handle=first, workflow_handle=None) is False
    assert (tmp_path / "apps.json").read_bytes() == disk_before
    runtime.finish(second)
    await settle(bridge, second)
    assert bridge.get_app(app_id)["last_run_status"] == "success"
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_stop_cancels_only_one_and_preserves_resources(components):
    bridge, runtime, manager, _, _, qos = components
    ids = [bridge.create(f"App {i}", REQUEST)["app_id"] for i in range(2)]
    runs = [await bridge.start(app_id) for app_id in ids]
    first, second = [r["workflow_id"] for r in runs]
    stopped = await bridge.stop(ids[0])
    assert stopped["status"] == "cancelling" and stopped["done"] is False
    assert manager.get_app(ids[0]).status == "stopping"
    assert manager.get_app(ids[0]).workflow_handle == first
    assert [c[0] for c in runtime.cancelled] == [first]
    assert bridge.resources_snapshot()["resources"]["concurrency"] == 2
    assert qos.get_metrics_dict() == []
    with pytest.raises(ManagementConflict):
        await bridge.start(ids[0])
    runtime.finish(first, "cancelled")
    await settle(bridge, first)
    assert bridge.get_app(ids[0])["last_run_status"] == "cancelled"
    assert bridge.get_app(ids[1])["status"] == "running"
    assert bridge.resources_snapshot()["resources"]["concurrency"] == 1
    assert qos.get_metrics("b1-1").failure_count == 1
    runtime.finish(second)
    await settle(bridge, second)
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_delete_defers_until_real_execution_finishes(components):
    bridge, runtime, manager, engine, _, _ = components
    app_id = bridge.create("Delete", REQUEST)["app_id"]
    workflow_id = (await bridge.start(app_id))["workflow_id"]
    assert (await bridge.delete(app_id))["deleted"] is False
    assert manager.get_app(app_id) is not None and engine.get_guidance(app_id) is not None
    runtime.finish(workflow_id, "cancelled")
    await settle(bridge, workflow_id)
    assert (await bridge.delete(app_id))["deleted"] is True
    assert manager.get_app(app_id) is None and engine.get_guidance(app_id) is None
    assert bridge.get_workflow(workflow_id)["management"]["status"] == "cancelled"
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_shutdown_waits_for_real_termination_and_survives_waiter_cancel(components):
    bridge, runtime, manager, _, registry, qos = components
    app_id = bridge.create("Shutdown", REQUEST)["app_id"]
    workflow_id = (await bridge.start(app_id))["workflow_id"]
    waiter = asyncio.create_task(bridge.shutdown())
    await runtime.shutdown_entered.wait()
    assert not waiter.done() and not bridge.closed and not runtime.closed
    assert manager.get_app(app_id).status == "stopping"
    assert bridge.resources_snapshot()["reservations"]
    assert qos.get_metrics_dict() == []
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    with pytest.raises(ManagementConflict):
        bridge.create("Late", REQUEST)
    runtime.finish(workflow_id, "cancelled")
    await bridge.shutdown()
    assert bridge.closed and runtime.closed
    assert bridge.get_app(app_id)["status"] == "stopped"
    assert registry.query_agents() == []


@pytest.mark.asyncio
async def test_initialize_and_shutdown_do_not_deadlock(components):
    bridge, runtime, _, _, _, _ = components
    runtime.initialize_gate = asyncio.Event()
    app_id = bridge.create("Race", REQUEST)["app_id"]
    starting = asyncio.create_task(bridge.start(app_id))
    await runtime.initialize_entered.wait()
    stopping = asyncio.create_task(bridge.shutdown())
    await asyncio.sleep(0)
    runtime.initialize_gate.set()
    with pytest.raises(ManagementConflict):
        await asyncio.wait_for(starting, 1)
    await asyncio.wait_for(stopping, 1)
    assert bridge.closed and runtime.records == {}


@pytest.mark.asyncio
async def test_concurrent_start_is_idempotent(components):
    bridge, runtime, _, _, _, _ = components
    app_id = bridge.create("Duplicate", REQUEST)["app_id"]
    starts = await asyncio.gather(bridge.start(app_id), bridge.start(app_id))
    assert starts[0] == starts[1]
    assert len(runtime.records) == 1 and runtime.initialize_calls == 1
    runtime.finish(starts[0]["workflow_id"])
    await settle(bridge, starts[0]["workflow_id"])
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_nested_runtime_snapshot_results_and_retry_qos(components):
    bridge, runtime, _, _, _, qos = components
    runtime.nested = True
    app_id = bridge.create("Nested", REQUEST)["app_id"]
    workflow_id = (await bridge.start(app_id))["workflow_id"]
    task = next(iter(runtime.records[workflow_id]["tasks"].values()))
    task["attempts"] = [
        {"instance_id": "b1-1", "attempt": 1, "started_at": 10, "finished_at": 10.1,
         "call_status": "error", "quality_status": "not_evaluated"},
        {"instance_id": "b1-2", "attempt": 2, "started_at": 10.1, "finished_at": 10.25,
         "call_status": "success", "quality_status": "valid"},
    ]
    runtime.finish(workflow_id, "completed")
    await settle(bridge, workflow_id)
    assert bridge.get_app(app_id)["last_run_status"] == "success"
    assert bridge.get_workflow(workflow_id)["result"]["predictions"] == {"flow": [1]}
    assert bridge.resources_snapshot()["resources"]["concurrency"] == 0
    assert bridge.resources_snapshot()["runtime_resources"]["measurement"]["rss_mb"] == 12
    assert qos.get_metrics("b1-1").failure_count == 1
    assert qos.get_metrics("b1-2").success_count == 1
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_failed_waiter_does_not_finish_running_execution(components):
    bridge, runtime, manager, _, _, qos = components
    app_id = bridge.create("Observer", REQUEST)["app_id"]
    workflow_id = (await bridge.start(app_id))["workflow_id"]
    runtime.waiters[workflow_id].set_exception(RuntimeError("waiter failed"))
    await settle(bridge, workflow_id)
    assert manager.get_app(app_id).status == "running"
    assert qos.get_metrics_dict() == []
    runtime.waiters[workflow_id] = asyncio.get_running_loop().create_future()
    runtime.finish(workflow_id)
    assert bridge.get_app(app_id)["last_run_status"] == "success"
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_submit_failure_persists_error_without_qos(components, tmp_path):
    bridge, runtime, _, _, _, qos = components
    app_id = bridge.create("Invalid runtime request", REQUEST)["app_id"]
    runtime.submit_error = ValueError("contract rejected")
    with pytest.raises(ValueError, match="contract rejected"):
        await bridge.start(app_id)
    assert json.loads((tmp_path / "apps.json").read_text())[app_id]["status"] == "error"
    assert bridge.get_app(app_id)["last_run_status"] is None
    assert qos.get_metrics_dict() == []
    await bridge.shutdown()


@pytest.mark.parametrize("completion", ["rerun", "shutdown"])
@pytest.mark.asyncio
async def test_one_shot_running_state_write_failure_cleans_up_after_real_terminal(
    components, tmp_path, monkeypatch, completion,
):
    bridge, runtime, manager, _, _, qos = components
    app_id = bridge.create("Transient persistence failure", REQUEST)["app_id"]
    save = manager._save_to_disk
    failures = []

    def fail_running_write_once():
        app = manager.get_app(app_id)
        if app.status == "running" and not failures:
            failures.append(app.workflow_handle)
            raise OSError("one-shot running state write failure")
        return save()

    monkeypatch.setattr(manager, "_save_to_disk", fail_running_write_once)
    with pytest.raises(OSError, match="one-shot running state write failure"):
        await bridge.start(app_id)
    workflow_id = failures[0]
    assert len(runtime.records) == 1 and runtime.cancelled[0][0] == workflow_id
    app = bridge.get_app(app_id)
    assert app["status"] == "stopping" and app["workflow_handle"] == workflow_id
    assert app["workflow_id"] == workflow_id and app["last_run_status"] is None
    stored = json.loads((tmp_path / "apps.json").read_text())[app_id]
    assert stored["status"] == "stopping" and stored["workflow_handle"] == workflow_id
    assert stored["guidance_file"]["metadata"]["traffic_run"]["active_workflow_id"] == workflow_id
    assert app["error_message"] == "one-shot running state write failure"
    with pytest.raises(ManagementConflict, match="cancelling"):
        await bridge.start(app_id)
    stopped = await bridge.stop(app_id)
    assert stopped["workflow_id"] == workflow_id and stopped["done"] is False
    assert stopped["status"] == "cancelling"
    assert bridge.resources_snapshot()["resources"]["concurrency"] == 1
    assert bridge.resources_snapshot()["reservations"]
    assert qos.get_metrics_dict() == []
    if completion == "shutdown":
        closing = asyncio.create_task(bridge.shutdown())
        await runtime.shutdown_entered.wait()
        assert not closing.done() and not bridge.closed
        assert manager.get_app(app_id).status == "stopping"
        assert bridge.resources_snapshot()["resources"]["concurrency"] == 1
    runtime.finish(workflow_id, "cancelled")
    await settle(bridge, workflow_id)
    app = bridge.get_app(app_id)
    assert app["status"] == "stopped" and app["workflow_handle"] is None
    assert app["last_run_status"] == "cancelled"
    assert qos.get_metrics("b1-1").total_calls == 1
    assert bridge.resources_snapshot()["reservations"] == {}
    assert json.loads((tmp_path / "apps.json").read_text())[app_id]["status"] == "stopped"
    if completion == "shutdown":
        await closing
        assert bridge.closed and runtime.closed
    else:
        restarted = await bridge.start(app_id)
        assert restarted["workflow_id"] and restarted["workflow_id"] != workflow_id
        runtime.finish(restarted["workflow_id"])
        await settle(bridge, restarted["workflow_id"])
        assert bridge.get_app(app_id)["last_run_status"] == "success"
        await bridge.shutdown()
    assert bridge._pending_bindings == {}


@pytest.mark.asyncio
async def test_pending_submission_recovers_after_storage_returns_and_execution_ended(
    components, tmp_path, monkeypatch,
):
    bridge, runtime, manager, _, _, qos = components
    app_id = bridge.create("Deferred binding cleanup", REQUEST)["app_id"]
    save = manager._save_to_disk
    recovered = False

    def unavailable_handle_store():
        if not recovered and manager.get_app(app_id).status in {"running", "stopping"}:
            raise OSError("handle store unavailable")
        return save()

    monkeypatch.setattr(manager, "_save_to_disk", unavailable_handle_store)
    with pytest.raises(OSError, match="handle store unavailable"):
        await bridge.start(app_id)
    workflow_id = next(iter(runtime.records))
    assert manager.get_app(app_id).status == "starting"
    assert manager.get_app(app_id).workflow_handle is None
    assert bridge._pending_bindings[app_id]["workflow_id"] == workflow_id
    assert qos.get_metrics_dict() == []
    assert bridge.resources_snapshot()["resources"]["concurrency"] == 1
    runtime.finish(workflow_id, "cancelled")
    await settle(bridge, workflow_id)
    assert manager.get_app(app_id).status == "starting"
    assert bridge._records[workflow_id]["done"] is False
    recovered = True
    app = bridge.get_app(app_id)
    assert app["status"] == "stopped" and app["last_run_status"] == "cancelled"
    assert app["workflow_id"] == workflow_id and app["workflow_handle"] is None
    assert bridge._pending_bindings == {}
    assert qos.get_metrics("b1-1").total_calls == 1
    assert json.loads((tmp_path / "apps.json").read_text())[app_id]["status"] == "stopped"
    await bridge.shutdown()
    assert bridge.closed


@pytest.mark.asyncio
async def test_deferred_failed_submission_cannot_overwrite_a_new_submission(
    components, tmp_path, monkeypatch,
):
    bridge, runtime, manager, _, _, _ = components
    app_id = bridge.create("Submission guard", REQUEST)["app_id"]
    save = manager._save_to_disk
    recovered = False

    def unavailable_handle_store():
        if not recovered and manager.get_app(app_id).status in {"running", "stopping"}:
            raise OSError("handle store unavailable")
        return save()

    monkeypatch.setattr(manager, "_save_to_disk", unavailable_handle_store)
    with pytest.raises(OSError):
        await bridge.start(app_id)
    first = next(iter(runtime.records))
    old_run = deepcopy(manager.get_app(app_id).guidance_file.metadata["traffic_run"])
    recovered = True
    # An independent newer start can also be in starting/handle=None. Its
    # persisted submission ID must defeat cleanup of the failed older start.
    new_run = {**old_run, "submission_id": "newer-independent-submission"}
    manager.update_state(app_id, "starting", workflow_handle=None, metadata={"traffic_run": new_run})
    before = (tmp_path / "apps.json").read_bytes()
    runtime.finish(first, "cancelled")
    await settle(bridge, first)
    assert (tmp_path / "apps.json").read_bytes() == before
    assert manager.get_app(app_id).guidance_file.metadata["traffic_run"]["submission_id"] == "newer-independent-submission"
    assert manager.get_app(app_id).status == "starting"
    assert bridge._pending_bindings == {}
    manager.update_state(app_id, "stopped", workflow_handle=None)
    restarted = await bridge.start(app_id)
    assert restarted["workflow_id"] and restarted["workflow_id"] != first
    assert manager.get_app(app_id).guidance_file.metadata["traffic_run"]["submission_id"] != old_run["submission_id"]
    runtime.finish(restarted["workflow_id"])
    await settle(bridge, restarted["workflow_id"])
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_legacy_manager_drives_traffic_and_waits_before_reporting_stopped(components):
    bridge, runtime, manager, engine, _, _ = components
    app_id = bridge.create("Traffic only", REQUEST)["app_id"]

    async def forbidden(*args):
        raise AssertionError("Traffic application entered the conversational engine")

    engine.start_app = forbidden
    workflow_id = await manager.start(app_id)
    assert workflow_id == manager.get_app(app_id).workflow_handle
    assert await manager.start_schedule(app_id) is False
    stopping = asyncio.create_task(manager.stop(app_id))
    await asyncio.sleep(0)
    assert not stopping.done() and manager.get_app(app_id).status == "stopping"
    runtime.finish(workflow_id, "cancelled")
    assert await stopping is True
    assert manager.get_app(app_id).status == "stopped"
    await bridge.shutdown()
    with pytest.raises(RuntimeError, match="unavailable"):
        await manager.start(app_id)


@pytest.mark.asyncio
async def test_restart_marks_interrupted_without_success(components, tmp_path):
    bridge, runtime, _, _, _, _ = components
    app_id = bridge.create("Restart", REQUEST)["app_id"]
    workflow_id = (await bridge.start(app_id))["workflow_id"]
    restored = AppManager(tmp_path / "apps.json", execution_backend=AppLogicEngine())
    app = restored.get_app(app_id)
    assert app.status == "stopped" and app.workflow_handle is None
    assert app.guidance_file.metadata["traffic_run"]["last_run_status"] == "interrupted"
    assert json.loads((tmp_path / "apps.json").read_text())[app_id]["status"] == "stopped"
    runtime.finish(workflow_id)
    await settle(bridge, workflow_id)
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_ordinary_and_periodic_manager_interfaces_retained(tmp_path):
    class LegacyBackend(AppLogicEngine):
        async def start_app(self, app_id):
            return "ordinary_handle"

        async def stop_app(self, app_id):
            return True

    class LegacyScheduler:
        async def start_schedule(self, *args):
            return True

        async def stop_schedule(self, app_id):
            return True

    manager = AppManager(tmp_path / "legacy.json", execution_backend=LegacyBackend())
    app = manager.install("Ordinary", GuidanceFile("legacy", "Task", constraints={"schedule_interval_seconds": 10}))
    assert await manager.start(app.app_id) == "ordinary_handle"
    assert await manager.stop(app.app_id) is True
    manager._get_scheduler = lambda: LegacyScheduler()
    assert await manager.start_schedule(app.app_id) is True
    assert manager.get_app(app.app_id).status == "scheduled"
    assert await manager.stop_schedule(app.app_id) is True
    assert manager.uninstall(app.app_id) is True


@pytest.fixture
async def client(components):
    bridge = components[0]
    app = FastAPI()
    app.include_router(create_management_router(bridge))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://traffic.test:8008") as client:
        yield client


@pytest.mark.asyncio
async def test_api_lifecycle_and_local_registry_routes(client, components):
    bridge, runtime, _, _, registry, _ = components
    created = await client.post("/traffic/platform/apps", json={"name": "API", "request": REQUEST})
    assert created.status_code == 201
    app_id = created.json()["app_id"]
    started = await client.post(f"/traffic/platform/apps/{app_id}/run")
    assert started.status_code == 202
    workflow_id = started.json()["workflow_id"]
    for path in ("overview", "catalog", "templates", "apps", f"apps/{app_id}",
                 f"workflows/{workflow_id}", "resources", "qos"):
        response = await client.get(f"/traffic/platform/{path}")
        assert response.status_code == 200, response.text
    for agent in registry.query_agents():
        endpoint = agent["endpoint"]
        assert endpoint.startswith("http://traffic.test:8008/traffic/platform/instances/")
        assert endpoint.endswith("/execute") and agent["endpoint_method"] == "POST"
        metadata = await client.get(endpoint.removesuffix("/execute"))
        assert metadata.json()["instance_id"] == agent["id"]
    stopped = await client.post(f"/traffic/platform/apps/{app_id}/stop")
    assert stopped.status_code == 202 and stopped.json()["status"] == "cancelling"
    assert (await client.delete(f"/traffic/platform/apps/{app_id}")).status_code == 202
    runtime.finish(workflow_id, "cancelled")
    await settle(bridge, workflow_id)
    assert (await client.post(f"/traffic/platform/apps/{app_id}/stop")).status_code == 200
    assert (await client.delete(f"/traffic/platform/apps/{app_id}")).status_code == 200
    assert (await client.get(f"/traffic/platform/apps/{app_id}")).status_code == 404
    assert (await client.get(f"/traffic/platform/workflows/{workflow_id}")).status_code == 200
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_api_workflow_binding_and_missing_ids(client, components):
    bridge, runtime, _, _, _, _ = components
    response = await client.post("/traffic/platform/workflows", json={"request": REQUEST})
    assert response.status_code == 202
    app_id, workflow_id = response.json()["app_id"], response.json()["workflow_id"]
    assert (await client.post("/traffic/platform/workflows", json={"request": REQUEST, "appid": app_id})).json()["workflow_id"] == workflow_id
    response = await client.post("/traffic/platform/workflows", json={"request": {**REQUEST, "template": "flow"}, "appid": app_id})
    assert response.status_code == 409
    for path in ("apps/missing", "workflows/missing", "instances/missing"):
        assert (await client.get(f"/traffic/platform/{path}")).status_code == 404
    assert (await client.post("/traffic/platform/apps/missing/run")).status_code == 404
    assert (await client.post("/traffic/platform/apps/missing/stop")).status_code == 404
    assert (await client.delete("/traffic/platform/apps/missing")).status_code == 404
    runtime.finish(workflow_id)
    await settle(bridge, workflow_id)
    await bridge.shutdown()


@pytest.mark.parametrize("patch", [
    {"template": "all"}, {"target_frames": [True]}, {"target_frames": ["1251"]},
    {"target_frames": []}, {"target_frames": [1251, 1251]}, {"target_frames": [0]},
    {"target_frames": [1248]}, {"target_frames": [1488]}, {"target_frames": [1251.0]},
    {"deadline_seconds": 0}, {"deadline_seconds": "30"}, {"deadline_seconds": True},
    {"priority": True}, {"max_attempts": 0}, {"enable_reuse": "true"},
    {"enable_contracts": 1}, {"enable_recovery": "false"}, {"unknown": 1},
    {"max_attempts": 101}, {"allowed_instances": []}, {"allowed_instances": [True]},
    {"allowed_instances": [" "]}, {"allowed_instances": ["b1-1", "b1-1"]},
])
@pytest.mark.asyncio
async def test_api_strict_request_validation(client, components, patch):
    response = await client.post("/traffic/platform/apps", json={"name": "Invalid", "request": {**REQUEST, **patch}})
    assert response.status_code == 422, response.text
    assert components[0].list_apps() == []
    assert components[1].initialize_calls == 0


@pytest.mark.parametrize("value", [float("inf"), float("nan"), -float("inf")])
def test_deadlines_are_finite(value):
    with pytest.raises(ValidationError):
        TrafficWorkflowRequest.model_validate({**REQUEST, "deadline_seconds": value})


@pytest.mark.asyncio
async def test_api_strict_envelopes_and_command_bodies(client, components):
    for body in ({"name": " ", "request": REQUEST}, {"name": 123, "request": REQUEST},
                 {"name": "App", "request": REQUEST, "extra": True}):
        assert (await client.post("/traffic/platform/apps", json=body)).status_code == 422
    for body in ({"request": REQUEST, "appid": 1}, {"request": REQUEST, "appid": " "},
                 {"request": REQUEST, "unknown": True}):
        assert (await client.post("/traffic/platform/workflows", json=body)).status_code == 422
    app_id = components[0].create("Commands", REQUEST)["app_id"]
    for action in ("run", "stop"):
        assert (await client.post(f"/traffic/platform/apps/{app_id}/{action}", json={"all": True})).status_code == 422


@pytest.mark.asyncio
async def test_read_apis_lazy_initialize_once(client, components):
    bridge, runtime, _, _, _, _ = components
    assert runtime.initialize_calls == 0
    responses = await asyncio.gather(*(client.get(f"/traffic/platform/{path}")
                                       for path in ("overview", "catalog", "templates")))
    assert all(r.status_code == 200 for r in responses)
    assert runtime.initialize_calls == 1
    await bridge.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["overview", "catalog", "templates", "resources"])
async def test_missing_artifacts_are_explicit_503(client, components, path):
    runtime = components[1]

    async def missing():
        raise FileNotFoundError("Missing B1 checkpoint: /configured/artifacts/B1.pt")

    runtime.initialize = missing
    response = await client.get(f"/traffic/platform/{path}")
    assert response.status_code == 503
    assert "Missing B1 checkpoint" in response.json()["detail"]
    assert runtime.records == {}


def test_default_bridge_reuses_singleton_and_explicit_store_isolates(components, monkeypatch, tmp_path):
    import src.app.app_manager as module

    _, runtime, manager, _, registry, qos = components
    # Use a fresh manager since each kind may have only one bridge owner.
    shared = AppManager(tmp_path / "shared.json", execution_backend=AppLogicEngine())
    monkeypatch.setattr(module, "_manager_instance", shared)
    monkeypatch.delenv("TRAFFIC_APPS_STORE_PATH", raising=False)
    bridge = TrafficManagementBridge(runtime, registry=registry, qos=qos)
    assert bridge.manager is shared
    monkeypatch.setenv("TRAFFIC_APPS_STORE_PATH", str(tmp_path / "isolated.json"))
    isolated = TrafficManagementBridge(FakeRuntime(), registry=registry, qos=qos)
    assert isolated.manager is not shared and isolated.manager is not manager


@pytest.mark.asyncio
async def test_instance_execution_endpoint_restricts_runtime_candidates(client, components):
    bridge, runtime, _, _, registry, _ = components
    await bridge.initialize()
    agent = registry.get_agent_by_id("b1-2")
    response = await client.post(agent["endpoint"], json={"request": {**REQUEST, "template": "flow"}})
    assert response.status_code == 202, response.text
    workflow_id = response.json()["workflow_id"]
    assert runtime.records[workflow_id]["request"]["allowed_instances"] == ["b1-2"]
    assert bridge.get_app(response.json()["app_id"])["request"]["allowed_instances"] == ["b1-2"]
    conflicting = await client.post(agent["endpoint"], json={"request": {**REQUEST, "allowed_instances": ["b1-1"]}})
    assert conflicting.status_code == 422
    assert (await client.post("/traffic/platform/instances/missing/execute", json={"request": REQUEST})).status_code == 404
    runtime.finish(workflow_id)
    await settle(bridge, workflow_id)
    await bridge.shutdown()


def injected_joint_backend(gate=None, entered=None, *, fail=False):
    """An explicit model fake; all compilation/scheduling/execution owners are real."""
    import numpy as np
    from src.service.traffic_joint_runtime import build_backend

    class Model:
        version = "B1:management-test"
        checkpoint_sha256 = "b" * 64

        def prepare_history(self, frames):
            return {name: np.ones((len(frames), 3, 6, 6, 36 if name == "od" else 2), np.float32)
                    for name in ("state", "flow", "od")}

        def predict(self, inputs, task="all"):
            if entered is not None:
                entered.set()
            if gate is not None and not gate.wait(3):
                raise RuntimeError("Injected test model gate did not open")
            if fail:
                raise RuntimeError("Injected B1 model failure")
            predictions = {}
            for name in ("state", "flow", "od"):
                channels = 36 if name == "od" else 2
                values = np.full((len(inputs["target_frames"]), 1, 6, 6, channels), 2, np.float32)
                predictions[name] = {"prediction": values, "normalized": values.copy(),
                                     "shape": list(values.shape), "layout": "B,H,Y,X,C",
                                     "units": [metadata["od_units"]] * channels if name == "od" else metadata[name + "_units"],
                                     "channels": [f"destination_{i}" for i in range(36)] if name == "od" else metadata[name + "_channels"]}
            return {"version": self.version, "checkpoint_sha256": self.checkpoint_sha256,
                    "predictions": predictions}

    metadata = {"grid": [6, 6], "step_minutes": 30, "processed_sha256": "c" * 64,
                "state_units": ["active trips", "mph"], "flow_units": ["trips/30min"] * 2,
                "state_channels": ["active_trip_count_at_interval_end_by_origin", "mean_completed_trip_speed_by_destination"],
                "flow_channels": ["dropoffs_in_interval", "pickups_in_interval"],
                "od_units": "trips/30min", "od_definition": "completed trip count by origin,destination and actual dropoff interval"}
    stats = {name: (np.zeros(36 if name == "od" else 2, np.float32),
                    np.ones(36 if name == "od" else 2, np.float32))
             for name in ("state", "flow", "od")}
    return build_backend(Model(), metadata=metadata, stats=stats, normalization_sha256="a" * 64)


@pytest.mark.asyncio
@pytest.mark.parametrize("template,outputs", [("flow", {"flow"}), ("analysis", {"state", "flow"}),
                                               ("od_hotspots", {"od"})])
async def test_management_with_real_joint_runtime_and_injected_model(components, template, outputs):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    bridge, _, manager, _, _, qos = components
    runtime = JointTrafficRuntime(backend_factory=lambda root: injected_joint_backend(), instance_count=2)
    bridge.runtime = runtime
    try:
        await bridge.initialize()
        started = await bridge.execute_instance("b1-2", {**REQUEST, "template": template})
        plan = runtime.workflows[started["workflow_id"]].plan
        assert all({candidate.instance_id for candidate in node.candidates} == {"b1-2"}
                   for node in plan.nodes)
        await bridge.wait(started["app_id"])
        workflow = bridge.get_workflow(started["workflow_id"])
        assert workflow["status"] == "success" and workflow["done"] is True
        assert set(workflow["result"]["predictions"]) == outputs
        assert {task["instance_id"] for task in workflow["tasks"].values()} == {"b1-2"}
        assert manager.get_app(started["app_id"]).status == "stopped"
        assert bridge.resources_snapshot()["resources"]["concurrency"] == 0
        assert bridge.resources_snapshot()["reservations"] == {}
        assert qos.get_metrics("b1-2").total_calls == len(workflow["result"]["profiles"])
        assert runtime.model_forward_count == 1
    finally:
        await bridge.shutdown()


@pytest.mark.asyncio
async def test_real_runtime_cancel_retains_work_until_physical_model_returns(components):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    bridge, _, manager, _, _, qos = components
    gate, entered = threading.Event(), threading.Event()
    bridge.runtime = JointTrafficRuntime(backend_factory=lambda root: injected_joint_backend(gate, entered), instance_count=2)
    try:
        app_id = bridge.create("Physical cancellation", {**REQUEST, "template": "flow", "allowed_instances": ["b1-2"]})["app_id"]
        workflow_id = await manager.start(app_id)
        assert await asyncio.to_thread(entered.wait, 2)
        result = await bridge.stop(app_id)
        assert result["status"] == "cancelling" and result["done"] is False
        assert bridge.resources_snapshot()["resources"]["concurrency"] == 1
        assert bridge.runtime.snapshot()["active_futures"] == 1
        assert qos.get_metrics_dict() == []
        legacy_stop = asyncio.create_task(manager.stop(app_id))
        await asyncio.sleep(0.01)
        assert not legacy_stop.done()
        gate.set()
        assert await legacy_stop is True
        assert bridge.get_app(app_id)["last_run_status"] == "cancelled"
        assert bridge.get_workflow(workflow_id)["done"] is True
        assert bridge.resources_snapshot()["reservations"] == {}
        assert qos.get_metrics("b1-2").failure_count == 1
    finally:
        gate.set()
        await bridge.shutdown()


@pytest.mark.asyncio
async def test_real_runtime_cache_hits_do_not_invent_physical_qos_calls(components):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    bridge, _, _, _, _, qos = components
    bridge.runtime = JointTrafficRuntime(backend_factory=lambda root: injected_joint_backend(), instance_count=1)
    try:
        for index in range(2):
            app_id = bridge.create(f"Cache {index}", {**REQUEST, "template": "flow"})["app_id"]
            await bridge.start(app_id)
            await bridge.wait(app_id)
        assert bridge.runtime.model_forward_count == 1
        assert bridge.runtime.cache_hits == 1
        assert qos.get_metrics("b1-1").total_calls == len(bridge.runtime.snapshot()["profiles"])
        assert qos.get_metrics("b1-1").total_calls == 7
    finally:
        await bridge.shutdown()


def joint_definition():
    """An explicit custom DAG using existing flow and OD capabilities together."""
    return {"requested_outputs": ["flow", "od"], "nodes": [
        {"node_id": "load_window", "capability_id": "history.prepare", "depends_on": [], "parameters": {"inputs": {}}},
        {"node_id": "joint_model", "capability_id": "traffic.predict", "depends_on": ["load_window"],
         "parameters": {"inputs": {"model_inputs": {"node_id": "load_window", "output": "inputs"}}}},
        {"node_id": "flow_check", "capability_id": "validate.flow", "depends_on": ["joint_model"],
         "parameters": {"inputs": {"prediction": {"node_id": "joint_model", "output": "flow"}}}},
        {"node_id": "od_check", "capability_id": "validate.od", "depends_on": ["joint_model"],
         "parameters": {"inputs": {"prediction": {"node_id": "joint_model", "output": "od"}}}},
        {"node_id": "join", "capability_id": "result.aggregate", "depends_on": ["flow_check", "od_check"],
         "parameters": {"inputs": {"flow": {"node_id": "flow_check", "output": "flow"},
                                   "od": {"node_id": "od_check", "output": "od"}}}},
    ]}


def management_for(runtime, tmp_path, *, qos=None, qos_clock=None):
    manager = AppManager(tmp_path / "feedback-apps.json", execution_backend=AppLogicEngine())
    config = tmp_path / "feedback-agents.json"
    config.write_text('{"agents": []}')
    registry = AgentRegistryClient(config_file=str(config))
    return TrafficManagementBridge(runtime, manager, registry, qos if qos is not None else QoSMonitor(),
                                   service_url="http://traffic.test:8008", qos_clock=qos_clock)


def management_http(bridge):
    app = FastAPI()
    app.include_router(create_management_router(bridge))
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://traffic.test:8008")


@pytest.mark.asyncio
async def test_custom_template_api_register_create_and_execute(tmp_path):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    runtime = JointTrafficRuntime(backend_factory=lambda root: injected_joint_backend(), instance_count=2)
    bridge = management_for(runtime, tmp_path)
    definition = joint_definition()
    try:
        async with management_http(bridge) as client:
            response = await client.post("/traffic/platform/templates", json={"template_id": "flow_and_od", "definition": definition})
            assert response.status_code == 201, response.text
            assert response.json()["outputs"] == ["flow", "od"]
            assert response.json()["capabilities"] == ["history.prepare", "traffic.predict", "validate.flow", "validate.od", "result.aggregate"]
            definition["nodes"][0]["capability_id"] = "modified.after.registration"
            directory = (await client.get("/traffic/platform/templates")).json()
            assert {item["template"] for item in directory} == {"flow", "analysis", "od_hotspots", "flow_and_od"}
            request = {**REQUEST, "template": "flow_and_od", "allowed_instances": ["b1-2"]}
            created = await client.post("/traffic/platform/apps", json={"name": "Custom joint traffic", "request": request})
            assert created.status_code == 201, created.text
            app_id = created.json()["app_id"]
            guidance = bridge.manager.get_app(app_id).guidance_file
            assert guidance.agents_required == response.json()["capabilities"]
            assert guidance.metadata["traffic_template"]["nodes"][0]["capability_id"] == "history.prepare"
            started = await client.post(f"/traffic/platform/apps/{app_id}/run")
            assert started.status_code == 202, started.text
            workflow_id = started.json()["workflow_id"]
            result = await client.get(f"/traffic/platform/workflows/{workflow_id}?wait=true")
            assert result.status_code == 200, result.text
            assert result.json()["done"] and result.json()["status"] == "success"
            assert set(result.json()["result"]["predictions"]) == {"flow", "od"}
            assert {task["node_id"] for task in result.json()["tasks"].values()} == {"load_window", "joint_model", "flow_check", "od_check", "join"}
            assert {task["instance_id"] for task in result.json()["tasks"].values()} == {"b1-2"}
            assert result.json()["reservations"] == {}
            assert runtime.model_forward_count == 1
    finally:
        await bridge.shutdown()


@pytest.mark.asyncio
async def test_templates_include_direct_compiler_registrations_and_are_isolated(tmp_path):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    bridge = management_for(JointTrafficRuntime(backend_factory=lambda root: injected_joint_backend()), tmp_path)
    try:
        await bridge.initialize()
        bridge.runtime.compiler.register_template("direct_joint", joint_definition())
        descriptor = next(item for item in bridge.templates() if item["template"] == "direct_joint")
        descriptor["definition"]["nodes"][0]["capability_id"] = "modified.read.result"
        async with management_http(bridge) as client:
            directory = (await client.get("/traffic/platform/templates")).json()
            actual = next(item for item in directory if item["template"] == "direct_joint")
            assert actual["definition"]["nodes"][0]["capability_id"] == "history.prepare"
            created = await client.post("/traffic/platform/apps", json={"name": "Directly registered", "request": {**REQUEST, "template": "direct_joint"}})
            assert created.status_code == 201, created.text
            duplicate = await client.post("/traffic/platform/templates", json={"template_id": "direct_joint", "definition": joint_definition()})
            assert duplicate.status_code == 409
            unknown = await client.post("/traffic/platform/apps", json={"name": "Unknown", "request": {**REQUEST, "template": "unknown_joint"}})
            assert unknown.status_code == 422
    finally:
        await bridge.shutdown()


@pytest.mark.asyncio
async def test_custom_application_template_survives_store_reload(tmp_path):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    def runtime():
        return JointTrafficRuntime(backend_factory=lambda root: injected_joint_backend(), instance_count=1)

    bridge = management_for(runtime(), tmp_path)
    try:
        await bridge.register_template("persistent_joint", joint_definition())
        app_id = bridge.create("Persistent custom app", {**REQUEST, "template": "persistent_joint"})["app_id"]
    finally:
        await bridge.shutdown()
    restored = management_for(runtime(), tmp_path)
    try:
        async with management_http(restored) as client:
            templates = await client.get("/traffic/platform/templates")
            assert templates.status_code == 200, templates.text
            assert "persistent_joint" in {item["template"] for item in templates.json()}
            started = await client.post(f"/traffic/platform/apps/{app_id}/run")
            assert started.status_code == 202, started.text
            result = await client.get(f"/traffic/platform/workflows/{started.json()['workflow_id']}?wait=true")
            assert result.status_code == 200, result.text
            assert result.json()["status"] == "success"
    finally:
        await restored.shutdown()


@pytest.mark.parametrize("body", [
    {"template_id": " ", "definition": {}}, {"template_id": True, "definition": {}},
    {"template_id": "bad", "definition": []},
    {"template_id": "bad", "definition": {}, "execute": "python"},
])
@pytest.mark.asyncio
async def test_template_envelope_is_strict(client, components, body):
    response = await client.post("/traffic/platform/templates", json=body)
    assert response.status_code == 422
    assert components[1].initialize_calls == 0


@pytest.mark.parametrize("change", ["unknown_capability", "unknown_definition_field", "executable_node", "missing_output", "cycle"])
@pytest.mark.asyncio
async def test_invalid_template_declarations_never_register(tmp_path, change):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    bridge = management_for(JointTrafficRuntime(backend_factory=lambda root: injected_joint_backend()), tmp_path)
    definition = joint_definition()
    if change == "unknown_capability":
        definition["nodes"][1]["capability_id"] = "python.arbitrary"
    elif change == "unknown_definition_field":
        definition["code"] = "print('execute')"
    elif change == "executable_node":
        definition["nodes"][1]["executor"] = "lambda: execute()"
    elif change == "missing_output":
        definition["nodes"][-1]["parameters"]["inputs"]["od"]["output"] = "undeclared"
    else:
        definition["nodes"][0]["depends_on"] = ["join"]
    try:
        async with management_http(bridge) as client:
            response = await client.post("/traffic/platform/templates", json={"template_id": "invalid_joint", "definition": definition})
            assert response.status_code == 422, response.text
            assert "invalid_joint" not in {item["template"] for item in (await client.get("/traffic/platform/templates")).json()}
            assert bridge.list_apps() == []
    finally:
        await bridge.shutdown()


@pytest.mark.parametrize("template", [True, 123, "", "   "])
@pytest.mark.asyncio
async def test_template_name_is_a_strict_nonblank_string(client, template):
    response = await client.post("/traffic/platform/apps", json={"name": "Bad name", "request": {**REQUEST, "template": template}})
    assert response.status_code == 422


@pytest.mark.asyncio
async def test_wait_true_uses_runtime_wait_and_keeps_default_snapshot(client, components):
    bridge, runtime, _, _, _, _ = components
    app_id = bridge.create("Wait API", REQUEST)["app_id"]
    workflow_id = (await bridge.start(app_id))["workflow_id"]
    snapshot = await client.get(f"/traffic/platform/workflows/{workflow_id}")
    assert snapshot.status_code == 200 and snapshot.json()["done"] is False
    reader = asyncio.create_task(client.get(f"/traffic/platform/workflows/{workflow_id}?wait=true"))
    await runtime.wait_entered.wait()
    await asyncio.sleep(0)
    assert not reader.done()
    count = runtime.snapshot_calls
    await asyncio.sleep(0.01)
    assert runtime.snapshot_calls == count
    assert len(runtime.wait_calls) == 2  # bridge owner and shared HTTP waiter
    runtime.finish(workflow_id)
    response = await reader
    assert response.status_code == 200, response.text
    assert response.json()["done"] is True and response.json()["status"] == "success"
    assert bridge.get_app(app_id)["status"] == "stopped"
    assert (await client.get("/traffic/platform/workflows/missing?wait=true")).status_code == 404
    await bridge.shutdown()


@pytest.mark.asyncio
async def test_cancelled_http_waiter_does_not_cancel_execution(client, components):
    bridge, runtime, _, _, _, _ = components
    app_id = bridge.create("Cancelled reader", REQUEST)["app_id"]
    workflow_id = (await bridge.start(app_id))["workflow_id"]
    reader = asyncio.create_task(client.get(f"/traffic/platform/workflows/{workflow_id}?wait=true"))
    await runtime.wait_entered.wait()
    await asyncio.sleep(0)
    reader.cancel()
    with pytest.raises(asyncio.CancelledError):
        await reader
    assert not bridge._api_waiters[workflow_id].done()
    assert runtime.cancelled == []
    assert bridge.resources_snapshot()["resources"]["concurrency"] == 1
    runtime.finish(workflow_id)
    response = await client.get(f"/traffic/platform/workflows/{workflow_id}?wait=true")
    assert response.status_code == 200 and response.json()["done"]
    assert len(runtime.wait_calls) == 2
    await bridge.shutdown()


class FailureAlertQoS(QoSMonitor):
    """Injected monitor configured to alert on the first terminal failed call."""

    def check_threshold(self, agent_id):
        metrics = self.get_metrics(agent_id)
        return bool(metrics and metrics.failure_count >= 1)


@pytest.mark.asyncio
async def test_qos_quarantine_is_owned_scoped_cooled_down_and_explicitly_recoverable(tmp_path):
    runtime = FakeRuntime()
    runtime.instances["other-owned"] = {"instance_id": "other-owned", "implementation_id": "rules", "healthy": True}
    clock = [0.0]
    monitor = FailureAlertQoS()
    bridge = management_for(runtime, tmp_path, qos=monitor, qos_clock=lambda: clock[0])
    try:
        await bridge.initialize()
        monitor.record_call("unrelated-registry-agent", 1, False)
        monitor.record_call("other-owned", 1, False)
        assert runtime.health_calls == []
        monitor.record_call("b1-1", 1, False)
        assert len(runtime.health_calls) == 1
        assert runtime.instances["b1-1"]["healthy"] is False
        assert runtime.instances["b1-2"]["healthy"] is True
        assert bridge.snapshot()["qos_actions"][0]["now"] == 0
        assert bridge.snapshot()["qos_actions"][0]["cooldown_until"] == 60
        for _ in range(3):
            monitor.record_call("b1-1", 1, False)
        assert len(runtime.health_calls) == 1
        async with management_http(bridge) as client:
            restored = await client.post("/traffic/platform/instances/b1-1/health", json={"healthy": True, "reason": "explicit recovery"})
            assert restored.status_code == 200, restored.text
            assert restored.json()["healthy"] is True and restored.json()["qos_quarantined"] is False
            clock[0] = 30
            monitor.record_call("b1-1", 1, False)
            assert runtime.instances["b1-1"]["healthy"] is True
            clock[0] = 60
            monitor.record_call("b1-1", 1, False)
            assert runtime.instances["b1-1"]["healthy"] is False
            assert (await client.post("/traffic/platform/instances/unrelated-registry-agent/health", json={"healthy": True})).status_code == 404
        actions = bridge.snapshot()["qos_actions"]
        assert [action["source"] for action in actions] == ["qos", "manual", "qos"]
        assert len([action for action in actions if action["source"] == "qos"]) == 2
    finally:
        await bridge.shutdown()
    count = len(runtime.health_calls)
    monitor.record_call("b1-2", 1, False)
    assert len(runtime.health_calls) == count


@pytest.mark.asyncio
async def test_real_failed_model_triggers_qos_once_and_next_graph_uses_backup(tmp_path):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    backends = iter([injected_joint_backend(fail=True), injected_joint_backend()])
    runtime = JointTrafficRuntime(backend_factory=lambda root: next(backends), instance_count=2)
    monitor = FailureAlertQoS()
    bridge = management_for(runtime, tmp_path, qos=monitor)
    try:
        async with management_http(bridge) as client:
            bad_request = {**REQUEST, "template": "flow", "allowed_instances": ["b1-1"], "enable_recovery": False, "max_attempts": 1}
            bad_app = (await client.post("/traffic/platform/apps", json={"name": "Failing real instance", "request": bad_request})).json()["app_id"]
            bad_run = (await client.post(f"/traffic/platform/apps/{bad_app}/run")).json()["workflow_id"]
            failed = await client.get(f"/traffic/platform/workflows/{bad_run}?wait=true")
            assert failed.status_code == 200, failed.text
            assert failed.json()["status"] == "failed" and failed.json()["done"] is True
            assert runtime.instances["b1-1"]["healthy"] is False
            assert len(bridge.snapshot()["qos_actions"]) == 1
            for _ in range(3):
                monitor.record_call("b1-1", 100, False)
            assert len(bridge.snapshot()["qos_actions"]) == 1
            backup_request = {**REQUEST, "template": "flow", "target_frames": [1252], "enable_reuse": False}
            backup = await client.post("/traffic/platform/apps", json={"name": "Backup selection", "request": backup_request})
            assert backup.status_code == 201, backup.text
            good_run = (await client.post(f"/traffic/platform/apps/{backup.json()['app_id']}/run")).json()["workflow_id"]
            good = await client.get(f"/traffic/platform/workflows/{good_run}?wait=true")
            assert good.status_code == 200, good.text
            assert good.json()["status"] == "success"
            assert {task["instance_id"] for task in good.json()["tasks"].values()} == {"b1-2"}
            assert all({candidate.instance_id for candidate in node.candidates} == {"b1-2"} for node in runtime.workflows[good_run].plan.nodes)
            assert bridge.resources_snapshot()["reservations"] == {}
    finally:
        await bridge.shutdown()


@pytest.mark.asyncio
async def test_latency_quarantine_never_releases_or_cancels_running_physical_work(tmp_path):
    from src.service.traffic_joint_runtime import JointTrafficRuntime

    gate, entered = threading.Event(), threading.Event()
    backends = iter([injected_joint_backend(gate, entered), injected_joint_backend()])
    runtime = JointTrafficRuntime(backend_factory=lambda root: next(backends), instance_count=2)
    monitor = QoSMonitor(max_avg_latency_ms=5)
    bridge = management_for(runtime, tmp_path, qos=monitor)
    try:
        async with management_http(bridge) as client:
            await bridge.initialize()
            first_app = bridge.create("Running unhealthy instance", {**REQUEST, "template": "flow", "allowed_instances": ["b1-1"]})["app_id"]
            first_run = (await bridge.start(first_app))["workflow_id"]
            assert await asyncio.to_thread(entered.wait, 2)
            reservations = deepcopy(bridge.resources_snapshot()["reservations"])
            monitor.record_call("b1-1", 100, False)
            assert runtime.instances["b1-1"]["healthy"] is False
            assert bridge.get_app(first_app)["status"] == "running"
            assert bridge.get_workflow(first_run)["done"] is False
            assert bridge.resources_snapshot()["reservations"] == reservations
            assert runtime.snapshot()["active_futures"] == 1
            running = [task for task in bridge.get_workflow(first_run)["tasks"].values() if task["schedule_status"] == "running"]
            assert len(running) == 1 and running[0]["node_id"] == "predict"
            backup = await bridge.submit_workflow({**REQUEST, "template": "flow", "target_frames": [1252], "enable_reuse": False})
            finished_backup = await client.get(f"/traffic/platform/workflows/{backup['workflow_id']}?wait=true")
            assert finished_backup.status_code == 200, finished_backup.text
            assert finished_backup.json()["status"] == "success"
            assert {task["instance_id"] for task in finished_backup.json()["tasks"].values()} == {"b1-2"}
            assert bridge.resources_snapshot()["reservations"] == reservations
            stopped = await bridge.stop(first_app)
            assert stopped["status"] == "cancelling" and stopped["done"] is False
            assert runtime.snapshot()["active_futures"] == 1
            gate.set()
            terminal = await client.get(f"/traffic/platform/workflows/{first_run}?wait=true")
            assert terminal.status_code == 200 and terminal.json()["status"] == "cancelled"
            assert bridge.resources_snapshot()["reservations"] == {}
            assert len([action for action in bridge.snapshot()["qos_actions"] if action["instance_id"] == "b1-1"]) == 1
    finally:
        gate.set()
        await bridge.shutdown()


@pytest.mark.asyncio
async def test_qos_from_another_thread_applies_health_on_the_runtime_loop(tmp_path):
    runtime = FakeRuntime()
    monitor = FailureAlertQoS()
    bridge = management_for(runtime, tmp_path, qos=monitor)
    try:
        await bridge.initialize()
        signal = runtime.health_changed
        await asyncio.to_thread(monitor.record_call, "b1-1", 1, False)
        await asyncio.wait_for(signal.wait(), 1)
        assert runtime.health_threads == [threading.get_ident()]
        assert len(bridge.snapshot()["qos_actions"]) == 1
    finally:
        await bridge.shutdown()


@pytest.mark.asyncio
async def test_ducktyped_qos_without_callback_still_records_terminal_calls(tmp_path):
    class FakeQos:
        def __init__(self):
            self.calls = []

        def record_call(self, agent_id, latency_ms, success):
            self.calls.append({"agent_id": agent_id, "latency_ms": latency_ms, "success": success})

        def get_metrics_dict(self):
            return deepcopy(self.calls)

    runtime, monitor = FakeRuntime(), FakeQos()
    bridge = management_for(runtime, tmp_path, qos=monitor)
    try:
        app_id = bridge.create("Ducktyped monitor", REQUEST)["app_id"]
        workflow_id = (await bridge.start(app_id))["workflow_id"]
        runtime.finish(workflow_id)
        await bridge.wait(app_id)
        assert monitor.calls == [{"agent_id": "b1-1", "latency_ms": 250, "success": True}]
        assert bridge.snapshot()["qos_feedback"]["callback_registered"] is False
        assert runtime.health_calls == []
    finally:
        await bridge.shutdown()


@pytest.mark.parametrize("body", [{"healthy": 1}, {"healthy": "true"}, {"healthy": True, "extra": 1},
                                  {"healthy": True, "reason": 123}, {"healthy": True, "reason": " "}])
@pytest.mark.asyncio
async def test_health_parameters_are_strict(client, components, body):
    response = await client.post("/traffic/platform/instances/b1-1/health", json=body)
    assert response.status_code == 422
    assert components[1].initialize_calls == 0
