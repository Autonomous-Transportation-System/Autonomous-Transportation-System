"""Joint runtime owners, physical forwards, explicit bindings and real B1."""
from __future__ import annotations

import asyncio
import copy
import hashlib
import os
import subprocess
import sys
import threading
import time
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pytest

from src.service.traffic_joint_runtime import JointTrafficRuntime, build_backend

REPO = Path(__file__).resolve().parents[2]


def locate_experiment_root():
    explicit = os.environ.get("TRAFFIC_EXPERIMENT_ROOT")
    if explicit:
        return Path(explicit).expanduser().resolve()
    candidates = [REPO / "data" / "traffic_artifacts"]
    candidates += [parent / "实验" / "研究点一_TBA" for parent in REPO.parents]
    return next((path for path in candidates if path.is_dir()), candidates[0])


ROOT = locate_experiment_root()


class FakeService:
    version = "B1:injected"
    checkpoint_sha256 = "b" * 64

    def __init__(self, gate=None, fail=False):
        self.gate = gate
        self.entered = threading.Event()
        self.fail = fail
        self.calls = 0

    def prepare_history(self, frames):
        return {name: np.ones((len(frames), 3, 6, 6, 36 if name == "od" else 2), dtype=np.float32)
                for name in ("state", "flow", "od")}

    def predict(self, inputs, task="all"):
        self.calls += 1
        self.entered.set()
        if self.gate is not None and not self.gate.wait(5):
            raise RuntimeError("Test forward gate did not open")
        if self.fail:
            raise RuntimeError("Injected model failure")
        predictions = {}
        for name in ("state", "flow", "od"):
            channels = 36 if name == "od" else 2
            values = np.full((len(inputs["target_frames"]), 1, 6, 6, channels), 2.0, dtype=np.float32)
            if name == "state":
                values[..., 1] = 5
            predictions[name] = dict(
                prediction=values, normalized=values.copy(), shape=list(values.shape), layout="B,H,Y,X,C",
                units=["trips/30min"] * channels if name != "state" else ["active trips", "mph"],
                channels=[f"destination_{i}" for i in range(36)] if name == "od" else (
                    ["active_trip_count_at_interval_end_by_origin", "mean_completed_trip_speed_by_destination"]
                    if name == "state" else ["dropoffs_in_interval", "pickups_in_interval"]))
        return dict(version=self.version, checkpoint_sha256=self.checkpoint_sha256, predictions=predictions)


def fake_backend(service):
    metadata = dict(grid=[6, 6], step_minutes=30, processed_sha256="c" * 64,
                    state_units=["active trips", "mph"], flow_units=["trips/30min"] * 2,
                    state_channels=["active_trip_count_at_interval_end_by_origin", "mean_completed_trip_speed_by_destination"],
                    flow_channels=["dropoffs_in_interval", "pickups_in_interval"],
                    od_units="trips/30min", od_definition="completed trip count by origin,destination and actual dropoff interval")
    stats = {name: (np.zeros(36 if name == "od" else 2, np.float32),
                    np.ones(36 if name == "od" else 2, np.float32)) for name in ("state", "flow", "od")}
    return build_backend(service, metadata=metadata, stats=stats, normalization_sha256="a" * 64)


def runtime_for(services, **kwargs):
    iterator = iter(services)
    return JointTrafficRuntime(backend_factory=lambda root: fake_backend(next(iterator)),
                               instance_count=len(services), **kwargs)


async def until(predicate, timeout=3):
    end = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() >= end:
            raise AssertionError("Expected runtime condition did not arrive")
        await asyncio.sleep(0.005)


def request(template="flow", frames=(1251, 1252), **kwargs):
    return dict(template=template, target_frames=list(frames), **kwargs)


def test_module_import_is_lazy():
    result = subprocess.run([sys.executable, "-c",
                             "import sys; import src.service.traffic_joint_runtime; "
                             "assert not any(n in sys.modules for n in ('numpy','torch','src.service.traffic_model'))"],
                            check=False, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_initialize_singleflight_ready_and_resident_cache_budget():
    services = [FakeService(), FakeService()]
    runtime = runtime_for(services)
    try:
        with pytest.raises(RuntimeError):
            runtime.submit(request())
        await asyncio.gather(*(runtime.initialize() for _ in range(8)))
        assert len(runtime.instances) == 2
        snapshot = runtime.snapshot()
        budget = snapshot["resources"]
        assert budget["task_memory_budget_mb"] == pytest.approx(
            2048 - budget["resident_memory_mb"] - budget["cache_resident_allowance_mb"])
        assert all(instance["lifecycle"] == "ready" for instance in runtime.instances.values())
        result = await runtime.run(request("analysis"))
        assert result["status"] == "completed"
        assert set(result["predictions"]) == {"state", "flow"}
        assert result["analysis"]["analysis"]["frames"][0]["congested_cells"] == 36
        assert not any(task["capability_id"] == "validate.od" for task in result["tasks"].values())
        assert result["profiles"] and all("thread_cpu_seconds" in profile for profile in result["profiles"])
    finally:
        await runtime.shutdown()
    assert runtime.closed and all(instance["lifecycle"] == "closed" for instance in runtime.instances.values())


def register_no_aggregate_template(runtime, outputs=("flow",)):
    nodes = [dict(node_id="prepare", capability_id="history.prepare", parameters={"inputs": {}}),
             dict(node_id="predict", capability_id="traffic.predict", depends_on=["prepare"],
                  parameters={"inputs": {"model_inputs": {"node_id": "prepare", "output": "inputs"}}})]
    nodes.extend(dict(node_id="validate_" + name, capability_id="validate." + name, depends_on=["predict"],
                      parameters={"inputs": {"prediction": {"node_id": "predict", "output": name}}})
                 for name in outputs)
    runtime.compiler.register_template("no_aggregate", dict(requested_outputs=list(outputs), nodes=nodes))


@pytest.mark.asyncio
@pytest.mark.parametrize("outputs", [("flow",), ("state", "flow"), ("state", "flow", "od")])
async def test_no_aggregate_collects_every_requested_terminal_prediction(outputs):
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        await runtime.initialize()
        register_no_aggregate_template(runtime, outputs)
        result = await runtime.run(request("no_aggregate"))
        assert result["status"] == "completed" and set(result["predictions"]) == set(outputs)
        assert not any(task["capability_id"] == "result.aggregate" for task in result["tasks"].values())
        for name in outputs:
            item = result["predictions"][name]
            assert item["shape"] == [2, 1, 6, 6, 36 if name == "od" else 2]
            assert {"prediction", "normalized", "layout", "units", "channels"} <= item.keys()
        assert not runtime.workflows[result["workflow_id"]].values
        original = copy.deepcopy(result["predictions"])
        result["predictions"][outputs[0]]["prediction"][0][0][0][0][0] = 999
        again = await runtime.wait(result["workflow_id"])
        assert again["predictions"] == original
        assert runtime.snapshot(result["workflow_id"])["result"]["predictions"] == original
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_no_aggregate_preserves_generic_array_and_tensor_payload_terminal_outputs():
    from src.service import traffic_contracts as c
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        await runtime.initialize()
        common = runtime.catalog.snapshot()["implementations"]["B1"]["contract"]
        runtime.catalog.register_implementation(dict(
            implementation_id="terminal-tools", version=runtime.instances["b1-1"]["version"], contract=common,
            capabilities={name: dict(resources=dict(cpu=0.1, memory_mb=1))
                          for name in ("plugin.vector", "plugin.tensor")}))
        runtime.catalog.register_instance(dict(instance_id="terminal-tools-1", implementation_id="terminal-tools",
                                               endpoint="local://terminal-tools-1", max_concurrency=2, base_memory_mb=0))
        values = np.arange(6, dtype=np.float32).reshape(2, 3)
        schema = c.TensorSchema(field="tensor", axes=("B", "C"), shape=(2, 3), units=("count",) * 3,
                                semantic="published count vector", version=runtime.instances["b1-1"]["version"],
                                grid_id=common["grid_id"])
        runtime.register_operation("plugin.vector", lambda *args: {"vector": values})
        runtime.register_operation("plugin.tensor", lambda *args: {
            "tensor": c.TensorPayload(values, schema, "registered-terminal-source")})
        runtime.compiler.register_template("terminal_values", dict(requested_outputs=["vector", "tensor"], nodes=[
            dict(node_id="vector", capability_id="plugin.vector", outputs=["vector"]),
            dict(node_id="tensor", capability_id="plugin.tensor", outputs=["tensor"])]))
        result = await runtime.run(request("terminal_values"))
        assert result["status"] == "completed" and set(result["predictions"]) == {"vector", "tensor"}
        np.testing.assert_array_equal(result["predictions"]["vector"], values)
        tensor = result["predictions"]["tensor"]
        np.testing.assert_array_equal(tensor["data"], values)
        assert tensor["schema"] == schema.to_dict() and tensor["source"] == "registered-terminal-source"
        assert not runtime.workflows[result["workflow_id"]].values and not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["terminal", "aggregate"])
async def test_ambiguous_compiler_accepted_exports_are_rejected_before_admission(kind):
    from src.service import traffic_contracts as c
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        await runtime.initialize()
        nodes = [dict(node_id="prepare", capability_id="history.prepare"),
                 dict(node_id="predict", capability_id="traffic.predict", depends_on=["prepare"],
                      parameters={"inputs": {"model_inputs": {"node_id": "prepare", "output": "inputs"}}}),
                 dict(node_id="validate_flow", capability_id="validate.flow", depends_on=["predict"],
                      parameters={"inputs": {"prediction": {"node_id": "predict", "output": "flow"}}})]
        if kind == "terminal":
            nodes.append(dict(node_id="validate_flow_other", capability_id="validate.flow", depends_on=["predict"],
                              parameters={"inputs": {"prediction": {"node_id": "predict", "output": "flow"}}}))
        else:
            nodes.append(dict(node_id="validate_flow_other", capability_id="validate.flow", depends_on=["predict"],
                              parameters={"inputs": {"prediction": {"node_id": "predict", "output": "flow"}}}))
            nodes.extend(dict(node_id=name, capability_id="result.aggregate", depends_on=[producer],
                              parameters={"inputs": {"flow": {"node_id": producer, "output": "flow"}}})
                         for name, producer in (("aggregate_left", "validate_flow"), ("aggregate_right", "validate_flow_other")))
        runtime.compiler.register_template("ambiguous_exports", dict(requested_outputs=["flow"], nodes=nodes))
        # The compiler accepts the graph, while runtime requires one unambiguous
        # terminal producer for each requested business output.
        runtime.compiler.compile(request("ambiguous_exports"), now=time.monotonic())
        with pytest.raises(c.ContractError, match="multiple terminal producers"):
            runtime.submit(request("ambiguous_exports"))
        assert not runtime.workflows and runtime.model_forward_count == 0
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_multiple_aggregates_of_same_producer_keep_the_complete_prediction():
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        await runtime.initialize()
        nodes = [dict(node_id="prepare", capability_id="history.prepare"),
                 dict(node_id="predict", capability_id="traffic.predict", depends_on=["prepare"],
                      parameters={"inputs": {"model_inputs": {"node_id": "prepare", "output": "inputs"}}}),
                 dict(node_id="validate_flow", capability_id="validate.flow", depends_on=["predict"],
                      parameters={"inputs": {"prediction": {"node_id": "predict", "output": "flow"}}})]
        nodes.extend(dict(node_id=name, capability_id="result.aggregate", depends_on=["validate_flow"],
                          parameters={"inputs": {"flow": {"node_id": "validate_flow", "output": "flow"}}})
                     for name in ("aggregate_left", "aggregate_right"))
        runtime.compiler.register_template("repeated_export", dict(requested_outputs=["flow"], nodes=nodes))
        result = await runtime.run(request("repeated_export"))
        assert result["status"] == "completed" and set(result["predictions"]) == {"flow"}
        assert result["predictions"]["flow"]["shape"] == [2, 1, 6, 6, 2]
        assert sum(task["capability_id"] == "result.aggregate" for task in result["tasks"].values()) == 2
        assert runtime.model_forward_count == 1 and not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_terminal_collector_merges_every_aggregate_and_direct_output():
    from src.service.traffic_types import PlanNode, WorkflowPlan
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        await runtime.initialize()
        plan = WorkflowPlan("collector", "", "multiple_exports", (
            PlanNode("flow_export", "result.aggregate", parameters={"inputs": {"flow": {}},
                                                                    "output_ports": ("predictions", "analysis")}),
            PlanNode("state_export", "result.aggregate", parameters={"inputs": {"state": {}},
                                                                     "output_ports": ("predictions", "analysis")}),
            PlanNode("od_export", "validate.od", parameters={"output_ports": ("od",)})),
            (1251,), ("flow", "state", "od"))
        record = SimpleNamespace(plan=plan, values={
            "flow_export": {"predictions": {"flow": [1]}, "analysis": {}},
            "state_export": {"predictions": {"state": [2]}, "analysis": {}},
            "od_export": {"od": [3]}})
        runtime._check_terminal_output_sources(plan)
        assert runtime._collect_terminal_results(record)["predictions"] == {"flow": [1], "state": [2], "od": [3]}
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_missing_declared_terminal_output_is_quality_invalid(monkeypatch):
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        await runtime.initialize()
        register_no_aggregate_template(runtime)
        monkeypatch.setattr(runtime, "_validate", lambda *args: {})
        result = await runtime.run(request("no_aggregate", enable_recovery=False))
        assert result["status"] == "failed" and not result["predictions"]
        task = next(task for task in result["tasks"].values() if task["capability_id"] == "validate.flow")
        assert task["call_status"] == "success" and task["quality_status"] == "invalid"
        assert any("declared output ports" in error for error in result["errors"])
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


def test_explicit_artifact_root_takes_precedence(monkeypatch, tmp_path):
    monkeypatch.setenv("TRAFFIC_EXPERIMENT_ROOT", str(tmp_path))
    assert locate_experiment_root() == tmp_path.resolve()


@pytest.mark.asyncio
async def test_singleflight_independent_cancel_and_true_terminal_release():
    gate = threading.Event()
    services = [FakeService(gate), FakeService(gate)]
    runtime = runtime_for(services)
    try:
        await runtime.initialize()
        first, second = runtime.submit(request()), runtime.submit(request())
        await until(lambda: runtime.singleflight_hits == 1)
        physical = next(iter(runtime._flights.values()))
        runtime.cancel(first, "one consumer stopped")
        await asyncio.sleep(0.04)
        assert not physical.done()
        assert not runtime.workflows[first].done.done()
        assert len(runtime.snapshot()["reservations"]) == 2
        assert runtime.model_forward_count == 1
        assert runtime.snapshot()["active_futures"] == 1
        assert runtime.snapshot()["active_consumers"] == 2
        gate.set()
        one, two = await asyncio.gather(runtime.wait(first), runtime.wait(second))
        assert one["status"] == "cancelled" and two["status"] == "completed"
        assert sum(service.calls for service in services) == 1
        assert runtime.snapshot()["reservations"] == {}
        cached = await runtime.run(request())
        cached["predictions"]["flow"]["prediction"][0][0][0][0][0] = 999
        again = await runtime.run(request())
        assert again["predictions"]["flow"]["prediction"][0][0][0][0][0] == 2
        assert runtime.model_forward_count == 1 and runtime.cache_hits == 2
    finally:
        gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_http_waiter_and_shutdown_owner_cancel_safely_drain_all():
    gate = threading.Event()
    runtime = runtime_for([FakeService(gate), FakeService(gate)])
    await runtime.initialize()
    first = runtime.submit(request(frames=(1251,)))
    second = runtime.submit(request(frames=(1252,)))
    await until(lambda: runtime.model_forward_count == 2)
    waiter = asyncio.create_task(runtime.wait(first))
    await asyncio.sleep(0)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    shutdown_waiter = asyncio.create_task(runtime.shutdown())
    await until(lambda: runtime.closing)
    shutdown_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await shutdown_waiter
    await asyncio.sleep(0.03)
    assert len(runtime.snapshot()["reservations"]) == 2
    assert not runtime.workflows[first].done.done() and not runtime.workflows[second].done.done()
    gate.set()
    await runtime.shutdown()
    assert all(record.done.done() for record in runtime.workflows.values())
    assert not runtime.snapshot()["reservations"]
    assert runtime.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("drop_callbacks", [False, True])
async def test_cancelled_driver_shutdown_independently_drains_sync_and_queued_work(monkeypatch, drop_callbacks):
    gate = threading.Event()
    runtime = runtime_for([FakeService(gate), FakeService(gate)])
    try:
        await runtime.initialize()
        workflows = [runtime.submit(request(frames=(1251 + i,))) for i in range(3)]
        await until(lambda: runtime.model_forward_count == 2)
        physical = {work.future for work in runtime._works.values()}
        if drop_callbacks:
            original = runtime._loop.call_soon_threadsafe
            def drop_completion(callback, *args, **kwargs):
                if callback in (runtime._finish, runtime._flight_finished):
                    return None
                return original(callback, *args, **kwargs)
            monkeypatch.setattr(runtime._loop, "call_soon_threadsafe", drop_completion)
        runtime._driver.cancel()
        shutdown = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0.04)
        assert not shutdown.done(), "Cancelled driver must not terminate synchronous shutdown supervision"
        assert not runtime.closed and all(not future.done() for future in physical)
        assert len(runtime.snapshot()["reservations"]) == 2
        assert all(not runtime.workflows[workflow].done.done() for workflow in workflows[:2])
        gate.set()
        await shutdown
        results = await asyncio.gather(*(runtime.wait(workflow) for workflow in workflows))
        assert all(result["status"] == "cancelled" for result in results)
        assert runtime.model_forward_count == 2 and all(future.done() for future in physical)
        assert runtime.closed and not runtime.snapshot()["reservations"]
        assert runtime._driver.done()
        assert not runtime._works and not runtime._flights
    finally:
        gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_at", [1, 2])
async def test_cancelled_shared_initialization_owner_keeps_real_load_until_shutdown(cancel_at):
    gate, entered, finished = threading.Event(), threading.Event(), threading.Event()
    calls = []
    def backend(root):
        calls.append(True)
        if len(calls) == cancel_at:
            entered.set()
            assert gate.wait(5)
        result = fake_backend(FakeService())
        if len(calls) == cancel_at:
            finished.set()
        return result
    runtime = JointTrafficRuntime(backend_factory=backend)
    waiters = [asyncio.create_task(runtime.initialize()) for _ in range(2)]
    try:
        await until(entered.is_set)
        runtime._initializing.cancel()
        shutdown = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0.04)
        assert not shutdown.done(), "Cancelled initialization owner must not orphan its model-loading Future"
        assert not finished.is_set() and not runtime.closed and not runtime.initialized
        with pytest.raises(RuntimeError):
            runtime.submit(request())
        gate.set()
        await shutdown
        outcomes = await asyncio.gather(*waiters, return_exceptions=True)
        assert all(isinstance(value, asyncio.CancelledError) for value in outcomes)
        assert finished.is_set() and len(calls) == cancel_at
        assert runtime.closed and not runtime.backends and not runtime.workflows
        assert runtime.snapshot()["active_futures"] == 0
    finally:
        gate.set()
        await asyncio.gather(*waiters, return_exceptions=True)
        await runtime.shutdown()


@pytest.mark.parametrize("mode", ["running", "initializing"])
def test_all_async_tasks_cancel_keeps_sync_future_owners_until_real_end(mode):
    script = '''
import asyncio, runpy, threading, sys
helpers = runpy.run_path("tests/traffic/test_joint_runtime.py")
FakeService, runtime_for = helpers["FakeService"], helpers["runtime_for"]
request, until = helpers["request"], helpers["until"]
JointTrafficRuntime, fake_backend = helpers["JointTrafficRuntime"], helpers["fake_backend"]
async def main():
    gate, entered, finished = threading.Event(), threading.Event(), threading.Event()
    if sys.argv[1] == "running":
        runtime = runtime_for([FakeService(gate), FakeService(gate)])
        await runtime.initialize()
        workflows = [runtime.submit(request(frames=(1251 + i,))) for i in range(3)]
        await until(lambda: runtime.model_forward_count == 2)
        physical = {work.future for work in runtime._works.values()}
    else:
        def backend(root):
            entered.set()
            assert gate.wait(5)
            result = fake_backend(FakeService())
            finished.set()
            return result
        runtime = JointTrafficRuntime(backend_factory=backend)
        waiters = [asyncio.create_task(runtime.initialize()) for _ in range(2)]
        await until(entered.is_set)
        physical = set(runtime._pending_loads)
    shutdown = asyncio.create_task(runtime.shutdown())
    await asyncio.sleep(0)
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    for task in tasks:
        task.cancel()
    draining = asyncio.gather(*tasks, return_exceptions=True)
    await asyncio.sleep(0.04)
    assert not draining.done() and not runtime.closed
    assert all(not future.done() for future in physical)
    if sys.argv[1] == "running":
        assert len(runtime.snapshot()["reservations"]) == 2
    gate.set()
    await draining
    await runtime.shutdown()
    assert runtime.closed and all(future.done() for future in physical)
    assert not runtime._works and not runtime._flights and not runtime._pending_loads
    assert runtime.snapshot()["active_futures"] == 0
    if sys.argv[1] == "running":
        assert not runtime.snapshot()["reservations"]
        assert all(record.done.done() for record in runtime.workflows.values())
        assert runtime.model_forward_count == 2
    else:
        assert finished.is_set() and not runtime.initialized and not runtime.backends
asyncio.run(main())
'''
    result = subprocess.run([sys.executable, "-c", script, mode], capture_output=True, text=True, timeout=12)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.asyncio
async def test_failure_retries_alternate_instance_without_caching_failure():
    services = [FakeService(fail=True), FakeService()]
    runtime = runtime_for(services)
    try:
        result = await runtime.run(request())
        assert result["status"] == "completed"
        predict = next(task for task in result["tasks"].values() if task["capability_id"] == "traffic.predict")
        assert predict["attempt"] == 2
        assert len(predict["attempts"]) == 2
        assert runtime.model_forward_count == 2
        assert runtime.snapshot()["cache"]["entries"] == 1
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("source_error", ["execution", "quality"])
async def test_singleflight_source_failure_executes_follower_same_attempt_and_reservation(source_error):
    source_gate, follower_gate = threading.Event(), threading.Event()
    services = [FakeService(source_gate, fail=source_error == "execution"), FakeService(follower_gate)]
    if source_error == "quality":
        original = services[0].predict
        def invalid_predict(*args, **kwargs):
            output = original(*args, **kwargs)
            output["predictions"]["od"]["normalized"].flat[0] = np.nan
            return output
        services[0].predict = invalid_predict
    runtime = runtime_for(services)
    try:
        await runtime.initialize()
        source = runtime.submit(request(max_attempts=1, allowed_instances=["b1-1"]))
        await until(services[0].entered.is_set)
        follower = runtime.submit(request(max_attempts=1, allowed_instances=["b1-2"]))
        await until(lambda: runtime.singleflight_hits == 1)
        before = next(task for task in runtime.snapshot(follower)["tasks"].values()
                      if task["capability_id"] == "traffic.predict")
        stale = runtime._works[before["task_id"]]
        source_gate.set()
        await until(lambda: services[1].entered.is_set() or runtime.workflows[follower].done.done())
        assert services[1].entered.is_set(), "Source failure must not be attributed to an unexecuted follower"
        during = runtime.snapshot(follower)["tasks"][before["task_id"]]
        assert during["attempt"] == before["attempt"] == 1
        assert during["reservation_id"] == before["reservation_id"]
        assert during["schedule_status"] == "running" and not during["attempts"]
        assert runtime._cache_bytes == 0
        runtime._finish(stale)
        assert before["reservation_id"] in runtime.snapshot()["reservations"]
        follower_gate.set()
        failed, succeeded = await asyncio.gather(runtime.wait(source), runtime.wait(follower))
        assert failed["status"] == "failed" and succeeded["status"] == "completed"
        source_profile = next(p for p in failed["profiles"] if p["capability_id"] == "traffic.predict")
        assert (source_profile["call_status"], source_profile["quality_status"]) == (
            ("failed", "not_evaluated") if source_error == "execution" else ("success", "invalid"))
        task = succeeded["tasks"][before["task_id"]]
        assert task["attempt"] == 1 and len(task["attempts"]) == 1
        assert task["attempts"][0]["call_status"] == "success"
        assert task["tried_instances"] == ["b1-2"]
        profile = next(p for p in succeeded["profiles"] if p["capability_id"] == "traffic.predict")
        assert profile["instance_id"] == profile["physical_source_instance"] == "b1-2"
        assert profile["model_forward_count"] == 1 and profile["execution_origin"] == "singleflight_fallback"
        assert services[0].calls == services[1].calls == 1
        assert runtime.model_forward_count == 2
        assert runtime.instances["b1-2"]["last_error"] is None
        assert not runtime.snapshot()["reservations"]
        from src.service.traffic_management_bridge import TrafficManagementBridge
        from src.runtime.qos_monitor import QoSMonitor
        bridge = object.__new__(TrafficManagementBridge)
        bridge.qos = QoSMonitor()
        bridge._qos_recorded = set()
        bridge._records = {source: {"execution_result": failed}, follower: {"execution_result": succeeded}}
        for workflow in (source, follower):
            bridge._record_qos(workflow, runtime.snapshot(workflow))
        assert bridge.qos.get_metrics("b1-1").failure_count == 1
        assert bridge.qos.get_metrics("b1-2").failure_count == 0
        assert bridge.qos.get_metrics("b1-2").total_calls == len(succeeded["profiles"])
        cached = await runtime.run(request(max_attempts=1, allowed_instances=["b1-1"]))
        consumed = next(event for event in cached["prediction_executions"] if event["event"] == "prediction_finished")
        assert consumed["reuse"] == "cache" and consumed["instance_id"] == "b1-1"
        assert consumed["physical_source_instance"] == "b1-2" and consumed["model_forward_count"] == 0
        assert consumed["source_model_forward_count"] == 1
        assert runtime.model_forward_count == 2
    finally:
        source_gate.set()
        follower_gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("stop", ["cancel", "timeout", "shutdown"])
async def test_failed_singleflight_does_not_start_stopped_follower(stop):
    source_gate = threading.Event()
    services = [FakeService(source_gate, fail=True), FakeService()]
    runtime = runtime_for(services)
    shutdown = None
    try:
        await runtime.initialize()
        source = runtime.submit(request(max_attempts=1, allowed_instances=["b1-1"]))
        await until(services[0].entered.is_set)
        follower = runtime.submit(request(max_attempts=1, allowed_instances=["b1-2"],
                                          timeout_seconds=0.03 if stop == "timeout" else 30))
        await until(lambda: runtime.singleflight_hits == 1)
        if stop == "cancel":
            runtime.cancel(follower, "Consumer stopped before shared source failure")
        elif stop == "timeout":
            await until(lambda: any(task["timeout_requested"] for task in runtime.snapshot(follower)["tasks"].values()))
        else:
            shutdown = asyncio.create_task(runtime.shutdown())
            await until(lambda: runtime.snapshot(follower)["workflows"][follower]["cancel_requested"])
        before = next(task for task in runtime.snapshot(follower)["tasks"].values()
                      if task["capability_id"] == "traffic.predict")
        assert before["reservation_id"] in runtime.snapshot()["reservations"]
        assert not runtime.workflows[follower].done.done()
        source_gate.set()
        one, two = await asyncio.gather(runtime.wait(source), runtime.wait(follower))
        if shutdown is not None:
            await shutdown
        assert two["status"] == ("failed" if stop == "timeout" else "cancelled")
        assert services[1].calls == 0 and runtime.model_forward_count == 1
        assert runtime.singleflight_fallbacks == 0
        assert not any(p["capability_id"] == "traffic.predict" for p in two["profiles"])
        finished = next(event for event in two["prediction_executions"] if event["event"] == "prediction_finished")
        assert finished["physical_source_instance"] == "b1-1" and finished["model_forward_count"] == 0
        assert not runtime.snapshot()["reservations"]
    finally:
        source_gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_follower_own_failure_is_recorded_once_and_not_cached():
    source_gate = threading.Event()
    services = [FakeService(source_gate, fail=True), FakeService(fail=True)]
    runtime = runtime_for(services)
    try:
        await runtime.initialize()
        source = runtime.submit(request(max_attempts=1, allowed_instances=["b1-1"]))
        await until(services[0].entered.is_set)
        follower = runtime.submit(request(max_attempts=1, allowed_instances=["b1-2"]))
        await until(lambda: runtime.singleflight_hits == 1)
        source_gate.set()
        one, two = await asyncio.gather(runtime.wait(source), runtime.wait(follower))
        assert one["status"] == two["status"] == "failed"
        task = next(task for task in two["tasks"].values() if task["capability_id"] == "traffic.predict")
        assert task["attempt"] == 1 and len(task["attempts"]) == 1
        assert task["attempts"][0]["instance_id"] == "b1-2"
        profiles = [p for p in two["profiles"] if p["capability_id"] == "traffic.predict"]
        assert len(profiles) == 1 and profiles[0]["physical_source_instance"] == "b1-2"
        assert not profiles[0]["success"] and services[1].calls == 1
        assert runtime.model_forward_count == 2
        assert runtime.snapshot()["cache"]["entries"] == 0
        assert not runtime.snapshot()["reservations"]
    finally:
        source_gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_failed_follower_launch_is_confirmed_without_inventing_physical_failure(monkeypatch):
    source_gate = threading.Event()
    services = [FakeService(source_gate, fail=True), FakeService()]
    runtime = runtime_for(services)
    try:
        await runtime.initialize()
        source = runtime.submit(request(max_attempts=1, allowed_instances=["b1-1"]))
        await until(services[0].entered.is_set)
        follower = runtime.submit(request(max_attempts=1, allowed_instances=["b1-2"]))
        await until(lambda: runtime.singleflight_hits == 1)
        original = runtime._executor.submit
        def reject_launch(operation, *args, **kwargs):
            if args and getattr(args[0], "instance_id", None) == "b1-2":
                raise RuntimeError("Injected follower executor launch failure")
            return original(operation, *args, **kwargs)
        monkeypatch.setattr(runtime._executor, "submit", reject_launch)
        source_gate.set()
        one, two = await asyncio.gather(runtime.wait(source), runtime.wait(follower))
        task = next(task for task in two["tasks"].values() if task["capability_id"] == "traffic.predict")
        assert two["status"] == "failed" and task["attempts"][0]["launch_failed"]
        assert task["call_status"] == "failed" and task["quality_status"] == "not_evaluated"
        assert services[1].calls == 0 and runtime.model_forward_count == 1
        assert not any(p["capability_id"] == "traffic.predict" for p in two["profiles"])
        finished = next(event for event in two["prediction_executions"] if event["event"] == "prediction_finished")
        assert finished["physical_source_instance"] is None and finished["model_forward_count"] == 0
        assert runtime.snapshot()["cache"]["entries"] == 0
        assert not runtime.snapshot()["reservations"]
    finally:
        source_gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_source_can_recover_on_healthy_follower_without_duplicate_physical_call():
    source_gate = threading.Event()
    services = [FakeService(source_gate, fail=True), FakeService()]
    runtime = runtime_for(services)
    try:
        await runtime.initialize()
        source = runtime.submit(request(max_attempts=2))
        await until(services[0].entered.is_set)
        follower = runtime.submit(request(max_attempts=2, allowed_instances=["b1-2"]))
        await until(lambda: runtime.singleflight_hits == 1)
        source_gate.set()
        one, two = await asyncio.gather(runtime.wait(source), runtime.wait(follower))
        assert one["status"] == two["status"] == "completed"
        leader_task = next(task for task in one["tasks"].values() if task["capability_id"] == "traffic.predict")
        follower_task = next(task for task in two["tasks"].values() if task["capability_id"] == "traffic.predict")
        assert leader_task["attempt"] == 2 and leader_task["tried_instances"] == ["b1-1", "b1-2"]
        assert follower_task["attempt"] == 1 and follower_task["tried_instances"] == ["b1-2"]
        assert services[0].calls == services[1].calls == 1
        assert runtime.model_forward_count == 2 and runtime.cache_hits == 1
        assert not runtime.snapshot()["reservations"]
    finally:
        source_gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["raw_shape", "normalized_shape", "raw_nan", "normalized_inf", "units", "channels", "missing_metadata"])
@pytest.mark.parametrize("enable_contracts", [True, False])
async def test_invalid_unrequested_head_never_enters_cache_and_has_quality_status(invalid, enable_contracts):
    service = FakeService()
    original = service.predict
    corrupt = [True]
    def predict(*args, **kwargs):
        result = original(*args, **kwargs)
        if corrupt[0]:
            item = result["predictions"]["od"]
            if invalid == "raw_shape":
                item["prediction"] = item["prediction"][:, 0]
            elif invalid == "normalized_shape":
                item["normalized"] = item["normalized"][:, 0]
            elif invalid == "raw_nan":
                item["prediction"].flat[0] = np.nan
            elif invalid == "normalized_inf":
                item["normalized"].flat[0] = np.inf
            elif invalid == "units":
                item["units"] = ["mph"] * 36
            elif invalid == "channels":
                item["channels"] = list(reversed(item["channels"]))
            else:
                del item["units"]
        return result
    service.predict = predict
    runtime = runtime_for([service])
    try:
        # A flow-only consumer must not admit a corrupt, unrequested OD head.
        bad = await runtime.run(request(enable_recovery=False, enable_contracts=enable_contracts))
        assert bad["status"] == "failed"
        task = next(task for task in bad["tasks"].values() if task["capability_id"] == "traffic.predict")
        assert task["call_status"] == "success" and task["quality_status"] == "invalid"
        profile = next(p for p in bad["profiles"] if p["capability_id"] == "traffic.predict")
        assert profile["call_status"] == "success" and profile["quality_status"] == "invalid"
        assert runtime.model_forward_count == 1 and runtime.model_forward_successes == 0
        assert runtime.snapshot()["cache"]["entries"] == runtime.snapshot()["cache"]["bytes"] == 0
        assert not runtime.snapshot()["reservations"]
        corrupt[0] = False
        good = await runtime.run(request(enable_contracts=enable_contracts))
        assert good["status"] == "completed" and service.calls == 2
        assert runtime.snapshot()["cache"]["entries"] == 1
        validate = next(task for task in good["tasks"].values() if task["capability_id"] == "validate.flow")
        assert validate["call_status"] == "success" and validate["quality_status"] == "valid"
        reused = await runtime.run(request("od_hotspots", enable_contracts=enable_contracts))
        assert reused["status"] == "completed" and service.calls == 2 and runtime.cache_hits == 1
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_execution_exception_is_failed_and_quality_not_evaluated():
    runtime = runtime_for([FakeService(fail=True)])
    try:
        result = await runtime.run(request(enable_recovery=False))
        task = next(task for task in result["tasks"].values() if task["capability_id"] == "traffic.predict")
        assert task["call_status"] == "failed" and task["quality_status"] == "not_evaluated"
        profile = next(p for p in result["profiles"] if p["capability_id"] == "traffic.predict")
        assert profile["call_status"] == "failed" and profile["quality_status"] == "not_evaluated"
        assert runtime.snapshot()["cache"]["entries"] == 0
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_recovery_and_reuse_ablations_and_lru_byte_bound():
    runtime = runtime_for([FakeService(fail=True), FakeService(fail=True)], cache_max_bytes=1)
    try:
        result = await runtime.run(request(enable_recovery=False))
        assert result["status"] == "failed" and runtime.model_forward_count == 1
        assert runtime.snapshot()["cache"]["bytes"] == 0
    finally:
        await runtime.shutdown()
    runtime = runtime_for([FakeService(), FakeService()], cache_max_entries=1)
    try:
        for frame in (1251, 1252, 1251):
            await runtime.run(request(frames=(frame,)))
        assert runtime.model_forward_count == 3
        assert runtime.snapshot()["cache"]["entries"] == 1
        await runtime.run(request(frames=(1251,), enable_reuse=False))
        assert runtime.model_forward_count == 4
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_node_timeout_keeps_real_work_reserved_until_end():
    gate = threading.Event()
    runtime = runtime_for([FakeService(gate), FakeService(gate)])
    try:
        await runtime.initialize()
        plan_compile = runtime.compiler.compile
        def compile_with_timeout(*args, **kwargs):
            plan = plan_compile(*args, **kwargs)
            return replace(plan, nodes=tuple(replace(node, timeout_seconds=0.03) if node.capability_id == "traffic.predict" else node
                                             for node in plan.nodes))
        runtime.compiler.compile = compile_with_timeout
        workflow = runtime.submit(request(enable_recovery=False))
        await until(lambda: runtime.model_forward_count == 1)
        await asyncio.sleep(0.08)
        task = next(task for task in runtime.snapshot(workflow)["tasks"].values() if task["capability_id"] == "traffic.predict")
        assert task["timeout_requested"] and task["schedule_status"] == "running"
        assert runtime.snapshot()["reservations"]
        gate.set()
        result = await runtime.wait(workflow)
        assert result["status"] == "failed"
        assert not runtime.snapshot()["reservations"]
    finally:
        gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_old_and_duplicate_completion_cannot_release_another_attempt():
    gate = threading.Event()
    runtime = runtime_for([FakeService(gate), FakeService(gate)])
    try:
        await runtime.initialize()
        workflow = runtime.submit(request())
        await until(lambda: runtime.model_forward_count == 1)
        work = next(work for work in runtime._works.values() if work.assignment.candidate.capability_id == "traffic.predict")
        stale = copy.copy(work)
        stale.assignment = replace(work.assignment, attempt=work.assignment.attempt + 1, reservation_id="old")
        gate.set()
        await runtime.wait(workflow)
        count = len(runtime.snapshot()["resource_events"])
        runtime._finish(stale)
        runtime._finish(work)
        assert len(runtime.snapshot()["resource_events"]) == count
    finally:
        gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_late_completion_during_real_retry_keeps_new_reservation():
    first_gate, second_gate = threading.Event(), threading.Event()
    runtime = runtime_for([FakeService(first_gate, fail=True), FakeService(second_gate)])
    try:
        await runtime.initialize()
        workflow = runtime.submit(request())
        await until(lambda: runtime.model_forward_count == 1)
        stale = copy.copy(next(work for work in runtime._works.values()
                               if work.assignment.candidate.capability_id == "traffic.predict"))
        first_gate.set()
        await until(lambda: runtime.model_forward_count == 2)
        before = runtime.snapshot()["reservations"]
        runtime._finish(stale)
        assert runtime.snapshot()["reservations"] == before
        second_gate.set()
        assert (await runtime.wait(workflow))["status"] == "completed"
    finally:
        first_gate.set()
        second_gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_deadline_is_soft_and_does_not_cancel_running_flow():
    gate = threading.Event()
    runtime = runtime_for([FakeService(gate), FakeService(gate)])
    try:
        await runtime.initialize()
        workflow = runtime.submit(request(deadline_seconds=0.03))
        await until(lambda: runtime.model_forward_count == 1)
        await asyncio.sleep(0.07)
        state = runtime.snapshot(workflow)["workflows"][workflow]
        assert state["deadline_missed"] and state["status"] == "running"
        assert runtime.snapshot()["reservations"]
        gate.set()
        result = await runtime.wait(workflow)
        assert result["status"] == "completed" and result["deadline_missed"]
    finally:
        gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_registered_rule_plugin_executes_on_custom_template():
    runtime = runtime_for([FakeService(), FakeService()])
    seen = []
    try:
        await runtime.initialize()
        catalog = runtime.catalog.snapshot()
        common = catalog["implementations"]["B1"]["contract"]
        runtime.catalog.register_implementation(dict(
            implementation_id="rule-plugin", version=runtime.instances["b1-1"]["version"], contract=common,
            capabilities={"rule.flow_total": dict(resources=dict(cpu=0.1, memory_mb=4),
                                                   profile=dict(expected_seconds=0.001),
                                                   contract=dict(inputs={}, outputs={}))}))
        runtime.catalog.register_instance(dict(instance_id="rule-1", implementation_id="rule-plugin",
                                               endpoint="local://rule-1", max_concurrency=1, base_memory_mb=0))
        def rule(inputs, parameters, context):
            seen.append(context)
            assert set(inputs) == {"flow"}
            values = inputs["flow"]["prediction"]
            return {"analysis": {"total_predicted_dropoffs": float(values[..., 0].sum())}}
        runtime.register_operation("rule.flow_total", rule)
        runtime.compiler.register_template("flow_with_total", dict(requested_outputs=["flow"], nodes=[
            dict(node_id="prepare", capability_id="history.prepare", depends_on=[], parameters={"inputs": {}}),
            dict(node_id="predict", capability_id="traffic.predict", depends_on=["prepare"],
                 parameters={"inputs": {"model_inputs": {"node_id": "prepare", "output": "inputs"}}}),
            dict(node_id="validate", capability_id="validate.flow", depends_on=["predict"],
                 parameters={"inputs": {"prediction": {"node_id": "predict", "output": "flow"}}}),
            dict(node_id="rule", capability_id="rule.flow_total", depends_on=["validate"], outputs=["analysis"],
                 parameters={"inputs": {"flow": {"node_id": "validate", "output": "flow"}}}),
            dict(node_id="aggregate", capability_id="result.aggregate", depends_on=["validate", "rule"],
                 parameters={"inputs": {"flow": {"node_id": "validate", "output": "flow"},
                                        "analysis": {"node_id": "rule", "output": "analysis"}}})]))
        result = await runtime.run(request("flow_with_total"))
        assert result["status"] == "completed"
        assert result["analysis"]["flow_with_total"]["total_predicted_dropoffs"] == 144
        task = next(task for task in result["tasks"].values() if task["capability_id"] == "rule.flow_total")
        assert task["instance_id"] == "rule-1" and task["call_status"] == "success"
        assert seen[0]["workflow_id"] == result["workflow_id"]
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


def register_binding_plugins(runtime, source_schemas, target_schemas, producer, consumer, *, bundle=False):
    common = runtime.catalog.snapshot()["implementations"]["B1"]["contract"]
    version = runtime.instances["b1-1"]["version"]
    source_output = "inputs" if bundle else "state"
    consumer_input = "model_inputs" if bundle else "prediction"
    source_contract = {"inputs": {}, "outputs": {source_output: source_schemas}}
    target_contract = {"inputs": {consumer_input: target_schemas},
                       "outputs": {"state": target_schemas["state"] if bundle else target_schemas}}
    for implementation, capability, contract in (
            ("binding-source", "plugin.binding_source", source_contract),
            ("binding-consumer", "plugin.binding_consumer", target_contract)):
        runtime.catalog.register_implementation(dict(
            implementation_id=implementation, version=version, contract=common,
            capabilities={capability: dict(resources=dict(cpu=0.1, memory_mb=8),
                                          profile=dict(expected_seconds=0.02), contract=contract)}))
    alternate = copy.deepcopy(source_contract)
    alternate_schema = alternate["outputs"][source_output]["state"] if bundle else alternate["outputs"][source_output]
    alternate_schema["units"][1] = "km/h"
    runtime.catalog.register_implementation(dict(
        implementation_id="binding-source-alternate", version=version, contract=common,
        capabilities={"plugin.binding_source": dict(resources=dict(cpu=0.1, memory_mb=8),
                                                     profile=dict(expected_seconds=0.02), contract=alternate)}))
    for instance, implementation in (("source-1", "binding-source-alternate"), ("source-2", "binding-source"),
                                     ("consumer-1", "binding-consumer")):
        runtime.catalog.register_instance(dict(instance_id=instance, implementation_id=implementation,
                                               endpoint="local://" + instance, max_concurrency=1, base_memory_mb=0))
    runtime.catalog.record_execution("source-2", "plugin.binding_source", 0.00001, True)
    runtime.register_operation("plugin.binding_source", producer)
    runtime.register_operation("plugin.binding_consumer", consumer)
    runtime.compiler.register_template("binding_adapter", dict(requested_outputs=["state"], nodes=[
        dict(node_id="source", capability_id="plugin.binding_source", outputs=[source_output],
             parameters={"inputs": {}}),
        dict(node_id="consumer", capability_id="plugin.binding_consumer", depends_on=["source"], outputs=["state"],
             parameters={"inputs": {consumer_input: {"node_id": "source", "output": source_output}}}),
        dict(node_id="aggregate", capability_id="result.aggregate", depends_on=["consumer"],
             parameters={"inputs": {"state": {"node_id": "consumer", "output": "state"}}})]))


@pytest.mark.asyncio
@pytest.mark.parametrize("payload", [False, True])
async def test_plugin_binding_really_converts_axes_and_speed_units_without_mutating_source(payload):
    from src.service import traffic_contracts as c
    runtime = runtime_for([FakeService(), FakeService()])
    source = np.arange(1, 145, dtype=np.float32).reshape(2, 1, 6, 6, 2)
    original = source.copy()
    seen = []
    try:
        await runtime.initialize()
        backend = runtime.backends["b1-1"]
        stats_before = {name: item.to_dict() for name, item in backend["stats"].items()}
        raw = backend["outputs"]["state"]
        target = replace(raw, axes=("B", "H", "C", "Y", "X"), shape=(None, 1, 2, 6, 6),
                         units=("active trips", "m/s"))
        source_value = c.TensorPayload(source, raw, "registered-plugin-source") if payload else source
        def producer(inputs, parameters, context):
            assert context["instance_id"] == "source-2"
            return {"state": source_value}
        def consumer(inputs, parameters, context):
            value = inputs["prediction"]
            if payload:
                assert value.schema == target and value.source == "registered-plugin-source"
                value = value.data
            assert not value.flags.writeable
            seen.append(value.copy())
            return {"state": value}
        register_binding_plugins(runtime, raw.to_dict(), target.to_dict(), producer, consumer)
        result = await runtime.run(request("binding_adapter", enable_reuse=False))
        assert result["status"] == "completed", result["errors"]
        expected = original.transpose(0, 1, 4, 2, 3).astype(np.float64)
        expected *= np.asarray([1.0, 0.44704]).reshape(1, 1, 2, 1, 1)
        np.testing.assert_allclose(seen[0], expected, rtol=1e-12, atol=1e-12)
        np.testing.assert_allclose(result["predictions"]["state"], expected, rtol=1e-12, atol=1e-12)
        np.testing.assert_array_equal(source, original)
        assert source.flags.writeable
        assert {name: item.to_dict() for name, item in backend["stats"].items()} == stats_before
        trace = result["schema_trace"]["consumer"]["bindings"]["prediction"]
        assert trace["source_instance"] == "source-2" and trace["target_instance"] == "consumer-1"
        assert {rule["rule"] for rule in trace["fields"]["state"]} >= {"transpose", "unit_scale"}
        assert runtime.model_forward_count == 0 and not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


def backend_with_training_stats(service, stats_path):
    backend = fake_backend(service)
    fitted = {name: (np.asarray(item.mean, dtype=np.float32), np.asarray(item.std, dtype=np.float32))
              for name, item in backend["stats"].items()}
    fitted["state"] = (np.asarray([2.0, 10.0], np.float32), np.asarray([3.0, 4.0], np.float32))
    if not stats_path.exists():
        np.savez(stats_path, **{name + suffix: value for name, pair in fitted.items()
                              for suffix, value in zip(("_mean", "_std"), pair)})
    service.normalization_path = stats_path
    return build_backend(service, metadata=backend["metadata"], stats=fitted,
                         normalization_sha256=hashlib.sha256(stats_path.read_bytes()).hexdigest())


@pytest.mark.asyncio
async def test_prediction_binding_converts_raw_and_normalized_with_original_train_stats(tmp_path):
    from src.service import traffic_contracts as c
    stats_path = tmp_path / "normalization_train_only.npz"
    runtime = JointTrafficRuntime(backend_factory=lambda root: backend_with_training_stats(FakeService(), stats_path))
    try:
        await runtime.initialize()
        backend = runtime.backends["b1-1"]
        train = backend["stats"]["state"]
        train_before, file_before = train.to_dict(), stats_path.read_bytes()
        z = np.arange(144, dtype=np.float32).reshape(2, 1, 6, 6, 2) / 16
        raw_values = z.astype(np.float64) * np.asarray(train.std) + np.asarray(train.mean)
        original_raw, original_z = raw_values.copy(), z.copy()
        source_schema = backend["outputs"]["state"]
        target_schema = replace(source_schema, axes=("B", "H", "C", "Y", "X"),
                                shape=(None, 1, 2, 6, 6), units=("active trips", "m/s"))
        prediction = dict(prediction=raw_values, normalized=z, shape=list(raw_values.shape), layout="B,H,Y,X,C",
                          units=list(source_schema.units), channels=backend["metadata"]["state_channels"])
        seen = []
        def consumer(inputs, parameters, context):
            item = inputs["prediction"]
            assert item["layout"] == "B,H,C,Y,X" and item["units"] == ["active trips", "m/s"]
            assert item["shape"] == [2, 1, 2, 6, 6]
            assert not item["prediction"].flags.writeable and not item["normalized"].flags.writeable
            seen.append(copy.deepcopy(item))
            return {"state": item}
        register_binding_plugins(runtime, source_schema.to_dict(), target_schema.to_dict(),
                                 lambda *args: {"state": prediction}, consumer)
        result = await runtime.run(request("binding_adapter", enable_reuse=False))
        assert result["status"] == "completed", result["errors"]
        expected_raw = original_raw.transpose(0, 1, 4, 2, 3) * np.asarray([1.0, 0.44704]).reshape(1, 1, 2, 1, 1)
        np.testing.assert_allclose(seen[0]["prediction"], expected_raw, rtol=1e-12, atol=1e-12)
        np.testing.assert_allclose(seen[0]["normalized"], original_z.transpose(0, 1, 4, 2, 3), rtol=1e-12, atol=1e-12)
        converted = seen[0]["normalization_statistics"]
        assert converted["source"] == train.source and converted["split"] == "train"
        np.testing.assert_allclose(converted["mean"], [2.0, 10.0 * 0.44704], rtol=1e-12)
        np.testing.assert_allclose(converted["std"], [3.0, 4.0 * 0.44704], rtol=1e-12)
        assert train.to_dict() == train_before and stats_path.read_bytes() == file_before
        np.testing.assert_array_equal(raw_values, original_raw)
        np.testing.assert_array_equal(z, original_z)
        trace = result["schema_trace"]["consumer"]["bindings"]["prediction"]["fields"]["state"]
        assert {item["rule"] for item in trace["normalized"]} >= {
            "transpose", "training_stats_unit_conversion", "denormalize", "unit_scale", "standardize"}
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_model_inputs_bundle_binding_really_normalizes_and_transposes(tmp_path):
    from src.service import traffic_contracts as c
    stats_path = tmp_path / "normalization_train_only.npz"
    runtime = JointTrafficRuntime(backend_factory=lambda root: backend_with_training_stats(FakeService(), stats_path))
    try:
        await runtime.initialize()
        backend = runtime.backends["b1-1"]
        file_before = stats_path.read_bytes()
        stats_before = {name: item.to_dict() for name, item in backend["stats"].items()}
        raw = {name: np.full((2, 3, 6, 6, 36 if name == "od" else 2), 18.0, np.float32)
               for name in ("state", "flow", "od")}
        source_before = {name: values.copy() for name, values in raw.items()}
        bundle = {name: c.TensorPayload(values, backend["storage"][name], "plugin-history-source")
                  for name, values in raw.items()}
        bundle["target_frames"] = (1251, 1252)
        targets = dict(backend["inputs"])
        targets["state"] = replace(targets["state"], units=("active trips", "m/s"))
        seen = []
        def consumer(inputs, parameters, context):
            model_inputs = inputs["model_inputs"]
            assert model_inputs["target_frames"] == (1251, 1252)
            assert all(isinstance(model_inputs[name], np.ndarray) and not model_inputs[name].flags.writeable
                       for name in ("state", "flow", "od"))
            seen.append({name: model_inputs[name].copy() for name in ("state", "flow", "od")})
            return {"state": model_inputs["state"]}
        register_binding_plugins(runtime, {name: schema.to_dict() for name, schema in backend["storage"].items()},
                                 {name: schema.to_dict() for name, schema in targets.items()},
                                 lambda *args: {"inputs": bundle}, consumer, bundle=True)
        result = await runtime.run(request("binding_adapter", enable_reuse=False))
        assert result["status"] == "completed", result["errors"]
        for name in ("state", "flow", "od"):
            train = backend["stats"][name]
            expected = (source_before[name].astype(np.float64) - np.asarray(train.mean)) / np.asarray(train.std)
            if name != "od":
                expected = expected.transpose(0, 1, 4, 2, 3)
            np.testing.assert_allclose(seen[0][name], expected, rtol=1e-12, atol=1e-12)
            np.testing.assert_array_equal(raw[name], source_before[name])
        assert stats_path.read_bytes() == file_before
        assert {name: item.to_dict() for name, item in backend["stats"].items()} == stats_before
        trace = result["schema_trace"]["consumer"]["bindings"]["model_inputs"]["fields"]
        assert {rule["rule"] for rule in trace["state"]} >= {"transpose", "unit_scale", "standardize"}
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["unsupported_value", "missing_schemas", "empty_schemas"])
async def test_planned_binding_adapter_cannot_be_silently_ignored(monkeypatch, fault):
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        await runtime.initialize()
        source = runtime.backends["b1-1"]["outputs"]["state"]
        target = replace(source, axes=("B", "H", "C", "Y", "X"), shape=(None, 1, 2, 6, 6),
                         units=("active trips", "m/s"))
        values = "unsupported tensor representation" if fault == "unsupported_value" else np.ones((2, 1, 6, 6, 2), np.float32)
        reached = []
        def consumer(inputs, parameters, context):
            reached.append(True)
            return {"state": inputs["prediction"]}
        register_binding_plugins(runtime, source.to_dict(), target.to_dict(),
                                 lambda *args: {"state": values}, consumer)
        original_compile = runtime.compiler.compile
        def compile_with_broken_schema(*args, **kwargs):
            plan = original_compile(*args, **kwargs)
            if fault == "unsupported_value":
                return plan
            nodes = []
            for node in plan.nodes:
                if node.node_id != "consumer":
                    nodes.append(node)
                    continue
                parameters = dict(node.parameters)
                if fault == "missing_schemas":
                    parameters.pop("candidate_adapter_schemas")
                else:
                    parameters["candidate_adapter_schemas"] = {
                        "consumer-1": {"prediction": {"source-1": {}, "source-2": {}}}}
                nodes.append(replace(node, parameters=parameters))
            return replace(plan, nodes=tuple(nodes))
        monkeypatch.setattr(runtime.compiler, "compile", compile_with_broken_schema)
        result = await runtime.run(request("binding_adapter", enable_recovery=False))
        assert result["status"] == "failed" and not reached
        task = next(task for task in result["tasks"].values() if task["node_id"] == "consumer")
        assert task["call_status"] == "success" and task["quality_status"] == "invalid"
        assert any("binding:" in error for error in result["errors"])
        assert not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_missing_assets_fail_without_fallback_or_partial_submission(tmp_path):
    runtime = JointTrafficRuntime(tmp_path)
    try:
        with pytest.raises(FileNotFoundError):
            await runtime.initialize()
        assert not runtime.initialized and not runtime.workflows and not runtime.backends
        with pytest.raises(RuntimeError):
            runtime.submit(request())
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_cancel_initialization_waiter_does_not_interrupt_actual_load():
    gate, entered = threading.Event(), threading.Event()
    def backend(root):
        entered.set()
        assert gate.wait(5)
        return fake_backend(FakeService())
    runtime = JointTrafficRuntime(backend_factory=backend)
    waiter = asyncio.create_task(runtime.initialize())
    await until(entered.is_set)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    shutdown = asyncio.create_task(runtime.shutdown())
    await asyncio.sleep(0.03)
    assert not shutdown.done()
    gate.set()
    await shutdown
    assert runtime.closed and len(runtime.instances) == 2


@pytest.mark.asyncio
async def test_singleflight_consumers_keep_independent_output_validation(monkeypatch):
    gate = threading.Event()
    runtime = runtime_for([FakeService(gate), FakeService(gate)])
    validated = []
    original = runtime._validate
    def validate(backend, record, name, item):
        validated.append(record.plan.app_id)
        if record.plan.app_id == "rejected-consumer":
            raise ValueError("Consumer contract rejects this output")
        return original(backend, record, name, item)
    monkeypatch.setattr(runtime, "_validate", validate)
    try:
        await runtime.initialize()
        rejected = runtime.submit(request(enable_recovery=False), "rejected-consumer")
        accepted = runtime.submit(request(), "accepted-consumer")
        await until(lambda: runtime.singleflight_hits == 1)
        gate.set()
        one, two = await asyncio.gather(runtime.wait(rejected), runtime.wait(accepted))
        assert one["status"] == "failed" and two["status"] == "completed"
        assert set(validated) == {"rejected-consumer", "accepted-consumer"}
        assert runtime.model_forward_count == 1
        assert not runtime.snapshot()["reservations"]
    finally:
        gate.set()
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_contract_ablation_preserves_actual_preparation(monkeypatch):
    runtime = runtime_for([FakeService(), FakeService()])
    try:
        strict = await runtime.run(request())
        ablated = await runtime.run(request(enable_contracts=False, enable_reuse=False))
        assert strict["predictions"] == ablated["predictions"]
        assert strict["schema_trace"]["prepare"] == ablated["schema_trace"]["prepare"]
        assert runtime.model_forward_count == 2
    finally:
        await runtime.shutdown()


@pytest.mark.asyncio
async def test_real_two_B1_instances_match_archived_eight_independent_windows(monkeypatch):
    if not ROOT.is_dir():
        if os.environ.get("TRAFFIC_EXPERIMENT_ROOT"):
            pytest.fail("Explicit TRAFFIC_EXPERIMENT_ROOT does not contain real B1 artifacts")
        pytest.skip("Real B1 assets are unavailable on this host")
    files_before = {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in ROOT.rglob("*") if p.is_file()}
    monkeypatch.setenv("TRAFFIC_EXPERIMENT_ROOT", str(ROOT))
    with np.load(ROOT / "results/B1_seed42_L3/test_predictions.npz", allow_pickle=False) as artifact:
        frames = artifact["target_frames"][:8].astype(int).tolist()
        reference = {name: artifact["pred_" + name][:8].copy() for name in ("state", "flow", "od")}
    runtime = JointTrafficRuntime(enable_reuse=False)
    try:
        results = await asyncio.gather(runtime.run(request("analysis", frames=frames)),
                                       runtime.run(request("od_hotspots", frames=frames)))
        assert len(runtime.instances) == 2
        assert len({id(b["service"]._model) for b in runtime.backends.values()}) == 2
        for result in results:
            assert result["status"] == "completed" and result["history_minutes"] == 90
            for name, item in result["predictions"].items():
                actual = np.asarray(item["prediction"], dtype=np.float32)
                expected = reference[name]
                expected = expected[:, None]
                np.testing.assert_allclose(actual, expected, rtol=1e-5, atol=1e-5)
                assert actual.shape[:2] == (8, 1)
        snapshot = runtime.snapshot()
        assert snapshot["model_forward_count"] == 2
        assert all(instance["model_forward_count"] == 1 for instance in snapshot["instances"].values())
        assert snapshot["resources"]["measurement"]["source"] == "psutil"
        assert not snapshot["reservations"]
        fired = []
        for backend in runtime.backends.values():
            original = backend["service"].predict
            def fail_before_forward(*args, original=original, **kwargs):
                if not fired:
                    fired.append(True)
                    raise RuntimeError("Injected service failure before a torch forward")
                return original(*args, **kwargs)
            monkeypatch.setattr(backend["service"], "predict", fail_before_forward)
        recovered = await runtime.run(request("flow", frames=frames))
        assert recovered["status"] == "completed"
        assert runtime.model_forward_count == 3
        assert runtime.model_forward_successes == 3
    finally:
        await runtime.shutdown()
    assert {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in ROOT.rglob("*") if p.is_file()} == files_before


@pytest.mark.asyncio
async def test_real_B1_template_without_aggregate_keeps_complete_flow_result():
    if not ROOT.is_dir():
        if os.environ.get("TRAFFIC_EXPERIMENT_ROOT"):
            pytest.fail("Explicit TRAFFIC_EXPERIMENT_ROOT does not contain real B1 artifacts")
        pytest.skip("Real B1 assets are unavailable on this host")
    files_before = {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in ROOT.rglob("*") if p.is_file()}
    with np.load(ROOT / "results/B1_seed42_L3/test_predictions.npz", allow_pickle=False) as artifact:
        frames = artifact["target_frames"][:8].astype(int).tolist()
        raw = artifact["pred_flow"][:8, None].copy()
        normalized = artifact["normalized_flow"][:8, None].copy()
    runtime = JointTrafficRuntime(ROOT)
    try:
        await runtime.initialize()
        register_no_aggregate_template(runtime)
        result = await runtime.run(request("no_aggregate", frames=frames))
        assert result["status"] == "completed" and set(result["predictions"]) == {"flow"}
        item = result["predictions"]["flow"]
        assert item["shape"] == [8, 1, 6, 6, 2] and item["layout"] == "B,H,Y,X,C"
        np.testing.assert_allclose(item["prediction"], raw, rtol=1e-5, atol=1e-5)
        np.testing.assert_allclose(item["normalized"], normalized, rtol=1e-5, atol=1e-5)
        assert item["units"] == ["trips/30min", "trips/30min"]
        assert item["channels"] == ["dropoffs_in_interval", "pickups_in_interval"]
        assert not any(task["capability_id"] == "result.aggregate" for task in result["tasks"].values())
        assert runtime.snapshot(result["workflow_id"])["result"]["predictions"] == result["predictions"]
        assert not runtime.workflows[result["workflow_id"]].values
        assert runtime.model_forward_count == 1 and not runtime.snapshot()["reservations"]
    finally:
        await runtime.shutdown()
    assert {str(p): (p.stat().st_size, p.stat().st_mtime_ns) for p in ROOT.rglob("*") if p.is_file()} == files_before

