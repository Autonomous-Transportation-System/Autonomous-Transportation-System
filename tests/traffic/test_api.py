"""Real B1 HTTP contract, failure isolation and owner/worker lifecycle checks."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import subprocess
import sys
import threading
from dataclasses import replace
from pathlib import Path

import httpx
import numpy as np
import pytest
from fastapi import FastAPI, HTTPException

from src.api import traffic
from src.service.traffic_model import TrafficModelService
from src.protocols.a2a_protocol import A2AMessage, A2ATaskRequest, A2ATaskResponse
from src.service.a2a_client import A2AClient
from traffic_server import app


REQUEST = dict(data_ref=traffic.DATA_REF, target_frames=[1251], task="all")
TASK_IDS = {"prepare", "predict", "validate_state", "validate_flow", "validate_od", "aggregate"}


@pytest.fixture(scope="module")
def service():
    return TrafficModelService()


@pytest.fixture
async def runtime(monkeypatch, service):
    value = traffic.TrafficRuntime(service)
    monkeypatch.setattr(traffic, "_runtime", value)
    yield value
    await value.shutdown()


@pytest.fixture
async def client(runtime):
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),
                                 base_url="http://traffic.test") as value:
        yield value
    pending = list(runtime.owners.values())
    if pending:
        await asyncio.gather(*pending)


def assert_reclaimed(data):
    assert data["done"] is True
    assert data["resources"]["cpu_used"] == 0
    assert data["resources"]["memory_used_mb"] == 0
    assert data["resources"]["concurrency"] == 0
    assert data["reservations"] == {}
    reserved = [event for event in data["resource_events"] if event["event"] == "reserve"]
    released = [event for event in data["resource_events"] if event["event"] == "release"]
    assert {event["reservation_id"] for event in reserved} == {
        event["reservation_id"] for event in released}
    assert len(reserved) == len(released)
    assert all(event["execution_finished"] for event in released)
    assert data["runtime"]["active_executions"] == 0


def assert_no_nested_predictions(data):
    for state in data["tasks"].values():
        result = state["result"]
        if result is not None:
            assert set(result) <= {"stage", "shape"}


def test_standalone_import_does_not_load_torch_or_llm():
    root = Path(__file__).resolve().parents[2]
    code = """
import json, sys
import traffic_server
print(json.dumps([name for name in sys.modules if name == 'torch' or name.startswith(('langchain', 'langgraph', 'src.graph.builder'))]))
assert '/traffic/predict' in traffic_server.app.openapi()['paths']
"""
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(root))
    result = subprocess.run([sys.executable, "-c", code], cwd=root, env=environment,
                            capture_output=True, text=True, check=True)
    assert json.loads(result.stdout) == []


@pytest.mark.asyncio
async def test_capabilities_initializes_real_model(client, runtime, service):
    response = await client.get("/traffic/capabilities")
    assert response.status_code == 200
    data = response.json()
    assert runtime.backend is not None
    assert data["version"] == service.version
    assert data["checkpoint_sha256"] == service.checkpoint_sha256
    assert data["data_refs"] == [traffic.DATA_REF]
    assert data["output_layout"] == "B,H,Y,X,C"
    assert data["max_batch"] == 32
    assert data["budgets"]["task_memory_budget"] == 1536
    assert data["normalization"]["split"] == "train"
    path = service.experiment_root / "data/processed/normalization_train_only.npz"
    assert data["normalization"]["stats_sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    contract = next(item for item in data["capabilities"] if item["name"] == "B1")
    assert contract["inputs"]["state"]["axes"] == ["B", "L", "C", "Y", "X"]
    assert contract["inputs"]["od"]["axes"] == ["B", "L", "Y", "X", "D"]
    for name in traffic.TASKS:
        capability = next(item for item in data["capabilities"] if item["name"] == name)
        assert set(capability["inputs"]) == set(traffic.TASKS)
        assert set(capability["outputs"]) == {name}


@pytest.mark.asyncio
@pytest.mark.parametrize("endpoint", ["/traffic/workflows/run", "/traffic/predict"])
async def test_real_B1_http_matches_saved_predictions(client, service, endpoint):
    response = await client.post(endpoint, json=REQUEST)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success", data["error"]
    assert data["version"] == service.version
    assert set(data["tasks"]) == TASK_IDS
    assert all(state["successful"] for state in data["tasks"].values())
    assert data["base_resources"]["memory_mb"] == 512
    assert data["budgets"] == dict(cpu_budget=4, memory_budget=2048,
                                   task_memory_budget=1536, max_concurrency=4)
    assert data["resources"]["memory_budget"] == 1536
    assert data["peaks"]["cpu_used"] <= 4
    assert data["peaks"]["memory_used_mb"] <= 1536
    assert data["peaks"]["concurrency"] == 3
    assert data["time_range"][0]["start_frame"] == 1248
    assert data["time_range"][0]["end_frame_exclusive"] == 1251
    assert data["target_time_range"][0]["start_frame"] == 1251
    assert data["target_time_range"][0]["end_frame_exclusive"] == 1252
    assert data["time_range"][0]["start"] == "2024-01-27T00:00:00"
    assert data["target_time_range"][0]["end_exclusive"] == "2024-01-27T02:00:00"
    assert data["target_time_range"][0]["grid_version"] == traffic.GRID_ID
    artifact = service.experiment_root / "results/B1_seed42_L3/test_predictions.npz"
    with np.load(artifact, allow_pickle=False) as saved:
        index = np.flatnonzero(saved["target_frames"] == 1251)[0]
        for name, item in data["predictions"].items():
            assert item["layout"] == "B,H,Y,X,C"
            assert item["shape"] == [1, 1, 6, 6, 36 if name == "od" else 2]
            np.testing.assert_allclose(np.asarray(item["prediction"])[0],
                                       saved["pred_" + name][index][None], rtol=1e-5, atol=1e-5)
            rules = [step["rule"] for step in data["schema_trace"][name]]
            assert rules == (["validate", "standardize", "validate"] if name == "od"
                             else ["validate", "transpose", "standardize", "validate"])
    assert_reclaimed(data)
    assert_no_nested_predictions(data)
    loaded = await client.get("/traffic/workflows/" + data["workflow_id"])
    assert loaded.status_code == 200
    assert loaded.json()["predictions"] == data["predictions"]


@pytest.mark.asyncio
@pytest.mark.parametrize("task", ["state", "flow", "od"])
async def test_selector_preserves_six_stage_contract(client, task):
    response = await client.post("/traffic/predict", json=dict(REQUEST, task=task))
    data = response.json()
    assert response.status_code == 200
    assert data["status"] == "success"
    assert data["task"] == task
    assert set(data["predictions"]) == {task}
    assert set(data["tasks"]) == TASK_IDS
    assert_reclaimed(data)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [
    {"data_ref": "/etc/passwd"}, {"data_ref": "../nyc_tlc_green_2024_01_6x6"},
    {"target_frames": []}, {"target_frames": [1251] * 33},
    {"target_frames": [True]}, {"target_frames": [1251.0]},
    {"target_frames": ["1251"]}, {"target_frames": [2]},
    {"target_frames": [1488]}, {"target_frames": [48]},
    {"target_frames": [10**100]}, {"task": "unknown"},
    {"horizon": 2}, {"horizon": True}, {"horizon": 1.0},
    {"policy": "unknown"}, {"path": "/etc/passwd"},
    {"resources": {"memory_budget": 512}}, {"resources": {"memory_budget": 4096}},
    {"resources": {"cpu_budget": 5}}, {"resources": {"max_concurrency": True}},
    {"resources": {"unknown": 1}},
])
async def test_request_rejection_before_model_or_execution(client, runtime, change):
    response = await client.post("/traffic/predict", json=dict(REQUEST, **change))
    assert response.status_code == 422
    assert runtime.backend is None
    assert runtime.records == {}


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["schema_version", "model_version"])
async def test_unknown_version_is_rejected(client, runtime, field):
    response = await client.post("/traffic/predict", json=dict(REQUEST, **{field: "unknown"}))
    assert response.status_code == 422
    assert runtime.active_executions == 0
    assert not runtime.execution_lock.locked()


@pytest.mark.asyncio
async def test_missing_workflow_is_404_and_does_not_load_model(client, runtime):
    assert (await client.get("/traffic/workflows/missing")).status_code == 404
    assert runtime.backend is None


@pytest.mark.asyncio
async def test_adaptation_rejects_wrong_semantics_and_skips_dependents(client, runtime, monkeypatch):
    original = runtime._prepare

    def incompatible(record):
        payload = original(record)
        tensors = dict(payload.tensors)
        tensors["state"] = replace(tensors["state"],
                                   schema=replace(tensors["state"].schema, semantic="wrong meaning"))
        return runtime.backend["contracts"].Payload(tensors)

    monkeypatch.setattr(runtime, "_prepare", incompatible)
    response = await client.post("/traffic/workflows/run", json=REQUEST)
    data = response.json()
    assert data["status"] == "failed"
    assert data["tasks"]["predict"]["call_status"] == "success"
    assert data["tasks"]["predict"]["quality_status"] == "invalid"
    assert "semantic" in data["tasks"]["predict"]["error"]
    assert all(data["tasks"][name]["schedule_status"] == "skipped"
               for name in TASK_IDS - {"prepare", "predict"})
    assert data["predictions"] == {}
    assert_reclaimed(data)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["shape", "units", "channels", "nonfinite", "normalized_shape"])
async def test_one_invalid_head_preserves_sibling_validation_and_blocks_aggregate(
        client, service, monkeypatch, failure):
    original = service.predict

    def corrupt(inputs, task="all"):
        result = original(inputs, task)
        item = result["predictions"]["state"]
        if failure == "shape":
            item["prediction"] = item["prediction"][:, 0]
        elif failure == "units":
            item["units"] = ["count", "mph"]
        elif failure == "channels":
            item["channels"] = list(reversed(item["channels"]))
        elif failure == "nonfinite":
            item["normalized"][0, 0, 0, 0, 0] = np.nan
        else:
            item["normalized"] = np.repeat(item["normalized"], 2, axis=0)
        return result

    monkeypatch.setattr(service, "predict", corrupt)
    data = (await client.post("/traffic/predict", json=REQUEST)).json()
    assert data["status"] == "failed"
    assert data["tasks"]["predict"]["successful"]
    assert data["tasks"]["validate_state"]["quality_status"] == "invalid"
    assert data["tasks"]["validate_flow"]["successful"]
    assert data["tasks"]["validate_od"]["successful"]
    assert data["tasks"]["aggregate"]["schedule_status"] == "skipped"
    assert data["predictions"] == {}
    assert_reclaimed(data)


@pytest.mark.asyncio
async def test_degraded_result_cannot_feed_aggregate(client, runtime, monkeypatch):
    original = runtime._validate

    def degrade(record, name):
        outcome = original(record, name)
        if name == "flow":
            outcome.quality_status = "degraded"
            outcome.error = "injected degradation"
        return outcome

    monkeypatch.setattr(runtime, "_validate", degrade)
    data = (await client.post("/traffic/predict", json=REQUEST)).json()
    assert data["tasks"]["validate_flow"]["call_status"] == "success"
    assert data["tasks"]["validate_flow"]["quality_status"] == "degraded"
    assert not data["tasks"]["validate_flow"]["successful"]
    assert data["tasks"]["validate_state"]["successful"]
    assert data["tasks"]["validate_od"]["successful"]
    assert data["tasks"]["aggregate"]["schedule_status"] == "skipped"
    assert data["predictions"] == {}
    assert_reclaimed(data)


@pytest.mark.asyncio
async def test_call_failure_is_separate_from_quality_and_reclaims(client, service, monkeypatch):
    def fail(inputs, task="all"):
        raise RuntimeError("injected model failure")
    monkeypatch.setattr(service, "predict", fail)
    data = (await client.post("/traffic/predict", json=REQUEST)).json()
    assert data["tasks"]["predict"]["call_status"] == "failed"
    assert data["tasks"]["predict"]["quality_status"] == "not_evaluated"
    assert data["predictions"] == {}
    assert_reclaimed(data)


@pytest.mark.asyncio
async def test_lower_resources_and_fcfs_are_applied(client):
    request = dict(REQUEST, policy="fcfs", resources=dict(
        cpu_budget=1, memory_budget=1024, max_concurrency=1))
    data = (await client.post("/traffic/workflows/run", json=request)).json()
    assert data["status"] == "success"
    assert data["policy"] == "fcfs"
    assert data["resources"]["memory_budget"] == 512
    assert data["peaks"]["concurrency"] == 1
    assert_reclaimed(data)


@pytest.mark.asyncio
async def test_task_budget_cannot_consume_resident_model_budget(client, runtime):
    response = await client.post("/traffic/predict", json=dict(
        REQUEST, resources={"memory_budget": 768}))
    assert response.status_code == 422
    assert runtime.active_executions == 0
    assert not runtime.execution_lock.locked()


@pytest.mark.asyncio
async def test_a2a_context_carries_this_request(client):
    request = dict(session_id="http-session", source_aoe_url="http://caller.test",
                   subtask=dict(task_id="traffic-subtask", context=dict(REQUEST, task="flow")))
    response = await client.post("/orchestration/dispatch", json=request)
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["status"] == "success"
    assert data["session_id"] == "http-session"
    assert data["task_id"] == "traffic-subtask"
    result = data["result"]
    assert data["workflow_handle"] == result["workflow_id"]
    assert result["context"]["subtask"]["context"]["target_frames"] == [1251]
    assert result["context"]["subtask"]["context"]["task"] == "flow"
    assert set(result["predictions"]) == {"flow"}
    assert_reclaimed(result)


@pytest.mark.asyncio
async def test_original_A2AClient_sends_native_envelope_and_correlates(runtime, monkeypatch):
    captured = {}

    async def record_exchange(request):
        captured["request"] = json.loads(request.content)

    async def record_response(response):
        await response.aread()
        captured["response"] = response.json()

    async_client = httpx.AsyncClient

    def in_process_client(**kwargs):
        return async_client(**kwargs, transport=httpx.ASGITransport(app=app),
                            event_hooks={"request": [record_exchange], "response": [record_response]})

    monkeypatch.setattr("src.service.a2a_client.httpx.AsyncClient", in_process_client)
    request = A2ATaskRequest(task_id="native-client", task_type="compute",
                            task_description="forecast flow", context=dict(REQUEST, task="flow"))
    result = await A2AClient(sender_id="native-scheduler").send_task_request(
        "http://traffic.test:8001", request)
    assert isinstance(result, A2ATaskResponse)
    assert result.status == "success", result.error_message
    assert set(result.result["predictions"]) == {"flow"}
    envelope = A2AMessage.model_validate(captured["response"])
    assert envelope.message_type == "response"
    assert envelope.correlation_id == captured["request"]["message_id"]
    assert envelope.receiver_id == "native-scheduler"
    assert envelope.sender_id == captured["request"]["receiver_id"]
    assert result.result["context"]["a2a_message"] == captured["request"]
    assert_reclaimed(result.result)


@pytest.mark.asyncio
@pytest.mark.parametrize("task_type,context,selected", [
    ("traffic", REQUEST, {"state", "flow", "od"}),
    ("state", {"target_frames": [1251]}, {"state"}),
    ("flow", {"target_frames": [1251]}, {"flow"}),
    ("od", {"target_frames": [1251]}, {"od"}),
])
async def test_native_a2a_task_types(client, task_type, context, selected):
    request = A2ATaskRequest(task_id="selector", task_type=task_type,
                            task_description="predict traffic", context=context)
    message = A2AMessage(sender_id="scheduler", receiver_id="traffic",
                         message_type="request", payload=request.model_dump())
    response = await client.post("/a2a/execute", json=message.model_dump())
    assert response.status_code == 200
    envelope = A2AMessage.model_validate(response.json())
    result = A2ATaskResponse.model_validate(envelope.payload)
    assert result.status == "success", result.error_message
    assert envelope.correlation_id == message.message_id
    assert set(result.result["predictions"]) == selected
    assert_reclaimed(result.result)


@pytest.mark.asyncio
@pytest.mark.parametrize("change,message_type", [
    ({"context": {"target_frames": [True]}}, "request"),
    ({"context": {"data_ref": "/etc/passwd", "target_frames": [1251]}}, "request"),
    ({"context": {"target_frames": [1251], "unexpected": 1}}, "request"),
    ({"task_type": "search"}, "request"),
    ({"task_type": "compute", "context": {"target_frames": [1251]}}, "request"),
    ({"timeout": 0}, "request"),
    ({}, "notification"),
])
async def test_native_a2a_invalid_context_or_type_returns_correlated_error(
        client, runtime, change, message_type):
    payload = dict(task_id="bad-native", task_type="traffic", task_description="invalid",
                   context=REQUEST, **{})
    payload.update(change)
    message = A2AMessage(sender_id="scheduler", receiver_id="traffic",
                         message_type=message_type, payload=payload)
    response = await client.post("/a2a/execute", json=message.model_dump())
    assert response.status_code == 200
    envelope = A2AMessage.model_validate(response.json())
    result = A2ATaskResponse.model_validate(envelope.payload)
    assert envelope.message_type == "error"
    assert envelope.correlation_id == message.message_id
    assert result.status == "error"
    assert result.error_message
    assert runtime.records == {}
    assert runtime.backend is None


@pytest.mark.asyncio
async def test_native_a2a_timeout_keeps_owner_and_resources_until_model_stops(
        client, runtime, service, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = service.predict

    def blocked(inputs, task="all"):
        entered.set()
        assert release.wait(5)
        return original(inputs, task)

    monkeypatch.setattr(service, "predict", blocked)
    task = A2ATaskRequest(task_id="timeout-native", task_type="traffic", timeout=1,
                         task_description="wait for model", context=REQUEST)
    message = A2AMessage(sender_id="scheduler", receiver_id="traffic",
                         message_type="request", payload=task.model_dump())
    request = asyncio.create_task(client.post("/a2a/execute", json=message.model_dump()))
    try:
        await wait_for_event(entered)
        response = await request
        envelope = A2AMessage.model_validate(response.json())
        task_response = A2ATaskResponse.model_validate(envelope.payload)
        assert task_response.status == "timeout"
        assert envelope.correlation_id == message.message_id
        record = runtime.records[task_response.metadata["workflow_id"]]
        owner = runtime.owners[record.workflow_id]
        snapshot = runtime.response(record)
        assert runtime.execution_lock.locked()
        assert runtime.active_executions == 1
        assert snapshot["tasks"]["predict"]["schedule_status"] == "running"
        assert snapshot["tasks"]["predict"]["cancel_requested"]
        assert snapshot["resources"]["memory_used_mb"] == 512
        release.set()
        await owner
        final = runtime.response(record)
        assert final["status"] == "cancelled"
        assert_reclaimed(final)
    finally:
        release.set()


async def wait_for_event(event):
    assert await asyncio.wait_for(asyncio.to_thread(event.wait, 5), timeout=6)


@pytest.mark.asyncio
async def test_outer_cancel_holds_lock_and_reservation_until_worker_stops(
        client, runtime, service, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = service.predict
    calls = []

    def blocked(inputs, task="all"):
        calls.append(tuple(inputs["target_frames"]))
        if len(calls) == 1:
            entered.set()
            assert release.wait(5), "Test did not release worker"
        return original(inputs, task)

    monkeypatch.setattr(service, "predict", blocked)
    first = asyncio.create_task(client.post("/traffic/workflows/run", json=REQUEST))
    try:
        await wait_for_event(entered)
        first_record = next(iter(runtime.records.values()))
        owner = runtime.owners[first_record.workflow_id]
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        second = asyncio.create_task(client.post("/traffic/predict", json=dict(REQUEST, target_frames=[1252])))
        a2a = asyncio.create_task(client.post("/orchestration/dispatch", json=dict(
            session_id="parallel", subtask={"context": dict(REQUEST, target_frames=[1253])})))
        await asyncio.sleep(0.03)
        assert len(runtime.records) == 3
        assert runtime.execution_lock.locked()
        assert runtime.active_executions == 1
        assert runtime.executions[first_record.workflow_id]["holds_lock"]
        assert not owner.done()
        assert calls == [(1251,)]
        running = (await client.get("/traffic/workflows/" + first_record.workflow_id)).json()
        predict_state = running["tasks"]["predict"]
        assert predict_state["schedule_status"] == "running"
        assert predict_state["cancel_requested"]
        assert running["resources"]["memory_used_mb"] == 512
        reservation = predict_state["reservation_id"]
        assert not any(event["event"] == "release" and event["reservation_id"] == reservation
                       for event in running["resource_events"])
        release.set()
        await owner
        responses = await asyncio.gather(second, a2a)
        assert all(response.status_code == 200 for response in responses)
        assert all(response.json()["status"] == "success" for response in responses)
        final = (await client.get("/traffic/workflows/" + first_record.workflow_id)).json()
        assert final["status"] == "cancelled"
        assert final["tasks"]["predict"]["call_status"] == "cancelled"
        assert_reclaimed(final)
        assert runtime.peak_executions == 1
        assert len(calls) == 3
        executions = sorted(runtime.executions.values(), key=lambda item: item["started_at"])
        assert all(left["finished_at"] <= right["started_at"]
                   for left, right in zip(executions, executions[1:]))
    finally:
        release.set()


@pytest.mark.asyncio
async def test_cancel_during_parallel_validation_drains_all_threads(
        client, runtime, monkeypatch):
    entered = {name: threading.Event() for name in traffic.TASKS}
    release = threading.Event()
    stopped = set()
    original = runtime._validate

    def blocked(record, name):
        entered[name].set()
        assert release.wait(5)
        result = original(record, name)
        stopped.add(name)
        return result

    monkeypatch.setattr(runtime, "_validate", blocked)
    request = asyncio.create_task(client.post("/traffic/predict", json=REQUEST))
    try:
        await asyncio.gather(*(wait_for_event(event) for event in entered.values()))
        record = next(iter(runtime.records.values()))
        owner = runtime.owners[record.workflow_id]
        request.cancel()
        with pytest.raises(asyncio.CancelledError):
            await request
        data = runtime.response(record)
        assert data["resources"]["concurrency"] == 3
        assert data["resources"]["memory_used_mb"] == 192
        assert runtime.execution_lock.locked()
        assert stopped == set()
        release.set()
        await owner
        data = runtime.response(record)
        assert stopped == set(traffic.TASKS)
        assert data["tasks"]["aggregate"]["schedule_status"] in {"skipped", "cancelled"}
        assert data["predictions"] == {}
        assert_reclaimed(data)
    finally:
        release.set()


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["predict", "validation"])
async def test_cancel_all_async_layers_repeatedly_keeps_real_work_reserved(
        client, runtime, service, monkeypatch, stage):
    names = ("predict",) if stage == "predict" else tuple("validate_" + name for name in traffic.TASKS)
    entered = {name: threading.Event() for name in names}
    stopped = {name: threading.Event() for name in names}
    release = threading.Event()

    if stage == "predict":
        original = service.predict

        def blocked(inputs, task="all"):
            entered["predict"].set()
            try:
                assert release.wait(5)
                return original(inputs, task)
            finally:
                stopped["predict"].set()
        monkeypatch.setattr(service, "predict", blocked)
    else:
        original = runtime._validate

        def blocked(record, name):
            task_id = "validate_" + name
            entered[task_id].set()
            try:
                assert release.wait(5)
                return original(record, name)
            finally:
                stopped[task_id].set()
        monkeypatch.setattr(runtime, "_validate", blocked)

    request = asyncio.create_task(client.post("/traffic/predict", json=REQUEST))
    try:
        await asyncio.gather(*(wait_for_event(event) for event in entered.values()))
        record = next(iter(runtime.records.values()))
        owner = runtime.owners[record.workflow_id]
        work = {name: record.thread_work[name] for name in names}
        executors = [task for task in asyncio.all_tasks()
                     if task.get_coro().__qualname__ == "TrafficRuntime._execute"]
        assert len(executors) == len(names)
        assert all(item.waiter is not None for item in work.values())
        current = asyncio.current_task()
        cancelled = set()
        # Reproduce asyncio.run's all-task cancellation, then cancel every
        # replacement wrapper/executor/owner repeatedly while functions stay live.
        for _ in range(5):
            for task in asyncio.all_tasks() - {current}:
                cancelled.add(task)
                task.cancel()
            owner.cancel()
            await asyncio.sleep(0.02)
            snapshot = runtime.response(record)
            assert not owner.done()
            assert runtime.execution_lock.locked()
            assert runtime.active_executions == 1
            assert snapshot["resources"]["concurrency"] == len(names)
            assert snapshot["resources"]["memory_used_mb"] == (512 if stage == "predict" else 192)
            assert all(not event.is_set() for event in stopped.values())
            assert all(item.future.running() and not item.future.done() for item in work.values())
            for name in names:
                state = snapshot["tasks"][name]
                assert state["schedule_status"] == "running"
                assert not any(event["event"] == "release"
                               and event["reservation_id"] == state["reservation_id"]
                               for event in snapshot["resource_events"])
        release.set()
        await asyncio.wait_for(asyncio.shield(owner), timeout=3)
        await asyncio.gather(*cancelled, return_exceptions=True)
        final = runtime.response(record)
        assert final["status"] == "cancelled"
        assert all(event.is_set() for event in stopped.values())
        assert all(item.future.done() for item in work.values())
        assert all(final["tasks"][name]["call_status"] == "cancelled" for name in names)
        assert runtime.thread_work == {}
        assert_reclaimed(final)
    finally:
        release.set()


@pytest.mark.asyncio
async def test_executor_cancelled_before_first_step_is_finished_by_owner(
        client, runtime, service, monkeypatch):
    entered, stopped, release = threading.Event(), threading.Event(), threading.Event()
    original_predict = service.predict
    create_task = asyncio.create_task
    executors = []

    def blocked(inputs, task="all"):
        entered.set()
        try:
            assert release.wait(5)
            return original_predict(inputs, task)
        finally:
            stopped.set()

    def cancel_before_start(coroutine, *args, **kwargs):
        task = create_task(coroutine, *args, **kwargs)
        if (coroutine.__qualname__ == "TrafficRuntime._execute"
                and coroutine.cr_frame.f_locals["task_id"] == "predict"):
            executors.append(task)
            task.cancel()
        return task

    monkeypatch.setattr(service, "predict", blocked)
    monkeypatch.setattr(traffic.asyncio, "create_task", cancel_before_start)
    request = create_task(client.post("/traffic/predict", json=REQUEST))
    try:
        await wait_for_event(entered)
        await asyncio.sleep(0.02)
        record = next(iter(runtime.records.values()))
        owner = runtime.owners[record.workflow_id]
        work = record.thread_work["predict"]
        assert executors[0].cancelled()
        assert work.future.running() and not work.future.done()
        assert runtime.execution_lock.locked()
        assert runtime.response(record)["resources"]["concurrency"] == 1
        assert not stopped.is_set()
        owner.cancel()
        await asyncio.sleep(0.02)
        assert runtime.execution_lock.locked()
        release.set()
        response = await asyncio.wait_for(request, timeout=3)
        assert response.json()["status"] == "cancelled"
        assert stopped.is_set()
        assert_reclaimed(runtime.response(record))
    finally:
        release.set()


@pytest.mark.asyncio
async def test_lifespan_drains_owners_and_real_threads_despite_repeated_cancellation(
        client, runtime, service, monkeypatch):
    entered, stopped, release = threading.Event(), threading.Event(), threading.Event()
    original = service.predict

    def blocked(inputs, task="all"):
        entered.set()
        try:
            assert release.wait(5)
            return original(inputs, task)
        finally:
            stopped.set()

    monkeypatch.setattr(service, "predict", blocked)
    lifespan = app.router.lifespan_context(app)
    await lifespan.__aenter__()
    request = asyncio.create_task(client.post("/traffic/predict", json=REQUEST))
    exiting = None
    try:
        await wait_for_event(entered)
        record = next(iter(runtime.records.values()))
        owner = runtime.owners[record.workflow_id]
        work = record.thread_work["predict"]
        queued = asyncio.create_task(client.post("/traffic/workflows/run", json=REQUEST))
        await asyncio.sleep(0.01)
        assert len(runtime.records) == 2
        exiting = asyncio.create_task(lifespan.__aexit__(None, None, None))
        await asyncio.sleep(0.01)
        assert runtime.closing
        assert (await client.post("/traffic/predict", json=REQUEST)).status_code == 503
        assert (await client.get("/traffic/capabilities")).status_code == 503
        for _ in range(3):
            work.waiter.cancel()
            owner.cancel()
            exiting.cancel()
            await asyncio.sleep(0.02)
            assert not exiting.done()
            assert not stopped.is_set()
            assert runtime.execution_lock.locked()
            assert runtime.active_executions == 1
            assert runtime.response(record)["resources"]["concurrency"] == 1
        release.set()
        await asyncio.wait_for(asyncio.shield(exiting), timeout=3)
        await asyncio.gather(request, queued, return_exceptions=True)
        assert runtime.closed
        assert runtime.owners == {}
        assert runtime.thread_work == {}
        assert all(not thread.is_alive() for thread in runtime._executor._threads)
        assert stopped.is_set()
        for record in runtime.records.values():
            assert record.status == "cancelled"
            assert_reclaimed(runtime.response(record))
    finally:
        release.set()
        if exiting is None:
            await lifespan.__aexit__(None, None, None)


@pytest.mark.asyncio
async def test_capability_initialization_shutdown_keeps_lock_until_real_load_ends(
        client, runtime, monkeypatch):
    entered, stopped, release = threading.Event(), threading.Event(), threading.Event()
    original = runtime._load

    def blocked():
        entered.set()
        try:
            assert release.wait(5)
            return original()
        finally:
            stopped.set()

    monkeypatch.setattr(runtime, "_load", blocked)
    request = asyncio.create_task(client.get("/traffic/capabilities"))
    try:
        await wait_for_event(entered)
        owner = next(iter(runtime.owners.values()))
        work = next(iter(runtime.thread_work.values()))
        request.cancel()
        work.waiter.cancel()
        owner.cancel()
        shutdown = asyncio.create_task(runtime.shutdown())
        await asyncio.sleep(0.03)
        assert not shutdown.done()
        assert not stopped.is_set()
        assert work.future.running() and not work.future.done()
        assert runtime.execution_lock.locked()
        assert runtime.active_executions == 1
        release.set()
        await asyncio.wait_for(shutdown, timeout=3)
        await asyncio.gather(request, return_exceptions=True)
        assert stopped.is_set()
        assert runtime.closed
        assert not runtime.execution_lock.locked()
        assert runtime.active_executions == 0
        assert runtime.thread_work == {}
    finally:
        release.set()


@pytest.mark.parametrize("mode", ["asyncio_run", "lifespan"])
def test_subprocess_asyncio_run_shutdown_exits_without_hang(mode):
    root = Path(__file__).resolve().parents[2]
    code = r'''
import asyncio, json, threading
from src.api import traffic
from traffic_server import app

entered, release, stopped = threading.Event(), threading.Event(), threading.Event()
holder = {}

async def main():
    runtime = traffic.TrafficRuntime()
    traffic._runtime = runtime
    holder['runtime'] = runtime
    # Initialize real B1 once, then hold its actual predict function through
    # event-loop shutdown; the independent timer represents execution ending.
    await runtime.capabilities()
    original = runtime.service.predict
    def blocked(inputs, task='all'):
        entered.set()
        try:
            assert release.wait(5)
            return original(inputs, task)
        finally:
            stopped.set()
    runtime.service.predict = blocked
    async def launch():
        asyncio.create_task(runtime.run(traffic.TrafficRequest(target_frames=[1251])))
        assert await asyncio.to_thread(entered.wait, 5)
        threading.Timer(0.2, release.set).start()
    if MODE == 'lifespan':
        async with app.router.lifespan_context(app):
            await launch()
    else:
        await launch()
        # Return with live owner/executor/wrapper: asyncio.run cancels them all.

asyncio.run(main())
runtime = holder['runtime']
assert stopped.is_set()
assert not runtime.execution_lock.locked()
assert runtime.active_executions == 0
assert runtime.thread_work == {}
for record in runtime.records.values():
    snapshot = record.scheduler.snapshot()
    assert snapshot['done']
    assert snapshot['resources']['concurrency'] == 0
    assert snapshot['resources']['memory_used_mb'] == 0
    assert record.status == 'cancelled'
runtime._executor.shutdown(wait=True)
assert all(not thread.is_alive() for thread in runtime._executor._threads)
print('shutdown complete')
'''
    environment = dict(os.environ, PYTHONDONTWRITEBYTECODE="1", PYTHONPATH=str(root))
    result = subprocess.run([sys.executable, "-c", "MODE = " + repr(mode) + "\n" + code],
                            cwd=root, env=environment, capture_output=True, text=True,
                            timeout=12, check=True)
    assert result.stdout.strip() == "shutdown complete"
    assert "Task was destroyed" not in result.stderr
    assert "Event loop is closed" not in result.stderr


@pytest.mark.asyncio
async def test_include_router_shutdown_hook_and_concurrent_shutdown_are_safe(
        runtime, service, monkeypatch):
    integrated = FastAPI()
    integrated.include_router(traffic.router)
    assert traffic.shutdown_traffic in integrated.router.on_shutdown
    entered, stopped, release = threading.Event(), threading.Event(), threading.Event()
    original = service.predict

    def blocked(inputs, task="all"):
        entered.set()
        try:
            assert release.wait(5)
            return original(inputs, task)
        finally:
            stopped.set()

    monkeypatch.setattr(service, "predict", blocked)
    lifespan = integrated.router.lifespan_context(integrated)
    await lifespan.__aenter__()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=integrated),
                                 base_url="http://main.test") as client:
        request = asyncio.create_task(client.post("/traffic/predict", json=REQUEST))
        try:
            await wait_for_event(entered)
            record = next(iter(runtime.records.values()))
            exiting = asyncio.create_task(lifespan.__aexit__(None, None, None))
            duplicate = asyncio.create_task(runtime.shutdown())
            await asyncio.sleep(0.02)
            duplicate.cancel()
            exiting.cancel()
            await asyncio.sleep(0.02)
            assert not exiting.done() and not duplicate.done()
            assert not stopped.is_set()
            assert runtime.execution_lock.locked()
            assert runtime.response(record)["resources"]["memory_used_mb"] == 512
            release.set()
            await asyncio.wait_for(asyncio.gather(exiting, duplicate, request), timeout=3)
            assert runtime.closed
            assert stopped.is_set()
            assert all(not thread.is_alive() for thread in runtime._executor._threads)
            assert_reclaimed(runtime.response(record))
            await traffic.shutdown_traffic()
            await asyncio.gather(runtime.shutdown(), runtime.shutdown())
            assert runtime.thread_work == {}
        finally:
            release.set()


@pytest.mark.asyncio
async def test_repeated_shutdown_preserves_completed_records_and_rejects_admission(client, runtime):
    response = await client.post("/traffic/predict", json=REQUEST)
    data = response.json()
    await runtime.shutdown()
    await runtime.shutdown()
    await traffic.shutdown_traffic()
    assert runtime.closed
    assert runtime.records[data["workflow_id"]].status == "success"
    assert_reclaimed(runtime.response(runtime.records[data["workflow_id"]]))
    with pytest.raises(HTTPException) as exc:
        await runtime.run(traffic.TrafficRequest.model_validate(REQUEST))
    assert exc.value.status_code == 503
    assert (await client.get("/traffic/capabilities")).status_code == 503


@pytest.mark.asyncio
async def test_async_executor_launch_failure_cannot_release_submitted_thread(
        client, runtime, service, monkeypatch):
    entered, stopped, release = threading.Event(), threading.Event(), threading.Event()
    original = service.predict
    create_task = asyncio.create_task

    def blocked(inputs, task="all"):
        entered.set()
        try:
            assert release.wait(5)
            return original(inputs, task)
        finally:
            stopped.set()

    def failed_launch(coroutine, *args, **kwargs):
        if (coroutine.__qualname__ == "TrafficRuntime._execute"
                and coroutine.cr_frame.f_locals["task_id"] == "predict"):
            raise RuntimeError("injected async executor launch failure")
        return create_task(coroutine, *args, **kwargs)

    monkeypatch.setattr(service, "predict", blocked)
    monkeypatch.setattr(traffic.asyncio, "create_task", failed_launch)
    request = create_task(client.post("/traffic/predict", json=REQUEST))
    try:
        await wait_for_event(entered)
        record = next(iter(runtime.records.values()))
        assert runtime.execution_lock.locked()
        assert runtime.response(record)["resources"]["memory_used_mb"] == 512
        assert not stopped.is_set()
        assert record.thread_work["predict"].future.running()
        release.set()
        response = await asyncio.wait_for(request, timeout=3)
        assert response.json()["status"] == "success"
        assert stopped.is_set()
        assert_reclaimed(runtime.response(record))
    finally:
        release.set()
