"""Traffic HTTP endpoints; importing this module never imports torch or an LLM.

An independent owner holds the process-wide admission lock until every worker
has stopped. Cancelling an HTTP waiter only requests cancellation of its DAG.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from dataclasses import dataclass, field
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional

from fastapi import APIRouter, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ConfigDict, Field, StrictInt, model_validator


DATA_REF = "nyc_tlc_green_2024_01_6x6"
GRID_ID = "nyc-green-6x6-v1"
TASKS = ("state", "flow", "od")
BASE_MEMORY_MB = 512
router = APIRouter(prefix="/traffic", tags=["traffic"])


@router.get("/ui", response_class=HTMLResponse, include_in_schema=False)
async def traffic_ui():
    from src.api.traffic_ui import get_traffic_ui_html

    return get_traffic_ui_html()


class Resources(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    cpu_budget: float = Field(4, gt=0, le=4, allow_inf_nan=False)
    memory_budget: float = Field(2048, gt=BASE_MEMORY_MB, le=2048, allow_inf_nan=False)
    max_concurrency: int = Field(4, ge=1, le=4)
    aging_interval: float = Field(30, gt=0, allow_inf_nan=False)
    resource_timeout: float = Field(30, ge=0, allow_inf_nan=False)


class TrafficRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    data_ref: Literal["nyc_tlc_green_2024_01_6x6"] = DATA_REF
    target_frames: List[StrictInt] = Field(..., min_length=1, max_length=32)
    task: Literal["all", "state", "flow", "od"] = "all"
    schema_version: Optional[str] = Field(None, min_length=1, strict=True)
    model_version: Optional[str] = Field(None, min_length=1, strict=True)
    horizon: Literal[1] = 1
    policy: Literal["drc", "fcfs"] = "drc"
    resources: Resources = Field(default_factory=Resources)

    @model_validator(mode="before")
    @classmethod
    def strict_horizon(cls, value):
        if isinstance(value, dict) and "horizon" in value:
            if type(value["horizon"]) is not int:
                raise ValueError("horizon must be integer 1")
        return value

    @model_validator(mode="after")
    def available_frames(self):
        if any(frame < 3 or frame >= 1488 or frame % 48 < 3
               for frame in self.target_frames):
            raise ValueError("target_frames must have available same-day history [t-3,t) and target [t,t+1)")
        return self


class DispatchSubtask(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: Optional[str] = None
    task_description: str = "traffic prediction"
    context: TrafficRequest


class DispatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    subtask: DispatchSubtask
    session_id: str = Field(..., min_length=1)
    source_aoe_url: str = ""


def _json(value):
    """Keep ndarray handling lazy and preserve all B/H singleton dimensions."""
    if isinstance(value, dict):
        return {key: _json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json(item) for item in value]
    if hasattr(value, "tolist"):
        return value.tolist()
    return value


@dataclass
class Execution:
    workflow_id: str
    request: TrafficRequest
    context: Optional[dict] = None
    status: str = "queued"
    scheduler: Any = None
    cancel_requested: bool = False
    schema_trace: dict = field(default_factory=dict)
    predictions: dict = field(default_factory=dict)
    values: dict = field(default_factory=dict)
    error: Optional[str] = None
    http_error: Optional[HTTPException] = None
    thread_work: dict = field(default_factory=dict)


@dataclass
class TaskOutcome:
    """An executor outcome; only success + valid may feed a successor."""

    result: Any = None
    call_status: str = "success"
    quality_status: str = "valid"
    error: Optional[str] = None
    schema_trace: dict = field(default_factory=dict)


@dataclass
class ThreadWork:
    """The synchronous Future is authoritative; an asyncio waiter is disposable."""

    key: str
    future: Future
    record: Optional[Execution] = None
    task_id: Optional[str] = None
    waiter: Optional[asyncio.Task] = None


class TrafficRuntime:
    """One lazy model and one execution lock shared by workflow/predict/A2A."""

    def __init__(self, service=None):
        self.service = service
        self._execution_lock = None
        self.records: Dict[str, Execution] = {}
        self.owners: Dict[str, asyncio.Task] = {}
        self.executions: Dict[str, dict] = {}
        self.active_executions = 0
        self.peak_executions = 0
        self.backend = None
        self.thread_work: Dict[str, ThreadWork] = {}
        self._executor = None
        self.closing = False
        self.closed = False

    @property
    def execution_lock(self):
        # Python 3.9 binds locks when constructed: create it on the serving loop,
        # rather than on an import-time/default loop that the server replaces.
        if self._execution_lock is None:
            self._execution_lock = asyncio.Lock()
        return self._execution_lock

    def _load(self):
        if self.backend is not None:
            return
        import numpy as np
        from src.service.traffic_model import TrafficModelService
        from src.service import traffic_contracts as contracts

        service = self.service or TrafficModelService()
        processed = service.experiment_root / "data" / "processed"
        metadata = json.loads((processed / "dataset_metadata.json").read_text())
        if metadata["grid"] != [6, 6] or metadata["step_minutes"] != 30:
            raise ValueError("Traffic API requires the versioned 6x6, 30-minute grid")
        stats_path = processed / "normalization_train_only.npz"
        storage, inputs, outputs, standardized_outputs, stats = {}, {}, {}, {}, {}
        with np.load(stats_path, allow_pickle=False) as fitted:
            for name in TASKS:
                channels = 36 if name == "od" else 2
                common = dict(
                    field=name,
                    units=(metadata["od_units"],) * channels if name == "od"
                    else tuple(metadata[name + "_units"]),
                    semantic=metadata["od_definition"] if name == "od"
                    else ";".join(metadata[name + "_channels"]),
                    version=service.version, grid_id=GRID_ID, frame_minutes=30,
                )
                axes = ("B", "L", "Y", "X", "D" if name == "od" else "C")
                storage[name] = contracts.TensorSchema(
                    **common, axes=axes, shape=(None, 3, 6, 6, channels))
                inputs[name] = contracts.TensorSchema(
                    **common, axes=axes if name == "od" else ("B", "L", "C", "Y", "X"),
                    shape=(None, 3, 6, 6, 36) if name == "od" else (None, 3, 2, 6, 6),
                    normalization="standardized")
                forecast = dict(common, axes=("B", "H", "Y", "X", "D" if name == "od" else "C"),
                                shape=(None, 1, 6, 6, channels))
                outputs[name] = contracts.TensorSchema(**forecast)
                standardized_outputs[name] = contracts.TensorSchema(
                    **forecast, normalization="standardized")
                stats[name] = contracts.TrainingStats(
                    **common, mean=tuple(fitted[name + "_mean"].reshape(-1)),
                    std=tuple(fitted[name + "_std"].reshape(-1)),
                    source=str(stats_path), split="train")
        registry = contracts.CapabilityRegistry()
        registry.register(contracts.CapabilityContract(
            "B1", service.version, inputs=inputs, outputs=outputs))
        for name in TASKS:
            registry.register(contracts.CapabilityContract(
                name, service.version, inputs=inputs, outputs={name: outputs[name]}))
            registry.register(contracts.CapabilityContract(
                "validate_" + name, service.version, inputs={name: outputs[name]}))
        self.service = service
        self.backend = dict(
            np=np, contracts=contracts, registry=registry, storage=storage,
            outputs=outputs, standardized_outputs=standardized_outputs,
            stats=stats, metadata=metadata,
            normalization=dict(
                method="standardized", split="train", source=stats_path.name,
                stats_sha256=hashlib.sha256(stats_path.read_bytes()).hexdigest(),
                sha256=hashlib.sha256(stats_path.read_bytes()).hexdigest(),
                statistics={name: item.to_dict() for name, item in stats.items()}))

    def _submit(self, operation, record=None, task_id=None):
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="traffic")
        # Never wrap/chain this Future with asyncio.wrap_future: cancelling an
        # asyncio wrapper must not cancel queued or running synchronous work.
        work = ThreadWork(uuid.uuid4().hex, self._executor.submit(operation), record, task_id)
        self.thread_work[work.key] = work
        if record is not None:
            record.thread_work[task_id or "initialize"] = work
        return work

    @staticmethod
    async def _completion(work):
        loop = asyncio.get_running_loop()
        signal = loop.create_future()

        def resolve():
            if not signal.done():
                signal.set_result(None)

        # Only the notification is cancellable. The underlying Future keeps
        # running, and every replacement waiter can observe its real completion.
        work.future.add_done_callback(lambda done: loop.call_soon_threadsafe(resolve))
        await signal

    async def _thread(self, operation=None, record=None, *, work=None):
        work = work or self._submit(operation, record)
        try:
            while not work.future.done():
                if work.waiter is None or work.waiter.done():
                    work.waiter = asyncio.create_task(self._completion(work))
                try:
                    await asyncio.shield(work.waiter)
                except asyncio.CancelledError:
                    if work.record is not None:
                        self.request_cancel(work.record)
                    # A cancelled waiter is replaced; it is never treated as
                    # execution termination, even during asyncio.run shutdown.
            return work.future.result()
        finally:
            if work.future.done():
                self.thread_work.pop(work.key, None)
                if work.waiter is not None and not work.waiter.done():
                    work.waiter.cancel()

    def snapshot(self):
        return dict(active_executions=self.active_executions,
                    peak_executions=self.peak_executions,
                    executions=_json(self.executions), closing=self.closing,
                    thread_work={key: dict(workflow_id=work.record.workflow_id if work.record else None,
                                           task_id=work.task_id, done=work.future.done(),
                                           running=work.future.running())
                                 for key, work in self.thread_work.items()})

    def _enter(self, key, context):
        self.active_executions += 1
        self.peak_executions = max(self.peak_executions, self.active_executions)
        self.executions[key] = dict(holds_lock=True, started_at=time.monotonic(), context=context)

    def _leave(self, key):
        self.executions[key].update(holds_lock=False, finished_at=time.monotonic())
        self.active_executions -= 1

    async def capabilities(self):
        if self.closing:
            raise HTTPException(503, detail="Traffic runtime is shutting down")
        key = "capabilities-" + uuid.uuid4().hex

        async def owner():
            async with self.execution_lock:
                self._enter(key, {"operation": "capabilities"})
                try:
                    await self._thread(self._load)
                    return dict(
                        data_refs=[DATA_REF], version=self.service.version,
                        checkpoint_sha256=self.service.checkpoint_sha256,
                        horizon=1, history_length=3, max_batch=32,
                        frame_minutes=30, grid_id=GRID_ID, grid_version=GRID_ID,
                        output_layout="B,H,Y,X,C", policies=["drc", "fcfs"],
                        base_resources={"memory_mb": BASE_MEMORY_MB},
                        budgets=self._budgets(Resources()),
                        normalization=self.backend["normalization"],
                        **self.backend["registry"].to_dict())
                finally:
                    self._leave(key)
        task = asyncio.create_task(owner())
        self.owners[key] = task
        task.add_done_callback(lambda done: self._owner_done(key, done))
        return _json(await asyncio.shield(task))

    @staticmethod
    def _budgets(resources):
        return dict(cpu_budget=resources.cpu_budget,
                    memory_budget=resources.memory_budget,
                    task_memory_budget=resources.memory_budget - BASE_MEMORY_MB,
                    max_concurrency=resources.max_concurrency)

    def _scheduler(self, record):
        from src.graph.traffic_scheduler import Scheduler, TaskSpec

        resources = record.request.resources
        cpu = min(1, resources.cpu_budget)
        # Declared worker envelopes fit the admitted budget even for B=32.
        memory = {"prepare": 128, "predict": 512, "aggregate": 64,
                  **{"validate_" + name: 64 for name in TASKS}}
        deps = {"prepare": (), "predict": ("prepare",),
                **{"validate_" + name: ("predict",) for name in TASKS},
                "aggregate": tuple("validate_" + name for name in TASKS)}
        now = time.monotonic()
        specs = [TaskSpec(name, parents, cpu, memory[name], enqueue_time=now,
                          resource_timeout=resources.resource_timeout)
                 for name, parents in deps.items()]
        try:
            return Scheduler(specs, resources.cpu_budget,
                             resources.memory_budget - BASE_MEMORY_MB,
                             resources.max_concurrency, policy=record.request.policy,
                             aging_interval=resources.aging_interval)
        except ValueError as exc:
            raise HTTPException(422, detail=str(exc)) from exc

    def _prepare(self, record):
        c, b = self.backend["contracts"], self.backend
        raw = self.service.prepare_history(record.request.target_frames)
        payload = c.Payload({name: c.TensorPayload(
            values, b["storage"][name], DATA_REF) for name, values in raw.items()})
        return payload

    def _predict(self, record):
        b = self.backend
        adapted, trace = b["registry"].adapt(
            "B1", self.service.version, record.values["prepare"], stats=b["stats"])
        b["registry"].validate("B1", self.service.version, adapted)
        inputs = {name: tensor.data for name, tensor in adapted.tensors.items()}
        inputs["target_frames"] = record.request.target_frames
        # All heads are validated in the six-task DAG, even for a selected head.
        output = self.service.predict(inputs, task="all")
        if (output["version"] != self.service.version
                or output["checkpoint_sha256"] != self.service.checkpoint_sha256):
            raise b["contracts"].ContractError("version", "model result identity mismatch")
        if set(output["predictions"]) != set(TASKS):
            raise b["contracts"].ContractError("modality", "all three model heads required")
        return TaskOutcome(output, schema_trace=trace)

    def _validate(self, record, name):
        b, c = self.backend, self.backend["contracts"]
        item = record.values["predict"]["predictions"][name]
        expected = b["outputs"][name]
        channels = (["destination_" + str(i) for i in range(36)] if name == "od"
                    else b["metadata"][name + "_channels"])
        if (item["layout"] != "B,H,Y,X,C" or item["units"] != list(expected.units)
                or item["channels"] != channels):
            raise c.ContractError("schema", name + ": output layout/units/channels mismatch")
        raw, normalized = item["prediction"], item["normalized"]
        shape = [len(record.request.target_frames), 1, 6, 6, len(expected.units)]
        if (list(raw.shape) != shape or list(normalized.shape) != shape
                or item["shape"] != shape):
            raise c.ContractError("shape", name + ": output batch/horizon/shape mismatch")
        c.validate(normalized, b["standardized_outputs"][name])
        payload = c.Payload({name: c.TensorPayload(raw, expected, "B1:" + self.service.checkpoint_sha256)})
        b["registry"].validate("validate_" + name, self.service.version, payload)
        if (raw < 0).any():
            raise c.ContractError("negative", name + ": physical prediction must be nonnegative")
        return TaskOutcome(item, schema_trace={"validate_" + name: [
            dict(rule="validate", side="output", schema=expected.to_dict())]})

    def _aggregate(self, record):
        selected = TASKS if record.request.task == "all" else (record.request.task,)
        return {name: record.values["validate_" + name] for name in selected}

    def _operation(self, record, task_id):
        if task_id == "prepare":
            return lambda: self._prepare(record)
        if task_id == "predict":
            return lambda: self._predict(record)
        if task_id == "aggregate":
            return lambda: self._aggregate(record)
        return lambda: self._validate(record, task_id[len("validate_"):])

    def _finish_thread(self, record, task_id, work):
        if not work.future.done():
            raise RuntimeError("Cannot finish a task while synchronous work is running")
        if record.scheduler.snapshot()["tasks"][task_id]["terminal"]:
            return
        try:
            result = work.future.result()
            outcome = result if isinstance(result, TaskOutcome) else TaskOutcome(result)
            record.schema_trace.update(outcome.schema_trace)
            if record.cancel_requested:
                record.scheduler.finish(task_id, time.monotonic(),
                                        "cancelled", "not_evaluated")
            else:
                if outcome.call_status == "success" and outcome.quality_status == "valid":
                    record.values[task_id] = outcome.result
                    if task_id == "aggregate":
                        record.predictions = outcome.result
                # Keep tensors out of the scheduler's deepcopy/event snapshots.
                summary = {"stage": task_id}
                if task_id.startswith("validate_") and isinstance(outcome.result, dict):
                    summary["shape"] = outcome.result.get("shape")
                record.scheduler.finish(task_id, time.monotonic(),
                                        outcome.call_status, outcome.quality_status,
                                        result=summary, error=outcome.error)
        except BaseException as exc:
            if record.cancel_requested or isinstance(exc, asyncio.CancelledError):
                self.request_cancel(record)
                record.scheduler.finish(task_id, time.monotonic(),
                                        "cancelled", "not_evaluated", error=str(exc))
                return
            contract_error = isinstance(exc, self.backend["contracts"].ContractError)
            record.scheduler.finish(task_id, time.monotonic(),
                                    "success" if contract_error else "failed",
                                    "invalid" if contract_error else "not_evaluated",
                                    error=str(exc))

    async def _execute(self, record, task_id, work):
        try:
            await self._thread(work=work)
        except BaseException:
            # The real Future contains the result/error and confirms completion.
            pass
        self._finish_thread(record, task_id, work)

    async def _drain_record(self, record):
        for task_id, work in tuple(record.thread_work.items()):
            try:
                await self._thread(work=work)
            except BaseException:
                pass
            if task_id != "initialize":
                self._finish_thread(record, task_id, work)

    def request_cancel(self, record):
        record.cancel_requested = True
        if record.scheduler is not None:
            for task_id, state in record.scheduler.snapshot()["tasks"].items():
                if not state["terminal"]:
                    record.scheduler.cancel(task_id, time.monotonic(), reason="request_cancelled")

    async def _drive(self, record):
        workers = {}
        try:
            while True:
                if record.cancel_requested:
                    self.request_cancel(record)
                for state in record.scheduler.tick(time.monotonic()):
                    record.scheduler.start(state.task_id, time.monotonic())
                    work = None
                    executor = None
                    try:
                        # Submit before the async executor starts. If that Task
                        # is cancelled before its first step, the owner still
                        # owns the Future and can drain/finish it.
                        work = self._submit(self._operation(record, state.task_id), record, state.task_id)
                        executor = self._execute(record, state.task_id, work)
                        worker = asyncio.create_task(executor)
                        workers[worker] = (state.task_id, work)
                    except Exception as exc:
                        if executor is not None:
                            executor.close()
                        if work is None:
                            record.scheduler.start_failed(state.task_id, time.monotonic(), error=str(exc))
                        else:
                            # Async observer launch failed after synchronous
                            # submission: the owner adopts the actual work.
                            try:
                                await self._thread(work=work)
                            except BaseException:
                                pass
                            self._finish_thread(record, state.task_id, work)
                if record.scheduler.snapshot()["done"] and not workers:
                    return
                if workers:
                    try:
                        done, _ = await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)
                        for worker in done:
                            task_id, work = workers[worker]
                            if worker.cancelled():
                                self.request_cancel(record)
                            # A cancelled async executor can never strand a
                            # running scheduler task or remove its real work.
                            try:
                                await self._thread(work=work)
                            except BaseException:
                                pass
                            self._finish_thread(record, task_id, work)
                            if not worker.cancelled():
                                worker.exception()
                            workers.pop(worker)
                    except asyncio.CancelledError:
                        self.request_cancel(record)
                else:
                    await asyncio.sleep(0.01)
        finally:
            # Even an unexpected dispatcher failure cannot release admission
            # while another validation/model thread still owns a reservation.
            if workers:
                self.request_cancel(record)
                await self._drain_record(record)
                drained = asyncio.gather(*workers, return_exceptions=True)
                while True:
                    try:
                        await asyncio.shield(drained)
                        break
                    except asyncio.CancelledError:
                        self.request_cancel(record)

    async def _own(self, record):
        async with self.execution_lock:
            self._enter(record.workflow_id, record.context)
            record.status = "running"
            try:
                await self._thread(self._load, record)
                for version in (record.request.schema_version, record.request.model_version):
                    if version is not None and version != self.service.version:
                        raise HTTPException(422, detail="Unregistered traffic schema/model version")
                record.scheduler = self._scheduler(record)
                await self._drive(record)
                states = record.scheduler.snapshot()["tasks"]
                record.status = ("cancelled" if record.cancel_requested else "success"
                                 if states["aggregate"]["successful"] else "failed")
                record.error = next((state["error"] for state in states.values()
                                     if state["error"]), None)
            except HTTPException as exc:
                record.status, record.http_error, record.error = "failed", exc, str(exc.detail)
            except asyncio.CancelledError:
                self.request_cancel(record)
                record.status = "cancelled"
            except Exception as exc:
                record.status, record.error = "failed", str(exc)
                record.http_error = HTTPException(503, detail="Traffic model unavailable: " + str(exc))
            finally:
                # This guard is independent of async executor/waiter survival.
                await self._drain_record(record)
                record.values.clear()
                record.thread_work.clear()
                self._leave(record.workflow_id)
        return self.response(record)

    def _owner_done(self, key, task):
        self.owners.pop(key, None)
        if not task.cancelled():
            task.exception()

    async def run(self, request, context=None):
        if self.closing:
            raise HTTPException(503, detail="Traffic runtime is shutting down")
        record = Execution("traffic-" + uuid.uuid4().hex, request, context=context)
        self.records[record.workflow_id] = record
        owner = asyncio.create_task(self._own(record))
        self.owners[record.workflow_id] = owner
        owner.add_done_callback(lambda done: self._owner_done(record.workflow_id, done))
        try:
            result = await asyncio.shield(owner)
        except asyncio.CancelledError:
            self.request_cancel(record)
            raise
        if record.http_error is not None:
            raise record.http_error
        return result

    async def shutdown(self):
        """Reject new admission, request cancellation, drain, then join threads."""
        if self.closed:
            return
        self.closing = True
        for record in self.records.values():
            if record.status in ("queued", "running"):
                self.request_cancel(record)
        for owner in tuple(self.owners.values()):
            while not owner.done():
                try:
                    await asyncio.shield(owner)
                except asyncio.CancelledError:
                    # Repeated cancellation of lifespan must still drain work.
                    for record in self.records.values():
                        if record.status in ("queued", "running"):
                            self.request_cancel(record)
                except Exception:
                    break
        # Owners cancelled before their first step may never run their finally.
        # Retained synchronous Futures remain authoritative in that case too.
        for work in tuple(self.thread_work.values()):
            try:
                await self._thread(work=work)
            except BaseException:
                pass
        for record in self.records.values():
            if record.status in ("queued", "running"):
                await self._drain_record(record)
                record.status = "cancelled"
                record.values.clear()
                record.thread_work.clear()
        if self._executor is not None:
            # All functions have actually returned; joining idle pool threads
            # here cannot wait on model/validation work or depend on event-loop
            # cancellation/default-executor shutdown.
            self._executor.shutdown(wait=True)
        self.closed = True

    def _ranges(self, request):
        metadata = self.backend["metadata"] if self.backend else {"start": "2024-01-01 00:00:00"}
        origin = datetime.fromisoformat(metadata["start"])

        def ranges(left, right):
            return [dict(target_frame=frame, start_frame=frame + left,
                         end_frame_exclusive=frame + right,
                         start=(origin + timedelta(minutes=30 * (frame + left))).isoformat(),
                         end_exclusive=(origin + timedelta(minutes=30 * (frame + right))).isoformat(),
                         interval="[start,end)", frame_minutes=30,
                         grid_id=GRID_ID, grid_version=GRID_ID)
                    for frame in request.target_frames]
        return ranges(-3, 0), ranges(0, 1)

    def response(self, record):
        history, target = self._ranges(record.request)
        snapshot = (record.scheduler.snapshot() if record.scheduler else
                    dict(tasks={}, resources={}, peaks={}, events=[], done=False))
        snapshot["events"] = snapshot.get("task_events", []) + snapshot.get("resource_events", [])
        return _json(dict(
            workflow_id=record.workflow_id, status=record.status, task=record.request.task,
            data_ref=record.request.data_ref, target_frames=record.request.target_frames,
            version=self.service.version if self.service else None,
            checkpoint_sha256=self.service.checkpoint_sha256 if self.service else None,
            predictions=record.predictions, schema_trace=record.schema_trace,
            time_range=history, target_time_range=target,
            normalization=self.backend["normalization"] if self.backend else None,
            base_resources={"memory_mb": BASE_MEMORY_MB},
            budgets=self._budgets(record.request.resources),
            runtime=self.snapshot(), cancel_requested=record.cancel_requested,
            error=record.error, context=record.context, **snapshot))


_runtime = TrafficRuntime()


def get_runtime():
    return _runtime


@router.on_event("shutdown")
async def shutdown_traffic():
    # include_router copies this hook to the existing main application's
    # lifecycle; the standalone lifespan may safely invoke shutdown again.
    await get_runtime().shutdown()


@router.get("/capabilities")
async def capabilities():
    try:
        return await get_runtime().capabilities()
    except Exception as exc:
        raise HTTPException(503, detail="Traffic model unavailable: " + str(exc)) from exc


@router.post("/workflows/run")
async def run_workflow(request: TrafficRequest):
    return await get_runtime().run(request)


@router.post("/predict")
async def predict(request: TrafficRequest):
    return await get_runtime().run(request)


@router.get("/workflows/{workflow_id}")
async def get_workflow(workflow_id: str):
    runtime = get_runtime()
    if workflow_id not in runtime.records:
        raise HTTPException(404, detail="Traffic workflow not found")
    return runtime.response(runtime.records[workflow_id])


async def dispatch(request: DispatchRequest):
    context = request.model_dump()
    result = await get_runtime().run(request.subtask.context, context=context)
    return dict(task_id=request.subtask.task_id or result["workflow_id"],
                session_id=request.session_id, workflow_handle=result["workflow_id"],
                status=result["status"], result=result)
