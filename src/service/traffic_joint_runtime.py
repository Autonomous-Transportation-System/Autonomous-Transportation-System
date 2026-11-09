"""Real B1 execution for the joint scheduler, with one process-wide owner.

Only a concurrent.futures.Future's completion releases a running reservation.
HTTP waiters and workflow cancellation never own, or cancel, model forwards.
Injected backends have the same service/schema bundle as the real backend;
there is deliberately no synthetic-data fallback.
"""
from __future__ import annotations

import asyncio
import copy
import hashlib
import json
import math
import os
import resource
import sys
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from typing import Any, Callable

from src.service.traffic_types import Assignment, WorkflowPlan

# Populated by initialization; importing API factories must remain lightweight.
np = contracts = None

DATA_REF = "nyc_tlc_green_2024_01_6x6"
GRID_ID = "nyc-green-6x6-v1"
TASKS = ("state", "flow", "od")
CAPABILITIES = (
    "history.prepare", "traffic.predict", "validate.state", "validate.flow",
    "validate.od", "analysis.congestion", "analysis.od_hotspots", "result.aggregate",
)


def _usage() -> dict:
    """Process CPU/RSS samples; CPU deltas can include concurrent operations."""
    try:
        import psutil
        process = psutil.Process()
        cpu = process.cpu_times()
        return {"rss_mb": process.memory_info().rss / (1024 ** 2),
                "cpu_seconds": cpu.user + cpu.system, "source": "psutil",
                "rss_kind": "current", "rss_native_unit": "bytes"}
    except ImportError:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        divisor = 1024 ** 2 if sys.platform == "darwin" else 1024
        return {"rss_mb": usage.ru_maxrss / divisor,
                "cpu_seconds": usage.ru_utime + usage.ru_stime,
                "source": "resource.getrusage", "rss_kind": "process_peak",
                "rss_native_unit": "bytes" if sys.platform == "darwin" else "KiB"}


def _json(value):
    if isinstance(value, dict):
        return {str(key): _json(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json(item) for item in value]
    if contracts is not None and isinstance(value, contracts.TensorPayload):
        return value.to_dict(include_data=True)
    if isinstance(value, (np.ndarray, np.generic)):
        return value.tolist()
    return value


def _array_bytes(value) -> int:
    if isinstance(value, np.ndarray):
        return value.nbytes
    if isinstance(value, dict):
        return sum(_array_bytes(item) for item in value.values())
    if isinstance(value, (tuple, list)):
        return sum(_array_bytes(item) for item in value)
    return 0


def _cache_size(value, seen=None) -> int:
    """Count payload arrays and Python metadata without duplicate references."""
    seen = set() if seen is None else seen
    if id(value) in seen:
        return 0
    seen.add(id(value))
    size = sys.getsizeof(value)
    if isinstance(value, np.ndarray):
        return size + (0 if value.flags.owndata else value.nbytes)
    if isinstance(value, dict):
        return size + sum(_cache_size(key, seen) + _cache_size(item, seen) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return size + sum(_cache_size(item, seen) for item in value)
    return size


def _freeze(value):
    if isinstance(value, np.ndarray):
        value.setflags(write=False)
    elif contracts is not None and isinstance(value, contracts.TensorPayload):
        value.data.setflags(write=False)
    elif isinstance(value, dict):
        for item in value.values():
            _freeze(item)
    elif isinstance(value, (tuple, list)):
        for item in value:
            _freeze(item)
    return value


def build_backend(service, *, metadata=None, stats=None, normalization_sha256=None):
    """Build deterministic contracts; explicit metadata/stats support unit tests.

    stats maps modality names to the externally fitted (mean, std) arrays.
    The injected service still implements prepare_history/predict and identities.
    """
    global np, contracts
    import numpy as np
    from src.service import traffic_contracts as contracts
    if metadata is None:
        metadata = json.loads(service.metadata_path.read_text())
    if metadata["grid"] != [6, 6] or metadata["step_minutes"] != 30:
        raise ValueError("Joint traffic runtime requires the versioned 6x6/30min grid")
    if stats is None:
        with np.load(service.normalization_path, allow_pickle=False) as fitted:
            stats = {name: (fitted[name + "_mean"].copy(), fitted[name + "_std"].copy())
                     for name in TASKS}
    if normalization_sha256 is None:
        normalization_sha256 = hashlib.sha256(service.normalization_path.read_bytes()).hexdigest()
    storage, inputs, outputs, standardized_outputs, training = {}, {}, {}, {}, {}
    for name in TASKS:
        channels = 36 if name == "od" else 2
        common = dict(field=name, units=(metadata["od_units"],) * channels if name == "od"
                      else tuple(metadata[name + "_units"]),
                      semantic=metadata["od_definition"] if name == "od"
                      else ";".join(metadata[name + "_channels"]),
                      version=service.version, grid_id=GRID_ID, frame_minutes=30)
        axes = ("B", "L", "Y", "X", "D" if name == "od" else "C")
        storage[name] = contracts.TensorSchema(**common, axes=axes, shape=(None, 3, 6, 6, channels))
        inputs[name] = contracts.TensorSchema(
            **common, axes=axes if name == "od" else ("B", "L", "C", "Y", "X"),
            shape=(None, 3, 6, 6, 36) if name == "od" else (None, 3, 2, 6, 6),
            normalization="standardized")
        forecast = dict(common, axes=("B", "H", "Y", "X", "D" if name == "od" else "C"),
                        shape=(None, 1, 6, 6, channels))
        outputs[name] = contracts.TensorSchema(**forecast)
        standardized_outputs[name] = contracts.TensorSchema(**forecast, normalization="standardized")
        mean, std = stats[name]
        training[name] = contracts.TrainingStats(
            **common, mean=tuple(np.asarray(mean).reshape(-1)), std=tuple(np.asarray(std).reshape(-1)),
            source=str(getattr(service, "normalization_path", "injected-training-stats")), split="train")
    registry = contracts.CapabilityRegistry()
    registry.register(contracts.CapabilityContract("B1", service.version, inputs=inputs, outputs=outputs))
    for name in TASKS:
        registry.register(contracts.CapabilityContract("validate_" + name, service.version,
                                                    inputs={name: outputs[name]}))
    return dict(service=service, registry=registry, storage=storage, inputs=inputs, outputs=outputs,
                standardized_outputs=standardized_outputs, stats=training, metadata=metadata,
                normalization=dict(method="standardized", split="train", sha256=normalization_sha256,
                                   statistics={name: item.to_dict() for name, item in training.items()}))


@dataclass
class _Workflow:
    plan: WorkflowPlan
    request: dict
    done: asyncio.Future
    values: dict = field(default_factory=dict)
    traces: dict = field(default_factory=dict)
    profiles: list = field(default_factory=list)
    errors: list = field(default_factory=list)
    prediction_executions: list = field(default_factory=list)
    selected_instances: dict = field(default_factory=dict)


@dataclass
class _Work:
    assignment: Assignment
    future: Future
    reuse: str = "none"
    completed: bool = False
    operation: Any = None
    cache_key: Any = None
    physical_source: dict = field(default_factory=dict)
    execution_origin: str = "direct"
    launch_error: Any = None


class JointTrafficRuntime:
    def __init__(self, experiment_root=None, *, cpu_budget=4, memory_budget=2048,
                 max_concurrency=4, policy="joint", instance_count=2, enable_reuse=True,
                 enable_recovery=True, backend_factory: Callable | None = None,
                 cache_max_entries=32, cache_max_bytes=64 * 1024 ** 2):
        for name, value in (("cpu_budget", cpu_budget), ("memory_budget", memory_budget)):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(name + " must be finite and positive")
        for name, value in (("instance_count", instance_count), ("max_concurrency", max_concurrency),
                            ("cache_max_entries", cache_max_entries), ("cache_max_bytes", cache_max_bytes)):
            if type(value) is not int or value < (0 if name.startswith("cache_") else 1):
                raise ValueError(name + " has an invalid bound")
        self.experiment_root = experiment_root if experiment_root is not None else os.environ.get("TRAFFIC_EXPERIMENT_ROOT")
        self.cpu_budget, self.memory_budget = cpu_budget, memory_budget
        self.max_concurrency, self.policy = max_concurrency, policy
        self.instance_count, self.enable_reuse, self.enable_recovery = instance_count, enable_reuse, enable_recovery
        self.backend_factory = backend_factory
        self.cache_max_entries, self.cache_max_bytes = cache_max_entries, cache_max_bytes
        self.catalog = self.compiler = self.scheduler = None
        self.instances: dict[str, dict] = {}
        self.backends: dict[str, dict] = {}
        self.workflows: dict[str, _Workflow] = {}
        self.model_forward_count = 0
        self.model_forward_successes = 0
        self.cache_hits = 0
        self.singleflight_hits = 0
        self.singleflight_fallbacks = 0
        self.profiles = deque(maxlen=512)
        self.prediction_events = deque(maxlen=512)
        self.operations: dict[str, Callable] = {}
        self._metrics_lock = threading.Lock()
        self._cache = OrderedDict()
        self._cache_bytes = 0
        self._flights: dict[str, Future] = {}
        self._flight_sources: dict[str, dict] = {}
        self._works: dict[str, _Work] = {}
        self._pending_loads: set[Future] = set()
        self._executor = None
        self._loop = None
        self._wake = None
        self._driver = self._initializing = self._shutdown_owner = None
        self.initialized = self.closing = self.closed = False
        self.resident_memory_mb = 0.0

    async def initialize(self):
        if self.closing or self.closed:
            raise RuntimeError("Traffic runtime is closing")
        if self._initializing is None:
            self._loop = asyncio.get_running_loop()
            self._wake = asyncio.Event()
            self._executor = ThreadPoolExecutor(max_workers=self.max_concurrency, thread_name_prefix="joint-traffic")
            self._initializing = self._loop.create_task(self._initialize())
        await asyncio.shield(self._initializing)

    async def _future_done(self, future):
        # A disposable notification cannot propagate cancellation into Future.
        signal = self._loop.create_future()
        def resolve():
            if not signal.done():
                signal.set_result(None)
        future.add_done_callback(lambda done: self._loop.call_soon_threadsafe(resolve))
        await signal
        return future.result()

    def _load_backend(self):
        if self.backend_factory is not None:
            return self.backend_factory(self.experiment_root)
        from src.service.traffic_model import TrafficModelService
        return build_backend(TrafficModelService(self.experiment_root))

    async def _initialize(self):
        global np, contracts
        import numpy as np
        from src.service import traffic_contracts as contracts
        # Imported lazily: importing this module never loads B1 or graph machinery.
        from src.service.traffic_catalog import TrafficCatalog
        from src.service.traffic_composer import TrafficWorkflowCompiler
        from src.graph.traffic_joint_scheduler import JointScheduler
        self.catalog = TrafficCatalog()
        try:
            for index in range(self.instance_count):
                instance_id = f"b1-{index + 1}"
                before, started = _usage(), time.monotonic()
                future = self._executor.submit(self._load_backend)
                self._pending_loads.add(future)
                future.add_done_callback(lambda done: self._loop.call_soon_threadsafe(self._wake.set))
                backend = await self._future_done(future)
                self._pending_loads.discard(future)
                service = backend["service"]
                after = _usage()
                data_bytes = _array_bytes(getattr(service, "_data", {}))
                stats_bytes = _array_bytes(getattr(service, "_stats", {}))
                model_bytes = 0
                owned_bytes = data_bytes + stats_bytes
                model = getattr(service, "_model", None)
                if model is not None:
                    model_bytes = sum(t.numel() * t.element_size() for t in (*model.parameters(), *model.buffers()))
                    owned_bytes += model_bytes
                base_mb = max(owned_bytes / (1024 ** 2), after["rss_mb"] - before["rss_mb"], 0)
                self.resident_memory_mb += base_mb
                self.backends[instance_id] = backend
                self.instances[instance_id] = dict(
                    instance_id=instance_id, implementation_id="B1", version=service.version,
                    backend_kind="injected" if self.backend_factory is not None else "real_B1",
                    checkpoint_sha256=service.checkpoint_sha256, healthy=True, lifecycle="ready",
                    base_memory_mb=base_mb, owned_array_model_mb=owned_bytes / (1024 ** 2),
                    data_resident_mb=data_bytes / (1024 ** 2), model_resident_mb=model_bytes / (1024 ** 2),
                    stats_resident_mb=stats_bytes / (1024 ** 2),
                    model_forward_count=0, model_forward_successes=0, active_forwards=0,
                    load_seconds=time.monotonic() - started, load_rss=after,
                    last_error=None)
                if model is not None:
                    # Count actual torch forwards, including forwards that fail.
                    # Input rejection in service.predict is not a model forward.
                    def count_forward(module, args, instance_id=instance_id):
                        with self._metrics_lock:
                            self.model_forward_count += 1
                            self.instances[instance_id]["model_forward_count"] += 1
                    backend["forward_hook"] = model.register_forward_pre_hook(count_forward)
            first = next(iter(self.backends.values()))
            identity = (first["service"].version, first["service"].checkpoint_sha256,
                        first["normalization"]["sha256"])
            if any((b["service"].version, b["service"].checkpoint_sha256, b["normalization"]["sha256"]) != identity
                   for b in self.backends.values()):
                raise ValueError("B1 instances must load the same checkpoint and normalization")
            task_memory = self.memory_budget - self.resident_memory_mb - self.cache_max_bytes / (1024 ** 2)
            if task_memory <= 0:
                raise ValueError("Resident B1 instances exceed the global memory budget")
            identity_contract = dict(
                data_ref=DATA_REF, data_version=first["metadata"].get("processed_sha256", DATA_REF),
                grid_id=GRID_ID, region_id=GRID_ID, grid=[6, 6], frame_minutes=30,
                model_version=identity[0], schema_version=identity[0], normalization_sha256=identity[2],
                history_length=3, horizon=1, frame_count=getattr(first["service"], "_frame_count", 1488), max_batch=32)
            model_schemas = {name: schema.to_dict() for name, schema in first["inputs"].items()}
            output_schemas = {name: schema.to_dict() for name, schema in first["outputs"].items()}
            ports = {
                "history.prepare": dict(inputs={}, outputs={"inputs": model_schemas}),
                "traffic.predict": dict(inputs={"model_inputs": model_schemas}, outputs=output_schemas),
                "analysis.congestion": dict(inputs={name: output_schemas[name] for name in ("state", "flow")}, outputs={}),
                "analysis.od_hotspots": dict(inputs={"od": output_schemas["od"]}, outputs={}),
                "result.aggregate": dict(inputs={}, outputs={})}
            for name in TASKS:
                ports["validate." + name] = dict(inputs={"prediction": output_schemas[name]}, outputs={name: output_schemas[name]})
            descriptors = {
                capability: dict(resources=dict(cpu=1.0, memory_mb=32.0 if capability == "traffic.predict" else 8.0),
                                 profile=dict(expected_seconds=0.1 if capability == "traffic.predict" else 0.01),
                                 contract=ports[capability])
                for capability in CAPABILITIES}
            self.catalog.register_implementation(dict(implementation_id="B1", version=identity[0],
                                                      capabilities=descriptors, contract=identity_contract))
            for instance_id, state in self.instances.items():
                self.catalog.register_instance(dict(
                    instance_id=instance_id, implementation_id="B1", endpoint="inproc://" + instance_id,
                    max_concurrency=1, healthy=True, base_memory_mb=state["base_memory_mb"]))
            self.compiler = TrafficWorkflowCompiler(self.catalog, identity[0], identity[2])
            self.scheduler = JointScheduler(self.cpu_budget, task_memory, self.max_concurrency,
                                            policy=self.policy, enable_recovery=self.enable_recovery)
            self.initialized = True
            self._driver = self._loop.create_task(self._drive())
        except BaseException as exc:
            if isinstance(exc, asyncio.CancelledError):
                self.closing = True
                # Keep this owner alive during all-task cancellation until its
                # submitted load reaches a real terminal state.
                await self._drain_initialization()
            for state in self.instances.values():
                state.update(healthy=False, lifecycle="failed")
            for backend in self.backends.values():
                if "forward_hook" in backend:
                    backend["forward_hook"].remove()
            self.backends.clear()
            raise

    def _assert_loop(self):
        if asyncio.get_running_loop() is not self._loop:
            raise RuntimeError("Traffic runtime must be used on its owning event loop")

    def submit(self, request: dict, app_id=""):
        self._assert_loop()
        if not self.initialized or self.closing:
            raise RuntimeError("Initialize traffic runtime before submit; runtime must be open")
        if not isinstance(request, dict):
            raise ValueError("Traffic request must be a dict")
        request = copy.deepcopy(request)
        request.setdefault("enable_reuse", self.enable_reuse)
        request.setdefault("enable_recovery", self.enable_recovery)
        if request.get("data_ref", DATA_REF) != DATA_REF:
            raise ValueError("Unsupported real traffic data_ref")
        now = time.monotonic()
        plan = self.compiler.compile(request, app_id=app_id, now=now)
        self._check_terminal_output_sources(plan)
        # Frame validation is an actual data check even in the contract ablation.
        service = next(iter(self.backends.values()))["service"]
        if hasattr(service, "_validate_frames"):
            service._validate_frames(plan.target_frames)
        record = _Workflow(plan, request, self._loop.create_future())
        self.scheduler.submit(plan, now)
        self.workflows[plan.workflow_id] = record
        self._wake.set()
        return plan.workflow_id

    async def run(self, request: dict, app_id=""):
        await self.initialize()
        return await self.wait(self.submit(request, app_id))

    async def wait(self, workflow_id):
        self._assert_loop()
        record = self.workflows[workflow_id]
        try:
            return copy.deepcopy(await asyncio.shield(record.done))
        except asyncio.CancelledError:
            self.cancel(workflow_id, "HTTP waiter cancelled")
            raise

    def cancel(self, workflow_id, reason=None):
        self._assert_loop()
        if workflow_id not in self.workflows:
            raise KeyError(workflow_id)
        self.scheduler.cancel(workflow_id, time.monotonic(), reason=reason)
        self._wake.set()

    def set_instance_health(self, instance_id, healthy, reason=None):
        self._assert_loop()
        if type(healthy) is not bool:
            raise ValueError("healthy must be boolean")
        if instance_id in self.instances:
            self.instances[instance_id].update(healthy=healthy, last_error=reason)
        self.catalog.set_health(instance_id, healthy, reason)
        self.scheduler.set_instance_health(instance_id, healthy, time.monotonic())
        self._wake.set()

    def register_operation(self, capability_id, operation):
        """Bind a catalog capability to an in-process synchronous executor.

        Register its implementation/instance through the public catalog, then
        register a template through compiler.register_template. The callable is
        operation(inputs, parameters, context) -> a mapping of declared output
        ports. Inputs come exclusively from explicit dependency bindings.
        context contains workflow/app/node/instance IDs and absolute frames.
        Plugin work uses the same reservations, profiling and safe completion.
        """
        if self.closing or self.closed:
            raise RuntimeError("Traffic runtime is closing")
        if not isinstance(capability_id, str) or not capability_id.strip() or not callable(operation):
            raise ValueError("register_operation requires a capability ID and callable")
        if capability_id in self.operations or capability_id in CAPABILITIES:
            raise ValueError("Capability executor is already registered")
        self.operations[capability_id] = operation

    def _cache_key(self, record, node_id, target_instance):
        backend = next(iter(self.backends.values()))
        service = backend["service"]
        identity = dict(version=service.version, checkpoint=service.checkpoint_sha256,
                        data_ref=record.request.get("data_ref", DATA_REF),
                        data_sha256=backend["metadata"].get("processed_sha256"),
                        target_frames=list(record.plan.target_frames), history_frames=3, horizon=1,
                        normalization=backend["normalization"]["sha256"], grid=GRID_ID,
                        input_schemas={name: schema.to_dict() for name, schema in backend["inputs"].items()})
        node = next(node for node in record.plan.nodes if node.node_id == node_id)
        binding = node.parameters["inputs"]["model_inputs"]
        source_instance = record.selected_instances[binding["node_id"]]
        identity["bound_model_inputs"] = self._binding_identity(record.values[binding["node_id"]][binding["output"]])
        # Schemas describe the actual producer/consumer connection; instance
        # IDs alone do not prevent equivalent windows sharing a physical call.
        identity["binding_contracts"] = node.parameters.get("candidate_adapter_schemas", {}).get(
            target_instance, {}).get("model_inputs", {}).get(source_instance, {})
        identity = _json(identity)
        return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    def _binding_identity(self, value):
        if isinstance(value, np.ndarray):
            return dict(dtype=value.dtype.str, shape=list(value.shape),
                        sha256=hashlib.sha256(value.tobytes(order="C")).hexdigest())
        if isinstance(value, contracts.TensorPayload):
            return dict(schema=value.schema.to_dict(), data=self._binding_identity(value.data), source=value.source)
        if isinstance(value, dict):
            return {key: self._binding_identity(item) for key, item in value.items()}
        if isinstance(value, (list, tuple)):
            return [self._binding_identity(item) for item in value]
        if isinstance(value, np.generic):
            return value.item()
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise contracts.ContractError("binding", "Unsupported model-input value for reuse identity")

    @staticmethod
    def _schema(declaration):
        if isinstance(declaration, contracts.TensorSchema):
            return declaration
        if not isinstance(declaration, dict):
            raise contracts.ContractError("schema", "Binding adapter requires source/target tensor schemas")
        values = dict(declaration)
        try:
            for name in ("axes", "shape", "units"):
                values[name] = tuple(values[name])
            return contracts.TensorSchema(**values)
        except (KeyError, TypeError, ValueError) as exc:
            raise contracts.ContractError("schema", "Invalid binding schema: " + str(exc)) from exc

    def _training_stats(self, schema, instance_id):
        backends = ([self.backends[instance_id]] if instance_id in self.backends else [])
        backends += [backend for key, backend in self.backends.items() if key != instance_id]
        stats = next((backend["stats"][schema.field] for backend in backends
                      if schema.field in backend.get("stats", {})), None)
        if not isinstance(stats, contracts.TrainingStats):
            raise contracts.ContractError("stats", "No declared TRAIN statistics for " + schema.field)
        for name in ("field", "semantic", "version", "grid_id", "frame_minutes"):
            if getattr(stats, name) != getattr(schema, name):
                raise contracts.ContractError("stats", "Binding TRAIN statistics " + name + " mismatch")
        if stats.units == schema.units:
            return stats, []
        # Re-express the original TRAIN mean/std with the same deterministic
        # linear unit adapter. Never fit statistics or change the artifact.
        axis = schema.channel_axis
        if axis is None:
            raise contracts.ContractError("stats", "Unit-converted statistics require a declared channel axis")
        common = dict(field=stats.field, axes=(axis,), shape=(len(stats.units),),
                      semantic=stats.semantic, version=stats.version, grid_id=stats.grid_id,
                      frame_minutes=stats.frame_minutes)
        old = contracts.TensorSchema(**common, units=stats.units)
        new = contracts.TensorSchema(**common, units=schema.units)
        mean, mean_trace = contracts.adapter(np.asarray(stats.mean, dtype=np.float64), old, new)
        std, std_trace = contracts.adapter(np.asarray(stats.std, dtype=np.float64), old, new)
        converted = replace(stats, units=schema.units, mean=tuple(mean), std=tuple(std))
        return converted, [dict(rule="training_stats_unit_conversion", source=stats.to_dict(),
                                target=converted.to_dict(), mean_trace=mean_trace, std_trace=std_trace)]

    def _adapt_array(self, data, source, target, source_instance, target_instance):
        # Different dynamic shape declarations can still require no numerical
        # operation. Validate both sides and preserve float32 bits in that case.
        old, new = source.to_dict(), target.to_dict()
        old.pop("shape")
        new.pop("shape")
        if old == new:
            contracts.validate(data, source)
            contracts.validate(data, target)
            return _freeze(data.copy()), [dict(rule="identity", source=source.to_dict(), target=target.to_dict())]
        stats, trace = {}, []
        for side, schema, instance in (("source", source, source_instance), ("target", target, target_instance)):
            if schema.normalization == "standardized":
                stats[side], derivation = self._training_stats(schema, instance)
                trace.extend(dict(item, side=side) for item in derivation)
        result, rules = contracts.adapter(data, source, target, stats=stats or None)
        return _freeze(result), trace + rules

    def _adapt_tensor_value(self, value, source, target, source_instance, target_instance):
        if isinstance(value, np.ndarray):
            return self._adapt_array(value, source, target, source_instance, target_instance)
        if isinstance(value, contracts.TensorPayload):
            actual, declared = value.schema.to_dict(), source.to_dict()
            actual.pop("shape")
            declared.pop("shape")
            if actual != declared:
                raise contracts.ContractError("schema", "TensorPayload contradicts its compiled source schema")
            contracts.validate(value.data, value.schema)
            data, trace = self._adapt_array(value.data, source, target, source_instance, target_instance)
            return contracts.TensorPayload(data, target, value.source), trace
        if not isinstance(value, dict) or not {"prediction", "normalized", "shape", "layout", "units", "channels"} <= value.keys():
            raise contracts.ContractError("binding", "Declared tensor adapter cannot handle this bound value")
        layout = ",".join(source.axes)
        layouts = {layout, layout.replace("D", "C")} if source.channel_axis == "D" else {layout}
        if value["layout"] not in layouts or value["units"] != list(source.units):
            raise contracts.ContractError("schema", "Prediction metadata contradicts its source schema")
        if not isinstance(value["prediction"], np.ndarray) or value["shape"] != list(value["prediction"].shape):
            raise contracts.ContractError("shape", "Prediction metadata shape mismatch")
        data, raw_trace = self._adapt_array(value["prediction"], source, target, source_instance, target_instance)
        normalized_source = replace(source, normalization="standardized")
        normalized_target = replace(target, normalization="standardized")
        normalized, normalized_trace = self._adapt_array(value["normalized"], normalized_source, normalized_target,
                                                         source_instance, target_instance)
        if source == target:
            result = copy.deepcopy(value)
        else:
            target_stats, stats_trace = self._training_stats(normalized_target, target_instance)
            result = dict(value, shape=list(data.shape), layout=",".join(target.axes), units=list(target.units),
                          normalization=target.normalization, schema=target.to_dict(),
                          normalized_schema=normalized_target.to_dict(), normalization_statistics=target_stats.to_dict())
            normalized_trace += stats_trace
        result.update(prediction=data, normalized=normalized)
        return _freeze(result), dict(prediction=raw_trace, normalized=normalized_trace)

    def _adapt_binding(self, value, fields, source_instance, target_instance, port):
        if not fields:
            return _freeze(copy.deepcopy(value)), {}
        pairs = {}
        for name, declaration in fields.items():
            if not isinstance(declaration, dict) or set(declaration) != {"source", "target"}:
                raise contracts.ContractError("schema", "Binding field requires explicit source and target")
            pairs[name] = (self._schema(declaration["source"]), self._schema(declaration["target"]))
        if isinstance(value, (np.ndarray, contracts.TensorPayload)) or isinstance(value, dict) and "prediction" in value:
            if len(pairs) != 1:
                raise contracts.ContractError("binding", "Single tensor value requires exactly one field schema")
            name, (source, target) = next(iter(pairs.items()))
            result, trace = self._adapt_tensor_value(value, source, target, source_instance, target_instance)
            return result, {name: trace}
        if not isinstance(value, dict):
            raise contracts.ContractError("binding", "Unsupported structure for planned binding adapter")
        result, trace = copy.deepcopy(value), {}
        for name, (source, target) in pairs.items():
            if name not in value:
                raise contracts.ContractError("binding", "Missing declared bundle field " + name)
            adapted, trace[name] = self._adapt_tensor_value(value[name], source, target, source_instance, target_instance)
            result[name] = adapted.data if port == "model_inputs" and isinstance(adapted, contracts.TensorPayload) else adapted
        return _freeze(result), trace

    def _bound_inputs(self, record, node, target_instance):
        bindings = node.parameters.get("inputs", {})
        inputs, traces = {}, {}
        for port, binding in bindings.items():
            producer, output = binding["node_id"], binding["output"]
            if producer not in node.depends_on or producer not in record.values:
                raise contracts.ContractError("binding", "input producer must be a successful dependency")
            value = record.values[producer]
            if not isinstance(value, dict) or output not in value:
                raise contracts.ContractError("binding", "undeclared producer output " + output)
            source_instance = record.selected_instances.get(producer)
            if source_instance is None:
                raise contracts.ContractError("binding", "Producer has no confirmed selected instance")
            declarations = node.parameters.get("candidate_adapter_schemas")
            planned = node.parameters.get("candidate_adapters", {}).get(target_instance, {})
            binding_rules = [rule for key, rules in planned.items()
                             if key.startswith(producer + ":" + source_instance + ":")
                             and (port == "model_inputs" or key.endswith(":" + output)) for rule in rules]
            if declarations is None:
                if any(rule.get("rule") != "validate" for rule in binding_rules):
                    raise contracts.ContractError("binding", "Planned adapter rules require executable source/target schemas")
                inputs[port] = _freeze(copy.deepcopy(value[output]))
                continue
            if port not in declarations.get(target_instance, {}):
                if any(rule.get("rule") != "validate" for rule in binding_rules):
                    raise contracts.ContractError("binding", "Declared conversion lacks executable binding schemas")
                inputs[port] = _freeze(copy.deepcopy(value[output]))
                continue
            try:
                fields = declarations[target_instance][port][source_instance]
            except (KeyError, TypeError) as exc:
                raise contracts.ContractError("binding", "Missing adapter schema for actual producer/consumer instances") from exc
            if not fields and any(rule.get("rule") != "validate" for rule in binding_rules):
                raise contracts.ContractError("binding", "Planned conversion cannot use an empty field schema")
            inputs[port], trace = self._adapt_binding(value[output], fields, source_instance, target_instance, port)
            if fields:
                traces[port] = dict(source_node=producer, source_instance=source_instance,
                                    target_instance=target_instance, fields=trace)
        if traces:
            record.traces[node.node_id] = {"bindings": traces}
        return inputs

    def _prepare(self, backend, record):
        raw = backend["service"].prepare_history(record.plan.target_frames)
        payload = contracts.Payload({name: contracts.TensorPayload(values, backend["storage"][name], DATA_REF)
                                     for name, values in raw.items()})
        adapted, trace = backend["registry"].adapt("B1", backend["service"].version, payload, stats=backend["stats"])
        inputs = {name: np.asarray(tensor.data, dtype=np.float32) for name, tensor in adapted.tensors.items()}
        inputs["target_frames"] = tuple(record.plan.target_frames)
        return dict(inputs=_freeze(inputs), schema_trace=trace)

    def _predict(self, instance_id, backend, model_inputs):
        state = self.instances[instance_id]
        with self._metrics_lock:
            if "forward_hook" not in backend:
                self.model_forward_count += 1
                state["model_forward_count"] += 1
            state["active_forwards"] += 1
            state["lifecycle"] = "running"
        try:
            result = backend["service"].predict(model_inputs, task="all")
            if not isinstance(result, dict):
                raise contracts.ContractError("schema", "B1 must return a prediction mapping")
            if result.get("version") != backend["service"].version or result.get("checkpoint_sha256") != backend["service"].checkpoint_sha256:
                raise contracts.ContractError("version", "B1 result identity mismatch")
            if not isinstance(result.get("predictions"), dict) or set(result["predictions"]) != set(TASKS):
                raise contracts.ContractError("modality", "B1 must return all three heads")
            # The reusable result contains all heads, even when this workflow
            # requests only flow. Admit no unchecked head into a shared cache.
            for name in TASKS:
                self._check_prediction_item(backend, len(model_inputs["target_frames"]),
                                            name, result["predictions"][name])
            with self._metrics_lock:
                self.model_forward_successes += 1
                state["model_forward_successes"] += 1
            return _freeze(result["predictions"])
        finally:
            with self._metrics_lock:
                state["active_forwards"] -= 1
                state["lifecycle"] = "ready"

    def _check_prediction_item(self, backend, batch_size, name, item):
        required = {"prediction", "normalized", "shape", "layout", "units", "channels"}
        if not isinstance(item, dict) or not required <= item.keys():
            raise contracts.ContractError("schema", name + ": incomplete prediction metadata")
        expected = backend["outputs"][name]
        channels = [f"destination_{i}" for i in range(36)] if name == "od" else backend["metadata"][name + "_channels"]
        shape = [batch_size, 1, 6, 6, len(expected.units)]
        if (item["layout"] != "B,H,Y,X,C" or item["units"] != list(expected.units) or item["channels"] != channels):
            raise contracts.ContractError("schema", name + ": layout/units/channel order mismatch")
        if not isinstance(item["prediction"], np.ndarray) or not isinstance(item["normalized"], np.ndarray):
            raise contracts.ContractError("data", name + ": prediction arrays must be numpy.ndarray")
        if list(item["prediction"].shape) != shape or list(item["normalized"].shape) != shape or item["shape"] != shape:
            raise contracts.ContractError("shape", name + ": batch/horizon shape mismatch")
        contracts.validate(item["normalized"], backend["standardized_outputs"][name])
        contracts.validate(item["prediction"], expected)
        if (item["prediction"] < 0).any():
            raise contracts.ContractError("negative", name + ": physical prediction must be nonnegative")

    def _validate(self, backend, record, name, item):
        # Candidate semantic-filter ablation never disables data safety.
        self._check_prediction_item(backend, len(record.plan.target_frames), name, item)
        return {name: item}

    def _congestion(self, record, inputs, parameters):
        state = inputs["state"]["prediction"][:, 0]
        flow = inputs["flow"]["prediction"][:, 0]
        speed_limit = float(parameters.get("speed_threshold_mph", record.request.get("speed_threshold_mph", 10.0)))
        active_limit = float(parameters.get("active_trip_threshold", record.request.get("active_trip_threshold", 1.0)))
        if not math.isfinite(speed_limit) or not math.isfinite(active_limit) or speed_limit <= 0 or active_limit < 0:
            raise ValueError("Congestion rule thresholds must be finite and nonnegative")
        # Zero speed is a missing sentinel; dropoffs indicate proxy observation support.
        supported = (flow[..., 0] > 0) & (state[..., 1] > 0)
        congested = supported & (state[..., 1] < speed_limit) & (state[..., 0] >= active_limit)
        return dict(method="proxy_rule", definition="dropoffs>0 and speed>0 and speed<threshold and active_trips>=threshold",
                    speed_threshold_mph=speed_limit, active_trip_threshold=active_limit,
                    limitations="Trip-derived activity/speed proxies; not measured road-link congestion.",
                    frames=[dict(target_frame=int(frame), supported_cells=int(supported[i].sum()),
                                 congested_cells=int(congested[i].sum()), mask=congested[i].tolist(),
                                 mean_supported_speed_mph=float(state[i, ..., 1][supported[i]].mean()) if supported[i].any() else None)
                            for i, frame in enumerate(record.plan.target_frames)])

    def _hotspots(self, record, inputs, parameters):
        top_k = parameters.get("top_k", record.request.get("top_k", 10))
        if type(top_k) is not int or not 1 <= top_k <= 1296:
            raise ValueError("OD top_k must be an integer in [1,1296]")
        od = inputs["od"]["prediction"][:, 0].reshape(-1, 36, 36)
        frames = []
        for frame, matrix in zip(record.plan.target_frames, od):
            order = np.argsort(-matrix.ravel(), kind="stable")[:top_k]
            pairs = [dict(origin=int(index // 36), destination=int(index % 36),
                          origin_cell=[int(index // 36 // 6), int(index // 36 % 6)],
                          destination_cell=[int(index % 36 // 6), int(index % 36 % 6)],
                          trips=float(matrix.ravel()[index])) for index in order if matrix.ravel()[index] > 0]
            frames.append(dict(target_frame=int(frame), pairs=pairs,
                               origin_totals=matrix.sum(axis=1).tolist(), destination_totals=matrix.sum(axis=0).tolist()))
        return dict(method="predicted_completed_od_rank", definition="completed trips by origin,destination at dropoff interval; row-major cells; positive pairs descending with stable cell tie-break",
                    units="trips/30min", top_k=top_k, frames=frames)

    def _operation(self, assignment, record):
        return lambda: self._execute_operation(assignment, record)

    def _execute_operation(self, assignment, record):
        node = next(node for node in record.plan.nodes if node.node_id == assignment.node_id)
        result = self._execute_capability(assignment, record, node)
        declared = set(node.parameters.get("output_ports", ()))
        if not isinstance(result, dict) or declared and set(result) != declared:
            raise contracts.ContractError("ports", "Execution result differs from declared output ports: " + node.node_id)
        if node.capability_id == "result.aggregate":
            expected = set(node.parameters["inputs"]) & set(record.plan.requested_outputs)
            if not isinstance(result.get("predictions"), dict) or set(result["predictions"]) != expected:
                raise contracts.ContractError("ports", "Aggregate omitted or changed its bound requested outputs")
        return result

    def _execute_capability(self, assignment, record, node):
        capability = node.capability_id
        inputs = self._bound_inputs(record, node, assignment.instance_id)
        if capability in self.operations:
            operation = self.operations[capability]
            parameters = _json(node.parameters)
            context = dict(workflow_id=record.plan.workflow_id, app_id=record.plan.app_id,
                           node_id=node.node_id, instance_id=assignment.instance_id,
                           target_frames=record.plan.target_frames, template=record.plan.template)
            output = operation(_freeze(copy.deepcopy(inputs)), parameters, context)
            return output
        backend = self.backends.get(assignment.instance_id)
        if capability == "history.prepare":
            if backend is None:
                raise RuntimeError("Selected instance has no loaded B1 backend")
            return self._prepare(backend, record)
        if capability == "traffic.predict":
            model_inputs = inputs["model_inputs"]
            if tuple(model_inputs.get("target_frames", ())) != record.plan.target_frames:
                raise contracts.ContractError("interval", "Bound model inputs must keep the plan's absolute target frames")
            return self._predict(assignment.instance_id, backend, model_inputs)
        if capability.startswith("validate."):
            name = capability.split(".", 1)[1]
            prediction = inputs["prediction"]
            return self._validate(backend, record, name, prediction)
        if capability.startswith("analysis."):
            operation = self._congestion if capability == "analysis.congestion" else self._hotspots
            return {"analysis": operation(record, inputs, node.parameters)}
        if capability == "result.aggregate":
            selected = {name: inputs[name] for name in record.plan.requested_outputs if name in inputs}
            analysis = {record.plan.template: inputs["analysis"]} if "analysis" in inputs else {}
            return dict(predictions=selected, analysis=analysis)
        raise ValueError("Unsupported runtime capability: " + capability)

    def _measure(self, assignment, operation):
        before, started = _usage(), time.monotonic()
        thread_cpu = time.thread_time()
        forward_before = self.instances.get(assignment.instance_id, {}).get("model_forward_count", 0)
        result, error = None, None
        try:
            result = operation()
        except BaseException as exc:
            error = exc
        after = _usage()
        profile = dict(task_id=assignment.task_id, instance_id=assignment.instance_id,
                       capability_id=assignment.candidate.capability_id, attempt=assignment.attempt,
                       started_at=started, finished_at=time.monotonic(), duration_seconds=time.monotonic() - started,
                       thread_cpu_seconds=time.thread_time() - thread_cpu,
                       process_cpu_seconds=max(0, after["cpu_seconds"] - before["cpu_seconds"]),
                       cpu_scope="thread CPU is per operation; process CPU includes concurrent operations",
                       rss_before_mb=before["rss_mb"], rss_after_mb=after["rss_mb"],
                       rss_delta_mb=after["rss_mb"] - before["rss_mb"],
                       rss_source=after["source"], rss_kind=after["rss_kind"], rss_native_unit=after["rss_native_unit"],
                       success=error is None)
        profile.update(self._physical_source(assignment))
        profile["model_forward_count"] = max(0, self.instances.get(assignment.instance_id, {}).get("model_forward_count", 0) - forward_before)
        profile["call_status"], profile["quality_status"] = self._outcome_status(error)
        return result, error, profile

    @staticmethod
    def _outcome_status(error):
        if error is None:
            return "success", "valid"
        if isinstance(error, contracts.ContractError):
            return "success", "invalid"
        return "failed", "not_evaluated"

    @staticmethod
    def _physical_source(assignment):
        return dict(physical_source_instance=assignment.instance_id,
                    physical_source_task_id=assignment.task_id,
                    physical_source_attempt=assignment.attempt,
                    physical_source_reservation_id=assignment.reservation_id)

    def _prediction_event(self, work, event, *, profile=None, error=None):
        source = dict(work.physical_source)
        if profile is not None:
            source["source_model_forward_count"] = profile["model_forward_count"]
        value = dict(source, event=event, now=time.monotonic(),
                     workflow_id=work.assignment.workflow_id, task_id=work.assignment.task_id,
                     instance_id=work.assignment.instance_id, attempt=work.assignment.attempt,
                     reservation_id=work.assignment.reservation_id,
                     reuse=work.reuse, execution_origin=work.execution_origin,
                     model_forward_count=profile["model_forward_count"] if profile is not None and work.reuse == "none" else 0,
                     error=None if error is None else str(error))
        value["call_status"], value["quality_status"] = self._outcome_status(error)
        self.prediction_events.append(value)
        self.workflows[work.assignment.workflow_id].prediction_executions.append(value)
        return value

    def _track_flight(self, key, future, source):
        # Multiple former followers can execute independently after a source
        # failure. Do not overwrite another physical Future's ownership.
        if key not in self._flights:
            self._flights[key] = future
            self._flight_sources[key] = dict(source)
        future.add_done_callback(lambda done: self._loop.call_soon_threadsafe(self._flight_finished, key, done))

    def _flight_finished(self, key, future):
        if self.closed:
            return
        if self._flights.get(key) is future:
            del self._flights[key]
            self._flight_sources.pop(key, None)
        if future.cancelled():
            return
        try:
            result, error, profile = future.result()
        except BaseException:
            return
        if error is not None:
            return
        source = {name: profile[name] for name in self._physical_source_fields()}
        source["source_model_forward_count"] = profile["model_forward_count"]
        size = _cache_size((result, source)) + sys.getsizeof(key) + sys.getsizeof((None, 0, None))
        if not self.cache_max_entries or size > self.cache_max_bytes:
            return
        if key in self._cache:
            self._cache_bytes -= self._cache.pop(key)[1]
        self._cache[key] = (result, size, source)
        self._cache_bytes += size
        while len(self._cache) > self.cache_max_entries or self._cache_bytes > self.cache_max_bytes:
            _, (_, removed, _) = self._cache.popitem(last=False)
            self._cache_bytes -= removed

    @staticmethod
    def _physical_source_fields():
        return ("physical_source_instance", "physical_source_task_id",
                "physical_source_attempt", "physical_source_reservation_id")

    def _dispatch(self, assignment):
        record = self.workflows[assignment.workflow_id]
        capability = assignment.candidate.capability_id
        reuse = "none"
        try:
            operation = self._operation(assignment, record)
            key = self._cache_key(record, assignment.node_id, assignment.instance_id) if capability == "traffic.predict" and record.request.get("enable_reuse", True) else None
            # Deadline/health checks happen before launching synchronous work.
            self.scheduler.start(assignment.task_id, time.monotonic())
            source = self._physical_source(assignment)
            if key in self._cache:
                result, _, source = self._cache[key]
                self._cache.move_to_end(key)
                self.cache_hits += 1
                reuse = "cache"
                future = Future()
                future.set_result((result, None, None))
            elif key is not None and key in self._flights:
                self.singleflight_hits += 1
                reuse = "singleflight"
                future = self._flights[key]
                source = self._flight_sources[key]
            else:
                future = self._executor.submit(self._measure, assignment, operation)
                if key is not None:
                    self._track_flight(key, future, source)
            # Start and register on the owner loop before any completion callback runs.
            work = _Work(assignment, future, reuse, operation=operation,
                         cache_key=key, physical_source=dict(source))
            self._works[assignment.task_id] = work
            if capability == "traffic.predict":
                self._prediction_event(work, "prediction_dispatched")
            future.add_done_callback(lambda done: self._loop.call_soon_threadsafe(self._finish, work))
        except Exception as exc:
            self.scheduler.start_failed(assignment.task_id, time.monotonic(), error=str(exc))
            record.errors.append(str(exc))

    def _execute_failed_follower(self, work, task, workflow, profile, error):
        """A failed shared source is not an execution on this candidate.

        Preserve the logical attempt, timeout origin and reservation. Scheduler
        retry begins only if this candidate's own physical call later fails.
        """
        self._prediction_event(work, "singleflight_source_failed", profile=profile, error=error)
        expired = time.monotonic() - task["started_at"] >= task["timeout_seconds"]
        if (self.closing or task["cancel_requested"] or task["timeout_requested"]
                or workflow["cancel_requested"] or expired):
            self._prediction_event(work, "singleflight_fallback_suppressed", profile=profile)
            return False
        try:
            future = self._executor.submit(self._measure, work.assignment, work.operation)
        except Exception as exc:
            work.launch_error = exc
            work.physical_source = {name: None for name in self._physical_source_fields()}
            work.execution_origin = "singleflight_fallback_launch_failed"
            self._prediction_event(work, "singleflight_fallback_launch_failed", error=exc)
            return False
        replacement = _Work(work.assignment, future, operation=work.operation,
                            cache_key=work.cache_key, physical_source=self._physical_source(work.assignment),
                            execution_origin="singleflight_fallback")
        work.completed = True
        self._works[work.assignment.task_id] = replacement
        self.singleflight_fallbacks += 1
        if work.cache_key is not None:
            self._track_flight(work.cache_key, future, replacement.physical_source)
        self._prediction_event(replacement, "singleflight_fallback_started")
        future.add_done_callback(lambda done: self._loop.call_soon_threadsafe(self._finish, replacement))
        return True

    def _finish(self, work):
        if work.completed or not work.future.done():
            return
        assignment = work.assignment
        current = self._works.get(assignment.task_id)
        token = (assignment.task_id, assignment.attempt, assignment.reservation_id)
        if current is not work or token != (current.assignment.task_id, current.assignment.attempt, current.assignment.reservation_id):
            return
        snapshot = self.scheduler.snapshot(assignment.workflow_id)
        task = snapshot["tasks"][assignment.task_id]
        if (task["attempt"], task["reservation_id"]) != (assignment.attempt, assignment.reservation_id):
            return
        record = self.workflows[assignment.workflow_id]
        if work.future.cancelled():
            result, error, profile = None, RuntimeError("Executor work cancelled before start"), None
        else:
            try:
                result, error, profile = work.future.result()
            except BaseException as exc:
                result, error, profile = None, exc, None
        if work.reuse == "singleflight" and error is not None and self._execute_failed_follower(
                work, task, snapshot["workflows"][assignment.workflow_id], profile, error):
            return
        if work.launch_error is not None:
            result, error, profile = None, work.launch_error, None
        work.completed = True
        try:
            if profile is not None and work.reuse == "none":
                profile = dict(profile, execution_origin=work.execution_origin)
                record.profiles.append(profile)
                self.profiles.append(profile)
                try:
                    self.catalog.record_execution(assignment.instance_id, assignment.candidate.capability_id,
                                                  profile["duration_seconds"], error is None)
                except Exception as exc:
                    record.errors.append("Profile recording failed: " + str(exc))
            if error is None:
                record.values[assignment.node_id] = _freeze(copy.deepcopy(result))
                record.selected_instances[assignment.node_id] = assignment.instance_id
                if assignment.candidate.capability_id == "history.prepare":
                    record.traces[assignment.node_id] = result["schema_trace"]
            else:
                record.errors.append(str(error))
                if work.reuse == "none":
                    if assignment.instance_id in self.instances:
                        self.instances[assignment.instance_id]["last_error"] = str(error)
            attribution = self._prediction_event(work, "prediction_finished", profile=profile, error=error) if assignment.candidate.capability_id == "traffic.predict" else {}
            call_status, quality_status = self._outcome_status(error)
            if work.launch_error is not None:
                self.scheduler.start_failed(assignment.task_id, time.monotonic(), error=str(error))
            else:
                self.scheduler.finish(assignment.task_id, time.monotonic(),
                                      call_status=call_status, quality_status=quality_status,
                                      result=dict(attribution, available=error is None, reuse=work.reuse),
                                      error=None if error is None else str(error))
        finally:
            self._works.pop(assignment.task_id, None)
            self._wake.set()

    @staticmethod
    def _terminal_nodes(plan):
        consumed = {dependency for node in plan.nodes for dependency in node.depends_on}
        return tuple(node for node in plan.nodes if node.node_id not in consumed)

    @staticmethod
    def _export_origin(node, name):
        if node.capability_id == "result.aggregate":
            binding = node.parameters.get("inputs", {}).get(name, {})
            if "node_id" in binding and "output" in binding:
                return binding["node_id"], binding["output"]
        return node.node_id, name

    def _check_terminal_output_sources(self, plan):
        sources = {}
        for node in self._terminal_nodes(plan):
            ports = node.parameters.get("inputs", {}) if node.capability_id == "result.aggregate" else node.parameters.get("output_ports", ())
            for name in set(ports) & set(plan.requested_outputs):
                origin = self._export_origin(node, name)
                if name in sources and sources[name][0] != origin:
                    raise contracts.ContractError("ambiguous", f"Requested output {name} has multiple terminal producers: {sources[name][1]}, {node.node_id}")
                sources[name] = origin, node.node_id
        missing = set(plan.requested_outputs) - sources.keys()
        if missing:
            raise contracts.ContractError("ports", "Requested outputs lack terminal producers: " + ", ".join(sorted(missing)))

    def _collect_terminal_results(self, record):
        predictions, analysis, origins = {}, {}, {}
        for node in self._terminal_nodes(record.plan):
            value = record.values.get(node.node_id)
            if not isinstance(value, dict):
                raise contracts.ContractError("ports", "Terminal producer returned no port mapping: " + node.node_id)
            if node.capability_id == "result.aggregate":
                outputs = value.get("predictions", {})
                for name, item in value.get("analysis", {}).items():
                    if name in analysis:
                        if self._binding_identity(analysis[name]) != self._binding_identity(item):
                            raise contracts.ContractError("ambiguous", "Multiple terminal analyses expose " + name)
                        continue
                    analysis[name] = item
            else:
                outputs = value
                if "analysis" in value:
                    analysis[node.node_id] = value["analysis"]
            for name in record.plan.requested_outputs:
                if name not in outputs:
                    continue
                if name in predictions:
                    if origins[name] != self._export_origin(node, name) or self._binding_identity(predictions[name]) != self._binding_identity(outputs[name]):
                        raise contracts.ContractError("ambiguous", "Multiple terminal producers expose " + name)
                    continue
                predictions[name] = outputs[name]
                origins[name] = self._export_origin(node, name)
        missing = set(record.plan.requested_outputs) - predictions.keys()
        if missing:
            raise contracts.ContractError("ports", "Missing requested business outputs: " + ", ".join(sorted(missing)))
        return dict(predictions=predictions, analysis=analysis)

    def _settle(self):
        snapshot = self.scheduler.snapshot()
        active = {work.assignment.workflow_id for work in self._works.values()}
        for workflow_id, state in snapshot["workflows"].items():
            record = self.workflows[workflow_id]
            if record.done.done() or workflow_id in active or not state.get("done", False):
                continue
            aggregate = {}
            if state["status"] == "completed":
                try:
                    aggregate = self._collect_terminal_results(record)
                except contracts.ContractError as exc:
                    # An integrity failure must be observable to waiters; never
                    # leave a completed DAG waiting or return an empty success.
                    record.errors.append(str(exc))
                    record.done.set_exception(exc)
                    record.values.clear()
                    continue
            response = dict(workflow_id=workflow_id, app_id=record.plan.app_id, template=record.plan.template,
                            status=state["status"], done=True, submitted_at=state.get("submitted_at"),
                            finished_at=state.get("finished_at"), target_frames=list(record.plan.target_frames),
                            deadline=state.get("deadline"), deadline_missed=state.get("deadline_missed", False),
                            deadline_missed_at=state.get("deadline_missed_at"),
                            history_frames=3, history_minutes=90, horizon=1,
                            window_semantics="independent one-step predictions from [t-3,t); no recursive rollout",
                            model_version=next(iter(self.instances.values()))["version"],
                            checkpoint_sha256=next(iter(self.instances.values()))["checkpoint_sha256"],
                            normalization=next(iter(self.backends.values()))["normalization"],
                            schema_trace=record.traces, profiles=record.profiles, errors=record.errors,
                            prediction_executions=record.prediction_executions,
                            predictions=aggregate.get("predictions", {}), analysis=aggregate.get("analysis", {}))
            response["tasks"] = {task_id: item for task_id, item in snapshot["tasks"].items()
                                 if item["workflow_id"] == workflow_id}
            record.done.set_result(_json(response))
            # Runtime owns tensors only while this DAG executes; public response is independent.
            record.values.clear()

    async def _drive(self):
        try:
            await self._drive_loop()
        except asyncio.CancelledError:
            # Shutdown owns release; a cancelled driver only waits for that
            # independent owner so asyncio.run cannot close the loop early.
            while not self.closed:
                self._ensure_shutdown()
                await self._drain_pause()
            raise

    async def _drive_loop(self):
        while not self.closed:
            self._wake.clear()
            if not self.closing:
                for assignment in self.scheduler.tick(time.monotonic()):
                    self._dispatch(assignment)
            else:
                self.scheduler.tick(time.monotonic())
            self._settle()
            if self.closing and not self._works:
                return
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=0.02)
            except asyncio.TimeoutError:
                pass

    def snapshot(self, workflow_id=None):
        scheduler = self.scheduler.snapshot(workflow_id) if self.scheduler is not None else {}
        with self._metrics_lock:
            instances = copy.deepcopy(self.instances)
        response = dict(scheduler, initialized=self.initialized, closing=self.closing, closed=self.closed,
                    catalog=self.catalog.snapshot() if self.catalog is not None else {},
                    scheduler=scheduler, instances=instances, model_forward_count=self.model_forward_count,
                    model_forward_successes=self.model_forward_successes,
                    cache=dict(entries=len(self._cache), bytes=self._cache_bytes, max_entries=self.cache_max_entries,
                               max_bytes=self.cache_max_bytes, hits=self.cache_hits,
                               singleflight_hits=self.singleflight_hits, inflight=len(self._flights)),
                    resources=dict(scheduler.get("resources", {}), global_cpu_budget=self.cpu_budget, global_memory_budget_mb=self.memory_budget,
                                   resident_memory_mb=self.resident_memory_mb,
                                   data_resident_mb=sum(instance.get("data_resident_mb", 0) for instance in instances.values()),
                                   model_resident_mb=sum(instance.get("model_resident_mb", 0) for instance in instances.values()),
                                   cache_resident_allowance_mb=self.cache_max_bytes / (1024 ** 2),
                                   cache_used_mb=self._cache_bytes / (1024 ** 2),
                                   task_memory_budget_mb=self.memory_budget - self.resident_memory_mb - self.cache_max_bytes / (1024 ** 2),
                                   task_reservation_scope="incremental operation/request buffers; singleflight followers hold request quotas",
                                   measurement=_usage()), profiles=copy.deepcopy(list(self.profiles)),
                    active_futures=len({work.future for work in self._works.values()}
                                       | set(self._flights.values()) | self._pending_loads),
                    active_consumers=len(self._works))
        response["initialization_futures"] = len(self._pending_loads)
        response["cache"]["singleflight_fallbacks"] = self.singleflight_fallbacks
        response["prediction_events"] = copy.deepcopy(list(self.prediction_events))
        response["runtime_resources"] = copy.deepcopy(response["resources"])
        if workflow_id is not None and workflow_id in self.workflows:
            record = self.workflows[workflow_id]
            response["result"] = copy.deepcopy(record.done.result()) if record.done.done() else None
        return response

    async def shutdown(self):
        await asyncio.shield(self._ensure_shutdown())

    def _ensure_shutdown(self):
        if self._shutdown_owner is None or (self._shutdown_owner.done()
                                           and self._shutdown_owner.cancelled() and not self.closed):
            self.closing = True
            self._shutdown_owner = asyncio.create_task(self._shutdown())
        return self._shutdown_owner

    async def _drain_pause(self):
        # Cancellation of a disposable notification never ends supervision.
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=0.02)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            pass

    async def _drain_initialization(self):
        while self._pending_loads:
            self._wake.clear()
            for future in tuple(self._pending_loads):
                if not future.running() and not future.done():
                    future.cancel()
                if future.done():
                    try:
                        backend = future.result()
                        if "forward_hook" in backend:
                            backend["forward_hook"].remove()
                    except BaseException:
                        pass
                    self._pending_loads.discard(future)
            if self._pending_loads:
                await self._drain_pause()

    @staticmethod
    async def _task_terminal(task):
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        try:
            task.result()
        except BaseException:
            pass

    async def _shutdown(self):
        if self._initializing is not None:
            await self._task_terminal(self._initializing)
        await self._drain_initialization()
        if self.scheduler is not None:
            # Request cancellation for all DAGs before waiting for any Future.
            for workflow_id, record in self.workflows.items():
                if not record.done.done():
                    self.scheduler.cancel(workflow_id, time.monotonic(), reason="Runtime shutdown")
            self._wake.set()
            if self._driver is not None and not self._driver.done():
                self._driver.cancel()
            # This loop is independent of the driver and asyncio callbacks.
            # Queued Futures can cancel; running threads must truly terminate.
            while self._works or self._flights:
                self._wake.clear()
                for future in {work.future for work in self._works.values()} | set(self._flights.values()):
                    if not future.running() and not future.done():
                        future.cancel()
                for key, future in tuple(self._flights.items()):
                    if future.done():
                        self._flight_finished(key, future)
                for work in tuple(self._works.values()):
                    if work.future.done():
                        self._finish(work)
                self.scheduler.tick(time.monotonic())
                self._settle()
                if self._works or self._flights:
                    await self._drain_pause()
            self._settle()
        if self._executor is not None:
            # All synchronous work has reached a real terminal state by this point.
            self._executor.shutdown(wait=True)
        self._cache.clear()
        self._cache_bytes = 0
        self._flights.clear()
        self._flight_sources.clear()
        for backend in self.backends.values():
            if "forward_hook" in backend:
                backend["forward_hook"].remove()
        self.backends.clear()
        for state in self.instances.values():
            state.update(healthy=False, lifecycle="closed")
        if self.catalog is not None:
            for instance_id in self.catalog.snapshot()["instances"]:
                self.catalog.set_health(instance_id, False, "Runtime shutdown")
        self.closed = True
        if self._wake is not None:
            self._wake.set()
        if self._driver is not None:
            await self._task_terminal(self._driver)

