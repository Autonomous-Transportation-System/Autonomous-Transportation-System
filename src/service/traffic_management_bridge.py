"""Persistent traffic application management over the public joint runtime API.

The runtime owns execution and resource release. A bridge observer can only
finish an application after a snapshot reports done; HTTP cancellation never
cancels that observer. No full conversational graph is used for traffic runs.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
import uuid
import weakref
from collections import deque
from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime
from typing import Any, Optional
from urllib.parse import quote, urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.app.app_manager import AppManager, get_app_manager
from src.app.models import GuidanceFile


class TrafficWorkflowRequest(BaseModel):
    """Registered templates, with strict scalar types and finite deadlines."""

    model_config = ConfigDict(extra="forbid", strict=True)

    template: str = Field(min_length=1)
    target_frames: list[int] = Field(min_length=1, max_length=32)
    deadline_seconds: float = Field(default=120.0, gt=0, allow_inf_nan=False)
    priority: int = Field(default=0, ge=0)
    max_attempts: int = Field(default=2, ge=1, le=100)
    enable_reuse: bool = True
    enable_contracts: bool = True
    enable_recovery: bool = True
    contract_overrides: Optional[dict[str, Any]] = None
    allowed_instances: Optional[list[str]] = Field(default=None, min_length=1)

    @field_validator("template")
    @classmethod
    def nonblank_template(cls, value):
        if not value.strip():
            raise ValueError("template must not be blank")
        return value.strip()

    @model_validator(mode="after")
    def check_frames(self):
        if len(set(self.target_frames)) != len(self.target_frames):
            raise ValueError("target_frames must be unique")
        if any(t < 3 or t >= 1488 or t % 48 < 3 for t in self.target_frames):
            raise ValueError("target_frames require same-day history [t-3,t) and target [t,t+1)")
        if self.allowed_instances is not None:
            if any(not item.strip() for item in self.allowed_instances) or len(set(self.allowed_instances)) != len(self.allowed_instances):
                raise ValueError("allowed_instances must contain unique nonblank IDs")
        return self


TEMPLATES = (
    {"template": "flow", "name": "流量预测", "outputs": ["flow"],
     "capabilities": ["history.prepare", "traffic.predict", "validate.flow", "result.aggregate"]},
    {"template": "analysis", "name": "状态与流量联合分析", "outputs": ["state", "flow"],
     "capabilities": ["history.prepare", "traffic.predict", "validate.state", "validate.flow",
                      "analysis.congestion", "result.aggregate"]},
    {"template": "od_hotspots", "name": "OD热点分析", "outputs": ["od"],
     "capabilities": ["history.prepare", "traffic.predict", "validate.od",
                      "analysis.od_hotspots", "result.aggregate"]},
)
_ACTIVE = {"starting", "running", "stopping"}


class ManagementConflict(RuntimeError):
    """An operation conflicts with an active execution or a closing bridge."""


class TrafficManagementBridge:
    def __init__(self, runtime, manager=None, registry=None, qos=None, *, service_url=None,
                 qos_cooldown_seconds=60.0, qos_clock=None):
        from src.runtime.qos_monitor import get_qos_monitor
        from src.service.agent_registry import get_registry_client

        self.runtime = runtime
        store = os.getenv("TRAFFIC_APPS_STORE_PATH")
        self.manager = manager if manager is not None else (
            AppManager(store_path=store) if store else get_app_manager()
        )
        self.registry = registry if registry is not None else get_registry_client()
        self.qos = qos if qos is not None else get_qos_monitor()
        if (type(qos_cooldown_seconds) not in (int, float) or
                not math.isfinite(qos_cooldown_seconds) or qos_cooldown_seconds < 0):
            raise ValueError("qos_cooldown_seconds must be finite and nonnegative")
        if qos_clock is not None and not callable(qos_clock):
            raise ValueError("qos_clock must be callable")
        self.qos_cooldown_seconds = float(qos_cooldown_seconds)
        self._qos_clock = qos_clock if qos_clock is not None else time.monotonic
        self.service_url = (service_url or os.getenv("TRAFFIC_SERVICE_URL", "http://127.0.0.1:18763")).rstrip("/")
        self._initialized = False
        self._init_lock = None
        self._locks: dict[str, asyncio.Lock] = {}
        self._owners: dict[str, asyncio.Task] = {}
        self._pending_bindings: dict[str, dict] = {}
        self._api_waiters: dict[str, asyncio.Task] = {}
        # JSON strings are immutable declaration copies, not a second compiler.
        self._template_declarations: dict[str, str] = {}
        self._records: dict[str, dict] = {}
        self._qos_recorded: set[tuple] = set()
        self._registered: set[str] = set()
        self._qos_callback_registered = False
        self._qos_loop = None
        self._qos_last_quarantine: dict[str, float] = {}
        self._qos_quarantined: set[str] = set()
        self._qos_actions = deque(maxlen=256)
        self._shutdown_task = None
        self.closing = False
        self.closed = False
        self.manager.register_execution_backend("traffic", self)
        self.manager.register_execution_backend("traffic_joint", self)

    def _require_open(self):
        if self.closing or self.closed:
            raise ManagementConflict("Traffic management is shutting down")

    @property
    def _initialize_lock(self):
        # Python 3.9 binds synchronization primitives at construction. The
        # router/bridge may be constructed before the serving loop exists.
        if self._init_lock is None:
            self._init_lock = asyncio.Lock()
        return self._init_lock

    async def initialize(self):
        if self._initialized:
            return
        self._require_open()
        async with self._initialize_lock:
            if not self._initialized:
                self._require_open()
                await self.runtime.initialize()
                self._restore_app_templates()
                self._register_instances()
                self._initialized = True
                self._register_qos_feedback()

    def templates(self):
        defaults = {item["template"]: item for item in TEMPLATES}
        definitions = self._template_definitions()
        identifiers = list(definitions) if definitions else list(defaults)
        for identifier in self._template_declarations:
            if identifier not in identifiers:
                identifiers.append(identifier)
        result = []
        for identifier in identifiers:
            definition = definitions.get(identifier)
            if definition is None and identifier in self._template_declarations:
                definition = json.loads(self._template_declarations[identifier])
            if definition is None:
                result.append(deepcopy(defaults[identifier]))
                continue
            result.append({"template": identifier, "template_id": identifier,
                           "name": defaults.get(identifier, {}).get("name", identifier),
                           "outputs": list(definition["requested_outputs"]),
                           "capabilities": list(dict.fromkeys(node["capability_id"] for node in definition["nodes"])),
                           "definition": definition})
        return result

    @staticmethod
    def _declaration_copy(value):
        """Accept finite JSON declaration data; never evaluate executable values."""
        try:
            return json.loads(json.dumps(value, allow_nan=False))
        except (TypeError, ValueError) as exc:
            raise ValueError("Template declarations must contain finite JSON data") from exc

    def _template_definitions(self):
        compiler = getattr(self.runtime, "compiler", None)
        definitions = getattr(compiler, "_templates", None)
        if not isinstance(definitions, Mapping):
            return {}
        lock = getattr(compiler, "_template_lock", None)
        if lock is not None:
            with lock:
                return self._declaration_copy(definitions)
        return self._declaration_copy(definitions)

    def _register_template(self, template_id, definition):
        compiler = getattr(self.runtime, "compiler", None)
        if not callable(getattr(compiler, "register_template", None)):
            raise ManagementConflict("Runtime compiler does not support template registration")
        if not isinstance(template_id, str) or not template_id.strip():
            raise ValueError("template_id must be a nonblank string")
        template_id = template_id.strip()
        if any(item["template"] == template_id for item in self.templates()):
            raise ManagementConflict(f"Template already registered: {template_id}")
        definition = self._declaration_copy(definition)
        if not isinstance(definition, dict) or not isinstance(definition.get("nodes"), list):
            raise ValueError("Template definition requires a nodes array")
        implementations = self._entries(self.catalog_snapshot().get("implementations"), "implementation_id")
        capabilities = {capability for item in implementations for capability in item.get("capabilities", {})}
        for node in definition["nodes"]:
            if not isinstance(node, dict) or node.get("capability_id") not in capabilities:
                raise ValueError("Template nodes must refer to registered capabilities")
        compiler.register_template(template_id, deepcopy(definition))
        self._template_declarations[template_id] = json.dumps(definition, allow_nan=False)
        return next(item for item in self.templates() if item["template"] == template_id)

    async def register_template(self, template_id, definition):
        self._require_open()
        await self.initialize()
        self._require_open()
        return self._register_template(template_id, definition)

    def _restore_app_templates(self):
        # Persist custom declarations with the applications that use them.
        for app in self.manager.list_apps():
            if not app.guidance_file or app.guidance_file.metadata.get("execution_backend") not in {"traffic", "traffic_joint"}:
                continue
            metadata = app.guidance_file.metadata
            definition = metadata.get("traffic_template")
            if definition is None:
                continue
            identifier = metadata["traffic_request"]["template"]
            existing = next((item for item in self.templates() if item["template"] == identifier), None)
            if existing is None:
                self._register_template(identifier, definition)
            elif existing.get("definition") != definition:
                raise ManagementConflict(f"Persisted template conflicts with compiler definition: {identifier}")

    def _owned_b1_instance(self, instance_id):
        instance = getattr(self.runtime, "instances", {}).get(instance_id)
        if not isinstance(instance, dict) or instance.get("implementation_id") != "B1":
            return None
        if instance.get("lifecycle") in {"closed", "stopped"}:
            return None
        return instance

    def _register_qos_feedback(self):
        register = getattr(self.qos, "register_alert_callback", None)
        if self._qos_callback_registered or not callable(register) or not callable(getattr(self.runtime, "set_instance_health", None)):
            return
        self._qos_loop = asyncio.get_running_loop()
        reference = weakref.ref(self)

        def on_alert(instance_id, metrics):
            bridge = reference()
            if bridge is None or bridge.closing or bridge.closed or bridge._qos_loop.is_closed():
                return
            if isinstance(metrics, Mapping):
                details = deepcopy(dict(metrics))
            elif callable(getattr(metrics, "to_dict", None)):
                details = deepcopy(metrics.to_dict())
            else:
                details = {key: getattr(metrics, key) for key in
                           ("avg_latency_ms", "success_rate", "total_calls", "failure_count") if hasattr(metrics, key)}
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None
            if loop is bridge._qos_loop:
                bridge._quarantine_from_qos(instance_id, details)
            else:
                bridge._qos_loop.call_soon_threadsafe(bridge._quarantine_from_qos, instance_id, details)

        register(on_alert)
        self._qos_callback_registered = True

    def _quarantine_from_qos(self, instance_id, metrics):
        if self.closing or self.closed:
            return
        instance = self._owned_b1_instance(instance_id)
        if instance is None or instance.get("healthy") is False:
            return
        now = self._qos_clock()
        previous = self._qos_last_quarantine.get(instance_id)
        if previous is not None and now - previous < self.qos_cooldown_seconds:
            return
        reason = "QoS failure or latency threshold exceeded"
        # Health only filters subsequent selections; running work retains its
        # reservation and the runtime remains its sole completion owner.
        self.runtime.set_instance_health(instance_id, False, reason=reason)
        self.registry.update_agent_status(instance_id, "offline")
        self._qos_last_quarantine[instance_id] = now
        self._qos_quarantined.add(instance_id)
        self._qos_actions.append({"event": "qos_quarantine", "action": "quarantine",
                                 "source": "qos", "instance_id": instance_id, "healthy": False,
                                 "now": now, "cooldown_until": now + self.qos_cooldown_seconds,
                                 "reason": reason, "metrics": deepcopy(metrics)})

    async def set_instance_health(self, instance_id, healthy, reason=None):
        self._require_open()
        if type(healthy) is not bool:
            raise ValueError("healthy must be a boolean")
        await self.initialize()
        self._require_open()
        instance = self._owned_b1_instance(instance_id)
        if instance is None:
            raise KeyError(instance_id)
        previous = instance.get("healthy", True)
        self.runtime.set_instance_health(instance_id, healthy, reason=reason)
        self.registry.update_agent_status(instance_id, "online" if healthy else "offline")
        if healthy:
            self._qos_quarantined.discard(instance_id)
        if previous != healthy:
            self._qos_actions.append({"event": "instance_health_changed", "source": "manual",
                                     "action": "restore" if healthy else "quarantine", "instance_id": instance_id,
                                     "healthy": healthy, "now": self._qos_clock(), "reason": reason})
        return {**self.get_instance(instance_id), "qos_quarantined": instance_id in self._qos_quarantined}

    def catalog_snapshot(self):
        catalog = getattr(self.runtime, "catalog", None)
        return deepcopy(catalog.snapshot()) if catalog is not None else {"implementations": {}, "instances": {}}

    @staticmethod
    def _entries(value, id_key):
        if isinstance(value, dict):
            return [{id_key: key, **item} for key, item in value.items()]
        return list(value or [])

    def _endpoint(self, instance):
        endpoint = instance.get("endpoint", "")
        if not endpoint or urlparse(endpoint).scheme in {"local", "inproc"}:
            return f"{self.service_url}/traffic/platform/instances/{quote(instance['instance_id'], safe='')}/execute"
        return endpoint

    def _register_instances(self):
        catalog = self.catalog_snapshot()
        implementations = {item["implementation_id"]: item for item in
                           self._entries(catalog.get("implementations"), "implementation_id")}
        for instance in self._entries(catalog.get("instances"), "instance_id"):
            instance_id = instance["instance_id"]
            endpoint = self._endpoint(instance)
            parsed = urlparse(endpoint)
            implementation = implementations.get(instance.get("implementation_id"), {})
            capabilities = instance.get("capabilities") or implementation.get("capabilities") or {"traffic.predict": {}}
            capability = "traffic.predict" if "traffic.predict" in capabilities else next(iter(capabilities))
            self.registry.register_agent(
                agent_id=instance_id, ip=parsed.hostname or "localhost",
                port=parsed.port or (443 if parsed.scheme == "https" else 80),
                capability=capability, status="online" if instance.get("healthy", True) else "offline",
                description=f"Traffic instance {instance_id}: {endpoint}",
            )
            # The existing registry exposes its registered dictionary publicly.
            agent = self.registry.get_agent_by_id(instance_id)
            if agent is not None:
                agent.update(endpoint=endpoint, instance_id=instance_id,
                             implementation_id=instance.get("implementation_id"),
                             capabilities=list(capabilities), endpoint_method="POST")
            self._registered.add(instance_id)

    def get_instance(self, instance_id):
        for instance in self._entries(self.catalog_snapshot().get("instances"), "instance_id"):
            if instance["instance_id"] == instance_id:
                live = self.runtime.snapshot().get("instances", {}).get(instance_id, {})
                return {**instance, **deepcopy(live), "endpoint": self._endpoint(instance),
                        "endpoint_method": "POST",
                        "metadata_url": f"{self.service_url}/traffic/platform/instances/{quote(instance_id, safe='')}"}
        raise KeyError(instance_id)

    def _app(self, app_id):
        app = self.manager.get_app(app_id)
        if app is None or app.guidance_file is None or app.guidance_file.metadata.get("execution_backend") not in {"traffic", "traffic_joint"}:
            raise KeyError(app_id)
        return app

    def create(self, name, request):
        self._require_open()
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 200:
            raise ValueError("name must contain 1 to 200 non-whitespace characters")
        normalized = TrafficWorkflowRequest.model_validate(request).model_dump(exclude_none=True)
        template = next((item for item in self.templates() if item["template"] == normalized["template"]), None)
        if template is None:
            raise ValueError(f"Unknown registered template: {normalized['template']}")
        app_id = f"app_{uuid.uuid4().hex}"
        compiler = getattr(self.runtime, "compiler", None)
        if callable(getattr(compiler, "compile", None)):
            plan = compiler.compile(deepcopy(normalized), app_id=app_id, now=time.monotonic())
            capabilities = list(dict.fromkeys(node.capability_id for node in plan.nodes))
        else:
            capabilities = list(template["capabilities"])
        metadata = {"execution_backend": "traffic", "traffic_request": normalized,
                    "traffic_run": {"last_run_status": None, "active_workflow_id": None,
                                    "last_workflow_id": None}}
        if normalized["template"] not in {item["template"] for item in TEMPLATES} and "definition" in template:
            metadata["traffic_template"] = deepcopy(template["definition"])
        guidance = GuidanceFile(
            app_id=app_id, task_description=template["name"],
            agents_required=capabilities, orchestration_mode="traffic_joint",
            constraints={"deadline_seconds": normalized["deadline_seconds"]},
            metadata=metadata,
        )
        app = self.manager.install(name.strip(), guidance)
        return self.get_app(app.app_id)

    def get_app(self, app_id):
        app = self._app(app_id)
        pending = self._pending_bindings.get(app_id)
        if pending is not None:
            self._recover_binding(pending["workflow_id"])
            app = self._app(app_id)
        if app.workflow_handle in self._records:
            self._reconcile(app.workflow_handle)
            app = self._app(app_id)
        run = app.guidance_file.metadata.get("traffic_run", {})
        return {**deepcopy(app.to_dict()), "request": deepcopy(app.guidance_file.metadata["traffic_request"]),
                "workflow_id": app.workflow_handle or run.get("last_workflow_id"),
                "last_run_status": run.get("last_run_status"), "last_run": deepcopy(run)}

    def list_apps(self):
        return [self.get_app(app.app_id) for app in self.manager.list_apps()
                if app.guidance_file and app.guidance_file.metadata.get("execution_backend") in {"traffic", "traffic_joint"}]

    def _lock(self, app_id):
        return self._locks.setdefault(app_id, asyncio.Lock())

    async def start(self, app_id):
        self._require_open()
        async with self._lock(app_id):
            self._require_open()
            current = self.get_app(app_id)
            if current["status"] in _ACTIVE:
                if current["status"] == "stopping":
                    raise ManagementConflict("Application is still cancelling")
                return {"app_id": app_id, "workflow_id": current["workflow_id"], "status": current["status"]}
            await self.initialize()
            self._require_open()
            app = self._app(app_id)
            previous_handle = app.workflow_handle
            run = deepcopy(app.guidance_file.metadata.get("traffic_run", {}))
            # A persisted submission ID distinguishes a rolled-back handle
            # from a later start that also temporarily has no handle.
            run.update(submission_id=uuid.uuid4().hex, active_workflow_id=None,
                       started_at=datetime.utcnow().isoformat(), finished_at=None)
            self.manager.update_state(app_id, "starting", expected_handle=previous_handle,
                                      metadata={"traffic_run": run}, error=None)
            try:
                workflow_id = self.runtime.submit(deepcopy(current["request"]), app_id=app_id)
            except Exception as exc:
                self.manager.update_state(app_id, "error", expected_handle=previous_handle, error=str(exc))
                raise
            run.update(active_workflow_id=workflow_id, last_workflow_id=workflow_id)
            self._records[workflow_id] = {"app_id": app_id, "workflow_id": workflow_id,
                                          "request": deepcopy(current["request"]), "status": "running",
                                          "done": False, "submitted_at": time.monotonic()}
            self._pending_bindings[app_id] = {
                "workflow_id": workflow_id, "expected_handle": previous_handle,
                "submission_id": run["submission_id"], "run": deepcopy(run),
                "status": "running", "error": None,
            }
            # Install the observer before persistence: even a disk error cannot
            # orphan a submitted execution or cause an early resource release.
            self._owners[workflow_id] = asyncio.create_task(self._observe(workflow_id))
            try:
                changed = self.manager.update_state(
                    app_id, "running", expected_handle=previous_handle, workflow_handle=workflow_id,
                    app_interface_url=f"/traffic/platform/apps/{app_id}",
                    metadata={"traffic_run": run}, error=None,
                )
                if not changed:
                    raise ManagementConflict("Application handle changed during submission")
            except Exception as exc:
                self._records[workflow_id].update(status="cancelling", start_error=str(exc))
                pending = self._pending_bindings[app_id]
                pending.update(status="stopping", error=str(exc))
                try:
                    # A one-shot write failure can be repaired immediately.
                    # Persistent failures retain this binding for later reads
                    # or completion; neither case declares execution stopped.
                    self._recover_binding(workflow_id)
                except Exception as cleanup_error:
                    self._records[workflow_id]["binding_error"] = str(cleanup_error)
                self.runtime.cancel(workflow_id, reason="Application state could not be persisted")
                raise
            self._pending_bindings.pop(app_id, None)
            return {"app_id": app_id, "workflow_id": workflow_id, "status": "running"}

    def _recover_binding(self, workflow_id):
        """Retry a submitted execution's state write without taking over a rerun."""
        record = self._records.get(workflow_id)
        if record is None:
            return False
        app_id = record["app_id"]
        pending = self._pending_bindings.get(app_id)
        if pending is None or pending["workflow_id"] != workflow_id:
            return False
        app = self.manager.get_app(app_id)
        run = app.guidance_file.metadata.get("traffic_run", {}) if app and app.guidance_file else {}
        if (app is None or app.status not in _ACTIVE or
                app.workflow_handle != pending["expected_handle"] or
                run.get("submission_id") != pending["submission_id"]):
            # A deleted application, changed handle or newer submission wins.
            self._pending_bindings.pop(app_id, None)
            return False
        changed = self.manager.update_state(
            app_id, pending["status"], expected_handle=pending["expected_handle"],
            workflow_handle=workflow_id,
            app_interface_url=f"/traffic/platform/apps/{app_id}",
            metadata={"traffic_run": pending["run"]}, error=pending["error"],
        )
        self._pending_bindings.pop(app_id, None)
        return changed

    @staticmethod
    def _state(snapshot, workflow_id):
        scheduler = snapshot.get("scheduler", snapshot)
        return scheduler.get("workflows", {}).get(workflow_id, scheduler)

    def _reconcile(self, workflow_id):
        snapshot = self.runtime.snapshot(workflow_id)
        self._complete(workflow_id, snapshot)
        return snapshot

    async def _observe(self, workflow_id):
        try:
            result = await self.runtime.wait(workflow_id)
            self._records[workflow_id]["execution_result"] = deepcopy(result)
            self._reconcile(workflow_id)
            if self._records[workflow_id]["done"]:
                self._record_qos(workflow_id, self.runtime.snapshot(workflow_id))
        except Exception as exc:
            self._records[workflow_id]["observer_error"] = str(exc)
            # A failed waiter is not evidence that synchronous execution ended.
            try:
                self._reconcile(workflow_id)
            except Exception:
                pass

    def _record_qos(self, workflow_id, snapshot):
        scheduler = snapshot.get("scheduler", snapshot)
        execution = self._records.get(workflow_id, {}).get("execution_result", {})
        profile_list = execution.get("profiles", snapshot.get("profiles"))
        profiles = None if profile_list is None else {
            (p["task_id"], p.get("attempt", 1), p.get("instance_id")): p for p in profile_list
        }
        for task_id, task in scheduler.get("tasks", {}).items():
            if task.get("workflow_id", workflow_id) != workflow_id:
                continue
            for attempt in task.get("attempts") or [task]:
                instance_id = attempt.get("instance_id", task.get("instance_id"))
                started, finished = attempt.get("started_at"), attempt.get("finished_at")
                call_status = attempt.get("call_status")
                key = (workflow_id, task_id, attempt.get("attempt", 1))
                profile = profiles.get((task_id, key[2], instance_id)) if profiles is not None else None
                if (not instance_id or started is None or finished is None or
                        call_status not in {"success", "failed", "error", "timeout", "cancelled"} or
                        key in self._qos_recorded or (profiles is not None and profile is None)):
                    continue
                success = call_status == "success" and attempt.get("quality_status", "valid") == "valid"
                duration = profile.get("duration_seconds", finished - started) if profile is not None else finished - started
                self.qos.record_call(instance_id, max(0.0, duration) * 1000, success)
                self._qos_recorded.add(key)

    def _complete(self, workflow_id, snapshot):
        record = self._records.get(workflow_id)
        state = self._state(snapshot, workflow_id)
        if record is None or record["done"] or state.get("done") is not True:
            return False
        status = state.get("status", snapshot.get("status"))
        if not status or status in {"queued", "pending", "running", "cancelling", "stopping"}:
            return False
        status = {"completed": "success", "succeeded": "success", "error": "failed",
                  "canceled": "cancelled"}.get(status, status)
        self._recover_binding(workflow_id)
        self._record_qos(workflow_id, snapshot)
        app = self.manager.get_app(record["app_id"])
        if app is not None and app.workflow_handle == workflow_id:
            run = deepcopy(app.guidance_file.metadata.get("traffic_run", {}))
            run.update(last_run_status=status, active_workflow_id=None, last_workflow_id=workflow_id,
                       finished_at=datetime.utcnow().isoformat())
            errors = record.get("execution_result", {}).get("errors", [])
            self.manager.update_state(
                app.app_id, "stopped", expected_handle=workflow_id, workflow_handle=None,
                app_interface_url=None, error=state.get("error", snapshot.get("error")) or
                (str(errors[-1]) if errors else record.get("start_error")), metadata={"traffic_run": run},
            )
        record.update(status=status, done=True, finished_at=state.get("finished_at"))
        return True

    def get_workflow(self, workflow_id):
        snapshot = self._reconcile(workflow_id)
        record = self._records.get(workflow_id)
        if record is None:
            state = self._state(snapshot, workflow_id)
            if state.get("workflow_id") != workflow_id:
                raise KeyError(workflow_id)
            return deepcopy(snapshot)
        return {**deepcopy(snapshot), **deepcopy(snapshot.get("scheduler", {})),
                "app_id": record["app_id"], "workflow_id": workflow_id,
                "status": record["status"], "done": record["done"],
                "result": deepcopy(record.get("execution_result")),
                "management": {key: deepcopy(value) for key, value in record.items() if key != "execution_result"}}

    async def wait_workflow(self, workflow_id):
        """Wait via runtime.wait, shielding execution from an HTTP reader's cancellation."""
        self.get_workflow(workflow_id)
        if workflow_id not in self._api_waiters:
            self._api_waiters[workflow_id] = asyncio.create_task(self.runtime.wait(workflow_id))
        result = await asyncio.shield(self._api_waiters[workflow_id])
        if workflow_id in self._records:
            self._records[workflow_id]["execution_result"] = deepcopy(result)
        snapshot = self._reconcile(workflow_id)
        if self._state(snapshot, workflow_id).get("done") is not True:
            raise ManagementConflict("Runtime waiter returned before real execution termination")
        return self.get_workflow(workflow_id)

    async def stop(self, app_id):
        async with self._lock(app_id):
            current = self.get_app(app_id)
            app = self._app(app_id)
            workflow_id = app.workflow_handle
            if not workflow_id or app.status not in _ACTIVE:
                return {"app_id": app_id, "workflow_id": current["workflow_id"],
                        "status": app.status, "done": True}
            self.manager.update_state(app_id, "stopping", expected_handle=workflow_id)
            self.runtime.cancel(workflow_id, reason=f"Application {app_id} stopped")
            self._reconcile(workflow_id)
            app = self._app(app_id)
            return {"app_id": app_id, "workflow_id": workflow_id,
                    "status": "cancelling" if app.workflow_handle == workflow_id else "stopped",
                    "done": app.workflow_handle != workflow_id}

    async def wait(self, app_id):
        """Wait for the current execution without handing ownership to its caller."""
        self.get_app(app_id)
        app = self._app(app_id)
        handle = app.workflow_handle
        if handle is not None:
            await asyncio.shield(self._owners[handle])
            self._reconcile(handle)
            if not self._records[handle]["done"]:
                raise ManagementConflict("Execution has not reached a real terminal state")
        return self.get_app(app_id)

    async def delete(self, app_id):
        self._require_open()
        async with self._lock(app_id):
            self._require_open()
            current = self.get_app(app_id)
            app = self._app(app_id)
            if app.workflow_handle and app.status in _ACTIVE:
                handle = app.workflow_handle
                self.manager.update_state(app_id, "stopping", expected_handle=handle)
                self.runtime.cancel(handle, reason=f"Application {app_id} deletion requested")
                self._reconcile(handle)
                if self._app(app_id).workflow_handle == handle:
                    return {"app_id": app_id, "workflow_id": handle, "deleted": False,
                            "status": "cancelling", "done": False}
            self.manager.uninstall(app_id)
            return {"app_id": app_id, "workflow_id": current["workflow_id"],
                    "deleted": True, "status": "deleted", "done": True}

    async def submit_workflow(self, request, appid=None):
        normalized = TrafficWorkflowRequest.model_validate(request).model_dump(exclude_none=True)
        if appid is None:
            appid = self.create(f"{normalized['template']} workflow", normalized)["app_id"]
        elif self.get_app(appid)["request"] != normalized:
            raise ManagementConflict("request must match the saved application configuration")
        return await self.start(appid)

    async def execute_instance(self, instance_id, request):
        """Run a durable application restricted to the addressed real instance."""
        self._require_open()
        await self.initialize()
        self.get_instance(instance_id)
        normalized = TrafficWorkflowRequest.model_validate(request).model_dump(exclude_none=True)
        if normalized.get("allowed_instances", [instance_id]) != [instance_id]:
            raise ValueError("This execute endpoint only permits the addressed instance")
        normalized["allowed_instances"] = [instance_id]
        return await self.submit_workflow(normalized)

    def resources_snapshot(self):
        snapshot = self.runtime.snapshot()
        scheduler = snapshot.get("scheduler", snapshot)
        result = {key: deepcopy(scheduler[key]) for key in
                  ("resources", "resource_events", "reservations", "peaks", "budgets", "instances")
                  if key in scheduler}
        if "scheduler" in snapshot:
            result["runtime_resources"] = deepcopy(snapshot.get("resources", {}))
            result["active_futures"] = snapshot.get("active_futures", 0)
        return result

    def snapshot(self):
        apps = self.list_apps()
        if self._initialized and not self.closed:
            self._register_instances()
        runtime = self.runtime.snapshot()
        return {**deepcopy(runtime), **deepcopy(runtime.get("scheduler", {})), "apps": apps,
                "instances": deepcopy(runtime.get("instances", {})),
                "runtime_resources": deepcopy(runtime.get("resources", {})),
                "catalog": self.catalog_snapshot(), "agents": deepcopy(self.registry.query_agents()),
                "qos": self.qos.get_metrics_dict(), "initialized": self._initialized,
                "qos_actions": deepcopy(list(self._qos_actions)),
                "qos_feedback": {"callback_registered": self._qos_callback_registered,
                                 "cooldown_seconds": self.qos_cooldown_seconds,
                                 "quarantined_instances": sorted(self._qos_quarantined)},
                "ready": self._initialized and not self.closing and not self.closed,
                "closing": self.closing, "closed": self.closed}

    async def shutdown(self):
        if self._shutdown_task is None:
            self.closing = True
            self._shutdown_task = asyncio.create_task(self._shutdown())
        await asyncio.shield(self._shutdown_task)

    async def _shutdown(self):
        # Serialize against initialization so late-loaded instances are also
        # covered by the runtime's shutdown and are never registered as online.
        async with self._initialize_lock:
            pass
        for app in self.list_apps():
            if app["status"] in _ACTIVE:
                await self.stop(app["app_id"])
        await self.runtime.shutdown()
        if self._owners:
            await asyncio.gather(*self._owners.values())
        if self._api_waiters:
            await asyncio.gather(*self._api_waiters.values(), return_exceptions=True)
        for workflow_id in self._records:
            self._reconcile(workflow_id)
        if any(not record["done"] for record in self._records.values()):
            raise ManagementConflict("Runtime shutdown returned before all executions terminated")
        for instance_id in self._registered:
            self.registry.update_agent_status(instance_id, "offline")
        self.manager.unregister_execution_backend("traffic", self)
        self.manager.unregister_execution_backend("traffic_joint", self)
        self.closed = True
