"""Explicit capability/implementation/instance catalog; no execution dependencies.

Capability descriptions contain ``resources`` (cpu, memory_mb), ``profile``
(expected_seconds, cold_start_seconds, adapter_seconds), and ``contract``.
Implementation contracts are inherited by capabilities. Candidate memory is
incremental working memory; resident instance memory is reported separately.
Registration is atomic, identifiers cannot be overwritten, and snapshots are detached.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from numbers import Real
from threading import RLock
from typing import Any

from src.service.traffic_types import Candidate

__all__ = ["TrafficCatalog", "CatalogError", "TrafficWorkflowCompiler", "CompilationError"]


def __getattr__(name):
    # Compatibility with runtime's catalog import, without a circular import
    # or loading any execution/model dependency.
    if name in ("TrafficWorkflowCompiler", "CompilationError"):
        from src.service import traffic_composer
        return getattr(traffic_composer, name)
    raise AttributeError(name)


class CatalogError(ValueError):
    """Invalid catalog declaration or operation, with a stable error code."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


class _FrozenDict(dict):
    """JSON/asdict-friendly immutable mapping used inside shared dataclasses."""

    def _readonly(self, *args, **kwargs):
        raise TypeError("traffic plan mappings are frozen")

    __setitem__ = __delitem__ = clear = pop = popitem = setdefault = update = _readonly
    __ior__ = _readonly

    def __deepcopy__(self, memo):
        return self


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return _FrozenDict((key, _freeze(item)) for key, item in value.items())
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(item) for item in value]
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float and math.isfinite(value):
        return value
    # CapabilityContract/TensorSchema declarations may be supplied directly.
    if callable(getattr(value, "to_dict", None)):
        return _plain(value.to_dict())
    raise CatalogError("declaration", "metadata must contain finite, JSON-safe values")


def _text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise CatalogError("declaration", f"{field} must be a nonempty trimmed string")
    return value


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    try:
        valid = (isinstance(value, Real) and not isinstance(value, bool)
                 and math.isfinite(value) and (value > 0 if positive else value >= 0))
    except (OverflowError, TypeError):
        valid = False
    if not valid:
        raise CatalogError("numeric", f"{field} must be finite and {'positive' if positive else 'nonnegative'}")
    return float(value)


def _integer(value: Any, field: str, *, minimum: int = 1) -> int:
    if type(value) is not int or value < minimum:
        raise CatalogError("numeric", f"{field} must be an integer >= {minimum}")
    return value


def _matches(actual: Any, required: Any) -> bool:
    """Required mappings are subsets; sequences and scalars are exact."""
    if isinstance(required, Mapping):
        return isinstance(actual, Mapping) and all(
            key in actual and _matches(actual[key], item) for key, item in required.items())
    if isinstance(required, (list, tuple)):
        return isinstance(actual, (list, tuple)) and len(actual) == len(required) and all(
            _matches(old, new) for old, new in zip(actual, required))
    return type(actual) is type(required) and actual == required


class TrafficCatalog:
    """Thread-safe, in-memory descriptions and measured execution profiles."""

    def __init__(self):
        self._implementations: dict[str, dict] = {}
        self._instances: dict[str, dict] = {}
        self._executions: dict[tuple[str, str], dict] = {}
        self._lock = RLock()

    def register_implementation(self, implementation: dict) -> None:
        if not isinstance(implementation, Mapping):
            raise CatalogError("declaration", "implementation must be a mapping")
        entry = _plain(implementation)
        identifier = _text(entry.get("implementation_id"), "implementation_id")
        version = _text(entry.get("version"), "version")
        capabilities = entry.get("capabilities")
        if not isinstance(capabilities, dict) or not capabilities:
            raise CatalogError("capability", "declare a nonempty capability mapping")
        inherited = entry.get("contract", {})
        if not isinstance(inherited, dict):
            raise CatalogError("contract", "implementation contract must be a mapping")
        normalized = {}
        for capability_id, description in capabilities.items():
            _text(capability_id, "capability_id")
            if not isinstance(description, dict):
                raise CatalogError("capability", "capability description must be a mapping")
            resources = description.get("resources", {})
            profile = description.get("profile", {})
            contract = description.get("contract", {})
            if not all(isinstance(item, dict) for item in (resources, profile, contract)):
                raise CatalogError("declaration", "resources, profile and contract must be mappings")
            effective = dict(inherited)
            effective.update(contract)
            effective.setdefault("version", version)
            effective.setdefault("model_version", version)
            if effective["version"] != version or effective["model_version"] != version:
                raise CatalogError("version", "implementation and capability versions must agree")
            normalized[capability_id] = dict(
                description,
                resources=dict(cpu=_number(resources.get("cpu", description.get("cpu", 1)), "cpu", positive=True),
                               memory_mb=_number(resources.get("memory_mb", description.get("memory_mb", 64)), "memory_mb")),
                profile={key: _number(profile.get(key, description.get(key, default)), key)
                         for key, default in (("expected_seconds", 0.01), ("cold_start_seconds", 0), ("adapter_seconds", 0))},
                contract=effective,
            )
            if not math.isfinite(sum(normalized[capability_id]["profile"].values())):
                raise CatalogError("numeric", "total profile duration must be finite")
        entry["capabilities"] = normalized
        with self._lock:
            if identifier in self._implementations:
                raise CatalogError("duplicate", f"implementation already registered: {identifier}")
            self._implementations[identifier] = entry

    def register_instance(self, instance: dict) -> None:
        if not isinstance(instance, Mapping):
            raise CatalogError("declaration", "instance must be a mapping")
        entry = _plain(instance)
        identifier = _text(entry.get("instance_id"), "instance_id")
        implementation_id = _text(entry.get("implementation_id"), "implementation_id")
        _text(entry.get("endpoint"), "endpoint")
        entry["max_concurrency"] = _integer(entry.get("max_concurrency", 1), "max_concurrency")
        entry["base_memory_mb"] = _number(entry.get("base_memory_mb", 0), "base_memory_mb")
        entry.setdefault("healthy", True)
        if type(entry["healthy"]) is not bool:
            raise CatalogError("health", "healthy must be a boolean")
        entry["health_reason"] = None
        with self._lock:
            if implementation_id not in self._implementations:
                raise CatalogError("implementation", f"unregistered implementation: {implementation_id}")
            if identifier in self._instances:
                raise CatalogError("duplicate", f"instance already registered: {identifier}")
            self._instances[identifier] = entry

    def candidates(self, capability_id, contract=None, allowed_instances=None) -> tuple[Candidate, ...]:
        _text(capability_id, "capability_id")
        if contract is not None and not isinstance(contract, Mapping) and not callable(getattr(contract, "to_dict", None)):
            raise CatalogError("contract", "required contract must be a mapping")
        required = _plain(contract) if contract is not None else {}
        allowed = None
        if allowed_instances is not None:
            if not isinstance(allowed_instances, (list, tuple, set, frozenset)):
                raise CatalogError("instance", "allowed_instances must be an explicit collection")
            allowed = {_text(item, "allowed instance") for item in allowed_instances}
        selected = []
        with self._lock:
            for identifier in sorted(self._instances):
                instance = self._instances[identifier]
                if not instance["healthy"] or allowed is not None and identifier not in allowed:
                    continue
                implementation = self._implementations[instance["implementation_id"]]
                description = implementation["capabilities"].get(capability_id)
                if description is None or not _matches(description["contract"], required):
                    continue
                profile = description["profile"]
                observed = self._executions.get((identifier, capability_id), {})
                expected = observed.get("mean_success_seconds", profile["expected_seconds"])
                selected.append(Candidate(
                    instance_id=identifier, capability_id=capability_id,
                    implementation_id=implementation["implementation_id"], version=implementation["version"],
                    cpu=description["resources"]["cpu"],
                    memory_mb=description["resources"]["memory_mb"],
                    expected_seconds=expected, cold_start_seconds=profile["cold_start_seconds"],
                    adapter_seconds=profile["adapter_seconds"], max_concurrency=instance["max_concurrency"], healthy=True,
                ))
        return tuple(selected)

    def set_health(self, instance_id, healthy, reason=None) -> None:
        _text(instance_id, "instance_id")
        if type(healthy) is not bool:
            raise CatalogError("health", "healthy must be a boolean")
        if reason is not None:
            _text(reason, "reason")
        with self._lock:
            if instance_id not in self._instances:
                raise CatalogError("instance", f"unregistered instance: {instance_id}")
            self._instances[instance_id].update(healthy=healthy, health_reason=reason)

    def record_execution(self, instance_id, capability_id, duration, success) -> None:
        _text(instance_id, "instance_id")
        _text(capability_id, "capability_id")
        duration = _number(duration, "duration")
        if type(success) is not bool:
            raise CatalogError("execution", "success must be a boolean")
        with self._lock:
            instance = self._instances.get(instance_id)
            if instance is None:
                raise CatalogError("instance", f"unregistered instance: {instance_id}")
            if capability_id not in self._implementations[instance["implementation_id"]]["capabilities"]:
                raise CatalogError("capability", f"instance does not provide {capability_id}")
            key = (instance_id, capability_id)
            record = dict(self._executions.get(key, dict(
                executions=0, successes=0, failures=0, total_seconds=0.0, success_seconds=0.0)))
            record["executions"] += 1
            record["total_seconds"] += duration
            record["successes" if success else "failures"] += 1
            if success:
                record["success_seconds"] += duration
                record["mean_success_seconds"] = record["success_seconds"] / record["successes"]
            if not math.isfinite(record["total_seconds"]) or not math.isfinite(record["success_seconds"]):
                raise CatalogError("numeric", "execution duration totals must remain finite")
            self._executions[key] = record

    def snapshot(self) -> dict:
        with self._lock:
            return _plain(dict(
                implementations=self._implementations, instances=self._instances,
                executions=[dict(instance_id=instance_id, capability_id=capability_id, **record)
                            for (instance_id, capability_id), record in sorted(self._executions.items())],
            ))

    def _copy_for_selection(self):
        """Capture registration, health and profiles atomically for compilation."""
        with self._lock:
            view = TrafficCatalog()
            view._implementations = _plain(self._implementations)
            view._instances = _plain(self._instances)
            view._executions = {key: _plain(record) for key, record in self._executions.items()}
            return view
