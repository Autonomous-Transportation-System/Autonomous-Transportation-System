"""Versioned traffic contracts for L2 orchestration; no graph/state mutation.

Axes: B=batch, L=history, H=forecast horizon, C=physical channel,
Y/X=grid row/column, D=destination channel (OD). Channel order is part of
``semantic``; equal shapes never imply equal meaning. ``None`` is a dynamic
dimension, never permission to squeeze/insert a singleton axis.

The service constructs the batch axis with np.expand_dims(storage_raw, axis=0)
and declares a batched raw source schema BEFORE calling the strict adapter.
The adapter only renames fields with explicit permission, permutes named axes,
removes declared B=1/H=1, scales supported physical units, and uses externally
fitted training statistics. It never inserts axes or fits statistics on input.
"""

from dataclasses import dataclass, field as dc_field
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import numpy as np


__all__ = [
    "ContractError", "TensorSchema", "TrainingStats", "TensorPayload", "Payload",
    "CapabilityContract", "CapabilityRegistry", "validate", "adapter",
    "adapt_payload",
]

_AXES = frozenset(("B", "L", "H", "C", "Y", "X", "D"))
_SINGLETON_AXES = frozenset(("B", "H"))
_SHARED_AXES = frozenset(("B", "L", "H", "Y", "X"))
_SPEED_IN_MPS = {"mph": 0.44704, "kmh": 1 / 3.6, "km/h": 1 / 3.6, "m/s": 1.0}
_UNITS = frozenset(_SPEED_IN_MPS) | frozenset(
    ("trips/30min", "active trips", "count", "1", "vehicles", "vehicles/30min")
)


class ContractError(ValueError):
    """A rejected contract/operation; ``code`` is stable for callers."""

    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


def _require(condition: bool, code: str, message: str) -> None:
    if not condition:
        raise ContractError(code, message)


def _text(value: Any, name: str) -> None:
    _require(isinstance(value, str) and bool(value.strip()), "declaration", name)


def _units(units: Tuple[str, ...]) -> None:
    _require(isinstance(units, tuple) and bool(units), "units", "declare channel units as a tuple")
    _require(all(isinstance(u, str) and u in _UNITS for u in units), "units", "unsupported or undeclared units")


@dataclass(frozen=True)
class TensorSchema:
    field: str
    axes: Tuple[str, ...]
    shape: Tuple[Optional[int], ...]
    units: Tuple[str, ...]
    semantic: str
    version: str
    normalization: str = "raw"
    frame_minutes: int = 30
    grid_id: str = "6x6"

    def __post_init__(self) -> None:
        for name in ("field", "semantic", "version", "grid_id"):
            _text(getattr(self, name), name)
        _require(isinstance(self.axes, tuple) and bool(self.axes), "axes", "declare axes as a tuple")
        _require(all(isinstance(a, str) and a in _AXES for a in self.axes), "axes", "unknown or undeclared axis")
        _require(len(set(self.axes)) == len(self.axes), "axes", "duplicate axis")
        _require(isinstance(self.shape, tuple) and len(self.shape) == len(self.axes), "shape", "shape must match declared axes")
        _require(all(n is None or (isinstance(n, (int, np.integer)) and not isinstance(n, (bool, np.bool_)) and n > 0) for n in self.shape), "shape", "dimensions must be positive integers or None")
        _require(not {"C", "D"}.issubset(self.axes), "axes", "declare either C or D as the channel axis")
        _units(self.units)
        channel_axis = self.channel_axis
        channels = self.shape[self.axes.index(channel_axis)] if channel_axis else 1
        _require(channels is None or channels == len(self.units), "units", "one unit required per channel")
        _require(self.normalization in ("raw", "standardized"), "normalization", "expected raw or standardized")
        _require(isinstance(self.frame_minutes, (int, np.integer)) and not isinstance(self.frame_minutes, (bool, np.bool_)) and self.frame_minutes > 0, "interval", "declare positive frame_minutes")
        _require("trips/30min" not in self.units and "vehicles/30min" not in self.units or self.frame_minutes == 30, "interval", "30-minute count units require frame_minutes=30")

    @property
    def channel_axis(self) -> Optional[str]:
        return "C" if "C" in self.axes else "D" if "D" in self.axes else None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field": self.field, "axes": list(self.axes),
            "shape": [None if n is None else int(n) for n in self.shape],
            "units": list(self.units), "semantic": self.semantic,
            "version": self.version, "normalization": self.normalization,
            "frame_minutes": int(self.frame_minutes), "grid_id": self.grid_id,
        }


def validate(data: np.ndarray, schema: TensorSchema) -> None:
    """Reject rank/shape/channel mismatches, non-real data and NaN/Inf."""
    _require(isinstance(schema, TensorSchema), "schema", "expected TensorSchema")
    _require(isinstance(data, np.ndarray), "data", "expected numpy.ndarray")
    _require(data.dtype.kind in "iuf", "data", "expected real numeric data")
    _require(data.ndim == len(schema.axes), "shape", f"{schema.field}: rank does not match axes")
    for axis, actual, declared in zip(schema.axes, data.shape, schema.shape):
        _require(actual > 0 and (declared is None or actual == declared), "shape", f"{schema.field}: {axis}={actual}, declared {declared}")
    channels = data.shape[schema.axes.index(schema.channel_axis)] if schema.channel_axis else 1
    _require(channels == len(schema.units), "units", f"{schema.field}: one unit required per actual channel")
    _require(bool(np.isfinite(data).all()), "nonfinite", f"{schema.field}: NaN/Inf")


@dataclass(frozen=True)
class TrainingStats:
    """Per-channel raw-unit statistics, fitted elsewhere on the training split.

    ``source`` identifies the training artifact. A single instance can serve
    both sides only if metadata/units agree; otherwise pass adapter stats as
    ``{'source': source_stats, 'target': target_stats}``.
    """

    field: str
    mean: Tuple[float, ...]
    std: Tuple[float, ...]
    units: Tuple[str, ...]
    semantic: str
    version: str
    source: str
    grid_id: str = "6x6"
    frame_minutes: int = 30
    split: str = "train"

    def __post_init__(self) -> None:
        for name in ("field", "semantic", "version", "source", "grid_id"):
            _text(getattr(self, name), name)
        _units(self.units)
        _require(self.split == "train", "stats", "statistics must come from the training split")
        _require(isinstance(self.frame_minutes, (int, np.integer)) and not isinstance(self.frame_minutes, (bool, np.bool_)) and self.frame_minutes > 0, "stats", "invalid statistics interval")
        for name in ("mean", "std"):
            values = getattr(self, name)
            _require(isinstance(values, tuple) and len(values) == len(self.units), "stats", f"{name}: one value per channel")
            _require(all(isinstance(v, (int, float, np.integer, np.floating)) and not isinstance(v, (bool, np.bool_)) and np.isfinite(v) for v in values), "stats", f"{name}: finite numeric values required")
        _require(all(v > 0 for v in self.std), "stats", "std must be strictly positive")

    def to_dict(self) -> Dict[str, Any]:
        return {
            "field": self.field, "mean": [float(v) for v in self.mean],
            "std": [float(v) for v in self.std], "units": list(self.units),
            "semantic": self.semantic, "version": self.version,
            "source": self.source, "grid_id": self.grid_id,
            "frame_minutes": int(self.frame_minutes), "split": self.split,
        }


StatsArgument = Optional[Union[TrainingStats, Mapping[str, TrainingStats]]]


def _stats_for(stats: StatsArgument, side: str, schema: TensorSchema) -> TrainingStats:
    selected = stats if isinstance(stats, TrainingStats) else stats.get(side) if isinstance(stats, Mapping) else None
    _require(isinstance(selected, TrainingStats), "stats", f"{side}: training statistics required")
    for key in ("field", "semantic", "version", "units", "grid_id", "frame_minutes"):
        _require(getattr(selected, key) == getattr(schema, key), "stats", f"{side}: statistics {key} mismatch")
    return selected


def _channel_vector(values: Tuple[float, ...], axes: List[str], channel_axis: Optional[str]) -> np.ndarray:
    # Reshape only the parameter vector for broadcasting, never tensor data.
    shape = [1] * len(axes)
    if channel_axis:
        shape[axes.index(channel_axis)] = len(values)
    return np.asarray(values, dtype=np.float64).reshape(shape)


def _compatible(source: TensorSchema, target: TensorSchema) -> None:
    for key, code in (("semantic", "semantic"), ("version", "version"), ("grid_id", "grid"), ("frame_minutes", "interval")):
        _require(getattr(source, key) == getattr(target, key), code, f"source/target {key} mismatch")
    _require(source.channel_axis == target.channel_axis, "axes", "channel axis cannot be renamed or removed")
    _require(len(source.units) == len(target.units), "units", "channel counts differ")


def adapter(
    source: np.ndarray,
    source_schema: TensorSchema,
    target_schema: TensorSchema,
    *,
    stats: StatsArgument = None,
    rename_rule: bool = False,
) -> Tuple[np.ndarray, List[Dict[str, Any]]]:
    """Return a separate array and a JSON-safe ordered rule trace.

    ``rename_rule=True`` authorizes this specific field rename only; semantics,
    version, grid and frame interval must still match exactly. Target axes must
    already exist on the source: the service constructs B explicitly before
    declaring the source schema. Removal is restricted to explicitly declared
    B=1/H=1 (actual size 1 alone fails).
    """
    validate(source, source_schema)
    _require(isinstance(target_schema, TensorSchema), "schema", "expected target TensorSchema")
    _require(isinstance(rename_rule, bool), "rename", "rename_rule must be a boolean")
    _compatible(source_schema, target_schema)
    _require(source_schema.field == target_schema.field or rename_rule, "rename", "field rename requires an explicit rule")
    missing_axes = sorted(set(target_schema.axes) - set(source_schema.axes))
    _require(not missing_axes, "axes", f"service must construct axes before source schema declaration: {missing_axes}")
    axes = list(source_schema.axes)
    data = source.copy()
    trace = [{"rule": "validate", "side": "source", "schema": source_schema.to_dict()}]
    if source_schema.field != target_schema.field:
        trace.append({"rule": "rename", "from": source_schema.field, "to": target_schema.field})

    for axis in source_schema.axes:
        if axis not in target_schema.axes:
            declared = source_schema.shape[source_schema.axes.index(axis)]
            _require(axis in _SINGLETON_AXES and declared == 1, "axes", f"squeeze requires declared {axis}=1 and B/H axis")
            index = axes.index(axis)
            data = np.squeeze(data, axis=index)
            axes.pop(index)
            trace.append({"rule": "squeeze", "axis": axis, "index": index, "declared_size": 1})
    permutation = [axes.index(axis) for axis in target_schema.axes]
    if permutation != list(range(len(axes))):
        data = np.transpose(data, permutation)
        trace.append({"rule": "transpose", "from_axes": axes[:], "to_axes": list(target_schema.axes), "permutation": permutation})
    axes = list(target_schema.axes)
    # Reject invalid layouts before any numerical transform, including equal
    # element counts with different declared dimensions.
    for axis, actual, declared in zip(axes, data.shape, target_schema.shape):
        _require(declared is None or actual == declared, "shape", f"target {axis}={actual}, declared {declared}")

    factors = []
    for old_unit, new_unit in zip(source_schema.units, target_schema.units):
        if old_unit == new_unit:
            factors.append(1.0)
        else:
            _require(old_unit in _SPEED_IN_MPS and new_unit in _SPEED_IN_MPS, "units", f"no declared ratio for {old_unit} -> {new_unit}")
            factors.append(_SPEED_IN_MPS[old_unit] / _SPEED_IN_MPS[new_unit])

    source_stats = _stats_for(stats, "source", source_schema) if source_schema.normalization == "standardized" else None
    target_stats = _stats_for(stats, "target", target_schema) if target_schema.normalization == "standardized" else None
    with np.errstate(over="ignore", invalid="ignore", divide="ignore"):
        if source_stats is not None:
            data = data.astype(np.float64) * _channel_vector(source_stats.std, axes, source_schema.channel_axis) + _channel_vector(source_stats.mean, axes, source_schema.channel_axis)
            trace.append({"rule": "denormalize", "stats": source_stats.to_dict()})
        if any(factor != 1.0 for factor in factors):
            data = data.astype(np.float64) * _channel_vector(tuple(factors), axes, target_schema.channel_axis)
            trace.append({"rule": "unit_scale", "from": list(source_schema.units), "to": list(target_schema.units), "factors": factors})
        if target_stats is not None:
            data = (data.astype(np.float64) - _channel_vector(target_stats.mean, axes, target_schema.channel_axis)) / _channel_vector(target_stats.std, axes, target_schema.channel_axis)
            trace.append({"rule": "standardize", "stats": target_stats.to_dict()})
    validate(data, target_schema)
    trace.append({"rule": "validate", "side": "target", "schema": target_schema.to_dict()})
    return data, trace


@dataclass(frozen=True)
class TensorPayload:
    """Bind each ndarray to its own schema and nonempty source artifact ID."""

    data: np.ndarray
    schema: TensorSchema
    source: str

    def __post_init__(self) -> None:
        _text(self.source, "tensor source")
        validate(self.data, self.schema)

    def to_dict(self, *, include_data: bool = False) -> Dict[str, Any]:
        validate(self.data, self.schema)
        result = {"schema": self.schema.to_dict(), "source": self.source, "shape": list(self.data.shape), "dtype": str(self.data.dtype)}
        if include_data:
            result["data"] = self.data.tolist()
        return result


def _coherent(schemas: Mapping[str, TensorSchema]) -> None:
    reference = None
    for key, schema in schemas.items():
        _require(isinstance(schema, TensorSchema), "schema", "expected TensorSchema")
        _require(key == schema.field, "field", "mapping key must match schema field")
        if reference is not None:
            for attr, code in (("version", "version"), ("grid_id", "grid"), ("frame_minutes", "interval")):
                _require(getattr(reference, attr) == getattr(schema, attr), code, f"cross-modality {attr} mismatch")
        reference = schema


@dataclass(frozen=True)
class Payload:
    tensors: Mapping[str, TensorPayload]

    def __post_init__(self) -> None:
        _require(isinstance(self.tensors, Mapping) and bool(self.tensors), "payload", "nonempty tensor mapping required")
        _require(all(isinstance(t, TensorPayload) for t in self.tensors.values()), "payload", "each tensor needs data/schema/source binding")
        object.__setattr__(self, "tensors", MappingProxyType(dict(self.tensors)))
        self.validate()

    def validate(self) -> None:
        _coherent({key: tensor.schema for key, tensor in self.tensors.items()})
        shared = {}
        for tensor in self.tensors.values():
            validate(tensor.data, tensor.schema)
            for axis, size in zip(tensor.schema.axes, tensor.data.shape):
                if axis in _SHARED_AXES:
                    _require(axis not in shared or shared[axis] == size, "shape", f"cross-modality {axis} mismatch")
                    shared[axis] = size

    def to_dict(self, *, include_data: bool = False) -> Dict[str, Any]:
        self.validate()
        return {"tensors": {name: tensor.to_dict(include_data=include_data) for name, tensor in self.tensors.items()}}


@dataclass(frozen=True)
class CapabilityContract:
    name: str
    version: str
    inputs: Mapping[str, TensorSchema]
    outputs: Mapping[str, TensorSchema] = dc_field(default_factory=dict)

    def __post_init__(self) -> None:
        _text(self.name, "capability name")
        _text(self.version, "capability version")
        _require(isinstance(self.inputs, Mapping) and bool(self.inputs), "capability", "declare required input modalities")
        _require(isinstance(self.outputs, Mapping), "capability", "outputs must be a schema mapping")
        for side in ("inputs", "outputs"):
            schemas = dict(getattr(self, side))
            _coherent(schemas)
            _require(all(s.version == self.version for s in schemas.values()), "version", "tensor/capability version mismatch")
            object.__setattr__(self, side, MappingProxyType(schemas))
        all_schemas = list(self.inputs.values()) + list(self.outputs.values())
        for schema in all_schemas[1:]:
            _require(schema.grid_id == all_schemas[0].grid_id, "grid", "input/output grid mismatch")
            _require(schema.frame_minutes == all_schemas[0].frame_minutes, "interval", "input/output interval mismatch")

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "version": self.version, "inputs": {k: s.to_dict() for k, s in self.inputs.items()}, "outputs": {k: s.to_dict() for k, s in self.outputs.items()}}


def _validate_payload(payload: Payload, schemas: Mapping[str, TensorSchema]) -> None:
    _require(isinstance(payload, Payload), "payload", "expected Payload")
    payload.validate()
    missing = sorted(set(schemas) - set(payload.tensors))
    extra = sorted(set(payload.tensors) - set(schemas))
    _require(not missing, "modality", f"missing required modalities: {missing}")
    _require(not extra, "modality", f"undeclared modalities: {extra}")
    for name, expected in schemas.items():
        actual = payload.tensors[name]
        _compatible(actual.schema, expected)
        _require(actual.schema.axes == expected.axes, "axes", f"{name}: use adapter for axis conversion")
        _require(actual.schema.units == expected.units, "units", f"{name}: use adapter for unit conversion")
        _require(actual.schema.normalization == expected.normalization, "normalization", f"{name}: use adapter for normalization")
        validate(actual.data, expected)


def adapt_payload(
    payload: Payload,
    target_schemas: Mapping[str, TensorSchema],
    *,
    stats: Optional[Mapping[str, StatsArgument]] = None,
    rename_rules: Optional[Mapping[str, str]] = None,
) -> Tuple[Payload, Dict[str, List[Dict[str, Any]]]]:
    """Adapt all required modalities atomically.

    ``rename_rules`` maps target field -> source field. ``stats`` maps target
    field -> adapter statistics. No modality may be omitted or consumed twice.
    Returned tensors keep their source artifact IDs; traces record the changes.
    """
    _require(isinstance(payload, Payload), "payload", "expected Payload")
    payload.validate()
    _require(isinstance(target_schemas, Mapping) and bool(target_schemas), "schema", "nonempty target schemas required")
    _coherent(target_schemas)
    renames = dict(rename_rules or {})
    _require(set(renames).issubset(target_schemas), "rename", "rename rule has unknown target")
    _require(stats is None or isinstance(stats, Mapping) and set(stats).issubset(target_schemas), "stats", "statistics must be keyed by target field")
    source_fields = [renames.get(name, name) for name in target_schemas]
    _require(all(isinstance(name, str) for name in source_fields), "rename", "source fields must be strings")
    _require(len(set(source_fields)) == len(source_fields), "modality", "a source modality cannot be reused")
    _require(set(source_fields) == set(payload.tensors), "modality", "missing or undeclared source modalities")
    tensors, traces = {}, {}
    for name, target in target_schemas.items():
        tensor = payload.tensors[renames.get(name, name)]
        data, trace = adapter(tensor.data, tensor.schema, target, stats=stats.get(name) if stats else None, rename_rule=name in renames)
        tensors[name] = TensorPayload(data=data, schema=target, source=tensor.source)
        traces[name] = trace
    result = Payload(tensors=tensors)
    _validate_payload(result, target_schemas)
    return result, traces


class CapabilityRegistry:
    """Local, explicit (name, version) registry; no implicit latest fallback."""

    def __init__(self) -> None:
        self._contracts: Dict[Tuple[str, str], CapabilityContract] = {}

    def register(self, contract: CapabilityContract) -> None:
        _require(isinstance(contract, CapabilityContract), "capability", "expected CapabilityContract")
        key = (contract.name, contract.version)
        _require(key not in self._contracts, "capability", "capability version already registered")
        self._contracts[key] = contract

    def get(self, name: str, version: str) -> CapabilityContract:
        _text(name, "capability name")
        _text(version, "capability version")
        _require((name, version) in self._contracts, "version", f"unregistered capability {name}@{version}")
        return self._contracts[(name, version)]

    def validate(self, name: str, version: str, payload: Payload, *, direction: str = "inputs") -> None:
        _require(direction in ("inputs", "outputs"), "capability", "direction must be inputs or outputs")
        _validate_payload(payload, getattr(self.get(name, version), direction))

    def adapt(self, name: str, version: str, payload: Payload, *, stats: Optional[Mapping[str, StatsArgument]] = None, rename_rules: Optional[Mapping[str, str]] = None) -> Tuple[Payload, Dict[str, List[Dict[str, Any]]]]:
        return adapt_payload(payload, self.get(name, version).inputs, stats=stats, rename_rules=rename_rules)

    def to_dict(self) -> Dict[str, Any]:
        return {"capabilities": [self._contracts[key].to_dict() for key in sorted(self._contracts)]}
