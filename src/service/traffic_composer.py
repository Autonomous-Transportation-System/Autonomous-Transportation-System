"""Compile traffic templates into frozen plans, without loading or running B1.

Register model_version/normalization_sha256 and spatial/window declarations in
capability contracts. Tensor ``inputs``/``outputs`` may use TensorSchema.to_dict
declarations; compatible connections get a deterministic adapter trace. Runtime
remains responsible for tensor validation, fitted statistics and execution.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping
from threading import RLock
from uuid import uuid4

from src.service.traffic_catalog import TrafficCatalog, CatalogError, _freeze, _plain, _text, _number, _integer
from src.service.traffic_types import PlanNode, WorkflowPlan

__all__ = ["TrafficWorkflowCompiler", "CompilationError"]

_DEFAULTS = dict(data_ref="nyc_tlc_green_2024_01_6x6", grid_id="nyc-green-6x6-v1",
                 frame_minutes=30, history_length=3, horizon=1, frame_count=1488, max_batch=32)
_IDENTITY = ("data_ref", "data_version", "region_id", "region", "grid_id", "grid", "frame_minutes",
             "history_length", "horizon", "frame_count", "max_batch", "model_version", "schema_version",
             "normalization_sha256")
_REQUEST_KEYS = frozenset(_IDENTITY) | frozenset(("template", "target_frames", "deadline_seconds", "priority",
    "max_attempts", "timeout_seconds", "enable_reuse", "enable_contracts", "enable_recovery",
    "contract_overrides", "allowed_instances", "top_k", "speed_threshold_mph", "active_trip_threshold"))
_OVERRIDE_KEYS = frozenset(_IDENTITY) | frozenset(("version", "semantic", "normalization", "axes", "units", "inputs", "outputs"))
_SPEED = {"mph": 0.44704, "kmh": 1 / 3.6, "km/h": 1 / 3.6, "m/s": 1.0}
_UNITS = frozenset(_SPEED) | {"trips/30min", "active trips", "count", "1", "vehicles", "vehicles/30min"}
_PORTS = {
    "history.prepare": ("inputs", "schema_trace"), "traffic.predict": ("state", "flow", "od"),
    "validate.state": ("state",), "validate.flow": ("flow",), "validate.od": ("od",),
    "analysis.congestion": ("analysis",), "analysis.od_hotspots": ("analysis",),
    "result.aggregate": ("predictions", "analysis"),
}
_INPUT_PORTS = {
    "history.prepare": {}, "traffic.predict": {"model_inputs": "inputs"},
    "validate.state": {"prediction": "state"}, "validate.flow": {"prediction": "flow"},
    "validate.od": {"prediction": "od"}, "analysis.congestion": {"state": "state", "flow": "flow"},
    "analysis.od_hotspots": {"od": "od"},
}


def _binding(node_id, output):
    return dict(node_id=node_id, output=output)


def _validate_filter_declaration(contract):
    """Ablations can vary values, never scalar types or declaration structure."""
    for key in ("frame_minutes", "history_length", "horizon", "frame_count", "max_batch"):
        if key in contract:
            _integer(contract[key], key)
    for key in ("data_ref", "data_version", "region_id", "region", "grid_id", "version", "model_version", "schema_version", "semantic"):
        if key in contract:
            _text(contract[key], key)
    if "normalization_sha256" in contract:
        _reject(isinstance(contract["normalization_sha256"], str)
                and bool(re.fullmatch(r"[0-9a-fA-F]{64}", contract["normalization_sha256"])),
                "normalization", "filter normalization_sha256 must be a SHA256 digest")
    if "grid" in contract:
        _reject(isinstance(contract["grid"], (list, tuple)) and len(contract["grid"]) == 2, "contract", "grid needs two dimensions")
        for dimension in contract["grid"]:
            _integer(dimension, "grid dimension")
    for key in ("inputs", "outputs"):
        if key in contract:
            _reject(isinstance(contract[key], Mapping), "contract", f"{key} must be a schema mapping")
    if "axes" in contract:
        axes = contract["axes"]
        _reject(isinstance(axes, (list, tuple)) and axes and all(isinstance(axis, str) for axis in axes)
                and len(set(axes)) == len(axes) and set(axes) <= {"B", "L", "H", "C", "Y", "X", "D"},
                "contract", "invalid filter axes")
    if "units" in contract:
        units = contract["units"]
        _reject(isinstance(units, (list, tuple)) and units
                and all(isinstance(unit, str) and unit in _UNITS for unit in units), "contract", "invalid filter units")
    if "normalization" in contract:
        _reject(contract["normalization"] in ("raw", "standardized"), "contract", "invalid filter normalization")


def _default_template(template, outputs):
    nodes = [dict(node_id="prepare", capability_id="history.prepare", depends_on=(), parameters={"inputs": {}}),
             dict(node_id="predict", capability_id="traffic.predict", depends_on=("prepare",),
                  parameters={"inputs": {"model_inputs": _binding("prepare", "inputs")}})]
    aggregate_inputs = {}
    dependencies = []
    for field in outputs:
        name = "validate_" + field
        nodes.append(dict(node_id=name, capability_id="validate." + field, depends_on=("predict",),
                          parameters={"inputs": {"prediction": _binding("predict", field)}}))
        aggregate_inputs[field] = _binding(name, field)
        dependencies.append(name)
    if template != "flow":
        capability = "analysis.congestion" if template == "analysis" else "analysis.od_hotspots"
        nodes.append(dict(node_id="analysis", capability_id=capability, depends_on=tuple(dependencies),
                          parameters={"inputs": dict(aggregate_inputs)}))
        aggregate_inputs["analysis"] = _binding("analysis", "analysis")
        dependencies.append("analysis")
    nodes.append(dict(node_id="aggregate", capability_id="result.aggregate", depends_on=tuple(dependencies),
                      parameters={"inputs": aggregate_inputs}))
    return dict(requested_outputs=outputs, nodes=nodes)


class CompilationError(ValueError):
    def __init__(self, code: str, message: str):
        self.code = code
        super().__init__(f"{code}: {message}")


def _reject(condition, code, message):
    if not condition:
        raise CompilationError(code, message)


def _schema_trace(source, target, sha):
    """Symbolic equivalent of the strict adapter; never touches tensor data."""
    _reject(isinstance(source, Mapping) and isinstance(target, Mapping), "contract", "tensor schemas must be mappings")
    for schema in (source, target):
        _reject(all(isinstance(schema.get(key), str) and schema[key].strip()
                    for key in ("field", "semantic", "version", "grid_id")), "contract", "declare tensor identity and semantics")
        _reject(type(schema.get("frame_minutes")) is int and schema["frame_minutes"] > 0,
                "contract", "invalid tensor frame interval")
    for key in ("field", "semantic", "version", "grid_id", "frame_minutes"):
        _reject(key in source and key in target and source[key] == target[key], "contract", f"tensor {key} mismatch")
    old, new = source.get("axes"), target.get("axes")
    old_shape, new_shape = source.get("shape"), target.get("shape")
    _reject(isinstance(old, (list, tuple)) and isinstance(new, (list, tuple))
            and old and new and all(isinstance(axis, str) for axis in (*old, *new))
            and len(set(old)) == len(old) and len(set(new)) == len(new)
            and set(old) <= {"B", "L", "H", "C", "Y", "X", "D"}
            and set(new) <= set(old), "contract", "invalid tensor axes; adapter cannot insert axes")
    _reject(isinstance(old_shape, (list, tuple)) and isinstance(new_shape, (list, tuple))
            and len(old_shape) == len(old) and len(new_shape) == len(new), "contract", "invalid tensor shape")
    _reject(all(item is None or type(item) is int and item > 0 for item in (*old_shape, *new_shape)),
            "contract", "invalid tensor dimension")
    old_channel = "C" if "C" in old else "D" if "D" in old else None
    new_channel = "C" if "C" in new else "D" if "D" in new else None
    _reject(old_channel == new_channel and not {"C", "D"} <= set(old), "contract", "tensor channel semantics mismatch")
    trace = []
    remaining = list(old)
    for axis, size in zip(old, old_shape):
        if axis not in new:
            _reject(axis in ("B", "H") and size == 1, "contract", "only declared B=1/H=1 may be removed")
            trace.append(dict(rule="squeeze", axis=axis, declared_size=1))
            remaining.remove(axis)
    for axis, size in zip(new, new_shape):
        declared = old_shape[old.index(axis)]
        _reject(size is None or declared == size, "contract", f"unproven target dimension for {axis}")
    if remaining != list(new):
        trace.append(dict(rule="transpose", from_axes=remaining, to_axes=list(new),
                          permutation=[remaining.index(axis) for axis in new]))
    units, target_units = source.get("units"), target.get("units")
    _reject(isinstance(units, (list, tuple)) and isinstance(target_units, (list, tuple))
            and units and len(units) == len(target_units), "contract", "invalid channel units")
    for axes, shape, declared_units in ((old, old_shape, units), (new, new_shape, target_units)):
        channels = shape[axes.index(old_channel)] if old_channel else 1
        _reject(channels is None or channels == len(declared_units), "contract", "one unit required per channel")
    factors = []
    for source_unit, target_unit in zip(units, target_units):
        _reject(isinstance(source_unit, str) and isinstance(target_unit, str)
                and source_unit in _UNITS and target_unit in _UNITS, "contract", "unsupported units")
        _reject(not ({source_unit, target_unit} & {"trips/30min", "vehicles/30min"})
                or source["frame_minutes"] == 30, "contract", "30-minute counts require 30-minute frames")
        _reject(source_unit == target_unit or source_unit in _SPEED and target_unit in _SPEED,
                "contract", f"unsupported unit conversion: {source_unit} -> {target_unit}")
        factors.append(1.0 if source_unit == target_unit else _SPEED[source_unit] / _SPEED[target_unit])
    old_norm, new_norm = source.get("normalization", "raw"), target.get("normalization", "raw")
    _reject(old_norm in ("raw", "standardized") and new_norm in ("raw", "standardized"), "contract", "invalid normalization")
    transform = old_norm != new_norm or any(factor != 1 for factor in factors)
    if transform and old_norm == "standardized":
        trace.append(dict(rule="denormalize", normalization_sha256=sha, split="train"))
    if any(factor != 1 for factor in factors):
        trace.append(dict(rule="unit_scale", factors=factors, **{"from": list(units), "to": list(target_units)}))
    if transform and new_norm == "standardized":
        trace.append(dict(rule="standardize", normalization_sha256=sha, split="train"))
    trace.append(dict(rule="validate", side="target", schema=target))
    return trace


def _validate_tensor_contract(contract, version, sha):
    """Check declared schemas even when the node has no tensor consumer."""
    def visit(schemas, *, direction):
        _reject(isinstance(schemas, Mapping), "contract", "tensor declarations must be mappings")
        for name, schema in schemas.items():
            _reject(isinstance(name, str) and isinstance(schema, Mapping), "contract", "invalid tensor declaration")
            if "axes" not in schema:
                _reject(name in ("inputs", "model_inputs"), "contract", "incomplete tensor schema")
                visit(schema, direction=direction)
                continue
            _schema_trace(schema, schema, sha)
            _reject(schema["version"] == version, "version", "tensor and implementation versions differ")
            for key in ("grid_id", "frame_minutes"):
                _reject(key not in contract or contract[key] == schema[key], "contract", f"tensor/capability {key} mismatch")
            _reject(name == schema["field"] or direction == "inputs" and name in ("prediction", "model_inputs"),
                    "semantic", "tensor port and field semantics differ")
    for direction in ("inputs", "outputs"):
        if direction in contract:
            visit(contract[direction], direction=direction)


class TrafficWorkflowCompiler:
    """Compile flow, analysis and od_hotspots against registered instances.

    Contract overrides simulate catalog filtering only. With enable_contracts
    false their filters are omitted, while execution identity and tensor rules
    remain intact. Reuse is limited to joint prepare/predict work and includes
    complete ordered absolute windows plus model/data/normalization identity.
    """

    def __init__(self, catalog, model_version, normalization_sha256):
        if not isinstance(catalog, TrafficCatalog):
            raise CompilationError("catalog", "catalog must be a TrafficCatalog")
        try:
            self.model_version = _text(model_version, "model_version")
            _text(normalization_sha256, "normalization_sha256")
        except CatalogError as exc:
            raise CompilationError(exc.code, str(exc)) from exc
        _reject(bool(re.fullmatch(r"[0-9a-fA-F]{64}", normalization_sha256)), "normalization", "normalization_sha256 must be a SHA256 digest")
        self.normalization_sha256 = normalization_sha256.lower()
        self.catalog = catalog
        self._templates = {}
        self._template_lock = RLock()
        for template, outputs in (("flow", ("flow",)), ("analysis", ("state", "flow")), ("od_hotspots", ("od",))):
            self.register_template(template, _default_template(template, outputs))

    def register_template(self, template_id, definition) -> None:
        """Register an immutable DAG definition (nodes, requested_outputs, contract).

        Node dictionaries use node_id/capability_id/depends_on/parameters and
        optional contract/outputs. Unknown capabilities must declare their
        output port names; their executor remains the caller's responsibility.
        Known traffic capabilities cannot change their execution ports.
        """
        try:
            _text(template_id, "template_id")
            _reject(isinstance(definition, Mapping), "template", "template definition must be a mapping")
            definition = _plain(definition)
            _reject(set(definition) <= {"nodes", "requested_outputs", "contract"}, "template", "unknown template fields")
            raw_nodes = definition.get("nodes")
            outputs = definition.get("requested_outputs")
            _reject(isinstance(raw_nodes, list) and raw_nodes, "dag", "template needs a nonempty node list")
            _reject(isinstance(outputs, list) and outputs and all(isinstance(field, str) and field.strip() for field in outputs)
                    and len(set(outputs)) == len(outputs), "template", "declare unique requested_outputs")
            _reject(isinstance(definition.get("contract", {}), dict), "contract", "template contract must be a mapping")
            _validate_filter_declaration(definition.get("contract", {}))
            nodes = []
            for raw in raw_nodes:
                _reject(isinstance(raw, dict) and set(raw) <= {"node_id", "capability_id", "depends_on", "parameters", "outputs", "contract"},
                        "dag", "unknown or malformed node fields")
                node_id, capability = _text(raw.get("node_id"), "node_id"), _text(raw.get("capability_id"), "capability_id")
                _reject(":" not in node_id, "dag", "node_id cannot contain the task-ID separator")
                deps = raw.get("depends_on", [])
                _reject(isinstance(deps, list) and all(isinstance(item, str) for item in deps)
                        and len(set(deps)) == len(deps), "dag", "depends_on must contain unique node IDs")
                parameters = raw.get("parameters", {})
                _reject(isinstance(parameters, dict), "binding", "parameters must be a mapping")
                parameters.setdefault("inputs", {})
                _reject(isinstance(raw.get("contract", {}), dict), "contract", "node contract must be a mapping")
                _validate_filter_declaration(raw.get("contract", {}))
                for key in ("speed_threshold_mph", "active_trip_threshold"):
                    if key in parameters:
                        _number(parameters[key], key)
                if "top_k" in parameters:
                    _integer(parameters["top_k"], "top_k")
                ports = raw.get("outputs", _PORTS.get(capability))
                _reject(isinstance(ports, (list, tuple)) and ports and all(isinstance(port, str) and port.strip() for port in ports)
                        and len(set(ports)) == len(ports), "port", "declare unique output ports")
                _reject(capability not in _PORTS or set(ports) == set(_PORTS[capability]), "port", "known capability ports cannot be changed")
                raw.update(depends_on=tuple(deps), parameters=parameters, outputs=tuple(ports))
                nodes.append(PlanNode(node_id, capability, tuple(deps), parameters))
            self._validate_dag(nodes)
            self._validate_bindings(raw_nodes)
            aggregates = [node for node in raw_nodes if node["capability_id"] == "result.aggregate"]
            for node in aggregates:
                bound = set(node["parameters"]["inputs"])
                _reject(bool(bound) and bound <= set(outputs) | {"analysis"},
                        "semantic", "aggregate bindings must match requested_outputs")
            consumed = {dependency for node in raw_nodes for dependency in node["depends_on"]}
            terminal_ports = set()
            for node in raw_nodes:
                if node["node_id"] in consumed:
                    continue
                ports = (node["parameters"]["inputs"] if node["capability_id"] == "result.aggregate"
                         else node["outputs"])
                terminal_ports.update(ports)
            _reject(set(outputs) <= terminal_ports, "port", "requested_outputs lack terminal producers")
            with self._template_lock:
                _reject(template_id not in self._templates, "template", "template already registered")
                self._templates[template_id] = _freeze(definition)
        except CatalogError as exc:
            raise CompilationError(exc.code, str(exc)) from exc

    def compile(self, request: dict, app_id="", workflow_id=None, now=0) -> WorkflowPlan:
        try:
            return self._compile(request, app_id, workflow_id, now)
        except CatalogError as exc:
            raise CompilationError(exc.code, str(exc)) from exc

    def _compile(self, request, app_id, workflow_id, now):
        _reject(isinstance(request, dict), "request", "request must be a dict")
        _reject(set(request) <= _REQUEST_KEYS, "request", "unknown request fields")
        request = _plain(request)
        template = request.get("template", "flow")
        _reject(isinstance(template, str), "template", "template must be a string")
        with self._template_lock:
            _reject(template in self._templates, "template", "unknown traffic template")
            definition = self._templates[template]
        _reject(isinstance(app_id, str), "request", "app_id must be a string")
        workflow_id = _text(workflow_id, "workflow_id") if workflow_id is not None else uuid4().hex
        now = _number(now, "now")
        priority = request.get("priority", 0)
        _reject(type(priority) is int and priority >= 0, "numeric", "priority must be a nonnegative integer")
        attempts = _integer(request.get("max_attempts", 2), "max_attempts")
        timeout = _number(request.get("timeout_seconds", 30), "timeout_seconds", positive=True)
        deadline = None
        if "deadline_seconds" in request:
            deadline = now + _number(request["deadline_seconds"], "deadline_seconds", positive=True)
            _reject(math.isfinite(deadline), "numeric", "absolute deadline overflow")
        options = {}
        for key in ("enable_reuse", "enable_contracts", "enable_recovery"):
            options[key] = request.get(key, True)
            _reject(type(options[key]) is bool, "request", f"{key} must be a boolean")
        overrides = request.get("contract_overrides", {})
        if overrides is None:
            overrides = {}
        _reject(isinstance(overrides, dict) and set(overrides) <= _OVERRIDE_KEYS, "contract", "unknown or malformed contract_overrides")
        _validate_filter_declaration(overrides)
        allowed = request.get("allowed_instances")
        _reject(allowed is None or isinstance(allowed, (list, tuple)), "instance", "allowed_instances must be a list of IDs")
        if allowed is not None:
            for item in allowed:
                _text(item, "allowed instance")
        catalog = self.catalog._copy_for_selection()
        snapshot = catalog.snapshot()
        identity = dict(_DEFAULTS, model_version=self.model_version, schema_version=self.model_version,
                        normalization_sha256=self.normalization_sha256)
        # Runtime can declare a different dataset/grid/frame count without any
        # model/data paths entering this compiler. Inconsistent declarations
        # are removed by the contract query below.
        prepare_query = {"model_version": self.model_version, "normalization_sha256": self.normalization_sha256}
        prepare_query.update({key: value for key, value in request.items() if key in _IDENTITY})
        prepare = catalog.candidates("history.prepare", contract=prepare_query, allowed_instances=allowed)
        if not prepare:
            prepare = catalog.candidates("history.prepare", contract={"model_version": self.model_version,
                                         "normalization_sha256": self.normalization_sha256}, allowed_instances=allowed)
        if prepare:
            description = snapshot["implementations"][prepare[0].implementation_id]["capabilities"]["history.prepare"]["contract"]
            identity.update({key: description[key] for key in _IDENTITY if key in description})
        for key in _IDENTITY:
            if key in request:
                _reject(key not in identity or request[key] == identity[key], "contract", f"request {key} differs from registered execution contract")
                identity[key] = request[key]
        _reject(identity["model_version"] == self.model_version and identity["schema_version"] == self.model_version,
                "version", "runtime and catalog versions differ")
        _reject(identity["normalization_sha256"] == self.normalization_sha256, "normalization", "runtime and catalog normalization differ")
        for key in ("data_ref", "data_version", "region_id", "region", "grid_id", "model_version", "schema_version", "normalization_sha256"):
            if key in identity:
                _text(identity[key], key)
        for key in ("frame_minutes", "history_length", "horizon", "frame_count", "max_batch"):
            identity[key] = _integer(identity[key], key)
        _reject(1440 % identity["frame_minutes"] == 0, "frames", "frame_minutes must divide a day")
        if "grid" in identity:
            _reject(isinstance(identity["grid"], list) and len(identity["grid"]) == 2, "contract", "grid must contain rows and columns")
            for dimension in identity["grid"]:
                _integer(dimension, "grid dimension")
        frames = request.get("target_frames")
        _reject(isinstance(frames, (list, tuple)) and 1 <= len(frames) <= identity["max_batch"],
                "frames", "target_frames must be a nonempty bounded list")
        day = 1440 // identity["frame_minutes"]
        history, horizon = identity["history_length"], identity["horizon"]
        for frame in frames:
            _reject(type(frame) is int and history <= frame and frame + horizon <= identity["frame_count"],
                    "frames", "target frame outside available history/horizon")
            _reject((frame - history) // day == (frame + horizon - 1) // day,
                    "frames", "history and forecast must remain in the same day")
        frames = tuple(frames)
        _reject(len(set(frames)) == len(frames), "frames", "target_frames must be unique")
        windows = tuple(dict(history_start=frame - history, history_end=frame,
                             target_start=frame, target_end=frame + horizon) for frame in frames)
        outputs = tuple(definition["requested_outputs"])
        specs = self._topological_specs(definition["nodes"])
        analysis_options = {}
        for key in ("speed_threshold_mph", "active_trip_threshold"):
            if key in request:
                _reject(any(item["capability_id"] == "analysis.congestion" for item in specs), "request", f"{key} requires congestion analysis")
                analysis_options[key] = _number(request[key], key)
        if "top_k" in request:
            _reject(any(item["capability_id"] == "analysis.od_hotspots" for item in specs), "request", "top_k requires OD analysis")
            analysis_options["top_k"] = _integer(request["top_k"], "top_k")
            grid = identity.get("grid", [6, 6])
            _reject(analysis_options["top_k"] <= (grid[0] * grid[1]) ** 2, "numeric", "top_k exceeds OD grid pairs")
        # Version/normalization are never disabled by the contract ablation.
        query = {"model_version": self.model_version, "normalization_sha256": self.normalization_sha256}
        query.update({key: identity[key] for key in _IDENTITY if key in request})
        if options["enable_contracts"]:
            # Only declared or explicitly requested optional keys are required;
            # versioned legacy descriptions need not invent a region label.
            declared = {key for item in snapshot["implementations"].values()
                        for desc in item["capabilities"].values() for key in desc["contract"]}
            query.update({key: value for key, value in identity.items() if key in declared or key in request})
            query.update(overrides)
        options.update(contract_overrides=overrides, allowed_instances=allowed)
        nodes = []
        for spec in specs:
            node_id, capability, dependencies = spec["node_id"], spec["capability_id"], spec["depends_on"]
            node_query = dict(query)
            node_query.update(definition.get("contract", {}))
            node_query.update(spec.get("contract", {}))
            for key, value in (("version", self.model_version), ("model_version", self.model_version),
                               ("normalization_sha256", self.normalization_sha256)):
                _reject(key not in node_query or node_query[key] == value, "contract", f"template or request cannot change execution {key}")
            candidates = catalog.candidates(capability, node_query, allowed)
            _reject(bool(candidates), "candidates", f"no healthy compatible candidates for {capability}")
            parameters = _plain(spec["parameters"])
            parameters.update(target_frames=frames, windows=windows, contract=identity, requested_outputs=outputs,
                              output_ports=spec["outputs"])
            if capability in ("history.prepare", "traffic.predict"):
                parameters["task"] = "all"
            if capability.startswith("validate."):
                parameters["field"] = capability.split(".", 1)[1]
            if capability.startswith("analysis."):
                parameters.update(analysis_options)
            adapters = {}
            adapter_schemas = {}
            compatible = []
            for candidate in candidates:
                target = snapshot["implementations"][candidate.implementation_id]["capabilities"][capability]["contract"]
                traces = {}
                matched_schemas = {}
                try:
                    _validate_tensor_contract(target, candidate.version, self.normalization_sha256)
                    _reject(set(target.get("outputs", {})) <= set(spec["outputs"]), "port", "catalog declares unsupported output ports")
                    declared_inputs = target.get("inputs", {})
                    _reject(isinstance(declared_inputs, Mapping), "contract", "input schemas must be a mapping")
                    _reject(not declared_inputs or bool(parameters["inputs"]), "binding", "declared tensor inputs need explicit producers")
                    if parameters["inputs"] and "model_inputs" not in parameters["inputs"]:
                        supplied_ports = set(parameters["inputs"]) | {
                            binding["output"] for binding in parameters["inputs"].values()}
                        _reject(set(declared_inputs) <= supplied_ports, "contract", "declared tensor input lacks a binding")
                    for input_name, binding in parameters["inputs"].items():
                        dependency = binding["node_id"]
                        predecessor = next(node for node in nodes if node.node_id == dependency)
                        for source_candidate in predecessor.candidates:
                            source = snapshot["implementations"][source_candidate.implementation_id]["capabilities"][predecessor.capability_id]["contract"]
                            output = binding["output"]
                            target_schemas = target.get("inputs", {})
                            source_schemas = source.get("outputs", {})
                            _reject(isinstance(source_schemas, Mapping), "contract", "output schemas must be a mapping")
                            if input_name == "model_inputs":
                                source_schemas = source_schemas.get("inputs", source_schemas)
                                target_schemas = target_schemas.get("model_inputs", target_schemas)
                                _reject(isinstance(source_schemas, Mapping) and isinstance(target_schemas, Mapping),
                                        "contract", "model input bundle must declare tensor schemas")
                            else:
                                expected_schema = target_schemas.get(input_name, target_schemas.get(output))
                                target_schemas = {output: expected_schema} if expected_schema is not None else {}
                            matched_fields = matched_schemas.setdefault(input_name, {}).setdefault(source_candidate.instance_id, {})
                            for field, target_schema in target_schemas.items():
                                _reject(field in source_schemas, "contract", "bound output lacks declared tensor schema")
                                traces[dependency + ":" + source_candidate.instance_id + ":" + field] = _schema_trace(
                                    source_schemas[field], target_schema, self.normalization_sha256)
                                schema_field = field if input_name == "model_inputs" else target_schema["field"]
                                source_schema_dict = _plain(source_schemas[field])
                                target_schema_dict = _plain(target_schema)
                                source_schema_dict.setdefault("normalization", "raw")
                                target_schema_dict.setdefault("normalization", "raw")
                                matched_fields[schema_field] = dict(
                                    source=source_schema_dict, target=target_schema_dict)
                except CompilationError:
                    continue
                compatible.append(candidate)
                adapters[candidate.instance_id] = traces
                adapter_schemas[candidate.instance_id] = matched_schemas
            _reject(bool(compatible), "contract", f"no semantically compatible connections for {capability}")
            parameters["candidate_adapters"] = adapters
            parameters["candidate_adapter_schemas"] = adapter_schemas
            reuse_key = None
            if options["enable_reuse"] and capability in ("history.prepare", "traffic.predict"):
                input_keys = {name: next(node for node in nodes if node.node_id == binding["node_id"]).reuse_key
                              for name, binding in parameters["inputs"].items()}
                material = dict(capability_id=capability, contract=identity, target_frames=frames,
                                windows=windows, task="all", input_keys=input_keys,
                                execution_parameters={key: value for key, value in parameters.items()
                                                      if key not in ("inputs", "requested_outputs", "candidate_adapters", "candidate_adapter_schemas", "output_ports")},
                                tensor_contracts=sorted({
                                    json.dumps(snapshot["implementations"][item.implementation_id]["capabilities"][capability]["contract"],
                                               sort_keys=True, separators=(",", ":")) for item in compatible}))
                if all(key is not None for key in input_keys.values()):
                    reuse_key = capability + ":" + hashlib.sha256(json.dumps(material, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()
            nodes.append(PlanNode(node_id=node_id, capability_id=capability, depends_on=dependencies,
                                  parameters=_freeze(parameters), candidates=tuple(compatible), priority=priority,
                                  max_attempts=attempts if options["enable_recovery"] else 1,
                                  timeout_seconds=timeout, reuse_key=reuse_key))
        self._validate_dag(nodes)
        return WorkflowPlan(workflow_id=workflow_id, app_id=app_id, template=template, nodes=tuple(nodes),
                            target_frames=frames, requested_outputs=outputs, contract=_freeze(identity), created_at=now,
                            deadline=deadline, priority=priority, options=_freeze(options))

    @staticmethod
    def _validate_bindings(specs):
        by_id = {spec["node_id"]: spec for spec in specs}
        for spec in specs:
            inputs = spec["parameters"].get("inputs")
            _reject(isinstance(inputs, Mapping), "binding", "inputs must be a binding mapping")
            capability = spec["capability_id"]
            if capability in _INPUT_PORTS:
                _reject(set(inputs) == set(_INPUT_PORTS[capability]), "binding", "missing or undeclared capability input")
            for input_name, binding in inputs.items():
                _reject(isinstance(input_name, str) and input_name.strip() and isinstance(binding, Mapping)
                        and set(binding) == {"node_id", "output"}, "binding", "binding requires node_id and output")
                producer, output = binding["node_id"], binding["output"]
                _reject(isinstance(producer, str) and producer in spec["depends_on"], "binding", "input producer must belong to depends_on")
                _reject(isinstance(output, str) and output in by_id[producer]["outputs"], "port", "unknown producer output port")
                if capability in _INPUT_PORTS:
                    _reject(output == _INPUT_PORTS[capability][input_name], "semantic", "input bound to the wrong modality")
                elif capability == "result.aggregate":
                    _reject(input_name in ("state", "flow", "od", "analysis") and output == input_name,
                            "semantic", "aggregate input must bind its declared modality")

    @staticmethod
    def _topological_specs(specs):
        pending, ordered, completed = list(specs), [], set()
        while pending:
            ready = [spec for spec in pending if set(spec["depends_on"]) <= completed]
            _reject(bool(ready), "cycle", "template DAG contains a cycle")
            for spec in ready:
                ordered.append(spec)
                completed.add(spec["node_id"])
                pending.remove(spec)
        return ordered

    @staticmethod
    def _validate_dag(nodes):
        identifiers = [node.node_id for node in nodes]
        _reject(len(set(identifiers)) == len(identifiers), "dag", "duplicate node IDs")
        dependencies = {node.node_id: set(node.depends_on) for node in nodes}
        _reject(all(required <= set(identifiers) for required in dependencies.values()), "dag", "unknown dependency")
        while dependencies:
            ready = {key for key, required in dependencies.items() if not required}
            _reject(bool(ready), "cycle", "workflow DAG contains a cycle")
            dependencies = {key: required - ready for key, required in dependencies.items() if key not in ready}
