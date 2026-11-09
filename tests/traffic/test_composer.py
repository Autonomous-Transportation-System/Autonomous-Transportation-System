"""Frozen template plans, contracts, absolute windows and illegal requests."""
import json
import subprocess
import sys
from dataclasses import asdict

import pytest

from src.service.traffic_catalog import TrafficCatalog
from src.service.traffic_composer import CompilationError, TrafficWorkflowCompiler
from src.service.traffic_types import PlanNode

VERSION = "B1_seed42_L3:test-digest"
SHA = "a" * 64
CAPABILITIES = ("history.prepare", "traffic.predict", "validate.state", "validate.flow", "validate.od",
                "analysis.congestion", "analysis.od_hotspots", "result.aggregate")


def make_catalog(version=VERSION, sha=SHA, contracts=None):
    catalog = TrafficCatalog()
    common = dict(model_version=version, schema_version=version, normalization_sha256=sha,
                  data_ref="nyc_tlc_green_2024_01_6x6", data_version="data-sha-v1", grid_id="nyc-green-6x6-v1",
                  region_id="nyc-green", grid=[6, 6], frame_minutes=30, history_length=3,
                  horizon=1, frame_count=1488, max_batch=32)
    catalog.register_implementation(dict(implementation_id="b1", version=version, contract=common,
        capabilities={name: dict(resources=dict(cpu=1, memory_mb=32), profile=dict(expected_seconds=0.2),
                                 contract=(contracts or {}).get(name, {})) for name in CAPABILITIES}))
    for name in ("one", "two"):
        catalog.register_instance(dict(instance_id=name, implementation_id="b1", endpoint="local://" + name,
                                       max_concurrency=1, healthy=True, base_memory_mb=512))
    return catalog


@pytest.fixture
def compiler():
    return TrafficWorkflowCompiler(make_catalog(), VERSION, SHA)


def predict(plan):
    return next(node for node in plan.nodes if node.node_id == "predict")


@pytest.mark.parametrize("template,outputs,capabilities", [
    ("flow", ("flow",), ("history.prepare", "traffic.predict", "validate.flow", "result.aggregate")),
    ("analysis", ("state", "flow"), ("history.prepare", "traffic.predict", "validate.state", "validate.flow", "analysis.congestion", "result.aggregate")),
    ("od_hotspots", ("od",), ("history.prepare", "traffic.predict", "validate.od", "analysis.od_hotspots", "result.aggregate")),
])
def test_three_templates_validate_only_requested_outputs(compiler, template, outputs, capabilities):
    plan = compiler.compile(dict(template=template, target_frames=[1251]), "app", "workflow", 10)
    assert (plan.workflow_id, plan.app_id, plan.template, plan.target_frames, plan.requested_outputs) == ("workflow", "app", template, (1251,), outputs)
    assert tuple(node.capability_id for node in plan.nodes) == capabilities
    completed = set()
    for node in plan.nodes:
        assert set(node.depends_on) <= completed
        assert len(node.candidates) == 2
        assert all(item.capability_id == node.capability_id and item.version == VERSION for item in node.candidates)
        completed.add(node.node_id)
    assert plan.nodes[1].depends_on == ("prepare",)
    if template != "flow":
        analysis = next(node for node in plan.nodes if node.node_id == "analysis")
        assert analysis.depends_on == tuple("validate_" + field for field in outputs)
    assert predict(plan).parameters["task"] == "all"


def test_request_controls_deadline_priority_retries_and_timeout(compiler):
    plan = compiler.compile(dict(target_frames=[1251], deadline_seconds=2.5, priority=3,
                                 max_attempts=4, timeout_seconds=7), now=10)
    assert (plan.created_at, plan.deadline, plan.priority) == (10, 12.5, 3)
    assert all((node.priority, node.max_attempts, node.timeout_seconds) == (3, 4, 7) for node in plan.nodes)
    assert compiler.compile(dict(target_frames=[1251])).deadline is None
    disabled = compiler.compile(dict(target_frames=[1251], enable_reuse=False, enable_recovery=False, max_attempts=4))
    assert all(node.max_attempts == 1 and node.reuse_key is None for node in disabled.nodes)


def test_reuse_is_cross_template_but_window_and_version_safe(compiler):
    first = compiler.compile(dict(template="flow", target_frames=[1251, 1252]))
    analysis = compiler.compile(dict(template="analysis", target_frames=[1251, 1252]))
    assert predict(first).reuse_key == predict(analysis).reuse_key
    assert first.nodes[0].reuse_key == analysis.nodes[0].reuse_key
    assert predict(first).reuse_key != predict(compiler.compile(dict(target_frames=[1252, 1251]))).reuse_key
    assert predict(first).reuse_key != predict(compiler.compile(dict(target_frames=[1251]))).reuse_key
    other = TrafficWorkflowCompiler(make_catalog("B1:new"), "B1:new", SHA)
    assert predict(first).reuse_key != predict(other.compile(dict(target_frames=[1251, 1252]))).reuse_key
    other_sha = TrafficWorkflowCompiler(make_catalog(sha="b" * 64), VERSION, "b" * 64)
    assert predict(first).reuse_key != predict(other_sha.compile(dict(target_frames=[1251, 1252]))).reuse_key
    catalog = make_catalog(contracts={name: {"data_version": "different"} for name in CAPABILITIES})
    other_data = TrafficWorkflowCompiler(catalog, VERSION, SHA)
    assert predict(first).reuse_key != predict(other_data.compile(dict(target_frames=[1251, 1252]))).reuse_key
    assert first.nodes[0].parameters["windows"][0] == dict(history_start=1248, history_end=1251, target_start=1251, target_end=1252)
    assert all(node.reuse_key is None for node in first.nodes[2:])


def test_healthy_candidates_and_explicit_instance_allowlist(compiler):
    compiler.catalog.set_health("one", False)
    plan = compiler.compile(dict(target_frames=[1251]))
    assert all([item.instance_id for item in node.candidates] == ["two"] for node in plan.nodes)
    with pytest.raises(CompilationError, match="candidates"):
        compiler.compile(dict(target_frames=[1251], allowed_instances=["one"]))
    compiler.catalog.set_health("two", False)
    with pytest.raises(CompilationError, match="candidates"):
        compiler.compile(dict(target_frames=[1251]))


@pytest.mark.parametrize("key,value", [("model_version", "wrong"), ("schema_version", "wrong"),
    ("normalization_sha256", "b" * 64), ("grid_id", "wrong"), ("region_id", "wrong"),
    ("frame_minutes", 60), ("history_length", 6), ("horizon", 2), ("data_ref", "wrong")])
def test_wrong_execution_contract_always_rejected(compiler, key, value):
    with pytest.raises(CompilationError):
        compiler.compile(dict(target_frames=[1251], **{key: value}))
    with pytest.raises(CompilationError):
        compiler.compile(dict(target_frames=[1251], enable_contracts=False, **{key: value}))


def test_controlled_contract_ablation_preserves_actual_execution(compiler):
    request = dict(target_frames=[1251], contract_overrides={"region_id": "incompatible"})
    with pytest.raises(CompilationError, match="candidates"):
        compiler.compile(request)
    disabled = compiler.compile(dict(request, enable_contracts=False))
    normal = compiler.compile(dict(target_frames=[1251]))
    assert disabled.contract == normal.contract
    assert predict(disabled).reuse_key == predict(normal).reuse_key
    assert disabled.options["contract_overrides"] == request["contract_overrides"]
    assert predict(disabled).parameters["contract"]["region_id"] == "nyc-green"


def schema(field="flow", axes=None, shape=None, **kwargs):
    return dict(field=field, axes=axes or ["B", "H", "Y", "X", "C"],
                shape=shape or [None, 1, 6, 6, 2], units=["trips/30min", "trips/30min"],
                semantic="origin;destination", version=VERSION, grid_id="nyc-green-6x6-v1",
                frame_minutes=30, normalization="raw", **kwargs)


def tensor_compiler(source, target):
    return TrafficWorkflowCompiler(make_catalog(contracts={
        "traffic.predict": {"outputs": {"flow": source}},
        "validate.flow": {"inputs": {"flow": target}, "outputs": {"flow": target}},
    }), VERSION, SHA)


def test_deterministic_axis_and_normalization_adapter_plan():
    source = schema()
    target = schema(axes=["B", "H", "C", "Y", "X"], shape=[None, 1, 2, 6, 6])
    target["normalization"] = "standardized"
    plan = tensor_compiler(source, target).compile(dict(target_frames=[1251]))
    node = next(node for node in plan.nodes if node.node_id == "validate_flow")
    trace = node.parameters["candidate_adapters"]["one"]["predict:one:flow"]
    assert [item["rule"] for item in trace] == ["transpose", "standardize", "validate"]
    assert trace[1]["normalization_sha256"] == SHA and trace[1]["split"] == "train"


@pytest.mark.parametrize("change", [dict(semantic="speed"), dict(version="wrong"), dict(grid_id="wrong"),
    dict(frame_minutes=60), dict(axes=["B", "L", "Y", "X", "C"]), dict(shape=[None, 2, 6, 6, 2]),
    dict(units=["mph", "mph"]), dict(normalization="minmax")])
def test_semantic_tensor_errors_reject_even_in_ablation(change):
    source = schema()
    target = dict(source, **change)
    compiler = tensor_compiler(source, target)
    for enabled in (True, False):
        with pytest.raises(CompilationError, match="contract"):
            compiler.compile(dict(target_frames=[1251], enable_contracts=enabled))


def test_missing_capability_and_missing_tensor_modality_rejected():
    catalog = TrafficCatalog()
    catalog.register_implementation(dict(implementation_id="empty", version=VERSION,
        contract={"normalization_sha256": SHA}, capabilities={"history.prepare": {}}))
    catalog.register_instance(dict(instance_id="one", implementation_id="empty", endpoint="local://one"))
    with pytest.raises(CompilationError, match="traffic.predict"):
        TrafficWorkflowCompiler(catalog, VERSION, SHA).compile(dict(target_frames=[1251]))
    with pytest.raises(CompilationError, match="contract"):
        tensor_compiler(schema(field="state"), schema()).compile(dict(target_frames=[1251]))


def test_plan_recursively_frozen_detached_and_json_serializable(compiler):
    request = dict(target_frames=[1251], contract_overrides={"grid": [6, 6]})
    plan = compiler.compile(request)
    request["target_frames"][0] = 1252
    request["contract_overrides"]["grid"][0] = 99
    assert plan.target_frames == (1251,)
    assert plan.options["contract_overrides"]["grid"] == (6, 6)
    with pytest.raises(TypeError):
        plan.contract["grid_id"] = "changed"
    with pytest.raises(TypeError):
        predict(plan).parameters["windows"][0]["target_start"] = 99
    with pytest.raises(TypeError):
        plan.options.update(enable_reuse=False)
    compiler.catalog.set_health("one", False)
    assert predict(plan).candidates[0].healthy is True
    assert json.loads(json.dumps(asdict(plan)))["target_frames"] == [1251]


@pytest.mark.parametrize("payload", [None, [], {}, dict(target_frames=[]), dict(target_frames="1251"),
    dict(target_frames=[True]), dict(target_frames=[1251.0]), dict(target_frames=["1251"]),
    dict(target_frames=[-1]), dict(target_frames=[2]), dict(target_frames=[1488]),
    dict(target_frames=[48]), dict(target_frames=[49]), dict(target_frames=[50]),
    dict(target_frames=[10**100]), dict(target_frames=[1251] * 33), dict(target_frames=[1251], template="unknown"),
    dict(target_frames=[1251], unexpected=1), dict(target_frames=[1251], priority=True), dict(target_frames=[1251], priority=-1),
    dict(target_frames=[1251], priority=1.5), dict(target_frames=[1251], max_attempts=0),
    dict(target_frames=[1251], max_attempts=True), dict(target_frames=[1251], max_attempts=1.5),
    dict(target_frames=[1251], deadline_seconds=0), dict(target_frames=[1251], deadline_seconds=True),
    dict(target_frames=[1251], deadline_seconds=float("inf")), dict(target_frames=[1251], deadline_seconds=float("nan")),
    dict(target_frames=[1251], timeout_seconds=-1), dict(target_frames=[1251], enable_reuse=1),
    dict(target_frames=[1251], enable_contracts="false"), dict(target_frames=[1251], enable_recovery=None),
    dict(target_frames=[1251], contract_overrides=[]), dict(target_frames=[1251], contract_overrides={"path": "/bad"}),
    dict(target_frames=[1251], allowed_instances="one"), dict(target_frames=[1251], allowed_instances=[]),
    dict(target_frames=[1251], history_length=True), dict(target_frames=[1251], frame_minutes=30.0),
    dict(target_frames=[1251, 1251])])
def test_invalid_requests_rejected(compiler, payload):
    with pytest.raises(CompilationError):
        compiler.compile(payload)


@pytest.mark.parametrize("now", [-1, True, float("inf"), float("nan"), "0"])
def test_invalid_monotonic_time(compiler, now):
    with pytest.raises(CompilationError):
        compiler.compile(dict(target_frames=[1251]), now=now)


def test_cycles_unknown_edges_and_duplicate_ids_rejected():
    for nodes in [(PlanNode("a", "x", ("b",)), PlanNode("b", "x", ("a",))),
                  (PlanNode("a", "x", ("missing",)),), (PlanNode("a", "x"), PlanNode("a", "x"))]:
        with pytest.raises(CompilationError):
            TrafficWorkflowCompiler._validate_dag(nodes)


def test_compiler_import_has_no_torch_or_model_execution_dependency():
    script = "import sys; from src.service.traffic_composer import TrafficWorkflowCompiler; assert 'torch' not in sys.modules; assert 'src.service.traffic_model' not in sys.modules"
    subprocess.run([sys.executable, "-c", script], check=True)


@pytest.mark.parametrize("version,sha", [("", SHA), (VERSION, "bad"), (VERSION, "g" * 64), (VERSION, True)])
def test_runtime_identity_must_be_explicit_and_valid(version, sha):
    with pytest.raises(CompilationError):
        TrafficWorkflowCompiler(make_catalog(), version, sha)


@pytest.mark.parametrize("template", ["flow", "analysis", "od_hotspots"])
def test_explicit_bindings_use_dependency_output_ports(compiler, template):
    plan = compiler.compile(dict(template=template, target_frames=[1251]))
    nodes = {node.node_id: node for node in plan.nodes}
    assert predict(plan).parameters["inputs"] == {"model_inputs": {"node_id": "prepare", "output": "inputs"}}
    for node in plan.nodes:
        for binding in node.parameters["inputs"].values():
            assert binding["node_id"] in node.depends_on
            assert binding["output"] in nodes[binding["node_id"]].parameters["output_ports"]
    aggregate = nodes["aggregate"]
    for field in plan.requested_outputs:
        assert nodes["validate_" + field].parameters["inputs"] == {"prediction": {"node_id": "predict", "output": field}}
        assert aggregate.parameters["inputs"][field] == {"node_id": "validate_" + field, "output": field}
    if template != "flow":
        assert nodes["analysis"].parameters["inputs"] == {
            field: {"node_id": "validate_" + field, "output": field} for field in plan.requested_outputs}
        assert aggregate.parameters["inputs"]["analysis"] == {"node_id": "analysis", "output": "analysis"}


def custom_template():
    # Deliberately not supplied in topological order.
    return dict(requested_outputs=["flow"], nodes=[
        dict(node_id="sink", capability_id="result.aggregate", depends_on=["checked"],
             parameters={"inputs": {"flow": {"node_id": "checked", "output": "flow"}}}),
        dict(node_id="checked", capability_id="validate.flow", depends_on=["joint"],
             parameters={"inputs": {"prediction": {"node_id": "joint", "output": "flow"}}}),
        dict(node_id="joint", capability_id="traffic.predict", depends_on=["history"],
             parameters={"inputs": {"model_inputs": {"node_id": "history", "output": "inputs"}}}),
        dict(node_id="history", capability_id="history.prepare", parameters={"inputs": {}}),
    ])


def test_custom_template_sorted_and_defensively_frozen(compiler):
    definition = custom_template()
    compiler.register_template("custom-flow", definition)
    definition["nodes"][0]["parameters"]["inputs"]["flow"]["output"] = "state"
    definition["requested_outputs"].append("state")
    plan = compiler.compile(dict(template="custom-flow", target_frames=[1251]))
    assert [node.node_id for node in plan.nodes] == ["history", "joint", "checked", "sink"]
    assert plan.requested_outputs == ("flow",)
    assert plan.nodes[-1].parameters["inputs"]["flow"]["output"] == "flow"
    with pytest.raises(TypeError):
        plan.nodes[-1].parameters["inputs"]["flow"]["output"] = "state"
    with pytest.raises(CompilationError, match="already registered"):
        compiler.register_template("custom-flow", custom_template())


@pytest.mark.parametrize("aggregate_state", [True, False])
def test_requested_outputs_can_be_split_across_terminal_exports(compiler, aggregate_state):
    definition = custom_template()
    definition["requested_outputs"] = ["flow", "state"]
    definition["nodes"].append(dict(node_id="state_checked", capability_id="validate.state",
        depends_on=["joint"], parameters={"inputs": {"prediction": {"node_id": "joint", "output": "state"}}}))
    if aggregate_state:
        definition["nodes"].append(dict(node_id="state_export", capability_id="result.aggregate",
            depends_on=["state_checked"], parameters={"inputs": {"state": {"node_id": "state_checked", "output": "state"}}}))
    compiler.register_template("split", definition)
    plan = compiler.compile(dict(template="split", target_frames=[1251]))
    assert plan.requested_outputs == ("flow", "state")
    assert next(node for node in plan.nodes if node.node_id == "sink").parameters["inputs"] == {
        "flow": {"node_id": "checked", "output": "flow"}}


def test_partial_aggregate_does_not_allow_missing_terminal_output(compiler):
    definition = custom_template()
    definition["requested_outputs"] = ["flow", "state"]
    with pytest.raises(CompilationError, match="lack terminal producers"):
        compiler.register_template("missing-state", definition)


@pytest.mark.parametrize("problem", ["cycle", "unknown_dep", "unknown_port", "not_dep", "wrong_modality",
                                      "missing_input", "parameters", "binding", "duplicate", "fake_output"])
def test_custom_binding_and_dag_errors_rejected(compiler, problem):
    definition = custom_template()
    sink, checked, joint, history = definition["nodes"]
    if problem == "cycle":
        history["depends_on"] = ["joint"]
    elif problem == "unknown_dep":
        sink["depends_on"] = ["absent"]
    elif problem == "unknown_port":
        sink["parameters"]["inputs"]["flow"]["output"] = "absent"
    elif problem == "not_dep":
        checked["parameters"]["inputs"]["prediction"]["node_id"] = "history"
    elif problem == "wrong_modality":
        checked["parameters"]["inputs"]["prediction"]["output"] = "state"
    elif problem == "missing_input":
        joint["parameters"]["inputs"] = {}
    elif problem == "parameters":
        joint["parameters"] = []
    elif problem == "binding":
        checked["parameters"]["inputs"]["prediction"]["extra"] = 1
    elif problem == "duplicate":
        sink["node_id"] = "checked"
    elif problem == "fake_output":
        joint["outputs"] = ["fake"]
    with pytest.raises(CompilationError):
        compiler.register_template("bad", definition)


def test_generic_capability_and_custom_contract_selection(compiler):
    compiler.catalog.register_implementation(dict(implementation_id="rules", version=VERSION,
        contract=dict(normalization_sha256=SHA, schema_version=VERSION, grid_id="nyc-green-6x6-v1",
                      data_version="data-sha-v1", data_ref="nyc_tlc_green_2024_01_6x6", region_id="nyc-green",
                      grid=[6, 6], frame_minutes=30, history_length=3, horizon=1, frame_count=1488, max_batch=32),
        capabilities={"analysis.custom": {"contract": {"semantic": "custom-score-v1"}}}))
    compiler.catalog.register_instance(dict(instance_id="rules-one", implementation_id="rules", endpoint="local://rules"))
    definition = dict(requested_outputs=["score"], nodes=[dict(node_id="score", capability_id="analysis.custom",
                     outputs=["score"], contract={"semantic": "custom-score-v1"}, parameters={"inputs": {}})])
    compiler.register_template("score", definition)
    assert compiler.compile(dict(template="score", target_frames=[1251])).nodes[0].candidates[0].instance_id == "rules-one"
    definition["nodes"][0]["contract"]["semantic"] = "incompatible"
    compiler.register_template("wrong-score", definition)
    with pytest.raises(CompilationError, match="candidates"):
        compiler.compile(dict(template="wrong-score", target_frames=[1251]))


@pytest.mark.parametrize("field,value", [("top_k", True), ("top_k", 0), ("top_k", 1297),
    ("speed_threshold_mph", True), ("speed_threshold_mph", float("nan")), ("active_trip_threshold", -1)])
def test_illegal_analysis_controls_rejected(compiler, field, value):
    template = "od_hotspots" if field == "top_k" else "analysis"
    with pytest.raises(CompilationError):
        compiler.compile(dict(template=template, target_frames=[1251], **{field: value}))


def test_runtime_flat_metadata_and_catalog_import_compatibility():
    from src.service.traffic_catalog import TrafficWorkflowCompiler as CompatibleCompiler
    catalog = TrafficCatalog()
    catalog.register_implementation(dict(implementation_id="B1", version=VERSION, capabilities={
        capability: dict(cpu=1, memory_mb=8, expected_seconds=0.01,
                         contract=dict(model_version=VERSION, normalization_sha256=SHA,
                                       grid_id="nyc-green-6x6-v1", frame_minutes=30)) for capability in CAPABILITIES}))
    catalog.register_instance(dict(instance_id="runtime", implementation_id="B1", endpoint="inproc://runtime", base_memory_mb=512))
    plan = CompatibleCompiler(catalog, VERSION, SHA).compile(dict(target_frames=[1251], contract_overrides=None))
    assert predict(plan).candidates[0].memory_mb == 8


def test_reuse_independent_of_health_and_template_node_names(compiler):
    first = predict(compiler.compile(dict(target_frames=[1251]))).reuse_key
    compiler.catalog.set_health("one", False)
    assert predict(compiler.compile(dict(target_frames=[1251]))).reuse_key == first
    compiler.register_template("renamed", custom_template())
    custom = compiler.compile(dict(template="renamed", target_frames=[1251]))
    assert custom.nodes[1].reuse_key == first
    changed = custom_template()
    changed["nodes"][-1]["parameters"]["history_transform"] = "different-v1"
    compiler.register_template("changed", changed)
    assert compiler.compile(dict(template="changed", target_frames=[1251])).nodes[1].reuse_key != first


@pytest.mark.parametrize("frame", [3, 47, 51, 1487])
def test_same_day_window_boundaries(compiler, frame):
    assert compiler.compile(dict(target_frames=[frame])).target_frames == (frame,)


@pytest.mark.parametrize("overrides", [{"frame_minutes": True}, {"history_length": 3.0}, {"grid": [6, False]},
    {"normalization_sha256": "not-a-digest"}, {"region_id": ""}, {"inputs": []}, {"axes": ["Y", "Y"]}])
def test_malformed_override_types_rejected_in_ablation(compiler, overrides):
    with pytest.raises(CompilationError):
        compiler.compile(dict(target_frames=[1251], enable_contracts=False, contract_overrides=overrides))


def test_runtime_tensor_bundles_and_scheduler_plan_compatibility():
    from src.graph.traffic_joint_scheduler import JointScheduler
    # Match runtime's explicit bundle input/output metadata exactly.
    tensor_schemas = {"flow": schema()}
    catalog = TrafficCatalog()
    ports = {
        "history.prepare": {"inputs": {}, "outputs": {"inputs": tensor_schemas}},
        "traffic.predict": {"inputs": {"model_inputs": tensor_schemas}, "outputs": tensor_schemas},
        "validate.flow": {"inputs": {"prediction": schema()}, "outputs": tensor_schemas},
        "result.aggregate": {"inputs": {}, "outputs": {}},
    }
    catalog.register_implementation(dict(implementation_id="bundle", version=VERSION,
        contract=dict(grid_id="nyc-green-6x6-v1", frame_minutes=30, normalization_sha256=SHA),
        capabilities={key: {"contract": value} for key, value in ports.items()}))
    catalog.register_instance(dict(instance_id="bundle", implementation_id="bundle", endpoint="inproc://bundle"))
    plan = TrafficWorkflowCompiler(catalog, VERSION, SHA).compile(dict(target_frames=[1251]), workflow_id="bundle", now=0)
    scheduler = JointScheduler(cpu_budget=4, memory_budget=1024, max_concurrency=2)
    scheduler.submit(plan, 0)
    now = 0
    while not scheduler.snapshot("bundle")["workflows"]["bundle"]["done"]:
        assignments = scheduler.tick(now)
        assert assignments
        for assignment in assignments:
            scheduler.start(assignment.task_id, now)
            scheduler.finish(assignment.task_id, now, result={})
        now += 1
    assert scheduler.snapshot("bundle")["workflows"]["bundle"]["status"] == "completed"


@pytest.mark.parametrize("side", ["inputs", "outputs"])
def test_generic_schema_ports_cannot_be_unbound_or_invented(compiler, side):
    common = compiler.catalog.snapshot()["implementations"]["b1"]["contract"]
    compiler.catalog.register_implementation(dict(implementation_id="bad-rule", version=VERSION,
        contract=common, capabilities={"rule.bad": {"contract": {side: {"flow": schema()}}}}))
    compiler.catalog.register_instance(dict(instance_id="bad-rule", implementation_id="bad-rule", endpoint="local://bad-rule"))
    compiler.register_template("bad-rule", dict(requested_outputs=["score"], nodes=[
        dict(node_id="score", capability_id="rule.bad", outputs=["score"], parameters={"inputs": {}})]))
    with pytest.raises(CompilationError, match="contract"):
        compiler.compile(dict(template="bad-rule", target_frames=[1251]))


def adapter_schema_compiler():
    source_schemas, target_schemas, forecasts = {}, {}, {}
    for field in ("state", "flow", "od"):
        channels = 36 if field == "od" else 2
        channel_axis = "D" if field == "od" else "C"
        source = schema(field=field, axes=["B", "L", "Y", "X", channel_axis],
                        shape=[None, 3, 6, 6, channels])
        source["units"] = (["active trips", "mph"] if field == "state"
                           else ["trips/30min"] * channels)
        source["semantic"] = field + "_channels"
        target = dict(source, normalization="standardized",
                      axes=["B", "L", channel_axis, "Y", "X"],
                      shape=[None, 3, channels, 6, 6])
        source_schemas[field], target_schemas[field] = source, target
        forecasts[field] = dict(source, axes=["B", "H", "Y", "X", channel_axis],
                                shape=[None, 1, 6, 6, channels])
    validated_flow = dict(forecasts["flow"], normalization="standardized",
                          axes=["B", "H", "C", "Y", "X"], shape=[None, 1, 2, 6, 6])
    catalog = make_catalog(contracts={
        "history.prepare": {"inputs": {}, "outputs": {"inputs": source_schemas}},
        "traffic.predict": {"inputs": {"model_inputs": target_schemas}, "outputs": forecasts},
        "validate.flow": {"inputs": {"prediction": validated_flow}, "outputs": {"flow": validated_flow}},
    })
    return TrafficWorkflowCompiler(catalog, VERSION, SHA), source_schemas, target_schemas


def test_candidate_adapter_schemas_execute_bundle_and_single_port():
    import numpy as np
    from src.service.traffic_contracts import TensorSchema, TrainingStats, adapter

    compiler, _, _ = adapter_schema_compiler()
    plan = compiler.compile(dict(target_frames=[1251]))
    bundle = predict(plan).parameters["candidate_adapter_schemas"]
    validator = next(node for node in plan.nodes if node.node_id == "validate_flow")
    single = validator.parameters["candidate_adapter_schemas"]
    aggregate = next(node for node in plan.nodes if node.node_id == "aggregate")
    passthrough = aggregate.parameters["candidate_adapter_schemas"]
    assert set(bundle) == set(single) == {"one", "two"}
    required = {"field", "axes", "shape", "units", "semantic", "version", "normalization", "frame_minutes", "grid_id"}

    def execute(pair):
        schemas = []
        for side in ("source", "target"):
            declaration = dict(pair[side])
            assert set(declaration) == required
            for key in ("axes", "shape", "units"):
                declaration[key] = tuple(declaration[key])
            schemas.append(TensorSchema(**declaration))
        source, target = schemas
        shape = tuple(1 if dimension is None else dimension for dimension in source.shape)
        values = np.arange(np.prod(shape), dtype=np.float64).reshape(shape) + 10
        stats = TrainingStats(field=target.field, mean=(10.0,) * len(target.units),
                              std=(2.0,) * len(target.units), units=target.units, semantic=target.semantic,
                              version=target.version, source="train-artifact", grid_id=target.grid_id,
                              frame_minutes=target.frame_minutes)
        actual, trace = adapter(values, source, target, stats=stats)
        expected = (np.transpose(values, [source.axes.index(axis) for axis in target.axes]) - 10) / 2
        np.testing.assert_allclose(actual, expected)
        assert any(item["rule"] == "transpose" for item in trace)
        assert any(item["rule"] == "standardize" for item in trace)

    for target_instance in ("one", "two"):
        assert set(bundle[target_instance]) == {"model_inputs"}
        assert set(single[target_instance]) == {"prediction"}
        assert set(passthrough[target_instance]) == {"flow"}
        for source_instance in ("one", "two"):
            assert passthrough[target_instance]["flow"][source_instance] == {}
            pairs = bundle[target_instance]["model_inputs"][source_instance]
            assert set(pairs) == {"state", "flow", "od"}
            for pair in pairs.values():
                execute(pair)
            pairs = single[target_instance]["prediction"][source_instance]
            assert set(pairs) == {"flow"}
            execute(pairs["flow"])


def test_candidate_adapter_schemas_are_detached_frozen_and_instance_neutral():
    compiler, source_schemas, target_schemas = adapter_schema_compiler()
    plan = compiler.compile(dict(target_frames=[1251]))
    node = predict(plan)
    pair = node.parameters["candidate_adapter_schemas"]["one"]["model_inputs"]["two"]["flow"]
    source_schemas["flow"]["shape"][1] = 99
    target_schemas["flow"]["axes"][0] = "X"
    assert pair["source"]["shape"] == (None, 3, 6, 6, 2)
    assert pair["target"]["axes"] == ("B", "L", "C", "Y", "X")
    with pytest.raises(TypeError):
        pair["source"]["normalization"] = "changed"
    with pytest.raises(TypeError):
        pair["target"]["axes"][0] = "X"
    with pytest.raises(TypeError):
        node.parameters["candidate_adapter_schemas"]["one"]["model_inputs"].clear()
    detached = json.loads(json.dumps(asdict(plan)))
    detached["nodes"][1]["parameters"]["candidate_adapter_schemas"]["one"]["model_inputs"]["two"]["flow"]["source"]["shape"][1] = 99
    assert pair["source"]["shape"][1] == 3

    snapshot = compiler.catalog.snapshot()
    renamed = TrafficCatalog()
    for implementation in snapshot["implementations"].values():
        renamed.register_implementation(implementation)
    for new_id, instance in zip(("alpha", "beta"), snapshot["instances"].values()):
        renamed.register_instance(dict(instance, instance_id=new_id, endpoint="local://" + new_id))
    renamed_plan = TrafficWorkflowCompiler(renamed, VERSION, SHA).compile(dict(target_frames=[1251]))
    assert set(predict(renamed_plan).parameters["candidate_adapter_schemas"]) == {"alpha", "beta"}
    assert predict(renamed_plan).reuse_key == node.reuse_key
    compiler.catalog.set_health("one", False)
    remaining = predict(compiler.compile(dict(target_frames=[1251])))
    assert set(remaining.parameters["candidate_adapter_schemas"]) == {"two"}
    assert set(remaining.parameters["candidate_adapter_schemas"]["two"]["model_inputs"]) == {"two"}
    assert remaining.reuse_key == node.reuse_key

    changed = TrafficCatalog()
    for implementation in snapshot["implementations"].values():
        implementation["capabilities"]["traffic.predict"]["contract"]["inputs"]["model_inputs"]["flow"]["normalization"] = "raw"
        changed.register_implementation(implementation)
    for instance in snapshot["instances"].values():
        changed.register_instance(instance)
    changed_plan = TrafficWorkflowCompiler(changed, VERSION, SHA).compile(dict(target_frames=[1251]))
    assert predict(changed_plan).reuse_key != node.reuse_key
