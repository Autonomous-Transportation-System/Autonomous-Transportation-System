"""Catalog selection, resource/profile semantics and declaration isolation."""
from dataclasses import FrozenInstanceError

import pytest

from src.service.traffic_catalog import CatalogError, TrafficCatalog


def implementation(identifier="b1", version="B1:test", **contract):
    return dict(implementation_id=identifier, version=version, contract=dict(
        normalization_sha256="a" * 64, grid_id="grid-v1", region_id="region-v1",
        frame_minutes=30, history_length=3, horizon=1, **contract), capabilities={
        "traffic.predict": dict(resources=dict(cpu=2, memory_mb=128),
                                profile=dict(expected_seconds=0.5, cold_start_seconds=1, adapter_seconds=0.1)),
        "validate.flow": dict(resources=dict(cpu=0.25, memory_mb=16))})


def instance(identifier="one", implementation_id="b1", **kwargs):
    return dict(instance_id=identifier, implementation_id=implementation_id,
                endpoint="local://" + identifier, max_concurrency=2, base_memory_mb=512, **kwargs)


@pytest.fixture
def catalog():
    catalog = TrafficCatalog()
    catalog.register_implementation(implementation())
    catalog.register_instance(instance())
    catalog.register_instance(instance("two"))
    return catalog


def test_real_capability_instance_resources_and_profile(catalog):
    candidates = catalog.candidates("traffic.predict")
    assert isinstance(candidates, tuple)
    assert [candidate.instance_id for candidate in candidates] == ["one", "two"]
    candidate = candidates[0]
    assert (candidate.capability_id, candidate.implementation_id, candidate.version) == ("traffic.predict", "b1", "B1:test")
    assert (candidate.cpu, candidate.memory_mb, candidate.max_concurrency) == (2, 128, 2)
    assert catalog.snapshot()["instances"]["one"]["base_memory_mb"] == 512
    assert (candidate.expected_seconds, candidate.cold_start_seconds, candidate.adapter_seconds) == (0.5, 1, 0.1)
    assert catalog.candidates("missing") == ()
    assert catalog.candidates("traffic.predict", allowed_instances=[]) == ()
    assert [item.instance_id for item in catalog.candidates("traffic.predict", allowed_instances=["two"])] == ["two"]


@pytest.mark.parametrize("key,value", [("version", "other"), ("model_version", "other"),
    ("grid_id", "other"), ("region_id", "other"), ("frame_minutes", 60),
    ("history_length", 6), ("horizon", 2), ("normalization_sha256", "b" * 64), ("missing", "declared")])
def test_contract_mismatches_filter(catalog, key, value):
    assert catalog.candidates("traffic.predict", contract={key: value}) == ()


def test_contracts_and_health_do_not_fall_back_to_latest(catalog):
    catalog.register_implementation(implementation("b2", "B1:new"))
    catalog.register_instance(instance("new", "b2"))
    assert [item.instance_id for item in catalog.candidates("traffic.predict", {"version": "B1:new"})] == ["new"]
    original = catalog.candidates("traffic.predict", {"version": "B1:test"})
    catalog.set_health("one", False, "inference failure")
    assert [item.instance_id for item in catalog.candidates("traffic.predict", {"version": "B1:test"})] == ["two"]
    assert original[0].healthy is True
    assert catalog.snapshot()["instances"]["one"]["health_reason"] == "inference failure"
    catalog.set_health("one", True)
    assert len(catalog.candidates("traffic.predict", {"version": "B1:test"})) == 2


def test_measured_profile_is_per_instance_and_capability(catalog):
    catalog.record_execution("one", "traffic.predict", 2, True)
    catalog.record_execution("one", "traffic.predict", 4, True)
    catalog.record_execution("one", "traffic.predict", 100, False)
    candidates = catalog.candidates("traffic.predict")
    assert [item.expected_seconds for item in candidates] == [3, 0.5]
    assert catalog.candidates("validate.flow")[0].expected_seconds == 0.01
    record = catalog.snapshot()["executions"][0]
    assert (record["executions"], record["successes"], record["failures"], record["total_seconds"]) == (3, 2, 1, 106)


def test_registration_snapshot_and_candidates_are_isolated():
    catalog = TrafficCatalog()
    description = implementation()
    node = instance()
    catalog.register_implementation(description)
    catalog.register_instance(node)
    description["capabilities"]["traffic.predict"]["resources"]["cpu"] = 999
    description["contract"]["grid_id"] = "changed"
    node["healthy"] = False
    snapshot = catalog.snapshot()
    snapshot["instances"]["one"]["healthy"] = False
    snapshot["implementations"]["b1"]["capabilities"]["traffic.predict"]["contract"]["grid_id"] = "changed"
    assert catalog.candidates("traffic.predict", {"grid_id": "grid-v1"})[0].cpu == 2
    with pytest.raises(FrozenInstanceError):
        catalog.candidates("traffic.predict")[0].healthy = False


@pytest.mark.parametrize("field,value", [("cpu", True), ("cpu", 0), ("cpu", -1),
    ("memory_mb", float("nan")), ("expected_seconds", float("inf")), ("cold_start_seconds", -1)])
def test_invalid_implementation_values_are_atomic(field, value):
    catalog = TrafficCatalog()
    description = implementation()
    target = "resources" if field in ("cpu", "memory_mb") else "profile"
    description["capabilities"]["traffic.predict"][target][field] = value
    with pytest.raises(CatalogError):
        catalog.register_implementation(description)
    assert catalog.snapshot()["implementations"] == {}


@pytest.mark.parametrize("change", [dict(max_concurrency=True), dict(max_concurrency=0),
    dict(base_memory_mb=-1), dict(healthy=1), dict(endpoint=""), dict(implementation_id="missing")])
def test_invalid_instances_are_atomic(catalog, change):
    description = instance("bad")
    description.update(change)
    with pytest.raises(CatalogError):
        catalog.register_instance(description)
    assert "bad" not in catalog.snapshot()["instances"]


def test_duplicate_and_unknown_operations(catalog):
    with pytest.raises(CatalogError, match="duplicate"):
        catalog.register_implementation(implementation())
    with pytest.raises(CatalogError, match="duplicate"):
        catalog.register_instance(instance())
    with pytest.raises(CatalogError, match="instance"):
        catalog.set_health("missing", False)
    with pytest.raises(CatalogError, match="capability"):
        catalog.record_execution("one", "missing", 1, True)
    with pytest.raises(CatalogError):
        catalog.record_execution("one", "traffic.predict", True, True)
    with pytest.raises(CatalogError):
        catalog.record_execution("one", "traffic.predict", 1, 1)
    with pytest.raises(CatalogError):
        catalog.candidates("traffic.predict", allowed_instances="one")


def test_conflicting_capability_version_rejected():
    catalog = TrafficCatalog()
    description = implementation()
    description["capabilities"]["traffic.predict"]["contract"] = {"version": "wrong"}
    with pytest.raises(CatalogError, match="version"):
        catalog.register_implementation(description)


def test_profile_total_and_execution_totals_cannot_overflow(catalog):
    description = implementation("huge")
    description["capabilities"]["traffic.predict"]["profile"] = dict(expected_seconds=1e308, cold_start_seconds=1e308)
    with pytest.raises(CatalogError, match="finite"):
        catalog.register_implementation(description)
    catalog.record_execution("one", "traffic.predict", 1e308, True)
    before = catalog.snapshot()
    with pytest.raises(CatalogError, match="finite"):
        catalog.record_execution("one", "traffic.predict", 1e308, True)
    assert catalog.snapshot() == before
