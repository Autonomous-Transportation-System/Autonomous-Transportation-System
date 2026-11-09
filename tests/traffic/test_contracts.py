"""Real traffic layouts, physical conversions and explicit refusal rules."""

import json
import os
from dataclasses import replace
from pathlib import Path

import numpy as np
import pytest

from src.service.traffic_contracts import (
    CapabilityContract, CapabilityRegistry, ContractError, Payload, TensorPayload,
    TensorSchema, TrainingStats, adapt_payload, adapter, validate,
)


def state_schema(**changes):
    defaults = dict(field="state", axes=("B", "L", "C", "Y", "X"), shape=(1, 3, 2, 6, 6), units=("active trips", "mph"), semantic="active_trip_count_by_origin;mean_completed_trip_speed_by_destination", version="1.0", grid_id="nyc-green-6x6-v1")
    defaults.update(changes)
    return TensorSchema(**defaults)


def flow_schema(**changes):
    defaults = dict(field="flow", units=("trips/30min", "trips/30min"), semantic="dropoffs_in_interval;pickups_in_interval")
    defaults.update(changes)
    return state_schema(**defaults)


def od_schema(**changes):
    defaults = dict(field="od", axes=("B", "L", "Y", "X", "D"), shape=(1, 3, 6, 6, 36), units=("trips/30min",) * 36, semantic="completed_trip_count_by_origin_destination_dropoff_interval")
    defaults.update(changes)
    return state_schema(**defaults)


def training_stats(schema, **changes):
    defaults = dict(field=schema.field, mean=(10.0, 30.0), std=(2.0, 5.0), units=schema.units, semantic=schema.semantic, version=schema.version, source="processed/train-frames-0-1008/stats.json", grid_id=schema.grid_id, frame_minutes=schema.frame_minutes)
    defaults.update(changes)
    return TrainingStats(**defaults)


def array_for(schema):
    return np.arange(np.prod(schema.shape), dtype=np.float64).reshape(schema.shape)


def bound(schema, data=None):
    return TensorPayload(array_for(schema) if data is None else data, schema, "processed/" + schema.field + ".npy")


def model_payload():
    return Payload({s.field: bound(s) for s in (flow_schema(), state_schema(), od_schema())})


def rules(trace):
    json.dumps(trace, allow_nan=False)
    return [step["rule"] for step in trace]


def service_batch(raw, storage_schema):
    # The calling service owns this construction, before strict validation.
    batched = np.expand_dims(raw, axis=0)
    schema = replace(storage_schema, axes=("B",) + storage_schema.axes, shape=(1,) + storage_schema.shape)
    return batched, schema


@pytest.mark.parametrize("factory", [state_schema, flow_schema])
def test_storage_to_model_and_back_preserves_coordinates(factory):
    storage = factory(axes=("L", "Y", "X", "C"), shape=(3, 6, 6, 2))
    data = array_for(storage)
    original = data.copy()
    model = factory()
    batched, source = service_batch(data, storage)
    result, trace = adapter(batched, source, model)
    np.testing.assert_array_equal(result, data.transpose(0, 3, 1, 2)[None])
    assert result[0, 2, 1, 4, 5] == data[2, 4, 5, 1]
    assert rules(trace) == ["validate", "transpose", "validate"]
    restored, trace = adapter(result, model, storage)
    np.testing.assert_array_equal(restored, original)
    assert rules(trace) == ["validate", "squeeze", "transpose", "validate"]
    result[:] = -99
    np.testing.assert_array_equal(data, original)


def test_od_destination_units_and_batch_axis():
    storage = od_schema(axes=("L", "Y", "X", "D"), shape=(3, 6, 6, 36))
    data = array_for(storage)
    batched, source = service_batch(data, storage)
    result, trace = adapter(batched, source, od_schema())
    np.testing.assert_array_equal(result, data[None])
    assert result[0, 1, 2, 3, 35] == data[1, 2, 3, 35]
    assert rules(trace) == ["validate", "validate"]
    np.testing.assert_array_equal(adapter(result, od_schema(), storage)[0], data)


def test_true_batch_transpose_and_dynamic_batch():
    source = state_schema(shape=(4, 3, 2, 6, 6))
    target = state_schema(axes=("B", "L", "Y", "X", "C"), shape=(None, 3, 6, 6, 2))
    result, trace = adapter(array_for(source), source, target)
    np.testing.assert_array_equal(result, array_for(source).transpose(0, 1, 3, 4, 2))
    assert rules(trace) == ["validate", "transpose", "validate"]


@pytest.mark.parametrize("axis", ["B", "H"])
def test_only_declared_batch_and_horizon_singletons_can_be_squeezed(axis):
    base = state_schema(axes=("L", "C", "Y", "X"), shape=(3, 2, 6, 6))
    expanded = replace(base, axes=(axis,) + base.axes, shape=(1,) + base.shape)
    result, trace = adapter(array_for(expanded), expanded, base)
    np.testing.assert_array_equal(result, array_for(expanded)[0])
    assert rules(trace)[1] == "squeeze"
    with pytest.raises(ContractError, match="axes"):
        adapter(result, base, expanded)
    undeclared = replace(expanded, shape=(None,) + base.shape)
    with pytest.raises(ContractError, match="axes"):
        adapter(array_for(expanded), undeclared, base)
    with pytest.raises(ContractError, match="axes"):
        adapter(array_for(base), base, undeclared)


@pytest.mark.parametrize("axis", ["B", "H"])
def test_non_singleton_batch_or_horizon_cannot_be_squeezed(axis):
    base = state_schema(axes=("L", "C", "Y", "X"), shape=(3, 2, 6, 6))
    source = replace(base, axes=(axis,) + base.axes, shape=(2,) + base.shape)
    with pytest.raises(ContractError, match="axes"):
        adapter(array_for(source), source, base)


def test_history_singleton_cannot_be_squeezed():
    source = state_schema(shape=(1, 1, 2, 6, 6))
    target = state_schema(axes=("B", "C", "Y", "X"), shape=(1, 2, 6, 6))
    with pytest.raises(ContractError, match="axes"):
        adapter(array_for(source), source, target)


@pytest.mark.parametrize("shape", [(1, 2, 3, 6, 6), (1, 3, 2, 3, 12)])
def test_equal_element_count_is_never_a_reshape_rule(shape):
    source = state_schema()
    target = replace(source, shape=shape, units=("active trips", "mph", "count") if shape[2] == 3 else source.units)
    assert np.prod(source.shape) == np.prod(target.shape)
    with pytest.raises(ContractError):
        adapter(array_for(source), source, target)


def test_field_rename_requires_rule_and_preserves_semantic():
    source = state_schema(field="stored_state")
    target = state_schema()
    with pytest.raises(ContractError, match="rename"):
        adapter(array_for(source), source, target)
    result, trace = adapter(array_for(source), source, target, rename_rule=True)
    np.testing.assert_array_equal(result, array_for(source))
    assert trace[1] == {"rule": "rename", "from": "stored_state", "to": "state"}
    assert "rename" in rules(trace)
    with pytest.raises(ContractError, match="semantic"):
        adapter(array_for(source), source, flow_schema(), rename_rule=True)


@pytest.mark.parametrize("unit,factor", [("kmh", 1.609344), ("km/h", 1.609344), ("m/s", 0.44704)])
def test_physical_speed_conversion_keeps_count_channel(unit, factor):
    source = state_schema()
    target = replace(source, units=("active trips", unit))
    data = array_for(source).astype(np.int64)
    result, trace = adapter(data, source, target)
    np.testing.assert_array_equal(result[:, :, 0], data[:, :, 0])
    np.testing.assert_allclose(result[:, :, 1], data[:, :, 1] * factor)
    assert result.dtype == np.float64
    assert trace[1]["factors"] == pytest.approx([1.0, factor])
    assert "unit_scale" in rules(trace)
    restored, _ = adapter(result, target, source)
    np.testing.assert_allclose(restored, data)


@pytest.mark.parametrize("storage", [False, True])
def test_standardize_with_training_stats_and_reverse(storage):
    raw = state_schema(axes=("L", "Y", "X", "C"), shape=(3, 6, 6, 2)) if storage else state_schema()
    normalized = replace(raw, normalization="standardized")
    stats = training_stats(raw)
    data = array_for(raw)
    result, trace = adapter(data, raw, normalized, stats=stats)
    channel = raw.axes.index("C")
    np.testing.assert_allclose(np.take(result, 0, axis=channel), (np.take(data, 0, axis=channel) - 10) / 2)
    np.testing.assert_allclose(np.take(result, 1, axis=channel), (np.take(data, 1, axis=channel) - 30) / 5)
    assert rules(trace) == ["validate", "standardize", "validate"]
    assert trace[1]["stats"]["source"] == stats.source
    restored, trace = adapter(result, normalized, raw, stats=stats)
    np.testing.assert_allclose(restored, data)
    assert rules(trace) == ["validate", "denormalize", "validate"]


def test_stats_follow_channel_after_layout_conversion():
    storage = state_schema(axes=("L", "Y", "X", "C"), shape=(3, 6, 6, 2))
    target = state_schema(normalization="standardized")
    raw = array_for(storage)
    data, source = service_batch(raw, storage)
    result, trace = adapter(data, source, target, stats=training_stats(target))
    expected = raw.transpose(0, 3, 1, 2)[None].copy()
    expected[:, :, 0] = (expected[:, :, 0] - 10) / 2
    expected[:, :, 1] = (expected[:, :, 1] - 30) / 5
    np.testing.assert_allclose(result, expected)
    assert rules(trace) == ["validate", "transpose", "standardize", "validate"]


def test_denormalize_convert_and_standardize_in_physical_order():
    source = state_schema(normalization="standardized")
    target = replace(source, units=("active trips", "kmh"))
    source_stats = training_stats(source)
    target_stats = training_stats(target, mean=(20.0, 60.0), std=(4.0, 10.0))
    data = array_for(source)
    result, trace = adapter(data, source, target, stats={"source": source_stats, "target": target_stats})
    np.testing.assert_allclose(result[:, :, 0], (data[:, :, 0] * 2 + 10 - 20) / 4)
    np.testing.assert_allclose(result[:, :, 1], ((data[:, :, 1] * 5 + 30) * 1.609344 - 60) / 10)
    assert rules(trace) == ["validate", "denormalize", "unit_scale", "standardize", "validate"]
    restored, _ = adapter(result, target, source, stats={"source": target_stats, "target": source_stats})
    np.testing.assert_allclose(restored, data, atol=1e-12)


def test_od_training_stats_broadcast_per_destination():
    source = od_schema()
    target = replace(source, normalization="standardized")
    means = tuple(float(i) for i in range(36))
    stds = tuple(float(i + 1) for i in range(36))
    result, trace = adapter(array_for(source), source, target, stats=training_stats(source, mean=means, std=stds))
    np.testing.assert_allclose(result, (array_for(source) - np.asarray(means)) / np.asarray(stds))
    assert "standardize" in rules(trace)


@pytest.mark.parametrize("source_norm,target_norm", [("raw", "standardized"), ("standardized", "raw"), ("standardized", "standardized")])
def test_standardized_data_requires_explicit_training_stats(source_norm, target_norm):
    source = state_schema(normalization=source_norm)
    target = replace(source, normalization=target_norm)
    with pytest.raises(ContractError, match="stats"):
        adapter(array_for(source), source, target)


@pytest.mark.parametrize("change", [dict(field="flow"), dict(semantic="road_link_speed"), dict(version="2.0"), dict(grid_id="different-grid"), dict(frame_minutes=60), dict(units=("active trips", "kmh"))])
def test_stats_metadata_cannot_be_substituted(change):
    source = state_schema()
    with pytest.raises(ContractError, match="stats"):
        adapter(array_for(source), source, replace(source, normalization="standardized"), stats=training_stats(source, **change))


@pytest.mark.parametrize("change", [dict(std=(0.0, 1.0)), dict(std=(-1.0, 1.0)), dict(mean=(np.nan, 1.0)), dict(std=(1.0, np.inf)), dict(mean=(1.0,)), dict(split="test"), dict(split="val"), dict(source=""), dict(mean=[1.0, 2.0]), dict(std=(True, 1.0))])
def test_invalid_or_non_training_stats_rejected(change):
    with pytest.raises(ContractError):
        training_stats(state_schema(), **change)


@pytest.mark.parametrize("change,code", [(dict(semantic="road_link_speed"), "semantic"), (dict(version="2.0"), "version"), (dict(grid_id="other-grid"), "grid"), (dict(frame_minutes=60), "interval"), (dict(units=("mph", "mph")), "units")])
def test_metadata_and_incompatible_unit_conflicts(change, code):
    source = state_schema()
    with pytest.raises(ContractError) as caught:
        adapter(array_for(source), source, replace(source, **change), rename_rule=True)
    assert caught.value.code == code


@pytest.mark.parametrize("change", [dict(axes=("B", "L", "C", "Y", "Y")), dict(axes=("B", "L", "C", "height", "width")), dict(axes=["B", "L", "C", "Y", "X"]), dict(shape=(1, 3, 2, 6)), dict(shape=(1, 3, 2, 0, 6)), dict(shape=(True, 3, 2, 6, 6)), dict(units=()), dict(units=("mph",)), dict(units=("active trips", "unknown")), dict(semantic=""), dict(version=""), dict(grid_id=""), dict(normalization="minmax"), dict(frame_minutes=0)])
def test_bad_schema_declarations(change):
    with pytest.raises(ContractError):
        state_schema(**change)


@pytest.mark.parametrize("value", [np.nan, np.inf, -np.inf])
def test_nonfinite_tensor_rejected(value):
    schema = state_schema()
    data = array_for(schema)
    data.flat[0] = value
    with pytest.raises(ContractError, match="nonfinite"):
        validate(data, schema)
    with pytest.raises(ContractError, match="nonfinite"):
        adapter(data, schema, schema)


@pytest.mark.parametrize("dtype", [bool, complex, object, str])
def test_non_real_numeric_arrays_rejected(dtype):
    schema = state_schema()
    with pytest.raises(ContractError, match="data"):
        validate(array_for(schema).astype(dtype), schema)


def test_rank_and_dynamic_channel_unit_count_rejected():
    schema = state_schema()
    with pytest.raises(ContractError, match="shape"):
        validate(array_for(schema)[0], schema)
    with pytest.raises(ContractError, match="units"):
        validate(np.zeros((1, 3, 3, 6, 6)), replace(schema, shape=(1, 3, None, 6, 6)))
    with pytest.raises(ContractError, match="data"):
        validate([1, 2], schema)


def test_arithmetic_overflow_rejected():
    source = state_schema()
    data = np.full(source.shape, np.finfo(np.float64).max)
    with pytest.raises(ContractError, match="nonfinite"):
        adapter(data, source, replace(source, units=("active trips", "kmh")))


def test_payload_binds_all_modalities_and_serializes():
    payload = model_payload()
    payload.validate()
    serialized = payload.to_dict(include_data=True)
    json.dumps(serialized, allow_nan=False)
    assert serialized["tensors"]["od"]["source"] == "processed/od.npy"
    assert len(serialized["tensors"]["od"]["schema"]["units"]) == 36
    with pytest.raises(TypeError):
        payload.tensors["state"] = bound(state_schema())
    with pytest.raises(ContractError, match="field"):
        Payload({"flow": bound(state_schema())})
    with pytest.raises(ContractError, match="payload"):
        Payload({"state": array_for(state_schema())})
    with pytest.raises(ContractError):
        TensorPayload(array_for(state_schema()), state_schema(), "")


@pytest.mark.parametrize("change,code", [(dict(version="2.0"), "version"), (dict(grid_id="other-grid"), "grid"), (dict(frame_minutes=60), "interval"), (dict(shape=(2, 3, 2, 6, 6)), "shape"), (dict(shape=(1, 4, 2, 6, 6)), "shape")])
def test_payload_cross_modality_conflicts(change, code):
    with pytest.raises(ContractError) as caught:
        Payload({"state": bound(state_schema(**change)), "flow": bound(flow_schema())})
    assert caught.value.code == code


def test_payload_validation_rechecks_mutable_ndarray():
    payload = model_payload()
    payload.tensors["state"].data.flat[0] = np.nan
    with pytest.raises(ContractError, match="nonfinite"):
        payload.validate()


def registry_for_model():
    registry = CapabilityRegistry()
    registry.register(CapabilityContract("traffic_forecast", "1.0", {s.field: s for s in (flow_schema(), state_schema(), od_schema())}))
    return registry


def test_registry_explicit_versions_no_overwrite_or_latest():
    registry = registry_for_model()
    registry.validate("traffic_forecast", "1.0", model_payload())
    original = registry.get("traffic_forecast", "1.0")
    newer = {s.field: replace(s, version="2.0") for s in original.inputs.values()}
    registry.register(CapabilityContract("traffic_forecast", "2.0", newer))
    assert registry.get("traffic_forecast", "1.0") is original
    assert registry.get("traffic_forecast", "2.0").version == "2.0"
    with pytest.raises(ContractError, match="version"):
        registry.get("traffic_forecast", "latest")
    with pytest.raises(ContractError, match="capability"):
        registry.register(original)
    with pytest.raises(ContractError, match="version"):
        registry.validate("traffic_forecast", "2.0", model_payload())
    json.dumps(registry.to_dict(), allow_nan=False)


def test_registry_requires_every_declared_modality():
    registry = registry_for_model()
    payload = model_payload()
    for omitted in payload.tensors:
        with pytest.raises(ContractError, match="modality"):
            registry.validate("traffic_forecast", "1.0", Payload({k: v for k, v in payload.tensors.items() if k != omitted}))
    extra = bound(state_schema(field="extra"))
    with pytest.raises(ContractError, match="modality"):
        registry.validate("traffic_forecast", "1.0", Payload(dict(payload.tensors, extra=extra)))


@pytest.mark.parametrize("change,code", [(dict(axes=("B", "L", "Y", "X", "C"), shape=(1, 3, 6, 6, 2)), "axes"), (dict(units=("active trips", "kmh")), "units"), (dict(normalization="standardized"), "normalization")])
def test_registry_validation_never_implicitly_adapts(change, code):
    payload = model_payload()
    changed = bound(state_schema(**change))
    with pytest.raises(ContractError) as caught:
        registry_for_model().validate("traffic_forecast", "1.0", Payload(dict(payload.tensors, state=changed)))
    assert caught.value.code == code


def test_registry_adapts_complete_real_storage_payload():
    sources = [factory(axes=("L", "Y", "X", "C"), shape=(3, 6, 6, 2)) for factory in (flow_schema, state_schema)]
    sources.append(od_schema(axes=("L", "Y", "X", "D"), shape=(3, 6, 6, 36)))
    tensors = {}
    for storage in sources:
        data, schema = service_batch(array_for(storage), storage)
        tensors[schema.field] = bound(schema, data)
    payload = Payload(tensors)
    registry = registry_for_model()
    result, traces = registry.adapt("traffic_forecast", "1.0", payload)
    registry.validate("traffic_forecast", "1.0", result)
    assert result.tensors["state"].data.shape == (1, 3, 2, 6, 6)
    assert result.tensors["flow"].data.shape == (1, 3, 2, 6, 6)
    assert result.tensors["od"].data.shape == (1, 3, 6, 6, 36)
    for name in result.tensors:
        assert result.tensors[name].source == payload.tensors[name].source
        assert "expand_dims" not in rules(traces[name])
    json.dumps(traces, allow_nan=False)


@pytest.mark.parametrize("history", [3, 6, 12])
def test_real_storage_raw_to_model_normalized_and_back(history):
    configured = os.getenv("TRAFFIC_EXPERIMENT_ROOT")
    candidates = ([Path(configured)] if configured else []) + [
        parent / relative
        for parent in Path(__file__).resolve().parents
        for relative in ("data/traffic_artifacts", "实验/研究点一_TBA")
    ]
    experiment = next((path for path in candidates if (path / "data/processed").is_dir()), candidates[-1])
    root = experiment / "data/processed"
    raw_file = root / "nyc_tlc_green_2024_01_6x6.npz"
    stats_file = root / "normalization_train_only.npz"
    if not raw_file.is_file() or not stats_file.is_file():
        pytest.skip("real workspace traffic artifacts are unavailable")
    tensors, targets, statistics, expected_raw = {}, {}, {}, {}
    with np.load(raw_file, allow_pickle=False) as raw, np.load(stats_file, allow_pickle=False) as saved:
        for name, factory in (("flow", flow_schema), ("state", state_schema), ("od", od_schema)):
            storage_axes = ("L", "Y", "X", "D" if name == "od" else "C")
            storage_shape = (history, 6, 6, 36 if name == "od" else 2)
            storage = factory(axes=storage_axes, shape=storage_shape)
            # Read held-out raw frames; normalization uses the saved TRAIN stats.
            data = raw[name][1248:1248 + history].copy()
            batched, source = service_batch(data, storage)
            tensors[name] = TensorPayload(batched, source, str(raw_file) + "#" + name)
            targets[name] = factory(shape=(1, history, 6, 6, 36) if name == "od" else (1, history, 2, 6, 6), normalization="standardized")
            mean = saved[name + "_mean"]
            std = saved[name + "_std"]
            # These arrays explicitly declare singleton spatial axes and a
            # final channel axis; indexing extracts the training vector.
            statistics[name] = training_stats(source, mean=tuple(float(v) for v in mean[0, 0, 0]), std=tuple(float(v) for v in std[0, 0, 0]), source=str(stats_file) + "#" + name)
            expected = (data.astype(np.float64) - mean.astype(np.float64)) / std.astype(np.float64)
            expected_raw[name] = data
            if name != "od":
                expected = expected.transpose(0, 3, 1, 2)
            expected_raw[name + "_normalized"] = expected[None]

    registry = CapabilityRegistry()
    registry.register(CapabilityContract("traffic_forecast", "1.0", targets))
    original = Payload(tensors)
    result, traces = registry.adapt("traffic_forecast", "1.0", original, stats=statistics)
    registry.validate("traffic_forecast", "1.0", result)
    json.dumps(traces, allow_nan=False)
    for name, tensor in result.tensors.items():
        np.testing.assert_allclose(tensor.data, expected_raw[name + "_normalized"], rtol=1e-12, atol=1e-12)
        assert "standardize" in rules(traces[name])
        assert "expand_dims" not in rules(traces[name])
        batched_raw, trace = adapter(tensor.data, tensor.schema, original.tensors[name].schema, stats=statistics[name])
        np.testing.assert_allclose(batched_raw[0], expected_raw[name], rtol=1e-12, atol=1e-12)
        assert "denormalize" in rules(trace)
        np.testing.assert_array_equal(original.tensors[name].data[0], expected_raw[name])


@pytest.mark.parametrize("factory", [state_schema, flow_schema, od_schema])
def test_unbatched_storage_requires_explicit_service_construction(factory):
    model = factory()
    storage = replace(model, axes=model.axes[1:], shape=model.shape[1:])
    with pytest.raises(ContractError, match="service must construct axes"):
        adapter(array_for(storage), storage, model)


def test_payload_explicit_aliases_stats_and_no_source_reuse():
    source = state_schema(field="stored_state")
    payload = Payload({source.field: bound(source)})
    target = state_schema(normalization="standardized")
    result, traces = adapt_payload(payload, {"state": target}, rename_rules={"state": "stored_state"}, stats={"state": training_stats(target)})
    assert result.tensors["state"].schema == target
    assert rules(traces["state"]) == ["validate", "rename", "standardize", "validate"]
    with pytest.raises(ContractError, match="modality"):
        adapt_payload(payload, {"state": state_schema()})
    with pytest.raises(ContractError, match="modality"):
        adapt_payload(payload, {"state": state_schema(), "flow": flow_schema()}, rename_rules={"state": "stored_state", "flow": "stored_state"})


def test_contract_output_and_input_versions_grid_and_interval():
    input_schema = state_schema()
    output_schema = state_schema(field="forecast", axes=("B", "H", "C", "Y", "X"), shape=(1, 1, 2, 6, 6))
    registry = CapabilityRegistry()
    registry.register(CapabilityContract("forecast", "1.0", {"state": input_schema}, {"forecast": output_schema}))
    registry.validate("forecast", "1.0", Payload({"forecast": bound(output_schema)}), direction="outputs")
    for change in (dict(version="2.0"), dict(grid_id="other-grid"), dict(frame_minutes=60)):
        with pytest.raises(ContractError):
            CapabilityContract("forecast", "1.0", {"state": input_schema}, {"forecast": replace(output_schema, **change)})
    with pytest.raises(ContractError, match="capability"):
        registry.validate("forecast", "1.0", model_payload(), direction="other")


def test_contract_mapping_is_snapshot():
    inputs = {"state": state_schema()}
    contract = CapabilityContract("state_consumer", "1.0", inputs)
    inputs.clear()
    assert "state" in contract.inputs
    with pytest.raises(TypeError):
        contract.inputs["flow"] = flow_schema()
