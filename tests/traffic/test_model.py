"""Real B1 artifacts through the production storage -> contract -> model chain.

Run with PYTHONPATH=. and a PyTorch-enabled interpreter. The experiment tree
is read-only: metrics is compiled in isolation; run_experiments is not imported.
"""

from __future__ import annotations

import ast
import hashlib
import json
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import pytest
import torch

from src.service.traffic_contracts import (
    CapabilityContract,
    CapabilityRegistry,
    Payload,
    TensorPayload,
    TensorSchema,
    TrainingStats,
)
from src.service.traffic_model import TASKS, TrafficModelService


@pytest.fixture(scope="module")
def experiment_root():
    return next(
        parent / "实验" / "研究点一_TBA"
        for parent in Path(__file__).resolve().parents
        if (parent / "实验" / "研究点一_TBA").is_dir()
    )


@pytest.fixture(scope="module", autouse=True)
def unchanged_experiment_tree(experiment_root):
    def snapshot():
        return {
            str(path.relative_to(experiment_root)): (
                path.stat().st_size,
                path.stat().st_mtime_ns,
            )
            for path in experiment_root.rglob("*")
            if path.is_file()
        }

    before = snapshot()
    yield
    assert snapshot() == before, "Tests must not write any research-point-one files"


@pytest.fixture(scope="module")
def service(experiment_root):
    return TrafficModelService(experiment_root=experiment_root)


@pytest.fixture(scope="module")
def reference(experiment_root):
    path = experiment_root / "results" / "B1_seed42_L3" / "test_predictions.npz"
    with np.load(path, allow_pickle=False) as artifact:
        return {key: artifact[key].copy() for key in artifact.files}


def adapted_history(service, frames):
    """Use actual registry/adapter rules; never standardize tensors in tests."""
    root = service.experiment_root
    processed = root / "data" / "processed"
    metadata = json.loads((processed / "dataset_metadata.json").read_text())
    history = service.prepare_history(frames)
    storage_schemas, model_schemas, training_stats = {}, {}, {}
    stats_path = processed / "normalization_train_only.npz"
    with np.load(stats_path, allow_pickle=False) as fitted:
        for name, raw in history.items():
            units = (
                (metadata["od_units"],) * 36
                if name == "od"
                else tuple(metadata[name + "_units"])
            )
            semantic = (
                metadata["od_definition"]
                if name == "od"
                else ";".join(metadata[name + "_channels"])
            )
            common = dict(
                field=name,
                units=units,
                semantic=semantic,
                version=service.version,
                grid_id="nyc-green-6x6-v1",
                frame_minutes=metadata["step_minutes"],
            )
            axes = ("B", "L", "Y", "X", "D" if name == "od" else "C")
            storage_schemas[name] = TensorSchema(
                **common, axes=axes, shape=tuple(raw.shape), normalization="raw"
            )
            model_schemas[name] = TensorSchema(
                **common,
                axes=axes if name == "od" else ("B", "L", "C", "Y", "X"),
                shape=tuple(raw.shape) if name == "od" else (len(raw), 3, 2, 6, 6),
                normalization="standardized",
            )
            training_stats[name] = TrainingStats(
                **common,
                mean=tuple(fitted[name + "_mean"].reshape(-1)),
                std=tuple(fitted[name + "_std"].reshape(-1)),
                source=str(stats_path),
                split="train",
            )
    payload = Payload(
        {
            name: TensorPayload(
                data=raw,
                schema=storage_schemas[name],
                source=str(processed / "nyc_tlc_green_2024_01_6x6.npz"),
            )
            for name, raw in history.items()
        }
    )
    registry = CapabilityRegistry()
    registry.register(
        CapabilityContract("B1", service.version, inputs=model_schemas)
    )
    adapted, traces = registry.adapt(
        "B1", service.version, payload, stats=training_stats
    )
    registry.validate("B1", service.version, adapted)
    for name, trace in traces.items():
        rules = [step["rule"] for step in trace]
        expected = (
            ["validate", "standardize", "validate"]
            if name == "od"
            else ["validate", "transpose", "standardize", "validate"]
        )
        assert rules == expected
        standardize = next(step for step in trace if step["rule"] == "standardize")
        assert standardize["stats"]["source"] == str(stats_path)
        assert standardize["stats"]["split"] == "train"
    inputs = {name: tensor.data for name, tensor in adapted.tensors.items()}
    inputs["target_frames"] = np.asarray(frames, dtype=np.int64)
    return inputs, traces


def artifact_metrics(experiment_root):
    """Reuse exactly the experiment's pure metrics function, without imports."""
    path = experiment_root / "run_experiments.py"
    tree = ast.parse(path.read_text(), filename=str(path))
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef) and node.name == "metrics"
    )
    isolated = ast.Module(body=[function], type_ignores=[])
    namespace = {"np": np}
    exec(compile(isolated, str(path), "exec"), namespace)
    return namespace["metrics"]


def test_225_frames_batch8_reload_matches_original_npz(
    experiment_root, reference, capsys
):
    frames = reference["target_frames"]
    assert len(frames) == 225
    expected_metrics = json.loads(
        (experiment_root / "results" / "B1_seed42_L3" / "result.json").read_text()
    )["test_metrics"]
    with np.load(
        experiment_root / "data" / "processed" / "nyc_tlc_green_2024_01_6x6.npz",
        allow_pickle=False,
    ) as data:
        truth = {name: data[name][frames].copy() for name in TASKS}
    for name in TASKS:
        np.testing.assert_array_equal(truth[name], reference["true_" + name])
    previous = None
    for reload_index in range(2):
        model_service = TrafficModelService(experiment_root=experiment_root)
        predictions = {name: [] for name in TASKS}
        normalized = {name: [] for name in TASKS}
        for start in range(0, len(frames), 8):
            inputs, _ = adapted_history(model_service, frames[start : start + 8])
            result = model_service.predict(inputs, task="all")
            assert result["version"] == model_service.version
            assert result["checkpoint_sha256"] == model_service.checkpoint_sha256
            assert set(result["predictions"]) == set(TASKS)
            for name, output in result["predictions"].items():
                assert output["layout"] == "B,H,Y,X,C"
                assert output["shape"] == list(output["prediction"].shape)
                assert output["prediction"].shape[1] == 1
                assert output["prediction"].dtype == np.float32
                assert output["normalized"].dtype == np.float32
                assert (output["prediction"] >= 0).all()
                predictions[name].append(output["prediction"][:, 0])
                normalized[name].append(output["normalized"][:, 0])
        predictions = {
            name: np.concatenate(parts) for name, parts in predictions.items()
        }
        normalized = {name: np.concatenate(parts) for name, parts in normalized.items()}
        max_differences = {}
        for name in TASKS:
            for kind, actual, key, tolerance in (
                ("raw", predictions[name], "pred_" + name, 2e-5),
                ("normalized", normalized[name], "normalized_" + name, 1e-5),
            ):
                difference = float(
                    np.max(np.abs(actual.astype(np.float64) - reference[key]))
                )
                max_differences[name + "_" + kind] = difference
                # Adapter arithmetic is float64; artifact preparation was
                # float32. Record and bound the actual discrepancy, not zero.
                np.testing.assert_allclose(
                    actual, reference[key], rtol=0, atol=tolerance
                )
        if previous is not None:
            for name in TASKS:
                np.testing.assert_array_equal(predictions[name], previous[0][name])
                np.testing.assert_array_equal(normalized[name], previous[1][name])
        previous = predictions, normalized
        metrics = artifact_metrics(experiment_root)(predictions, truth)
        assert set(metrics) == {
            "active_trips",
            "speed_all",
            "speed_observed",
            "inflow",
            "outflow",
            "od_all",
            "od_nonzero",
        }
        for name, values in metrics.items():
            assert values["n"] == expected_metrics[name]["n"]
            for metric in ("mae", "rmse"):
                assert values[metric] == pytest.approx(
                    expected_metrics[name][metric], rel=0, abs=1e-7
                )
        with capsys.disabled():
            print(
                json.dumps(
                    {
                        "reload": reload_index,
                        "max_abs_diff": max_differences,
                        "metrics": metrics,
                    }
                )
            )


def test_history_is_raw_same_day_and_excludes_target(service, experiment_root):
    frames = np.array([1251, 1295, 1299, 1487])
    history = service.prepare_history(frames)
    with np.load(
        experiment_root / "data" / "processed" / "nyc_tlc_green_2024_01_6x6.npz",
        allow_pickle=False,
    ) as data:
        for name in TASKS:
            expected = np.stack([data[name][frame - 3 : frame] for frame in frames])
            np.testing.assert_array_equal(history[name], expected)
            history[name][:] = -999
            np.testing.assert_array_equal(
                service.prepare_history(frames)[name], expected
            )


@pytest.mark.parametrize(
    "frames",
    [
        [], [-1], [0], [2], [48], [49], [50], [1488], [1e30], [1251.0],
        [True], [1251, True], [[1251]], "1251", None,
    ],
)
def test_invalid_history_frames_rejected(service, frames):
    with pytest.raises(ValueError):
        service.prepare_history(frames)


@pytest.mark.parametrize("task", ["state", "flow", "od", "all"])
def test_one_real_forward_per_request_and_output_metadata(service, task):
    inputs, _ = adapted_history(service, [1251, 1252])
    # Tensors and arrays both follow the same already-adapted contract.
    inputs["state"] = torch.from_numpy(inputs["state"]).requires_grad_(True)
    calls = []

    def observe(model, args):
        calls.append((model.training, torch.is_grad_enabled()))

    hook = service._model.register_forward_pre_hook(observe)
    try:
        result = service.predict(inputs, task=task)
    finally:
        hook.remove()
    assert calls == [(False, False)]
    assert torch.get_num_threads() == 1
    assert set(result["predictions"]) == (set(TASKS) if task == "all" else {task})
    for name, output in result["predictions"].items():
        assert len(output["units"]) == output["prediction"].shape[-1]
        assert len(output["channels"]) == len(output["units"])
        if name == "state":
            assert output["units"] == ["active trips", "mph"]
        else:
            assert set(output["units"]) == {"trips/30min"}


def test_invalid_requests_rejected_before_forward(service):
    inputs, _ = adapted_history(service, [1251, 1252])
    cases = []
    for task in ("speed", "ALL", "", None, True, []):
        cases.append((inputs, task))
    for missing in TASKS:
        cases.append(
            ({key: value for key, value in inputs.items() if key != missing}, "all")
        )
    for name in TASKS:
        for value in (np.nan, np.inf, -np.inf, np.finfo(np.float64).max):
            bad = inputs[name].copy()
            bad.flat[0] = value
            cases.append(({**inputs, name: bad}, "all"))
        for bad in (
            inputs[name][0], inputs[name].astype(bool), inputs[name].astype(complex)
        ):
            cases.append(({**inputs, name: bad}, "all"))
    cases.extend(
        [
            (None, "all"),
            ({**inputs, "flow": inputs["flow"][:1]}, "all"),
            ({**inputs, "state": inputs["state"].transpose(0, 1, 3, 4, 2)}, "all"),
            ({**inputs, "od": inputs["od"][..., :35]}, "all"),
            ({**inputs, "extra": 1}, "all"),
            ({**inputs, "target_frames": [1248, 1252]}, "all"),
            ({**inputs, "target_frames": [1251]}, "all"),
            ({**inputs, **{name: inputs[name][:0] for name in TASKS}}, "all"),
        ]
    )
    calls = []
    hook = service._model.register_forward_pre_hook(lambda model, args: calls.append(1))
    try:
        for request, task in cases:
            with pytest.raises(ValueError):
                service.predict(request, task=task)
    finally:
        hook.remove()
    assert not calls


def test_real_concurrent_predictions_are_serialized_and_repeatable(service):
    inputs, _ = adapted_history(service, [1251, 1252])
    expected = service.predict(inputs)
    guard = threading.Lock()
    counters = {"active": 0, "peak": 0, "calls": 0}

    def before(model, args):
        with guard:
            counters["active"] += 1
            counters["calls"] += 1
            counters["peak"] = max(counters["peak"], counters["active"])
        # Keep actual forwards overlapping in wall time if the lock is absent.
        time.sleep(0.005)

    def after(model, args, result):
        with guard:
            counters["active"] -= 1

    pre_hook = service._model.register_forward_pre_hook(before)
    post_hook = service._model.register_forward_hook(after)
    try:
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda _: service.predict(inputs), range(6)))
    finally:
        pre_hook.remove()
        post_hook.remove()
    assert counters == {"active": 0, "peak": 1, "calls": 6}
    for result in results:
        for name in TASKS:
            for key in ("prediction", "normalized"):
                np.testing.assert_array_equal(
                    result["predictions"][name][key], expected["predictions"][name][key]
                )


def copy_artifacts(experiment_root, destination):
    shutil.copytree(
        experiment_root / "model",
        destination / "model",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    processed = destination / "data" / "processed"
    processed.mkdir(parents=True)
    for name in (
        "dataset_metadata.json",
        "normalization_train_only.npz",
        "nyc_tlc_green_2024_01_6x6.npz",
    ):
        shutil.copy2(experiment_root / "data" / "processed" / name, processed / name)
    checkpoint = destination / "results" / "B1_seed42_L3" / "best.pt"
    checkpoint.parent.mkdir(parents=True)
    shutil.copy2(experiment_root / "results" / "B1_seed42_L3" / "best.pt", checkpoint)
    return checkpoint


def test_checkpoint_identity_saved_config_and_strict_load(
    service, experiment_root, tmp_path
):
    checkpoint = experiment_root / "results" / "B1_seed42_L3" / "best.pt"
    digest = hashlib.sha256(checkpoint.read_bytes()).hexdigest()
    assert service.checkpoint_sha256 == digest
    assert service.version == "B1_seed42_L3:" + digest[:12]
    assert service._model.use_tba
    assert service._model.seq_len == 3
    assert service._model.pred_len == 1
    assert (service._model.m, service._model.n, service._model.hidden) == (6, 6, 64)
    assert sum(parameter.numel() for parameter in service._model.parameters()) == 712143
    copied = copy_artifacts(experiment_root, tmp_path)
    saved = torch.load(copied, map_location="cpu", weights_only=True)
    del saved["model"]["adapters.state.up.weight"]
    torch.save(saved, copied)
    with pytest.raises(RuntimeError, match="adapters.state.up.weight"):
        TrafficModelService(experiment_root=tmp_path)


@pytest.mark.parametrize("bad_std", [0.0, -1.0, np.nan, np.inf])
def test_invalid_saved_training_stats_rejected(experiment_root, tmp_path, bad_std):
    copy_artifacts(experiment_root, tmp_path)
    path = tmp_path / "data" / "processed" / "normalization_train_only.npz"
    with np.load(path, allow_pickle=False) as original:
        stats = {key: original[key].copy() for key in original.files}
    stats["state_std"].flat[0] = bad_std
    np.savez(path, **stats)
    with pytest.raises(ValueError, match="normalization"):
        TrafficModelService(experiment_root=tmp_path)
