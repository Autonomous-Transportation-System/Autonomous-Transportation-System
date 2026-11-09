"""Read-only CPU inference for the trained B1 traffic model.

``prepare_history`` returns raw storage arrays (B,L,Y,X,C). Pass these through
the platform's contract adapter before ``predict``; this service deliberately
does not standardize inputs. Model inputs use (B,L,C,Y,X) for state/flow and
(B,L,Y,X,C) for OD. Every returned array uses (B,H,Y,X,C), including H=1.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import importlib.abc
import importlib.util
import io
import json
import os
import sys
import threading
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import numpy as np
import torch


TASKS = ("state", "flow", "od")
_IMPORT_LOCK = threading.Lock()


class _ModelLoader(importlib.abc.Loader):
    """Import existing experiment code without writing bytecode to its tree."""

    def __init__(self, path: Path, package: str) -> None:
        self.path = path
        self.package = package

    def create_module(self, spec: Any) -> None:
        return None

    def exec_module(self, module: Any) -> None:
        tree = ast.parse(self.path.read_text(), filename=str(self.path))
        # The experiment uses absolute `model.*` imports. Give each root its
        # own namespace, so unrelated platform packages named model are safe.
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                if node.module == "model" or node.module.startswith("model."):
                    node.module = self.package + node.module[len("model") :]
        exec(compile(tree, str(self.path), "exec"), module.__dict__)


class _ModelFinder(importlib.abc.MetaPathFinder):
    def __init__(self, root: Path, package: str) -> None:
        self.root = root / "model"
        self.package = package

    def find_spec(self, fullname: str, path: Any, target: Any = None) -> Any:
        if fullname == self.package:
            source = self.root / "__init__.py"
            locations = [str(self.root)]
        elif fullname.startswith(self.package + "."):
            source = self.root.joinpath(*fullname.split(".")[1:]).with_suffix(".py")
            locations = None
        else:
            return None
        if not source.is_file():
            return None
        return importlib.util.spec_from_file_location(
            fullname,
            source,
            loader=_ModelLoader(source, self.package),
            submodule_search_locations=locations,
        )


def _load_traffic_net(root: Path) -> Any:
    package = "_traffic_b1_" + hashlib.sha256(str(root).encode()).hexdigest()[:16]
    with _IMPORT_LOCK:
        finder = _ModelFinder(root, package)
        sys.meta_path.insert(0, finder)
        try:
            return importlib.import_module(package + ".traffic_net").TrafficNet
        finally:
            sys.meta_path.remove(finder)


class TrafficModelService:
    """Load B1_seed42_L3 once, then serialize shared three-head forwards.

    Args:
        experiment_root: Directory containing ``model/``, ``data/processed/``
            and ``results/B1_seed42_L3/``. If omitted, use the explicit
            ``TRAFFIC_EXPERIMENT_ROOT`` environment variable, or find
            ``实验/研究点一_TBA`` under an ancestor of this source file.

    Raises:
        FileNotFoundError: A required real artifact is missing.
        ValueError: Artifact metadata, statistics, or request inputs are invalid.
        RuntimeError: Strict checkpoint loading or model inference fails.

    No training or experiment preparation functions are imported or invoked.
    PyTorch must be installed in the interpreter running the platform. Inference
    runs on CPU with one intra-op thread; torch.set_num_threads is process-wide.
    """

    def __init__(self, experiment_root: str | Path | None = None) -> None:
        if experiment_root is None:
            experiment_root = os.environ.get("TRAFFIC_EXPERIMENT_ROOT") or None
        if experiment_root is None:
            experiment_root = next(
                (
                    parent / "实验" / "研究点一_TBA"
                    for parent in Path(__file__).resolve().parents
                    if (parent / "实验" / "研究点一_TBA").is_dir()
                ),
                None,
            )
            if experiment_root is None:
                raise FileNotFoundError("Supply experiment_root for the B1 artifacts")
        self.experiment_root = Path(experiment_root).expanduser().resolve()
        checkpoint_path = (
            self.experiment_root / "results" / "B1_seed42_L3" / "best.pt"
        )
        self.checkpoint_path = checkpoint_path
        checkpoint_bytes = checkpoint_path.read_bytes()
        self._checkpoint_sha256 = hashlib.sha256(checkpoint_bytes).hexdigest()
        checkpoint = torch.load(
            io.BytesIO(checkpoint_bytes), map_location="cpu", weights_only=True
        )
        if (
            not isinstance(checkpoint, dict)
            or not isinstance(checkpoint.get("config"), dict)
            or not isinstance(checkpoint.get("model"), Mapping)
        ):
            raise ValueError("B1 checkpoint must contain its saved config and model")
        config = checkpoint["config"]
        if checkpoint.get("name") != "B1" or checkpoint.get("seed") != 42:
            raise ValueError("Expected a B1 seed42 checkpoint")
        for key in ("m", "n", "seq_len", "pred_len"):
            value = config.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"Invalid saved config dimension: {key}")
        if config["seq_len"] != 3:
            raise ValueError("Expected the saved B1 L3 configuration")
        self._height, self._width = config["m"], config["n"]
        self._seq_len, self._pred_len = config["seq_len"], config["pred_len"]
        self._channels = {"state": 2, "flow": 2, "od": self._height * self._width}
        self._version = "B1_seed42_L3:" + self._checkpoint_sha256[:12]
        self._model_lock = threading.Lock()

        processed = self.experiment_root / "data" / "processed"
        self.metadata_path = processed / "dataset_metadata.json"
        self.normalization_path = processed / "normalization_train_only.npz"
        self.data_path = processed / "nyc_tlc_green_2024_01_6x6.npz"
        self._metadata = json.loads((processed / "dataset_metadata.json").read_text())
        if self._metadata["grid"] != [self._height, self._width]:
            raise ValueError("Dataset grid does not match the saved config")
        step = self._metadata["step_minutes"]
        if isinstance(step, bool) or not isinstance(step, int) or step < 1:
            raise ValueError("Invalid dataset step_minutes")
        if 1440 % step:
            raise ValueError("Dataset step_minutes must divide one day")
        self._frames_per_day = 1440 // step
        self._stats: dict[str, tuple[np.ndarray, np.ndarray]] = {}
        with np.load(
            processed / "normalization_train_only.npz", allow_pickle=False
        ) as f:
            for task in TASKS:
                mean = np.array(f[task + "_mean"], dtype=np.float32, copy=True)
                std = np.array(f[task + "_std"], dtype=np.float32, copy=True)
                expected = (1, 1, 1, self._channels[task])
                if mean.shape != expected or std.shape != expected:
                    raise ValueError(f"Invalid {task} normalization shape: {expected}")
                if not np.isfinite(mean).all() or not np.isfinite(std).all():
                    raise ValueError(f"{task} normalization must be finite")
                if not (std > 0).all():
                    raise ValueError(f"{task} normalization std must be positive")
                self._stats[task] = (mean, std)
        self._data: dict[str, np.ndarray] = {}
        with np.load(
            processed / "nyc_tlc_green_2024_01_6x6.npz", allow_pickle=False
        ) as f:
            for task in TASKS:
                values = np.array(f[task], dtype=np.float32, copy=True)
                expected = (self._height, self._width, self._channels[task])
                if values.ndim != 4 or values.shape[1:] != expected:
                    raise ValueError(f"Invalid {task} storage shape: (T,{expected})")
                if not np.isfinite(values).all():
                    raise ValueError(f"{task} storage must be finite")
                values.setflags(write=False)
                self._data[task] = values
        self._frame_count = len(self._data["state"])
        if not self._frame_count or any(
            len(values) != self._frame_count for values in self._data.values()
        ):
            raise ValueError(
                "Stored modalities must have the same nonempty frame count"
            )

        torch.set_num_threads(1)
        self._model = _load_traffic_net(self.experiment_root)(**config, use_tba=True)
        self._model.load_state_dict(checkpoint["model"], strict=True)
        self._model.cpu().eval()

    @property
    def version(self) -> str:
        return self._version

    @property
    def checkpoint_sha256(self) -> str:
        return self._checkpoint_sha256

    def _validate_frames(self, target_frames: Sequence[int]) -> np.ndarray:
        if isinstance(target_frames, (str, bytes)):
            raise ValueError("target_frames must be a nonempty sequence of integers")
        try:
            values = tuple(target_frames)
        except TypeError as exc:
            raise ValueError("target_frames must be a sequence of integers") from exc
        if not values or any(
            isinstance(value, (bool, np.bool_))
            or not isinstance(value, (int, np.integer))
            for value in values
        ):
            raise ValueError("target_frames must be a nonempty sequence of integers")
        if any(
            value < self._seq_len or value + self._pred_len > self._frame_count
            for value in values
        ):
            raise ValueError("target_frames are outside the available history/horizon")
        frames = np.asarray(values, dtype=np.int64)
        if np.any(
            (frames - self._seq_len) // self._frames_per_day
            != (frames + self._pred_len - 1) // self._frames_per_day
        ):
            raise ValueError(
                "History and prediction target must not cross a day boundary"
            )
        return frames

    def prepare_history(self, target_frames: Sequence[int]) -> dict[str, np.ndarray]:
        """Return copied, raw history [t-L,t) for each absolute target frame.

        State/flow shape is (B,3,6,6,2); OD shape is (B,3,6,6,36).
        The full history and prediction horizon must be within one calendar day.
        No target observations are included and no normalization is performed.
        """
        frames = self._validate_frames(target_frames)
        history_frames = frames[:, None] + np.arange(-self._seq_len, 0)
        return {task: self._data[task][history_frames].copy() for task in TASKS}

    def predict(
        self, inputs: Mapping[str, Any], task: str = "all"
    ) -> dict[str, Any]:
        """Run one three-head forward on already standardized model inputs.

        All three modalities are required even for a single output task.
        State/flow: (B,L,2,Y,X); OD: (B,L,Y,X,Y*X). NumPy arrays, tensors
        and numeric nested sequences are accepted; finite float32 CPU snapshots
        prevent caller mutation from changing an in-progress forward.
        Optional inputs['target_frames'] enables absolute same-day validation;
        tensor values alone carry no timestamps and cannot establish a date.

        Return ``{'task', 'version', 'checkpoint_sha256', 'predictions'}``.
        ``predictions`` maps selected tasks to ``prediction`` (raw units,
        inverse-standardized then clipped at zero), ``normalized``, ``shape``,
        ``layout``, ``units`` and ``channels``. Arrays remain NumPy float32 and
        use (B,H,Y,X,C); callers serialize them at the HTTP boundary.
        """
        if not isinstance(task, str) or task not in (*TASKS, "all"):
            raise ValueError("task must be state, flow, od, or all")
        if not isinstance(inputs, Mapping):
            raise ValueError("inputs must map state, flow and od to model arrays")
        missing = set(TASKS) - inputs.keys()
        if missing:
            raise ValueError("Missing input modalities: " + ", ".join(sorted(missing)))
        extra = inputs.keys() - {*TASKS, "target_frames"}
        if extra:
            raise ValueError("Unknown input keys: " + ", ".join(map(str, extra)))
        tensors = {}
        batch_size = None
        for name in TASKS:
            value = inputs[name]
            try:
                if isinstance(value, torch.Tensor):
                    if value.is_complex() or value.dtype == torch.bool:
                        raise ValueError("Expected real numeric values")
                    tensor = (
                        value.detach().to(device="cpu", dtype=torch.float32).clone()
                    )
                else:
                    array = np.asarray(value)
                    if array.dtype.kind not in "fiu":
                        raise ValueError("Expected real numeric values")
                    with np.errstate(over="ignore", invalid="ignore"):
                        tensor = torch.from_numpy(
                            np.array(array, dtype=np.float32, order="C", copy=True)
                        )
            except (TypeError, ValueError, RuntimeError) as exc:
                raise ValueError(f"Invalid {name} input: {exc}") from exc
            tail = (
                (self._seq_len, self._height, self._width, self._channels[name])
                if name == "od"
                else (self._seq_len, 2, self._height, self._width)
            )
            if tensor.ndim != 5 or tuple(tensor.shape[1:]) != tail:
                raise ValueError(f"{name} input shape must be (B,{tail})")
            if tensor.shape[0] == 0:
                raise ValueError("Input batch must be nonempty")
            if batch_size is not None and tensor.shape[0] != batch_size:
                raise ValueError("Input modalities must have the same batch size")
            if not torch.isfinite(tensor).all().item():
                raise ValueError(
                    f"{name} input must be finite after float32 conversion"
                )
            batch_size = tensor.shape[0]
            tensors[name] = tensor.contiguous()
        if "target_frames" in inputs:
            frames = self._validate_frames(inputs["target_frames"])
            if len(frames) != batch_size:
                raise ValueError(
                    "target_frames must have the same batch size as inputs"
                )

        selected = TASKS if task == "all" else (task,)
        predictions = {}
        with self._model_lock, torch.no_grad():
            outputs = self._model(tensors["flow"], tensors["od"], tensors["state"])
            if len(outputs) != len(TASKS):
                raise RuntimeError("B1 forward must return state, flow and od")
            for name, output in zip(TASKS, outputs):
                if name != "od":
                    output = output.permute(0, 1, 3, 4, 2)
                normalized = output.cpu().numpy().copy(order="C")
                expected = (
                    batch_size,
                    self._pred_len,
                    self._height,
                    self._width,
                    self._channels[name],
                )
                if normalized.shape != expected or not np.isfinite(normalized).all():
                    raise RuntimeError(f"Invalid {name} model output shape or values")
                if name not in selected:
                    continue
                mean, std = self._stats[name]
                with np.errstate(over="ignore", invalid="ignore"):
                    raw = np.maximum(normalized * std + mean, 0)
                if not np.isfinite(raw).all():
                    raise RuntimeError(
                        f"{name} inverse normalization produced nonfinite values"
                    )
                if name == "od":
                    units = [self._metadata["od_units"]] * self._channels[name]
                    channels = [f"destination_{i}" for i in range(self._channels[name])]
                else:
                    units = list(self._metadata[name + "_units"])
                    channels = list(self._metadata[name + "_channels"])
                predictions[name] = {
                    "prediction": raw,
                    "normalized": normalized,
                    "shape": list(raw.shape),
                    "layout": "B,H,Y,X,C",
                    "units": units,
                    "channels": channels,
                }
        return {
            "task": task,
            "version": self.version,
            "checkpoint_sha256": self.checkpoint_sha256,
            "predictions": predictions,
        }
