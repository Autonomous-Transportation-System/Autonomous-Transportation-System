#!/usr/bin/env python3
"""Real B1 experiments. Randomness creates arrivals only, never measurements."""
from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import resource
import statistics
import subprocess
import sys
import threading
import time
import traceback
import uuid

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
POLICIES = ("fcfs", "drc", "edf", "heft", "joint")
TEMPLATES = ("flow", "analysis", "od_hotspots")
LOADS = {"low": 0.25, "medium": 0.9, "high": 1.8}
DEFAULT_ARTIFACT_ROOT = REPO.parent / "实验" / "研究点一_TBA"
DEFAULT_OUTPUT_ROOT = REPO.parent / "实验" / "研究点二_平台" / "联合编排实施_20261109" / "results"


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False,
                                     allow_nan=False).encode()).hexdigest()


def write_json(path, value):
    with Path(path).open("x", encoding="utf-8") as stream:
        json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
        stream.write("\n")


def write_csv(path, rows):
    rows = list(rows)
    keys = list(dict.fromkeys(key for row in rows for key in row))
    with Path(path).open("x", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=keys)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: json.dumps(value, ensure_ascii=False) if isinstance(value, (dict, list, tuple))
                             else value for key, value in row.items()})


def portable(value, artifact_root):
    """Strip workstation paths from reports; provenance is a file SHA manifest."""
    if isinstance(value, dict):
        return {str(key): portable(item, artifact_root) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [portable(item, artifact_root) for item in value]
    if isinstance(value, str):
        value = value.replace(str(Path(artifact_root).resolve()), "artifact_root")
        value = value.replace(str(REPO), "repository")
        # TrainingStats may contain another absolute source supplied by a backend.
        if value.startswith(("/Users/", "/home/", "/private/", "/tmp/")):
            return "local_source_sha256:" + hashlib.sha256(value.encode()).hexdigest()
    return value


def provenance(artifact_root=None):
    files = ["scripts/run_traffic_joint_experiments.py", "scripts/run_traffic_joint_simulation.py",
             "src/__init__.py", "src/service/__init__.py", "src/graph/__init__.py", "scripts/__init__.py",
             "docs/traffic_module_contracts.md", "src/service/traffic_joint_runtime.py",
             "src/graph/traffic_joint_scheduler.py", "src/service/traffic_model.py",
             "src/service/traffic_composer.py", "src/service/traffic_catalog.py",
             "src/service/traffic_contracts.py", "src/service/traffic_types.py", "src/graph/traffic_scheduler.py"]
    answer = {"code": {name: sha(REPO / name) for name in files if (REPO / name).is_file()},
              "python": sys.version, "platform": sys.platform}
    if artifact_root is not None:
        names = ["results/B1_seed42_L3/best.pt", "results/B1_seed42_L3/test_predictions.npz",
                 "data/processed/normalization_train_only.npz", "data/processed/dataset_metadata.json",
                 "data/processed/nyc_tlc_green_2024_01_6x6.npz"]
        answer["artifacts"] = {name: sha(Path(artifact_root) / name) for name in names}
        answer["model_source"] = {str(p.relative_to(artifact_root)): sha(p)
                                  for p in sorted((Path(artifact_root) / "model").glob("*.py"))}
    return answer


def snapshot_code(root):
    """Freeze only traffic execution dependencies; shared worker files remain untouched."""
    files = list(provenance()["code"])
    source = {name: (REPO / name).read_bytes() for name in files}
    expected = {name: hashlib.sha256(content).hexdigest() for name, content in source.items()}
    if any(sha(REPO / name) != expected[name] for name in files):
        raise RuntimeError("Traffic source changed while capturing snapshot; retain this attempt and retry a new run")
    # Seal the scripts package so a namespace lookup cannot select another checkout.
    source.setdefault("scripts/__init__.py", b"")
    destination = root / "runner_source"
    destination.mkdir()
    for name, content in source.items():
        path = destination / name
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("xb") as stream:
            stream.write(content)
    manifest = {"kind": "frozen_traffic_execution_source", "repository_files_edited": False,
                "files": {name: hashlib.sha256(content).hexdigest() for name, content in source.items()},
                "generated_empty_package_markers": [name for name in source if name not in files]}
    write_json(root / "runner_source_manifest.json", manifest)
    return destination, manifest


def usage():
    try:
        import psutil
        process = psutil.Process()
        cpu = process.cpu_times()
        return {"rss_mb": process.memory_info().rss / 1024 ** 2,
                "cpu_seconds": cpu.user + cpu.system, "source": "psutil", "rss_kind": "current"}
    except ImportError:
        sample = resource.getrusage(resource.RUSAGE_SELF)
        return {"rss_mb": sample.ru_maxrss / (1024 ** 2 if sys.platform == "darwin" else 1024),
                "cpu_seconds": sample.ru_utime + sample.ru_stime,
                "source": "resource.getrusage", "rss_kind": "process_lifetime_peak"}


def percentile(values, percent):
    values = sorted(values)
    if not values:
        return None
    pos = (len(values) - 1) * percent / 100
    lo, hi = math.floor(pos), math.ceil(pos)
    return values[lo] * (hi - pos) + values[hi] * (pos - lo) if hi != lo else values[lo]


def workflow_rows(snapshot):
    rows = []
    for workflow_id, flow in snapshot["workflows"].items():
        tasks = [t for t in snapshot["tasks"].values() if t["workflow_id"] == workflow_id]
        starts = [a["started_at"] for t in tasks for a in t.get("attempts", [])
                  if a.get("started_at") is not None]
        starts += [t["started_at"] for t in tasks if t.get("started_at") is not None]
        submit, finish = flow["submitted_at"], flow.get("finished_at")
        success = flow["status"] in ("completed", "success", "succeeded")
        rows.append({"workflow_id": workflow_id, "template": flow.get("template"),
                     "submit": submit, "start": min(starts) if starts else None, "finish": finish,
                     "latency_seconds": finish - submit if finish is not None else None,
                     "status": flow["status"], "success": success, "deadline": flow.get("deadline"),
                     "on_time": success and finish is not None and
                     (flow.get("deadline") is None or finish <= flow["deadline"]),
                     "attempts": sum(t.get("attempt", 0) for t in tasks)})
    return rows


def metrics(rows, snapshot, samples=()):
    successful = [r for r in rows if r["success"]]
    latencies = [r["latency_seconds"] for r in successful if r["latency_seconds"] is not None]
    finishes = [r["finish"] for r in rows if r["finish"] is not None]
    span = max(finishes) - min(r["submit"] for r in rows) if rows and finishes else 0
    cpu = samples[-1]["cpu_seconds"] - samples[0]["cpu_seconds"] if len(samples) >= 2 else None
    cpu_start = samples[0]["now"] if samples else None
    cpu_end = samples[-1]["now"] if samples else None
    cpu_span = cpu_end - cpu_start if len(samples) >= 2 else None
    return {"workflows": len(rows), "successful": len(successful),
            "failed_or_cancelled": len(rows) - len(successful),
            "p50_seconds": percentile(latencies, 50), "p95_seconds": percentile(latencies, 95),
            "p99_seconds": percentile(latencies, 99), "measurement_span_seconds": span,
            "workflow_measurement_span_seconds": span,
            "throughput_per_second": len(successful) / span if span > 0 else None,
            "on_time_count": sum(r["on_time"] for r in rows),
            "on_time_fraction": sum(r["on_time"] for r in rows) / len(rows) if rows else None,
            "latency_population": "successful workflows; failures counted separately",
            "resource_peaks": snapshot.get("peaks", {}),
            "model_forward_count": snapshot.get("model_forward_count"),
            "model_forward_successes": snapshot.get("model_forward_successes"),
            "cache": snapshot.get("cache"),
            "rss_peak_sampled_mb": max((s["rss_mb"] for s in samples), default=None),
            "rss_measurement": samples[0].get("rss_kind") if samples else None,
            "process_cpu_seconds": cpu,
            "cpu_measurement_start": cpu_start, "cpu_measurement_end": cpu_end,
            "cpu_measurement_span": cpu_span, "cpu_measurement_span_seconds": cpu_span,
            "cpu_scope": "process CPU delta over actual sample interval; includes overlapping work and response processing within these boundaries; separate from workflow throughput window",
            "cpu_mean_cores": cpu / cpu_span if cpu is not None and cpu_span is not None and cpu_span > 0 else None}


def audit(snapshot, *, runtime=False):
    """Recompute ledger invariants from the saved scheduler JSON."""
    scheduler = snapshot.get("scheduler", snapshot)
    resources = scheduler["resources"]
    errors = []
    for value in [resources, scheduler["peaks"], *scheduler["resource_events"]]:
        for key, budget in (("cpu_used", "cpu_budget"), ("memory_used_mb", "memory_budget"),
                            ("concurrency", "max_concurrency")):
            if not -1e-9 <= value[key] <= resources[budget] + 1e-9:
                errors.append("resource bound: " + key)
    for name in ("events", "resource_events"):
        times = [e["now"] for e in scheduler.get(name, [])]
        if times != sorted(times) or any(t < 0 or not math.isfinite(t) for t in times):
            errors.append("event time order: " + name)
    reserved, released = {}, {}
    for event in scheduler["resource_events"]:
        target = reserved if event["event"] == "reserve" else released if event["event"] == "release" else None
        if target is not None:
            key = event["reservation_id"]
            if key in target:
                errors.append("duplicate reservation event: " + key)
            target[key] = event
    if set(reserved) != set(released):
        errors.append("reserve/release mismatch")
    for key in set(reserved) & set(released):
        if released[key]["now"] < reserved[key]["now"]:
            errors.append("release precedes reserve")
    if scheduler["reservations"] or resources["concurrency"] or resources["cpu_used"] or resources["memory_used_mb"]:
        errors.append("terminal resource leak")
    for task in scheduler["tasks"].values():
        if not task.get("terminal", False):
            errors.append("nonterminal task: " + task["task_id"])
        if task.get("attempt", 0) > task.get("max_attempts", 2):
            errors.append("attempt bound")
        if task.get("started_at") is not None:
            for dep in task.get("depends_on", []):
                parent = scheduler["tasks"].get(task["workflow_id"] + ":" + dep)
                if parent is None or parent.get("finished_at") is None or parent["finished_at"] > task["started_at"]:
                    errors.append("dependency started prematurely: " + task["task_id"])
    if runtime:
        if not snapshot.get("closed") or snapshot.get("active_futures") != 0 or snapshot.get("cache", {}).get("inflight") != 0:
            errors.append("shutdown/future leak")
        if any(i.get("active_forwards", 0) for i in snapshot.get("instances", {}).values()):
            errors.append("active model forward after shutdown")
        if len(snapshot.get("instances", {})) != 2:
            errors.append("expected two actual B1 instances")
        if sum(i.get("model_forward_count", 0) for i in snapshot.get("instances", {}).values()) != snapshot.get("model_forward_count"):
            errors.append("instance/global forward count mismatch")
        # A release must follow the actual synchronous operation's completion, including failed attempts.
        by_attempt = {(p["task_id"], p["attempt"]): p for p in snapshot.get("profiles", [])}
        for task in scheduler["tasks"].values():
            for attempt in task.get("attempts", []):
                profile = by_attempt.get((task["task_id"], attempt["attempt"]))
                release = released.get(attempt.get("reservation_id"))
                if profile and release and release["now"] < profile["finished_at"]:
                    errors.append("release precedes actual future completion")
    return {"passed": not errors, "errors": sorted(set(errors)), "reservations_created": len(reserved),
            "reservations_released": len(released), "terminal_tasks": len(scheduler["tasks"]),
            "checked": ["resource bounds", "event monotonicity", "balanced reservations", "DAG order",
                        "attempt bounds", "terminal resources"] + (["closed runtime", "zero active futures"] if runtime else [])}


def make_trace(seed, count, load, service_seconds, *, repeated=False, deadline_scale=4.0):
    rng = random.Random(seed)
    order = [TEMPLATES[i % 3] for i in range(count)]
    rng.shuffle(order)
    clock, rows = 0.0, []
    mean_interval = max(service_seconds / (2 * LOADS[load]), 1e-5)
    for index, template in enumerate(order):
        if index and not repeated:
            clock += rng.expovariate(1 / mean_interval)
        frames = list(range(1264, 1272)) if repeated else [1264 + index % 8]
        rows.append({"index": index, "arrival_seconds": clock,
                     "request": {"template": template, "target_frames": frames,
                                 "deadline_seconds": max(service_seconds * deadline_scale, 0.001),
                                 "priority": index % 3, "max_attempts": 2}})
    return rows


def profile_summary(responses):
    groups = {}
    for response in responses:
        for row in response.get("profiles", []):
            if row.get("success"):
                groups.setdefault((row["capability_id"], row["instance_id"]), []).append(row["duration_seconds"])
    return [{"capability_id": cap, "instance_id": instance, "count": len(values),
             "median_seconds": statistics.median(values), "p95_seconds": percentile(values, 95),
             "min_seconds": min(values), "max_seconds": max(values)}
            for (cap, instance), values in sorted(groups.items())]


async def execute_case(config, artifact_root, directory):
    from src.service.traffic_joint_runtime import JointTrafficRuntime
    runtime = JointTrafficRuntime(artifact_root, cpu_budget=config["cpu_budget"],
                                  memory_budget=config["memory_budget"], max_concurrency=config["max_concurrency"],
                                  instance_count=2, policy=config["policy"],
                                  enable_reuse=config.get("reuse", False), enable_recovery=config.get("recovery", True))
    responses, samples, submissions, injections = [], [], [], []
    sampling = None
    failure = None
    before = provenance(artifact_root)
    try:
        load_start = time.monotonic()
        await runtime.initialize()
        initialized = runtime.snapshot()
        write_json(directory / "initialized.json", portable(initialized, artifact_root))
        write_json(directory / "initialization.json", {"seconds": time.monotonic() - load_start,
                                                       "usage": usage(), "included_in_latency": False})
        if config.get("fault"):
            # A controlled real call exception before forward; successful retries still run real B1.
            guard, fired = threading.Lock(), [False]
            for backend in runtime.backends.values():
                service, original = backend["service"], backend["service"].predict
                def predict(*args, _original=original, **kwargs):
                    with guard:
                        inject = not fired[0]
                        if inject:
                            fired[0] = True
                            injections.append({"event": "controlled_predict_exception_before_forward",
                                               "now": time.monotonic()})
                    if inject:
                        raise RuntimeError("experiment: one controlled predict call failure before forward")
                    return _original(*args, **kwargs)
                service.predict = predict
        origin = time.monotonic()
        async def sample():
            while True:
                samples.append(dict(now=time.monotonic(), **usage()))
                await asyncio.sleep(config.get("sample_interval", 0.01))
        sampling = asyncio.create_task(sample())
        samples.append(dict(now=time.monotonic(), **usage()))
        for item in config["trace"]:
            await asyncio.sleep(max(0, origin + item["arrival_seconds"] - time.monotonic()))
            request = dict(item["request"], enable_reuse=config.get("reuse", False),
                           enable_recovery=config.get("recovery", True))
            workflow_id = runtime.submit(request, app_id="experiment")
            submissions.append({"workflow_id": workflow_id, "index": item["index"],
                                "scheduled_submit": origin + item["arrival_seconds"],
                                "actual_submit": time.monotonic(), "request": request})
            if config.get("sequential"):
                responses.append(await runtime.wait(workflow_id))
        if config.get("shutdown_active"):
            for _ in range(1000):
                active = runtime.snapshot()
                if active.get("active_futures", 0):
                    break
                await asyncio.sleep(0.001)
            write_json(directory / "before_shutdown.json", portable(active, artifact_root))
            if not active.get("active_futures", 0):
                raise RuntimeError("shutdown probe did not observe active futures")
            await runtime.shutdown()
        elif not config.get("sequential"):
            responses = await asyncio.wait_for(asyncio.gather(*(runtime.wait(row["workflow_id"])
                                                              for row in submissions)), config.get("case_timeout", 120))
        samples.append(dict(now=time.monotonic(), **usage()))
        pre_shutdown = runtime.snapshot()
        write_json(directory / "before_terminal_shutdown.json", portable(pre_shutdown, artifact_root))
    except Exception as exc:
        failure = {"type": type(exc).__name__, "message": str(exc), "traceback": traceback.format_exc()}
    finally:
        if sampling is not None:
            sampling.cancel()
            await asyncio.gather(sampling, return_exceptions=True)
        await runtime.shutdown()
    final = runtime.snapshot()
    final = portable(final, artifact_root)
    write_json(directory / "snapshot.json", final)
    write_json(directory / "responses.json", portable(responses, artifact_root))
    write_json(directory / "submissions.json", submissions)
    write_json(directory / "faults.json", injections)
    write_csv(directory / "samples.csv", samples)
    rows = workflow_rows(final)
    write_csv(directory / "workflows.csv", rows)
    write_csv(directory / "tasks.csv", final.get("tasks", {}).values())
    write_csv(directory / "profiles.csv", final.get("profiles", []))
    write_json(directory / "events.json", {"scheduler": final.get("events", []),
                                         "resources": final.get("resource_events", []), "faults": injections})
    checks = audit(final, runtime=True) if final.get("scheduler", {}).get("resources") else {"passed": False, "errors": ["initialization failed"]}
    after = provenance(artifact_root)
    unchanged = before["artifacts"] == after["artifacts"] and before["model_source"] == after["model_source"]
    checks["artifact_sha256_unchanged"] = unchanged
    checks["code_sha256_unchanged_during_case"] = before["code"] == after["code"]
    checks["passed"] = checks["passed"] and unchanged and checks["code_sha256_unchanged_during_case"]
    write_json(directory / "invariants.json", checks)
    result_metrics = metrics(rows, final, samples)
    write_json(directory / "metrics.json", result_metrics)
    write_json(directory / "profile_summary.json", profile_summary(responses))
    write_json(directory / "provenance.json", before)
    if failure:
        write_json(directory / "error.json", portable(failure, artifact_root))
    return not failure and checks["passed"]


def validate_predictions(directory, artifact_root, tolerance):
    import numpy as np
    responses = json.loads((directory / "responses.json").read_text())
    frames = list(range(1264, 1272))
    tests = []
    with np.load(Path(artifact_root) / "results/B1_seed42_L3/test_predictions.npz", allow_pickle=False) as reference:
        # Saved test_predictions stores test frames in chronological order. Reconstruct with the saved split/day rule.
        metadata = json.loads((Path(artifact_root) / "data/processed/dataset_metadata.json").read_text())
        split = metadata["split_frames"]["test"]
        first, stop = split if isinstance(split, list) else (split["start"], split["end"])
        expected_frames = [t for t in range(first, stop) if t % 48 >= 3]
        if len(expected_frames) != reference["pred_flow"].shape[0]:
            raise ValueError("saved test reference length does not match split/day reconstruction")
        indices = [expected_frames.index(t) for t in frames]
        for response in responses:
            for name, prediction in response.get("predictions", {}).items():
                actual = np.asarray(prediction["prediction"], dtype=np.float64)
                expected = reference["pred_" + name][indices].astype(np.float64)
                if actual.shape != (8, 1, *expected.shape[1:]):
                    raise ValueError("unexpected B,H,Y,X,C prediction shape")
                diff = float(np.max(np.abs(actual[:, 0] - expected)))
                tests.append({"template": response["template"], "modality": name,
                              "shape": list(actual.shape), "max_abs_error": diff,
                              "tolerance": tolerance, "passed": diff <= tolerance})
    modalities = {row["modality"] for row in tests}
    answer = {"passed": modalities == {"state", "flow", "od"} and all(row["passed"] for row in tests),
              "target_frames": frames, "history": "[t-3,t)", "horizon": 1, "batch_size": 8,
              "semantics": "eight independent one-step predictions, not an eight-step forecast",
              "reference_sha256": sha(Path(artifact_root) / "results/B1_seed42_L3/test_predictions.npz"),
              "checks": tests}
    write_json(directory / "prediction_equivalence.json", answer)
    return answer


def grouped_summary(root, case_names):
    groups = {}
    for name in case_names:
        directory = root / name
        config = json.loads((directory / "config.json").read_text())
        if not (directory / "metrics.json").exists():
            continue
        measurement = json.loads((directory / "metrics.json").read_text())
        key = (config["kind"], config["policy"], config.get("load", "none"),
               config.get("reuse", False), config.get("recovery", True), config.get("deadline_profile", "balanced"))
        groups.setdefault(key, []).append((name, measurement))
    rows = []
    for key, cases in sorted(groups.items()):
        row = dict(zip(("kind", "policy", "load", "reuse", "recovery", "deadline_profile"), key))
        row.update(repetitions=len(cases), sources=[name + "/metrics.json" for name, _ in cases])
        for field in ("p50_seconds", "p95_seconds", "p99_seconds", "throughput_per_second", "on_time_fraction",
                      "model_forward_count", "rss_peak_sampled_mb", "process_cpu_seconds",
                      "cpu_measurement_span_seconds", "cpu_mean_cores"):
            values = [m[field] for _, m in cases if m.get(field) is not None]
            row[field + "_mean"] = statistics.mean(values) if values else None
            row[field + "_stdev"] = statistics.stdev(values) if len(values) > 1 else None
        rows.append(row)
    write_json(root / "summary.json", rows)
    write_csv(root / "summary.csv", rows)
    return rows


def paired_checks(root, cases):
    """Check fairness and ablation outcomes from saved JSON, never prior assumptions."""
    import numpy as np
    grouped, comparisons = {}, []
    for name in cases:
        config = json.loads((root / name / "config.json").read_text())
        if config["kind"] == "compare":
            grouped.setdefault((config["seed"], config["load"]), []).append(name)
    for (seed, load), names in grouped.items():
        configs = [json.loads((root / n / "config.json").read_text()) for n in names]
        provenances = [json.loads((root / n / "provenance.json").read_text()) for n in names
                       if (root / n / "provenance.json").exists()]
        matching = all(c["trace"] == configs[0]["trace"] and
                       all(c[k] == configs[0][k] for k in ("cpu_budget", "memory_budget", "max_concurrency", "reuse", "recovery"))
                       for c in configs)
        same_code = len(provenances) == len(names) and len({digest_json(p["code"]) for p in provenances}) == 1
        comparisons.append({"seed": seed, "load": load, "cases": names,
                            "same_trace_resources": matching, "same_code_sha256": same_code,
                            "passed": matching and same_code})
    reuse = []
    seeds = sorted({json.loads((root / n / "config.json").read_text()).get("seed") for n in cases
                    if n.startswith("reuse_")})
    for seed in seeds:
        names = [f"reuse_{seed}_{flag}" for flag in (0, 1)]
        if not all((root / n / "responses.json").exists() for n in names):
            reuse.append({"seed": seed, "passed": False, "reason": "paired output missing"})
            continue
        maps, measurements = [], []
        for name in names:
            submissions = json.loads((root / name / "submissions.json").read_text())
            index = {r["workflow_id"]: r["index"] for r in submissions}
            responses = json.loads((root / name / "responses.json").read_text())
            maps.append({index[r["workflow_id"]]: r for r in responses})
            measurements.append(json.loads((root / name / "metrics.json").read_text()))
        max_error = 0.0
        equal = maps[0].keys() == maps[1].keys()
        for index in maps[0].keys() & maps[1].keys():
            left, right = maps[0][index], maps[1][index]
            equal = equal and left["status"] == right["status"] and left["predictions"].keys() == right["predictions"].keys()
            for modality in left["predictions"].keys() & right["predictions"].keys():
                a, b = np.asarray(left["predictions"][modality]["prediction"]), np.asarray(right["predictions"][modality]["prediction"])
                if a.shape != b.shape:
                    equal = False
                else:
                    max_error = max(max_error, float(np.max(np.abs(a - b))))
        reuse.append({"seed": seed, "sources": names, "max_abs_prediction_difference": max_error,
                      "passed": equal and max_error <= 1e-5,
                      "forward_count_off": measurements[0]["model_forward_count"],
                      "forward_count_on": measurements[1]["model_forward_count"],
                      "p95_off": measurements[0]["p95_seconds"], "p95_on": measurements[1]["p95_seconds"]})
    result = {"strategy_fairness": comparisons, "reuse_output_equivalence": reuse,
              "passed": all(r["passed"] for r in comparisons + reuse)}
    write_json(root / "paired_checks.json", result)
    return result


def run_child(args, root, name, config):
    directory = root / name
    directory.mkdir()
    write_json(directory / "config.json", config)
    runner_root = root / "runner_source"
    command = [sys.executable, str(runner_root / "scripts/run_traffic_joint_experiments.py"),
               "--case-config", str(directory / "config.json"),
               "--artifact-root", str(args.artifact_root)]
    env = dict(os.environ, PYTHONPATH=str(runner_root), PYTHONDONTWRITEBYTECODE="1")
    timed_out = False
    with (directory / "process.log").open("x") as stream:
        try:
            process = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT, env=env,
                                     timeout=args.case_timeout + 120)
            code = process.returncode
        except subprocess.TimeoutExpired:
            timed_out, code = True, None
    write_json(directory / "process.json", {"exit_code": code, "isolated_process": True, "timed_out": timed_out})
    print(name + ": " + ("OK" if code == 0 else "FAILED"), flush=True)
    return code == 0


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--artifact-root", type=Path, default=Path(os.environ.get("TRAFFIC_EXPERIMENT_ROOT", DEFAULT_ARTIFACT_ROOT)))
    result.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    result.add_argument("--mode", choices=("all", "validate", "profile", "compare", "ablations", "shutdown"), default="all")
    result.add_argument("--quick", action="store_true", help="smoke only: one seed, six workflows; not statistical evidence")
    result.add_argument("--seeds", type=int, nargs="+", default=[17, 29, 42, 53, 71])
    result.add_argument("--policies", choices=POLICIES, nargs="+", default=list(POLICIES))
    result.add_argument("--workflows", type=int, default=24)
    result.add_argument("--cpu-budget", type=float, default=2)
    result.add_argument("--memory-budget", type=float, default=2048)
    result.add_argument("--max-concurrency", type=int, default=2)
    result.add_argument("--case-timeout", type=float, default=120)
    result.add_argument("--tolerance", type=float, default=1e-5)
    result.add_argument("--profile-json", type=Path, help="an earlier real profile case responses.json")
    result.add_argument("--case-config", type=Path, help=argparse.SUPPRESS)
    return result


def main():
    args = parser().parse_args()
    args.artifact_root = args.artifact_root.expanduser().resolve()
    args.output_root = args.output_root.expanduser().resolve()
    if args.case_config:
        config = json.loads(args.case_config.read_text())
        return 0 if asyncio.run(execute_case(config, args.artifact_root, args.case_config.parent)) else 1
    if args.workflows < 3 or args.tolerance <= 0 or args.tolerance > 1e-5:
        raise SystemExit("require at least three workflows and 0 < tolerance <= 1e-5")
    args.output_root.mkdir(parents=True, exist_ok=True)
    root = args.output_root / ("real_" + time.strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8])
    root.mkdir()
    try:
        _, frozen = snapshot_code(root)
    except RuntimeError as exc:
        write_json(root / "run.json", {"passed": False, "stage": "source_snapshot", "cases": [],
                                      "failures": [str(exc)], "quick": args.quick})
        print(str(root), flush=True)
        return 1
    sources = provenance(args.artifact_root)
    sources["code"] = frozen["files"]
    sources["execution_source"] = "runner_source"
    sources["runner_source_manifest_sha256"] = sha(root / "runner_source_manifest.json")
    write_json(root / "provenance.json", sources)
    seeds, count = (args.seeds[:1], 6) if args.quick else (args.seeds, args.workflows)
    base = {"cpu_budget": args.cpu_budget, "memory_budget": args.memory_budget,
            "max_concurrency": args.max_concurrency, "case_timeout": args.case_timeout,
            "policy": "joint", "reuse": False, "recovery": True}
    cases, failures = [], []
    def run(name, config):
        cases.append(name)
        ok = run_child(args, root, name, dict(base, **config))
        if not ok:
            failures.append(name)
        return ok
    if args.mode in ("all", "validate"):
        trace = [{"index": i, "arrival_seconds": 0, "request": {"template": template,
                  "target_frames": list(range(1264, 1272)), "deadline_seconds": 60}}
                 for i, template in enumerate(TEMPLATES)]
        if run("validation", {"kind": "validation", "trace": trace}):
            if not validate_predictions(root / "validation", args.artifact_root, args.tolerance)["passed"]:
                failures.append("prediction_equivalence")
    service_seconds = None
    if args.profile_json:
        responses = json.loads(args.profile_json.read_text())
        service_seconds = statistics.median(r["finished_at"] - r["submitted_at"] for r in responses)
        write_json(root / "profile_source.json", {"sha256": sha(args.profile_json), "kind": "real_B1_responses"})
    elif args.mode in ("all", "profile", "compare", "ablations"):
        trace = [{"index": i, "arrival_seconds": 0, "request": {"template": TEMPLATES[i % 3],
                  "target_frames": [1264 + i % 8], "deadline_seconds": 60,
                  "allowed_instances": [f"b1-{1 + i % 2}"]}}
                 for i in range(6 if args.quick else 18)]
        if run("profile", {"kind": "profile", "trace": trace, "sequential": True}):
            responses = json.loads((root / "profile/responses.json").read_text())
            service_seconds = statistics.median(r["finished_at"] - r["submitted_at"] for r in responses)
    if service_seconds is not None:
        write_json(root / "load_calibration.json", {"service_seconds": service_seconds,
                   "source": "median real sequential workflow finish-submit; includes platform overhead, excludes initialization",
                   "offered_load": LOADS, "instances": 2,
                   "caveat": "nominal load from sequential profile; observed contention is reported separately"})
        if args.mode in ("all", "compare"):
            for seed in seeds:
                for load in LOADS:
                    trace = make_trace(seed, count, load, service_seconds)
                    for policy in args.policies:
                        run(f"compare_{load}_{seed}_{policy}", {"kind": "compare", "policy": policy,
                            "seed": seed, "load": load, "trace": trace, "trace_sha256": digest_json(trace)})
        if args.mode in ("all", "ablations"):
            for seed in seeds:
                for reuse in (False, True):
                    trace = make_trace(seed, count, "high", service_seconds, repeated=True)
                    run(f"reuse_{seed}_{int(reuse)}", {"kind": "reuse", "seed": seed, "load": "high",
                        "reuse": reuse, "trace": trace, "trace_sha256": digest_json(trace)})
                for recovery in (False, True):
                    trace = make_trace(seed, 6 if args.quick else 12, "high", service_seconds)
                    run(f"recovery_{seed}_{int(recovery)}", {"kind": "recovery", "seed": seed, "load": "high",
                        "recovery": recovery, "fault": True, "trace": trace, "trace_sha256": digest_json(trace)})
                for name, scale in (("tight", 1), ("balanced", 4), ("relaxed", 16)):
                    trace = make_trace(seed, count, "high", service_seconds, deadline_scale=scale)
                    run(f"deadline_{seed}_{name}", {"kind": "deadline", "seed": seed, "load": "high",
                        "deadline_profile": name, "trace": trace, "trace_sha256": digest_json(trace)})
    elif args.mode in ("all", "compare", "ablations"):
        failures.append("real_profile_unavailable")
    if args.mode in ("all", "shutdown"):
        trace = make_trace(17, 12, "high", 0.05, repeated=True)
        run("shutdown", {"kind": "shutdown", "trace": trace, "shutdown_active": True})
    grouped_summary(root, cases)
    if not paired_checks(root, cases)["passed"]:
        failures.append("paired_checks")
    write_json(root / "output_manifest.json", {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob("*"))
                                               if p.is_file()})
    write_json(root / "run.json", {"kind": "real_B1", "quick": args.quick, "seeds": seeds,
               "cases": cases, "failures": failures, "passed": not failures, "measurements": "real JSON/CSV",
               "artifacts_unchanged": provenance(args.artifact_root)["artifacts"] ==
               json.loads((root / "provenance.json").read_text())["artifacts"]})
    print(str(root), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
