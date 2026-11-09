#!/usr/bin/env python3
"""Discrete-event mixed-workflow scheduler comparison, never an inference benchmark."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import heapq
import json
from pathlib import Path
import random
import statistics
import sys
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.run_traffic_joint_experiments import (
    DEFAULT_OUTPUT_ROOT, LOADS, POLICIES, TEMPLATES, audit, digest_json, grouped_summary,
    metrics, provenance, sha, workflow_rows, write_csv, write_json,
)
from src.graph.traffic_joint_scheduler import JointScheduler
from src.service.traffic_types import Candidate, PlanNode, WorkflowPlan

CAPABILITIES = ("history.prepare", "traffic.predict", "validate.state", "validate.flow", "validate.od",
                "analysis.congestion", "analysis.od_hotspots", "result.aggregate")
SYNTHETIC = dict(zip(CAPABILITIES, (0.02, 0.12, 0.012, 0.012, 0.03, 0.025, 0.04, 0.01)))


def topology(template):
    nodes = [("prepare", "history.prepare", ()), ("predict", "traffic.predict", ("prepare",))]
    if template == "flow":
        nodes += [("validate_flow", "validate.flow", ("predict",))]
        terminal = ("validate_flow",)
    elif template == "analysis":
        nodes += [("validate_state", "validate.state", ("predict",)),
                  ("validate_flow", "validate.flow", ("predict",)),
                  ("congestion", "analysis.congestion", ("validate_state", "validate_flow"))]
        terminal = ("validate_state", "validate_flow", "congestion")
    else:
        nodes += [("validate_od", "validate.od", ("predict",)),
                  ("hotspots", "analysis.od_hotspots", ("validate_od",))]
        terminal = ("validate_od", "hotspots")
    return nodes + [("aggregate", "result.aggregate", terminal)]


def load_profile(path):
    """Use only complete measured profiles. A partial source never silently fills gaps."""
    if path is None:
        return None, {"kind": "seeded_synthetic_durations", "reason": "no measured profile supplied"}
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError("profile must be real responses.json or profile_summary.json array")
    evidence_path = path.parent / "provenance.json"
    evidence = json.loads(evidence_path.read_text()) if evidence_path.exists() else {}
    if "results/B1_seed42_L3/best.pt" not in evidence.get("artifacts", {}):
        return None, {"kind": "seeded_synthetic_durations", "source_sha256": sha(path),
                      "reason": "profile lacks real B1 artifact SHA provenance"}
    rows = [p for response in data for p in response.get("profiles", [])] if data and "profiles" in data[0] else data
    grouped = {}
    for row in rows:
        if row.get("success", True):
            duration = row.get("duration_seconds", row.get("median_seconds"))
            if duration is not None and duration > 0:
                grouped.setdefault((row["capability_id"], row["instance_id"]), []).append(duration)
    missing = [cap for cap in CAPABILITIES if not any(key[0] == cap for key in grouped)]
    source = {"source_sha256": sha(path), "missing_capabilities": missing,
              "provenance_sha256": sha(evidence_path), "artifact_sha256": evidence["artifacts"]}
    if missing:
        return None, dict(source, kind="seeded_synthetic_durations", reason="measured profile incomplete; full synthetic fallback")
    measured = {(cap, instance): statistics.median(values) for (cap, instance), values in grouped.items()}
    # If one instance never executed a capability, use the measured capability median and disclose it.
    imputed = []
    for cap in CAPABILITIES:
        for instance in ("b1-1", "b1-2"):
            if (cap, instance) not in measured:
                measured[cap, instance] = statistics.median(v for (c, _), v in measured.items() if c == cap)
                imputed.append({"capability_id": cap, "instance_id": instance})
    return measured, dict(source, kind="simulation_from_measured_profile",
                          missing_instance_cells_using_capability_median=imputed,
                          caveat="event simulation uses measured medians, not actual concurrent B1 execution")


def workload(seed, count, load, measured):
    rng = random.Random(seed)
    estimates = {(cap, instance): measured[cap, instance] if measured else SYNTHETIC[cap] * (1 if instance == "b1-1" else 1.3)
                 for cap in CAPABILITIES for instance in ("b1-1", "b1-2")}
    service = statistics.mean(sum(min(estimates[cap, i] for i in ("b1-1", "b1-2"))
                                  for _, cap, _ in topology(template)) for template in TEMPLATES)
    mean_interval = service / (2 * LOADS[load])
    types = [TEMPLATES[i % 3] for i in range(count)]
    rng.shuffle(types)
    arrivals, duration_table, now = [], {}, 0.0
    for index, template in enumerate(types):
        if index:
            now += rng.expovariate(1 / mean_interval)
        workflow_id = f"w{index:05d}"
        arrivals.append({"workflow_id": workflow_id, "arrival": now, "template": template,
                         "priority": rng.randrange(3), "deadline": now + service * rng.choice((1, 2, 4, 8)),
                         "target_frames": [1264 + index % 8]})
        for node_id, cap, _ in topology(template):
            for instance in ("b1-1", "b1-2"):
                # Runtime uncertainty is seeded before policy execution and saved for every task/instance.
                duration_table[workflow_id + ":" + node_id + "@" + instance] = estimates[cap, instance] * rng.uniform(0.8, 1.2)
    return arrivals, duration_table, estimates


def simulate(arrivals, durations, estimates, args, policy, directory):
    scheduler = JointScheduler(args.cpu_budget, args.memory_budget, args.max_concurrency,
                               policy=policy, aging_interval=args.aging_interval)
    plans = []
    for item in arrivals:
        nodes = []
        for node_id, cap, deps in topology(item["template"]):
            candidates = tuple(Candidate(instance_id=instance, capability_id=cap, implementation_id="B1-profile",
                                         version="simulation-v1", cpu=1, memory_mb=32 if cap == "traffic.predict" else 8,
                                         expected_seconds=estimates[cap, instance], max_concurrency=1)
                               for instance in ("b1-1", "b1-2"))
            nodes.append(PlanNode(node_id, cap, depends_on=deps, candidates=candidates,
                                  max_attempts=2, timeout_seconds=3600))
        outputs = {"flow": ("flow",), "analysis": ("state", "flow"), "od_hotspots": ("od",)}[item["template"]]
        plans.append(WorkflowPlan(item["workflow_id"], "simulation", item["template"], tuple(nodes),
                                  tuple(item["target_frames"]), outputs, created_at=item["arrival"],
                                  deadline=item["deadline"], priority=item["priority"]))
    write_json(directory / "plans.json", [asdict(plan) for plan in plans])
    index, sequence, now, running = 0, 0, 0.0, []
    execution = []
    while index < len(plans) or running:
        next_arrival = arrivals[index]["arrival"] if index < len(plans) else float("inf")
        next_finish = running[0][0] if running else float("inf")
        now = min(next_arrival, next_finish)
        if now == float("inf"):
            raise RuntimeError("simulation stalled: no next arrival or real completion event")
        while running and running[0][0] <= now:
            finish, _, assignment, started = heapq.heappop(running)
            scheduler.finish(assignment.task_id, finish)
            execution.append({"task_id": assignment.task_id, "instance_id": assignment.instance_id,
                              "attempt": assignment.attempt, "start": started, "finish": finish,
                              "duration_seconds": finish - started})
        while index < len(plans) and arrivals[index]["arrival"] <= now:
            scheduler.submit(plans[index], now)
            index += 1
        for assignment in scheduler.tick(now):
            scheduler.start(assignment.task_id, now)
            duration = durations[assignment.task_id + "@" + assignment.instance_id]
            heapq.heappush(running, (now + duration, sequence, assignment, now))
            sequence += 1
    snapshot = scheduler.snapshot()
    rows = workflow_rows(snapshot)
    checks = audit(snapshot)
    write_json(directory / "snapshot.json", snapshot)
    write_json(directory / "invariants.json", checks)
    write_json(directory / "metrics.json", dict(metrics(rows, snapshot),
               measurement_kind="discrete_event_simulation", actual_model_forwards=0))
    write_json(directory / "events.json", {"scheduler": snapshot["events"], "resources": snapshot["resource_events"]})
    write_csv(directory / "execution.csv", execution)
    write_csv(directory / "workflows.csv", rows)
    write_csv(directory / "tasks.csv", snapshot["tasks"].values())
    return checks["passed"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--profile-json", type=Path)
    parser.add_argument("--quick", action="store_true", help="two seeds, twelve workflows; smoke only")
    parser.add_argument("--seeds", type=int, nargs="+", default=list(range(20)))
    parser.add_argument("--workflows", type=int, default=24)
    parser.add_argument("--policies", choices=POLICIES, nargs="+", default=list(POLICIES))
    parser.add_argument("--cpu-budget", type=float, default=2)
    parser.add_argument("--memory-budget", type=float, default=256)
    parser.add_argument("--max-concurrency", type=int, default=2)
    parser.add_argument("--aging-interval", type=float, default=0.1)
    args = parser.parse_args()
    if args.workflows < 3:
        parser.error("require at least three workflows")
    args.output_root.mkdir(parents=True, exist_ok=True)
    root = args.output_root / ("simulation_" + time.strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:8])
    root.mkdir()
    measured, source = load_profile(args.profile_json)
    write_json(root / "duration_source.json", source)
    write_json(root / "provenance.json", provenance())
    seeds, count = (args.seeds[:2], 12) if args.quick else (args.seeds, args.workflows)
    cases, failures = [], []
    for seed in seeds:
        for load in LOADS:
            arrivals, durations, estimates = workload(seed, count, load, measured)
            workload_name = f"workload_{load}_{seed}.json"
            saved = {"arrivals": arrivals, "duration_table": durations,
                     "estimated_profiles": [{"capability_id": c, "instance_id": i, "seconds": v}
                                            for (c, i), v in sorted(estimates.items())]}
            write_json(root / workload_name, saved)
            for policy in args.policies:
                name = f"compare_{load}_{seed}_{policy}"
                directory = root / name
                directory.mkdir()
                cases.append(name)
                write_json(directory / "config.json", {"kind": source["kind"], "policy": policy,
                           "seed": seed, "load": load, "workflows": count, "cpu_budget": args.cpu_budget,
                           "memory_budget": args.memory_budget, "max_concurrency": args.max_concurrency,
                           "aging_interval": args.aging_interval, "workload_source": workload_name,
                           "workload_sha256": digest_json(saved), "reuse": False, "recovery": True})
                if not simulate(arrivals, durations, estimates, args, policy, directory):
                    failures.append(name)
            print(f"seed={seed} load={load}: {len(args.policies)} policies saved", flush=True)
    grouped_summary(root, cases)
    write_json(root / "output_manifest.json", {str(p.relative_to(root)): sha(p) for p in sorted(root.rglob("*"))
                                               if p.is_file()})
    write_json(root / "run.json", {"quick": args.quick, "seeds": seeds, "cases": cases,
               "duration_source": source, "failures": failures, "passed": not failures,
               "actual_model_forwards": 0, "comparison": "same arrivals, task-instance durations, profiles and resources per seed/load"})
    print(str(root), flush=True)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
