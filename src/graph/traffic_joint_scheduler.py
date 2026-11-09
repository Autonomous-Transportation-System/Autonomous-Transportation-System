"""Joint, deterministic admission of numeric DAGs into one ResourcePool.

The lock protects metadata transactions only; it never owns executor work or
waits for execution. A running reservation survives cancellation and execution
timeout until the executor confirms termination. Workflow deadlines
are soft targets: a miss is recorded without cancelling work. Callbacks for one
task must be delivered in attempt order: the shared finish API has no attempt
token with which to identify an old callback after a retry has started.

FCFS uses submission order. DRC uses priority plus waiting age. EDF and HEFT
promote age before deadline/upward rank; joint combines effective priority,
deadline slack, upward rank and EFT. Equal scores favor the less-dispatched
workflow, then submission/node order. All policies scan feasible work and use
the same dependency, health, instance capacity and resource admission rules.
Only the standard library is needed, including on Python 3.9.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from fractions import Fraction
import math
from numbers import Real
from threading import RLock
from typing import Any, Dict, List, Optional, Tuple

from src.graph.traffic_scheduler import ResourcePool
from src.service.traffic_types import Assignment, Candidate, PlanNode, WorkflowPlan

__all__ = ["JointScheduler"]

_TERMINAL = {"completed", "failed", "cancelled", "skipped", "start_failed"}
_CALLS = {"success", "failed", "timeout", "cancelled"}
_QUALITIES = {"valid", "invalid", "degraded", "not_evaluated"}


def _number(value: Any, name: str, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite number")
    try:
        value = float(value)
    except (ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(f"{name} must be finite and {'positive' if positive else 'nonnegative'}")
    return value


def _integer(value: Any, name: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _identifier(value: Any, name: str, local: bool = False) -> str:
    if not isinstance(value, str) or not value.strip() or (local and ":" in value):
        raise ValueError(f"{name} must be a nonempty string" + (" without ':'" if local else ""))
    return value


def _optional_text(value: Any, name: str) -> None:
    if value is not None and not isinstance(value, str):
        raise ValueError(f"{name} must be a string or None")


def _duration(candidate: Candidate) -> float:
    return candidate.expected_seconds + candidate.cold_start_seconds + candidate.adapter_seconds


def _plus(left: float, right: float) -> float:
    # Estimates saturate at the largest finite time rather than emitting Inf.
    return min(left + right, float.fromhex("0x1.fffffffffffffp+1023"))


@dataclass(frozen=True)
class _Task:
    task_id: str
    workflow_id: str
    spec: PlanNode
    order: int
    critical_path: float
    minimum_duration: float
    schedule_status: str = "pending"
    call_status: str = "not_called"
    quality_status: str = "not_evaluated"
    ready_at: Optional[float] = None
    reserved_at: Optional[float] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    reservation_id: Optional[str] = None
    candidate: Optional[Candidate] = None
    attempt: int = 0
    tried_instances: Tuple[str, ...] = ()
    cancel_requested: bool = False
    timeout_requested: bool = False
    reason: Optional[str] = None
    error: Optional[str] = None
    result: Any = None
    selection: Optional[Dict[str, Any]] = None
    attempts: Tuple[Dict[str, Any], ...] = ()

    @property
    def terminal(self) -> bool:
        return self.schedule_status in _TERMINAL

    @property
    def successful(self) -> bool:
        return self.schedule_status == "completed" and self.call_status == "success" and self.quality_status == "valid"


@dataclass(frozen=True)
class _Workflow:
    plan: WorkflowPlan
    submitted_at: float
    order: int
    topology: Tuple[str, ...]
    dispatched: int = 0
    stop_reason: Optional[str] = None
    status: str = "pending"
    done: bool = False
    finished_at: Optional[float] = None
    deadline_missed: bool = False
    deadline_missed_at: Optional[float] = None


class JointScheduler:
    """Use memory_budget for incremental task memory, after resident deductions.

    New submissions carry the catalog's current candidate profiles. Existing
    plans remain isolated snapshots; explicit health overrides stay live and
    can be restored independently of those snapshots.
    """

    def __init__(
        self, cpu_budget: float, memory_budget: float, max_concurrency: int,
        policy: str = "joint", aging_interval: float = 30,
        enable_recovery: bool = True,
    ) -> None:
        if not isinstance(policy, str) or policy not in {"fcfs", "drc", "edf", "heft", "joint"}:
            raise ValueError("policy must be fcfs/drc/edf/heft/joint")
        if not isinstance(enable_recovery, bool):
            raise ValueError("enable_recovery must be bool")
        self.policy = policy
        self.aging_interval = _number(aging_interval, "aging_interval", positive=True)
        self.enable_recovery = enable_recovery
        self._lock = RLock()
        self._pool = ResourcePool(cpu_budget, memory_budget, max_concurrency)
        self._tasks: Dict[str, _Task] = {}
        self._workflows: Dict[str, _Workflow] = {}
        self._instance_limits: Dict[str, int] = {}
        self._instance_used: Dict[str, int] = {}
        self._health: Dict[str, bool] = {}
        self._events: List[Dict[str, Any]] = []
        self._now: float = 0.0

    def _time(self, now: float) -> float:
        now = _number(now, "now")
        if now < self._now:
            raise ValueError("scheduler time must be nondecreasing")
        return now

    def _event(self, event: str, now: float, **details: Any) -> None:
        self._events.append({"event": event, "now": now, **details})

    def _set(self, task: _Task, now: float, **changes: Any) -> _Task:
        task = replace(task, **changes)
        self._tasks[task.task_id] = task
        self._event("task_state", now, task_id=task.task_id, workflow_id=task.workflow_id,
                    schedule_status=task.schedule_status, call_status=task.call_status,
                    quality_status=task.quality_status, attempt=task.attempt,
                    instance_id=task.candidate.instance_id if task.candidate else None,
                    reservation_id=task.reservation_id, cancel_requested=task.cancel_requested,
                    reason=task.reason)
        return task

    def _validate_plan(self, plan: WorkflowPlan, now: float) -> Tuple[WorkflowPlan, Tuple[str, ...], Dict[str, float], Dict[str, int]]:
        if not isinstance(plan, WorkflowPlan):
            raise ValueError("plan must be a WorkflowPlan")
        _identifier(plan.workflow_id, "workflow_id", local=True)
        if not isinstance(plan.app_id, str) or not isinstance(plan.template, str):
            raise ValueError("app_id and template must be strings")
        created_at = _number(plan.created_at, "created_at")
        if created_at > now:
            raise ValueError("submission precedes created_at")
        deadline = _number(plan.deadline, "deadline") if plan.deadline is not None else None
        if deadline is not None and deadline < created_at:
            raise ValueError("deadline precedes created_at")
        _integer(plan.priority, "workflow priority")
        if not isinstance(plan.contract, dict) or not isinstance(plan.options, dict):
            raise ValueError("contract and options must be dicts")
        if "enable_recovery" in plan.options and not isinstance(plan.options["enable_recovery"], bool):
            raise ValueError("plan enable_recovery must be bool")
        if not isinstance(plan.target_frames, tuple):
            raise ValueError("target_frames must be a tuple of nonnegative integers")
        for frame in plan.target_frames:
            _integer(frame, "target_frame")
        if not isinstance(plan.requested_outputs, tuple) or any(
            not isinstance(key, str) or not key.strip() for key in plan.requested_outputs
        ):
            raise ValueError("requested_outputs must be a tuple of output names")
        if not isinstance(plan.nodes, tuple) or not plan.nodes:
            raise ValueError("nodes must be a nonempty tuple")
        nodes: Dict[str, PlanNode] = {}
        limits = dict(self._instance_limits)
        durations: Dict[str, float] = {}
        for node in plan.nodes:
            if not isinstance(node, PlanNode):
                raise ValueError("nodes must contain PlanNode instances")
            _identifier(node.node_id, "node_id", local=True)
            _identifier(node.capability_id, "capability_id")
            if node.node_id in nodes:
                raise ValueError("duplicate node_id")
            if not isinstance(node.depends_on, tuple) or any(
                not isinstance(dep, str) or not dep.strip() for dep in node.depends_on
            ) or len(set(node.depends_on)) != len(node.depends_on):
                raise ValueError("depends_on must contain unique node ids")
            if not isinstance(node.parameters, dict):
                raise ValueError("parameters must be a dict")
            _optional_text(node.reuse_key, "reuse_key")
            _integer(node.priority, "node priority")
            _integer(node.max_attempts, "max_attempts", 1)
            timeout_seconds = _number(node.timeout_seconds, "timeout_seconds")
            if not isinstance(node.candidates, tuple) or not node.candidates:
                raise ValueError("candidates must be a nonempty tuple")
            instances = set()
            fits_budget = False
            candidates = []
            for candidate in node.candidates:
                if not isinstance(candidate, Candidate):
                    raise ValueError("candidates must contain Candidate instances")
                for name in ("instance_id", "capability_id", "implementation_id", "version"):
                    _identifier(getattr(candidate, name), name)
                if candidate.capability_id != node.capability_id:
                    raise ValueError("candidate capability mismatch")
                if candidate.instance_id in instances:
                    raise ValueError("duplicate candidate instance")
                instances.add(candidate.instance_id)
                candidate = replace(candidate, **{
                    name: _number(getattr(candidate, name), name)
                    for name in ("cpu", "memory_mb", "expected_seconds", "cold_start_seconds", "adapter_seconds")
                })
                if not math.isfinite(_duration(candidate)):
                    raise ValueError("candidate total profile duration must be finite")
                _integer(candidate.max_concurrency, "instance max_concurrency", 1)
                if not isinstance(candidate.healthy, bool):
                    raise ValueError("candidate healthy must be bool")
                if candidate.instance_id in limits and limits[candidate.instance_id] != candidate.max_concurrency:
                    raise ValueError("inconsistent instance max_concurrency")
                limits[candidate.instance_id] = candidate.max_concurrency
                fits_budget |= candidate.cpu <= self._pool.cpu_budget and candidate.memory_mb <= self._pool.memory_budget
                candidates.append(candidate)
            if not fits_budget:
                raise ValueError(f"node {node.node_id!r} permanently exceeds resource budget")
            nodes[node.node_id] = replace(node, candidates=tuple(candidates), timeout_seconds=timeout_seconds)
            durations[node.node_id] = min(_duration(c) for c in candidates)
        successors: Dict[str, List[str]] = {key: [] for key in nodes}
        indegree = {key: len(node.depends_on) for key, node in nodes.items()}
        for key, node in nodes.items():
            for dep in node.depends_on:
                if dep not in nodes:
                    raise ValueError(f"missing dependency {dep!r}")
                successors[dep].append(key)
        ready = deque(key for key in nodes if indegree[key] == 0)
        topology = []
        while ready:
            key = ready.popleft()
            topology.append(key)
            for successor in successors[key]:
                indegree[successor] -= 1
                if indegree[successor] == 0:
                    ready.append(successor)
        if len(topology) != len(nodes):
            raise ValueError("dependency cycle")
        ranks = {}
        for key in reversed(topology):
            ranks[key] = durations[key] + max((ranks[s] for s in successors[key]), default=0)
            if not math.isfinite(ranks[key]):
                raise ValueError("critical path profile must be finite")
        plan = replace(plan, nodes=tuple(nodes.values()), created_at=created_at, deadline=deadline)
        return plan, tuple(topology), ranks, limits

    def submit(self, plan: WorkflowPlan, now: float) -> Dict[str, Any]:
        # Isolate mutable dictionaries inside frozen shared dataclasses.
        plan = deepcopy(plan)
        with self._lock:
            now = self._time(now)
            plan, topology, ranks, limits = self._validate_plan(plan, now)
            if plan.workflow_id in self._workflows:
                raise ValueError(f"duplicate workflow_id: {plan.workflow_id}")
            self._now = now
            self._instance_limits = limits
            flow = _Workflow(plan, now, len(self._workflows), topology)
            self._workflows[plan.workflow_id] = flow
            for order, node in enumerate(plan.nodes):
                task_id = f"{plan.workflow_id}:{node.node_id}"
                self._tasks[task_id] = _Task(task_id, plan.workflow_id, node, order,
                                            ranks[node.node_id], min(_duration(c) for c in node.candidates))
            self._event("submit", now, workflow_id=plan.workflow_id, deadline=plan.deadline)
            self._maintain(now)
            return self._workflow_dict(self._workflows[plan.workflow_id])

    def _release(self, task: _Task, now: float, *, execution_finished: bool = False) -> None:
        if task.reservation_id is None:
            return
        if self._pool.release(task.reservation_id, now, execution_finished=execution_finished):
            instance_id = task.candidate.instance_id
            self._instance_used[instance_id] -= 1

    def _stop_workflow(self, flow: _Workflow, now: float, reason: str) -> None:
        if flow.done or flow.stop_reason is not None:
            return
        self._workflows[flow.plan.workflow_id] = replace(flow, stop_reason=reason)
        self._event("cancel_request", now, workflow_id=flow.plan.workflow_id, reason=reason)
        for node_id in flow.topology:
            task = self._tasks[f"{flow.plan.workflow_id}:{node_id}"]
            if task.terminal:
                continue
            if task.schedule_status == "running":
                self._set(task, now, cancel_requested=True, reason=reason)
            else:
                self._release(task, now)
                self._set(task, now, schedule_status="cancelled", cancel_requested=True,
                          finished_at=now, reason=reason)

    def _maintain(self, now: float) -> None:
        for flow in tuple(self._workflows.values()):
            if not flow.done and not flow.deadline_missed and flow.plan.deadline is not None and now > flow.plan.deadline:
                self._workflows[flow.plan.workflow_id] = replace(flow, deadline_missed=True, deadline_missed_at=now)
                self._event("deadline_miss", now, workflow_id=flow.plan.workflow_id, deadline=flow.plan.deadline)
        for task in tuple(self._tasks.values()):
            if task.schedule_status == "running" and not task.cancel_requested and (
                Fraction(str(now)) - Fraction(str(task.started_at)) >= Fraction(str(task.spec.timeout_seconds))
            ):
                self._set(task, now, cancel_requested=True, timeout_requested=True, reason="execution_timeout")
                self._event("cancel_request", now, task_id=task.task_id, workflow_id=task.workflow_id,
                            attempt=task.attempt, reason="execution_timeout")
        for flow in tuple(self._workflows.values()):
            for node_id in flow.topology:
                task = self._tasks[f"{flow.plan.workflow_id}:{node_id}"]
                if task.terminal or task.schedule_status in {"reserved", "running"}:
                    continue
                dependencies = [self._tasks[f"{task.workflow_id}:{dep}"] for dep in task.spec.depends_on]
                bad = next((dep for dep in dependencies if dep.terminal and not dep.successful), None)
                if bad is not None:
                    self._set(task, now, schedule_status="skipped", finished_at=now,
                              reason=f"dependency_not_successful:{bad.task_id}")
                elif all(dep.successful for dep in dependencies) and task.schedule_status == "pending":
                    self._set(task, now, schedule_status="ready", ready_at=now)
            self._refresh_workflow(flow.plan.workflow_id, now)

    def _refresh_workflow(self, workflow_id: str, now: float) -> None:
        flow = self._workflows[workflow_id]
        tasks = [self._tasks[f"{workflow_id}:{node_id}"] for node_id in flow.topology]
        done = all(task.terminal for task in tasks)
        if done:
            if flow.stop_reason is not None:
                status = "cancelled"
            elif all(task.successful for task in tasks):
                status = "completed"
            else:
                status = "failed"
        elif flow.stop_reason is not None:
            status = "cancelling"
        elif any(task.schedule_status in {"running", "reserved"} or task.attempt for task in tasks):
            status = "running"
        else:
            status = "pending"
        finished_at = (flow.finished_at if flow.done else now) if done else None
        updated = replace(flow, done=done, status=status, finished_at=finished_at)
        self._workflows[workflow_id] = updated
        if (flow.status, flow.done) != (status, done):
            self._event("workflow_state", now, workflow_id=workflow_id, status=status, done=done)

    def _age(self, task: _Task, now: float) -> int:
        origin = task.ready_at if task.ready_at is not None else self._workflows[task.workflow_id].submitted_at
        return int((Fraction(str(now)) - Fraction(str(origin))) // Fraction(str(self.aging_interval)))

    def _priority(self, task: _Task, now: float) -> int:
        return self._age(task, now) + task.spec.priority + self._workflows[task.workflow_id].plan.priority

    def _order_key(self, task: _Task, now: float, selection: Dict[str, Any]) -> Tuple[Any, ...]:
        flow = self._workflows[task.workflow_id]
        deadline = flow.plan.deadline if flow.plan.deadline is not None else math.inf
        tail = (flow.dispatched, flow.submitted_at, flow.order, task.order, task.task_id)
        if self.policy == "fcfs":
            return (flow.submitted_at, flow.order, task.order, task.task_id)
        if self.policy == "drc":
            return (-self._priority(task, now), *tail)
        if self.policy == "edf":
            return (-self._age(task, now), deadline, -self._priority(task, now), *tail)
        if self.policy == "heft":
            return (-self._age(task, now), -task.critical_path, -self._priority(task, now), *tail)
        slack = deadline - _plus(now, task.critical_path)
        return (-self._priority(task, now), slack, -task.critical_path,
                selection["estimated_finish"], *tail)

    def _candidate_options(self, task: _Task, now: float, resources: Dict[str, Any]) -> List[Dict[str, Any]]:
        flow = self._workflows[task.workflow_id]
        options = []
        for candidate in task.spec.candidates:
            reasons = []
            healthy = self._health.get(candidate.instance_id, candidate.healthy)
            used = self._instance_used.get(candidate.instance_id, 0)
            if candidate.instance_id in task.tried_instances:
                reasons.append("already_attempted")
            if not healthy:
                reasons.append("unhealthy")
            if candidate.cpu > self._pool.cpu_budget or candidate.memory_mb > self._pool.memory_budget:
                reasons.append("exceeds_budget")
            if used >= candidate.max_concurrency:
                reasons.append("instance_capacity")
            if resources["cpu_exact"] + Fraction(str(candidate.cpu)) > Fraction(str(self._pool.cpu_budget)):
                reasons.append("cpu_capacity")
            if resources["memory_exact"] + Fraction(str(candidate.memory_mb)) > Fraction(str(self._pool.memory_budget)):
                reasons.append("memory_capacity")
            if resources["concurrency"] >= self._pool.max_concurrency:
                reasons.append("global_concurrency")
            available_at = now
            if used >= candidate.max_concurrency:
                live = [t for t in self._tasks.values() if t.schedule_status in {"reserved", "running"}
                        and t.candidate.instance_id == candidate.instance_id]
                available_at = max(now, min(_plus(t.started_at if t.started_at is not None else t.reserved_at,
                                                _duration(t.candidate)) for t in live))
            eft = _plus(available_at, _duration(candidate))
            projected_finish = _plus(eft, max(0.0, task.critical_path - task.minimum_duration))
            options.append({
                "instance_id": candidate.instance_id, "implementation_id": candidate.implementation_id,
                "version": candidate.version, "healthy": healthy, "instance_used": used,
                "max_concurrency": candidate.max_concurrency, "cpu": candidate.cpu,
                "memory_mb": candidate.memory_mb, "expected_seconds": candidate.expected_seconds,
                "cold_start_seconds": candidate.cold_start_seconds, "adapter_seconds": candidate.adapter_seconds,
                "profile_seconds": _duration(candidate), "estimated_start": available_at,
                "estimated_finish": eft, "projected_workflow_finish": projected_finish,
                "deadline_feasible": flow.plan.deadline is None or projected_finish <= flow.plan.deadline,
                "feasible": not reasons, "reasons": reasons,
            })
        return options

    def tick(self, now: float) -> List[Assignment]:
        with self._lock:
            now = self._time(now)
            self._now = now
            self._maintain(now)
            assignments = []
            ledger = self._pool.snapshot()
            resources = dict(ledger["resources"])
            # Recover decimal amounts from public reservations, avoiding float
            # rounding in feasibility and repeated copying of the event history.
            resources["cpu_exact"] = sum((Fraction(str(r["cpu"])) for r in ledger["reservations"].values()), Fraction(0))
            resources["memory_exact"] = sum((Fraction(str(r["memory_mb"])) for r in ledger["reservations"].values()), Fraction(0))
            # Re-evaluate after each reservation: capacity and fairness change.
            while True:
                choices = []
                for task in tuple(self._tasks.values()):
                    if task.schedule_status not in {"ready", "blocked"}:
                        continue
                    options = self._candidate_options(task, now, resources)
                    feasible = [option for option in options if option["feasible"]]
                    if not feasible:
                        reason = "capacity_or_health"
                        if all("already_attempted" in o["reasons"] or "exceeds_budget" in o["reasons"] for o in options):
                            self._set(task, now, schedule_status="failed", finished_at=now, reason="candidates_exhausted")
                        elif task.schedule_status != "blocked":
                            self._set(task, now, schedule_status="blocked", reason=reason)
                        continue
                    chosen = min(feasible, key=lambda o: (
                        not o["deadline_feasible"] if self.policy == "joint" else False,
                        o["estimated_finish"], o["instance_id"],
                    ))
                    selection = {**chosen, "policy": self.policy, "critical_path_seconds": task.critical_path,
                                 "effective_priority": self._priority(task, now), "aging_steps": self._age(task, now),
                                 "deadline": self._workflows[task.workflow_id].plan.deadline,
                                 "deadline_slack_seconds": (
                                     self._workflows[task.workflow_id].plan.deadline - chosen["projected_workflow_finish"]
                                     if self._workflows[task.workflow_id].plan.deadline is not None else None),
                                 "workflow_dispatch_count": self._workflows[task.workflow_id].dispatched,
                                 "alternatives": options,
                                 "reason": "feasible instance with minimum EFT; profile includes execution, cold start and adapter"}
                    choices.append((self._order_key(task, now, selection), task, selection))
                if not choices:
                    break
                _, task, selection = min(choices, key=lambda choice: choice[0])
                candidate = next(c for c in task.spec.candidates if c.instance_id == selection["instance_id"])
                reservation = self._pool.reserve(task.task_id, candidate.cpu, candidate.memory_mb, now)
                if reservation is None:
                    raise RuntimeError("atomic admission ledger disagrees with feasibility")
                resources["cpu_exact"] += Fraction(str(reservation.cpu))
                resources["memory_exact"] += Fraction(str(reservation.memory_mb))
                resources["concurrency"] += 1
                self._instance_used[candidate.instance_id] = self._instance_used.get(candidate.instance_id, 0) + 1
                self._set(task, now, schedule_status="reserved", candidate=candidate, attempt=task.attempt + 1,
                          tried_instances=task.tried_instances + (candidate.instance_id,),
                          reservation_id=reservation.reservation_id, reserved_at=now, started_at=None,
                          finished_at=None, cancel_requested=False, timeout_requested=False,
                          call_status="not_called", quality_status="not_evaluated", reason=None,
                          error=None, result=None, selection=selection)
                flow = self._workflows[task.workflow_id]
                self._workflows[task.workflow_id] = replace(flow, dispatched=flow.dispatched + 1)
                self._event("selection", now, task_id=task.task_id, workflow_id=task.workflow_id,
                            attempt=task.attempt + 1, selection=selection)
                assignments.append(Assignment(task.task_id, task.workflow_id, task.spec.node_id,
                                              candidate.instance_id, reservation.reservation_id,
                                              task.attempt + 1, candidate, deepcopy(task.spec.parameters)))
            self._maintain(now)
            return assignments

    def start(self, task_id: str, now: float) -> Dict[str, Any]:
        _identifier(task_id, "task_id")
        with self._lock:
            now = self._time(now)
            task = self._tasks[task_id]
            if task.schedule_status != "reserved":
                raise RuntimeError(f"cannot start task in {task.schedule_status!r}")
            self._now = now
            self._maintain(now)
            task = self._tasks[task_id]
            if task.schedule_status != "reserved":
                raise RuntimeError("task was cancelled before launch")
            if not self._health.get(task.candidate.instance_id, task.candidate.healthy):
                raise RuntimeError("selected instance is unhealthy; confirm start_failed")
            self._pool.start(task.reservation_id, now)
            task = self._set(task, now, schedule_status="running", call_status="running", started_at=now)
            self._refresh_workflow(task.workflow_id, now)
            return self._task_dict(task)

    def _ended(self, task: _Task, now: float, call_status: str, quality_status: str,
               result: Any, error: Optional[str], launch_failed: bool = False) -> _Task:
        self._release(task, now, execution_finished=True)
        flow = self._workflows[task.workflow_id]
        if task.timeout_requested and flow.stop_reason is None:
            call_status, quality_status = "timeout", "not_evaluated"
        elif flow.stop_reason is not None:
            call_status, quality_status = "cancelled", "not_evaluated"
        success = call_status == "success" and quality_status == "valid"
        status = "completed" if success else ("start_failed" if launch_failed else "failed")
        if call_status == "cancelled":
            status = "cancelled"
        remaining = [c for c in task.spec.candidates if c.instance_id not in task.tried_instances
                     and c.cpu <= self._pool.cpu_budget and c.memory_mb <= self._pool.memory_budget]
        retry = (not success and call_status != "cancelled" and flow.stop_reason is None
                 and self.enable_recovery and flow.plan.options.get("enable_recovery", True)
                 and task.attempt < task.spec.max_attempts and bool(remaining))
        reason = task.reason
        if not success and flow.stop_reason is None:
            reason = "retry_pending" if retry else (
                "recovery_disabled" if not self.enable_recovery or not flow.plan.options.get("enable_recovery", True)
                else "max_attempts" if task.attempt >= task.spec.max_attempts else "candidates_exhausted")
        record = {"attempt": task.attempt, "instance_id": task.candidate.instance_id,
                  "reservation_id": task.reservation_id, "reserved_at": task.reserved_at,
                  "started_at": task.started_at, "finished_at": now, "call_status": call_status,
                  "quality_status": quality_status, "error": error, "launch_failed": launch_failed,
                  "selection": task.selection, "reason": task.reason}
        task = self._set(task, now, schedule_status="ready" if retry else status,
                         call_status=call_status, quality_status=quality_status, finished_at=None if retry else now,
                         result=result if success else None, error=error, reason=reason,
                         reservation_id=None, cancel_requested=False if retry else task.cancel_requested,
                         timeout_requested=False if retry else task.timeout_requested,
                         attempts=task.attempts + (record,))
        if retry:
            self._event("retry", now, task_id=task.task_id, workflow_id=task.workflow_id,
                        attempt=task.attempt, remaining_instances=[c.instance_id for c in remaining])
        self._maintain(now)
        return self._tasks[task.task_id]

    def finish(self, task_id: str, now: float, call_status: str = "success",
               quality_status: str = "valid", *, result: Any = None,
               error: Optional[str] = None) -> Dict[str, Any]:
        _identifier(task_id, "task_id")
        if not isinstance(call_status, str) or call_status not in _CALLS:
            raise ValueError("unsupported call_status")
        if not isinstance(quality_status, str) or quality_status not in _QUALITIES:
            raise ValueError("unsupported quality_status")
        _optional_text(error, "error")
        if call_status != "success" and quality_status == "valid":
            quality_status = "not_evaluated"
        result = deepcopy(result)
        with self._lock:
            now = self._time(now)
            task = self._tasks[task_id]
            if task.terminal or (task.attempts and task.schedule_status in {"ready", "blocked", "reserved"}):
                self._now = now
                self._maintain(now)
                return self._task_dict(self._tasks[task_id])
            if task.schedule_status != "running":
                raise RuntimeError(f"cannot finish task in {task.schedule_status!r}; use start_failed")
            self._now = now
            self._maintain(now)
            task = self._ended(self._tasks[task_id], now, call_status, quality_status, result, error)
            return self._task_dict(task)

    def start_failed(self, task_id: str, now: float, *, error: Optional[str] = None) -> Dict[str, Any]:
        """Confirm that launch failed and no executing work exists, even after start."""
        _identifier(task_id, "task_id")
        _optional_text(error, "error")
        with self._lock:
            now = self._time(now)
            task = self._tasks[task_id]
            if task.terminal or (task.attempts and task.schedule_status in {"ready", "blocked"}):
                self._now = now
                self._maintain(now)
                return self._task_dict(self._tasks[task_id])
            if task.schedule_status not in {"reserved", "running"}:
                raise RuntimeError(f"cannot report launch failure in {task.schedule_status!r}")
            self._now = now
            self._maintain(now)
            task = self._tasks[task_id]
            if task.terminal:
                return self._task_dict(task)
            return self._task_dict(self._ended(task, now, "failed", "not_evaluated", None, error, True))

    def cancel(self, workflow_id: str, now: float, *, reason: Optional[str] = None) -> Dict[str, Any]:
        _identifier(workflow_id, "workflow_id", local=True)
        _optional_text(reason, "reason")
        with self._lock:
            now = self._time(now)
            flow = self._workflows[workflow_id]
            self._now = now
            self._maintain(now)
            self._stop_workflow(self._workflows[flow.plan.workflow_id], now, reason or "cancelled")
            self._maintain(now)
            return self._workflow_dict(self._workflows[workflow_id])

    def set_instance_health(self, instance_id: str, healthy: bool, now: float) -> None:
        _identifier(instance_id, "instance_id")
        if not isinstance(healthy, bool):
            raise ValueError("healthy must be bool")
        with self._lock:
            now = self._time(now)
            self._now = now
            self._health[instance_id] = healthy
            self._event("instance_health", now, instance_id=instance_id, healthy=healthy)
            self._maintain(now)

    def _task_dict(self, task: _Task, include_results: bool = False) -> Dict[str, Any]:
        flow = self._workflows[task.workflow_id]
        value = {
            "task_id": task.task_id, "workflow_id": task.workflow_id, "node_id": task.spec.node_id,
            "capability_id": task.spec.capability_id, "depends_on": list(task.spec.depends_on),
            "schedule_status": task.schedule_status, "status": task.schedule_status,
            "call_status": task.call_status, "quality_status": task.quality_status,
            "instance_id": task.candidate.instance_id if task.candidate else None,
            "instance": task.candidate.instance_id if task.candidate else None,
            "candidate": asdict(task.candidate) if task.candidate else None,
            "attempt": task.attempt, "max_attempts": task.spec.max_attempts,
            "tried_instances": list(task.tried_instances), "reservation_id": task.reservation_id,
            "submitted_at": flow.submitted_at, "ready_at": task.ready_at,
            "reserved_at": task.reserved_at, "started_at": task.started_at, "finished_at": task.finished_at,
            "timeout_seconds": task.spec.timeout_seconds, "deadline": flow.plan.deadline,
            "deadline_missed": flow.plan.deadline is not None and (
                (task.finished_at if task.finished_at is not None else self._now) > flow.plan.deadline),
            "priority": task.spec.priority + flow.plan.priority,
            "effective_priority": self._priority(task, self._now), "critical_path_seconds": task.critical_path,
            "cancel_requested": task.cancel_requested, "timeout_requested": task.timeout_requested,
            "terminal": task.terminal, "successful": task.successful,
            "reason": task.reason, "error": task.error, "selection": task.selection,
            "selection_reason": task.selection, "attempts": list(task.attempts),
        }
        if include_results:
            value["result"] = task.result
        return deepcopy(value)

    def _workflow_dict(self, flow: _Workflow) -> Dict[str, Any]:
        return {"workflow_id": flow.plan.workflow_id, "app_id": flow.plan.app_id,
                "template": flow.plan.template, "status": flow.status, "done": flow.done,
                "submitted_at": flow.submitted_at, "created_at": flow.plan.created_at,
                "deadline": flow.plan.deadline, "finished_at": flow.finished_at,
                "deadline_missed": flow.deadline_missed, "deadline_missed_at": flow.deadline_missed_at,
                "finished": flow.done, "priority": flow.plan.priority,
                "cancel_requested": flow.stop_reason is not None, "reason": flow.stop_reason,
                "dispatch_count": flow.dispatched,
                "task_ids": [f"{flow.plan.workflow_id}:{node_id}" for node_id in flow.topology]}

    def snapshot(self, workflow_id: Optional[str] = None, include_results: bool = False) -> Dict[str, Any]:
        if workflow_id is not None:
            _identifier(workflow_id, "workflow_id", local=True)
        if not isinstance(include_results, bool):
            raise ValueError("include_results must be bool")
        with self._lock:
            if workflow_id is not None and workflow_id not in self._workflows:
                raise KeyError(workflow_id)
            flows = {key: self._workflow_dict(flow) for key, flow in self._workflows.items()
                     if workflow_id is None or key == workflow_id}
            ledger = self._pool.snapshot()
            return {"now": self._now, "policy": self.policy, "aging_interval": self.aging_interval,
                    "enable_recovery": self.enable_recovery,
                    "done": all(flow["done"] for flow in flows.values()),
                    "workflows": flows,
                    "tasks": {key: self._task_dict(task, include_results) for key, task in self._tasks.items()
                              if workflow_id is None or task.workflow_id == workflow_id},
                    "instances": {key: {"max_concurrency": limit,
                                        "concurrency": self._instance_used.get(key, 0),
                                        "healthy_override": self._health.get(key)}
                                  for key, limit in self._instance_limits.items()},
                    "events": deepcopy(self._events), "task_events": deepcopy(self._events), **ledger}
