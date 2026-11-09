"""Deterministic, thread-safe scheduling for numeric DAG workflows.

The executor owns process/coroutine lifetimes. ``tick`` only reserves resources;
call ``start`` before launching work and ``finish`` after it has actually stopped.
A timeout signal or a running-task cancellation never reclaims a reservation.
``start_failed`` explicitly confirms that launch failed and no work is alive.

Times are finite, nonnegative, and nondecreasing within a Scheduler. DRC orders
feasible tasks by descending floor((now - enqueue_time) / aging_interval) +
priority, then enqueue time and task_id. FCFS scans feasible tasks in enqueue
order with exactly the same dependency, resource, and timeout rules.
This module uses only the standard library and supports Python 3.9+.
"""

from __future__ import annotations

from collections import deque
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from fractions import Fraction
import math
from numbers import Real
from threading import RLock
from typing import Any, Dict, Iterable, List, Optional, Tuple

__all__ = ["TaskSpec", "TaskState", "Reservation", "ResourcePool", "Scheduler"]


def _number(value: Real, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, Real):
        raise ValueError(f"{name} must be a finite number")
    try:
        number = float(value)
    except (OverflowError, ValueError) as exc:
        raise ValueError(f"{name} must be finite") from exc
    if not math.isfinite(number) or number < 0 or (positive and number == 0):
        raise ValueError(f"{name} must be finite and {'positive' if positive else 'nonnegative'}")
    return number


def _integer(value: int, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{name} must be an integer >= {minimum}")
    return value


def _amount(value: float) -> Fraction:
    # Exact decimal accounting avoids epsilon allowances and cumulative drift.
    return Fraction(str(value))


@dataclass(frozen=True)
class TaskSpec:
    task_id: str
    depends_on: Tuple[str, ...]
    cpu: float
    memory_mb: float
    priority: int = 0
    enqueue_time: float = 0
    resource_timeout: float = 30

    def __post_init__(self) -> None:
        if not isinstance(self.task_id, str) or not self.task_id.strip():
            raise ValueError("task_id must be a nonempty string")
        if not isinstance(self.depends_on, tuple) or any(
            not isinstance(dep, str) or not dep.strip() for dep in self.depends_on
        ):
            raise ValueError("depends_on must be a tuple of nonempty task ids")
        if len(set(self.depends_on)) != len(self.depends_on):
            raise ValueError("duplicate dependency")
        _integer(self.priority, "priority")
        for name in ("cpu", "memory_mb", "enqueue_time", "resource_timeout"):
            object.__setattr__(self, name, _number(getattr(self, name), name))


@dataclass(frozen=True)
class TaskState:
    spec: TaskSpec
    schedule_status: str = "pending"
    call_status: str = "not_called"
    quality_status: str = "not_evaluated"
    reservation_id: Optional[str] = None
    effective_priority: int = 0
    blocked_since: Optional[float] = None
    reserved_at: Optional[float] = None
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    cancel_requested: bool = False
    reason: Optional[str] = None
    error: Optional[str] = None
    result: Any = None

    @property
    def task_id(self) -> str:
        return self.spec.task_id

    @property
    def successful(self) -> bool:
        return (
            self.schedule_status == "completed"
            and self.call_status == "success"
            and self.quality_status == "valid"
        )

    @property
    def terminal(self) -> bool:
        return self.schedule_status in {
            "completed", "cancelled", "skipped", "resource_timeout", "start_failed"
        }

    def to_dict(self) -> Dict[str, Any]:
        value = asdict(self)
        value.update(value["spec"])
        value["depends_on"] = list(self.spec.depends_on)
        value["terminal"] = self.terminal
        value["successful"] = self.successful
        return value


@dataclass(frozen=True)
class Reservation:
    reservation_id: str
    task_id: str
    cpu: float
    memory_mb: float
    reserved_at: float
    running: bool = False
    started_at: Optional[float] = None


class ResourcePool:
    """Atomic CPU, memory and concurrency ledger.

    ``release(..., execution_finished=True)`` is an executor confirmation, not
    a timer: the caller asserts no executing work remains. Without it a running
    reservation cannot be released. Releasing a known released id returns False.
    Reservations that never started may be cancelled without that confirmation.
    """

    def __init__(
        self, cpu_budget: float, memory_budget: float, max_concurrency: int,
        *, _lock: Any = None,
    ) -> None:
        self.cpu_budget = _number(cpu_budget, "cpu_budget")
        self.memory_budget = _number(memory_budget, "memory_budget")
        self.max_concurrency = _integer(max_concurrency, "max_concurrency", minimum=1)
        self._lock = _lock if _lock is not None else RLock()
        self._cpu_budget = _amount(self.cpu_budget)
        self._memory_budget = _amount(self.memory_budget)
        self._cpu = Fraction(0)
        self._memory = Fraction(0)
        self._peak_cpu = Fraction(0)
        self._peak_memory = Fraction(0)
        self._peak_concurrency = 0
        self._reservations: Dict[str, Reservation] = {}
        self._released = set()
        self._sequence = 0
        self._events: List[Dict[str, Any]] = []

    def _event(self, event: str, reservation: Reservation, now: float, **extra: Any) -> None:
        self._events.append({
            "event": event, "now": now, "task_id": reservation.task_id,
            "reservation_id": reservation.reservation_id,
            "cpu": reservation.cpu, "memory_mb": reservation.memory_mb,
            "cpu_used": float(self._cpu), "memory_used_mb": float(self._memory),
            "concurrency": len(self._reservations), **extra,
        })

    def reserve(
        self, task_id: str, cpu: float, memory_mb: float, now: float,
    ) -> Optional[Reservation]:
        if not isinstance(task_id, str) or not task_id.strip():
            raise ValueError("task_id must be a nonempty string")
        cpu = _number(cpu, "cpu")
        memory_mb = _number(memory_mb, "memory_mb")
        now = _number(now, "now")
        cpu_amount, memory_amount = _amount(cpu), _amount(memory_mb)
        if cpu_amount > self._cpu_budget or memory_amount > self._memory_budget:
            raise ValueError(f"task {task_id!r} permanently exceeds resource budget")
        with self._lock:
            if any(r.task_id == task_id for r in self._reservations.values()):
                raise ValueError(f"task {task_id!r} already has a reservation")
            if (
                self._cpu + cpu_amount > self._cpu_budget
                or self._memory + memory_amount > self._memory_budget
                or len(self._reservations) >= self.max_concurrency
            ):
                return None
            self._sequence += 1
            reservation = Reservation(
                f"reservation-{self._sequence:08d}", task_id, cpu, memory_mb, now
            )
            self._reservations[reservation.reservation_id] = reservation
            self._cpu += cpu_amount
            self._memory += memory_amount
            self._peak_cpu = max(self._peak_cpu, self._cpu)
            self._peak_memory = max(self._peak_memory, self._memory)
            self._peak_concurrency = max(self._peak_concurrency, len(self._reservations))
            self._event("reserve", reservation, now)
            return reservation

    def start(self, reservation_id: str, now: float) -> Reservation:
        now = _number(now, "now")
        with self._lock:
            reservation = self._reservations[reservation_id]
            if reservation.running:
                raise RuntimeError("reservation already started")
            if now < reservation.reserved_at:
                raise ValueError("start precedes reservation")
            reservation = replace(reservation, running=True, started_at=now)
            self._reservations[reservation_id] = reservation
            self._event("start", reservation, now)
            return reservation

    def release(
        self, reservation_id: str, now: float, *, execution_finished: bool = False,
    ) -> bool:
        now = _number(now, "now")
        if not isinstance(execution_finished, bool):
            raise ValueError("execution_finished must be bool")
        with self._lock:
            if reservation_id in self._released:
                return False
            reservation = self._reservations[reservation_id]
            if now < reservation.reserved_at:
                raise ValueError("release precedes reservation")
            if reservation.started_at is not None and now < reservation.started_at:
                raise ValueError("release precedes execution start")
            if reservation.running and not execution_finished:
                raise RuntimeError("executor must confirm execution has finished")
            del self._reservations[reservation_id]
            self._released.add(reservation_id)
            self._cpu -= _amount(reservation.cpu)
            self._memory -= _amount(reservation.memory_mb)
            self._event("release", reservation, now, execution_finished=execution_finished)
            return True

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            peaks = {
                "cpu_used": float(self._peak_cpu),
                "memory_used_mb": float(self._peak_memory),
                "concurrency": self._peak_concurrency,
            }
            return {
                "resources": {
                    "cpu_budget": self.cpu_budget, "memory_budget": self.memory_budget,
                    "max_concurrency": self.max_concurrency,
                    "cpu_used": float(self._cpu), "memory_used_mb": float(self._memory),
                    "concurrency": len(self._reservations), "peaks": peaks,
                },
                "peaks": dict(peaks),
                "reservations": {key: asdict(r) for key, r in self._reservations.items()},
                "resource_events": deepcopy(self._events),
            }


class Scheduler:
    def __init__(
        self, tasks: Iterable[TaskSpec], cpu_budget: float, memory_budget: float,
        max_concurrency: int, policy: str = "drc", aging_interval: float = 30,
    ) -> None:
        if policy not in ("drc", "fcfs"):
            raise ValueError("policy must be 'drc' or 'fcfs'")
        self.policy = policy
        self.aging_interval = _number(aging_interval, "aging_interval", positive=True)
        self._lock = RLock()
        self._pool = ResourcePool(cpu_budget, memory_budget, max_concurrency, _lock=self._lock)
        self._states: Dict[str, TaskState] = {}
        self._now: Optional[float] = None
        self._events: List[Dict[str, Any]] = []
        for task in tasks:
            if not isinstance(task, TaskSpec):
                raise ValueError("tasks must contain TaskSpec instances")
            if task.task_id in self._states:
                raise ValueError(f"duplicate task_id: {task.task_id}")
            if task.cpu > self._pool.cpu_budget or task.memory_mb > self._pool.memory_budget:
                raise ValueError(f"task {task.task_id!r} permanently exceeds resource budget")
            self._states[task.task_id] = TaskState(task, effective_priority=task.priority)
        self._order = {task_id: i for i, task_id in enumerate(self._states)}
        self._topology = self._validate_dag()

    def _validate_dag(self) -> List[str]:
        indegree = {}
        successors: Dict[str, List[str]] = {task_id: [] for task_id in self._states}
        for task_id, state in self._states.items():
            indegree[task_id] = len(state.spec.depends_on)
            for dependency in state.spec.depends_on:
                if dependency not in self._states:
                    raise ValueError(f"missing dependency {dependency!r} for {task_id!r}")
                successors[dependency].append(task_id)
        ready = deque(task_id for task_id, count in indegree.items() if count == 0)
        topology = []
        while ready:
            task_id = ready.popleft()
            topology.append(task_id)
            for successor in successors[task_id]:
                indegree[successor] -= 1
                if indegree[successor] == 0:
                    ready.append(successor)
        if len(topology) != len(self._states):
            raise ValueError("dependency cycle")
        return topology

    def _time(self, now: float) -> float:
        now = _number(now, "now")
        if self._now is not None and now < self._now:
            raise ValueError("scheduler time must be nondecreasing")
        return now

    def _set(self, task_id: str, now: float, **changes: Any) -> TaskState:
        state = replace(self._states[task_id], **changes)
        self._states[task_id] = state
        if any(key in changes for key in (
            "schedule_status", "call_status", "quality_status", "cancel_requested"
        )):
            self._events.append({
                "now": now, "task_id": task_id,
                "schedule_status": state.schedule_status, "call_status": state.call_status,
                "quality_status": state.quality_status,
                "reservation_id": state.reservation_id,
                "cancel_requested": state.cancel_requested, "reason": state.reason,
            })
        return state

    def _dependencies(self, now: float) -> None:
        # Topological order propagates failure through arbitrarily deep DAGs
        # without recursion, including descendants not yet enqueued.
        for task_id in self._topology:
            state = self._states[task_id]
            if state.terminal or state.schedule_status in ("reserved", "running"):
                continue
            dependencies = [self._states[dep] for dep in state.spec.depends_on]
            bad = next((dep for dep in dependencies if dep.terminal and not dep.successful), None)
            if bad is not None:
                self._set(task_id, now, schedule_status="skipped", finished_at=now,
                          reason=f"dependency_not_successful:{bad.task_id}")
            elif (
                state.schedule_status == "pending" and now >= state.spec.enqueue_time
                and all(dep.successful for dep in dependencies)
            ):
                self._set(task_id, now, schedule_status="ready")

    def _priority(self, spec: TaskSpec, now: float) -> int:
        return (
            (_amount(now) - _amount(spec.enqueue_time)) // _amount(self.aging_interval)
            + spec.priority
        )

    def tick(self, now: float) -> List[TaskState]:
        """Return newly reserved tasks; repeated ticks cannot dispatch them twice."""
        with self._lock:
            now = self._time(now)
            self._now = now
            self._dependencies(now)
            for task_id, state in tuple(self._states.items()):
                if (
                    state.schedule_status == "blocked" and state.blocked_since is not None
                    and _amount(now) - _amount(state.blocked_since) >= _amount(state.spec.resource_timeout)
                ):
                    self._set(task_id, now, schedule_status="resource_timeout",
                              finished_at=now, reason="resource_timeout")
            self._dependencies(now)
            candidates = [s for s in self._states.values() if s.schedule_status in ("ready", "blocked")]
            if self.policy == "drc":
                candidates.sort(key=lambda s: (
                    -self._priority(s.spec, now), s.spec.enqueue_time, s.task_id
                ))
            else:
                candidates.sort(key=lambda s: (s.spec.enqueue_time, self._order[s.task_id]))
            reserved = []
            for candidate in candidates:
                task_id, spec = candidate.task_id, candidate.spec
                priority = self._priority(spec, now)
                reservation = self._pool.reserve(task_id, spec.cpu, spec.memory_mb, now)
                if reservation is not None:
                    state = self._set(
                        task_id, now, schedule_status="reserved", reserved_at=now,
                        reservation_id=reservation.reservation_id, effective_priority=priority,
                    )
                    reserved.append(deepcopy(state))
                else:
                    first_blocked = candidate.blocked_since
                    if first_blocked is None:
                        first_blocked = now
                    changes = {"blocked_since": first_blocked, "effective_priority": priority}
                    if candidate.schedule_status != "blocked":
                        changes["schedule_status"] = "blocked"
                    self._set(task_id, now, **changes)
                    if spec.resource_timeout == 0:
                        self._set(task_id, now, schedule_status="resource_timeout",
                                  finished_at=now, reason="resource_timeout")
            self._dependencies(now)
            return reserved

    def start(self, task_id: str, now: float) -> TaskState:
        """Claim execution once, before launching the process/coroutine."""
        with self._lock:
            now = self._time(now)
            state = self._states[task_id]
            if state.schedule_status != "reserved":
                raise RuntimeError(f"cannot start task in {state.schedule_status!r}")
            self._pool.start(state.reservation_id, now)
            self._now = now
            return deepcopy(self._set(task_id, now, schedule_status="running",
                                      call_status="running", started_at=now))

    def finish(
        self, task_id: str, now: float, call_status: str = "success",
        quality_status: str = "valid", *, result: Any = None, error: Optional[str] = None,
    ) -> TaskState:
        """Confirm actual execution termination, record outcome, and release once.

        call_status: success/failed/timeout/cancelled. quality_status:
        valid/invalid/degraded/not_evaluated. Failed calls cannot have valid output.
        A cancellation request can lose a race to a successful completion; pass
        call_status='cancelled' when the executor confirms cancellation instead.
        Duplicate terminal notifications return the original recorded outcome.
        """
        if not isinstance(call_status, str) or call_status not in {"success", "failed", "timeout", "cancelled"}:
            raise ValueError("unsupported call_status")
        if not isinstance(quality_status, str) or quality_status not in {"valid", "invalid", "degraded", "not_evaluated"}:
            raise ValueError("unsupported quality_status")
        if call_status != "success" and quality_status == "valid":
            quality_status = "not_evaluated"
        result = deepcopy(result)
        with self._lock:
            now = self._time(now)
            state = self._states[task_id]
            if state.terminal and state.started_at is not None:
                return deepcopy(state)
            if state.schedule_status != "running":
                raise RuntimeError(f"cannot finish task in {state.schedule_status!r}; use start_failed for launch failure")
            self._pool.release(state.reservation_id, now, execution_finished=True)
            self._now = now
            state = self._set(
                task_id, now,
                schedule_status="cancelled" if call_status == "cancelled" else "completed",
                call_status=call_status, quality_status=quality_status, finished_at=now,
                result=result, error=error,
            )
            self._dependencies(now)
            return deepcopy(state)

    def start_failed(self, task_id: str, now: float, *, error: Optional[str] = None) -> TaskState:
        """Executor confirms failed launch: no process/coroutine is executing.

        Valid after reservation or after the start claim when spawning failed.
        This confirmation releases the reservation; it must never be used to
        report a timeout or an error while background work is still alive.
        """
        with self._lock:
            now = self._time(now)
            state = self._states[task_id]
            if state.schedule_status == "start_failed":
                return deepcopy(state)
            if state.schedule_status not in ("reserved", "running"):
                raise RuntimeError(f"cannot report launch failure in {state.schedule_status!r}")
            self._pool.release(state.reservation_id, now, execution_finished=True)
            self._now = now
            state = self._set(task_id, now, schedule_status="start_failed", call_status="failed",
                              finished_at=now, error=error, reason="start_failed")
            self._dependencies(now)
            return deepcopy(state)

    def cancel(self, task_id: str, now: float, *, reason: Optional[str] = None) -> TaskState:
        """Cancel unstarted work; running work keeps resources until finish."""
        with self._lock:
            now = self._time(now)
            state = self._states[task_id]
            if state.terminal:
                return deepcopy(state)
            self._now = now
            if state.schedule_status == "running":
                if not state.cancel_requested:
                    state = self._set(task_id, now, cancel_requested=True,
                                      reason=reason or "cancel_requested")
                return deepcopy(state)
            if state.reservation_id is not None:
                self._pool.release(state.reservation_id, now)
            state = self._set(task_id, now, schedule_status="cancelled", finished_at=now,
                              cancel_requested=True, reason=reason or "cancelled")
            self._dependencies(now)
            return deepcopy(state)

    def snapshot(self) -> Dict[str, Any]:
        """Return an isolated snapshot with states, resource events and peaks."""
        with self._lock:
            return {
                "now": self._now, "policy": self.policy, "aging_interval": self.aging_interval,
                "tasks": {key: state.to_dict() for key, state in self._states.items()},
                "task_events": deepcopy(self._events),
                "done": all(state.terminal for state in self._states.values()),
                **self._pool.snapshot(),
            }
