"""Scheduler contracts, actual contention and executor-lifetime fault tests."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from itertools import permutations
import json
import subprocess
import sys
from threading import Barrier

import pytest

from src.graph.traffic_scheduler import ResourcePool, Scheduler, TaskSpec


def task(task_id, depends_on=(), cpu=1, memory_mb=1, **kwargs):
    return TaskSpec(task_id, depends_on, cpu, memory_mb, **kwargs)


def ids(states):
    return [state.task_id for state in states]


def assert_ledger(snapshot):
    resources = snapshot["resources"]
    for value in [resources, snapshot["peaks"], *snapshot["resource_events"]]:
        assert 0 <= value["cpu_used"] <= resources["cpu_budget"]
        assert 0 <= value["memory_used_mb"] <= resources["memory_budget"]
        assert 0 <= value["concurrency"] <= resources["max_concurrency"]
    live = snapshot["reservations"].values()
    assert resources["concurrency"] == len(snapshot["reservations"])
    assert resources["cpu_used"] == pytest.approx(sum(r["cpu"] for r in live))
    assert resources["memory_used_mb"] == pytest.approx(sum(r["memory_mb"] for r in live))


@pytest.mark.parametrize("field", ["cpu", "memory_mb", "enqueue_time", "resource_timeout"])
@pytest.mark.parametrize("value", [-1, float("nan"), float("inf"), -float("inf"), True, "1"])
def test_reject_invalid_task_numbers(field, value):
    values = dict(task_id="x", depends_on=(), cpu=1, memory_mb=1)
    values[field] = value
    with pytest.raises(ValueError):
        TaskSpec(**values)


@pytest.mark.parametrize("values", [
    {"task_id": ""}, {"task_id": " "}, {"task_id": 3},
    {"depends_on": ["x"]}, {"depends_on": ("",)}, {"depends_on": (3,)},
    {"depends_on": ("a", "a")}, {"priority": -1}, {"priority": 0.5},
    {"priority": float("nan")}, {"priority": True},
])
def test_reject_invalid_task_shape(values):
    kwargs = dict(task_id="x", depends_on=(), cpu=1, memory_mb=1)
    kwargs.update(values)
    with pytest.raises(ValueError):
        TaskSpec(**kwargs)


@pytest.mark.parametrize("values", [
    {"cpu_budget": -1}, {"cpu_budget": float("nan")},
    {"memory_budget": -1}, {"memory_budget": float("inf")},
    {"max_concurrency": 0}, {"max_concurrency": -1},
    {"max_concurrency": 1.5}, {"max_concurrency": True},
    {"max_concurrency": float("nan")}, {"aging_interval": 0},
    {"aging_interval": -1}, {"aging_interval": float("nan")},
    {"policy": "unknown"},
])
def test_reject_invalid_scheduler_settings(values):
    kwargs = dict(cpu_budget=2, memory_budget=2, max_concurrency=2)
    kwargs.update(values)
    with pytest.raises(ValueError):
        Scheduler([], **kwargs)


@pytest.mark.parametrize("tasks, message", [
    ([task("x"), task("x")], "duplicate task_id"),
    ([task("x", ("absent",))], "missing dependency"),
    ([task("x", ("x",))], "cycle"),
    ([task("x", ("y",)), task("y", ("x",))], "cycle"),
    ([task("ok"), task("x", ("y",)), task("y", ("x",))], "cycle"),
    ([task("x", cpu=3)], "permanently exceeds"),
    ([task("x", memory_mb=3)], "permanently exceeds"),
    ([{"task_id": "x"}], "TaskSpec"),
])
def test_reject_invalid_workflow(tasks, message):
    with pytest.raises(ValueError, match=message):
        Scheduler(tasks, 2, 2, 2)


@pytest.mark.parametrize("policy", ["drc", "fcfs"])
def test_diamond_dag_only_readies_after_valid_completion(policy):
    scheduler = Scheduler([
        task("join", ("left", "right")), task("left", ("root",)),
        task("right", ("root",)), task("root"),
    ], 2, 2, 2, policy)
    assert ids(scheduler.tick(0)) == ["root"]
    assert scheduler.tick(0) == []
    scheduler.start("root", 0)
    assert scheduler.tick(1) == []
    scheduler.finish("root", 1)
    assert ids(scheduler.tick(1)) == ["left", "right"]
    scheduler.start("left", 1)
    scheduler.start("right", 1)
    scheduler.finish("left", 2)
    assert scheduler.tick(2) == []
    scheduler.finish("right", 3)
    assert ids(scheduler.tick(3)) == ["join"]
    scheduler.start("join", 3)
    scheduler.finish("join", 4)
    snapshot = scheduler.snapshot()
    assert snapshot["done"]
    assert snapshot["resources"]["concurrency"] == 0
    assert all(state["successful"] for state in snapshot["tasks"].values())
    assert_ledger(snapshot)


@pytest.mark.parametrize("policy", ["drc", "fcfs"])
@pytest.mark.parametrize("call, quality", [
    ("success", "invalid"), ("success", "degraded"),
    ("success", "not_evaluated"), ("failed", "valid"),
    ("timeout", "not_evaluated"), ("cancelled", "not_evaluated"),
])
def test_bad_outcome_skips_transitive_successors_and_isolates_independent_work(policy, call, quality):
    scheduler = Scheduler([
        task("grandchild", ("child",), enqueue_time=1000),
        task("child", ("root",)), task("root"), task("independent"),
    ], 2, 2, 2, policy)
    assert set(ids(scheduler.tick(0))) == {"root", "independent"}
    scheduler.start("root", 0)
    scheduler.start("independent", 0)
    outcome = scheduler.finish("root", 1, call, quality, error="injected fault")
    assert not outcome.successful
    if call != "success":
        assert outcome.quality_status == "not_evaluated"
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["child"]["schedule_status"] == "skipped"
    assert snapshot["tasks"]["grandchild"]["schedule_status"] == "skipped"
    assert snapshot["tasks"]["child"]["call_status"] == "not_called"
    assert snapshot["resources"]["concurrency"] == 1
    scheduler.finish("independent", 2)
    assert scheduler.tick(3) == []
    assert scheduler.snapshot()["done"]
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("policy", ["drc", "fcfs"])
def test_feasible_scan_checks_cpu_memory_and_k_without_head_of_line_blocking(policy):
    scheduler = Scheduler([
        task("holder", cpu=2, memory_mb=2, priority=3),
        task("cpu_blocked", cpu=3, memory_mb=1, priority=2),
        task("memory_blocked", cpu=1, memory_mb=3, priority=1),
        task("fits", cpu=1, memory_mb=1),
        task("k_blocked", cpu=0, memory_mb=0),
    ], 4, 4, 2, policy)
    assert ids(scheduler.tick(0)) == ["holder", "fits"]
    states = scheduler.snapshot()["tasks"]
    assert all(states[key]["schedule_status"] == "blocked" for key in (
        "cpu_blocked", "memory_blocked", "k_blocked"
    ))
    scheduler.start_failed("holder", 1)
    assert ids(scheduler.tick(1)) == ["cpu_blocked"]
    assert_ledger(scheduler.snapshot())


def test_drc_aging_floor_base_priority_and_stable_ties():
    specifications = [
        task("old", enqueue_time=0), task("new", priority=2, enqueue_time=20),
        task("tie_first", enqueue_time=0),
    ]
    early = Scheduler(specifications, 1, 1, 1, aging_interval=10)
    assert ids(early.tick(20)) == ["old"]
    assert early.snapshot()["tasks"]["old"]["effective_priority"] == 2
    late = Scheduler([
        task("old", enqueue_time=0), task("new", priority=2, enqueue_time=19),
    ], 1, 1, 1, aging_interval=10)
    assert ids(late.tick(19)) == ["new"]
    boundary = Scheduler([
        task("old", enqueue_time=0), task("new", priority=2, enqueue_time=19),
    ], 1, 1, 1, aging_interval=10)
    assert ids(boundary.tick(20)) == ["old"]


@pytest.mark.parametrize("input_ids", list(permutations(("zulu", "alpha", "middle"))))
@pytest.mark.parametrize("policy", ["drc", "fcfs"])
def test_equal_score_equal_enqueue_ties_use_task_id_for_drc_and_input_order_for_fcfs(input_ids, policy):
    specifications = [task(task_id, priority=3, enqueue_time=7) for task_id in input_ids]
    scheduler = Scheduler(specifications, 3, 3, 3, policy, aging_interval=10)
    dispatched = scheduler.tick(27)
    expected = sorted(input_ids) if policy == "drc" else list(input_ids)
    assert ids(dispatched) == expected
    assert {state.effective_priority for state in dispatched} == {5}
    limited = Scheduler(specifications, 3, 3, 1, policy, aging_interval=10)
    assert ids(limited.tick(27)) == expected[:1]


def test_fcfs_ignores_priority_and_respects_enqueue_time():
    specifications = [task("new", priority=100, enqueue_time=1), task("old")]
    assert ids(Scheduler(specifications, 1, 1, 1, "drc").tick(1)) == ["new"]
    assert ids(Scheduler(specifications, 1, 1, 1, "fcfs").tick(1)) == ["old"]
    future = Scheduler([task("future", enqueue_time=10)], 1, 1, 1)
    assert future.tick(9) == []
    assert future.snapshot()["tasks"]["future"]["blocked_since"] is None
    assert ids(future.tick(10)) == ["future"]


@pytest.mark.parametrize("policy", ["drc", "fcfs"])
def test_timeout_starts_at_first_resource_block_and_never_resets(policy):
    scheduler = Scheduler([
        task("holder", priority=20, resource_timeout=500),
        task("waiting", ("holder",), enqueue_time=1, resource_timeout=3),
        task("competitor", priority=10, resource_timeout=500),
        task("downstream", ("waiting",)),
    ], 1, 1, 1, policy)
    scheduler.tick(0)
    scheduler.start("holder", 0)
    assert scheduler.tick(99) == []
    assert scheduler.snapshot()["tasks"]["waiting"]["schedule_status"] == "pending"
    # Competitor reserves after the dependency finishes; waiting is only blocked
    # now, not during the 100 units spent waiting for its predecessor.
    scheduler.finish("holder", 100)
    assert ids(scheduler.tick(100)) == ["competitor"]
    scheduler.start("competitor", 100)
    assert scheduler.snapshot()["tasks"]["waiting"]["blocked_since"] == 100
    assert scheduler.tick(102.999) == []
    assert scheduler.snapshot()["tasks"]["waiting"]["blocked_since"] == 100
    assert scheduler.tick(103) == []
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["waiting"]["schedule_status"] == "resource_timeout"
    assert snapshot["tasks"]["downstream"]["schedule_status"] == "skipped"
    assert snapshot["resources"]["concurrency"] == 1


@pytest.mark.parametrize("policy", ["drc", "fcfs"])
def test_blocked_deadline_is_inclusive_even_when_resources_free_at_deadline(policy):
    scheduler = Scheduler([task("holder"), task("waiting", resource_timeout=3)], 1, 1, 1, policy)
    scheduler.tick(10)
    scheduler.start("holder", 10)
    scheduler.tick(12)
    scheduler.finish("holder", 13)
    assert scheduler.tick(13) == []
    assert scheduler.snapshot()["tasks"]["waiting"]["finished_at"] == 13
    assert scheduler.snapshot()["tasks"]["waiting"]["blocked_since"] == 10


@pytest.mark.parametrize("policy", ["drc", "fcfs"])
def test_k_full_alone_is_resource_blocking_and_starts_resource_deadline(policy):
    scheduler = Scheduler([
        task("holder", priority=1), task("waiting", resource_timeout=2),
    ], 10, 10, 1, policy)
    assert ids(scheduler.tick(5)) == ["holder"]
    scheduler.start("holder", 5)
    snapshot = scheduler.snapshot()
    assert snapshot["resources"]["cpu_used"] == 1
    assert snapshot["resources"]["memory_used_mb"] == 1
    assert snapshot["resources"]["concurrency"] == 1
    assert snapshot["tasks"]["waiting"]["schedule_status"] == "blocked"
    assert snapshot["tasks"]["waiting"]["blocked_since"] == 5
    assert scheduler.tick(6) == []
    assert scheduler.snapshot()["tasks"]["waiting"]["blocked_since"] == 5
    assert scheduler.tick(7) == []
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["waiting"]["schedule_status"] == "resource_timeout"
    assert snapshot["resources"]["concurrency"] == 1
    assert not any(event["event"] == "release" for event in snapshot["resource_events"])
    scheduler.finish("holder", 8)
    assert scheduler.snapshot()["done"]
    assert_ledger(scheduler.snapshot())


def test_zero_timeout_and_zero_resources_still_obey_k():
    scheduler = Scheduler([
        task("first", cpu=0, memory_mb=0),
        task("second", cpu=0, memory_mb=0, resource_timeout=0),
        task("child", ("second",), cpu=0, memory_mb=0),
    ], 0, 0, 1)
    assert ids(scheduler.tick(0)) == ["first"]
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["second"]["schedule_status"] == "resource_timeout"
    assert snapshot["tasks"]["second"]["blocked_since"] == 0
    assert snapshot["tasks"]["child"]["schedule_status"] == "skipped"
    second_events = [event["schedule_status"] for event in snapshot["task_events"] if event["task_id"] == "second"]
    assert second_events == ["ready", "blocked", "resource_timeout"]
    assert_ledger(snapshot)


@pytest.mark.parametrize("claimed", [False, True])
def test_launch_failure_releases_once_and_skips_successor(claimed):
    scheduler = Scheduler([task("root", priority=1), task("child", ("root",)), task("other")], 1, 1, 1)
    reservation = scheduler.tick(0)[0]
    assert reservation.reservation_id
    if claimed:
        scheduler.start("root", 0)
    state = scheduler.start_failed("root", 1, error="spawn failed")
    assert state.schedule_status == "start_failed"
    assert state.call_status == "failed"
    assert scheduler.start_failed("root", 1) == state
    assert scheduler.snapshot()["tasks"]["child"]["schedule_status"] == "skipped"
    assert ids(scheduler.tick(1)) == ["other"]
    events = scheduler.snapshot()["resource_events"]
    assert sum(e["event"] == "release" and e["task_id"] == "root" for e in events) == 1
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("phase", ["pending", "ready", "blocked", "reserved"])
def test_cancel_unstarted_work_is_terminal_and_releases_only_its_reservation(phase):
    if phase == "pending":
        specifications = [task("x", enqueue_time=100), task("child", ("x",))]
    elif phase in ("ready", "blocked"):
        specifications = [task("holder"), task("x"), task("child", ("x",))]
    else:
        specifications = [task("x"), task("child", ("x",))]
    scheduler = Scheduler(specifications, 1, 1, 1)
    if phase != "pending":
        scheduler.tick(0)
    if phase == "ready":
        # READY is externally observable immediately when a dependency finishes.
        scheduler = Scheduler([task("root"), task("x", ("root",)), task("child", ("x",))], 1, 1, 1)
        scheduler.tick(0)
        scheduler.start("root", 0)
        scheduler.finish("root", 1)
    state = scheduler.cancel("x", 2)
    assert state.schedule_status == "cancelled"
    assert state.call_status == "not_called"
    assert scheduler.cancel("x", 2) == state
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["child"]["schedule_status"] == "skipped"
    assert snapshot["resources"]["concurrency"] == (1 if phase == "blocked" else 0)
    with pytest.raises(RuntimeError):
        scheduler.start("x", 2)
    assert_ledger(snapshot)


def test_running_cancel_and_timeout_ticks_hold_resources_until_confirmed_finish():
    scheduler = Scheduler([
        task("running", resource_timeout=0), task("waiting", resource_timeout=1000),
        task("child", ("running",)),
    ], 1, 1, 1)
    scheduler.tick(0)
    scheduler.start("running", 0)
    state = scheduler.cancel("running", 1)
    assert state.schedule_status == "running"
    assert state.cancel_requested
    assert scheduler.tick(100) == []
    snapshot = scheduler.snapshot()
    assert snapshot["resources"]["concurrency"] == 1
    assert snapshot["tasks"]["child"]["schedule_status"] == "pending"
    state = scheduler.finish("running", 101, "cancelled", "not_evaluated")
    assert state.schedule_status == "cancelled"
    assert ids(scheduler.tick(101)) == ["waiting"]
    assert scheduler.snapshot()["tasks"]["child"]["schedule_status"] == "skipped"
    assert_ledger(scheduler.snapshot())


def test_pool_release_guard_idempotency_and_fractional_accounting():
    pool = ResourcePool(0.3, 0.3, 3)
    first = pool.reserve("a", 0.1, 0.2, 0)
    second = pool.reserve("b", 0.2, 0.1, 0)
    assert first and second
    assert pool.reserve("c", 1e-15, 0, 0) is None
    pool.start(first.reservation_id, 0)
    with pytest.raises(RuntimeError, match="confirm"):
        pool.release(first.reservation_id, 1)
    assert pool.snapshot()["resources"]["cpu_used"] == 0.3
    assert pool.release(first.reservation_id, 1, execution_finished=True)
    assert not pool.release(first.reservation_id, 1, execution_finished=True)
    assert pool.release(second.reservation_id, 1)
    assert pool.snapshot()["resources"]["cpu_used"] == 0
    assert pool.snapshot()["resources"]["memory_used_mb"] == 0
    for i in range(100):
        reservation = pool.reserve(str(i), 0.1, 0.1, 2)
        assert reservation
        pool.release(reservation.reservation_id, 2)
    assert_ledger(pool.snapshot())


def test_pool_rejects_double_reservation_and_release_before_execution_started():
    pool = ResourcePool(1, 1, 1)
    reservation = pool.reserve("x", 1, 1, 1)
    before = pool.snapshot()
    with pytest.raises(ValueError, match="already has"):
        pool.reserve("x", 1, 1, 1)
    with pytest.raises(ValueError, match="permanently exceeds"):
        pool.reserve("oversized", 2, 0, 1)
    with pytest.raises(ValueError):
        pool.reserve("nan", 0, float("nan"), 1)
    assert pool.snapshot() == before
    pool.start(reservation.reservation_id, 5)
    before = pool.snapshot()
    with pytest.raises(ValueError, match="execution start"):
        pool.release(reservation.reservation_id, 4, execution_finished=True)
    with pytest.raises(KeyError):
        pool.release("unknown", 6, execution_finished=True)
    assert pool.snapshot() == before
    pool.release(reservation.reservation_id, 6, execution_finished=True)
    assert_ledger(pool.snapshot())


@pytest.mark.parametrize("workers", [8, 24])
def test_actual_concurrent_pool_reservations_release_and_peaks(workers):
    pool = ResourcePool(4.2, 7.3, 5)
    reserve_gate = Barrier(workers)
    all_reserved = Barrier(workers)
    all_observed = Barrier(workers)

    def contender(i):
        reserve_gate.wait(timeout=10)
        reservation = pool.reserve(str(i), [0.3, 0.8, 1.1][i % 3], [0.5, 2.2, 1.7][i % 3], 0)
        all_reserved.wait(timeout=10)
        snapshot = pool.snapshot()
        assert_ledger(snapshot)
        all_observed.wait(timeout=10)
        if reservation:
            pool.start(reservation.reservation_id, 0)
            assert pool.release(reservation.reservation_id, 1, execution_finished=True)
            assert not pool.release(reservation.reservation_id, 1, execution_finished=True)
        return reservation

    with ThreadPoolExecutor(max_workers=workers) as executor:
        reservations = list(executor.map(contender, range(workers)))
    live_ids = [r.reservation_id for r in reservations if r]
    assert 0 < len(live_ids) <= 5
    assert len(live_ids) == len(set(live_ids))
    snapshot = pool.snapshot()
    assert snapshot["resources"]["concurrency"] == 0
    assert snapshot["peaks"]["concurrency"] == len(live_ids)
    assert sum(e["event"] == "release" for e in snapshot["resource_events"]) == len(live_ids)
    assert_ledger(snapshot)


def test_actual_concurrent_ticks_never_double_dispatch_and_finish_replay_releases_once():
    scheduler = Scheduler([task(str(i), cpu=0.1, memory_mb=0.2) for i in range(50)], 0.7, 1.4, 7)
    gate = Barrier(16)

    def dispatch(_):
        gate.wait(timeout=10)
        return scheduler.tick(0)

    with ThreadPoolExecutor(max_workers=16) as executor:
        batches = list(executor.map(dispatch, range(16)))
    reserved = [state for batch in batches for state in batch]
    assert len(reserved) == 7
    assert len(set(ids(reserved))) == 7
    for state in reserved:
        scheduler.start(state.task_id, 0)
    finish_gate = Barrier(14)

    def completion(i):
        finish_gate.wait(timeout=10)
        return scheduler.finish(reserved[i % 7].task_id, 1)

    with ThreadPoolExecutor(max_workers=14) as executor:
        completed = list(executor.map(completion, range(14)))
    assert all(state.successful for state in completed)
    snapshot = scheduler.snapshot()
    assert snapshot["resources"]["concurrency"] == 0
    assert sum(e["event"] == "release" for e in snapshot["resource_events"]) == 7
    assert len(scheduler.tick(1)) == 7
    assert_ledger(scheduler.snapshot())


def test_concurrent_start_is_claimed_exactly_once():
    scheduler = Scheduler([task("x")], 1, 1, 1)
    scheduler.tick(0)
    gate = Barrier(8)

    def claim(_):
        gate.wait(timeout=10)
        try:
            scheduler.start("x", 0)
            return True
        except RuntimeError:
            return False

    with ThreadPoolExecutor(max_workers=8) as executor:
        assert sum(executor.map(claim, range(8))) == 1
    assert scheduler.snapshot()["resources"]["concurrency"] == 1
    scheduler.finish("x", 1)
    assert_ledger(scheduler.snapshot())


def test_concurrent_cancel_start_race_cannot_release_started_work():
    for _ in range(20):
        scheduler = Scheduler([task("x"), task("child", ("x",))], 1, 1, 1)
        scheduler.tick(0)
        gate = Barrier(2)

        def launch():
            gate.wait(timeout=10)
            try:
                scheduler.start("x", 1)
                return True
            except RuntimeError:
                return False

        def cancel():
            gate.wait(timeout=10)
            return scheduler.cancel("x", 1)

        with ThreadPoolExecutor(max_workers=2) as executor:
            launch_future = executor.submit(launch)
            cancel_future = executor.submit(cancel)
            launched = launch_future.result(timeout=10)
            cancel_future.result(timeout=10)
        snapshot = scheduler.snapshot()
        if launched:
            assert snapshot["tasks"]["x"]["schedule_status"] == "running"
            assert snapshot["tasks"]["x"]["cancel_requested"]
            assert snapshot["resources"]["concurrency"] == 1
            scheduler.finish("x", 2, "cancelled", "not_evaluated")
        else:
            assert snapshot["tasks"]["x"]["schedule_status"] == "cancelled"
            assert snapshot["resources"]["concurrency"] == 0
        snapshot = scheduler.snapshot()
        assert snapshot["tasks"]["child"]["schedule_status"] == "skipped"
        assert sum(e["event"] == "release" for e in snapshot["resource_events"]) == 1
        assert_ledger(snapshot)


def test_live_subprocess_timeout_signal_does_not_reclaim_resources():
    scheduler = Scheduler([task("process"), task("waiting", resource_timeout=1000)], 1, 1, 1)
    scheduler.tick(0)
    scheduler.start("process", 0)
    process = subprocess.Popen(
        [sys.executable, "-u", "-c", "import sys; print('ready', flush=True); sys.stdin.readline()"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        with pytest.raises(subprocess.TimeoutExpired):
            process.wait(timeout=0.02)
        scheduler.cancel("process", 1, reason="executor_timeout_signal")
        assert scheduler.tick(30) == []
        assert process.poll() is None
        assert scheduler.snapshot()["resources"]["concurrency"] == 1
        process.communicate("\n", timeout=5)
        assert process.returncode == 0
        scheduler.finish("process", 31, "timeout", "not_evaluated")
        assert ids(scheduler.tick(31)) == ["waiting"]
        assert_ledger(scheduler.snapshot())
    finally:
        if process.poll() is None:
            process.kill()
        process.communicate(timeout=5)


def test_coroutine_that_catches_cancellation_holds_resources_until_it_exits():
    async def scenario():
        scheduler = Scheduler([task("coroutine"), task("waiting")], 1, 1, 1)
        scheduler.tick(0)
        scheduler.start("coroutine", 0)
        entered, cancellation_seen, stop = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def stubborn_work():
            entered.set()
            try:
                await stop.wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                await stop.wait()

        running = asyncio.create_task(stubborn_work())
        try:
            await asyncio.wait_for(entered.wait(), 5)
            scheduler.cancel("coroutine", 1)
            running.cancel()
            await asyncio.wait_for(cancellation_seen.wait(), 5)
            assert not running.done()
            assert scheduler.tick(2) == []
            assert scheduler.snapshot()["resources"]["concurrency"] == 1
            stop.set()
            await asyncio.wait_for(running, 5)
            scheduler.finish("coroutine", 3, "cancelled", "not_evaluated")
            assert ids(scheduler.tick(3)) == ["waiting"]
            assert_ledger(scheduler.snapshot())
        finally:
            stop.set()
            await asyncio.wait_for(running, 5)

    asyncio.run(scenario())


def test_invalid_transitions_and_bad_outcomes_do_not_mutate_snapshot():
    scheduler = Scheduler([task("x")], 1, 1, 1)
    before = scheduler.snapshot()
    with pytest.raises(RuntimeError):
        scheduler.start("x", 0)
    with pytest.raises(RuntimeError):
        scheduler.finish("x", 0)
    with pytest.raises(KeyError):
        scheduler.cancel("absent", 0)
    assert scheduler.snapshot() == before
    scheduler.tick(0)
    before = scheduler.snapshot()
    with pytest.raises(RuntimeError):
        scheduler.finish("x", 1)
    with pytest.raises(ValueError):
        scheduler.tick(float("nan"))
    assert scheduler.snapshot() == before
    scheduler.start("x", 1)
    before = scheduler.snapshot()
    with pytest.raises(ValueError):
        scheduler.finish("x", 2, call_status="unknown")
    with pytest.raises(ValueError):
        scheduler.finish("x", 2, quality_status="unknown")
    with pytest.raises(ValueError):
        scheduler.tick(0)
    assert scheduler.snapshot() == before


def test_snapshots_and_returned_results_are_isolated_and_json_serializable():
    scheduler = Scheduler([task("x")], 1, 1, 1)
    reserved = scheduler.tick(0)[0]
    with pytest.raises(FrozenInstanceError):
        reserved.schedule_status = "completed"
    scheduler.start("x", 0)
    result = {"numbers": [1, 2]}
    outcome = scheduler.finish("x", 1, result=result)
    result["numbers"].append(3)
    outcome.result["numbers"].append(4)
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["x"]["result"] == {"numbers": [1, 2]}
    json.dumps(snapshot, allow_nan=False)
    snapshot["tasks"]["x"]["result"]["numbers"].append(5)
    snapshot["resource_events"].clear()
    snapshot["resources"]["cpu_used"] = 999
    assert scheduler.snapshot()["tasks"]["x"]["result"] == {"numbers": [1, 2]}
    assert len(scheduler.snapshot()["resource_events"]) == 3
    assert scheduler.finish("x", 2, call_status="failed").successful


def test_long_dependency_chain_failure_propagates_without_recursion():
    specifications = [task(str(i), (str(i - 1),) if i else ()) for i in range(1500)]
    scheduler = Scheduler(reversed(specifications), 1, 1, 1)
    assert ids(scheduler.tick(0)) == ["0"]
    scheduler.start("0", 0)
    scheduler.finish("0", 1, "failed")
    snapshot = scheduler.snapshot()
    assert snapshot["done"]
    assert sum(s["schedule_status"] == "skipped" for s in snapshot["tasks"].values()) == 1499


def test_deterministic_replay_empty_workflow_and_extreme_finite_priority():
    def replay():
        scheduler = Scheduler([task("a", cpu=0.1), task("b", cpu=0.2)], 0.3, 2, 2)
        for state in scheduler.tick(0):
            scheduler.start(state.task_id, 0)
        scheduler.finish("b", 1)
        scheduler.finish("a", 2)
        return scheduler.snapshot()

    assert replay() == replay()
    empty = Scheduler([], 0, 0, 1)
    assert empty.tick(0) == []
    assert empty.snapshot()["done"]
    extreme = Scheduler([task("x")], 1, 1, 1, aging_interval=5e-324)
    assert ids(extreme.tick(1e308)) == ["x"]
    assert extreme.snapshot()["tasks"]["x"]["effective_priority"] > 10 ** 600
