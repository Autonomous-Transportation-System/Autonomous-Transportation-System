"""Shared-budget, executor-lifetime and real thread-race contracts."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from fractions import Fraction
import json
from threading import Barrier, Event

import pytest

from src.graph.traffic_joint_scheduler import JointScheduler
from src.graph.traffic_scheduler import ResourcePool
from src.service.traffic_types import Assignment, Candidate, PlanNode, WorkflowPlan


POLICIES = ("fcfs", "drc", "edf", "heft", "joint")


def candidate(instance="i", **kwargs):
    values = dict(instance_id=instance, capability_id="work", implementation_id="impl", version="v1",
                  cpu=1, memory_mb=1, expected_seconds=1, max_concurrency=8)
    values.update(kwargs)
    return Candidate(**values)


def node(node_id="root", **kwargs):
    values = dict(node_id=node_id, capability_id="work", candidates=(candidate(),))
    values.update(kwargs)
    return PlanNode(**values)


def plan(workflow_id="f", nodes=None, **kwargs):
    values = dict(workflow_id=workflow_id, app_id="app", template="test",
                  nodes=tuple(nodes) if nodes is not None else (node(),), target_frames=(1,), requested_outputs=("flow",))
    values.update(kwargs)
    return WorkflowPlan(**values)


def ids(assignments):
    return [a.task_id for a in assignments]


def releases(snapshot, task_id=None):
    return [event for event in snapshot["resource_events"] if event["event"] == "release"
            and (task_id is None or event["task_id"] == task_id)]


def assert_ledger(snapshot):
    resources = snapshot["resources"]
    for value in [resources, snapshot["peaks"], *snapshot["resource_events"]]:
        assert 0 <= value["cpu_used"] <= resources["cpu_budget"]
        assert 0 <= value["memory_used_mb"] <= resources["memory_budget"]
        assert 0 <= value["concurrency"] <= resources["max_concurrency"]
    live = list(snapshot["reservations"].values())
    assert resources["concurrency"] == len(live)
    assert resources["cpu_used"] == pytest.approx(sum(r["cpu"] for r in live))
    assert resources["memory_used_mb"] == pytest.approx(sum(r["memory_mb"] for r in live))
    task_instances = [snapshot["tasks"][r["task_id"]]["instance_id"] for r in live]
    for instance_id, state in snapshot["instances"].items():
        assert state["concurrency"] == task_instances.count(instance_id)
        assert 0 <= state["concurrency"] <= state["max_concurrency"]
    for events in (snapshot["events"], snapshot["resource_events"]):
        times = [event["now"] for event in events]
        assert times == sorted(times)
        assert all(time >= 0 for time in times)


@pytest.mark.parametrize("policy", POLICIES)
def test_cross_workflow_fractional_shared_budget_and_dynamic_submit(policy):
    scheduler = JointScheduler(0.3, 0.3, 3, policy)
    assert isinstance(scheduler._pool, ResourcePool)
    scheduler.submit(plan("a", [node(candidates=(candidate(cpu=0.1, memory_mb=0.2),))]), 0)
    scheduler.submit(plan("b", [node(candidates=(candidate(cpu=0.2, memory_mb=0.1),))]), 0)
    assignments = scheduler.tick(0)
    assert set(ids(assignments)) == {"a:root", "b:root"}
    assert all(isinstance(a, Assignment) for a in assignments)
    for assignment in assignments:
        scheduler.start(assignment.task_id, 0)
    scheduler.submit(plan("c", [node(candidates=(candidate(cpu=0.1, memory_mb=0.1),))]), 1)
    assert scheduler.tick(1) == []
    scheduler.finish("a:root", 2)
    assert ids(scheduler.tick(2)) == ["c:root"]
    assert scheduler.snapshot()["resources"]["cpu_used"] == 0.3
    scheduler.start("c:root", 2)
    scheduler.finish("b:root", 3)
    scheduler.finish("c:root", 3)
    snapshot = scheduler.snapshot()
    assert snapshot["done"] and snapshot["resources"]["concurrency"] == 0
    assert snapshot["peaks"]["cpu_used"] == 0.3
    assert_ledger(snapshot)


@pytest.mark.parametrize("policy", POLICIES)
@pytest.mark.parametrize("constraint", ["cpu", "memory_mb", "concurrency", "instance"])
def test_same_feasibility_no_head_of_line_blocking(policy, constraint):
    holder = candidate("h", cpu=2, memory_mb=2, max_concurrency=1)
    blocked = candidate("b", cpu=1, memory_mb=1)
    fits = candidate("s", cpu=1, memory_mb=1)
    if constraint in {"cpu", "memory_mb"}:
        blocked = replace(blocked, **{constraint: 3})
    if constraint == "instance":
        blocked = holder
    scheduler = JointScheduler(4, 4, 1 if constraint == "concurrency" else 2, policy)
    for key, c in (("a", holder), ("b", blocked), ("c", fits)):
        scheduler.submit(plan(key, [node(candidates=(c,))]), 0)
    first = scheduler.tick(0)
    # Profiles and workflow scores tie; stable submission order chooses a.
    assert ids(first) == (["a:root"] if constraint == "concurrency" else ["a:root", "c:root"])
    assert scheduler.snapshot()["tasks"]["b:root"]["status"] == "blocked"
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("policy", POLICIES)
def test_profile_instance_choice_and_capacity_are_traced(policy):
    cold = candidate("cold", expected_seconds=1, cold_start_seconds=10, max_concurrency=1)
    warm = candidate("warm", expected_seconds=3, adapter_seconds=1, max_concurrency=1)
    scheduler = JointScheduler(3, 3, 3, policy)
    for key in ("a", "b", "c"):
        scheduler.submit(plan(key, [node(candidates=(cold, warm))]), 0)
    assignments = scheduler.tick(0)
    assert [(a.workflow_id, a.instance_id) for a in assignments] == [("a", "warm"), ("b", "cold")]
    selection = scheduler.snapshot()["tasks"]["a:root"]["selection"]
    assert selection["estimated_finish"] == 4
    assert selection["profile_seconds"] == 4 and selection["adapter_seconds"] == 1
    assert selection["critical_path_seconds"] == 4 and selection["reason"]
    b_options = scheduler.snapshot()["tasks"]["b:root"]["selection"]["alternatives"]
    assert "instance_capacity" in next(o for o in b_options if o["instance_id"] == "warm")["reasons"]
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("policy", POLICIES)
def test_diamond_dependencies_success_valid_only_and_failure_propagation(policy):
    scheduler = JointScheduler(2, 2, 2, policy)
    scheduler.submit(plan(nodes=[node("join", depends_on=("left", "right")),
                                 node("left", depends_on=("root",)), node("right", depends_on=("root",)),
                                 node("root", max_attempts=1)]), 0)
    assert ids(scheduler.tick(0)) == ["f:root"]
    scheduler.start("f:root", 0)
    scheduler.finish("f:root", 1)
    assert set(ids(scheduler.tick(1))) == {"f:left", "f:right"}
    scheduler.start("f:left", 1)
    scheduler.start("f:right", 1)
    scheduler.finish("f:left", 2)
    assert scheduler.tick(2) == []
    scheduler.finish("f:right", 3)
    assert ids(scheduler.tick(3)) == ["f:join"]
    scheduler.start("f:join", 3)
    scheduler.finish("f:join", 4)
    assert scheduler.snapshot()["workflows"]["f"]["status"] == "completed"
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("call,quality", [
    ("success", "invalid"), ("success", "degraded"), ("success", "not_evaluated"),
    ("failed", "valid"), ("timeout", "not_evaluated"), ("cancelled", "not_evaluated"),
])
def test_failed_quality_does_not_release_descendants_and_other_flow_is_independent(call, quality):
    scheduler = JointScheduler(2, 2, 2)
    scheduler.submit(plan("a", [node("grandchild", depends_on=("child",)),
                                node("child", depends_on=("root",)), node(max_attempts=1)]), 0)
    scheduler.submit(plan("b"), 0)
    for a in scheduler.tick(0):
        scheduler.start(a.task_id, 0)
    scheduler.finish("a:root", 1, call, quality)
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["a:child"]["status"] == "skipped"
    assert snapshot["tasks"]["a:grandchild"]["status"] == "skipped"
    assert snapshot["resources"]["concurrency"] == 1
    assert not snapshot["tasks"]["a:root"]["successful"]
    scheduler.finish("b:root", 2)
    assert scheduler.snapshot()["done"]
    assert_ledger(scheduler.snapshot())


def test_policy_deadline_and_critical_path_orders_are_distinct_and_replayable():
    def run(policy):
        scheduler = JointScheduler(1, 1, 1, policy)
        scheduler.submit(plan("first", [node(candidates=(candidate(expected_seconds=1),))], deadline=100), 0)
        scheduler.submit(plan("critical", [node("child", depends_on=("root",),
                                                candidates=(candidate(expected_seconds=10),)), node()], deadline=80), 0)
        scheduler.submit(plan("urgent", deadline=5), 0)
        return ids(scheduler.tick(0)), scheduler.snapshot()
    assert run("fcfs")[0] == ["first:root"]
    assert run("drc")[0] == ["first:root"]
    assert run("edf")[0] == ["urgent:root"]
    assert run("heft")[0] == ["critical:root"]
    assert run("joint")[0] == ["urgent:root"]
    for policy in POLICIES:
        assert run(policy) == run(policy)


@pytest.mark.parametrize("policy", ["drc", "edf", "heft", "joint"])
def test_waiting_age_beats_fresh_high_priority_deadline_or_heft_rank(policy):
    scheduler = JointScheduler(1, 1, 1, policy, aging_interval=1)
    scheduler.submit(plan("old"), 0)
    scheduler.submit(plan("fresh", [node(candidates=(candidate(expected_seconds=20),))],
                          priority=2, deadline=6, created_at=3), 3)
    assert ids(scheduler.tick(3)) == ["old:root"]
    assert scheduler.snapshot()["tasks"]["old:root"]["selection"]["aging_steps"] == 3


@pytest.mark.parametrize("policy", ["drc", "edf", "heft", "joint"])
def test_equal_scores_share_dispatches_between_workflows_with_stable_ties(policy):
    scheduler = JointScheduler(2, 2, 2, policy)
    scheduler.submit(plan("a", [node("z"), node("y"), node("x")]), 0)
    scheduler.submit(plan("b", [node("z"), node("y")]), 0)
    assert ids(scheduler.tick(0)) == ["a:z", "b:z"]
    assert scheduler.snapshot()["workflows"]["a"]["dispatch_count"] == 1
    assert scheduler.snapshot()["workflows"]["b"]["dispatch_count"] == 1


@pytest.mark.parametrize("policy", POLICIES)
@pytest.mark.parametrize("phase", ["pending", "reserved", "running"])
def test_soft_deadline_records_miss_without_cancelling_and_valid_results_continue(policy, phase):
    scheduler = JointScheduler(1, 1, 1, policy)
    scheduler.submit(plan("a", [node("child", depends_on=("root",)), node()], deadline=2), 0)
    if phase != "pending":
        assert ids(scheduler.tick(0)) == ["a:root"]
    if phase == "running":
        scheduler.start("a:root", 0)
    assignments = scheduler.tick(3)
    snapshot = scheduler.snapshot()
    flow = snapshot["workflows"]["a"]
    assert not snapshot["tasks"]["a:root"]["cancel_requested"]
    assert flow["deadline_missed"] and flow["deadline_missed_at"] == 3
    assert not flow["done"] and flow["finished_at"] is None
    assert snapshot["resources"]["concurrency"] == 1 and not releases(snapshot)
    assert ids(assignments) == (["a:root"] if phase == "pending" else [])
    if phase != "running":
        scheduler.start("a:root", 3)
    scheduler.finish("a:root", 4)
    assert ids(scheduler.tick(4)) == ["a:child"]
    scheduler.start("a:child", 4)
    scheduler.finish("a:child", 5)
    assert scheduler.snapshot()["workflows"]["a"]["status"] == "completed"
    assert scheduler.snapshot()["workflows"]["a"]["deadline_missed"]
    assert len([e for e in scheduler.snapshot()["events"] if e["event"] == "deadline_miss"]) == 1
    assert_ledger(scheduler.snapshot())


def test_completion_at_deadline_is_on_time_and_later_ticks_do_not_create_miss():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(deadline=2), 0)
    scheduler.tick(0)
    scheduler.start("f:root", 0)
    scheduler.finish("f:root", 2)
    scheduler.tick(3)
    assert not scheduler.snapshot()["workflows"]["f"]["deadline_missed"]


@pytest.mark.parametrize("mutation", ["tick", "finish"])
def test_execution_timeout_requests_cancellation_and_recovers_only_after_exit(mutation):
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node("child", depends_on=("root",)),
                                 node(timeout_seconds=2, candidates=(candidate("fast"), candidate("backup", expected_seconds=2)))]), 0)
    assert scheduler.tick(0)[0].instance_id == "fast"
    scheduler.start("f:root", 0)
    if mutation == "tick":
        assert scheduler.tick(2) == []
        before = scheduler.snapshot()
        assert before["tasks"]["f:root"]["timeout_requested"]
        assert before["resources"]["concurrency"] == 1 and not releases(before)
        assert before["tasks"]["f:child"]["status"] == "pending"
    scheduler.finish("f:root", 3)
    assert scheduler.snapshot()["tasks"]["f:root"]["attempts"][0]["call_status"] == "timeout"
    assignment = scheduler.tick(3)[0]
    assert assignment.instance_id == "backup" and assignment.attempt == 2
    scheduler.start(assignment.task_id, 3)
    scheduler.finish(assignment.task_id, 4)
    assert ids(scheduler.tick(4)) == ["f:child"]
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("claimed", [False, True])
def test_start_failed_releases_once_recovers_and_never_unblocks_successors(claimed):
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node(candidates=(candidate("a"), candidate("b"))),
                                 node("child", depends_on=("root",))]), 0)
    scheduler.tick(0)
    if claimed:
        scheduler.start("f:root", 0)
    state = scheduler.start_failed("f:root", 1, error="spawn failed")
    assert state["status"] == "ready" and state["reason"] == "retry_pending"
    assert state["attempts"][0]["launch_failed"]
    scheduler.start_failed("f:root", 1)
    scheduler.finish("f:root", 1, "failed")
    assert len(releases(scheduler.snapshot())) == 1
    assert scheduler.snapshot()["tasks"]["f:child"]["status"] == "pending"
    assignment = scheduler.tick(1)[0]
    assert assignment.instance_id == "b" and assignment.attempt == 2
    # Replayed finish before a retry starts must not release its reservation.
    scheduler.finish("f:root", 1, "failed")
    assert scheduler.snapshot()["resources"]["concurrency"] == 1
    scheduler.start("f:root", 1)
    scheduler.finish("f:root", 2)
    assert len(releases(scheduler.snapshot())) == 2
    assert ids(scheduler.tick(2)) == ["f:child"]
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("attempts,count,recovery,reason", [
    (1, 3, True, "max_attempts"), (5, 2, True, "candidates_exhausted"),
    (3, 3, False, "recovery_disabled"),
])
def test_bounded_recovery_candidate_exhaustion_and_failure_propagation(attempts, count, recovery, reason):
    scheduler = JointScheduler(1, 1, 1, enable_recovery=recovery)
    scheduler.submit(plan(nodes=[node(candidates=tuple(candidate(str(i)) for i in range(count)), max_attempts=attempts),
                                 node("child", depends_on=("root",)), node("grandchild", depends_on=("child",))]), 0)
    expected = min(attempts, count) if recovery else 1
    for i in range(expected):
        a = scheduler.tick(i)[0]
        assert a.instance_id == str(i) and a.attempt == i + 1
        scheduler.start(a.task_id, i)
        scheduler.finish(a.task_id, i + 1, "failed", error="fault")
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["f:root"]["reason"] == reason
    assert snapshot["tasks"]["f:child"]["status"] == "skipped"
    assert snapshot["tasks"]["f:grandchild"]["status"] == "skipped"
    assert snapshot["workflows"]["f"]["status"] == "failed" and snapshot["done"]
    assert len(releases(snapshot)) == expected
    assert_ledger(snapshot)


def test_per_plan_recovery_toggle_and_invalid_quality_use_same_failure_path():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node(candidates=(candidate("a"), candidate("b")))],
                          options={"enable_recovery": False}), 0)
    scheduler.tick(0)
    scheduler.start("f:root", 0)
    scheduler.finish("f:root", 1, "success", "invalid")
    assert scheduler.snapshot()["done"]
    assert scheduler.snapshot()["tasks"]["f:root"]["reason"] == "recovery_disabled"


def test_dynamic_health_overrides_candidate_snapshots_and_recovers_when_restored():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.set_instance_health("a", False, 0)
    scheduler.submit(plan(nodes=[node(candidates=(candidate("a"), candidate("b", healthy=False)))]), 0)
    assert scheduler.tick(0) == []
    scheduler.set_instance_health("b", True, 1)
    assert scheduler.tick(1)[0].instance_id == "b"
    scheduler.start("f:root", 1)
    scheduler.finish("f:root", 2, "failed")
    assert scheduler.tick(2) == []
    assert not scheduler.snapshot()["done"]
    scheduler.set_instance_health("a", True, 3)
    assert scheduler.tick(3)[0].instance_id == "a"
    scheduler.start("f:root", 3)
    scheduler.finish("f:root", 4)
    assert scheduler.snapshot()["done"]
    assert_ledger(scheduler.snapshot())


def test_new_submissions_use_updated_profiles_without_mutating_existing_plans():
    scheduler = JointScheduler(2, 2, 2, "fcfs")
    original = (candidate("a", expected_seconds=2), candidate("b", expected_seconds=10))
    refreshed = (replace(original[0], expected_seconds=8), replace(original[1], expected_seconds=1))
    scheduler.submit(plan("old", [node(candidates=original)]), 0)
    scheduler.submit(plan("new", [node(candidates=refreshed)]), 1)
    assignments = scheduler.tick(1)
    assert [(a.workflow_id, a.instance_id, a.candidate.expected_seconds) for a in assignments] == [
        ("old", "a", 2), ("new", "b", 1),
    ]
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["old:root"]["selection"]["estimated_finish"] == 3
    assert snapshot["tasks"]["new:root"]["selection"]["estimated_finish"] == 2
    assert_ledger(snapshot)


def test_public_ledger_decimal_rounding_cannot_over_admit_an_extra_task():
    scheduler = JointScheduler(0.3, 3, 3, "fcfs")
    for key, cpu in (("a", 0.1), ("b", 0.10000000000000002), ("c", 0.1)):
        scheduler.submit(plan(key, [node(candidates=(candidate(cpu=cpu),))]), 0)
    # Public cpu_used rounds the first two decimal amounts to 0.2. Their exact
    # sum leaves less than 0.1, so the third admission must still be blocked.
    assert ids(scheduler.tick(0)) == ["a:root", "b:root"]
    assert scheduler.snapshot()["resources"]["cpu_used"] == 0.2
    assert scheduler.snapshot()["tasks"]["c:root"]["status"] == "blocked"
    assert_ledger(scheduler.snapshot())


def test_real_numbers_are_normalized_to_same_amounts_as_public_pool():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node(candidates=(candidate(cpu=Fraction(1, 3), memory_mb=Fraction(1, 2),
                                                          expected_seconds=Fraction(1, 4)),))]), 0)
    assignment = scheduler.tick(0)[0]
    assert assignment.candidate.cpu == float(Fraction(1, 3))
    assert assignment.candidate.expected_seconds == 0.25
    json.dumps(scheduler.snapshot(), allow_nan=False)
    assert_ledger(scheduler.snapshot())


def test_health_change_after_reserve_refuses_start_and_start_failed_reselects():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node(candidates=(candidate("a"), candidate("b")))]), 0)
    scheduler.tick(0)
    scheduler.set_instance_health("a", False, 1)
    with pytest.raises(RuntimeError, match="unhealthy"):
        scheduler.start("f:root", 1)
    assert scheduler.snapshot()["resources"]["concurrency"] == 1
    scheduler.start_failed("f:root", 1)
    assert scheduler.tick(1)[0].instance_id == "b"
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("phase", ["pending", "reserved", "running"])
def test_cancel_workflow_preserves_live_resource_owner_and_prevents_retry(phase):
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan("a", [node(candidates=(candidate("a"), candidate("b"))), node("child", depends_on=("root",))]), 0)
    scheduler.submit(plan("b"), 0)
    if phase != "pending":
        scheduler.tick(0)
    if phase == "running":
        scheduler.start("a:root", 0)
    scheduler.cancel("a", 1, reason="user stop")
    scheduler.cancel("a", 1, reason="duplicate")
    snapshot = scheduler.snapshot()
    assert snapshot["tasks"]["a:child"]["status"] == "cancelled"
    if phase == "running":
        assert not snapshot["workflows"]["a"]["done"] and not releases(snapshot)
        assert scheduler.tick(2) == []
        scheduler.finish("a:root", 3, "failed")
        assert len(releases(scheduler.snapshot())) == 1
        assert ids(scheduler.tick(3)) == ["b:root"]
    else:
        assert snapshot["workflows"]["a"]["done"]
        assert ids(scheduler.tick(2)) == ["b:root"]
    assert scheduler.snapshot()["workflows"]["a"]["status"] == "cancelled"
    assert_ledger(scheduler.snapshot())


def test_cancel_after_completion_cannot_rewrite_result_or_finished_time():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(), 0)
    scheduler.tick(0)
    scheduler.start("f:root", 0)
    scheduler.finish("f:root", 1)
    scheduler.cancel("f", 2)
    scheduler.finish("f:root", 3, "failed")
    snapshot = scheduler.snapshot()
    assert snapshot["workflows"]["f"]["status"] == "completed"
    assert snapshot["workflows"]["f"]["finished_at"] == 1
    assert snapshot["tasks"]["f:root"]["call_status"] == "success"
    assert len(releases(snapshot)) == 1


def test_snapshot_isolation_filtering_results_and_global_ledger():
    scheduler = JointScheduler(2, 2, 2)
    parameters = {"list": [1]}
    scheduler.submit(plan("a", [node(parameters=parameters)]), 0)
    parameters["list"].append(2)
    scheduler.submit(plan("b"), 0)
    assignments = scheduler.tick(0)
    assert assignments[0].parameters == {"list": [1]}
    assignments[0].parameters["list"].append(3)
    for a in assignments:
        scheduler.start(a.task_id, 0)
    result = {"value": [1]}
    scheduler.finish("a:root", 1, result=result)
    result["value"].append(2)
    filtered = scheduler.snapshot("a", include_results=True)
    assert set(filtered["tasks"]) == {"a:root"} and set(filtered["workflows"]) == {"a"}
    assert filtered["resources"]["concurrency"] == 1 and filtered["reservations"]
    assert filtered["tasks"]["a:root"]["result"] == {"value": [1]}
    assert "result" not in scheduler.snapshot()["tasks"]["a:root"]
    json.dumps(filtered, allow_nan=False)
    filtered["tasks"]["a:root"]["result"]["value"].append(3)
    filtered["tasks"]["a:root"]["attempts"][0]["selection"]["alternatives"].clear()
    filtered["events"].clear()
    assert scheduler.snapshot("a", True)["tasks"]["a:root"]["result"] == {"value": [1]}
    assert scheduler.snapshot()["tasks"]["a:root"]["attempts"][0]["selection"]["alternatives"]
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("values", [
    {"cpu_budget": -1}, {"cpu_budget": True}, {"cpu_budget": float("nan")},
    {"memory_budget": float("inf")}, {"memory_budget": "1"},
    {"max_concurrency": 0}, {"max_concurrency": 1.5}, {"max_concurrency": True},
    {"aging_interval": 0}, {"aging_interval": float("nan")}, {"aging_interval": True},
    {"enable_recovery": 1}, {"policy": "unknown"}, {"policy": []},
])
def test_constructor_validation(values):
    kwargs = dict(cpu_budget=1, memory_budget=1, max_concurrency=1)
    kwargs.update(values)
    with pytest.raises(ValueError):
        JointScheduler(**kwargs)


@pytest.mark.parametrize("field", ["cpu", "memory_mb", "expected_seconds", "cold_start_seconds", "adapter_seconds"])
@pytest.mark.parametrize("value", [-1, True, "1", float("nan"), float("inf")])
def test_candidate_number_validation_is_atomic(field, value):
    scheduler = JointScheduler(1, 1, 1)
    before = scheduler.snapshot()
    with pytest.raises(ValueError):
        scheduler.submit(plan(nodes=[node(candidates=(candidate(**{field: value}),))]), 1)
    assert scheduler.snapshot() == before


@pytest.mark.parametrize("invalid", [
    {"nodes": ()}, {"nodes": []}, {"workflow_id": ""}, {"workflow_id": "a:b"},
    {"created_at": 2}, {"created_at": -1}, {"deadline": -1}, {"deadline": float("inf")},
    {"priority": True}, {"priority": -1}, {"target_frames": [1]}, {"target_frames": (True,)},
    {"requested_outputs": ("",)}, {"options": {"enable_recovery": 1}}, {"contract": []},
])
def test_workflow_validation_is_atomic(invalid):
    scheduler = JointScheduler(1, 1, 1)
    before = scheduler.snapshot()
    with pytest.raises(ValueError):
        scheduler.submit(replace(plan(), **invalid), 1)
    assert scheduler.snapshot() == before


@pytest.mark.parametrize("nodes", [
    (node(), node()), (node(depends_on=("missing",)),), (node(depends_on=("root",)),),
    (node("a", depends_on=("b",)), node("b", depends_on=("a",))),
    (node(candidates=()),), (node(max_attempts=0),), (node(timeout_seconds=-1),),
    (node(timeout_seconds=float("nan")),), (node(priority=0.5),), (node(depends_on=["a"]),),
    (node(candidates=(candidate(capability_id="wrong"),)),),
    (node(candidates=(candidate(cpu=2),)),), (node(candidates=(candidate(memory_mb=2),)),),
    (node(candidates=(candidate(max_concurrency=0),)),), (node(candidates=(candidate(healthy=1),)),),
    (node(candidates=(candidate(), candidate())),),
    (node("a", candidates=(candidate(max_concurrency=1),)), node("b", candidates=(candidate(max_concurrency=2),))),
])
def test_node_dag_and_capacity_validation_is_atomic(nodes):
    scheduler = JointScheduler(1, 1, 1)
    before = scheduler.snapshot()
    with pytest.raises(ValueError):
        scheduler.submit(plan(nodes=nodes), 1)
    assert scheduler.snapshot() == before


def test_oversized_candidate_is_filtered_when_alternative_fits():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node(candidates=(candidate("large", cpu=2, expected_seconds=0), candidate("small")))]), 0)
    assert scheduler.tick(0)[0].instance_id == "small"
    assert "exceeds_budget" in scheduler.snapshot()["tasks"]["f:root"]["selection"]["alternatives"][0]["reasons"]


def test_duplicate_submit_and_unknown_calls_do_not_mutate():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(), 0)
    before = scheduler.snapshot()
    operations = [lambda: scheduler.submit(plan(), 1), lambda: scheduler.cancel("missing", 1),
                  lambda: scheduler.start("missing", 1), lambda: scheduler.finish("missing", 1),
                  lambda: scheduler.start_failed("missing", 1), lambda: scheduler.snapshot("missing"),
                  lambda: scheduler.start("f:root", 1), lambda: scheduler.finish("f:root", 1),
                  lambda: scheduler.start_failed("f:root", 1)]
    for operation in operations:
        with pytest.raises((ValueError, KeyError, RuntimeError)):
            operation()
        assert scheduler.snapshot() == before


@pytest.mark.parametrize("operation", [
    lambda s: s.finish("f:root", 2, call_status="unknown"),
    lambda s: s.finish("f:root", 2, quality_status="unknown"),
    lambda s: s.finish("f:root", 2, call_status=[]),
    lambda s: s.finish("f:root", 2, error=1),
    lambda s: s.start_failed("f:root", 2, error=[]),
    lambda s: s.cancel("f", 2, reason=1),
    lambda s: s.set_instance_health("i", 1, 2),
    lambda s: s.set_instance_health("", True, 2),
    lambda s: s.snapshot(include_results=1),
])
def test_invalid_outcomes_and_flags_do_not_mutate(operation):
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(), 0)
    scheduler.tick(0)
    scheduler.start("f:root", 1)
    before = scheduler.snapshot()
    with pytest.raises(ValueError):
        operation(scheduler)
    assert scheduler.snapshot() == before


@pytest.mark.parametrize("now", [-1, True, "1", float("nan"), float("inf"), 0.5])
@pytest.mark.parametrize("operation", ["submit", "tick", "start", "finish", "start_failed", "cancel", "health"])
def test_every_event_rejects_bad_or_regressing_time_atomically(now, operation):
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(), 1)
    scheduler.tick(1)
    scheduler.start("f:root", 1)
    before = scheduler.snapshot()
    calls = {
        "submit": lambda: scheduler.submit(plan("other"), now), "tick": lambda: scheduler.tick(now),
        "start": lambda: scheduler.start("f:root", now), "finish": lambda: scheduler.finish("f:root", now),
        "start_failed": lambda: scheduler.start_failed("f:root", now),
        "cancel": lambda: scheduler.cancel("f", now), "health": lambda: scheduler.set_instance_health("i", False, now),
    }
    with pytest.raises(ValueError):
        calls[operation]()
    assert scheduler.snapshot() == before


def test_zero_resources_zero_timeout_and_extreme_finite_aging():
    scheduler = JointScheduler(0, 0, 1, aging_interval=5e-324)
    scheduler.submit(plan(nodes=[node(candidates=(candidate(cpu=0, memory_mb=0),), timeout_seconds=0)]), 0)
    assert ids(scheduler.tick(1e308)) == ["f:root"]
    assert scheduler.snapshot()["tasks"]["f:root"]["effective_priority"] > 10 ** 600
    scheduler.start("f:root", 1e308)
    scheduler.tick(1e308)
    assert scheduler.snapshot()["resources"]["concurrency"] == 1
    scheduler.finish("f:root", 1e308)
    assert scheduler.snapshot()["done"]
    assert_ledger(scheduler.snapshot())


def test_long_reverse_order_dag_propagates_failure_without_recursion():
    nodes = [node(str(i), depends_on=(str(i - 1),) if i else (), max_attempts=1) for i in range(1100)]
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=reversed(nodes)), 0)
    assert ids(scheduler.tick(0)) == ["f:0"]
    scheduler.start("f:0", 0)
    scheduler.finish("f:0", 1, "failed")
    snapshot = scheduler.snapshot()
    assert snapshot["done"]
    assert sum(t["status"] == "skipped" for t in snapshot["tasks"].values()) == 1099


def test_real_concurrent_submit_ticks_and_duplicate_finish_share_exact_budget():
    scheduler = JointScheduler(0.7, 1.4, 7)
    submit_gate = Barrier(16)

    def submit(i):
        submit_gate.wait(timeout=10)
        scheduler.submit(plan(str(i), [node(candidates=(candidate(cpu=0.1, memory_mb=0.2),))]), 0)

    with ThreadPoolExecutor(max_workers=16) as executor:
        list(executor.map(submit, range(16)))
    gate = Barrier(16)

    def dispatch(_):
        gate.wait(timeout=10)
        return scheduler.tick(0)

    with ThreadPoolExecutor(max_workers=16) as executor:
        batches = list(executor.map(dispatch, range(16)))
    assignments = [a for batch in batches for a in batch]
    assert len(assignments) == 7 and len(set(ids(assignments))) == 7
    for a in assignments:
        scheduler.start(a.task_id, 0)
    finish_gate = Barrier(14)

    def finish(i):
        finish_gate.wait(timeout=10)
        return scheduler.finish(assignments[i % 7].task_id, 1)

    with ThreadPoolExecutor(max_workers=14) as executor:
        outcomes = list(executor.map(finish, range(14)))
    assert all(state["successful"] for state in outcomes)
    snapshot = scheduler.snapshot()
    assert len(releases(snapshot)) == 7 and snapshot["resources"]["concurrency"] == 0
    assert snapshot["peaks"]["concurrency"] == 7
    assert len(scheduler.tick(1)) == 7
    assert_ledger(scheduler.snapshot())


def test_real_start_race_claims_exactly_once():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(), 0)
    scheduler.tick(0)
    gate = Barrier(8)

    def claim(_):
        gate.wait(timeout=10)
        try:
            scheduler.start("f:root", 0)
            return True
        except RuntimeError:
            return False

    with ThreadPoolExecutor(max_workers=8) as executor:
        assert sum(executor.map(claim, range(8))) == 1
    assert scheduler.snapshot()["resources"]["concurrency"] == 1
    scheduler.finish("f:root", 1)
    assert_ledger(scheduler.snapshot())


def test_real_cancel_start_race_never_reclaims_started_work():
    for _ in range(16):
        scheduler = JointScheduler(1, 1, 1)
        scheduler.submit(plan(), 0)
        scheduler.tick(0)
        gate = Barrier(2)

        def start():
            gate.wait(timeout=10)
            try:
                scheduler.start("f:root", 1)
                return True
            except RuntimeError:
                return False

        def cancel():
            gate.wait(timeout=10)
            scheduler.cancel("f", 1)

        with ThreadPoolExecutor(max_workers=2) as executor:
            launch = executor.submit(start)
            stop = executor.submit(cancel)
            launched = launch.result(timeout=10)
            stop.result(timeout=10)
        snapshot = scheduler.snapshot()
        if launched:
            assert snapshot["resources"]["concurrency"] == 1 and not releases(snapshot)
            assert snapshot["tasks"]["f:root"]["cancel_requested"]
            scheduler.finish("f:root", 2, "cancelled")
        else:
            assert snapshot["resources"]["concurrency"] == 0
        assert len(releases(scheduler.snapshot())) == 1
        assert_ledger(scheduler.snapshot())


def test_real_failed_finish_race_enqueues_only_one_retry_and_releases_once():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node(candidates=(candidate("a"), candidate("b")))]), 0)
    scheduler.tick(0)
    scheduler.start("f:root", 0)
    gate = Barrier(12)

    def finish(_):
        gate.wait(timeout=10)
        return scheduler.finish("f:root", 1, "failed")

    with ThreadPoolExecutor(max_workers=12) as executor:
        outcomes = list(executor.map(finish, range(12)))
    assert all(state["status"] == "ready" and len(state["attempts"]) == 1 for state in outcomes)
    assert len(releases(scheduler.snapshot())) == 1
    assert len([e for e in scheduler.snapshot()["events"] if e["event"] == "retry"]) == 1
    assignment = scheduler.tick(1)[0]
    assert assignment.attempt == 2 and assignment.instance_id == "b"
    assert scheduler.tick(1) == []
    assert_ledger(scheduler.snapshot())


def test_real_launch_failure_race_releases_and_enqueues_backup_only_once():
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan(nodes=[node(candidates=(candidate("a"), candidate("b")))]), 0)
    scheduler.tick(0)
    gate = Barrier(8)

    def start_failed(_):
        gate.wait(timeout=10)
        return scheduler.start_failed("f:root", 1, error="spawn")

    with ThreadPoolExecutor(max_workers=8) as executor:
        outcomes = list(executor.map(start_failed, range(8)))
    assert all(state["status"] == "ready" and len(state["attempts"]) == 1 for state in outcomes)
    assert len(releases(scheduler.snapshot())) == 1
    assert scheduler.tick(1)[0].instance_id == "b"
    assert_ledger(scheduler.snapshot())


def test_real_health_restoration_and_new_profile_submissions_are_atomic():
    scheduler = JointScheduler(4, 4, 4, "fcfs")
    scheduler.set_instance_health("a", False, 0)
    scheduler.set_instance_health("b", False, 0)
    scheduler.submit(plan("old", [node(candidates=(candidate("a"), candidate("b", expected_seconds=2)))]), 0)
    assert scheduler.tick(0) == []
    gate = Barrier(10)

    def mutate(i):
        gate.wait(timeout=10)
        if i < 2:
            scheduler.set_instance_health("ab"[i], True, 1)
        else:
            scheduler.submit(plan(str(i), [node(candidates=(candidate("a", expected_seconds=4), candidate("b")))]), 1)
        return scheduler.snapshot()

    with ThreadPoolExecutor(max_workers=10) as executor:
        snapshots = list(executor.map(mutate, range(10)))
    assert all(snapshot["now"] == 1 for snapshot in snapshots)
    assignments = scheduler.tick(1)
    assert len(assignments) == 4
    assert assignments[0].workflow_id == "old" and assignments[0].instance_id == "a"
    assert all(a.instance_id == "b" for a in assignments[1:])
    assert len(scheduler.snapshot()["workflows"]) == 9
    assert_ledger(scheduler.snapshot())


@pytest.mark.parametrize("trigger", ["cancel", "timeout"])
def test_live_thread_retains_reservation_until_executor_actually_exits(trigger):
    scheduler = JointScheduler(1, 1, 1)
    scheduler.submit(plan("a", [node(timeout_seconds=1 if trigger == "timeout" else 30)]), 0)
    scheduler.submit(plan("b"), 0)
    scheduler.tick(0)
    entered, cancellation_seen, exit_permission = Event(), Event(), Event()

    def actual_execution():
        scheduler.start("a:root", 0)
        entered.set()
        assert cancellation_seen.wait(5)
        assert exit_permission.wait(5)
        scheduler.finish("a:root", 3, "cancelled" if trigger == "cancel" else "timeout")

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(actual_execution)
        try:
            assert entered.wait(5)
            if trigger == "cancel":
                scheduler.cancel("a", 1)
            else:
                scheduler.tick(1)
            cancellation_seen.set()
            assert not future.done()
            assert scheduler.tick(2) == []
            snapshot = scheduler.snapshot()
            assert snapshot["resources"]["concurrency"] == 1 and not releases(snapshot)
            assert snapshot["tasks"]["a:root"]["cancel_requested"]
        finally:
            cancellation_seen.set()
            exit_permission.set()
        future.result(timeout=5)
    assert ids(scheduler.tick(3)) == ["b:root"]
    assert_ledger(scheduler.snapshot())
