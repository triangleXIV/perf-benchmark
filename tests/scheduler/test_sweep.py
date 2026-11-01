"""Search decisions are tested against controlled measurements, never HTTP timing."""
import asyncio
from contextlib import AbstractAsyncContextManager
from unittest.mock import create_autospec

from perf_benchmark.pipeline.contracts.scheduler import SessionRunner
from perf_benchmark.pipeline.scheduling.mechanics import SchedulerBase
from dataclasses import replace
from types import SimpleNamespace

import pytest

from perf_benchmark.pipeline.context import SchedulerContext
from perf_benchmark.pipeline.contracts import (
    MetricPoint, TrialOutcome, TrialSchedulingFact, TrialExecution, TrialValidity,
)
from perf_benchmark.pipeline.scheduling.sweep import threshold, evaluate
from perf_benchmark.plugins import create_registry
from perf_benchmark.plugins.metrics.standard import StandardMetrics
from perf_benchmark.runtime.cancellation import CancellationToken


RULES = [
    {"metric": "ttft", "statistic": "am", "op": "le", "value": "1s"},
    {"metric": "tpot", "statistic": "am", "op": "le", "value": "14ms"},
    {"metric": "e2e", "statistic": "p99", "op": "le", "value": "5s"},
]


def plugin(*, search=None, values=None, rules=None, repetitions=1, name="single-turn-closed-loop", **settings):
    sweep = {"axis": "max_concurrency", "slo": {"rules": RULES if rules is None else rules}}
    if values is not None:
        sweep["values"] = values
    else:
        sweep["search"] = {
            "min": 1, "max": 32, "initial": 2, "resolution": 1,
            "max_trials": 80, "timeout_s": 10, **(search or {}),
        }
    instance = create_registry().bind("scheduler", name, {
        "sweep": sweep, "repetitions": repetitions, **settings,
    }).plugin
    assert isinstance(instance, SchedulerBase)
    instance.validate_metrics(StandardMetrics({}))
    return instance


class Trials:
    def __init__(self):
        self.records = []

    def open(self, plan, strategy) -> AbstractAsyncContextManager[TrialExecution]:
        raise AssertionError("test must supply Trial execution")

    async def record_sweep(self, record):
        self.records.append(record)


def outcome(value, ordinal, *, count=1000):
    identity = f"trial-{ordinal}"
    points = tuple(
        MetricPoint("trial", identity, "overall", metric, stat, amount, unit,
                    "standard-window-v8", "available", None, "test", count, count, 0)
        for metric, stat, amount, unit in (
            ("ttft", "am", value / 10, "s"),
            ("tpot", "am", value * 2, "ms"),
            ("e2e", "p99", value / 2, "s"),
        )
    )
    return TrialOutcome(identity, "succeeded", TrialSchedulingFact(
        offered_sessions=count, admitted_sessions=count, completed_sessions=count,
        offered_attempts=count, admitted_attempts=count, completed_attempts=count,
    ), points, TrialValidity(identity, "succeeded", "pass"))


async def run(instance, monkeypatch, transform=lambda result, plan, number: result, delay=0):
    host, ctx, plans = Trials(), SchedulerContext(CancellationToken()), []

    async def execute(plan, trials, context):
        plans.append(plan)
        if delay:
            await asyncio.sleep(delay)
        result = outcome(plan.comparison.axis_value, len(plans))
        return SimpleNamespace(outcome=transform(result, plan, len(plans)))

    monkeypatch.setattr(instance, "execute_plan", execute)
    await instance.schedule(host, ctx)
    return host.records, plans


@pytest.mark.parametrize("value,unit,expected", [
    ("1s", "s", 1), ("1000ms", "s", 1), ("1m", "s", 60), ("1h", "s", 3600),
    ("1.5m", "ms", 90000), ("0.5s", "ms", 500), ("1us", "ms", 0.001),
    ("1000000ns", "ms", 1), ("1s", "ms", 1000), (0.99, "ratio", 0.99),
    ("1000tok/s", "tok/s", 1000), ("10req/s", "req/s", 10),
    ("10sessions/s", "sessions/s", 10), (1, "s", 1),
])
def test_threshold_units(value, unit, expected):
    assert threshold(value, unit) == pytest.approx(expected)


@pytest.mark.parametrize("value,unit", [
    (True, "s"), (float("inf"), "s"), (float("nan"), "s"), (-1, "s"),
    ("1e999s", "s"), ("1month", "s"), ("1tok/s", "s"), ("10s", "tok/s"),
    ("99%", "ratio"), ("1", "s"), ("1s+1m", "s"), ("__import__('os')", "s"),
])
def test_bad_thresholds_are_rejected(value, unit):
    with pytest.raises(ValueError):
        threshold(value, unit)


def test_tpot_hm_is_rejected_before_execution():
    with pytest.raises(ValueError, match="unsupported SLO metric/statistic"):
        plugin(rules=[{"metric": "tpot", "statistic": "hm", "op": "le", "value": "50ms"}])


def test_legacy_list_and_axis_conflicts_rejected():
    registry = create_registry()
    with pytest.raises(ValueError):
        registry.bind("scheduler", "single-turn-closed-loop", {"max_concurrency": [1, 2]})
    with pytest.raises(ValueError, match="non-duplicated"):
        plugin(max_concurrency=4)
    with pytest.raises(ValueError, match="greater than or equal"):
        plugin(name="multi-turn-closed-loop", max_active_sessions=16)


async def test_joint_slo_interpolation_finds_tpot_boundary_and_confirms(monkeypatch):
    records, plans = await run(plugin(repetitions=2), monkeypatch)
    final = records[-1]
    assert (final.status, final.lower, final.upper) == ("boundary_confirmed", 7, 8)
    assert [p.comparison.axis_value for p in plans][-4:] == [7, 7, 8, 8]
    assert [p.repetition.index for p in plans][-4:] == [1, 2, 1, 2]
    assert all(len(r.checks) == 3 for r in records if r.phase in {"probe", "confirm"})
    assert any(c.rule.metric == "tpot" and c.status == "fail" for r in records for c in r.checks)
    assert records[0].definition["normalized_rules"][0]["threshold"] == 1
    assert len(plans) < 32  # fewer probes than enumeration, with the same repetitions


@pytest.mark.parametrize("search,expected,boundary", [
    ({"initial": 32}, "boundary_confirmed", 7),
    ({"max": 4}, "upper_limit_passed", 4),
    ({"min": 9, "initial": 12}, "no_feasible_point", None),
    ({"min": 4, "max": 4, "initial": 4}, "upper_limit_passed", 4),
    ({"max_trials": 1}, "budget_exhausted", 2),
    ({"max_trials": 1, "initial": 7}, "budget_exhausted", 7),
])
async def test_search_termination(monkeypatch, search, expected, boundary):
    records, plans = await run(plugin(search=search), monkeypatch)
    assert records[-1].status == expected
    assert records[-1].lower == boundary
    assert len(plans) <= search.get("max_trials", 80)


async def test_resolution_reports_interval_not_exact_maximum(monkeypatch):
    records, _ = await run(plugin(search={"resolution": 4}), monkeypatch)
    final = records[-1]
    assert final.status == "boundary_confirmed"
    assert 0 < final.upper - final.lower <= 4


async def test_confirmation_flip_stops_without_retry_loop(monkeypatch):
    def flip(result, plan, number):
        if plan.metadata["sweep_phase"] == "confirm":
            return replace(result, points=tuple(replace(p, value=9999) for p in result.points))
        return result
    records, plans = await run(plugin(), monkeypatch, flip)
    assert records[-1].status == "unstable"
    assert len([p for p in plans if p.metadata["sweep_phase"] == "confirm"]) == 1


async def test_all_repetitions_must_pass(monkeypatch):
    def occasional_failure(result, plan, number):
        if plan.repetition.index == 2 and plan.comparison.axis_value >= 5:
            return replace(result, points=tuple(replace(p, value=9999) for p in result.points))
        return result
    records, _ = await run(plugin(repetitions=2), monkeypatch, occasional_failure)
    assert records[-1].lower == 4
    assert records[-1].upper == 5


async def test_missing_statistic_stops_and_retains_reason(monkeypatch):
    records, plans = await run(plugin(), monkeypatch, lambda r, p, n: replace(r, points=r.points[1:]))
    assert records[-1].status == "indeterminate"
    assert len(plans) == 1
    assert records[2].checks[0].reason == "metric_not_reported"


async def test_fixed_points_continue_after_slo_failure(monkeypatch):
    records, plans = await run(plugin(values=[12, 4, 8]), monkeypatch)
    assert [p.comparison.axis_value for p in plans] == [12, 4, 8]
    assert records[-1].status == "completed"
    assert records[-1].lower == 4


async def test_timeout_cancels_active_trial_and_records_budget(monkeypatch):
    records, plans = await run(plugin(search={"timeout_s": 0.01}), monkeypatch, delay=1)
    assert records[-1].status == "budget_exhausted"
    assert records[-1].reason == "timeout"
    assert len(plans) == 1


@pytest.mark.parametrize("name,settings", [
    ("single-turn-closed-loop", {}),
    ("multi-turn-closed-loop", {"max_active_sessions": 32}),
    ("single-turn-open-loop", {"request_rate": 500, "max_pending_requests": 8}),
    ("multi-turn-open-loop", {"session_rate": 500, "request_rate": 500,
                              "max_pending_requests": 8, "max_pending_sessions": 8}),
])
@pytest.mark.parametrize("max_trials,expected", [(80, "boundary_confirmed"), (1, "budget_exhausted")])
async def test_null_timeout_search_uses_boundaries_and_trial_budget(
    monkeypatch, name, settings, max_trials, expected,
):
    instance = plugin(name=name, search={"timeout_s": None, "max_trials": max_trials}, **settings)
    loop = asyncio.get_running_loop()
    timers = []
    original_call_later = loop.call_later

    def call_later(delay, callback, *args, **kwargs):
        timers.append(delay)
        return original_call_later(delay, callback, *args, **kwargs)

    monkeypatch.setattr(loop, "call_later", call_later)
    records, plans = await run(instance, monkeypatch)
    assert timers == []
    assert records[0].definition["search"]["timeout_s"] is None
    assert records[-1].status == expected
    assert len(plans) <= max_trials
    assert records[-1].reason != "timeout"


@pytest.mark.parametrize("timeout", [0, -1, True, "null", float("inf"), float("nan")])
def test_search_timeout_rejects_invalid_values(timeout):
    with pytest.raises(ValueError, match="timeout_s must be finite and positive"):
        plugin(search={"timeout_s": timeout})


async def test_tool_timeout_is_not_a_search_budget(monkeypatch):
    instance, host = plugin(), Trials()
    async def broken(*_):
        raise TimeoutError("executor failed")
    monkeypatch.setattr(instance, "execute_plan", broken)
    with pytest.raises(TimeoutError, match="executor failed"):
        await instance.schedule(host, SchedulerContext(CancellationToken()))
    assert host.records[-1].status == "failed"


def test_trial_outcome_is_inaccessible_until_commit():
    async def sessions():
        for session in ():
            yield session
    execution = TrialExecution(sessions(), create_autospec(SessionRunner, instance=True))
    with pytest.raises(RuntimeError, match="commit"):
        _ = execution.outcome


def test_invalid_metric_evidence_and_fail_precedence():
    instance = plugin()
    result = outcome(2, 1, count=2)
    checks = evaluate(result, instance.slo_rules, "overall", 10)
    assert all(c.status == "indeterminate" and c.reason == "insufficient_samples" for c in checks)
    result = replace(result, points=(replace(result.points[0], value=2), *result.points[1:]))
    checks = evaluate(result, instance.slo_rules, "overall", 1)
    assert [c.status for c in checks] == ["fail", "pass", "pass"]
    for point in (replace(result.points[0], unit="ms"), replace(result.points[0], value=float("nan"))):
        assert evaluate(replace(result, points=(point,)), instance.slo_rules, "overall", 1)[0].status == "indeterminate"


def test_search_slo_can_consume_derived_goodput_metric():
    instance = plugin(
        rules=[
            {
                "metric": "goodput_percentage",
                "statistic": "value",
                "op": "ge",
                "value": 0.99,
            }
        ]
    )
    point = MetricPoint(
        "trial",
        "trial-1",
        "overall",
        "goodput_percentage",
        "value",
        0.995,
        "ratio",
        "standard-window-v8",
        "available",
        None,
        "goodput_slo",
        100, 100, 0,
    )
    result = TrialOutcome("trial-1", "succeeded", TrialSchedulingFact(), (point,))

    assert evaluate(result, instance.slo_rules, "overall", 1)[0].status == "pass"


@pytest.mark.parametrize("name,settings,axis,values", [
    ("single-turn-closed-loop", {}, "max_concurrency", [1, 2]),
    ("multi-turn-closed-loop", {"max_active_sessions": 4}, "max_concurrency", [1, 2]),
    ("multi-turn-closed-loop", {"max_concurrency": 2}, "max_active_sessions", [2, 4]),
    ("single-turn-open-loop", {"request_rate": 500, "max_pending_requests": 8}, "max_concurrency", [1, 2]),
    ("single-turn-open-loop", {"max_concurrency": 2, "max_pending_requests": 8}, "request_rate", [200, 400]),
    ("multi-turn-open-loop", {"session_rate": 500, "request_rate": 500, "max_pending_requests": 8,
                              "max_pending_sessions": 8}, "max_concurrency", [1, 2]),
    ("multi-turn-open-loop", {"session_rate": 500, "request_rate": 500, "max_pending_requests": 8,
                              "max_pending_sessions": 8}, "max_active_sessions", [1, 2]),
    ("multi-turn-open-loop", {"session_rate": 500, "max_pending_requests": 8,
                              "max_pending_sessions": 8}, "request_rate", [200, 400]),
    ("multi-turn-open-loop", {"request_rate": 500, "max_pending_requests": 8,
                              "max_pending_sessions": 8}, "session_rate", [200, 400]),
])
@pytest.mark.parametrize("adaptive", [False, True])
async def test_all_scheduler_axes_use_public_runner_and_outcomes(name, settings, axis, values, adaptive):
    from contextlib import asynccontextmanager
    from perf_benchmark.core.models import Session, Turn
    from perf_benchmark.pipeline.preparation.payloads import PayloadRef
    sweep = {"axis": axis, "slo": {"rules": [{"metric": "ttft", "statistic": "am", "op": "le", "value": "1h"}]}}
    if adaptive:
        sweep["search"] = {"min": values[0], "max": values[-1], "initial": values[0],
                           "resolution": 1, "max_trials": 10, "timeout_s": 5}
    else:
        sweep["values"] = values
    instance = create_registry().bind("scheduler", name, {**settings, "sweep": sweep, "repetitions": 1}).plugin
    assert isinstance(instance, SchedulerBase)
    instance.validate_metrics(StandardMetrics({}))
    plans, facts = [], []

    class Host(Trials):
        @asynccontextmanager
        async def open(self, plan, strategy):
            plans.append(plan)
            async def sessions():
                for index in range(2):
                    session = Session(id=f"s{index}", turns=[
                        Turn(id=f"s{index}t{turn}", ordinal=turn, request_payload_ref=PayloadRef(0, 1))
                        for turn in range(3 if name.startswith("multi-turn") else 1)
                    ])
                    session.link()
                    yield session
            class Runner:
                async def run(self, session, admission):
                    for _ in session.turns:
                        ticket = await strategy.attempts.offer()
                        lease = await strategy.attempts.admit(ticket)
                        lease.release()
                async def cancel(self, session, reason):
                    pass
                async def record_queue(self, kind, value):
                    pass
                def record_scheduling(self, fact):
                    facts.append(fact)
                async def worker_started(self, index):
                    pass
                async def worker_stopped(self, index):
                    pass
            execution = TrialExecution(sessions(), Runner())
            yield execution
            execution._outcome = outcome(plan.comparison.axis_value, len(plans))

    host = Host()
    await instance.schedule(host, SchedulerContext(CancellationToken()))
    assert host.records[-1].status == ("upper_limit_passed" if adaptive else "completed")
    assert {p.comparison.axis_value for p in plans} == set(values)
    assert all(f.completed_sessions == 2 for f in facts)
    assert len([r for r in host.records if r.checks]) == len(plans)


@pytest.mark.parametrize("timeout", [10, None])
async def test_token_cancellation_stops_before_next_trial(monkeypatch, timeout):
    instance, host = plugin(search={"timeout_s": timeout}), Trials()
    token = CancellationToken()
    calls = []
    async def execute(plan, trials, ctx):
        calls.append(plan)
        token.cancel("user stop")
        return SimpleNamespace(outcome=outcome(2, 1))
    monkeypatch.setattr(instance, "execute_plan", execute)
    await instance.schedule(host, SchedulerContext(token))
    assert host.records[-1].status == "cancelled"
    assert len(calls) == 1


async def test_search_writer_failure_propagates(monkeypatch):
    instance = plugin()
    class Host(Trials):
        async def record_sweep(self, record):
            if record.phase == "probe":
                raise RuntimeError("writer broken")
            await super().record_sweep(record)
    host = Host()
    calls = []
    async def execute(plan, trials, ctx):
        calls.append(plan)
        return SimpleNamespace(outcome=outcome(2, 1))
    monkeypatch.setattr(instance, "execute_plan", execute)
    with pytest.raises(RuntimeError, match="writer broken"):
        await instance.schedule(host, SchedulerContext(CancellationToken()))
    assert len(calls) == 1


@pytest.mark.parametrize("value", [2, 32])
@pytest.mark.parametrize("change,reason", [
    ({"demand_limited": True}, "scheduler_lag_tolerance_not_configured"),
    ({"completed_attempts": 999}, "incomplete_attempt_coverage"),
    ({"completed_attempts": 0}, "no_completed_attempts"),
    ({"admitted_sessions": 999}, "incomplete_session_coverage"),
    ({"demand_exhausted": True, "termination_reason": "duration_elapsed"}, "premature_demand_exhaustion"),
])
async def test_invalid_load_cannot_establish_either_capacity_boundary(monkeypatch, value, change, reason):
    records, _ = await run(plugin(search={"initial": value}), monkeypatch,
                          lambda r, p, n: replace(r, scheduling=replace(r.scheduling, **change),
                              validity=TrialValidity(r.trial_id, r.status, "indeterminate", (reason,))))
    assert records[-1].status == "indeterminate"
    assert records[-1].lower is None and records[-1].upper is None
    probe = next(r for r in records if r.phase == "probe")
    assert reason in probe.validity[0].reasons
    assert probe.checks[0].status == ("pass" if value == 2 else "fail")


async def test_failed_execution_cannot_pass_even_with_good_slo(monkeypatch):
    records, _ = await run(plugin(), monkeypatch, lambda r, p, n: replace(r, status="failed", validity=TrialValidity(r.trial_id, "failed", "indeterminate", ("execution_failed",))))
    assert records[-1].status == "indeterminate"
    assert records[2].validity[0].execution_status == "failed"
    assert "execution_failed" in records[2].validity[0].reasons


async def test_missing_rule_evidence_overrides_other_rule_failure(monkeypatch):
    records, _ = await run(plugin(search={"initial": 32}), monkeypatch,
                          lambda r, p, n: replace(r, points=r.points[:1]))
    assert records[-1].status == "indeterminate"
    assert records[-1].upper is None
    assert records[2].checks[0].status == "fail"


def test_sweep_defaults_to_three_predeclared_repetitions():
    instance = create_registry().bind("scheduler", "single-turn-closed-loop", {
        "sweep": {"axis": "max_concurrency", "values": [1, 2]},
    }).plugin
    assert isinstance(instance, SchedulerBase)
    assert instance.settings["repetitions"] == 3


async def test_service_failed_trial_can_establish_a_valid_slo_failure(monkeypatch):
    records, _ = await run(plugin(), monkeypatch, lambda r, p, n: replace(r, status='failed'))
    assert records[-1].status == 'boundary_confirmed'
    assert any(row.status == 'fail' and row.validity[0].execution_status == 'succeeded' for row in records if row.validity)
