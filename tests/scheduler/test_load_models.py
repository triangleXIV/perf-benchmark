import asyncio
from contextlib import asynccontextmanager
from time import perf_counter_ns

import pytest

from perf_benchmark.core.models import Session, Turn
from perf_benchmark.pipeline.context import SchedulerContext
from perf_benchmark.pipeline.contracts.facts import (
    LoadProfile,
    ResourceLimits,
    TrialSchedulingFact,
)
from perf_benchmark.pipeline.contracts.scheduler import StopPlan, TrialExecution, TrialPlan, TrialOutcome
from perf_benchmark.pipeline.preparation.payloads import PayloadRef
from perf_benchmark.pipeline.scheduling.mechanics import (
    BoundedAttemptGate,
    RateClock,
    SchedulerOverloaded,
    StandardTurnStrategy,
    SchedulerBase,
    parse_common_settings,
)
from perf_benchmark.plugins import create_registry
from perf_benchmark.pipeline.contracts.executor import AttemptResult
from perf_benchmark.runtime.cancellation import CancellationToken


async def _sessions(count, turns=1):
    for index in range(count):
        session = Session(
            id=f"s{index}",
            turns=[
                Turn(
                    id=f"s{index}-t{turn}",
                    ordinal=turn,
                    request_payload_ref=PayloadRef(0, 1),
                )
                for turn in range(turns)
            ],
        )
        session.link()
        yield session


class _Trials:
    def __init__(self, count, turns=1, delay=0.001, execute_attempts=False):
        self.count, self.turns, self.delay = count, turns, delay
        self.execute_attempts = execute_attempts
        self.plans, self.active, self.maximum = [], 0, 0
        self.scheduling = []
        self.session_admissions, self.attempt_admissions = [], []

    @asynccontextmanager
    async def open(self, plan, strategy):
        self.plans.append(plan)
        owner = self

        class Runner:
            async def run(self, session, admission):
                session.measurements.target_ns = admission.target_ns
                session.measurements.offered_ns = admission.offered_ns
                owner.session_admissions.append(admission)
                owner.active += 1
                owner.maximum = max(owner.maximum, owner.active)
                if owner.execute_attempts:
                    for _ in session.turns:
                        ticket = await strategy.attempts.offer()
                        lease = await strategy.attempts.admit(ticket)
                        owner.attempt_admissions.append(lease)
                        lease.release()
                await asyncio.sleep(owner.delay)
                owner.active -= 1

            async def cancel(self, session, reason):
                pass

            async def record_queue(self, kind, value):
                pass

            def record_scheduling(self, fact):
                owner.scheduling.append(fact)

            async def worker_started(self, index):
                pass

            async def worker_stopped(self, index):
                pass

        execution = TrialExecution(_sessions(self.count, self.turns), Runner())
        yield execution
        execution._outcome = TrialOutcome(str(len(self.plans)), "succeeded", self.scheduling[-1], ())

    async def record_sweep(self, record):
        pass


def _ctx():
    return SchedulerContext(CancellationToken())


def _plugin(name, settings):
    plugin = create_registry().bind("scheduler", name, settings).plugin
    assert isinstance(plugin, SchedulerBase)
    return plugin


async def test_single_turn_closed_loop_merges_serial_sweep():
    settings = {
        "sweep": {"axis": "max_concurrency", "values": [1, 3]},
        "repetitions": 2,
        "stop": {"session_count": 4},
        "on_exhausted": "error",
    }
    trials = _Trials(4)
    await _plugin("single-turn-closed-loop", settings).schedule(trials, _ctx())

    assert [plan.repetition.index for plan in trials.plans] == [1, 2, 1, 2]
    assert [plan.load_profile.max_concurrency for plan in trials.plans] == [1, 1, 3, 3]
    assert all(fact.completed_sessions == 4 for fact in trials.scheduling)


@pytest.mark.parametrize("turns", [1, 2, 7])
async def test_multi_turn_closed_loop_separates_active_sessions_from_attempt_limit(turns):
    settings = {
        "max_active_sessions": 100,
        "max_concurrency": 10,
        "stop": {"session_count": 100},
        "on_exhausted": "error",
    }
    trials = _Trials(100, turns=turns, delay=0.01)
    await _plugin("multi-turn-closed-loop", settings).schedule(trials, _ctx())

    assert trials.maximum == 100
    assert trials.plans[0].load_profile.max_active_sessions == 100
    assert trials.plans[0].load_profile.max_concurrency == 10


async def test_fixed_pool_duration_preserves_admitted_sessions_after_source_end():
    trials = _Trials(32, turns=2, delay=0.06)
    await _plugin("multi-turn-closed-loop", {
        "max_active_sessions": 32, "max_concurrency": 32,
        "stop": {"duration_s": 0.03}, "on_exhausted": "stop",
    }).schedule(trials, _ctx())
    (fact,) = trials.scheduling
    assert trials.maximum == 32
    assert fact.offered_sessions == fact.admitted_sessions == fact.completed_sessions == 32
    assert fact.demand_exhausted and fact.termination_reason == "source_exhausted"


@pytest.mark.parametrize("turns", [2, 5])
async def test_multi_turn_open_loop_schedules_both_rates_and_reports_final_fact(turns):
    settings = {
        "session_rate": 200,
        "request_rate": 100,
        "max_active_sessions": 1,
        "max_concurrency": 1,
        "max_pending_sessions": 4,
        "max_pending_requests": 4,
        "stop": {"session_count": 3},
        "on_exhausted": "error",
    }
    trials = _Trials(3, turns=turns, delay=0, execute_attempts=True)

    await _plugin("multi-turn-open-loop", settings).schedule(trials, _ctx())

    (plan,) = trials.plans
    assert plan.load_profile.target_session_rate == 200
    assert plan.load_profile.target_request_rate == 100
    assert [
        right.target_ns - left.target_ns
        for left, right in zip(trials.session_admissions, trials.session_admissions[1:])
    ] == [5_000_000, 5_000_000]
    assert [
        second.target_ns - first.target_ns
        for first, second in zip(
            trials.attempt_admissions[::turns], trials.attempt_admissions[1::turns]
        )
    ] == [10_000_000] * 3

    (fact,) = trials.scheduling
    assert isinstance(fact, TrialSchedulingFact)
    assert (
        fact.offered_sessions,
        fact.admitted_sessions,
        fact.completed_sessions,
    ) == (3, 3, 3)
    assert (
        fact.offered_attempts,
        fact.admitted_attempts,
        fact.completed_attempts,
    ) == (3 * turns, 3 * turns, 3 * turns)
    assert fact.demand_exhausted is True
    assert fact.demand_limited is True
    assert fact.termination_reason == "session_count_reached"


def test_registry_exposes_explicit_scheduler_plugins():
    assert create_registry().names("scheduler") == (
        "multi-turn-closed-loop",
        "multi-turn-open-loop",
        "single-turn-closed-loop",
        "single-turn-open-loop",
        "trace-causal",
        "trace-timeline",
    )


@pytest.mark.parametrize("name,settings,turns", [
    ("single-turn-closed-loop", {"max_concurrency": 1}, 2),
    ("single-turn-open-loop", {"request_rate": 10, "max_pending_requests": 2}, 3),
    ("multi-turn-closed-loop", {"max_active_sessions": 1, "max_concurrency": 1}, 0),
    ("multi-turn-open-loop", {"session_rate": 10, "request_rate": 10,
                              "max_pending_sessions": 2, "max_pending_requests": 2}, 0),
])
async def test_schedulers_reject_invalid_session_shapes(name, settings, turns):
    with pytest.raises(ValueError, match="received a Session"):
        await _plugin(name, settings).schedule(_Trials(1, turns=turns), _ctx())


@pytest.mark.parametrize(
    "name,settings",
    [
        ("single-turn-closed-loop", {"max_concurrency": 1}),
        ("single-turn-open-loop", {"request_rate": 1, "max_pending_requests": 1}),
        ("multi-turn-closed-loop", {"max_active_sessions": 2, "max_concurrency": 1}),
        (
            "multi-turn-open-loop",
            {
                "session_rate": 1,
                "request_rate": 1,
                "max_pending_sessions": 1,
                "max_pending_requests": 1,
            },
        ),
    ],
)
def test_all_scheduler_plugins_allow_selector_sized_default_stop(name, settings):
    binding = create_registry().bind("scheduler", name, settings)
    assert isinstance(binding.plugin, SchedulerBase)
    assert tuple(binding.plugin.trial_plans())[0].stop == StopPlan()


@pytest.mark.parametrize(
    "name,settings,message",
    [
        (
            "single-turn-closed-loop",
            {"max_concurrency": 1, "on_exhausted": "cycle"},
            "explicit stop",
        ),
        (
            "single-turn-open-loop",
            {
                "request_rate": 1,
                "max_pending_requests": 1,
                "stop": {"session_count": 1},
                "on_exhausted": "error",
                "max_concurrency": 0,
            },
            "max_concurrency",
        ),
        (
            "multi-turn-closed-loop",
            {
                "max_active_sessions": 2,
                "max_concurrency": 3,
                "stop": {"session_count": 1},
                "on_exhausted": "error",
            },
            "greater",
        ),
        (
            "multi-turn-open-loop",
            {
                "session_rate": 1,
                "request_rate": 1,
                "max_pending_sessions": 1,
                "max_pending_requests": 1,
                "stop": {"session_count": 1, "duration_s": 1},
                "on_exhausted": "cycle",
            },
            "exactly one",
        ),
        (
            "single-turn-closed-loop",
            {"max_concurrency": 1, "mystery": True},
            "unknown scheduler settings",
        ),
    ],
)
def test_configuration_is_strict(name, settings, message):
    with pytest.raises(ValueError, match=message):
        create_registry().bind("scheduler", name, settings)


async def test_attempt_gate_is_fifo_and_concurrency_is_optional():
    gate = BoundedAttemptGate(2, 1, None, None)
    first, second = await gate.offer(), await gate.offer()
    lease = await gate.admit(first)
    waiting = asyncio.create_task(gate.admit(second))
    await asyncio.sleep(0)
    assert not waiting.done()
    lease.release()
    second_lease = await waiting
    assert first.offered_ns <= second.offered_ns <= second_lease.admitted_ns
    second_lease.release()

    unbounded = BoundedAttemptGate(1, None, None, None)
    ticket = await unbounded.offer()
    assert (await unbounded.admit(ticket)).semaphore is None


async def test_pending_overflow_stops_arrivals_but_keeps_accepted_ticket():
    gate = BoundedAttemptGate(1, 1, None, None)
    accepted = await gate.offer()
    with pytest.raises(Exception, match="attempt_pending_overflow"):
        await gate.offer()
    assert isinstance(gate.failure, SchedulerOverloaded)
    lease = await gate.admit(accepted)
    lease.release()


async def test_rate_clock_preserves_absolute_targets_when_late():
    clock = RateClock(1000, "constant", 0)
    first = await clock.arrive(None)
    second = await clock.arrive(None)
    assert second.target_ns - first.target_ns == 1_000_000
    clock.next_ns = target = perf_counter_ns() - 10_000_000
    late = await clock.arrive(None)
    assert late.target_ns == target
    assert late.offered_ns > late.target_ns
    assert clock.next_ns == target + 1_000_000
    assert clock.late_arrivals >= 1


async def test_poisson_seed_reproduces_intervals():
    left, right = RateClock(100000, "poisson", 7), RateClock(100000, "poisson", 7)
    assert [left.interval_ns() for _ in range(4)] == [right.interval_ns() for _ in range(4)]


async def test_duration_stops_retry_at_deadline_without_discarding_failed_attempt():
    settings = {
        "retry": {"max_attempts": 2, "delay_s": 60},
        "control": {},
        "continue_on_cancel": False,
        "retain_partial": False,
    }
    plan = TrialPlan(
        "duration",
        StopPlan(duration_s=0.01),
        ResourceLimits(1, 1, 1),
        LoadProfile(
            "multi-turn-closed-loop",
            target_concurrency=1,
            max_active_sessions=1,
            max_concurrency=1,
        ),
    )
    strategy = StandardTurnStrategy(
        parse_common_settings(settings, allowed=set()), plan, perf_counter_ns(),
        seed=0, reuse_session_arrival=False,
    )
    failed = AttemptResult("turn", 1, status="failed", retryable=True, recoverable=True)

    class Control:
        stop_reason = None
        session_ordinal = 0

        async def send(self):
            return failed

        async def retry(self, delay):
            raise AssertionError("retry must not be offered after the duration deadline")

    control = Control()
    assert await strategy.run_turn(Turn(request_payload_ref=PayloadRef(0, 0), id="turn-fixture-365"), control) is failed
    assert control.stop_reason == "duration_elapsed"


@pytest.mark.parametrize("mode", ["single-turn-open-loop", "external-bursty-model"])
@pytest.mark.parametrize("reuse", [False, True])
async def test_arrival_reuse_is_explicit_policy_independent_of_mode(mode, reuse):
    plan = TrialPlan("custom", StopPlan(session_count=1), ResourceLimits(2, 2, 2),
                     LoadProfile(mode, target_session_rate=10, target_request_rate=20, max_concurrency=1))
    strategy = StandardTurnStrategy({}, plan, perf_counter_ns(), seed=0, reuse_session_arrival=reuse)
    assert (strategy.attempts.clock is strategy.session_clock) is reuse
    arrival = await strategy.offer_session(Session(turns=[], id="session-fixture-376"))
    ticket = await strategy.attempts.offer(arrival)
    lease = await strategy.attempts.admit(ticket)
    assert lease.target_ns == arrival.target_ns
    # Independent clocks share the Trial origin but advance at different rates.
    assert strategy.attempts.clock is not None and strategy.session_clock is not None
    assert strategy.attempts.clock.next_ns is not None and strategy.session_clock.next_ns is not None
    assert strategy.attempts.clock.next_ns - strategy.session_clock.next_ns == (0 if reuse else -50_000_000)
    lease.release()


async def test_scheduler_base_uses_plugin_strategy_factory():
    from dataclasses import replace

    original = _plugin("single-turn-closed-loop", {"max_concurrency": 1, "stop": {"session_count": 2}, "on_exhausted": "error"})
    plan = replace(next(iter(original.trial_plans())), load_profile=LoadProfile(
        "external-model", max_active_sessions=1, max_concurrency=1, target_concurrency=1))
    selected = []

    class CustomStrategy(StandardTurnStrategy):
        async def offer_session(self, session):
            selected.append("offered")
            return await super().offer_session(session)

    class CustomScheduler(SchedulerBase):
        expected_turns = 1

        def trial_plans(self):
            yield plan

        def create_strategy(self, plan, started_ns, *, seed):
            assert plan.load_profile.mode == "external-model"
            selected.append("created")
            return CustomStrategy(self.settings, plan, started_ns, seed=seed, reuse_session_arrival=False)

    trials = _Trials(2)
    await CustomScheduler(original.settings).schedule(trials, _ctx())
    assert selected == ["created", "offered", "offered"]
    assert trials.scheduling[0].completed_sessions == 2


def test_gamma_arrivals_have_seeded_unscaled_intervals():
    import random
    generator = random.Random(7)
    clock = RateClock(100, "gamma", 7, gamma_shape=0.3)
    assert [clock.interval_ns() for _ in range(20)] == [
        max(1, int(generator.gammavariate(0.3, 1 / 30) * 1e9)) for _ in range(20)
    ]


@pytest.mark.parametrize("end", [0.2, 1.0, 4.0])
def test_linear_ramp_follows_elapsed_time_and_integrated_rate(end):
    clock = RateClock(10, "constant", 0, started_ns=0,
                      ramp={"duration_s": 2.0, "end_multiplier": end})
    for _ in range(100):
        assert clock.next_ns is not None
        start = clock.next_ns / 1e9
        interval = clock.interval_ns()
        finish = start + interval / 1e9
        def integral(t):
            ramp_t = min(t, 2.0)
            return ramp_t + (end - 1) / 4 * ramp_t**2 + max(0, t - 2) * end
        assert integral(finish) - integral(start) == pytest.approx(0.1, abs=4e-9)
        clock.next_ns += interval


async def test_cancellation_interrupts_long_arrival_wait_without_admission():
    token = CancellationToken()
    trial = _Trials(10, delay=0)
    scheduler = _plugin('single-turn-open-loop', {
        'request_rate': 0.001, 'max_concurrency': 1, 'max_pending_requests': 1,
    })
    task = asyncio.create_task(scheduler.schedule(trial, SchedulerContext(token)))
    while not trial.session_admissions:
        await asyncio.sleep(0)
    token.cancel('user_stop')
    await asyncio.wait_for(task, 1)
    assert trial.scheduling[0].offered_sessions == 1
    assert trial.scheduling[0].termination_reason == 'user_stop'
