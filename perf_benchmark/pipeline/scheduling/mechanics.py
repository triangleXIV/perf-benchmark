"""Common scheduler runtime: clocks, admission, retry and bounded workers."""

from __future__ import annotations

from perf_benchmark.core.validation import positive_int, positive_number

import asyncio
from collections import deque
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field, replace
import random
import math
from time import perf_counter_ns
from typing import cast

from perf_benchmark.core.models import Session, Turn, thaw_snapshot
from perf_benchmark.pipeline.context import SchedulerContext
from perf_benchmark.pipeline.contracts.facts import TrialSchedulingFact, RepetitionPlan, ValidityRequirements
from perf_benchmark.pipeline.contracts.scheduler import (
    AttemptTicket as TicketIdentity,
    NextTurn,
    SchedulerPlugin,
    SchedulingStopped,
    SessionAdmission,
    StopPlan,
    TrialExecution,
    TrialHost,
    TrialPlan,
    TurnEligibility,
)
from perf_benchmark.pipeline.resources import finish_task
from perf_benchmark.pipeline.contracts.executor import AttemptPlan, AttemptResult
from perf_benchmark.pipeline.scheduling.scenarios import cancellation_trigger, parse_scenarios, selected
from perf_benchmark.runtime.cancellation import CancellationToken


class SchedulerOverloaded(RuntimeError):
    pass


def parse_common_settings(
    settings: dict,
    *,
    allowed: set[str],
    open_loop: bool = False,
) -> dict:
    if not isinstance(settings, dict):
        raise ValueError("scheduler settings must be an object")
    common = {
        "stop", "repetitions", "validity",
        "on_exhausted",
        "retry",
        "cancel",
        "control",
        "continue_on_cancel",
        "retain_partial",
    }
    if open_loop:
        common.update({"arrival_distribution", "gamma_shape", "arrival_ramp"})
    unknown = set(settings) - common - allowed
    if unknown:
        raise ValueError(f"unknown scheduler settings: {', '.join(sorted(unknown))}")
    parsed = thaw_snapshot(settings)
    if "validity" in parsed:
        if not isinstance(parsed["validity"], dict):
            raise ValueError("validity must be an object")
        ValidityRequirements(**parsed["validity"])
    positive_int(parsed.setdefault("repetitions", 1), "repetitions")
    if "stop" not in parsed:
        if "on_exhausted" in parsed:
            raise ValueError("on_exhausted requires an explicit stop")
    else:
        stop = parsed["stop"]
        if not isinstance(stop, dict) or set(stop) not in ({"session_count"}, {"duration_s"}):
            raise ValueError("stop must contain exactly one of session_count or duration_s")
        on_exhausted = parsed.get("on_exhausted")
        if not isinstance(on_exhausted, str):
            raise ValueError("on_exhausted must be cycle, error or stop")
        StopPlan(**stop, on_exhausted=on_exhausted)
    control = parsed.setdefault("control", {})
    if not isinstance(control, dict):
        raise ValueError("control must be an object")
    AttemptPlan(**control)
    parse_scenarios(parsed)
    for name in ("continue_on_cancel", "retain_partial"):
        value = parsed.setdefault(name, False)
        if not isinstance(value, bool):
            raise ValueError(f"{name} must be a boolean")
    if open_loop:
        distribution = parsed.setdefault("arrival_distribution", "constant")
        if distribution not in {"constant", "poisson", "gamma"}:
            raise ValueError("arrival_distribution must be constant, poisson or gamma")
        if "gamma_shape" in parsed and distribution != "gamma":
            raise ValueError("gamma_shape requires gamma arrivals")
        if distribution == "gamma":
            positive_number(parsed.get("gamma_shape"), "gamma_shape")
        if "arrival_ramp" in parsed:
            ramp = parsed["arrival_ramp"]
            if not isinstance(ramp, dict) or set(ramp) != {"duration_s", "end_multiplier"}:
                raise ValueError("arrival_ramp requires duration_s and end_multiplier")
            for key, value in ramp.items():
                positive_number(value, key)
    return parsed


def stop_plan(settings: Mapping[str, object]) -> StopPlan:
    value = settings.get("stop")
    if value is None:
        return StopPlan()
    assert isinstance(value, Mapping)
    return StopPlan(**value, on_exhausted=str(settings["on_exhausted"]))


class RateClock:
    def __init__(self, rate: float, distribution: str, seed: int, *,
                 started_ns: int | None = None, gamma_shape: float = 1.0,
                 ramp: Mapping[str, float] | None = None):
        self.rate, self.distribution = rate, distribution
        self.random = random.Random(seed)
        self.next_ns = self.started_ns = started_ns
        self.gamma_shape, self.ramp = gamma_shape, ramp
        self.lock = asyncio.Lock()
        self.late_arrivals = 0

    def interval_ns(self) -> int:
        seconds = (
            1.0 / self.rate
            if self.distribution == "constant"
            else self.random.gammavariate(self.gamma_shape, 1.0 / (self.rate * self.gamma_shape))
            if self.distribution == "gamma"
            else self.random.expovariate(self.rate)
        )
        if self.ramp is not None:
            duration, end = self.ramp["duration_s"], self.ramp["end_multiplier"]
            elapsed = ((self.next_ns or 0) - (self.started_ns or 0)) / 1e9
            slope = (end - 1) / duration
            remaining = max(0.0, duration - elapsed)
            current = 1 + slope * min(elapsed, duration)
            area = current * remaining + slope * remaining * remaining / 2
            if seconds >= area:
                seconds = remaining + (seconds - area) / end
            else:
                # Solve integrated linear rate; stable also for a decreasing ramp.
                seconds = 2 * seconds / (current + math.sqrt(current * current + 2 * slope * seconds))
        return max(1, int(seconds * 1e9))

    async def arrive(self, deadline_ns: int | None) -> SessionAdmission:
        async with self.lock:
            now = perf_counter_ns()
            if deadline_ns is not None and now >= deadline_ns:
                raise SchedulingStopped("duration_elapsed")
            if self.next_ns is None:
                self.next_ns = self.started_ns = now
            target = self.next_ns
            if target < now:
                self.late_arrivals += 1
            if deadline_ns is not None and target >= deadline_ns:
                raise SchedulingStopped("duration_elapsed")
            if target > now:
                await asyncio.sleep((target - now) / 1e9)
            self.next_ns = target + self.interval_ns()
            return SessionAdmission(target, perf_counter_ns())


@dataclass(slots=True, eq=False)
class AttemptTicket:
    target_ns: int
    offered_ns: int
    queued: bool = True
    at_head: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(slots=True)
class AttemptLease:
    target_ns: int
    offered_ns: int
    admitted_ns: int
    semaphore: asyncio.Semaphore | None
    completed: Callable[[], None]
    released: bool = False

    def release(self) -> None:
        if self.released:
            return
        self.released = True
        if self.semaphore is not None:
            self.semaphore.release()
        self.completed()


class BoundedAttemptGate:
    def __init__(
        self,
        capacity: int,
        concurrency: int | None,
        clock: RateClock | None,
        deadline_ns: int | None,
        *,
        reuse_session_arrival: bool = False,
    ) -> None:
        self.capacity, self.clock, self.deadline_ns = capacity, clock, deadline_ns
        self.reuse_session_arrival = reuse_session_arrival
        self.semaphore = asyncio.Semaphore(concurrency) if concurrency is not None else None
        self.queue: deque[AttemptTicket] = deque()
        self.lock = asyncio.Lock()
        self.failure: SchedulerOverloaded | None = None
        self.failed = asyncio.Event()
        self.offered = self.admitted = self.completed = 0
        self.last_offered_ns: int | None = None

    async def offer(self, preset: SessionAdmission | None = None) -> AttemptTicket:
        if self.failure is not None:
            raise SchedulingStopped("attempt_pending_overflow")
        arrival = (
            preset
            if preset is not None and self.reuse_session_arrival
            else await self.clock.arrive(self.deadline_ns)
            if self.clock is not None
            else SessionAdmission(perf_counter_ns(), perf_counter_ns())
        )
        if self.deadline_ns is not None and arrival.offered_ns >= self.deadline_ns:
            raise SchedulingStopped("duration_elapsed")
        async with self.lock:
            if len(self.queue) >= self.capacity:
                self.failure = SchedulerOverloaded(
                    f"Attempt pending queue exceeded max_pending_requests={self.capacity}"
                )
                self.failed.set()
                raise SchedulingStopped("attempt_pending_overflow")
            ticket = AttemptTicket(arrival.target_ns, arrival.offered_ns)
            self.queue.append(ticket)
            if len(self.queue) == 1:
                ticket.at_head.set()
            self.offered += 1
            self.last_offered_ns = arrival.offered_ns
            return ticket

    async def admit(self, ticket: TicketIdentity) -> AttemptLease:
        ticket = cast(AttemptTicket, ticket)
        # Only the next eligible ticket wakes. Broadcasting every queue change
        # to thousands of pending requests makes FIFO admission quadratic.
        await ticket.at_head.wait()
        async with self.lock:
            if ticket.queued:
                assert self.queue[0] is ticket
                self.queue.popleft()
                ticket.queued = False
                if self.queue:
                    self.queue[0].at_head.set()
        if self.semaphore is not None:
            await self.semaphore.acquire()
        self.admitted += 1
        return AttemptLease(
            ticket.target_ns,
            ticket.offered_ns,
            perf_counter_ns(),
            self.semaphore,
            self._completed,
        )

    async def cancel(self, ticket: TicketIdentity) -> None:
        ticket = cast(AttemptTicket, ticket)
        async with self.lock:
            if ticket.queued:
                was_head = self.queue[0] is ticket
                self.queue.remove(ticket)
                ticket.queued = False
                ticket.at_head.set()
                if was_head and self.queue:
                    self.queue[0].at_head.set()

    def _completed(self) -> None:
        self.completed += 1


class StandardTurnStrategy:
    def __init__(self, settings: Mapping[str, object], plan: TrialPlan, started_ns: int,
                 *, seed: int, reuse_session_arrival: bool):
        self.settings, self.plan = settings, plan
        self.deadline_ns = (
            started_ns + int(plan.stop.duration_s * 1e9)
            if plan.stop.duration_s is not None
            else None
        )
        self.seed = seed
        self.cancellation: CancellationToken | None = None
        distribution = str(settings.get("arrival_distribution", "constant"))
        gamma_shape = float(cast(float, settings.get("gamma_shape", 1.0)))
        ramp = cast(Mapping[str, float] | None, settings.get("arrival_ramp"))
        session_rate = plan.load_profile.target_session_rate
        request_rate = plan.load_profile.target_request_rate
        self.session_clock = (
            RateClock(session_rate, distribution, seed + 1, started_ns=started_ns, gamma_shape=gamma_shape, ramp=ramp)
            if session_rate is not None
            else None
        )
        request_clock = (
            self.session_clock
            if reuse_session_arrival
            else RateClock(request_rate, distribution, seed + 2, started_ns=started_ns, gamma_shape=gamma_shape, ramp=ramp)
            if request_rate is not None
            else None
        )
        self.attempts = BoundedAttemptGate(
            plan.resources.max_pending_requests,
            plan.load_profile.max_concurrency,
            request_clock,
            self.deadline_ns,
            reuse_session_arrival=reuse_session_arrival,
        )

    def start_clock(self, started_ns: int) -> None:
        self.deadline_ns = started_ns + int(self.plan.stop.duration_s * 1e9) if self.plan.stop.duration_s else None
        for clock in (self.session_clock, self.attempts.clock):
            if clock is not None:
                clock.next_ns = clock.started_ns = started_ns
        self.attempts.deadline_ns = self.deadline_ns

    async def offer_session(self, session: Session) -> SessionAdmission:
        if self.attempts.failed.is_set():
            raise SchedulingStopped("attempt_pending_overflow")
        if self.session_clock is None:
            now = perf_counter_ns()
            return SessionAdmission(now, now)
        return await self.wait_arrival(self.session_clock.arrive(self.deadline_ns))

    async def wait_arrival(self, arrival):
        arrival_task = asyncio.create_task(arrival)
        failure_task = asyncio.create_task(self.attempts.failed.wait())
        cancel_task = asyncio.create_task(self.cancellation.wait()) if self.cancellation is not None else None
        tasks = [arrival_task, failure_task, *([cancel_task] if cancel_task is not None else [])]
        try:
            done, _ = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            if cancel_task in done:
                assert self.cancellation is not None
                raise SchedulingStopped(self.cancellation.reason)
            if failure_task in done:
                raise SchedulingStopped("attempt_pending_overflow")
            return arrival_task.result()
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def wait_turn(self, turn: Turn) -> TurnEligibility:
        raise ValueError("causal input requires a causal Scheduler")

    def begin_turn(self, turn: Turn) -> str | None:
        if self.deadline_ns is not None and perf_counter_ns() >= self.deadline_ns:
            return "duration_elapsed"
        return None

    def plan_attempt(self, turn: Turn, attempt: int, *, session_ordinal: int) -> AttemptPlan:
        control = self.settings.get("control", {})
        assert isinstance(control, Mapping)
        cancel = self.settings.get("cancel")
        if cancel is None:
            return AttemptPlan(**control)
        assert isinstance(cancel, Mapping)
        trigger = cancellation_trigger(cancel, turn, attempt, session_ordinal, self.seed)
        return AttemptPlan(**{**control, **trigger})

    def retry_delay(self, turn: Turn, result: AttemptResult, *, session_ordinal: int) -> float | None:
        retry = self.settings["retry"]
        assert isinstance(retry, Mapping)
        return (
            float(retry["delay_s"])
            if result.status in retry["on_status"]
            and (result.status == "succeeded" or result.retryable)
            and result.recoverable
            and result.fatal_error is None
            and result.attempt < retry["max_attempts"]
            and selected(retry, turn, result.attempt, "retry", session_ordinal, self.seed)
            else None
        )

    async def run_turn(self, turn: Turn, control) -> AttemptResult | None:
        result = await control.send()
        while result is not None and (delay := self.retry_delay(
            turn, result, session_ordinal=control.session_ordinal,
        )) is not None:
            if self.deadline_ns is not None:
                remaining = max(0.0, (self.deadline_ns - perf_counter_ns()) / 1e9)
                if delay >= remaining:
                    if remaining:
                        await asyncio.sleep(remaining)
                    control.stop_reason = "duration_elapsed"
                    return result
            retried = await control.retry(delay)
            if retried is None:
                return result
            result = retried
        return result

    def next_turn(self, turn: Turn, result: AttemptResult) -> NextTurn:
        if self.deadline_ns is not None and perf_counter_ns() >= self.deadline_ns:
            return NextTurn(False, cancel_reason="duration_elapsed")
        proceed = result.status == "succeeded" or (
            result.status == "cancelled" and bool(self.settings["continue_on_cancel"])
        )
        return NextTurn(proceed, bool(self.settings["retain_partial"]))

    def demand_limited(self) -> bool:
        clocks = {
            id(clock): clock
            for clock in (self.session_clock, self.attempts.clock)
            if clock is not None
        }
        return any(clock.late_arrivals for clock in clocks.values())


async def run_workload(
    execution: TrialExecution,
    strategy: StandardTurnStrategy,
    plan: TrialPlan,
    ctx: SchedulerContext,
    *,
    expected_turns: int | None,
) -> None:
    active_limit = plan.load_profile.max_active_sessions
    pending_limit = plan.resources.max_pending_sessions
    open_loop = (
        plan.load_profile.target_session_rate is not None
        or plan.load_profile.target_request_rate is not None
        or plan.load_profile.arrival_distribution == "timeline"
    )
    offering_started_ns = perf_counter_ns()
    strategy.start_clock(offering_started_ns)
    strategy.cancellation = ctx.cancellation
    queue: asyncio.Queue = asyncio.Queue(maxsize=pending_limit)
    stop = object()
    producer_failure: BaseException | None = None
    producer_done_ns: int | None = None
    offered_sessions = admitted_sessions = completed_sessions = 0
    demand_exhausted = False
    termination_reason: str | None = None
    aborting = False

    async def produce() -> None:
        nonlocal producer_failure, producer_done_ns, offered_sessions
        nonlocal demand_exhausted, termination_reason
        try:
            async for session in execution.sessions:
                if ctx.cancellation.is_cancelled():
                    termination_reason = ctx.cancellation.reason
                    break
                if strategy.deadline_ns is not None and perf_counter_ns() >= strategy.deadline_ns:
                    termination_reason = "duration_elapsed"
                    break
                if not session.turns or (
                    expected_turns is not None and len(session.turns) != expected_turns
                ):
                    raise ValueError(
                        f"{plan.load_profile.mode} received a Session with "
                        f"{len(session.turns)} Turns; expected {expected_turns or 'at least 1'}"
                    )
                admission = await strategy.offer_session(session)
                try:
                    queue.put_nowait((session, admission))
                except asyncio.QueueFull:
                    if not open_loop:
                        await queue.put((session, admission))
                    else:
                        producer_failure = SchedulerOverloaded(
                            "Session pending queue exceeded "
                            f"max_pending_sessions={pending_limit}"
                        )
                        termination_reason = "session_pending_overflow"
                        break
                offered_sessions += 1
                await execution.runner.record_queue("session_pending", queue.qsize())
            else:
                demand_exhausted = True
                termination_reason = (
                    "session_count_reached"
                    if plan.stop.session_count is not None
                    else "source_exhausted"
                )
        except SchedulingStopped as exc:
            termination_reason = exc.reason
        except BaseException as exc:
            producer_failure = exc
        finally:
            producer_done_ns = perf_counter_ns()
            if not aborting:
                for _ in range(active_limit or 1):
                    await queue.put(stop)

    async def run_one(session: Session, admission: SessionAdmission) -> None:
        nonlocal admitted_sessions, completed_sessions
        admitted_sessions += 1
        await execution.runner.run(session, admission)
        completed_sessions += 1

    async def worker(index: int) -> None:
        await execution.runner.worker_started(index)
        try:
            while True:
                item = await queue.get()
                try:
                    await execution.runner.record_queue("session_pending", queue.qsize())
                    if item is stop:
                        return
                    session, admission = item
                    if ctx.cancellation.is_cancelled():
                        await execution.runner.cancel(session, ctx.cancellation.reason)
                    else:
                        await run_one(session, admission)
                finally:
                    queue.task_done()
        finally:
            await execution.runner.worker_stopped(index)

    producer = asyncio.create_task(produce())
    tasks = [producer]
    workers: list[asyncio.Task] = []
    failure: BaseException | None = None
    try:
        if active_limit is None:

            async def dispatch() -> None:
                while True:
                    item = await queue.get()
                    queue.task_done()
                    await execution.runner.record_queue("session_pending", queue.qsize())
                    if item is stop:
                        break
                    session, admission = item
                    workers.append(asyncio.create_task(run_one(session, admission)))
                if workers:
                    await asyncio.gather(*workers)

            tasks.append(asyncio.create_task(dispatch()))
        else:
            tasks.extend(asyncio.create_task(worker(index)) for index in range(active_limit))
        await asyncio.gather(*tasks)
    except BaseException as exc:
        failure = exc
        if ctx.cancellation.is_cancelled():
            termination_reason = ctx.cancellation.reason
        aborting = True

        async def drain() -> None:
            for task in [*tasks, *workers]:
                if not task.done() and not task.cancelling():
                    task.cancel()
            await asyncio.gather(*tasks, *workers, return_exceptions=True)

        await finish_task(asyncio.create_task(drain()))
    drain_ms = (perf_counter_ns() - producer_done_ns) / 1e6 if producer_done_ns else 0.0
    execution.runner.record_scheduling(
        TrialSchedulingFact(
            offered_sessions=offered_sessions,
            admitted_sessions=admitted_sessions,
            completed_sessions=completed_sessions,
            offered_attempts=strategy.attempts.offered,
            admitted_attempts=strategy.attempts.admitted,
            completed_attempts=strategy.attempts.completed,
            offering_started_ns=offering_started_ns,
            session_offering_finished_ns=min(producer_done_ns, strategy.deadline_ns) if producer_done_ns and strategy.deadline_ns else producer_done_ns,
            request_offering_finished_ns=(strategy.deadline_ns if strategy.deadline_ns is not None and termination_reason == "duration_elapsed"
                                           else strategy.attempts.last_offered_ns + 1 if strategy.attempts.last_offered_ns is not None else None),
            drain_duration_ms=drain_ms,
            demand_limited=strategy.demand_limited(),
            demand_exhausted=demand_exhausted,
            termination_reason=termination_reason,
        )
    )
    failure = failure or producer_failure or strategy.attempts.failure
    if failure is not None:
        raise failure


class SchedulerBase(SchedulerPlugin):
    expected_turns: int | None

    def __init__(self, settings: object) -> None:
        if not isinstance(settings, Mapping):
            raise TypeError("Scheduler settings must be a mapping")
        self.settings: Mapping[str, object] = settings
        self.slo_rules = ()

    def validate_metrics(self, metrics) -> None:
        from perf_benchmark.pipeline.scheduling.sweep import resolve_rules
        sweep = cast(Mapping[str, object], self.settings.get("sweep", {}))
        self.slo_rules = resolve_rules(sweep, metrics.describe()) if "slo" in sweep else ()

    def trial_plan(self, settings) -> TrialPlan:
        raise NotImplementedError

    def trial_plans(self) -> Iterable[TrialPlan]:
        plan = self.trial_plan(self.settings)
        if "validity" in self.settings:
            requirements = self.settings["validity"]
            assert isinstance(requirements, Mapping)
            plan = replace(plan, validity_requirements=ValidityRequirements(**requirements))
        count = cast(int, self.settings.get("repetitions", 1))
        for index in range(1, count + 1):
            yield replace(plan, name=plan.name if count == 1 else f"{plan.name}-repetition-{index}",
                          repetition=RepetitionPlan(index, count))

    def create_strategy(self, plan: TrialPlan, started_ns: int, *, seed: int) -> StandardTurnStrategy:
        raise NotImplementedError

    async def execute_plan(self, plan, trials, ctx):
        strategy = self.create_strategy(plan, perf_counter_ns(), seed=ctx.seed)
        async with trials.open(plan, strategy) as execution:
            await run_workload(execution, strategy, plan, ctx, expected_turns=self.expected_turns)
        return execution

    async def schedule(self, trials: TrialHost, ctx: SchedulerContext) -> None:
        if "sweep" in self.settings:
            from perf_benchmark.pipeline.scheduling.sweep import Sweep
            await Sweep(self, trials, ctx).run()
            return
        for plan in self.trial_plans():
            if ctx.cancellation.is_cancelled():
                return
            await self.execute_plan(plan, trials, ctx)
