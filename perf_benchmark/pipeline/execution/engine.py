from __future__ import annotations

from perf_benchmark.core.profiles import Continuation
from perf_benchmark.pipeline.contracts.observation import TraceContext

import asyncio
import gc
import math
from contextlib import suppress
from dataclasses import asdict
from functools import partial
from time import perf_counter_ns

from perf_benchmark.core import lifecycle
from perf_benchmark.runtime.attempt import AttemptSignal, assess_result
from perf_benchmark.core.models import thaw_snapshot

from perf_benchmark.core.models import Run, Session, Trial, Turn
from perf_benchmark.pipeline.context import TrialContext
from perf_benchmark.pipeline.contracts.executor import AttemptInput, AttemptResult
from perf_benchmark.pipeline.contracts.facts import TrialSchedulingFact
from perf_benchmark.pipeline.contracts.scheduler import (
    SchedulingStopped,
    SessionAdmission,
    TurnStrategy,
)
from perf_benchmark.pipeline.resources import finish_task

class ExecutionEngine:
    def __init__(
        self,
        run: Run,
        runtime,
        metrics,
        sink,
        ctx: TrialContext,
        strategy: TurnStrategy,
        resource_concurrency: int | None = None,
    ) -> None:
        self.domain_run = run
        self.runtime = runtime
        self.metrics = metrics
        self.sink = sink
        self.ctx = ctx
        self._scheduler_event = partial(ctx.events.emit, stage="scheduler", trial_id=ctx.trial.id)
        self._runtime_event = partial(ctx.events.emit, stage="executor", trial_id=ctx.trial.id)
        self.strategy = strategy
        self.controls = {}
        self.trial: Trial = ctx.trial
        self.occupancy = {"active_sessions": 0, "in_flight": 0, "request_pending": 0}
        self.diagnostics = partial(dict, self.occupancy, trial_id=ctx.trial.id)
        self.cancellation_watcher: asyncio.Task[None] | None = None
        self.interruptible_tasks: set[asyncio.Task] = set()
        self.ready_slots: asyncio.Semaphore | None = None
        self.resource_slots = (
            asyncio.Semaphore(2 * resource_concurrency)
            if resource_concurrency is not None else None
        )
        self.resource_tasks: set[asyncio.Future] = set()
        self.resource_errors: list[BaseException] = []
        self.scheduling_fact: TrialSchedulingFact | None = None

    def open_ready_window(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("request-ready window capacity must be positive")
        if self.ready_slots is not None:
            raise RuntimeError("request-ready window is already open")
        self.ready_slots = asyncio.Semaphore(capacity)

    def release_resource(self, settlement):
        if self.resource_slots is None:
            return
        if settlement is None:
            self.resource_slots.release()
            return
        task = asyncio.ensure_future(settlement)
        self.resource_tasks.add(task)
        def finished(value):
            self.resource_tasks.discard(value)
            self.resource_slots.release()
            if not value.cancelled() and (error := value.exception()) is not None:
                self.resource_errors.append(error)
                self.ctx.cancellation.cancel("executor cleanup failure")
        task.add_done_callback(finished)

    async def open(self) -> bool:
        self.cancellation_watcher = asyncio.create_task(self._cancel_active_tasks())
        await self._runtime_event(
            lifecycle.RUNTIME_CREATED,
            data={"runtime": type(self.runtime).__name__},
        )
        if self.ctx.cancellation.is_cancelled():
            return False
        await self._runtime_event(
            lifecycle.RUNTIME_PREPARE_STARTED,
        )
        try:
            completed, _ = await self._interruptible(
                self.runtime.prepare,
            )
        except BaseException as exc:
            await self._runtime_event(
                lifecycle.RUNTIME_PREPARE_FAILED,
                data={"error": f"{type(exc).__name__}: {exc}"},
            )
            raise
        if not completed:
            await self._runtime_event(
                lifecycle.RUNTIME_PREPARE_FAILED,
                data={
                    "error": self.ctx.cancellation.reason,
                    "cancelled": True,
                },
            )
            return False
        await self._runtime_event(
            lifecycle.RUNTIME_PREPARE_COMPLETED,
        )
        self._restore_gc = gc.isenabled()
        gc.disable()
        return True

    async def run(self, session: Session, admission: SessionAdmission) -> None:
        if self.ctx.cancellation.is_cancelled():
            await self.cancel(session, self.ctx.cancellation.reason)
            return
        if session.status != "pending":
            raise RuntimeError("Session can only run once")
        if session.turns and session.turns[0].causal is not None:
            if await self._wait_turn(session.turns[0]) is not None:
                return
        if self.resource_slots is not None:
            completed, _ = await self._interruptible(self.metrics.wait_for_headroom)
            if not completed:
                return
        session.measurements.target_ns = admission.target_ns
        session.measurements.offered_ns = admission.offered_ns
        session.measurements.admitted_ns = perf_counter_ns()
        session.start()
        await self.sink.write_session(session)
        await self._scheduler_event(
            lifecycle.SESSION_STARTED,session_id=session.id
        )
        await self._occupancy("active_sessions", 1)
        history = {}
        try:
            stop_reason = None
            for index, turn in enumerate(session.turns):
                if self.ctx.cancellation.is_cancelled():
                    stop_reason = self.ctx.cancellation.reason
                    break
                proceed, stop_reason = await self._run_turn(session, turn, history, waited=index == 0)
                if not proceed:
                    if index == len(session.turns) - 1 and turn.status == "succeeded":
                        stop_reason = None
                    break
            await self._finish_session(session, cancellation_reason=stop_reason)
        except BaseException as error:
            if session.status == "running":
                try:
                    await self._finish_session(session, error)
                except BaseException as exc:
                    error.add_note(f"session finalization failed: {type(exc).__name__}")
            raise
        finally:
            self.controls.pop(session.id, None)
            history.clear()
            await finish_task(asyncio.create_task(self.runtime.close_session(session.id)))
            await self._occupancy("active_sessions", -1)

    async def cancel(self, session: Session, reason: str) -> None:
        if session.id in self.controls:
            self.controls[session.id].cancel(reason)

    async def close(self) -> None:
        # Release runtime even when emitting a lifecycle observation fails.
        try:
            await self._runtime_event(
                lifecycle.RUNTIME_CLOSE_STARTED,            )
        finally:
            try:
                await self.runtime.close()
                if self.resource_tasks:
                    await finish_task(asyncio.gather(*tuple(self.resource_tasks), return_exceptions=True))
                if self.resource_errors:
                    raise self.resource_errors[0]
            except BaseException as exc:
                await self._runtime_event(
                    lifecycle.RUNTIME_CLOSE_FAILED,
                    data={"error": f"{type(exc).__name__}: {exc}"},
                )
                raise
            finally:
                self.ready_slots = None
                if self.cancellation_watcher is not None:
                    self.cancellation_watcher.cancel()
                    with suppress(asyncio.CancelledError):
                        await self.cancellation_watcher
                if getattr(self, "_restore_gc", False):
                    gc.enable()
        await self._runtime_event(
            lifecycle.RUNTIME_CLOSE_COMPLETED,        )

    async def _wait_turn(self, turn):
        turn.measurements.eligibility_started_ns = perf_counter_ns()
        completed, eligibility = await self._interruptible(lambda: self.strategy.wait_turn(turn))
        turn.measurements.eligibility_finished_ns = perf_counter_ns()
        if not completed:
            return self.ctx.cancellation.reason or "cancelled"
        assert eligibility is not None
        turn.measurements.eligibility_target_ns = eligibility.target_ns
        turn.measurements.dependency_ready_ns = eligibility.dependency_ready_ns
        return eligibility.reason

    async def _run_turn(self, session, turn, history, *, waited=False):
        if turn.causal is not None and not waited:
            reason = await self._wait_turn(turn)
            if reason is not None:
                return False, reason
        reason = self.strategy.begin_turn(turn)
        if reason is not None:
            return False, reason
        if turn.measurements.total_started_ns is None:
            turn.measurements.total_started_ns = perf_counter_ns()
        turn.start()
        await self._scheduler_event(
            lifecycle.TURN_STARTED,
            session_id=session.id,
            turn_id=turn.id,
        )
        control = TurnControl(self, session, turn, history)
        self.controls[session.id] = control
        try:
            completed, result = await self._interruptible(
                lambda: self.strategy.run_turn(turn, control),
            )
        except asyncio.CancelledError:
            if control.cancel_reason is None:
                self.ctx.cancellation.cancel("task_cancelled")
            completed = False
            result = None
        if completed and result is None and control.stop_reason is not None:
            turn.status, turn.started_at = "pending", None
            turn.measurements.lifecycle = type(turn.measurements.lifecycle)()
            self.controls.pop(session.id, None)
            return False, control.stop_reason
        if completed and (result is None or result is not control.result):
            raise RuntimeError("Scheduler must return the actual TurnControl result")
        if not completed:
            result = control.result or AttemptResult(turn.id, 0, status="cancelled")
            if result.fatal_error is not None:
                turn.fail(result.error or type(result.fatal_error).__name__)
            else:
                turn.cancel(control.cancel_reason or self.ctx.cancellation.reason)
        else:
            assert result is not None
            if result.status == "succeeded":
                turn.succeed()
            elif result.status == "cancelled":
                turn.cancel(result.error or "cancelled")
            else:
                turn.fail(result.error or "request failed")
        turn.scenario_passed = (
            all(control.assessments) if completed and control.assessments else None
        )
        if (
            turn.scenario_passed is True
            and result.scenario_passed is None
            and result.status != "succeeded"
        ):
            turn.scenario_passed = None
        await self._finish_turn(turn, result.trace)
        decision = self.strategy.next_turn(turn, result)
        if completed and (
            result.status == "succeeded" or (decision.proceed and decision.retain_partial)
        ):
            if not result.continuation_valid:
                raise ValueError("protocol cannot retain this partial response as live history")
            if result.continuation_payload is not None:
                history[turn.id] = Continuation(result.continuation_payload, result.continuation_profile)
        result.continuation_payload = None
        self.controls.pop(session.id, None)
        if result.fatal_error is not None:
            raise result.fatal_error
        return (
            completed and decision.proceed and not self.ctx.cancellation.is_cancelled(),
            decision.cancel_reason,
        )

    async def _finish_turn(self, turn: Turn, trace=None) -> None:
        self.metrics.record_turn(turn, trace)
        event_name = (
            lifecycle.TURN_COMPLETED
            if turn.status == "succeeded"
            else lifecycle.TURN_CANCELLED
            if turn.status == "cancelled"
            else lifecycle.TURN_FAILED
        )
        await self._scheduler_event(
            event_name,
            session_id=turn.session_id,
            turn_id=turn.id,
            data={"status": turn.status, "error": turn.error},
        )

    async def _finish_session(self, session: Session, error=None, cancellation_reason=None) -> None:
        session.finish()
        if error is not None:
            session.status, session.error = "failed", type(error).__name__
        elif cancellation_reason is not None:
            session.status, session.error = "cancelled", cancellation_reason
        self.metrics.record_session(session)
        await self._scheduler_event(
            lifecycle.SESSION_COMPLETED,
            session_id=session.id,
            data={"status": session.status},
        )
        self.trial.observe(session)

    async def _occupancy(self, kind: str, delta: int) -> None:
        self.occupancy[kind] += delta
        value = self.occupancy[kind]
        at_ns = perf_counter_ns()
        # Scheduling transitions may wait outside the raw HTTP timing window.
        # Recheck both writers here, including after a simultaneous completion
        # wave, before emitting nonblocking Attempt/Turn/Session facts.
        await self.metrics.wait_for_headroom()
        self.metrics.record_occupancy(kind, at_ns, value)
        await self._scheduler_event(
            lifecycle.SCHEDULER_CONCURRENCY_CHANGED,
            at_ns=at_ns,
            data={"kind": kind, "value": value},
        )

    async def record_queue(self, kind: str, value: int) -> None:
        # Queue transitions are scheduling facts, outside HTTP timing. Preserve
        # their original timestamp/value while waiting for reliable delivery.
        at_ns = perf_counter_ns()
        self.occupancy[kind] = value
        await self.metrics.wait_for_headroom()
        self.metrics.record_occupancy(kind, at_ns, value)

    def record_scheduling(self, fact: TrialSchedulingFact) -> None:
        if self.scheduling_fact is not None:
            raise RuntimeError("Trial scheduling facts may be recorded only once")
        self.scheduling_fact = fact
        self.trial.measurements.diagnostics.update(thaw_snapshot(asdict(fact)))

    async def worker_started(self, index: int) -> None:
        await self._scheduler_event(
            lifecycle.SCHEDULER_WORKER_STARTED,
            data={"worker": index},
        )

    async def worker_stopped(self, index: int) -> None:
        await self._scheduler_event(
            lifecycle.SCHEDULER_WORKER_STOPPED,
            data={"worker": index},
        )

    async def _interruptible(self, operation):
        if self.ctx.cancellation.is_cancelled():
            return False, None
        if self.cancellation_watcher is None:
            raise RuntimeError("execution engine is not open")
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("interruptible operation has no asyncio Task")
        self.interruptible_tasks.add(task)
        try:
            result = await operation()
            if self.ctx.cancellation.is_cancelled():
                return False, None
            return True, result
        except asyncio.CancelledError:
            if not self.ctx.cancellation.is_cancelled():
                raise
            if task.uncancel():
                raise
            return False, None
        finally:
            self.interruptible_tasks.discard(task)

    async def _cancel_active_tasks(self) -> None:
        await self.ctx.cancellation.wait()
        for task in tuple(self.interruptible_tasks):
            if not task.cancelling():
                task.cancel()


class TurnControl:
    result: AttemptResult | None
    signal: AttemptSignal | None
    task: asyncio.Task | None
    running: bool
    stop_reason: str | None

    def __init__(self, engine, session, turn, history):
        self.engine, self.session, self.turn = engine, session, turn
        self.history = history
        self.target = turn.requested_output_tokens
        self.result = self.signal = self.task = None
        self.assessments = []
        self.cancel_reason = None
        self.stop_reason = None
        self.running = False
        self.started_callback = None

        self.admission = SessionAdmission(
            session.measurements.target_ns, session.measurements.offered_ns
        )

    @property
    def session_ordinal(self) -> int:
        return self.session.ordinal

    def observe_started(self, callback):
        if self.running or self.result is not None:
            raise RuntimeError("start observation must be installed before sending")
        self.started_callback = callback

    async def send(self, plan=None):
        if self.result is not None or self.running or self.turn.status != "running":
            raise RuntimeError("send requires a running Turn without an Attempt")
        return await self._execute(1, plan)

    async def retry(self, delay_s: float, plan=None):
        if (
            self.running
            or self.turn.status != "running"
            or self.result is None
            or self.result.status not in {"succeeded", "failed"}
        ):
            raise RuntimeError("retry requires a completed succeeded or failed Attempt in a running Turn")
        if not self.result.recoverable or self.result.fatal_error is not None:
            raise RuntimeError("runtime cannot safely retry this Attempt")
        if not math.isfinite(delay_s) or delay_s < 0:
            raise ValueError("retry delay must be finite and nonnegative")
        self.running = True
        try:
            await asyncio.sleep(delay_s)
        finally:
            self.running = False
        return await self._execute(self.result.attempt + 1, plan)

    def cancel(self, reason):
        if self.turn.status != "running":
            raise RuntimeError("cannot cancel a terminal Turn")
        self.cancel_reason = reason
        if self.signal is not None and not self.signal.closed:
            self.signal.trigger(reason, action="cancel")
        elif self.task is not None:
            self.task.cancel()

    async def _execute(self, attempt, supplied_plan):
        self.running, self.task = True, asyncio.current_task()
        engine, turn = self.engine, self.turn
        ctx = engine.ctx
        plan = supplied_plan or engine.strategy.plan_attempt(
            turn, attempt, session_ordinal=self.session_ordinal,
        )
        self.signal = AttemptSignal(plan)
        self.signal.started_callback = self.started_callback
        assert self.task is not None
        self.signal.bind(self.task.cancel)
        prepared = None
        ticket = lease = None
        occupied = admitted = ready_slot = pending = resource_slot = False
        trace_context = (
            TraceContext.create(
                ctx.domain_run.id, turn.id, attempt, 1.0 if ctx.observation.trace_mode == "debug" else ctx.observation.sample_rate,
                seed=ctx.seed, session_ordinal=self.session_ordinal, turn_ordinal=turn.ordinal,
            )
            if ctx.observation.enabled and ctx.observation.trace_mode != "benchmark" else None
        )
        result_ready_ns = None
        fields = dict(
            stage="scheduler",
            trial_id=ctx.trial.id,
            session_id=self.session.id,
            turn_id=turn.id,
            attempt=attempt,
        )
        try:
            if ctx.cancellation.is_cancelled() or self.cancel_reason:
                raise asyncio.CancelledError
            preset = self.admission if attempt == 1 and turn.ordinal == 0 else None
            if turn.causal is not None and attempt == 1:
                preset = SessionAdmission(turn.measurements.eligibility_target_ns, perf_counter_ns())
            ticket = await engine.strategy.attempts.offer(preset)
            pending = True
            await engine._occupancy("request_pending", 1)
            assert engine.ready_slots is not None
            await engine.ready_slots.acquire()
            ready_slot = True
            turn.requested_output_tokens = self.target
            prepared = await engine.runtime.prepare_attempt(
                AttemptInput.capture(
                    self.session,
                    turn,
                    self.history,
                    attempt=attempt,
                    payloads=ctx.payloads,
                    trace_context=trace_context,
                )
            )
            if engine.resource_slots is not None:
                await engine.resource_slots.acquire()
                resource_slot = True
                await engine.metrics.wait_for_headroom()
            lease = await engine.strategy.attempts.admit(ticket)
            admitted = True
            if pending:
                pending = False
                await engine._occupancy("request_pending", -1)
            if ready_slot:
                engine.ready_slots.release()
                ready_slot = False
            await ctx.events.emit(lifecycle.REQUEST_QUEUED, **fields)
            await ctx.events.emit(lifecycle.TURN_ATTEMPT_STARTED, **fields)
            await engine._occupancy("in_flight", 1)
            occupied = True
            result = await engine.runtime.execute_attempt(prepared, self.signal)
            result_ready_ns = perf_counter_ns()
        except SchedulingStopped as exc:
            self.stop_reason = exc.reason
            result = None
        except asyncio.CancelledError:
            result = AttemptResult(
                turn.id,
                attempt,
                status="cancelled",
                error=self.cancel_reason or ctx.cancellation.reason,
                error_origin="client",
            )
        finally:
            result_ready_ns = result_ready_ns or perf_counter_ns()
            self.running = False
            self.signal.close()
            if ticket is not None and not admitted:
                await engine.strategy.attempts.cancel(ticket)
            if pending:
                await engine._occupancy("request_pending", -1)
            if ready_slot:
                engine.ready_slots.release()
            settlement = None
            try:
                if prepared is not None:
                    settlement = await engine.runtime.release(prepared)
            finally:
                if resource_slot:
                    engine.release_resource(settlement)
                try:
                    if occupied:
                        await engine._occupancy("in_flight", -1)
                finally:
                    if lease is not None:
                        lease.release()
        if result is None:
            return None
        if not isinstance(result, AttemptResult) or (result.turn_id, result.attempt) != (
            turn.id,
            attempt,
        ):
            raise TypeError("runtime returned a result for another Attempt")
        if admitted:
            assert lease is not None
            result.request.result_ready_ns = result_ready_ns
            result.request.trace_context = trace_context
            result.request.attempts = attempt
            result.request.target_ns = lease.target_ns
            result.request.offered_ns = lease.offered_ns
            result.request.admitted_ns = lease.admitted_ns
            assess_result(result, plan)
            if result.scenario_passed is not None:
                self.assessments.append(result.scenario_passed)
        elif self.result is not None:
            result.attempt = self.result.attempt
            result.request, result.usage, result.trace = (
                self.result.request,
                self.result.usage,
                self.result.trace,
            )
        else:
            result.attempt = result.request.attempts = 0
        if result.request_facts is not None:
            result.request.profile = result.request_facts.profile
            turn.requested_output_tokens = result.request_facts.requested_output_tokens
            turn.metadata.update(result.request_facts.metadata)
        turn.measurements.request, turn.usage = result.request, dict(result.usage)
        self.result = result
        if admitted:
            engine.metrics.record_attempt(turn, result)
        else:
            return result
        await emit_attempt_observations(ctx, self.session, result)
        suffix = {"succeeded": "completed", "failed": "failed", "cancelled": "cancelled"}[
            result.status
        ]
        event = f"turn.attempt.{suffix}"
        await ctx.events.emit(
            event, **fields, data={"status": result.status, "error": result.error}
        )
        return result


async def emit_attempt_observations(ctx, session, result):
    fields = dict(
        stage="executor",
        trial_id=ctx.trial.id,
        session_id=session.id,
        turn_id=result.turn_id,
        attempt=result.attempt,
    )
    for name, at_ns in (
        ("request.started", result.request.started_ns),
        ("request.first_output", result.request.first_output_ns),
    ):
        if at_ns is not None:
            await ctx.events.emit(name, **fields, at_ns=at_ns)
    if result.usage:
        await ctx.events.emit("usage.received", **fields, data=dict(result.usage))
    suffix = {"succeeded": "completed", "failed": "failed", "cancelled": "cancelled"}[result.status]
    await ctx.events.emit(
        f"request.{suffix}",
        **fields,
        at_ns=result.request.response_completed_ns,
        data={
            "http_status": result.request.http_status,
            "error": result.error,
            "error_origin": result.error_origin,
        },
    )
