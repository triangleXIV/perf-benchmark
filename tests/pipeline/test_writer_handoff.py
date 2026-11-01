import asyncio
from collections import Counter
from contextlib import suppress
from threading import Event
from time import perf_counter_ns, sleep
from types import SimpleNamespace

import pytest

from perf_benchmark.core.models import Run, Session, Turn
from perf_benchmark.pipeline.context import MetricsContext, SaverContext, SchedulerContext
from perf_benchmark.pipeline.contracts.executor import AttemptResult
from perf_benchmark.pipeline.contracts.metrics import MetricUpdate
from perf_benchmark.pipeline.contracts.results import ComputedResultBatch, SessionResultRecord
from perf_benchmark.pipeline.contracts.scheduler import TrialExecution
from perf_benchmark.pipeline.execution.engine import ExecutionEngine
from perf_benchmark.pipeline.preparation.payloads import PayloadRef
from perf_benchmark.pipeline.results.async_writer import AsyncBatchWriter, BatchWriterOverloaded
from perf_benchmark.pipeline.results.measurement import AttemptFact, OccupancyFact, SessionFact
from perf_benchmark.pipeline.results.metrics import MetricsRuntime
from perf_benchmark.pipeline.results.saver import SaverRuntime, _snapshot
from perf_benchmark.pipeline.scheduling.mechanics import run_workload
from perf_benchmark.plugins import create_registry
from perf_benchmark.runtime.cancellation import CancellationToken


async def test_metrics_result_batch_waits_when_8156_saver_records_are_pending(tmp_path):
    release, publishing = Event(), Event()
    stored = []

    class Backend:
        def open(self): pass
        def close(self): pass
        def write_batch(self, records):
            assert release.wait(10)
            stored.extend(records)

    token = CancellationToken()
    sink = SaverRuntime(Backend(), Run(name="handoff"), SaverContext(tmp_path, token))
    await sink.writer.start()
    records = tuple(_snapshot(SessionResultRecord, Session(id=f"s-{i}", turns=[]), turn_count=0) for i in range(8206))
    sink.writer.submit_many(records[:8156])

    def publish(batch):
        publishing.set()
        sink.write_results(batch)

    source = AsyncBatchWriter(SimpleNamespace(open=lambda: None, close=lambda: None,
        write_batch=lambda _: ComputedResultBatch(sessions=records[8156:])), publish=publish)
    await source.start()
    source.submit("metrics batch")
    try:
        assert await asyncio.to_thread(publishing.wait, 2)
        await asyncio.sleep(.05)
        assert source.error is None and sink.writer.rejection is None
        assert sink.writer.queue.unfinished_tasks == 8156
        release.set()
        await asyncio.wait_for(source.flush(), 5)
        await sink.flush()
        assert len(stored) == 8206 and len({r.id for r in stored}) == 8206
        assert not token.is_cancelled()
    finally:
        release.set()
        with suppress(BatchWriterOverloaded): await source.close()
        with suppress(BatchWriterOverloaded): await sink.close()


@pytest.mark.parametrize("outcome", ["failure", "overload", "close"])
async def test_blocked_writer_handoff_wakes_on_failure_overload_and_close(outcome):
    release, entered = Event(), Event()
    class Backend:
        def open(self): pass
        def close(self): pass
        def write_batch(self, records):
            entered.set()
            assert release.wait(5)
            if outcome == "failure": raise ValueError("storage failed")
    writer = AsyncBatchWriter(Backend(), capacity=100)
    await writer.start()
    writer.submit_many(tuple(range(100)))
    assert await asyncio.to_thread(entered.wait, 1)
    pending = asyncio.create_task(asyncio.to_thread(writer.submit_many_wait, ("next",)))
    closing = None
    try:
        await asyncio.sleep(.05)
        assert not pending.done()
        if outcome == "failure": release.set()
        elif outcome == "overload":
            with pytest.raises(BatchWriterOverloaded): writer.submit("overflow")
        else: closing = asyncio.create_task(writer.close())
        expected = {"failure": ValueError, "overload": BatchWriterOverloaded, "close": RuntimeError}[outcome]
        with pytest.raises(expected): await asyncio.wait_for(pending, 2)
    finally:
        release.set()
        with suppress(ValueError, BatchWriterOverloaded):
            await (closing if closing is not None else writer.close())


@pytest.mark.parametrize("response_status", ["succeeded", "failed"])
async def test_A12000_C7480_real_engine_and_both_writers_handle_fast_completions(
    tmp_path, trial_context, response_status,
):
    release = Event()
    saved = []
    class Storage:
        def open(self): pass
        def close(self): pass
        def write_batch(self, records):
            assert release.wait(10)
            sleep(.0005)
            saved.extend(records)
    class Calculator:
        def open(self): pass
        def close(self): pass
        def release_trial(self, trial_id): pass
        def consume(self, facts):
            return [MetricUpdate(
                "attempt" if isinstance(f, AttemptFact) else "session" if isinstance(f, SessionFact) else "turn",
                f.id, {}, f.attempts if isinstance(f, AttemptFact) else 0,
            ) for f in facts if not isinstance(f, OccupancyFact)]
    class Runtime:
        async def prepare(self): pass
        async def close(self): pass
        async def close_session(self, session_id): pass
        async def release(self, prepared): pass
        async def prepare_attempt(self, value): return value
        async def execute_attempt(self, value, signal):
            result = AttemptResult(value.turn.id, value.attempt, status=response_status,
                error="HTTP 500" if response_status == "failed" else None,
                continuation_payload=b'{"role":"assistant","content":"ok"}')
            result.request.started_ns = perf_counter_ns()
            result.request.response_completed_ns = perf_counter_ns()
            return result
    run = Run(name="full-A12000-C7480-burst")
    ctx = trial_context("scheduler", {}, run)
    sink = SaverRuntime(Storage(), run, SaverContext(tmp_path, ctx.cancellation))
    await sink.writer.start()
    metrics = MetricsRuntime(Calculator(), MetricsContext(ctx.cancellation), sink.write_results, sink.wait_for_headroom)
    await metrics.writer.start()
    collector = await metrics.open_trial(ctx.trial)
    plugin = create_registry().bind("scheduler", "multi-turn-closed-loop", {
        "max_active_sessions": 12000, "max_concurrency": 7480,
        "stop": {"session_count": 12000}, "on_exhausted": "error",
    }).plugin
    plan, = plugin.trial_plans()
    strategy = plugin.create_strategy(plan, perf_counter_ns(), seed=42)
    engine = ExecutionEngine(run, Runtime(), collector, sink, ctx, strategy, resource_concurrency=7480)
    assert await engine.open()
    engine.open_ready_window(7480)
    async def sessions():
        for i in range(12000):
            session = Session(id=f"s-{i}", trial_id=ctx.trial.id, ordinal=i,
                turns=[Turn(id=f"t-{i}", ordinal=0, request_payload_ref=PayloadRef(0, 1))])
            session.link()
            yield session
    task = asyncio.create_task(run_workload(TrialExecution(sessions(), engine), strategy, plan,
        SchedulerContext(ctx.cancellation), expected_turns=None))
    try:
        await asyncio.sleep(.1)
        assert not ctx.cancellation.is_cancelled()
        release.set()
        await asyncio.wait_for(task, 120)
        await metrics.writer.flush()
        await sink.flush()
        counts = Counter(type(record).__name__ for record in saved)
        assert counts == {"SessionResultRecord": 24000, "TurnResultRecord": 12000, "AttemptResultRecord": 12000}
        assert engine.scheduling_fact.completed_sessions == 12000
        assert not ctx.cancellation.is_cancelled()
        assert metrics.writer.rejection is sink.writer.rejection is None
    finally:
        release.set()
        if not task.done(): task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        await engine.close()
        await collector.close()
        await metrics.close()
        await sink.close()
