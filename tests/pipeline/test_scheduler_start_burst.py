import asyncio
from threading import Event
from time import perf_counter_ns

from perf_benchmark.core.models import Run, Session, Turn
from perf_benchmark.pipeline.context import SchedulerContext
from perf_benchmark.pipeline.contracts.scheduler import TrialExecution
from perf_benchmark.pipeline.execution.engine import ExecutionEngine
from perf_benchmark.pipeline.preparation.payloads import PayloadRef
from perf_benchmark.pipeline.results.async_writer import AsyncBatchWriter
from perf_benchmark.pipeline.scheduling.mechanics import run_workload
from perf_benchmark.plugins import create_registry


async def test_A12000_producer_workers_and_stop_facts_respect_metrics_capacity(trial_context):
    release = Event()
    stored = []

    class Backend:
        def open(self):
            pass

        def write_batch(self, items):
            assert release.wait(10)
            stored.extend(items)

        def close(self):
            pass

    writer = AsyncBatchWriter(Backend())
    await writer.start()

    class Metrics:
        async def wait_for_headroom(self):
            await writer.wait_for_headroom()

        def record_occupancy(self, kind, at_ns, value):
            writer.submit((kind, at_ns, value))

    class Runner(ExecutionEngine):
        async def run(self, session, admission):
            await asyncio.sleep(.001)

        async def worker_started(self, index):
            pass

        async def worker_stopped(self, index):
            pass

        def record_scheduling(self, fact):
            self.scheduling_fact = fact

    async def sessions():
        for index in range(12000):
            yield Session(id=f"s-{index}", turns=[Turn(id=f"t-{index}", request_payload_ref=PayloadRef(0, 1))])

    plugin = create_registry().bind("scheduler", "multi-turn-closed-loop", {
        "max_active_sessions": 12000, "max_concurrency": 7480,
        "stop": {"session_count": 12000}, "on_exhausted": "error",
    }).plugin
    plan, = plugin.trial_plans()
    run = Run(name="actual-scheduler-start-burst")
    context = trial_context("scheduler", {}, run)
    runner = Runner(run, None, Metrics(), None, context, None)
    task = asyncio.create_task(run_workload(
        TrialExecution(sessions(), runner), plugin.create_strategy(plan, perf_counter_ns(), seed=42),
        plan, SchedulerContext(context.cancellation), expected_turns=None,
    ))
    try:
        await asyncio.sleep(.1)
        assert not task.done()
        assert writer.rejection is None
        assert writer.queue.unfinished_tasks <= 2048
        release.set()
        await asyncio.wait_for(task, 30)
        await writer.flush()
        assert runner.scheduling_fact.offered_sessions == 12000
        assert runner.scheduling_fact.completed_sessions == 12000
        assert len(stored) == 36000  # producer + consumption + stop-sentinel consumption
        assert max(stored, key=lambda item: item[1])[2] == 0
        assert writer.rejection is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await writer.close()
