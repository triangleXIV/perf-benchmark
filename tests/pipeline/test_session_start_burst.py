import asyncio
from threading import Event
from types import SimpleNamespace

import pytest

from perf_benchmark.core.models import Run, Session
from perf_benchmark.pipeline.context import SaverContext
from perf_benchmark.pipeline.results.saver import SaverRuntime
from perf_benchmark.runtime.cancellation import CancellationToken


async def test_12000_session_starts_wait_for_sqlite_headroom(tmp_path):
    """Reproduce the real A12000 startup burst, with a stalled writer."""
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

    cancellation = CancellationToken()
    sink = SaverRuntime(Backend(), Run(name="burst"), SaverContext(tmp_path, cancellation))
    await sink.writer.start()
    starts = [asyncio.create_task(sink.write_session(Session(id=f"s-{n}", turns=[]))) for n in range(12000)]
    try:
        await asyncio.sleep(0.1)
        assert not cancellation.is_cancelled()
        assert sum(task.done() for task in starts) <= 2048
        assert sink.writer.queue.unfinished_tasks <= 2048
        release.set()
        await asyncio.wait_for(asyncio.gather(*starts), 30)
        await sink.flush()
        assert len(stored) == 12000
        assert len({record.id for record in stored}) == 12000
        assert not cancellation.is_cancelled()
    finally:
        release.set()
        await asyncio.gather(*starts, return_exceptions=True)
        await sink.close()


async def test_headroom_does_not_erase_a_worker_completion_notification():
    from perf_benchmark.pipeline.results.async_writer import AsyncBatchWriter

    writer = AsyncBatchWriter(SimpleNamespace(), capacity=4)
    writer.queue.unfinished_tasks = 1
    event = asyncio.Event()

    class CompletionDuringClear:
        def clear(self):
            writer.queue.unfinished_tasks = 0
            event.set()

        async def wait(self):
            pytest.fail("completion was lost instead of rechecking capacity")

    writer.headroom = CompletionDuringClear()
    await writer.wait_for_headroom()
    writer.executor.shutdown()
