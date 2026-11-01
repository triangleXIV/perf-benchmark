import asyncio
from time import perf_counter

from perf_benchmark.pipeline.scheduling.mechanics import BoundedAttemptGate


async def test_A12000_fills_C7480_with_reverse_body_readiness():
    gate = BoundedAttemptGate(12000, 7480, None, None)
    tickets = [await gate.offer() for _ in range(12000)]
    admitted = asyncio.Queue()
    released = asyncio.Event()

    async def send(index):
        lease = await gate.admit(tickets[index])
        admitted.put_nowait(index)
        try:
            await released.wait()
        finally:
            lease.release()

    # This reproduces a large FIFO queue whose bodies become ready out of
    # order. Admission must fill the real concurrency window before responses.
    started = perf_counter()
    tasks = [asyncio.create_task(send(i)) for i in reversed(range(12000))]
    try:
        async with asyncio.timeout(5):
            first_wave = [await admitted.get() for _ in range(7480)]
        assert first_wave == list(range(7480))
        assert gate.admitted == 7480 and gate.completed == 0
        print(f'A12000/C7480 admission fill seconds={perf_counter()-started:.6f}')
        released.set()
        await asyncio.wait_for(asyncio.gather(*tasks), 5)
        assert gate.admitted == gate.completed == 12000
        assert gate.semaphore._value == 7480
    finally:
        released.set()
        for task in tasks:
            if not task.done(): task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_cancelled_queue_heads_do_not_strand_ready_followers():
    gate = BoundedAttemptGate(100, 1, None, None)
    tickets = [await gate.offer() for _ in range(100)]
    waiting = asyncio.create_task(gate.admit(tickets[-1]))
    await asyncio.sleep(0)
    for ticket in tickets[:-1]:
        await gate.cancel(ticket)
    lease = await asyncio.wait_for(waiting, 1)
    lease.release()
    assert not gate.queue and gate.admitted == gate.completed == 1
