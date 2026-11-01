import asyncio
from collections import namedtuple
from collections.abc import Callable, Collection
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import suppress
from queue import Empty, Queue
from threading import Event as ThreadEvent, Lock
from time import monotonic, perf_counter
from typing import Generic, TypeVar

from perf_benchmark.pipeline.contracts.observation import DiagnosticSource, WriterSnapshot


class BatchWriterOverloaded(RuntimeError):
    pass


_Call = namedtuple("_Call", "function args future after_error", defaults=(False,))
_STOP = object()


_Published = TypeVar("_Published")
_Return = TypeVar("_Return")


class AsyncBatchWriter(Generic[_Published]):
    task: Future[None] | None
    loop: asyncio.AbstractEventLoop | None
    error: BaseException | None
    rejection: BatchWriterOverloaded | None
    batch_seconds: float

    def __init__(
        self,
        backend,
        *,
        capacity: int = 8192,
        batch_size: int = 100,
        publish: Callable[[_Published], None] | None = None,
        on_error: Callable[[str], None] | None = None,
    ) -> None:
        if capacity <= 0 or not 0 < batch_size <= 100:
            raise ValueError("writer capacity must be positive and batch_size must be in [1, 100]")
        self.backend, self.publish, self.on_error = backend, publish, on_error
        self.queue: Queue[object] = Queue(maxsize=capacity)
        self.batch_size, self.executor = batch_size, ThreadPoolExecutor(max_workers=1)
        self.task = self.loop = self.error = self.rejection = None
        self.closed = self._observed = False
        self.processing = self.processed = self.batch_seconds = 0
        self.submit_lock = Lock()
        self.headroom = asyncio.Event()
        self.capacity_ready = ThreadEvent()

    def enable_diagnostics(self) -> DiagnosticSource:
        self._observed = True
        return self._snapshot

    def _snapshot(self) -> WriterSnapshot:
        return WriterSnapshot(self.queue.qsize(), self.processing, self.processed, self.batch_seconds)

    async def start(self) -> None:
        self.loop = asyncio.get_running_loop()
        try:
            await asyncio.wrap_future(self.executor.submit(self.backend.open))
        except BaseException:
            with suppress(Exception):
                await asyncio.wrap_future(self.executor.submit(self.backend.close))
            self.executor.shutdown(wait=True)
            self.closed = True
            raise
        self.task = self.executor.submit(self._work)

    def submit(self, item: object) -> None:
        self.submit_many((item,))

    async def wait_for_headroom(self) -> None:
        # Leave room for a wave of terminal facts before admitting more work.
        limit = max(1, self.queue.maxsize // 4)
        while True:
            # Clear before checking: the worker may finish on another thread
            # between the check and the await. Never erase its notification.
            self.headroom.clear()
            self._raise_error()
            if self.closed:
                raise RuntimeError("batch writer is closed")
            if self.queue.unfinished_tasks < limit:
                return
            await self.headroom.wait()

    def submit_many(self, items: Collection[object]) -> None:
        if self.closed:
            raise RuntimeError("batch writer is closed")
        self._raise_error()
        with self.submit_lock:
            if self.queue.unfinished_tasks + len(items) > self.queue.maxsize:
                self.rejection = BatchWriterOverloaded(
                    "background writer queue is full; measurement is invalid "
                    f"(backend={type(self.backend).__name__}, pending={self.queue.unfinished_tasks}, "
                    f"incoming={len(items)}, capacity={self.queue.maxsize})"
                )
                if self.on_error is not None and self.loop is not None:
                    self.loop.call_soon_threadsafe(self.on_error, str(self.rejection))
                self._wake_waiters()
                raise self.rejection
            for item in items:
                self.queue.put_nowait(item)
        self._raise_error()

    def submit_many_wait(self, items: Collection[object]) -> None:
        """Reliable handoff between background writers, with bounded storage.

        A direct call on the event loop retains nonblocking overload semantics.
        HTTP completion facts continue to use submit()/submit_many().
        """
        if len(items) > self.queue.maxsize:
            raise ValueError("handoff batch exceeds writer capacity")
        while True:
            # Clear before checking, just as in the asynchronous admission path.
            self.capacity_ready.clear()
            with self.submit_lock:
                self._raise_error()
                if self.closed:
                    raise RuntimeError("batch writer is closed")
                if self.queue.unfinished_tasks + len(items) <= self.queue.maxsize:
                    for item in items:
                        self.queue.put_nowait(item)
                    break
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                self.capacity_ready.wait()
            else:
                self.submit_many(items)
                return
        self._raise_error()

    async def flush(self) -> None:
        await self.call(tuple, after_error=True)
        self._raise_error()

    async def call(self, function: Callable[..., _Return], *args, after_error=False) -> _Return:
        if self.task is None or self.task.done():
            raise RuntimeError("batch writer is not running")
        command = _Call(function, args, Future(), after_error)
        await asyncio.to_thread(self.queue.put, command)
        return await asyncio.wrap_future(command.future)

    async def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._wake_waiters()
        failure: BaseException | None = None
        try:
            await self.flush()
        except BaseException as exc:
            failure = exc
        self.queue.put(_STOP)
        assert self.task is not None
        await asyncio.wrap_future(self.task)
        try:
            await asyncio.wrap_future(self.executor.submit(self.backend.close))
        except BaseException as exc:
            failure = failure or exc
        self.executor.shutdown(wait=True)
        if failure is not None:
            raise failure

    def _work(self) -> None:
        carry = None
        while True:
            first = carry if carry is not None else self.queue.get()
            carry = None
            if first is _STOP:
                self.queue.task_done()
                return
            if isinstance(first, _Call):
                self._execute(first)
                continue
            batch = [first]
            deadline = monotonic() + 0.1
            while len(batch) < self.batch_size:
                try:
                    item = self.queue.get(timeout=max(0, deadline - monotonic()))
                except Empty:
                    break
                if item is _STOP or isinstance(item, _Call):
                    carry = item
                    break
                batch.append(item)
            self._write(batch)

    def _write(self, batch) -> None:
        try:
            if self.error is None:
                function = self._write_batch if self._observed else self.backend.write_batch
                result = function(tuple(batch))
                if self.publish is not None and result is not None:
                    self.publish(result)
        except BaseException as exc:
            self._fail(exc, f"background writer failed: {type(exc).__name__}")
        finally:
            for _ in batch:
                self.queue.task_done()
            self.capacity_ready.set()
            if self.queue.unfinished_tasks < max(1, self.queue.maxsize // 4):
                self._wake_waiters()

    def _execute(self, command) -> None:
        try:
            if not command.future.set_running_or_notify_cancel():
                return
            if not command.after_error:
                self._raise_error()
            command.future.set_result(command.function(*command.args))
        except BaseException as exc:
            command.future.set_exception(exc)
        finally:
            self.queue.task_done()
            self.capacity_ready.set()
            if self.queue.unfinished_tasks < max(1, self.queue.maxsize // 4):
                self._wake_waiters()

    def _write_batch(self, items):
        self.processing = len(items)
        started = perf_counter()
        try:
            result = self.backend.write_batch(items)
            self.processed += len(items)
            return result
        finally:
            self.batch_seconds = perf_counter() - started
            self.processing = 0

    def _wake_waiters(self):
        self.capacity_ready.set()
        if self.loop is not None:
            self.loop.call_soon_threadsafe(self.headroom.set)

    def _fail(self, error, message) -> None:
        if self.error is None:
            self.error = error
            self._wake_waiters()
            if self.on_error is not None and self.loop is not None:
                self.loop.call_soon_threadsafe(self.on_error, message)

    def _raise_error(self) -> None:
        failure = self.error or self.rejection
        if failure is not None:
            raise failure
