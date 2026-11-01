"""Common bounded Saver runtime.

Saver Plugins provide only a synchronous storage backend.  This Host owns
snapshotting, queue capacity, batching, the single writer thread, flushing and
failure propagation.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping
from dataclasses import fields, is_dataclass, replace
from itertools import islice
from functools import cache
import logging
from types import MappingProxyType

from perf_benchmark.core.models import Run, Session, Trial
from perf_benchmark.pipeline.results.async_writer import AsyncBatchWriter
from perf_benchmark.pipeline.context import SaverContext
from perf_benchmark.pipeline.contracts.facts import TrialFact
from perf_benchmark.pipeline.contracts.sweep import SweepRecord, TrialValidity
from perf_benchmark.pipeline.contracts.results import (
    ComputedResultBatch,
    MetricPoint,
    GroupedMetricRecord,
    MeasurementRecord,
    MetricWindowResultRecord,
    RunRecord,
    SessionResultRecord,
    TrialFactRecord,
    TrialResultRecord,
)
from perf_benchmark.pipeline.contracts.saver import SaverArtifact, SaverBackend, SaverPlugin


def _freeze(value):
    if value is None or type(value) in (str, int, float, bool):
        return value
    if is_dataclass(value):
        return MappingProxyType(
            {name: _freeze(getattr(value, name)) for name in _record_fields(type(value))}
        )
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


@cache
def _record_fields(record_type):
    return tuple(item.name for item in fields(record_type))


def _snapshot(record_type, source, **overrides):
    """Copy only the fields declared by the persistence contract, never bodies."""
    return record_type(**{
        name: _freeze(overrides[name] if name in overrides else getattr(source, name))
        for name in _record_fields(record_type)
    })


def _trial_records(trial: Trial, fact: TrialFact | None):
    if trial.run_id is None:
        raise ValueError("cannot persist a Trial before it is registered with a Run")
    yield _snapshot(
        TrialResultRecord, trial,
        failed_or_cancelled_turns=trial.failed_turns + trial.cancelled_turns,
    )
    for name in ("lifecycle", "metric_counts", "diagnostics"):
        yield MeasurementRecord(
            trial.run_id, "trial", trial.id, name, _freeze(getattr(trial.measurements, name)),
        )
    for window in trial.measurements.metric_windows:
        yield _snapshot(
            MetricWindowResultRecord, window, run_id=trial.run_id, scope="trial", scope_id=trial.id,
        )
    if fact is not None:
        if fact.trial_id != trial.id or fact.status != trial.status:
            raise ValueError("TrialFact belongs to another Trial state")
        yield TrialFactRecord(fact)




class SaverRuntime:
    def __init__(self, backend: SaverBackend, run: Run, ctx: SaverContext) -> None:
        self.artifact: SaverArtifact
        self.backend = backend
        self.run = run
        def committed(receipt):
            if ctx.committed is not None:
                try:
                    ctx.committed(receipt)
                except Exception:
                    logging.getLogger(__name__).exception("commit wakeup notification failed")
        self.writer = AsyncBatchWriter(backend, publish=committed if ctx.committed else None,
                                       on_error=ctx.cancellation.cancel)
        self.enable_diagnostics = self.writer.enable_diagnostics
        self.close = self.writer.close
        self.flush = self.writer.flush
        self.wait_for_headroom = self.writer.wait_for_headroom

    @classmethod
    async def create(cls, plugin: SaverPlugin, run: Run, ctx: SaverContext):
        runtime = cls(plugin.create_backend(_snapshot(RunRecord, run), ctx), run, ctx)
        await runtime.writer.start()
        runtime.artifact = runtime.backend.artifact
        run.artifacts[runtime.artifact.kind] = runtime.artifact.location
        return runtime

    async def write_run_measurement(self, name: str, value: object) -> None:
        self.writer.submit(MeasurementRecord(self.run.id, "run", self.run.id, name, _freeze(value)))

    def write_results(self, batch: ComputedResultBatch) -> None:
        records = (
            batch.turns + batch.sessions + batch.attempts
            + tuple(record for series in batch.series for record in (
                _snapshot(MetricWindowResultRecord, series.window, run_id=self.run.id,
                          scope="trial", scope_id=series.trial_id),
                *(_snapshot(MetricPoint, point) for point in series.points),
            ))
        )
        # Metrics publishes from its own writer thread. Wait on that thread
        # when Session starts or earlier batches occupy the Saver queue.
        source = iter(records)
        while chunk := tuple(islice(source, 100)):
            self.writer.submit_many_wait(chunk)

    async def write_session(self, session: Session) -> None:
        # Session admission can emit more starts than the entire result queue
        # before any HTTP request runs. Wait here, outside request timing, and
        # submit without another suspension so concurrent admissions recheck.
        await self.writer.wait_for_headroom()
        self.writer.submit(_snapshot(SessionResultRecord, session, turn_count=len(session.turns)))

    async def write_trial(self, trial: Trial, fact: TrialFact | None = None) -> None:
        self.writer.submit_many(tuple(_trial_records(trial, fact)))

    async def write_sweep(self, record: SweepRecord) -> None:
        if record.run_id != self.run.id:
            raise ValueError("sweep record belongs to another Run")
        self.writer.submit(replace(record, definition=_freeze(record.definition),
                                   validity=tuple(_snapshot(TrialValidity, item) for item in record.validity),
                                   checks=tuple(record.checks), trial_ids=tuple(record.trial_ids)))

    async def write_metric_points(self, points: Collection[MetricPoint | GroupedMetricRecord]) -> None:
        source = iter(points)
        while batch := tuple(islice(source, 100)):
            self.writer.submit_many(tuple(_snapshot(type(item), item) for item in batch))
            await self.writer.flush()

    async def finalize(self, run: Run):
        await self.write_run_measurement("lifecycle", run.measurements.lifecycle)
        return await self.writer.call(self.backend.finalize, _snapshot(RunRecord, run))
