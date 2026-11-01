from typing import Any, cast
from collections import Counter

import pyarrow as pa
import pytest

from perf_benchmark.pipeline.contracts.executor import AttemptResult, TurnRequestFacts, ExecutorPlugin
from perf_benchmark.pipeline.contracts.exporter import ExportResult
from perf_benchmark.pipeline.contracts.loader import LoaderPlugin, SOURCE_ORDER_COLUMN
from perf_benchmark.pipeline.coordinator import Coordinator
from perf_benchmark.pipeline.contracts.normalization import NormalizationPlugin, TurnDraft
from perf_benchmark.pipeline.registry import StageRegistry
from perf_benchmark.pipeline.contracts.metrics import MetricUpdate, TrialMetricReport
from perf_benchmark.pipeline.contracts.saver import SaverArtifact
from perf_benchmark.pipeline.contracts.results import TrialResultRecord
from perf_benchmark.pipeline.contracts.selector import SelectionResult, SelectorPlugin
from perf_benchmark.pipeline.results.measurement import AttemptFact, OccupancyFact, SessionFact
from perf_benchmark.plugins.scheduler.single_turn_closed_loop import (
    SingleTurnClosedLoopScheduler,
    validate as parse_closed_loop,
)
from perf_benchmark.plugins.scheduler.single_turn_open_loop import (
    SingleTurnOpenLoopScheduler,
    validate as parse_open_loop,
)
from perf_benchmark.runtime.cancellation import CancellationToken


def pipeline_resources(*, fail=None, cancel=None):
    closed = Counter()
    saved = []
    faults = {fail} if isinstance(fail, str) else set(fail or ())

    def close(name):
        closed[name] += 1
        if f"{name}.close" in faults:
            raise RuntimeError(f"{name}.close")

    class Source:
        schema = pa.schema([pa.field("value", pa.int64(), nullable=False)])

        def batches(self):
            yield pa.RecordBatch.from_pylist([{"value": 1}], schema=self.schema)

        async def close(self):
            close("source")

    class Loader(LoaderPlugin):
        def __init__(self, settings):
            pass

        async def open(self, ctx):
            return Source()

    class Normalizer(NormalizationPlugin):
        def __init__(self, settings):
            pass

        def normalize_batch(self, batch):
            if "normalization" in faults:
                raise ValueError("normalization")
            yield TurnDraft(
                "session",
                "turn",
                0,
                {},
                1,
                1,
                source_order=int(batch.column(SOURCE_ORDER_COLUMN)[0].as_py()),
            )

    class Selector(SelectorPlugin):
        def __init__(self, settings):
            pass

        def select(self, table):
            return SelectionResult(pa.array(range(table.num_rows), type=pa.int64()))

    class Prepared:
        def __init__(self, value):
            self.input = value
            self.request_facts = TurnRequestFacts(value.turn.requested_output_tokens)

    class Runtime:
        async def prepare(self):
            if "runtime.prepare" in faults:
                raise ValueError("runtime.prepare")
            if cancel:
                cancel.cancel("requested stop")

        async def prepare_attempt(self, value):
            return Prepared(value)

        async def execute_attempt(self, prepared, signal):
            result = AttemptResult(prepared.input.turn.id, prepared.input.attempt)
            result.request_facts = prepared.request_facts
            return result

        async def release(self, prepared):
            pass

        async def close_session(self, session_id):
            pass

        async def close(self):
            close("runtime")

    class Executor:
        def __init__(self, settings):
            pass

        async def create_runtime(self, trial, ctx):
            return Runtime()

    class Metrics:
        def __init__(self, settings):
            pass

        def create_backend(self, *, seed: int):
            return self

        def open(self):
            pass

        def consume(self, facts):
            return [MetricUpdate(
                "attempt" if isinstance(f, AttemptFact) else "session" if isinstance(f, SessionFact) else "turn",
                f.id, {}, f.attempts if isinstance(f, AttemptFact) else 0,
            ) for f in facts if not isinstance(f, OccupancyFact)]

        def finalize_trial(self, *args):
            if "collector.finalize" in faults:
                raise ValueError("collector.finalize")
            return TrialMetricReport((), (), {}, {})

        def release_trial(self, trial_id):
            close("collector")

        def finalize_run(self, run_id):
            return ()

        def close(self):
            close("metrics")

    class SaverBackend:
        artifact = SaverArtifact("memory", "memory://results")

        def open(self):
            pass

        def write_batch(self, records):
            for record in records:
                if isinstance(record, TrialResultRecord):
                    Saver.written_trial_parameters.append(dict(record.parameters))

        def finalize(self, run):
            saved.append(run.status)
            return object()

        def close(self):
            close("saver")

    class Saver:
        written_trial_parameters = []

        def __init__(self, settings):
            pass

        def create_backend(self, run, ctx):
            return SaverBackend()

    class Exporter:
        def __init__(self, settings):
            pass

        async def export(self, *args):
            if "exporter" in faults:
                raise ValueError("exporter")
            return ExportResult()

    registry = StageRegistry()
    factories = (
        Loader,
        Normalizer,
        Selector,
        Executor,
        SingleTurnClosedLoopScheduler,
        Metrics,
        Saver,
        Exporter,
    )
    parsers = (
        lambda value: value,
        lambda value: value,
        lambda value: value,
        lambda value: value,
        parse_closed_loop,
        lambda value: value,
        lambda value: value,
        lambda value: value,
    )
    for stage, factory, parser in zip(StageRegistry.STAGES, factories, parsers):
        registry.register(
            stage=stage,
            name="console" if stage == "exporter" else "test",
            description="test resource boundary",
            settings_parser=parser,
        )(factory)
    registry.register(stage="exporter", name="tabular", description="default table resource boundary",
                      settings_parser=lambda value: value)(Exporter)
    config: dict[str, Any] = {
        "logging": {"level": "WARNING"},
        "pipeline": {stage: {"plugin": "test"} for stage in StageRegistry.STAGES[:-1]},
    }
    config["pipeline"]["exporter"] = {}
    config["pipeline"]["scheduler"]["settings"] = {
        "max_concurrency": 1,
        "stop": {"session_count": 1},
        "on_exhausted": "error",
    }
    return Coordinator(registry), config, closed, saved


async def test_run_and_trial_resources_close_once(tmp_path):
    coordinator, config, closed, saved = pipeline_resources()
    run = await coordinator.execute(config, base_dir=tmp_path)
    assert run.status == "succeeded"
    assert closed == {"source": 1, "runtime": 1, "collector": 1, "metrics": 1, "saver": 1}
    assert saved == ["succeeded"]
    for stage in StageRegistry.STAGES:
        assert run.event_counts[f"{stage}.completed"] == 1
        assert not run.event_counts.get(f"{stage}.failed", 0)
        assert not run.event_counts.get(f"{stage}.cancelled", 0)


@pytest.mark.parametrize("failure", [None, "exporter", "collector.finalize"])
async def test_stage_timings_survive_finalization_and_export_failure(tmp_path, failure):
    from perf_benchmark.plugins.saver.sqlite import SQLiteResultReader, SQLiteSaver

    coordinator, config, _, _ = pipeline_resources(fail=failure)
    path = tmp_path / "stages.db"
    coordinator.registry.register(stage="saver", name="test", description="stage timings",
                                  settings_parser=lambda value: value, replace=True)(SQLiteSaver)
    config["pipeline"]["saver"]["settings"] = {"path": str(path)}
    if failure:
        with pytest.raises(ValueError, match=failure):
            await coordinator.execute(config, base_dir=tmp_path)
    else:
        run = await coordinator.execute(config, base_dir=tmp_path)
        assert set(run.measurements.stages) == set(StageRegistry.STAGES)
    reader = SQLiteResultReader(path)
    run_id = reader.run_ids()[0]
    stages = reader.measurements(run_id, scope="run", scope_id=run_id)["stages"]
    assert set(stages) == set(StageRegistry.STAGES) - ({"exporter"} if failure == "collector.finalize" else set())
    for stage, timing in stages.items():
        assert timing["plugin"] == ("console,tabular" if stage == "exporter" else "test")
        assert timing["started_at"] and timing["completed_at"]
        assert timing["completed_ns"] >= timing["started_ns"]
        assert timing["duration_s"] == (timing["completed_ns"] - timing["started_ns"]) / 1e9
        failed_stages = {"exporter"} if failure == "exporter" else {"scheduler", "executor", "metrics"} if failure else set()
        expected = "failed" if stage in failed_stages else "completed"
        assert timing["status"] == expected
    if not failure:
        # Stream lifetimes overlap; their durations are not exclusive CPU times.
        assert stages["loader"]["started_ns"] < stages["selector"]["started_ns"] < stages["loader"]["completed_ns"]
        assert stages["exporter"]["started_ns"] >= stages["saver"]["completed_ns"]


async def test_stage_timing_clock_is_monotonic_and_terminal_is_exclusive(tmp_path, monkeypatch, run_context):
    from dataclasses import asdict
    import perf_benchmark.pipeline.coordinator as module
    from perf_benchmark.core.models import Run

    run = Run(name="timing")
    ctx = run_context("scheduler", run=run)
    events = ctx.events
    lifecycle = module._StageLifecycle(ctx, run.measurements.stages)
    records = []
    class Sink:
        async def write_run_measurement(self, name, value):
            records.append({stage: asdict(timing) for stage, timing in value.items()})
    monkeypatch.setattr(lifecycle, "sink", Sink())
    samples = iter([1_000_000_000, 3_000_000_000])
    monkeypatch.setattr(module, "perf_counter_ns", lambda: next(samples))
    walls = iter(["2026-09-08T12:00:00+00:00", "2026-09-08T11:00:00+00:00"])
    monkeypatch.setattr(module, "now", lambda: next(walls))
    await lifecycle.start("scheduler")
    # High-frequency events neither add stage samples nor invoke either clock.
    import perf_benchmark.runtime.events as event_module
    def unexpected_clock():
        raise AssertionError("unobserved hot event must not read a diagnostic clock")
    with monkeypatch.context() as hot:
        hot.setattr(event_module, "perf_counter_ns", unexpected_clock)
        hot.setattr(event_module, "now", unexpected_clock)
        for _ in range(10_000):
            await events.emit("scheduler.concurrency.changed")
            await events.emit("request.completed")
    await lifecycle.finish("scheduler", outcome="cancelled")
    frozen = asdict(run.measurements.stages["scheduler"])
    await lifecycle.finish("scheduler", "loader", outcome="failed")
    assert asdict(run.measurements.stages["scheduler"]) == frozen
    assert frozen["duration_s"] == 2.0
    assert len(records) == 2
    assert records[0]["scheduler"]["status"] == "started"
    assert records[1]["scheduler"]["status"] == "cancelled"
    assert run.event_counts["scheduler.cancelled"] == 1
    assert "scheduler.failed" not in run.event_counts
    assert "loader" not in run.measurements.stages
    await events.close()


@pytest.mark.parametrize("mode", ["closed", "open"])
async def test_omitted_stop_resolves_to_selector_session_count_before_persistence(
    tmp_path, mode
):
    coordinator, config, _, _ = pipeline_resources()
    if mode == "closed":
        config["pipeline"]["scheduler"]["settings"] = {"max_concurrency": 1}
    else:
        coordinator.registry.register(
            stage="scheduler",
            name="open",
            description="open-loop default stop",
            settings_parser=parse_open_loop,
        )(SingleTurnOpenLoopScheduler)
        config["pipeline"]["scheduler"] = {
            "plugin": "open",
            "settings": {"request_rate": 1000, "max_pending_requests": 1},
        }

    run = await coordinator.execute(config, base_dir=tmp_path)

    saver = coordinator.registry.resolve("saver", "test").factory
    assert run.status == "succeeded"
    assert getattr(saver, "written_trial_parameters")
    for parameters in getattr(saver, "written_trial_parameters"):
        assert parameters["stop"] == {"session_count": 1}
        assert parameters["stop_source"] == "selector"
        assert parameters["on_exhausted"] == "error"


@pytest.mark.parametrize("invalid", [False, True])
async def test_trial_request_parameters_are_validated_before_runtime(tmp_path, invalid):
    from perf_benchmark.pipeline.hooks import HookSpec

    coordinator, config, _, _ = pipeline_resources()
    calls = []
    retained = {}

    class Override:
        async def handle(self, event, payload, ctx):
            calls.append("hook")
            assert "parameters" not in payload
            retained.update({"messages": ["body"]} if invalid else {"thinking": {"enabled": False}})
            return {"request_parameters": retained}

    coordinator.hook_registry.register(HookSpec("override", lambda settings: Override(), "test"))
    executor = coordinator.registry.resolve("executor", "test").factory

    @coordinator.registry.register(
        stage="executor",
        name="inspect",
        description="inspect Trial",
        settings_parser=lambda value: value,
    )
    class InspectExecutor:
        def __init__(self, settings):
            pass

        async def create_runtime(self, trial, ctx):
            calls.append("runtime")
            retained["thinking"]["enabled"] = True
            assert trial.request_parameters == {"thinking": {"enabled": False}}
            return await getattr(executor({}), "create_runtime")(trial, ctx)

    config["pipeline"]["executor"] = {"plugin": "inspect"}
    config["hooks"] = {"events": {"trial.before_start": [{"hook": "override"}]}}
    if invalid:
        with pytest.raises(ValueError, match="request_parameters cannot contain: messages"):
            await coordinator.execute(config, base_dir=tmp_path)
        assert calls == ["hook"]
    else:
        run = await coordinator.execute(config, base_dir=tmp_path)
        assert run.status == "succeeded"
        assert calls == ["hook", "runtime"]


async def test_configuration_constructs_only_selected_plugin_and_passes_settings(tmp_path):
    coordinator, config, _, _ = pipeline_resources()
    executor = coordinator.registry.resolve("executor", "test").factory
    constructed = []
    bound_settings = []

    @coordinator.registry.register(
        stage="executor",
        name="selected",
        description="selected executor",
        settings_parser=lambda value: value,
    )
    class SelectedExecutor:
        def __init__(self, settings):
            constructed.append("selected")
            bound_settings.append(dict(settings))

        async def create_runtime(self, trial, ctx):
            return await getattr(executor({}), "create_runtime")(trial, ctx)

    @coordinator.registry.register(
        stage="executor",
        name="unused",
        description="unused executor",
        settings_parser=lambda value: value,
    )
    class UnusedExecutor:
        def __init__(self, settings):
            pytest.fail("configuration did not select this executor")

    config["pipeline"]["executor"] = {"plugin": "selected", "settings": {"marker": "configured"}}
    assert constructed == []
    run = await coordinator.execute(config, base_dir=tmp_path)
    assert run.status == "succeeded"
    assert constructed == ["selected"]
    assert bound_settings == [{"marker": "configured"}]


@pytest.mark.parametrize("failure", ["runtime.prepare", "collector.finalize"])
async def test_original_failure_survives_collector_close_failure(tmp_path, failure):
    coordinator, config, closed, saved = pipeline_resources(fail=(failure, "collector.close"))
    with pytest.raises(ValueError, match=failure):
        await coordinator.execute(config, base_dir=tmp_path)
    assert closed == {"source": 1, "runtime": 1, "collector": 1, "metrics": 1, "saver": 1}
    assert saved[-1] == "failed"


async def test_cancelled_run_closes_acquired_resources_once(tmp_path):
    cancellation = CancellationToken()
    coordinator, config, closed, saved = pipeline_resources(cancel=cancellation)
    run = await coordinator.execute(config, base_dir=tmp_path, cancellation=cancellation)
    assert run.status == "cancelled"
    assert closed == {"source": 1, "runtime": 1, "collector": 1, "metrics": 1, "saver": 1}
    assert saved == ["cancelled"]


async def test_exporter_failure_updates_saved_run_and_closes_once(tmp_path):
    coordinator, config, closed, saved = pipeline_resources(fail="exporter")
    with pytest.raises(ValueError, match="exporter"):
        await coordinator.execute(config, base_dir=tmp_path)
    assert closed == {"source": 1, "runtime": 1, "collector": 1, "metrics": 1, "saver": 1}
    assert saved == ["succeeded", "failed"]


async def test_preparation_failure_closes_source_before_execution(tmp_path):
    coordinator, config, closed, saved = pipeline_resources(fail="normalization")
    with pytest.raises(ValueError, match="normalization"):
        await coordinator.execute(config, base_dir=tmp_path)
    assert closed == {"source": 1}
    assert saved == []


async def test_owned_close_finishes_once_despite_repeated_waiter_cancellation():
    import asyncio
    from contextlib import AsyncExitStack
    from perf_benchmark.pipeline.resources import own

    started, release = asyncio.Event(), asyncio.Event()
    closed = []

    async def close():
        started.set()
        await release.wait()
        closed.append("done")

    scope = AsyncExitStack()
    early_close = own(scope, close)
    closing = asyncio.create_task(early_close())
    await started.wait()
    closing.cancel()
    await asyncio.sleep(0)
    closing.cancel()
    await asyncio.sleep(0)
    assert not closing.done()
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(closing, 1)
    await early_close()
    await scope.aclose()
    assert closed == ["done"]


async def test_primary_error_survives_multiple_close_errors_and_cancellation():
    import asyncio
    from contextlib import AsyncExitStack
    from perf_benchmark.pipeline.resources import own

    started, release = asyncio.Event(), asyncio.Event()
    closed = []
    primary = ValueError("original failure")

    async def first():
        started.set()
        await release.wait()
        closed.append("first")
        raise RuntimeError("first close")

    async def second():
        closed.append("second")
        raise RuntimeError("second close")

    async def run():
        async with AsyncExitStack() as scope:
            own(scope, second)
            own(scope, first)
            raise primary

    running = asyncio.create_task(run())
    await started.wait()
    running.cancel()
    await asyncio.sleep(0)
    running.cancel()
    release.set()
    with pytest.raises(ValueError, match="original failure") as retained:
        await asyncio.wait_for(running, 1)
    assert retained.value is primary
    assert closed == ["first", "second"]
    assert any("first close" in note for note in primary.__notes__)
    assert any("second close" in note for note in primary.__notes__)


async def test_scheduler_drains_workers_before_rethrowing_original_failure():
    import asyncio
    from time import perf_counter_ns
    from perf_benchmark.core.models import Session, Turn
    from perf_benchmark.pipeline.context import SchedulerContext
    from perf_benchmark.pipeline.contracts.scheduler import TrialExecution
    from perf_benchmark.pipeline.preparation.payloads import PayloadRef
    from perf_benchmark.pipeline.scheduling.mechanics import (
        run_workload,
    )

    entered, cleaning, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    closed = []
    primary = ValueError("producer failure")

    async def sessions():
        yield Session(
            id="session", turns=[Turn(request_payload_ref=PayloadRef(0, 1), id="turn-fixture-547")]
        )
        await entered.wait()
        raise primary

    class Runner:
        async def run(self, session, admission):
            entered.set()
            cleaning.set()
            await release.wait()
            closed.append(session.id)

        async def cancel(self, session, reason):
            pass

        async def record_queue(self, kind, value):
            pass

        def record_scheduling(self, fact):
            pass

        async def worker_started(self, index):
            pass

        async def worker_stopped(self, index):
            pass

    settings = {
        "max_concurrency": 1, "stop": {"session_count": 2}, "on_exhausted": "error"
    }
    plugin = SingleTurnClosedLoopScheduler(parse_closed_loop(settings))
    plan = next(iter(plugin.trial_plans()))
    execution = TrialExecution(sessions(), Runner())
    ctx = SchedulerContext(CancellationToken())
    strategy = plugin.create_strategy(plan, perf_counter_ns(), seed=ctx.seed)
    running = asyncio.create_task(
        run_workload(execution, strategy, plan, ctx, expected_turns=1)
    )
    await asyncio.wait_for(cleaning.wait(), 1)
    assert not running.done()
    release.set()
    with pytest.raises(ValueError, match="producer failure") as retained:
        await asyncio.wait_for(running, 1)
    assert retained.value is primary
    assert closed == ["session"]


async def test_scheduler_drain_cancels_blocked_producer_without_waiting_for_stop_slots():
    import asyncio
    from time import perf_counter_ns
    from perf_benchmark.core.models import Session, Turn
    from perf_benchmark.pipeline.context import SchedulerContext
    from perf_benchmark.pipeline.contracts.scheduler import TrialExecution
    from perf_benchmark.pipeline.preparation.payloads import PayloadRef
    from perf_benchmark.pipeline.scheduling.mechanics import (
        run_workload,
    )

    third_offered = asyncio.Event()
    primary = ValueError("worker failure with full pending queue")

    async def sessions():
        for ordinal in range(3):
            if ordinal == 2:
                third_offered.set()
            yield Session(
                id=f"session-{ordinal}",
                turns=[Turn(request_payload_ref=PayloadRef(0, 1), id="turn-fixture-614")],
            )

    class Runner:
        async def run(self, session, admission):
            await third_offered.wait()
            raise primary

        async def cancel(self, session, reason):
            pass

        async def record_queue(self, kind, value):
            pass

        def record_scheduling(self, fact):
            pass

        async def worker_started(self, index):
            pass

        async def worker_stopped(self, index):
            pass

    settings = {
        "max_concurrency": 1,
        "stop": {"session_count": 3},
        "on_exhausted": "error",
    }
    plugin = SingleTurnClosedLoopScheduler(parse_closed_loop(settings))
    plan = next(iter(plugin.trial_plans()))
    execution = TrialExecution(sessions(), Runner())
    ctx = SchedulerContext(CancellationToken())
    strategy = plugin.create_strategy(plan, perf_counter_ns(), seed=ctx.seed)

    with pytest.raises(ValueError, match="worker failure with full pending queue") as retained:
        await asyncio.wait_for(
            run_workload(execution, strategy, plan, ctx, expected_turns=1), 1
        )
    assert retained.value is primary


@pytest.mark.parametrize("external_cancel", [False, True])
async def test_sweep_deadline_preserves_cancelled_attempt_and_turn(tmp_path, external_cancel):
    import asyncio
    from perf_benchmark.plugins.metrics.standard import StandardMetrics
    from perf_benchmark.plugins.saver.sqlite import SQLiteSaver, SQLiteResultReader

    coordinator, config, closed, _ = pipeline_resources()
    executor = cast(ExecutorPlugin, coordinator.registry.bind("executor", "test", {}).plugin)
    started = asyncio.Event()

    class SlowExecutor:
        def __init__(self, settings):
            pass

        async def create_runtime(self, trial, ctx):
            runtime = await executor.create_runtime(trial, ctx)
            async def execute(prepared, signal) -> AttemptResult:
                started.set()
                await asyncio.sleep(10)
                raise AssertionError("test must cancel this attempt")
            runtime.execute_attempt = execute
            return runtime

    coordinator.registry.register(stage="executor", name="test", description="deadline test",
                                  settings_parser=lambda value: value, replace=True)(SlowExecutor)
    coordinator.registry.register(stage="metrics", name="test", description="deadline test",
                                  settings_parser=lambda value: value, replace=True)(
                                      lambda settings: StandardMetrics(cast(dict[str, object], settings)))
    coordinator.registry.register(stage="saver", name="test", description="deadline test",
                                  settings_parser=lambda value: value, replace=True)(SQLiteSaver)
    config["pipeline"]["saver"]["settings"] = {"path": str(tmp_path / "deadline.db")}
    config["pipeline"]["scheduler"]["settings"] = {
        "sweep": {"axis": "max_concurrency", "search": {
            "min": 1, "max": 2, "initial": 1, "resolution": 1, "max_trials": 6, "timeout_s": 1,
        }, "slo": {"rules": [{"metric": "ttft", "statistic": "am", "op": "le", "value": "1s"}]}},
    }
    async with asyncio.timeout(5):
        running = asyncio.create_task(coordinator.execute(config, base_dir=tmp_path))
        if external_cancel:
            await started.wait()
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
        else:
            run = await running
            assert run.status == "cancelled"
    assert started.is_set()
    assert closed["runtime"] == 1
    reader = SQLiteResultReader(tmp_path / "deadline.db")
    run_id = reader.run_ids()[0]
    records = reader.sweep_records(run_id)
    expected = ("cancelled", "task_cancelled") if external_cancel else ("budget_exhausted", "timeout")
    assert (records[-1].status, records[-1].reason) == expected
    assert len(reader.trials(run_id)) == 1
    assert reader.trials(run_id)[0]["status"] == "cancelled"
    attempts = tuple(reader.iter_attempt_records(run_id))
    turns = tuple(reader.iter_turn_records(run_id))
    assert len(attempts) == len(turns) == 1
    assert attempts[0]["status"] == turns[0]["status"] == "cancelled"
