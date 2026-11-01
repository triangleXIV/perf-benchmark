import json

import pytest

from perf_benchmark.core.models import Run
from perf_benchmark.pipeline.coordinator import _StageLifecycle
from perf_benchmark.pipeline.preparation.streaming import PreparedCorpus
from perf_benchmark.plugins import create_registry


def _context(run_context, source, *, policy="empty", seed=42):
    run = Run(name="cache-test")
    ctx = run_context("loader", run=run)
    registry = create_registry()
    settings = {
        "loader": ("hf-datasets", {"dataset": "json", "data_files": str(source), "split": "train",
                                    "cache_dir": str(source.parent / "hf-cache")}),
        "normalization": ("open-swe-traces", {"missing_tool_call_id": policy}),
        "selector": ("all", {}),
    }
    ctx.bindings = {stage: registry.bind(stage, name, value) for stage, (name, value) in settings.items()}
    ctx.seed = seed
    run.prepare()
    return run, ctx, PreparedCorpus(ctx.payloads)


@pytest.fixture
def source(tmp_path, monkeypatch):
    monkeypatch.setenv("PERF_BENCHMARK_PREPARED_CACHE", str(tmp_path / "cache"))
    monkeypatch.setenv("PERF_BENCHMARK_PREPARATION_WORKERS", "1")
    value = {"benchmark_session_id": "s", "trajectory_id": "t", "tools": [],
             "messages": [{"role": "user", "content": "中文"},
                          {"role": "assistant", "content": "recorded"},
                          {"role": "tool", "content": "ok"},
                          {"role": "assistant", "content": "final"}],
             "benchmark_input_tokens": [3, 7], "benchmark_output_tokens": [2, 4]}
    path = tmp_path / "source.jsonl"
    path.write_text(json.dumps(value) + "\n")
    return path


async def _prepare(run, ctx, corpus):
    await corpus.prepare(run, ctx, _StageLifecycle(ctx, run.measurements.stages))


async def test_cache_survives_client_close_and_rebinds_run_ids(source, run_context, monkeypatch):
    run, ctx, corpus = _context(run_context, source)
    await _prepare(run, ctx, corpus)
    bodies = [bytes(corpus.payloads.view(t.request_payload_ref)) for p in corpus.plans for t in p.turns]
    metadata = [t.metadata for p in corpus.plans for t in p.turns]
    counts = dict(run.counts)
    await corpus.close()

    def forbidden(*args, **kwargs):
        raise AssertionError("cache hit must skip loading and normalization")

    monkeypatch.setattr("perf_benchmark.pipeline.preparation.streaming.LoaderHost.open", forbidden)
    run2, ctx2, corpus2 = _context(run_context, source)
    await _prepare(run2, ctx2, corpus2)
    try:
        assert corpus2.session_count == 1 and corpus2.turn_count == 2
        assert dict(run2.counts) == counts
        assert bodies == [bytes(corpus2.payloads.view(t.request_payload_ref)) for p in corpus2.plans for t in p.turns]
        assert [t.metadata for p in corpus2.plans for t in p.turns] == metadata
        from perf_benchmark.core.models import Trial
        sessions = corpus2.materialize_sessions(Trial(id=run2.id + ":trial-1"))
        assert all(t.id.startswith(run2.id + ":") for s in sessions for t in s.turns)
        assert run.id != run2.id
        assert len(list((source.parent / "cache").glob("*/manifest.json"))) == 1
    finally:
        await corpus2.close()


@pytest.mark.parametrize("change", ["source", "policy", "seed"])
async def test_changed_inputs_invalidate_preparation_cache(source, run_context, change):
    run, ctx, corpus = _context(run_context, source)
    await _prepare(run, ctx, corpus)
    await corpus.close()
    kwargs = {}
    if change == "source":
        value = json.loads(source.read_text())
        value["messages"][0]["content"] = "changed"
        source.write_text(json.dumps(value) + "\n")
    elif change == "policy":
        kwargs["policy"] = "preserve"
    else:
        kwargs["seed"] = 43
    run2, ctx2, corpus2 = _context(run_context, source, **kwargs)
    try:
        await _prepare(run2, ctx2, corpus2)
        assert len(list((source.parent / "cache").glob("*/manifest.json"))) == 2
    finally:
        await corpus2.close()


async def test_modified_payload_is_rejected(source, run_context):
    run, ctx, corpus = _context(run_context, source)
    await _prepare(run, ctx, corpus)
    await corpus.close()
    body, = (source.parent / "cache").glob("*/payloads.bin")
    body.chmod(0o644)
    with body.open("ab") as stream:
        stream.write(b"changed")
    run2, ctx2, corpus2 = _context(run_context, source)
    try:
        with pytest.raises(ValueError, match="artifact changed"):
            await _prepare(run2, ctx2, corpus2)
    finally:
        await corpus2.close()


async def test_incomplete_preparation_is_not_published(source, run_context, monkeypatch):
    async def fail(*args, **kwargs):
        raise RuntimeError("interrupted preparation")

    monkeypatch.setattr("perf_benchmark.pipeline.preparation.streaming.SelectorHost.select", fail)
    run, ctx, corpus = _context(run_context, source)
    try:
        with pytest.raises(RuntimeError, match="interrupted preparation"):
            await _prepare(run, ctx, corpus)
        assert not list((source.parent / "cache").glob("*/manifest.json"))
        assert not list((source.parent / "cache").glob("*.building-*"))
    finally:
        await corpus.close()
