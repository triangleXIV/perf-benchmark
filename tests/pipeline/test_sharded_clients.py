import json
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from benchmarks.sharded_clients import ShardPreparation, final_summary, write_json


async def test_shards_are_disjoint_complete_and_do_not_warm_inference(tmp_path):
    write_json(tmp_path / "go.json", {"start": True})
    source = tuple(SimpleNamespace(ordinal=i, turns=(None,) * ((i % 3) + 1)) for i in range(16))
    ordinals = []
    turns = 0
    for i in range(4):
        corpus = SimpleNamespace(
            plans=source,
            session_count=len(source),
            turn_count=sum(len(p.turns) for p in source),
            max_graph_branches=0,
        )
        adapter = ShardPreparation(tmp_path, i, 4)
        fact = await adapter.prepare(
            None, SimpleNamespace(is_cancelled=lambda: False), corpus, None
        )
        assert fact.planned_requests == fact.succeeded_requests == 0
        ready = json.loads((tmp_path / f"worker-{i}.ready.json").read_text())
        ordinals.extend(ready["source_ordinals"])
        turns += ready["turns"]
        assert ready["sessions"] == 4
        with pytest.raises(ValueError):
            await adapter.prepare(None, None, corpus, None)
    assert sorted(ordinals) == list(range(16)) and turns == sum(len(p.turns) for p in source)


def test_pooled_quantiles_use_numeric_samples_and_global_concurrency(tmp_path):
    base = {
        "status": "succeeded",
        "request_ns": 1_000_000_000,
        "response_completed_ns": 3_000_000_000,
        "prompt": 100,
        "completion": 11,
        "cached": 80,
        "ttft": 100.0,
        "tpot": 20.0,
        "e2e": 1000.0,
        "decode_tokens": 10,
        "decode_ns": 200_000_000,
    }
    for i in range(2):
        p = tmp_path / f"worker-{i}"
        p.mkdir()
        row = (
            base
            if i == 0
            else {
                **base,
                "completion": 21,
                "ttft": 300.0,
                "tpot": 40.0,
                "e2e": 2000.0,
                "decode_tokens": 20,
                "decode_ns": 800_000_000,
            }
        )
        pq.write_table(pa.Table.from_pylist([row]), p / "key-samples.parquet")
        pq.write_table(
            pa.table({"value": [10.0, 20.0] if i == 0 else [30.0, 40.0]}), p / "itl-samples.parquet"
        )
        pq.write_table(
            pa.table({"kind": ["in_flight"] * 2, "at_ns": [1 + i, 5 + i], "value": [2, 0]}),
            p / "concurrency.parquet",
        )
    result = final_summary(
        {"output": str(tmp_path), "workers": 2, "concurrency": 4, "active_users": 6}
    )
    assert result["turns"] == {"total": 2, "succeeded": 2, "failed": 0, "cancelled": 0}
    assert result["latency_seconds"]["ttft"]["p90"] == pytest.approx(0.28)
    assert result["itl_seconds"]["p90"] == pytest.approx(0.037)
    assert result["completed_output_tok_s"] == 16
    assert result["decode_time_weighted_tok_s"] == 30
    assert result["peak_in_flight"] == 4


def test_latency_previews_pool_sums_and_keep_missing_samples_unknown():
    from benchmarks.sharded_clients import latency_preview

    rows = [
        {
            "latency_preview": {
                "ttft": {"sum": 1.0, "samples": 10, "unit": "s"},
                "tpot": {"sum": 20.0, "samples": 10, "unit": "ms"},
            },
            "decode_preview": {"tokens": 10, "seconds": 0.1},
        },
        {
            "latency_preview": {
                "ttft": {"sum": 3.0, "samples": 2, "unit": "s"},
                "tpot": {"sum": 30.0, "samples": 2, "unit": "ms"},
            },
            "decode_preview": {"tokens": 30, "seconds": 0.9},
        },
        {},
    ]
    value = latency_preview(rows)
    assert value["ttft_mean_s"] == pytest.approx(4 / 12)
    assert value["tpot_mean_ms"] == pytest.approx(50 / 12)
    assert value["e2e_mean_s"] is None and value["e2e_samples"] == 0
    assert value["decode_time_weighted_tok_s"] == 40
    assert value["latency_workers_available"] == 2 and value["latency_workers_expected"] == 3


def test_final_summary_converts_native_milliseconds_and_uses_same_decode_cohort(tmp_path):
    from perf_benchmark.plugins.metrics.standard.formulas import request_values, itl_values

    fact = SimpleNamespace(
        response_mode="stream",
        output_kind="text",
        request_ns=1_000_000_000,
        first_ns=1_100_000_000,
        last_ns=1_300_000_000,
        response_completed_ns=1_400_000_000,
        output_event_count=2,
        usage={"prompt_tokens": 100, "completion_tokens": 11},
        turn_completed_ns=1_400_000_000,
        total_started_ns=1_000_000_000,
        response_samples=((0, 1_100_000_000), (1, 1_300_000_000)),
    )
    values = request_values(fact)
    row = {
        "status": "succeeded",
        "request_ns": fact.request_ns,
        "response_completed_ns": fact.response_completed_ns,
        "prompt": 100,
        "completion": 11,
        "cached": 0,
        **{name: values[name] for name in ("ttft", "tpot", "e2e", "decode_tokens", "decode_ns")},
    }
    worker = tmp_path / "worker-0"
    worker.mkdir()
    # A successful response with missing decode timing must not add token weight.
    missing = {**row, "completion": 101, "decode_tokens": 100, "decode_ns": None}
    pq.write_table(pa.Table.from_pylist([row, missing]), worker / "key-samples.parquet")
    pq.write_table(pa.table({"value": list(itl_values(fact))}), worker / "itl-samples.parquet")
    result = final_summary(
        {"output": str(tmp_path), "workers": 1, "concurrency": 1, "active_users": 1}
    )
    assert result["latency_seconds"]["ttft"]["p90"] == pytest.approx(0.1)
    assert result["latency_seconds"]["tpot"]["p90"] == pytest.approx(0.02)
    assert result["latency_seconds"]["e2e"]["p90"] == pytest.approx(0.4)
    assert result["itl_seconds"]["p90"] == pytest.approx(0.2)
    assert result["inverse_p90_itl_events_s"] == pytest.approx(5.0)
    assert result["decode_time_weighted_tok_s"] == pytest.approx(50.0)
