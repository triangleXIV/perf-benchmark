"""Parallel CPU clients over disjoint views of one sealed prepared corpus.

The existing Executor, HTTP clocks, Scheduler and Metrics remain in use. Only
aggregate metrics and compact numeric samples needed for pooled quantiles persist.
"""

import argparse
import asyncio
from collections import deque
import concurrent.futures
import datetime
import json
from pathlib import Path
import re
import signal
import time
import urllib.request


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


class ShardPreparation:
    def __init__(self, directory, index, count):
        self.directory, self.index, self.count = Path(directory), index, count
        self.used = False

    async def prepare(self, trial, cancellation, corpus, executor):
        from perf_benchmark.pipeline.contracts.preparation import PreparationFact

        if self.used:
            raise ValueError("one measured Trial per shard is required")
        self.used = True
        if corpus.max_graph_branches:
            raise ValueError("causal graphs cannot be split by this benchmark adapter")
        original_count, original_turns = corpus.session_count, corpus.turn_count
        selected = corpus.plans[self.index :: self.count]
        ordinals = [p.ordinal for p in selected]
        corpus.plans = selected
        corpus.turn_count = sum(len(p.turns) for p in selected)
        write_json(
            self.directory / f"worker-{self.index}.ready.json",
            {
                "index": self.index,
                "count": self.count,
                "source_sessions": original_count,
                "source_turns": original_turns,
                "sessions": len(selected),
                "turns": corpus.turn_count,
                "source_ordinals": ordinals,
                "inference_warmup_requests": 0,
            },
        )
        while not (self.directory / "go.json").exists():
            if cancellation.is_cancelled():
                raise asyncio.CancelledError
            await asyncio.sleep(0.1)
        return PreparationFact(
            "disjoint-sealed-corpus-shard",
            0,
            0,
            "cold_boundary_verified_by_coordinator",
            "not_applicable",
        )


async def worker(spec, index):
    from perf_benchmark.plugins import create_registry
    from perf_benchmark.plugins.executor.openai_chat import OpenAIChatExecutor
    from perf_benchmark.plugins.metrics.standard import StandardMetrics
    from perf_benchmark.plugins.metrics.standard.backend import StandardMetricsBackend
    from perf_benchmark.pipeline.coordinator import Coordinator
    from perf_benchmark.runtime.cancellation import CancellationToken

    directory = Path(spec["directory"])
    config = json.loads((directory / f"worker-{index}.json").read_text())
    output = Path(config["pipeline"]["saver"]["settings"]["path"]).parent
    registry = create_registry()

    class SharedSaltExecutor(OpenAIChatExecutor):
        def cache_salt(self, value, settings):
            return spec["cache_salt"] if settings.get("cache_salt_enabled") else None

    class NumericSamplesBackend(StandardMetricsBackend):
        def finalize_trial(self, fact):
            report = super().finalize_trial(fact)
            trial = fact.trial_id.replace("'", "''")
            selects = {
                "key-samples": "SELECT status,request_ns,response_completed_ns,prompt,completion,cached,"
                "ttft,tpot,e2e,decode_tokens,decode_ns FROM turns "
                f"WHERE trial_id='{trial}'",
                "itl-samples": "SELECT i.value FROM itl i JOIN turns t USING(trial_id,turn_id) "
                f"WHERE i.trial_id='{trial}' AND t.status='succeeded'",
                "concurrency": "SELECT kind,at_ns,value FROM occupancy WHERE kind IN "
                f"('in_flight','active_sessions') AND trial_id='{trial}'",
            }
            for name, query in selects.items():
                path = output / (name + ".parquet")
                temporary = path.with_suffix(".parquet.tmp")
                escaped = str(temporary).replace("'", "''")
                self.db.execute(f"COPY ({query}) TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)")
                temporary.replace(path)
            return report

    class NumericSamplesMetrics(StandardMetrics):
        def create_backend(self, *, seed):
            return NumericSamplesBackend(self.settings, seed=seed)

    for stage, name, factory in [
        ("executor", "openai-chat", SharedSaltExecutor),
        ("metrics", "standard", NumericSamplesMetrics),
    ]:
        parser = registry.resolve(stage, name).settings_parser
        registry.register(
            stage=stage,
            name=name,
            description="Sharded benchmark adapter",
            settings_parser=parser,
            replace=True,
        )(factory)
    token = CancellationToken()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, token.cancel, "signal")
    status = "failed"
    try:
        run = await Coordinator(registry).execute(
            config,
            base_dir=Path(spec["workdir"]),
            cancellation=token,
            preparation=ShardPreparation(directory, index, spec["workers"]),
            evidence={
                "conditions": {
                    "shard_index": index,
                    "shards": spec["workers"],
                    "global_concurrency": spec["concurrency"],
                    "global_active_sessions": spec["active_users"],
                    "cache_salt_shared_across_shards": True,
                    "prepared_corpus_shared_readonly": True,
                }
            },
        )
        status = run.status
        return 0 if status == "succeeded" else 1
    finally:
        write_json(directory / f"worker-{index}.finished.json", {"index": index, "status": status})


def metrics(url):
    try:
        op = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        body = op.open(url, timeout=2).read().decode()
        return [
            (m[1], m[2] or "", float(m[3]))
            for line in body.splitlines()
            if (m := re.match(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})?\s+([0-9.eE+\-]+)$", line))
        ]
    except Exception:
        return None


def latency_preview(summaries):
    """Pool sufficient statistics; a missing shard stays visible as partial."""
    result = {"latency_preview": True}
    available = sum("latency_preview" in row for row in summaries)
    result["latency_workers_available"] = available
    result["latency_workers_expected"] = len(summaries)
    for name, unit in (("ttft", "s"), ("tpot", "ms"), ("e2e", "s")):
        totals = [row.get("latency_preview", {}).get(name, {}) for row in summaries]
        samples = sum(row.get("samples", 0) for row in totals if row.get("unit") == unit)
        total = sum(row.get("sum", 0.0) for row in totals if row.get("unit") == unit)
        result[name + "_mean_" + unit] = total / samples if samples else None
        result[name + "_samples"] = samples
    tokens = sum(row.get("decode_preview", {}).get("tokens", 0) for row in summaries)
    seconds = sum(row.get("decode_preview", {}).get("seconds", 0.0) for row in summaries)
    result["decode_time_weighted_tok_s"] = tokens / seconds if seconds else None
    return result


def final_summary(spec):
    import duckdb

    db = duckdb.connect()
    output = Path(spec["output"])
    sources = [str(output / f"worker-{i}/key-samples.parquet") for i in range(spec["workers"])]
    if not all(Path(p).exists() for p in sources):
        return {"status": "partial", "reason": "some numeric sample files are unavailable"}
    db.read_parquet(sources).create_view("samples")
    counts = db.execute(
        "SELECT count(*),count(*) FILTER(WHERE status='succeeded'),"
        "count(*) FILTER(WHERE status='failed'),count(*) FILTER(WHERE status='cancelled'),"
        "min(request_ns),max(response_completed_ns) FROM samples"
    ).fetchone()
    elapsed = (
        (counts[5] - counts[4]) / 1e9 if counts[4] is not None and counts[5] > counts[4] else None
    )
    result = {
        "status": "completed" if counts[2] == counts[3] == 0 else "completed_with_request_errors",
        "concurrency": spec["concurrency"],
        "active_users": spec["active_users"],
        "turns": dict(zip(("total", "succeeded", "failed", "cancelled"), counts[:4])),
        "request_start_to_last_response_seconds": elapsed,
        "latency_seconds": {},
        "retention": "aggregate metrics and compact numeric samples only",
    }
    for name in ("ttft", "tpot", "e2e"):
        row = db.execute(
            f"SELECT count({name}),avg({name}),quantile_cont({name},[.5,.9,.99]) "
            "FROM samples WHERE status='succeeded'"
        ).fetchone()
        result["latency_seconds"][name] = {
            "samples": row[0],
            "mean": row[1],
            **dict(zip(("p50", "p90", "p99"), row[2] or [None] * 3)),
        }
    totals = db.execute(
        "SELECT sum(prompt),sum(completion),sum(cached),sum(decode_tokens),sum(decode_ns) "
        "FROM samples WHERE status='succeeded'"
    ).fetchone()
    result["tokens"] = dict(zip(("prompt", "output", "cached_prompt"), totals[:3]))
    result["completed_output_tok_s"] = (
        totals[1] / elapsed if elapsed and totals[1] is not None else None
    )
    result["decode_time_weighted_tok_s"] = totals[3] * 1e9 / totals[4] if totals[4] else None
    itl = [str(output / f"worker-{i}/itl-samples.parquet") for i in range(spec["workers"])]
    if all(Path(p).exists() for p in itl):
        db.read_parquet(itl).create_view("itl_samples")
        row = db.execute(
            "SELECT count(*),quantile_cont(value,[.5,.9,.99]) FROM itl_samples"
        ).fetchone()
        result["itl_seconds"] = {
            "samples": row[0],
            **dict(zip(("p50", "p90", "p99"), row[1] or [None] * 3)),
        }
        result["inverse_p90_itl_tok_s"] = 1 / row[1][1] if row[1] and row[1][1] > 0 else None
    occupancy = [str(output / f"worker-{i}/concurrency.parquet") for i in range(spec["workers"])]
    if all(Path(p).exists() for p in occupancy):
        db.read_parquet(occupancy, filename=True).create_view("occupancy_samples")
        peak = db.execute(
            "WITH changes AS (SELECT at_ns,value-lag(value,1,0) OVER "
            "(PARTITION BY filename ORDER BY at_ns) AS delta FROM occupancy_samples WHERE kind='in_flight'),"
            "summed AS (SELECT at_ns,sum(delta) AS delta FROM changes GROUP BY at_ns),"
            "flow AS (SELECT sum(delta) OVER(ORDER BY at_ns) n FROM summed) SELECT max(n) FROM flow"
        ).fetchone()[0]
        result["peak_in_flight"] = int(peak or 0)
    db.close()
    return result


def aggregate(spec):
    directory, output = Path(spec["directory"]), Path(spec["output"])
    history = deque()
    while True:
        at = time.monotonic()
        ready = sum((directory / f"worker-{i}.ready.json").exists() for i in range(spec["workers"]))
        done = sum(
            (directory / f"worker-{i}.finished.json").exists() for i in range(spec["workers"])
        )
        urls = [
            f"http://127.0.0.1:{spec.get('observer_base_port', 28200) + i}/metrics"
            for i in range(spec["workers"])
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=spec["workers"]) as pool:
            sources = list(pool.map(metrics, urls))
        value = {
            "time": datetime.datetime.now(
                datetime.timezone(datetime.timedelta(hours=8))
            ).isoformat(),
            "ready_workers": ready,
            "finished_workers": done,
            "configured_concurrency": spec["concurrency"],
        }
        counts, summaries = {}, []
        for i in range(spec["workers"]):
            path = output / f"worker-{i}/results.summary.json"
            if path.exists():
                summary = json.loads(path.read_text())
                for key, n in summary.get("counts", {}).items():
                    counts[key] = counts.get(key, 0) + n
            else:
                summary = {}
            # For an already running older client, an external observer can
            # supply previews without changing that client's measured execution.
            preview = directory / f"worker-{i}.preview.json"
            if "latency_preview" not in summary and preview.exists():
                summary.update(json.loads(preview.read_text()))
            summaries.append(summary)
        value["counts"] = counts
        value.update(latency_preview(summaries))
        for key in ("in_flight", "active_sessions", "request_pending"):
            samples = [
                next(
                    (
                        n
                        for name, labels, n in rows or []
                        if name == "perf_benchmark_load" and f'metric="{key}"' in labels
                    ),
                    None,
                )
                for rows in sources
            ]
            value[key] = sum(samples) if all(n is not None for n in samples) else None
        completed = counts.get("completion_tokens", 0)
        history.append((at, completed))
        while len(history) > 1 and at - history[1][0] >= 20:
            history.popleft()
        value["completed_output_tok_s"] = (
            (completed - history[0][1]) / (at - history[0][0]) if at - history[0][0] >= 10 else None
        )
        value["output_rate_window_seconds"] = at - history[0][0]
        write_json(output / "live.json", value)
        print(json.dumps(value, ensure_ascii=False), flush=True)
        if done == spec["workers"]:
            final = final_summary(spec)
            write_json(output / "summary.json", final)
            print(json.dumps(final, ensure_ascii=False), flush=True)
            return
        time.sleep(max(0, 5 - (time.monotonic() - at)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--worker", type=int)
    parser.add_argument("--aggregate", action="store_true")
    args = parser.parse_args()
    spec = json.loads(args.spec.read_text())
    if args.aggregate:
        aggregate(spec)
    else:
        import uvloop

        raise SystemExit(uvloop.run(worker(spec, args.worker)))
