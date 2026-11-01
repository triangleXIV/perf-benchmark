"""Explicit aggregate-only retention for capacity tests; no request rows."""

from collections import Counter
import json
import math
from pathlib import Path
from time import monotonic

from perf_benchmark.pipeline.contracts.results import (
    AttemptResultRecord,
    SessionResultRecord,
    TurnResultRecord,
)


class SummaryRetention:
    def __init__(self, path):
        self.path = Path(path).with_suffix(".summary.json")
        self.counts = Counter()
        self.errors = Counter()
        self.last_write = monotonic()
        self.finished = False
        self.latencies = {
            name: {"sum": 0.0, "samples": 0, "unit": unit}
            for name, unit in (("ttft", "s"), ("tpot", "ms"), ("e2e", "s"))
        }
        self.decode_tokens = 0
        self.decode_seconds = 0.0

    def filter(self, records):
        kept = []
        for record in records:
            if isinstance(record, (AttemptResultRecord, TurnResultRecord, SessionResultRecord)):
                scope = (
                    "attempt"
                    if isinstance(record, AttemptResultRecord)
                    else "turn"
                    if isinstance(record, TurnResultRecord)
                    else "session"
                )
                self.counts[f"{scope}_{record.status}"] += 1
                if scope == "turn" and record.status == "succeeded":
                    for name, total in self.latencies.items():
                        metric = record.metrics.get(name)
                        number = metric.statistics.get("value") if metric else None
                        if (
                            metric
                            and metric.availability == "available"
                            and metric.unit == total["unit"]
                            and number is not None
                            and math.isfinite(number)
                        ):
                            total["sum"] += number
                            total["samples"] += 1
                    metric = record.metrics.get("decode_throughput")
                    speed = metric.statistics.get("value") if metric else None
                    tokens = record.usage.get("completion_tokens")
                    if (
                        metric
                        and metric.availability == "available"
                        and metric.unit == "tok/s"
                        and speed is not None
                        and math.isfinite(speed)
                        and speed > 0
                        and type(tokens) is int
                        and tokens > 1
                    ):
                        self.decode_tokens += tokens - 1
                        self.decode_seconds += (tokens - 1) / speed
                    for key in ("prompt_tokens", "completion_tokens", "cached_tokens"):
                        number = record.usage.get(key)
                        if type(number) is int and number >= 0:
                            self.counts[key] += number
                        else:
                            self.counts["missing_" + key] += 1
                if scope == "attempt" and record.error:
                    key = record.error[:512]
                    if key not in self.errors and len(self.errors) >= 64:
                        key = "other_errors"
                    self.errors[key] += 1
            else:
                kept.append(record)
        if not self.finished and monotonic() - self.last_write >= 10:
            self.write(
                {
                    "status": "running",
                    "counts": dict(self.counts),
                    "attempt_errors": dict(self.errors),
                }
            )
        return tuple(kept)

    def write(self, values):
        values = {
            "format": "benchmark-summary-only-v1",
            "per_request_records_retained": False,
            "latency_preview": self.latencies,
            "decode_preview": {"tokens": self.decode_tokens, "seconds": self.decode_seconds},
            **values,
        }
        temporary = self.path.with_suffix(".json.tmp")
        temporary.write_text(
            json.dumps(values, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
        )
        temporary.replace(self.path)
        self.last_write = monotonic()

    def finalize(self, reader, run_id):
        trials = reader.trials(run_id)
        self.finished = True
        self.write(
            {
                "status": reader.summary(run_id)["status"],
                "run": reader.summary(run_id),
                "counts": dict(self.counts),
                "attempt_errors": dict(self.errors),
                "trials": [
                    dict(
                        t,
                        metrics={
                            window["window"]: reader.metric_points(
                                run_id,
                                scope="trial",
                                scope_id=t["trial_id"],
                                window=window["window"],
                            )
                            for window in reader.metric_windows(
                                run_id, scope="trial", scope_id=t["trial_id"]
                            )
                        },
                    )
                    for t in trials
                ],
            }
        )
