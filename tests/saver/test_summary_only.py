import json
import sqlite3

import pytest

from perf_benchmark.core.models import Run, Trial, Session, Turn, MetricWindowMeasurement
from perf_benchmark.pipeline.contracts.results import (
    AttemptResultRecord,
    ComputedResultBatch,
    MetricPoint,
    TurnResultRecord,
    SessionResultRecord,
)
from perf_benchmark.pipeline.results.saver import SaverRuntime, _snapshot
from perf_benchmark.pipeline.preparation.payloads import PayloadRef
from perf_benchmark.plugins.saver.sqlite import SQLiteSaver, parse_settings


@pytest.mark.parametrize("value", [1, "true", None])
def test_summary_only_setting_requires_boolean(value):
    with pytest.raises(ValueError):
        parse_settings({"path": "results.db", "summary_only": value})


async def test_summary_only_preserves_counts_and_metric_values_without_request_rows(
    tmp_path, stage_context
):
    outputs = []
    for summary_only in (False, True):
        run = Run(id="run-retention", name="retention")
        run.prepare()
        run.start()
        trial = Trial(id="trial-retention", run_id=run.id)
        trial.start()
        turn = Turn(
            id="turn-retention",
            session_id="session-retention",
            request_payload_ref=PayloadRef(0, 1),
        )
        session = Session(id="session-retention", trial_id=trial.id, turns=[turn])
        session.start()
        path = tmp_path / f"retention-{summary_only}.db"
        sink = await SaverRuntime.create(
            SQLiteSaver({"path": str(path), "summary_only": summary_only}),
            run,
            stage_context("saver", run=run),
        )
        try:
            await sink.write_trial(trial)
            await sink.write_session(session)
            turn.start()
            turn.measurements.request.attempts = 1
            turn.succeed()
            session.finish()
            attempt = AttemptResultRecord(
                turn.id,
                session.id,
                1,
                "succeeded",
                None,
                None,
                {},
                {},
                {},
                None,
                None,
                None,
                None,
                {},
            )
            sink.write_results(
                ComputedResultBatch(
                    turns=(_snapshot(TurnResultRecord, turn),),
                    sessions=(_snapshot(SessionResultRecord, session, turn_count=1),),
                    attempts=(attempt,),
                )
            )
            trial.observe(session)
            trial.finish()
            trial.measurements.metric_windows = (
                MetricWindowMeasurement("overall", 1, 2, 1e-9, "available", None),
            )
            await sink.write_trial(trial)
            await sink.write_metric_points(
                (
                    MetricPoint(
                        "trial",
                        trial.id,
                        "overall",
                        "ttft",
                        "p90",
                        0.25,
                        "s",
                        "test-v1",
                        "available",
                        None,
                        "test",
                        1,
                        1,
                        0,
                    ),
                )
            )
            run.register_trial(trial)
            run.observe_trial(trial)
            run.finish()
            reader = await sink.finalize(run)
            outputs.append(
                (
                    reader.summary(run.id)["session_count"],
                    reader.summary(run.id)["turn_count"],
                    reader.metric_points(
                        run.id, scope="trial", scope_id=trial.id, window="overall"
                    ),
                )
            )
            with sqlite3.connect(path) as db:
                assert [
                    db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                    for table in ("sessions", "turns", "attempts")
                ] == ([0, 0, 0] if summary_only else [1, 1, 1])
            if summary_only:
                saved = json.loads(path.with_suffix(".summary.json").read_text())
                assert saved["per_request_records_retained"] is False
                assert saved["counts"]["attempt_succeeded"] == 1
                assert saved["counts"]["turn_succeeded"] == 1
                assert saved["run"]["turn_count"] == 1
                assert saved["trials"][0]["metrics"]["overall"]["ttft"]["statistics"]["p90"] == 0.25
        finally:
            await sink.close()
        # The automatic Parquet archive must retain logical observed counts,
        # even though summary-only mode deliberately has zero detail rows.
        archived = SQLiteSaver.open_reader(str(path))
        assert archived.summary(run.id)["session_count"] == 1
        assert archived.summary(run.id)["turn_count"] == 1
    assert outputs[0] == outputs[1]


def test_summary_only_bounds_error_categories(tmp_path):
    from perf_benchmark.plugins.saver.sqlite.summary_retention import SummaryRetention

    retention = SummaryRetention(tmp_path / "summary.db")
    records = tuple(
        AttemptResultRecord(
            "turn",
            "session",
            i,
            "failed",
            f"error-{i}",
            "transport",
            {},
            {},
            {},
            None,
            None,
            None,
            None,
            {},
        )
        for i in range(1000)
    )
    assert retention.filter(records) == ()
    assert retention.counts["attempt_failed"] == 1000
    assert sum(retention.errors.values()) == 1000 and len(retention.errors) <= 65


def test_summary_only_retains_live_latency_sufficient_statistics(tmp_path):
    from dataclasses import replace
    from perf_benchmark.pipeline.contracts.results import ScalarMetricRecord
    from perf_benchmark.plugins.saver.sqlite.summary_retention import SummaryRetention

    def metric(value, unit):
        return ScalarMetricRecord(unit, "available", None, None, {"value": value})

    turn = Turn(id="preview", session_id="session", request_payload_ref=PayloadRef(0, 1))
    turn.start()
    turn.succeed()
    record = replace(
        _snapshot(TurnResultRecord, turn),
        usage={"completion_tokens": 11},
        metrics={
            "ttft": metric(0.2, "s"),
            "tpot": metric(10.0, "ms"),
            "e2e": metric(0.3, "s"),
            "decode_throughput": metric(100.0, "tok/s"),
        },
    )
    sink = SummaryRetention(tmp_path / "summary.db")
    sink.filter((record, record))
    sink.write({"status": "running"})
    saved = json.loads(sink.path.read_text())
    assert saved["latency_preview"]["ttft"] == {"sum": 0.4, "samples": 2, "unit": "s"}
    assert saved["latency_preview"]["tpot"]["sum"] == 20.0
    assert saved["latency_preview"]["e2e"]["samples"] == 2
    assert saved["decode_preview"] == {"tokens": 20, "seconds": 0.2}
