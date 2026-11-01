"""Complete normalized SQLite persistence and optional streaming scale replay.

Includes encoding, dictionary resolution, indexes, triggers and commits. The
streaming replay also measures producer/Metrics work and exact Trial finalization.
This diagnostic never claims HTTP capacity or Run qualification.
"""
import argparse
import json
import sqlite3
import resource
import sys
from contextlib import closing
from dataclasses import replace
import gc
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter, process_time
from threading import Event, Thread
from types import SimpleNamespace

from benchmarks.result_pipeline import events, read_counts
from perf_benchmark.core.models import Run, Session, Trial
from perf_benchmark.pipeline.context import SaverContext
from perf_benchmark.pipeline.contracts.results import RunRecord
from perf_benchmark.pipeline.results.metrics import _MetricProcessor
from perf_benchmark.pipeline.results.saver import SaverRuntime, _snapshot, _trial_records
from perf_benchmark.plugins.metrics.standard import StandardMetrics
from perf_benchmark.plugins.saver.sqlite import SQLiteSaver, SQLiteResultReader
from perf_benchmark.runtime.cancellation import CancellationToken


def prepared_batches(count, identity_order="random", batch_size=100, diagnostics=None):
    output = []
    runtime = SaverRuntime.__new__(SaverRuntime)
    runtime.run = Run(id="run", name="storage-capacity")
    runtime.writer = SimpleNamespace(submit_many_wait=output.extend)
    processor = _MetricProcessor(StandardMetrics({}).create_backend(seed=1))
    processor.open()
    directory = Path(processor.backend.temporary.name)
    stop_sampling = Event()
    def sample():
        while not stop_sampling.wait(0.25):
            size = 0
            for path in directory.rglob('*'):
                try:
                    if path.is_file():
                        size += path.stat().st_size
                except FileNotFoundError:
                    pass  # Spill files can be replaced between directory/stat reads.
            diagnostics['metrics_temporary_peak_bytes'] = max(diagnostics.get('metrics_temporary_peak_bytes', 0), size)
    sampler = Thread(target=sample, name='storage-scale-sampler') if diagnostics is not None else None
    if sampler is not None:
        sampler.start()
    try:
        batch = []
        for event in events(count, identity_order):
            if isinstance(event, Session):
                from perf_benchmark.pipeline.contracts.results import SessionResultRecord
                output.append(_snapshot(SessionResultRecord, event, turn_count=1))
            else:
                batch.append(event)
            if len(batch) == 100:
                computed = processor.write_batch(tuple(batch))
                if computed is not None:
                    runtime.write_results(computed)
                batch.clear()
            while len(output) >= batch_size:
                yield tuple(output[:batch_size])
                del output[:batch_size]
        if batch:
            computed = processor.write_batch(tuple(batch))
            if computed is not None:
                runtime.write_results(computed)
        processor.backend._flush_pending(force=True)
        if diagnostics is not None:
            from perf_benchmark.pipeline.contracts.facts import (
                TrialFact, LoadProfile, ResourceLimits, RepetitionPlan, TrialSchedulingFact,
            )
            from perf_benchmark.pipeline.contracts.results import MetricWindowResultRecord, MeasurementRecord, TrialFactRecord
            started = perf_counter()
            fact = TrialFact(
                'trial', 'succeeded', LoadProfile('closed_loop', target_concurrency=1000),
                ResourceLimits(1000, 1000, 1000), None, RepetitionPlan(),
                TrialSchedulingFact(offered_sessions=count, admitted_sessions=count, completed_sessions=count,
                    offered_attempts=count, admitted_attempts=count, completed_attempts=count,
                    offering_started_ns=1_000_000_000, request_offering_finished_ns=1_000_000_000 + count * 200_000,
                    session_offering_finished_ns=1_000_000_000 + count * 200_000, demand_exhausted=True),
                execution_status='succeeded')
            report = processor.backend.finalize_trial(fact)
            diagnostics['metrics_finalization_seconds'] = perf_counter() - started
            output.append(TrialFactRecord(replace(fact, validity=report.validity)))
            output.extend(report.points)
            output.extend(report.groups)
            output.extend(_snapshot(MetricWindowResultRecord, window, run_id='run', scope='trial', scope_id='trial')
                          for window in report.windows)
            output.append(MeasurementRecord('run', 'trial', 'trial', 'diagnostics', report.diagnostics))
            output.extend(processor.backend.finalize_run('run'))
            processor.backend.release_trial('trial')
        while output:
            yield tuple(output[:batch_size])
            del output[:batch_size]
    finally:
        try:
            processor.close()
        finally:
            stop_sampling.set()
            if sampler is not None:
                sampler.join()
        if diagnostics is not None:
            diagnostics['metrics_temporary_sample_interval_seconds'] = 0.25
            diagnostics['metrics_temporary_removed'] = not directory.exists()


def prepare(count, identity_order="random"):
    return [record for batch in prepared_batches(count, identity_order) for record in batch]


def measure(count, batch_size, cache_kib=0, checkpoint_pages=0, page_size=8192, identity_order="prepared", streaming=False):
    """Complete normalized Saver cost, including definition lookup and encoding.

    The former pre-encoded SQL probe is invalid after normalization: dictionary
    identities belong to the transaction, so they cannot be encoded beforehand.
    """
    from benchmarks.result_diagnostics import source_identity
    provenance = source_identity()
    diagnostics = {}
    records = None if streaming else prepare(count, identity_order)
    batches = prepared_batches(count, identity_order, batch_size, diagnostics) if streaming else (
        records[offset:offset + batch_size] for offset in range(0, len(records), batch_size))
    run, trial = Run(id="run", name="storage-capacity"), Trial(id="trial", name="fixture")
    run.register_trial(trial)
    with TemporaryDirectory(prefix="result-storage-") as directory:
        path = Path(directory) / "results.db"
        with closing(sqlite3.connect(path)) as db:
            db.execute(f"PRAGMA page_size={int(page_size)}")
            db.execute("VACUUM")
        backend = SQLiteSaver({"path": str(path)}).create_backend(
            _snapshot(RunRecord, run), SaverContext(Path(directory), CancellationToken()))
        backend.open()
        if cache_kib:
            backend.connection.execute(f"PRAGMA cache_size=-{int(cache_kib)}")
        if checkpoint_pages:
            backend.connection.execute(f"PRAGMA wal_autocheckpoint={int(checkpoint_pages)}")
        try:
            backend.write_batch(tuple(_trial_records(trial, None)))
            started = perf_counter()
            seconds = cpu = 0.0
            record_count = 0
            peak_database_bytes = 0
            for batch in batches:
                writing, writing_cpu = perf_counter(), process_time()
                backend.write_batch(batch)
                seconds += perf_counter() - writing
                cpu += process_time() - writing_cpu
                record_count += len(batch)
                if record_count % 10000 < len(batch):
                    peak_database_bytes = max(peak_database_bytes, sum(p.stat().st_size for p in Path(directory).glob('results.db*')))
            if streaming:
                finalization = perf_counter()
                from perf_benchmark.pipeline.contracts.results import TrialResultRecord
                trial_record = next(r for r in _trial_records(trial, None) if isinstance(r, TrialResultRecord))
                backend.write_batch((replace(trial_record, status='succeeded', session_count=count,
                    turn_count=count, succeeded_turns=count),))
                backend.finalize(replace(_snapshot(RunRecord, run), status='succeeded'))
                diagnostics['saver_finalization_seconds'] = perf_counter() - finalization
            complete_seconds = perf_counter() - started
            committed = read_counts(path)
            assert committed == dict(turns=count, attempts=count, sessions=count)
            checkpoint = perf_counter()
            status = backend.connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            checkpoint = perf_counter() - checkpoint
            assert status[0] == 0, status
            size = path.stat().st_size
            from benchmarks.result_diagnostics import database_layout
            layout = database_layout(path)
            reader = SQLiteResultReader(path)
            if streaming:
                mismatches = backend.connection.execute("""
                    SELECT count(*) FROM turns t JOIN turn_keys k ON k.id=t.id
                    JOIN sessions s ON s.id=k.session_key JOIN request_facts f ON f.id=t.fact_id
                    JOIN attempts a ON a.id=t.final_attempt_id
                    WHERE f.started_ns != 1000000000+s.ordinal*200000 OR t.fact_id != a.fact_id
                """).fetchone()[0]
                assert mismatches == 0
                diagnostics['request_fact_mismatches'] = mismatches
                from perf_benchmark.plugins.exporter.html.render import render_report
                report_path = Path(directory) / 'report.html'
                exporting = perf_counter()
                render_report('run', reader, report_path)
                diagnostics['export_seconds'] = perf_counter() - exporting
                diagnostics['html_bytes'] = report_path.stat().st_size
            page_seconds = []
            for index in range(40):
                query = perf_counter()
                page = reader.request_page('run', scope='turn' if index % 2 else 'attempt',
                                           offset=(index * max(1, count // 40)) % count, limit=50)
                assert page['total'] == count
                page_seconds.append(perf_counter() - query)
            diagnostics['page_seconds'] = page_seconds
            diagnostics['page_p99_seconds'] = max(page_seconds)
            if streaming:
                update_started = perf_counter()
                cursor, update_times, identities = None, [], {}
                while True:
                    reading = perf_counter()
                    update = reader.read_updates(cursor, 1000)
                    update_times.append(perf_counter() - reading)
                    for item in update.updates:
                        kind = type(item.record).__name__
                        identities[kind] = identities.get(kind, 0) + 1
                    cursor = update.cursor
                    if update.caught_up:
                        break
                assert all(identities.get(kind) == count for kind in (
                    'SessionResultRecord', 'TurnResultRecord', 'AttemptResultRecord'))
                diagnostics['updates_seconds'] = perf_counter() - update_started
                diagnostics['updates_pages'] = len(update_times)
                diagnostics['updates_record_counts'] = identities
                diagnostics['updates_commit_revision'] = update.commit_revision
                diagnostics['updates_page_p99_seconds'] = sorted(update_times)[max(0, int(len(update_times)*.99)-1)]
                diagnostics['updates_page_max_seconds'] = max(update_times)

            diagnostics['peak_process_rss_bytes'] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (1 if sys.platform == 'darwin' else 1024)
        finally:
            backend.close()
    return dict(provenance=provenance, page_size=page_size, database_layout=layout, identity_order=identity_order,
                requests=count, records=record_count, batch_size=batch_size,
                streaming=streaming, complete_seconds=complete_seconds,
                complete_requests_s=count/complete_seconds, database_peak_bytes=peak_database_bytes,
                diagnostics=diagnostics,
                seconds=seconds, cpu_seconds=cpu, requests_s=count / seconds,
                checkpoint_seconds=checkpoint, database_bytes=size,
                bytes_per_turn=size/count, persisted=committed, http_capacity_claim=False,
                timing_scope="complete_seconds includes source generation, Metrics and finalization" if streaming else "complete normalized Saver; includes encoding and SQL")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--streaming", action="store_true")
    parser.add_argument("--requests", type=int, default=30000)
    parser.add_argument("--batch-size", type=int, choices=(100, 256, 512, 1024), default=100)
    parser.add_argument("--cache-kib", type=int, default=0)
    parser.add_argument("--checkpoint-pages", type=int, default=0)
    parser.add_argument("--page-size", type=int, choices=(4096, 8192, 16384), default=8192)
    parser.add_argument("--identity-order", choices=("random", "prepared"), default="prepared")
    args = parser.parse_args()
    if args.requests < 1 or args.cache_kib < 0 or args.checkpoint_pages < 0:
        parser.error("requests must be positive")
    gc.disable()
    print(json.dumps(measure(args.requests, args.batch_size, args.cache_kib,
                             args.checkpoint_pages, args.page_size, args.identity_order, args.streaming), sort_keys=True))
