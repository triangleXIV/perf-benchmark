"""Immutable, validated Parquet sealing and local seven-day retention.

Concrete SQLite Saver storage policy. No metric calculation or optional SDKs.
"""
from contextlib import closing, contextmanager
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from itertools import zip_longest
from pathlib import Path
import os
import logging
import shutil
import sqlite3
import struct
from tempfile import mkdtemp
from uuid import uuid4

from filelock import FileLock, Timeout
import orjson
from platformdirs import user_cache_path
import pyarrow as pa
import pyarrow.parquet as pq

from perf_benchmark.core.errors import ResultArchiveError

from .schema import ARTIFACT_SCHEMA_VERSION, MEASUREMENT_CONTRACT, RESULT_TABLES, validate_result_database
from .layout import ENTITY_TABLES

ARCHIVE_VERSION = 1
SUMMARY_TABLES = tuple(table for table in RESULT_TABLES if table not in ENTITY_TABLES) + (
    'artifact_meta', 'result_identity', 'result_state', 'metric_definitions',
)
DETAIL_TABLES = ENTITY_TABLES + (
    'turn_keys', 'request_facts', 'request_profiles', 'attempt_plans', 'error_definitions', 'metric_definitions',
)
TYPES = {'INTEGER': pa.int64(), 'REAL': pa.float64(), 'TEXT': pa.string(), 'BLOB': pa.binary()}


def archive_path(source):
    source = Path(source).resolve()
    return source.with_name(source.name + '.archive')


def utc_now():
    return datetime.now(timezone.utc)


def expires_at(finished):
    if finished is None:
        return None
    parsed = datetime.fromisoformat(finished.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('Run finish time requires a UTC offset')
    return (parsed.astimezone(timezone.utc) + timedelta(days=7)).isoformat()


def _coordination(source):
    # Shared by readers, writers and cleanup; formal result volumes may be read-only.
    key = sha256(str(Path(source).resolve()).encode()).hexdigest()
    root = _sealing_root() / 'readers' / key
    root.mkdir(parents=True, exist_ok=True)
    return root


def _lock(source):
    return FileLock(_coordination(source) / 'access.lock', timeout=30)


def _digest_rows(rows, digest):
    for row in rows:
        for value in row:
            if value is None:
                payload = b'n'
            elif isinstance(value, int):
                payload = b'i' + struct.pack('>q', value)
            elif isinstance(value, float):
                payload = b'f' + struct.pack('>d', value)
            elif isinstance(value, bytes):
                payload = b'b' + value
            else:
                payload = b's' + value.encode('utf-8')
            digest.update(struct.pack('>Q', len(payload)))
            digest.update(payload)


def _file_hash(path):
    digest = sha256()
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _sync(path):
    with path.open('rb') as stream:
        os.fsync(stream.fileno())


def _sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _selection(table, tier):
    where = ''
    if tier == 'summary' and table == 'metric_definitions':
        where = ' WHERE id IN (SELECT definition_id FROM metric_points UNION SELECT definition_id FROM grouped_metrics)'
    if tier == 'summary' and table == 'measurements':
        where = " WHERE scope IN ('run','trial')"
    return f'SELECT * FROM "{table}"{where} ORDER BY rowid'


def _encode(db, table, tier, destination):
    schema = pa.schema([(row[1], TYPES[row[2].upper()]) for row in db.execute(f'PRAGMA table_info("{table}")')])
    digest, count = sha256(), 0
    cursor = db.execute(_selection(table, tier))
    with pq.ParquetWriter(destination, schema, compression='zstd', compression_level=3) as writer:
        while rows := cursor.fetchmany(65536):
            _digest_rows(rows, digest)
            count += len(rows)
            arrays = [pa.array([row[i] for row in rows], type=field.type) for i, field in enumerate(schema)]
            writer.write_batch(pa.RecordBatch.from_arrays(arrays, schema=schema))
    # Stream the entire encoded artifact back, including nulls and int64 timestamps.
    actual, observed = sha256(), 0
    encoded = pq.ParquetFile(destination)
    if encoded.schema_arrow != schema:
        raise ValueError(f'archive schema mismatch: {table}')
    for batch in encoded.iter_batches(batch_size=65536):
        columns = [column.to_pylist() for column in batch.columns]
        _digest_rows(zip(*columns), actual)
        observed += batch.num_rows
    if count != observed or digest.digest() != actual.digest():
        raise ValueError(f'archive facts mismatch: {table}')
    _sync(destination)
    return {'path': f'{tier}/{table}.parquet', 'rows': count, 'sha256': _file_hash(destination),
            'schema': str(schema), 'facts_sha256': digest.hexdigest()}


def read_info(directory, *, verify=False, verify_details=False):
    directory = Path(directory).resolve()
    rows = pq.ParquetFile(directory / 'summary' / 'artifact_info.parquet').read().to_pylist()
    if len(rows) != 1:
        raise ValueError('archive requires exactly one artifact identity')
    info = rows[0]
    if (info['archive_version'] != ARCHIVE_VERSION or info['sqlite_schema'] != ARTIFACT_SCHEMA_VERSION
            or info['measurement_contract'] != MEASUREMENT_CONTRACT):
        raise ValueError('incompatible result archive')
    if Path(info['source_name']).name != info['source_name']:
        raise ValueError('invalid archive source name')
    if info['detail_expires_at'] != expires_at(info['finished_at']):
        raise ValueError('archive expiry differs from Run finish time')
    files = orjson.loads(info['files_json'])
    expected = {f'summary/{name}.parquet' for name in SUMMARY_TABLES}
    expected |= {f'details/{name}.parquet' for name in DETAIL_TABLES}
    expected.add('summary/run_totals.parquet')
    if {entry['path'] for entry in files} != expected or len(files) != len(expected):
        raise ValueError('invalid archive file inventory')
    run = pq.ParquetFile(directory / 'summary' / 'runs.parquet').read().to_pylist()
    identity = pq.ParquetFile(directory / 'summary' / 'result_identity.parquet').read().to_pylist()
    revision = pq.ParquetFile(directory / 'summary' / 'result_state.parquet').read().to_pylist()
    if (len(run) != 1 or run[0]['run_id'] != info['run_id'] or run[0]['finished_at'] != info['finished_at']
            or identity[0]['database_id'] != info['database_id'] or revision[0]['revision'] != info['revision']):
        raise ValueError('archive source identity mismatch')
    totals = pq.ParquetFile(directory / 'summary' / 'run_totals.parquet').read().to_pylist()
    if len(totals) != 1 or totals[0]['run_id'] != info['run_id']:
        raise ValueError('archive final counts identity mismatch')
    if verify:
        for entry in files:
            if not entry['path'].startswith('summary/') and not verify_details:
                continue
            path = directory / entry['path']
            if (_file_hash(path) != entry['sha256'] or pq.ParquetFile(path).metadata.num_rows != entry['rows']
                    or str(pq.read_schema(path)) != entry['schema']):
                raise ValueError('corrupt result archive: ' + entry['path'])
    return info


def _seal(source):
    """Seal a CLOSED writer's exact committed snapshot; failures retain SQLite."""
    source = Path(source).resolve()
    directory = archive_path(source)
    with FileLock(_coordination(source) / 'seal.lock', timeout=-1), closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as db:
        validate_result_database(source)
        db.execute('BEGIN')
        runs = db.execute('SELECT run_id,finished_at,status FROM runs').fetchall()
        if len(runs) != 1 or runs[0][2] not in {'succeeded', 'failed', 'cancelled'}:
            raise ValueError('archive requires one terminal Run')
        run_id, finished, _ = runs[0]
        stages = db.execute("SELECT value_json FROM measurements WHERE scope='run' AND name='stages'").fetchone()
        if stages is not None and any(stage['status'] == 'started' for stage in orjson.loads(stages[0]).values()):
            raise ValueError('archive_incomplete_finalization')
        database_id = db.execute('SELECT database_id FROM result_identity').fetchone()[0]
        revision = db.execute('SELECT revision FROM result_state').fetchone()[0]
        if directory.exists():
            info = read_info(directory, verify=True)
            expiry = info['detail_expires_at']
            if expiry is None or utc_now() < datetime.fromisoformat(expiry):
                read_info(directory, verify=True, verify_details=True)
            if (info['run_id'], info['database_id'], info['revision']) != (run_id, database_id, revision):
                raise ValueError('archive already exists for a different source revision')
            return directory
        temporary = Path(mkdtemp(prefix='.' + directory.name + '-', dir=source.parent))
        try:
            files = []
            for tier, tables in (('summary', SUMMARY_TABLES), ('details', DETAIL_TABLES)):
                (temporary / tier).mkdir()
                files.extend(_encode(db, table, tier, temporary / tier / (table + '.parquet')) for table in tables)
            counts = {table: db.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
                      for table in ('trials', 'sessions', 'turns', 'metric_points')}
            retention = db.execute("SELECT value_json FROM measurements WHERE scope='run' "
                                   "AND name='result_retention'").fetchone()
            if retention and orjson.loads(retention[0]).get('summary_only'):
                counts['sessions'], counts['turns'] = db.execute(
                    'SELECT coalesce(sum(session_count),0),coalesce(sum(turn_count),0) FROM trials').fetchone()
            totals = pa.Table.from_pylist([{'run_id': run_id, **counts}], schema=pa.schema([
                ('run_id', pa.string()), *((name, pa.int64()) for name in counts)]))
            totals_path = temporary / 'summary' / 'run_totals.parquet'
            pq.write_table(totals, totals_path, compression='zstd')
            _sync(totals_path)
            files.append({'path': 'summary/run_totals.parquet', 'rows': 1, 'sha256': _file_hash(totals_path),
                          'schema': str(totals.schema)})
            observation = db.execute("SELECT value_json FROM measurements WHERE scope='run' AND name='observation_mode'").fetchone()
            info = {'archive_version': ARCHIVE_VERSION, 'sqlite_schema': ARTIFACT_SCHEMA_VERSION,
                    'measurement_contract': MEASUREMENT_CONTRACT, 'run_id': run_id,
                    'database_id': database_id, 'revision': revision, 'source_name': source.name,
                    'finished_at': finished, 'detail_expires_at': expires_at(finished),
                    'sealed_at': utc_now().isoformat(), 'observation_enabled': orjson.loads(observation[0])['enabled'] if observation else None,
                    'files_json': orjson.dumps(files).decode()}
            # Explicit nullable strings avoid an all-null Arrow type for unknown expiry.
            schema = pa.schema([(key, pa.int64() if type(value) is int else pa.bool_() if key == 'observation_enabled'
                                 else pa.string()) for key, value in info.items()])
            pq.write_table(pa.Table.from_pylist([info], schema=schema), temporary / 'summary' / 'artifact_info.parquet')
            _sync(temporary / 'summary' / 'artifact_info.parquet')
            read_info(temporary, verify=True)
            from .parquet_reader import SummaryReader
            from . import SQLiteResultReader
            original, permanent = SQLiteResultReader(source), SummaryReader(temporary)
            if original.summary(run_id) != permanent.summary(run_id):
                raise ValueError('summary counts mismatch')
            for expected, actual in zip_longest(original.iter_summary_records(run_id), permanent.iter_summary_records(run_id)):
                if expected != actual:
                    raise ValueError('summary-only formal records mismatch')
            # All source tables were checked above, and typed summary decoding is checked independently.
            _sync_directory(temporary / 'summary')
            _sync_directory(temporary / 'details')
            _sync_directory(temporary)
            temporary.rename(directory)
            _sync_directory(source.parent)
        finally:
            if temporary.exists():
                shutil.rmtree(temporary)
        return directory


@contextmanager
def detail_lease(source, *, now=None):
    """Register independent reader locks; cleanup never removes an active reader."""
    source = Path(source).resolve()
    directory = archive_path(source)
    lease_directory = _coordination(source) / 'leases'
    lease = None
    with _lock(source):
        if directory.exists():
            expiry = read_info(directory)['detail_expires_at']
        elif source.exists():
            with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as db:
                row = db.execute('SELECT finished_at FROM runs').fetchone()
                expiry = expires_at(row[0]) if row else None
        else:
            raise ValueError('detail_unavailable')
        if expiry is not None and (now or utc_now()) >= datetime.fromisoformat(expiry):
            raise ValueError('detail_expired')
        lease_directory.mkdir(exist_ok=True)
        lease = FileLock(str(lease_directory / (uuid4().hex + '.lock')))
        lease.acquire()
    try:
        yield
    finally:
        with _lock(source):
            lease.release()
            Path(lease.lock_file).unlink(missing_ok=True)


def cleanup(directory, *, now=None):
    """Delete both detail formats only after a valid permanent summary and leases."""
    directory = Path(directory).resolve()
    info = read_info(directory)
    source = directory.parent / info['source_name']
    if archive_path(source) != directory:
        raise ValueError('archive source location mismatch')
    with FileLock(_coordination(source) / 'seal.lock', timeout=-1), _lock(source):
        info = read_info(directory, verify=True)
        expiry = info['detail_expires_at']
        if expiry is None:
            return {'run_id': info['run_id'], 'state': 'expiry_unknown'}
        if (now or utc_now()) < datetime.fromisoformat(expiry):
            return {'run_id': info['run_id'], 'state': 'retained'}
        leases = _coordination(source) / 'leases'
        for path in sorted(leases.glob('*.lock')):
            try:
                with FileLock(path, timeout=0):
                    pass
            except Timeout:
                return {'run_id': info['run_id'], 'state': 'reader_active'}
            path.unlink(missing_ok=True)
        from .parquet_reader import SummaryReader
        reader = SummaryReader(directory)
        if reader.run_ids() != (info['run_id'],):
            raise ValueError('invalid permanent Run identity')
        for _ in reader.iter_summary_records(info['run_id']):
            pass
        if source.exists():
            with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as db:
                actual = db.execute('SELECT database_id FROM result_identity').fetchone()[0]
                revision = db.execute('SELECT revision FROM result_state').fetchone()[0]
                if (actual, revision) != (info['database_id'], info['revision']):
                    return {'run_id': info['run_id'], 'state': 'source_changed'}
            for suffix in ('', '-wal', '-shm'):
                Path(str(source) + suffix).unlink(missing_ok=True)
        if (directory / 'details').exists():
            shutil.rmtree(directory / 'details')
        _sync_directory(directory)
        return {'run_id': info['run_id'], 'state': 'detail_expired'}



def _sealing_root():
    root = user_cache_path('perf-benchmark') / 'result-sealing'
    root.mkdir(parents=True, exist_ok=True)
    return root


def _active_writers(root):
    active = False
    for path in root.glob('*.writer'):
        try:
            with FileLock(path, timeout=0):
                pass
        except Timeout:
            active = True
        else:
            path.unlink(missing_ok=True)
    return active


def register_writer(source):
    """Hold a process-safe lifetime lease before requests can be scheduled."""
    root = _sealing_root()
    with FileLock(root / 'access.lock', timeout=-1):
        if archive_path(source).exists():
            raise ValueError('cannot write a frozen result archive')
        key = sha256(str(Path(source).resolve()).encode()).hexdigest()
        lease = FileLock(root / (key + '.writer'))
        try:
            lease.acquire(timeout=0)
        except Timeout as exc:
            raise ValueError('result_writer_already_active') from exc
        return lease


def finish_writer(lease, source, *, finalized):
    """Serial sealing after the last local writer stops; persist deferred work."""
    root = _sealing_root()
    with FileLock(root / 'access.lock', timeout=-1):
        lease.release()
        Path(lease.lock_file).unlink(missing_ok=True)
        source = Path(source).resolve()
        if finalized:
            key = sha256(str(source).encode()).hexdigest()
            pending = root / (key + '.pending')
            temporary = root / ('.' + key + '.' + uuid4().hex)
            try:
                temporary.write_text(str(source), encoding='utf-8')
                _sync(temporary)
                temporary.replace(pending)
                _sync_directory(root)
            finally:
                temporary.unlink(missing_ok=True)
        if _active_writers(root):
            return
        failure = None
        for pending in sorted(root.glob('*.pending')):
            path = Path(pending.read_text(encoding='utf-8'))
            if not path.exists():
                pending.unlink()
                continue
            try:
                _seal(path)
            except Exception as exc:
                if path == source:
                    failure = exc
                else:
                    logging.getLogger(__name__).exception('deferred result archive failed')
            else:
                pending.unlink()
        if failure is not None:
            raise ResultArchiveError(f"result archive delivery failed: {failure}") from failure


def seal(source):
    root = _sealing_root()
    with FileLock(root / 'access.lock', timeout=-1):
        if _active_writers(root):
            raise ValueError('archive_pending_active_run')
        directory = _seal(source)
        key = sha256(str(Path(source).resolve()).encode()).hexdigest()
        (root / (key + '.pending')).unlink(missing_ok=True)
        return directory
