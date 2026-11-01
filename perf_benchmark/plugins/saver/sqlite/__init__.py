from __future__ import annotations

from perf_benchmark.pipeline.contracts.facts import ValidityRequirements, GraphCoverage

from collections import defaultdict
from collections.abc import Collection, Mapping, Sequence
from contextlib import closing
from itertools import chain
from pathlib import Path
import sqlite3
from typing import Any, cast

from types import MappingProxyType

import orjson

from perf_benchmark.plugins.saver.sqlite.encoding import _json_text, _json_dump, _field_names, _validate_value
from perf_benchmark.plugins.saver.sqlite.entities import EntityStore, decode_entities, entity_select
from perf_benchmark.plugins.saver.sqlite.layout import ENTITY_TABLES
from perf_benchmark.plugins.saver.sqlite import summaries
from perf_benchmark.core.models import freeze_snapshot
from perf_benchmark.pipeline.contracts.sweep import SweepRecord, SloCheck, SloRule, TrialValidity
from perf_benchmark.pipeline.contracts.facts import (
    ComparisonPoint, LoadProfile, RepetitionPlan, ResourceLimits, TrialFact, TrialSchedulingFact,
)
from perf_benchmark.pipeline.contracts.results import (
    ComparisonRecord,
    MetricPoint,
    GroupedMetricRecord,
    MetricRecord,
    MetricWindowRecord,
    RunSummary,
    StatisticMetadata,
    TrialRecord,
    ResultUpdateCursor,
    ResultUpdate,
    ResultCommit,
    ResultUpdatePage,
    AttemptResultRecord,
    MeasurementRecord,
    MetricWindowResultRecord,
    RunRecord,
    ResultRecord,
    SessionResultRecord,
    TrialFactRecord,
    TrialResultRecord,
    TurnResultRecord,
    ScalarMetricRecord,
)
from perf_benchmark.pipeline.contracts.saver import SaverArtifact, SaverBackend
from perf_benchmark.pipeline.context import SaverContext
from perf_benchmark.plugins import register_plugin
from perf_benchmark.plugins.saver.sqlite.schema import (
    RESULT_TABLES,
    ResultDatabase,
    validate_result_database,
)


_RESULT_RECORDS = {
    "runs": RunRecord,
    "trials": TrialResultRecord,
    "sessions": SessionResultRecord,
    "turns": TurnResultRecord,
    "attempts": AttemptResultRecord,
    "measurements": MeasurementRecord,
    "metric_windows": MetricWindowResultRecord,
    "metric_points": MetricPoint,
    "grouped_metrics": GroupedMetricRecord,
}


def _result_record(table: str, row: sqlite3.Row) -> tuple[ResultRecord, str, str | None]:
    values = _decoded_row(row)
    run_id, trial_id = values["run_id"], values.get("trial_id")
    if values.get("scope") == "trial":
        trial_id = values["scope_id"]
    if table == "sweeps":
        record = _sweep_record(values["payload"])
    elif table == "trial_facts":
        assert trial_id is not None
        comparison = None if values["comparison_group_id"] is None else ComparisonPoint(
            values["comparison_group_id"], values["comparison_axis_name"],
            values["comparison_axis_value"], values["comparison_axis_unit"],
        )
        scheduling: dict[str, Any] = dict(values["scheduling"])
        scheduling["graph_coverage"] = tuple(GraphCoverage(**g) for g in scheduling["graph_coverage"])
        record = TrialFactRecord(TrialFact(
            trial_id, values["status"], LoadProfile(**values["load_profile"]),
            ResourceLimits(**values["resource_limits"]), comparison,
            RepetitionPlan(values["repetition_index"], values["expected_repetitions"]),
            TrialSchedulingFact(**scheduling),
            ValidityRequirements(**values["validity_requirements"]) if values["validity_requirements"] is not None else None,
            _validity(values["validity"]), values["execution_status"],
        ))
    else:
        record_type = _RESULT_RECORDS[table]
        if table in {"runs", "trials", "sessions", "turns"}:
            values["id"] = values[table[:-1] + "_id"]
        if values.get("scenario_passed") is not None:
            values["scenario_passed"] = bool(values["scenario_passed"])
        frozen = dict(freeze_snapshot({name: values[name] for name in _field_names(record_type)}))
        if table in {"sessions", "turns", "attempts"}:
            frozen["metrics"] = MappingProxyType({name: ScalarMetricRecord(**value)
                                                 for name, value in values["metrics"].items()})
        record = record_type(**frozen)
    return record, run_id, trial_id


class SQLiteResultReader:
    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        validate_result_database(self.path)
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            self.keys = {table: tuple(r[1] for r in sorted(
                (r for r in db.execute(f'PRAGMA table_info({table})') if r[5]), key=lambda r: r[5]))
                for table in RESULT_TABLES}
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            self.entity_columns = {table: tuple(r[1] for r in db.execute(f"PRAGMA table_info({table})"))
                                   for table in ENTITY_TABLES}
        self.keys.update(sessions=('session_id',), turns=('turn_id',), attempts=('turn_id', 'attempt'))

    @property
    def location(self) -> str:
        return str(self.path)

    def _rows(self, query, params=()):
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as connection:
            connection.row_factory = sqlite3.Row
            return connection.execute(summaries.query_prefix(query) + query, params).fetchall()

    def read_updates(self, cursor=None, limit=500):
        from .archive import detail_lease
        if type(limit) is not int or not 1 <= limit <= 5000:
            raise ValueError("result page limit must be between 1 and 5000")
        with detail_lease(self.path), closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            identity = db.execute("SELECT database_id FROM result_identity").fetchone()[0]
            high = db.execute("SELECT revision FROM result_state WHERE id=1").fetchone()[0]
            cursor = cursor or ResultUpdateCursor(identity)
            if (cursor.database_id != identity or type(cursor.revision) is not int or not 0 <= cursor.revision <= high
                    or type(cursor.entity_kind) is not int or not -1 <= cursor.entity_kind <= len(RESULT_TABLES)
                    or type(cursor.internal_record_id) is not int or cursor.internal_record_id < 0):
                raise ValueError("result cursor belongs to another database or is ahead of its commits")
            selections, params = [], []
            for kind, table in enumerate(RESULT_TABLES):
                selections.append(f"SELECT revision,{kind} AS kind,rowid AS record_id FROM {table} "
                    "WHERE revision>=? AND revision<=? AND (revision,?,rowid)>(?,?,?)")
                params.extend((cursor.revision, high, kind, cursor.revision, cursor.entity_kind, cursor.internal_record_id))
            updates = db.execute("SELECT * FROM (" + " UNION ALL ".join(selections) + ") "
                "ORDER BY revision,kind,record_id LIMIT ?", (*params, limit + 1)).fetchall()
            caught_up = len(updates) <= limit
            updates = updates[:limit]
            grouped = defaultdict(set)
            for update in updates:
                grouped[RESULT_TABLES[update['kind']]].add(update['record_id'])
            rows = {}
            for table, identities in grouped.items():
                select, source = entity_select(table) if table in ENTITY_TABLES else summaries.select(table)
                if table == 'trial_facts':
                    source += ' JOIN trials t USING(trial_id)'
                    select += ',t.run_id,t.status'
                for offset in range(0, len(identities), 400):
                    batch = tuple(identities)[offset:offset + 400]
                    found = db.execute(f"SELECT r.rowid AS _record_id,{select} {source} "
                        f"WHERE r.rowid IN ({','.join('?' for _ in batch)})", batch).fetchall()
                    decoded = decode_entities(db, table, found) if table in ENTITY_TABLES else [dict(r) for r in found]
                    rows.update(((table, row['_record_id']), row) for row in decoded)
            if caught_up:
                next_cursor = ResultUpdateCursor(identity, high, len(RESULT_TABLES), 0)
            elif updates:
                last = updates[-1]
                next_cursor = ResultUpdateCursor(identity, last['revision'], last['kind'], last['record_id'])
            else:
                next_cursor = cursor
            contract = db.execute("SELECT measurement_contract FROM artifact_meta").fetchone()[0]
        values = {key: _result_record(key[0], row) for key, row in rows.items()}
        output = []
        for update in updates:
            table = RESULT_TABLES[update['kind']]
            key = table, update['record_id']
            record, run_id, trial_id = values[key]
            output.append(ResultUpdate(update['revision'], update['kind'], update['record_id'],
                tuple(rows[key][name] for name in self.keys[table]), record, run_id, trial_id))
        return ResultUpdatePage(tuple(output), next_cursor, high, caught_up, contract)

    def summary(self, run_id: str) -> RunSummary:
        rows = self._rows(
            """
            SELECT run_id, name, status, error,
              (SELECT count(*) FROM trials WHERE run_id=r.run_id) AS trial_count,
              (SELECT count(*) FROM sessions s JOIN trials t USING(trial_id)
               WHERE t.run_id=r.run_id) AS session_count,
              (SELECT count(*) FROM turns u JOIN turn_keys k ON k.id=u.id JOIN sessions s ON s.id=k.session_key
               JOIN trials t USING(trial_id) WHERE t.run_id=r.run_id) AS turn_count,
              (SELECT count(*) FROM metric_points WHERE run_id=r.run_id) AS metric_count
            FROM runs r WHERE run_id=?
        """,
            [run_id],
        )
        if not rows:
            raise ValueError(f"run not found: {run_id}")
        result = RunSummary(**dict(rows[0]))
        retention = self.measurements(run_id, scope='run', scope_id=run_id).get('result_retention', {})
        if retention.get('summary_only'):
            counts = self._rows('SELECT coalesce(sum(session_count),0),coalesce(sum(turn_count),0) '
                                'FROM trials WHERE run_id=?', (run_id,))[0]
            result['session_count'], result['turn_count'] = counts
        return result

    def run_record(self, run_id: str) -> RunRecord:
        rows = self._rows("SELECT * FROM runs WHERE run_id=?", (run_id,))
        if not rows:
            raise ValueError(f"run not found: {run_id}")
        row = dict(rows[0])
        row["id"] = row.pop("run_id")
        row["counts"] = _json_object(row.pop("counts_json"))
        row.pop("revision", None)
        return RunRecord(**row)

    def measurements(self, run_id, *, scope, scope_id):
        return {row["name"]: orjson.loads(row["value_json"]) for row in self._rows(
            "SELECT name,value_json FROM measurements WHERE run_id=? AND scope=? AND scope_id=?",
            (run_id, scope, scope_id),
        )}

    def metric_series(self, run_id, *, trial_id, metric, start_ns, end_ns, limit=10000):
        if type(limit) is not int or not 1 <= limit <= 10000:
            raise ValueError("metric series limit must be between 1 and 10000")
        if type(start_ns) is not int or type(end_ns) is not int or start_ns > end_ns:
            raise ValueError("invalid metric series time bounds")
        where = "p.run_id=? AND p.scope='trial' AND p.window LIKE 'timeline:%' AND p.metric=? AND w.end_ns>? AND w.start_ns<?"
        params = [run_id, metric, start_ns, end_ns]
        if trial_id is not None:
            where += " AND p.scope_id=?"
            params.append(trial_id)
        join = "metric_points p JOIN metric_windows w USING(run_id,scope,scope_id,window)"
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            total = db.execute(f"SELECT count(*) FROM {join} WHERE {where}", params).fetchone()[0]
            rows = db.execute(
                summaries.query_prefix(join) + f"SELECT p.*,w.start_ns,w.end_ns FROM {join} WHERE {where} "
                "ORDER BY w.start_ns,p.scope_id,p.statistic LIMIT ?", [*params, limit],
            ).fetchall()
        return {"items": tuple({k: v for k, v in dict(row).items() if k not in {"revision", "definition_id", "rowid"}}
                               for row in rows), "total": total}

    def request_page(self, run_id, *, scope="turn", trial_id=None, turn_id=None, status=None,
                     offset=0, limit=50, from_ms=None, to_ms=None, sampled_only=False, session_id=None, detail=True):
        if scope not in {"turn", "attempt"}:
            raise ValueError("request scope must be turn or attempt")
        if type(offset) is not int or offset < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ValueError("invalid request page bounds")
        if any(value is not None and type(value) is not int for value in (from_ms, to_ms)):
            raise ValueError("request time bounds must be integer Unix milliseconds")
        if from_ms is not None and to_ms is not None and from_ms > to_ms:
            raise ValueError("request time range is reversed")
        table = "turns" if scope == "turn" else "attempts"
        select, joins = entity_select(table, 'q')
        if type(detail) is not bool:
            raise ValueError("request detail must be boolean")
        if not detail:
            columns = ",".join("q." + name for name in self.entity_columns[table]
                               if name not in {"metadata_json", "measurements_json"})
            select = select.replace("q.*", columns)
        if sampled_only or (scope == 'attempt' and (from_ms is not None or to_ms is not None)):
            joins += ' JOIN request_facts f ON f.id=q.fact_id'
        turn = 'q'
        if scope == 'attempt':
            joins += ' LEFT JOIN turns u ON u.id=q.turn_key'
            turn = 'u'
        identity = (f",t.ordinal AS trial_ordinal,s.ordinal AS session_ordinal,{turn}.ordinal AS turn_ordinal"
                    + "," + ",".join(
                        f"json_extract({turn}.metadata_json,'$.{key}') AS {key}"
                        for key in ("source_order", "source_session_id", "source_turn_id", "source_branch_id")))
        conditions, params = ["t.run_id=?"], [run_id]
        if type(sampled_only) is not bool:
            raise ValueError("sampled_only must be boolean")
        if sampled_only:
            conditions.append("json_extract(f.trace_json,'$.sampled')=1")
        for column, value in (("t.trial_id", trial_id), ("k.turn_id", turn_id), ("q.status", status), ("s.session_id", session_id)):
            if value is not None:
                conditions.append(f"{column}=?")
                params.append(value)
        observation = self.measurements(run_id, scope="run", scope_id=run_id).get("observation", {})
        clock = observation.get("clock")
        if scope == "turn":
            start = "q.start_ns"
            end = "q.end_ns"
        else:
            start = "COALESCE(f.admitted_ns,f.started_ns)"
            end = "f.result_ready_ns"
        for expression, operator, value in ((end, ">=", from_ms), (start, "<=", to_ms)):
            if value is None:
                continue
            if clock:
                conditions.append(f"{expression}{operator}?")
                params.append(value * 1_000_000 - clock["unix_ns"] + clock["monotonic_ns"])
            elif scope == "turn":
                column = "finished_at" if operator == ">=" else "started_at"
                conditions.append(f"julianday(q.{column}){operator}julianday(?,'unixepoch')")
                params.append(value / 1000)
            else:
                raise ValueError("Attempt time filtering requires a persisted clock anchor")
        where = " WHERE " + " AND ".join(conditions)
        order = "t.ordinal,s.ordinal," + ("q.ordinal,k.turn_id" if scope == "turn" else "k.turn_id,q.attempt")
        from .archive import detail_lease
        with detail_lease(self.path), closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute("BEGIN")
            # All request rows belong to the only Run, if the snapshot proves it.
            # Count the actual rows; final Run/Trial counters may still be pending.
            run_ids = tuple(row[0] for row in db.execute("SELECT run_id FROM runs"))
            if run_ids == (run_id,) and trial_id is None and turn_id is None and session_id is None:
                count_source = f"FROM {table} q"
                if sampled_only or (scope == 'attempt' and (from_ms is not None or to_ms is not None)):
                    count_source += ' JOIN request_facts f ON f.id=q.fact_id'
                count_where = ' WHERE ' + ' AND '.join(conditions[1:]) if len(conditions) > 1 else ''
                total = db.execute('SELECT count(*) ' + count_source + count_where, params[1:]).fetchone()[0]
            else:
                total = db.execute("SELECT count(*) " + joins + where, params).fetchone()[0]
            keys = []
            if offset < total:
                trials = db.execute('SELECT trial_id FROM trials WHERE run_id=?' +
                                    (' AND trial_id=?' if trial_id is not None else ''),
                                    [run_id, *([trial_id] if trial_id is not None else [])]).fetchall()
                key_where, key_params = where, list(params)
                if len(trials) == 1:
                    # A fixed Trial ordinal cannot affect ordering. This lets the
                    # sessions_trial_order index stream each Session's small sort.
                    key_where += ' AND s.trial_id=?'
                    key_params.append(trials[0][0])
                    order = order.removeprefix('t.ordinal,')
                page_size = min(limit, total - offset)
                reverse_offset = total - offset - page_size
                # Near the middle, changing direction saves too little traversal.
                reverse = reverse_offset * 2 < offset
                if reverse:
                    order = ','.join(column + ' DESC' for column in order.split(','))
                skip = reverse_offset if reverse else offset
                key_query = "SELECT q.id " + joins + key_where + " ORDER BY " + order + " LIMIT ? OFFSET ?"
                key_arguments = [*key_params, page_size, skip]
                if (scope == 'turn' and len(trials) == 1 and run_ids == (run_id,)
                        and turn_id is None and session_id is None and status is None
                        and from_ms is None and to_ms is None and not sampled_only
                        and skip >= max(total // 4, 4 * page_size)
                        and db.execute('SELECT count(*) FROM sessions').fetchone()[0] == total):
                    # Actual keys, not planned turn_count, prove one Turn per
                    # Session and unique ordinals in this same read snapshot.
                    sizes = db.execute('''SELECT
                        (SELECT count(*) FROM turn_keys),
                        (SELECT count(DISTINCT session_key) FROM turn_keys),
                        (SELECT count(DISTINCT ordinal) FROM sessions WHERE trial_id=?)''',
                        (trials[0][0],)).fetchone()
                    if all(size == total for size in sizes):
                        direction = ' DESC' if reverse else ''
                        key_query = ('SELECT q.id FROM (SELECT id,ordinal FROM sessions WHERE trial_id=? '
                                     'ORDER BY ordinal' + direction + ' LIMIT ? OFFSET ?) s '
                                     'JOIN turn_keys k ON k.session_key=s.id JOIN turns q ON q.id=k.id '
                                     'ORDER BY ' + order)
                        key_arguments = [trials[0][0], page_size, skip]
                keys = [row[0] for row in db.execute(key_query, key_arguments)]
                if reverse:
                    keys.reverse()
            rows = []
            for start_index in range(0, len(keys), 400):
                batch = keys[start_index:start_index + 400]
                rows.extend(db.execute("SELECT " + select + identity + " " + joins +
                    " WHERE q.id IN (" + ','.join('?' for _ in batch) + ')', batch).fetchall())
            by_key = {row['id']: row for row in rows}
            decoded = decode_entities(db, table, [by_key[key] for key in keys], detail=detail)
            for row in decoded:
                row.pop("run_id", None)
        return {"total": total, "items": tuple(decoded)}

    def trials(self, run_id: str) -> tuple[TrialRecord, ...]:
        rows = self._rows(
            """
            SELECT t.trial_id,t.name,t.ordinal,t.status,t.error,t.started_at,t.finished_at,
                   t.parameters_json AS parameters,t.metadata_json AS metadata,
                   t.session_count,t.turn_count,
                   f.load_profile_json,f.resource_limits_json,
                   f.comparison_group_id,f.comparison_axis_name,
                   f.comparison_axis_value_json,f.comparison_axis_unit,
                   f.repetition_index,f.expected_repetitions,f.scheduling_json,f.validity_requirements_json,f.validity_json,f.execution_status
            FROM trials t LEFT JOIN trial_facts f USING(trial_id)
            WHERE t.run_id=? ORDER BY t.ordinal
        """,
            [run_id],
        )
        output = []
        for row in rows:
            comparison: ComparisonRecord | None = None
            if row["comparison_group_id"] is not None:
                comparison = {
                    "group_id": row["comparison_group_id"],
                    "axis_name": row["comparison_axis_name"],
                    "axis_value": orjson.loads(row["comparison_axis_value_json"]),
                    "axis_unit": row["comparison_axis_unit"],
                }
            output.append(
                TrialRecord(
                    trial_id=row["trial_id"],
                    name=row["name"],
                    ordinal=row["ordinal"],
                    status=row["status"],
                    error=row["error"],
                    started_at=row["started_at"],
                    finished_at=row["finished_at"],
                    parameters=_json_object(row["parameters"]),
                    metadata=_json_object(row["metadata"]),
                    session_count=row["session_count"],
                    turn_count=row["turn_count"],
                    load_profile=(
                        _json_object(row["load_profile_json"])
                        if row["load_profile_json"] is not None
                        else None
                    ),
                    resource_limits=(
                        _json_object(row["resource_limits_json"])
                        if row["resource_limits_json"] is not None
                        else None
                    ),
                    comparison=comparison,
                    repetition=(
                        {
                            "index": row["repetition_index"],
                            "expected": row["expected_repetitions"],
                        }
                        if row["repetition_index"] is not None
                        else None
                    ),
                    validity_requirements=_json_object(row["validity_requirements_json"]) if row["validity_requirements_json"] is not None else None,
                    validity=_json_object(row["validity_json"]) if row["validity_json"] is not None else None,
                    execution_status=row["execution_status"],
                    scheduling=(
                        _json_object(row["scheduling_json"])
                        if row["scheduling_json"] is not None
                        else None
                    ),
                )
            )
        return tuple(output)

    def run_ids(self) -> tuple[str, ...]:
        return tuple(row[0] for row in self._rows("SELECT run_id FROM runs ORDER BY rowid"))

    def iter_turn_records(self, run_id):
        yield from self._iter_entities('turns', run_id)

    def iter_attempt_records(self, run_id):
        yield from self._iter_entities('attempts', run_id)

    def _iter_entities(self, table, run_id):
        select, source = entity_select(table)
        order = 't.ordinal,s.ordinal,' + ('r.ordinal,k.turn_id' if table == 'turns' else 'k.turn_id,r.attempt')
        from .archive import detail_lease
        with detail_lease(self.path), closing(sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            cursor = db.execute(f'SELECT {select} {source} WHERE t.run_id=? ORDER BY {order}', (run_id,))
            while rows := cursor.fetchmany(500):
                for row in decode_entities(db, table, rows):
                    row.pop('run_id', None)
                    if table == 'attempts':
                        row.pop('trial_id', None)
                    yield row

    def iter_summary_records(self, run_id):
        """Stream only formal summaries; never scan or decode request entities."""
        with closing(sqlite3.connect(self.path.as_uri() + '?mode=ro', uri=True)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            for table in RESULT_TABLES:
                if table in ENTITY_TABLES:
                    continue
                select, source = summaries.select(table)
                where = 'r.run_id=?'
                if table == 'trial_facts':
                    select += ',t.run_id,t.status'
                    source += ' JOIN trials t USING(trial_id)'
                    where = 't.run_id=?'
                cursor = db.execute(f'SELECT {select} {source} WHERE {where} ORDER BY r.rowid', (run_id,))
                while rows := cursor.fetchmany(500):
                    for row in rows:
                        yield _result_record(table, row)[0]

    def metric_windows(
        self, run_id: str, *, scope: str, scope_id: str
    ) -> tuple[MetricWindowRecord, ...]:
        return tuple(
            map(
                lambda row: MetricWindowRecord(**dict(row)),
                self._rows(
                    """
            SELECT "window",start_ns,end_ns,duration_s,availability,unavailable_reason
            FROM metric_windows WHERE run_id=? AND scope=? AND scope_id=?
            ORDER BY CASE "window" WHEN 'overall' THEN 0 WHEN 'last30s' THEN 1
              WHEN 'drop_first_20pct' THEN 2 WHEN 'steady_occupancy' THEN 3
              WHEN 'steady_strip_10pct' THEN 4 ELSE 5 END, "window"
        """,
                    [run_id, scope, scope_id],
                ),
            )
        )

    def metric_points(
        self,
        run_id: str,
        *,
        scope: str,
        scope_id: str,
        window: str,
        names: Collection[str] | None = None,
    ) -> Mapping[str, MetricRecord]:
        params: list[object] = [run_id, scope, scope_id, window]
        where = 'run_id=? AND scope=? AND scope_id=? AND "window"=?'
        if names:
            ordered = tuple(sorted(set(names)))
            where += f" AND metric IN ({','.join('?' for _ in ordered)})"
            params.extend(ordered)
        rows = self._rows(
            "SELECT metric,statistic,value,unit,formula_version,"
            "availability,unavailable_reason,source,valid_sample_count,"
            "expected_sample_count,missing_sample_count FROM metric_points "
            f"WHERE {where} ORDER BY metric,statistic",
            params,
        )
        result: dict[str, MetricRecord] = {}
        for row in rows:
            metric, statistic, value, unit, version = row[:5]
            availability, reason, source = row[5:8]
            if metric not in result:
                result[metric] = MetricRecord(
                    unit=str(unit),
                    formula_version=str(version),
                    availability="unavailable",
                    unavailable_reason=(str(reason) if reason is not None else None),
                    source=str(source) if source is not None else None,
                    statistics={},
                    statistic_metadata={},
                )
            point = result[metric]
            point["statistics"][statistic] = value
            point["statistic_metadata"][statistic] = StatisticMetadata(
                unit=str(unit),
                availability=str(availability),
                unavailable_reason=(str(reason) if reason is not None else None),
                source=str(source) if source is not None else None,
                valid_sample_count=row[8],
                expected_sample_count=row[9],
                missing_sample_count=row[10],
            )
            if availability == "available":
                point["availability"] = "available"
                point["unavailable_reason"] = None
            elif availability == "partial" and point["availability"] == "unavailable":
                point["availability"] = "partial"
                point["unavailable_reason"] = str(reason) if reason is not None else None
        return result

    def sweep_records(self, run_id: str) -> tuple[SweepRecord, ...]:
        return tuple(_sweep_record(orjson.loads(row[0])) for row in self._rows(
            "SELECT payload_json FROM sweeps WHERE run_id=? ORDER BY sequence", (run_id,),
        ))

    def grouped_metrics(self, run_id: str, trial_id: str) -> tuple[GroupedMetricRecord, ...]:
        return tuple(cast(GroupedMetricRecord, _result_record("grouped_metrics", row)[0]) for row in self._rows(
            "SELECT * FROM grouped_metrics WHERE run_id=? AND trial_id=? "
            'ORDER BY "window",dimensions_json,metric', (run_id, trial_id),
        ))

    def comparison_points(self, run_id: str) -> tuple[ComparisonRecord, ...]:
        stored = {
            row[0]
            for row in self._rows(
                "SELECT DISTINCT scope_id FROM metric_points "
                "WHERE run_id=? AND scope='comparison'",
                [run_id],
            )
        }
        ordered: dict[str, ComparisonRecord] = {}
        for trial in self.trials(run_id):
            point = trial["comparison"]
            if point is None or point["group_id"] not in stored:
                continue
            record = ComparisonRecord(**point)
            previous = ordered.setdefault(point["group_id"], record)
            if previous != record:
                raise RuntimeError(
                    f"inconsistent comparison definition: {point['group_id']}"
                )
        return tuple(ordered.values())


class _SQLiteBackend:
    def __init__(self, path: Path, run: RunRecord, *, summary_only=False) -> None:
        self.path, self.run = path, run
        self.connection: sqlite3.Connection | None = None
        self.finalized = False
        self.writer_lease = None
        from .summary_retention import SummaryRetention
        self.retention = SummaryRetention(path) if summary_only else None

    @property
    def artifact(self) -> SaverArtifact:
        return SaverArtifact("database", str(self.path))

    def open(self) -> None:
        from .archive import archive_path, register_writer
        if archive_path(self.path).exists():
            raise ValueError("cannot write a frozen result archive")
        self.writer_lease = register_writer(self.path)
        self.connection = ResultDatabase(self.path).connect()
        if self.connection.execute("SELECT count(*) FROM runs").fetchone()[0]:
            raise ValueError("result file already contains a Run")
        revision = self.connection.execute(
            "UPDATE result_state SET revision=revision+1 WHERE id=1 RETURNING revision"
        ).fetchone()[0]
        self.connection.execute(
            "INSERT INTO runs(run_id,name,status,created_at,preparing_at,started_at,finished_at,error,counts_json,revision) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (*_encode_row(_run_row(self.run)), revision),
        )
        self.connection.commit()
        self.entities = EntityStore(self.connection)
        self.statements = {}
        for kind, table in (
            ("sweep", "sweeps"),
            ("trial", "trials"),
            ("trial_fact", "trial_facts"),
            ("measurement", "measurements"),
            ("window", "metric_windows"),
            ("point", "metric_points"),
            ("group", "grouped_metrics"),
        ):
            columns = [c for c in self.connection.execute(f"PRAGMA table_info({table})") if c[1] != "revision"]
            placeholders = ",".join("?" for _ in columns)
            primary = ",".join(column[1] for column in columns if column[5])
            updates = ",".join(
                f"{column[1]}=excluded.{column[1]}" for column in columns if not column[5]
            )
            # REPLACE deletes parent rows and scans their already-persisted children.
            self.statements[kind] = (
                f"INSERT INTO {table}({','.join(c[1] for c in columns)},revision) VALUES ",
                f"({placeholders},?)",
                f" ON CONFLICT({primary}) DO UPDATE SET {updates},revision=excluded.revision WHERE " + " OR ".join(
                    f"{table}.{c[1]} IS NOT excluded.{c[1]}" for c in columns if not c[5]),
            )
        if self.retention is not None:
            self.write_batch((MeasurementRecord(self.run.id, 'run', self.run.id, 'result_retention',
                                               {'summary_only': True, 'request_details_retained': False}),))

    def write_batch(self, records: Sequence[ResultRecord]) -> ResultCommit | None:
        if self.retention is not None:
            records = self.retention.filter(records)
            if not records:
                return None
        rows: dict[str, list[tuple]] = defaultdict(list)
        entities = []
        for item in records:
            if isinstance(item, (SessionResultRecord, TurnResultRecord, AttemptResultRecord)):
                entities.append(item)
            elif not isinstance(item, (MetricPoint, GroupedMetricRecord)):
                kind, payload = _database_row(self.run.id, item)
                rows[kind].append(_encode_row(payload, json_columns={
                    'sweep': (2,), 'trial_fact': (5,), 'measurement': (4,),
                }.get(kind, ())))
        assert self.connection is not None
        try:
            with self.connection:
                before = self.connection.total_changes
                revision = self.connection.execute("UPDATE result_state SET revision=revision+1 WHERE id=1 RETURNING revision").fetchone()[0]
                self.entities.revision = revision
                for kind, values in summaries.rows(records, self.run.id, self.entities.definitions).items():
                    rows[kind].extend(_encode_row(value) for value in values)
                for kind, (prefix, placeholders, suffix) in self.statements.items():
                    if rows[kind]:
                        statement = prefix + ",".join([placeholders] * len(rows[kind])) + suffix
                        self.connection.execute(statement, tuple(chain.from_iterable((*row,revision) for row in rows[kind])))
                self.entities.write(entities)
                if self.connection.total_changes == before + 1:
                    self.connection.execute("UPDATE result_state SET revision=revision-1 WHERE id=1")
                    return None
            return ResultCommit(revision)
        except BaseException:
            self.entities.clear()
            raise

    def finalize(self, run: RunRecord) -> SQLiteResultReader:
        assert self.connection is not None
        if self.retention is not None:
            self.write_batch((MeasurementRecord(run.id, 'run', run.id, 'request_summary',
                {'counts': dict(self.retention.counts), 'attempt_errors': dict(self.retention.errors)}),))
        before = self.connection.total_changes
        with self.connection:
            revision = self.connection.execute("UPDATE result_state SET revision=revision+1 WHERE id=1 RETURNING revision").fetchone()[0]
            values = _encode_row(_run_row(run))
            names = 'name,status,created_at,preparing_at,started_at,finished_at,error,counts_json'.split(',')
            self.connection.execute("UPDATE runs SET " + ','.join(name+'=?' for name in names) + ",revision=?" +
                " WHERE run_id=? AND (" + ' OR '.join(name+' IS NOT ?' for name in names) + ')',
                (*values[1:], revision, values[0], *values[1:]))
            if self.connection.total_changes == before + 1:
                self.connection.execute("UPDATE result_state SET revision=revision-1 WHERE id=1")
        self.run, self.finalized = run, True
        reader = SQLiteResultReader(self.path)
        if self.retention is not None:
            self.retention.finalize(reader, run.id)
        return reader

    def close(self) -> None:
        if self.connection is not None:
            self.connection.close()
            self.connection = None
        if self.writer_lease is not None:
            from .archive import finish_writer
            lease, self.writer_lease = self.writer_lease, None
            finish_writer(lease, self.path, finalized=self.finalized and self.run.status in {"succeeded", "failed", "cancelled"})


def parse_settings(settings: dict) -> dict:
    unknown = set(settings) - {"path", "summary_only"}
    if unknown:
        raise ValueError(f"unknown sqlite saver setting: {sorted(unknown)[0]}")
    value = settings.get("path")
    if not isinstance(value, str) or not value:
        raise ValueError("saver path is required")
    summary_only = settings.get('summary_only', False)
    if type(summary_only) is not bool:
        raise ValueError('saver summary_only must be boolean')
    return {"path": value, "summary_only": summary_only}


@register_plugin(
    stage="saver",
    name="sqlite",
    description="Persist windowed Trial and comparison metrics",
    settings_parser=parse_settings,
)
class SQLiteSaver:
    @staticmethod
    def open_reader(location: str):
        from .archive import archive_path
        from .parquet_reader import ArchiveReader
        path = Path(location).resolve()
        if path.is_dir():
            return ArchiveReader(path)
        directory = archive_path(path)
        return ArchiveReader(directory) if directory.exists() else SQLiteResultReader(path)

    def __init__(self, settings) -> None:
        self.path = str(settings["path"])
        self.summary_only = settings.get('summary_only', False)

    def create_backend(self, run: RunRecord, ctx: SaverContext) -> SaverBackend:
        path = Path(self.path).expanduser()
        if not path.is_absolute():
            path = ctx.base_dir / path
        return _SQLiteBackend(path, run, summary_only=self.summary_only)


def _run_row(run: RunRecord) -> tuple:
    return (
        run.id,
        run.name,
        run.status,
        run.created_at,
        run.preparing_at,
        run.started_at,
        run.finished_at,
        run.error,
        run.counts,
    )


def _database_row(run_id: str, item: ResultRecord) -> tuple[str, tuple]:
    if isinstance(item, SweepRecord):
        return "sweep", (item.run_id, item.sequence, _json_text(item))
    if isinstance(item, TrialResultRecord):
        return "trial", (
            item.id,
            item.run_id,
            item.name,
            item.ordinal,
            item.status,
            item.parameters,
            item.metadata,
            item.started_at,
            item.finished_at,
            item.error,
            item.session_count,
            item.turn_count,
            item.succeeded_turns,
            item.failed_or_cancelled_turns,
        )
    if isinstance(item, TrialFactRecord):
        fact = item.fact
        comparison = fact.comparison
        return "trial_fact", (
            fact.trial_id,
            fact.load_profile,
            fact.resource_limits,
            comparison.group_id if comparison is not None else None,
            comparison.axis_name if comparison is not None else None,
            _json_text(comparison.axis_value) if comparison is not None else None,
            comparison.axis_unit if comparison is not None else None,
            fact.repetition.index,
            fact.repetition.expected,
            fact.scheduling,
            fact.validity_requirements,
            fact.validity,
            fact.execution_status,
        )
    if isinstance(item, MeasurementRecord):
        return "measurement", (
            item.run_id,
            item.scope,
            item.scope_id,
            item.name,
            _json_text(item.value),
        )
    if isinstance(item, MetricWindowResultRecord):
        return "window", tuple(getattr(item, name) for name in _field_names(type(item)))
    raise TypeError(f"unsupported Saver record: {type(item).__name__}")


def _encode_row(row: tuple, *, json_columns=()) -> tuple:
    # These columns already validated their constituent facts through _json_text.
    # Their serialized JSON length is distinct from the per-fact string limit.
    return tuple(value if index in json_columns else _json_dump(value) if _validate_value(value) else value
                 for index, value in enumerate(row))


def _decoded_row(row):
    return {
        key[:-5] if key.endswith("_json") else key:
        orjson.loads(row[key]) if key.endswith("_json") and row[key] is not None else row[key]
        for key in row.keys() if key != "revision"
    }


def _json_object(value: str) -> dict[str, Any]:
    decoded = orjson.loads(value)
    if not isinstance(decoded, dict):
        raise TypeError("stored JSON value must be an object")
    return decoded


__all__ = [
    "SQLiteResultReader",
    "SQLiteSaver",
    "parse_settings",
]


def _validity(value):
    if value is None:
        return None
    return TrialValidity(value["trial_id"], value["execution_status"], value["status"],
                         tuple(value["reasons"]),
                         tuple((name, number, unit) for name, number, unit in value["observations"]))


def _sweep_record(value: dict[str, Any]) -> SweepRecord:
    value = dict(value)
    checks = []
    for item in value["checks"]:
        item = dict(item)
        item["rule"] = SloRule(**item["rule"])
        checks.append(SloCheck(**item))
    value["checks"] = tuple(checks)
    value["validity"] = tuple(_validity(item) for item in value["validity"])
    value["trial_ids"] = tuple(value["trial_ids"])
    value["definition"] = freeze_snapshot(value["definition"])
    return SweepRecord(**value)
