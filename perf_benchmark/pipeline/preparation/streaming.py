from __future__ import annotations
from datetime import datetime

import asyncio
from collections.abc import AsyncGenerator, AsyncIterator, Iterator, Mapping
from contextlib import AsyncExitStack
from copy import deepcopy
from dataclasses import asdict, dataclass, fields, replace
import json
from itertools import chain, groupby
from functools import cached_property
import sys
from pathlib import Path
from types import MappingProxyType
from typing import Protocol, cast

import pyarrow as pa

from perf_benchmark.core.causal import CausalNode, validate_graph
from perf_benchmark.core.models import (
    ContextMode,
    thaw_snapshot,
    InputKind,
    Metadata,
    Run,
    Session,
    Trial,
    Turn,
    require_metadata,
)
from perf_benchmark.core.lifecycle import (
    NORMALIZATION_BATCH_COMPLETED,
    NORMALIZATION_RECORD_FAILED,
    SELECTOR_BATCH_COMPLETED,
)
from perf_benchmark.core.tokenization import ModelTokenizer
from perf_benchmark.core.profiles import RequestProfile
from perf_benchmark.core.validation import positive_int, validate_extra_headers
from perf_benchmark.pipeline.context import (
    LoaderContext, NormalizationContext, RunContext, SelectorContext,
)
from perf_benchmark.pipeline.contracts.loader import (
    DatasetSource,
    LoaderPlugin,
    SOURCE_ORDER_COLUMN,
)
from perf_benchmark.pipeline.contracts.normalization import (
    NORMALIZED_SCHEMA,
    NormalizationPlugin,
    RecordNormalizationError,
    RecordNormalizationPlugin,
    TokenTexts,
    TurnDraft,
    compact_json_bytes,
    compact_json_text,
    resolve_token,
    token_plan,
    required_text,
)
from perf_benchmark.pipeline.contracts.selector import (
    SELECTED_SCHEMA,
    SelectionResult,
    SelectorPlugin,
)
from perf_benchmark.pipeline.preparation.datasets import LoaderHost
from perf_benchmark.pipeline.preparation.payloads import BlobStore, PayloadRef
from perf_benchmark.pipeline.resources import own

_DATA_STAGES = ("loader", "normalization", "selector")
ArrowBatchStream = AsyncIterator[pa.RecordBatch]

class _EventSink(Protocol):
    async def emit(self, name: str, *, stage: str, data: Mapping[str, object]) -> object: ...


@dataclass
class _TokenizerState:
    path: str | None
    base_dir: Path

    @cached_property
    def tokenizer(self) -> ModelTokenizer:
        return ModelTokenizer(self.path, base_dir=self.base_dir)


class NormalizationHost:
    def __init__(
        self,
        plugin: NormalizationPlugin,
        *,
        run_id: str,
        payloads: BlobStore,
        events: _EventSink,
    ) -> None:
        self._plugin = plugin
        self._run_id = run_id
        self._payloads = payloads
        self._events = events

    async def transform(
        self,
        source: DatasetSource,
        ctx: NormalizationContext,
    ) -> AsyncGenerator[pa.RecordBatch, None]:
        await asyncio.to_thread(self._plugin.prepare, ctx)
        if isinstance(self._plugin, RecordNormalizationPlugin):
            from perf_benchmark.pipeline.preparation.parallel import normalize_batches
            batches = normalize_batches(self._plugin, source, ctx)
        else:
            batches = source.batches()
        tokenizer = _TokenizerState(self._plugin.tokenizer_path, ctx.base_dir)
        try:
            async for value in batches:
                if ctx.cancellation.is_cancelled():
                    return
                batch, drafts = value if isinstance(value, tuple) else (value, None)
                rows = await asyncio.to_thread(
                    _prepare_normalized_batch, self._plugin, batch, tokenizer,
                    self._payloads, self._run_id, drafts,
                )
                if not rows:
                    continue
                output = pa.RecordBatch.from_pylist(rows, schema=NORMALIZED_SCHEMA)
                await self._events.emit(
                    NORMALIZATION_BATCH_COMPLETED,
                    stage="normalization",
                    data={"rows": output.num_rows, "metadata_bytes": output.nbytes},
                )
                yield output
        except RecordNormalizationError as exc:
            await self._events.emit(
                NORMALIZATION_RECORD_FAILED,
                stage="normalization",
                data={"source_order": exc.source_order,
                      "error": f"{type(exc.error).__name__}: {exc.error}"},
            )
            raise exc.error
        finally:
            await batches.aclose()

def _prepare_normalized_batch(
    plugin: NormalizationPlugin,
    batch: pa.RecordBatch,
    tokenizer: _TokenizerState,
    payloads: BlobStore,
    run_id: str,
    drafts: list[TurnDraft] | None = None,
) -> list[dict[str, object]]:
    if SOURCE_ORDER_COLUMN not in batch.schema.names:
        raise TypeError(f"loader output is missing {SOURCE_ORDER_COLUMN}")
    order_column = batch.column(SOURCE_ORDER_COLUMN)
    if not order_column.type.equals(pa.int64()) or order_column.null_count:
        raise TypeError("loader source order must be non-null int64")
    source_orders = set(order_column.to_pylist())
    if drafts is None:
        drafts = list(plugin.normalize_batch(batch))
    for draft in drafts:
        source_order = draft.source_order
        try:
            _validate_turn_draft(draft, source_orders)
        except BaseException as exc:
            if source_order is None:
                raise
            raise RecordNormalizationError(source_order, exc) from exc
    rows: list[dict[str, object]] = []
    for draft in _resolve_draft_tokens(drafts, tokenizer):
        try:
            rows.append(_normalized_row(draft, run_id, payloads))
        except BaseException as exc:
            raise RecordNormalizationError(draft.source_order, exc) from exc
    return rows


def _validate_turn_draft(draft: TurnDraft, source_orders: set[int]) -> None:
    if not isinstance(draft, TurnDraft):
        raise TypeError("normalization plugin must yield TurnDraft values")
    for name in ("source_order", "turn_order_key"):
        positive_int(getattr(draft, name), name, allow_zero=True)
    if draft.source_order not in source_orders:
        raise ValueError("TurnDraft source_order does not belong to the source batch")
    identity = {name: getattr(draft, name) for name in ("session_id", "turn_id", "input_kind")}
    if draft.source_session_id is not None:
        identity["source_session_id"] = draft.source_session_id
    for name in identity:
        required_text(identity, name)
    if draft.context_mode not in {"snapshot", "recorded", "live"}:
        raise ValueError(f"unsupported context_mode: {draft.context_mode}")
    if draft.context_mode == "recorded" and draft.recorded_response is None:
        raise ValueError("recorded context requires a recorded assistant response")
    if draft.created_at is not None and draft.created_at.tzinfo is None:
        raise ValueError("created_at must include a timezone")
    require_metadata(draft.metadata, event="normalization TurnDraft")
    if draft.extra_headers is not None:
        validate_extra_headers(draft.extra_headers)
    reserved = {"source_order", "source_session_id", "source_turn_id"}.intersection(
        draft.metadata
    )
    if reserved:
        raise ValueError(
            "TurnDraft metadata uses reserved keys: " + ", ".join(sorted(reserved))
        )
    positive_int(draft.requested_output_tokens, "requested_output_tokens", optional=True)


def _resolve_draft_tokens(
    drafts: list[TurnDraft],
    tokenizer: _TokenizerState,
) -> Iterator[TurnDraft]:
    token_fields = ("estimated_input_tokens", "reference_output_tokens", "system_tokens", "developer_tokens")
    plans = []
    for draft in drafts:
        values = {name: getattr(draft, name) for name in token_fields}
        if tokenizer.path is None:
            values.update(system_tokens=None, developer_tokens=None)
        plans.append({name: token_plan(value, name) if value is not None else None
                      for name, value in values.items()})
    texts = [text for row in plans for plan in row.values()
             if isinstance(plan, TokenTexts) for text in plan.texts]
    lengths = iter(tokenizer.tokenizer.count_batch(texts) if texts else ())
    for draft, row in zip(drafts, plans, strict=True):
        values = {name: resolve_token(plan, lengths) if plan is not None else None
                  for name, plan in row.items()}
        profile = draft.profile
        if profile is not None:
            profile = replace(profile,
                system_tokens_estimate=0 if profile.system_chars == 0 else values["system_tokens"],
                developer_tokens_estimate=0 if profile.developer_chars == 0 else values["developer_tokens"],
                tokenizer_fingerprint=tokenizer.tokenizer.fingerprint if "tokenizer" in vars(tokenizer) else profile.tokenizer_fingerprint)
        yield replace(draft, **values, profile=profile)


def _normalized_row(
    draft: TurnDraft,
    run_id: str,
    payloads: BlobStore,
) -> dict[str, object]:
    assert draft.source_order is not None
    metadata = {
        "source_order": draft.source_order,
        "source_session_id": draft.source_session_id or draft.session_id,
        "source_turn_id": draft.turn_id,
        **draft.metadata,
    }
    row = {
        "source_order": draft.source_order,
        "session_id": f"{run_id}:{draft.session_id}",
        "turn_id": f"{run_id}:{draft.turn_id}",
        "turn_order_key": draft.turn_order_key,
        "input_kind": draft.input_kind,
        "context_mode": draft.context_mode,
        "estimated_input_tokens": draft.estimated_input_tokens,
        "reference_output_tokens": draft.reference_output_tokens,
        "requested_output_tokens": draft.requested_output_tokens,
        "created_at": draft.created_at,
        "causal": asdict(draft.causal) if draft.causal else None,
        "metadata_json": compact_json_text(metadata),
    }
    for name, value in (
        ("request", draft.request),
        ("extra_headers", draft.extra_headers or None),
        ("recorded_response", draft.recorded_response),
    ):
        ref = payloads.append(compact_json_bytes(value)) if name == "request" or value is not None else None
        row[f"{name}_payload_offset"] = ref.offset if ref else None
        row[f"{name}_payload_bytes"] = ref.byte_length if ref else None
    for name in ("profile", "recorded_profile"):
        profile = getattr(draft, name)
        row[f"{name}_json"] = json.dumps(asdict(profile)) if profile else None
    return row


@dataclass(frozen=True, slots=True)
class SelectedDataset:
    batches: tuple[pa.RecordBatch, ...]
    counts: Mapping[str, int]


class SelectorHost:
    def __init__(self, plugin: SelectorPlugin, *, events: _EventSink) -> None:
        self._plugin = plugin
        self._events = events

    async def select(
        self,
        source: ArrowBatchStream,
        ctx: SelectorContext,
    ) -> SelectedDataset:
        batches: list[pa.RecordBatch] = []
        async for batch in source:
            if not batch.schema.equals(NORMALIZED_SCHEMA):
                raise TypeError("normalization output does not match normalized schema")
            batches.append(batch)
            if ctx.cancellation.is_cancelled():
                return SelectedDataset((), MappingProxyType({}))
        if ctx.cancellation.is_cancelled():
            return SelectedDataset((), MappingProxyType({}))
        table = pa.Table.from_batches(batches, schema=NORMALIZED_SCHEMA)
        batches.clear()
        selected = await asyncio.to_thread(_select_table, self._plugin, table)
        for index, batch in enumerate(selected.batches):
            await self._events.emit(
                SELECTOR_BATCH_COMPLETED,
                stage="selector",
                data={"batch_index": index, "rows": batch.num_rows},
            )
        return selected


def _select_table(plugin: SelectorPlugin, table: pa.Table) -> SelectedDataset:
    if table.num_rows == 0:
        raise ValueError("selector received no normalized records")
    sessions = _validate_source_turns(table)
    result = plugin.select(table)
    if not isinstance(result, SelectionResult):
        raise TypeError("selector plugin must return SelectionResult")
    indices = result.indices
    if not isinstance(indices, pa.Array) or not indices.type.equals(pa.int64()):
        raise TypeError("SelectionResult indices must be an int64 Arrow array")
    if indices.null_count:
        raise ValueError("SelectionResult indices cannot contain nulls")
    values = indices.to_pylist()
    if not values:
        raise ValueError("selector produced no records")
    if len(values) != len(set(values)):
        raise ValueError("SelectionResult indices must be unique")
    if min(values) < 0 or max(values) >= table.num_rows:
        raise ValueError("SelectionResult index is outside normalized input")
    selected = table.take(indices)
    session_ordinals, turn_ordinals = _selection_ordinals(selected, sessions)
    selected = selected.append_column(
        "session_ordinal", pa.array(session_ordinals, type=pa.int32())
    ).append_column("turn_ordinal", pa.array(turn_ordinals, type=pa.int32()))
    selected = selected.select(SELECTED_SCHEMA.names).cast(SELECTED_SCHEMA)
    diagnostics = _validate_diagnostics(result.diagnostics)
    counts = MappingProxyType(
        {
            "candidates": len(sessions),
            "candidate_turns": table.num_rows,
            "selected": session_ordinals[-1] + 1,
            "selected_turns": selected.num_rows,
            **diagnostics,
        }
    )
    return SelectedDataset(
        tuple(selected.to_batches(max_chunksize=65_536)),
        counts,
    )


def _validate_source_turns(table: pa.Table) -> dict[str, tuple[int, ...]]:
    sessions: dict[str, set[int]] = {}
    for session_id, order_key in zip(
        table.column("session_id").to_pylist(),
        table.column("turn_order_key").to_pylist(),
        strict=True,
    ):
        keys = sessions.setdefault(session_id, set())
        if order_key in keys:
            raise ValueError(f"session has ambiguous turn_order_key: {session_id}")
        keys.add(order_key)
    return {name: tuple(sorted(values)) for name, values in sessions.items()}


def _selection_ordinals(
    table: pa.Table,
    source_sessions: Mapping[str, tuple[int, ...]],
) -> tuple[list[int], list[int]]:
    session_ordinals: list[int] = []
    turn_ordinals: list[int] = []
    seen: set[str] = set()
    rows = zip(table.column("session_id").to_pylist(), table.column("turn_order_key").to_pylist(), strict=True)
    for session_id, group in groupby(rows, key=lambda row: row[0]):
        if session_id in seen:
            raise ValueError(f"selected session_id is not contiguous: {session_id}")
        keys = tuple(row[1] for row in group)
        if keys != source_sessions[session_id][:len(keys)]:
            raise ValueError(f"selected Turns are not a Session prefix: {session_id}")
        session_ordinals.extend([len(seen)] * len(keys))
        turn_ordinals.extend(range(len(keys)))
        seen.add(session_id)
    return session_ordinals, turn_ordinals


def _validate_diagnostics(values: Mapping[str, int]) -> dict[str, int]:
    if not isinstance(values, Mapping):
        raise TypeError("SelectionResult diagnostics must be a mapping")
    reserved = {"candidates", "candidate_turns", "selected", "selected_turns"}
    diagnostics: dict[str, int] = {}
    for name, value in values.items():
        if not isinstance(name, str) or not name:
            raise TypeError("selector diagnostic names must be non-empty text")
        if name in reserved:
            raise ValueError(f"selector diagnostic is reserved: {name}")
        diagnostics[name] = positive_int(value, f"selector diagnostic {name}", allow_zero=True)
    return diagnostics


@dataclass(frozen=True, slots=True)
class _TurnPlan:
    ordinal: int
    source_created_at: datetime | None
    causal: CausalNode | None
    input_kind: InputKind
    context_mode: ContextMode
    request_payload_ref: PayloadRef
    recorded_response_payload_ref: PayloadRef | None
    estimated_input_tokens: int
    reference_output_tokens: int | None
    requested_output_tokens: int | None
    metadata: Metadata
    profile: RequestProfile | None
    recorded_profile: RequestProfile | None
    extra_headers_payload_ref: PayloadRef | None = None


@dataclass(frozen=True, slots=True)
class _SessionPlan:
    ordinal: int
    source_session_id: str
    turns: tuple[_TurnPlan, ...]


def _decode_session_plans(batches: tuple[pa.RecordBatch, ...]) -> tuple[_SessionPlan, ...]:
    # Only SelectorHost produces this immutable schema and validates its order.
    sessions: list[_SessionPlan] = []
    rows = chain.from_iterable(batch.to_pylist() for batch in batches)
    for source_id, group in groupby(rows, key=lambda row: str(row["session_id"])):
        turns = []
        for ordinal, row in enumerate(group):
            metadata = json.loads(row["metadata_json"])
            # Selected rows come from immutable normalized Arrow input; the
            # Selector supplies only indices. Recheck order, not source fields.
            for name in ("request", "recorded_response", "extra_headers"):
                offset = row[f"{name}_payload_offset"]
                row[f"{name}_payload_ref"] = PayloadRef(offset, row[f"{name}_payload_bytes"]) if offset is not None else None
            for name in ("profile", "recorded_profile"):
                row[name] = RequestProfile(**json.loads(row[f"{name}_json"])) if row[f"{name}_json"] else None
            turns.append(_TurnPlan(
                ordinal=ordinal,
                source_created_at=row["created_at"],
                causal=CausalNode.decode(row["causal"]),
                metadata={"source_session_id": source_id, "source_turn_id": str(row["turn_id"]), **metadata},
                **{field.name: row[field.name] for field in fields(_TurnPlan)
                   if field.name not in {"ordinal", "source_created_at", "causal", "metadata"}},
            ))
        sessions.append(_SessionPlan(
            len(sessions), str(turns[0].metadata.get("source_session_id", source_id)), tuple(turns),
        ))
    return tuple(sessions)


def _materialize_session(
    plan: _SessionPlan,
    trial: Trial,
    ordinal: int,
    *,
    cycle: int | None = None,
) -> Session:
    turns = [
            Turn(
                id=f"{trial.id}:turn-{ordinal}-{turn.ordinal}",
                **{f.name: getattr(turn, f.name) for f in fields(_TurnPlan) if f.name != "metadata"},
                metadata={**deepcopy(turn.metadata), **({"causal": thaw_snapshot(asdict(turn.causal))} if turn.causal else {})},
            )
            for turn in plan.turns
    ]
    metadata: Metadata = {"source_session_id": plan.source_session_id}
    if cycle is not None:
        metadata.update(cycle=cycle, source_session_ordinal=plan.ordinal)
    session = Session(
        id=f"{trial.id}:session-{ordinal}", trial_id=trial.id, ordinal=ordinal,
        turns=turns, metadata=metadata,
    )
    session.link()
    return session


class DatasetExhausted(RuntimeError):
    pass


def _validate_causal_plans(plans):
    branches = set()
    for plan in plans:
        nodes = [t.causal for t in plan.turns if t.causal]
        if nodes:
            keys = {(node.graph_id, node.branch_id) for node in nodes}
            if len(keys) != 1 or len(nodes) != len(plan.turns) or keys & branches:
                raise ValueError("each causal branch must map to exactly one complete Session")
            branches.update(keys)
    return validate_graph(t.causal for p in plans for t in p.turns if t.causal)


class PreparedCorpus:
    def __init__(
        self,
        payloads: BlobStore,
        plans: tuple[_SessionPlan, ...] = (),
    ) -> None:
        self.payloads = payloads
        self.plans = plans
        self.turn_count = sum(len(plan.turns) for plan in plans)
        self.max_graph_branches = _validate_causal_plans(plans)

    @property
    def requires_start_notifications(self):
        return any(edge.event == "started" for p in self.plans for t in p.turns if t.causal for edge in t.causal.dependencies)

    async def prepare(
        self,
        run: Run,
        ctx: RunContext,
        lifecycle,
    ) -> None:
        resources = AsyncExitStack()
        try:
            plugins = {stage: ctx.binding(stage).plugin for stage in _DATA_STAGES}
            for stage, contract in zip(_DATA_STAGES, (LoaderPlugin, NormalizationPlugin, SelectorPlugin)):
                if not isinstance(plugins[stage], contract):
                    raise TypeError(f"bound {stage} does not implement {contract.__name__}")
            from perf_benchmark.pipeline.preparation.cache import PreparedCache
            cache = await asyncio.to_thread(PreparedCache.open, ctx)
            cached = None
            if cache is not None:
                resources.callback(cache.close)
                cached = await asyncio.to_thread(cache.load, ctx.payloads, run.id)
            if cached is not None:
                for stage in _DATA_STAGES:
                    await lifecycle.start(stage)
                selected = SelectedDataset(cached[0], MappingProxyType(cached[1]))
                run.counts.update(selected.counts)
            else:
                loader = LoaderHost(cast(LoaderPlugin, plugins["loader"]), events=ctx.events)
                normalizer = NormalizationHost(
                    cast(NormalizationPlugin, plugins["normalization"]),
                    run_id=run.id,
                    payloads=ctx.payloads,
                    events=ctx.events,
                )
                selector = SelectorHost(cast(SelectorPlugin, plugins["selector"]), events=ctx.events)
                await lifecycle.start("loader")
                source = await loader.open(LoaderContext(ctx.base_dir, ctx.cancellation))
                own(resources, source.close)
                await lifecycle.start("normalization")
                normalized = normalizer.transform(source, NormalizationContext(ctx.base_dir, ctx.cancellation, ctx.seed))
                own(resources, normalized.aclose)
                await lifecycle.start("selector")
                selected = await selector.select(normalized, SelectorContext(ctx.cancellation))
                run.counts.update(selected.counts)
                run.counts["source_rows"] = source.rows_read
            ctx.payloads.seal()
            plans: tuple[_SessionPlan, ...] = ()
            if not ctx.cancellation.is_cancelled():
                plans = await asyncio.to_thread(
                    _decode_session_plans,
                    selected.batches,
                )
            self.plans = plans
            self.turn_count = sum(len(plan.turns) for plan in plans)
            self.max_graph_branches = _validate_causal_plans(plans)
            if cache is not None and cached is None and not ctx.cancellation.is_cancelled():
                await asyncio.to_thread(cache.publish, selected.batches, run.counts, run.id)
            await lifecycle.finish(*_DATA_STAGES,
                                   outcome="cancelled" if ctx.cancellation.is_cancelled() else "completed")
        except BaseException as exc:
            await lifecycle.finish(*_DATA_STAGES, outcome="failed", error=f"{type(exc).__name__}: {exc}")
            raise
        finally:
            await resources.__aexit__(*sys.exc_info())

    @property
    def session_count(self) -> int:
        return len(self.plans)

    def materialize_sessions(self, trial: Trial) -> tuple[Session, ...]:
        return tuple(_materialize_session(plan, trial, index) for index, plan in enumerate(self.plans))

    def warmup_sessions(self, trial: Trial, request_count: int):
        if self.turn_count < request_count:
            raise DatasetExhausted("selected dataset has fewer Turns than warmup_requests")
        remaining = request_count
        for ordinal, plan in enumerate(self.plans):
            if remaining == 0:
                break
            session = _materialize_session(plan, trial, ordinal)
            session.id = f"{trial.id}:warmup-session-{ordinal}"
            session.metadata["warmup"] = True
            session.turns = session.turns[:remaining]
            for turn in session.turns:
                turn.id = f"{trial.id}:warmup-turn-{ordinal}-{turn.ordinal}"
            session.link()
            remaining -= len(session.turns)
            yield session

    def session_stream(self, trial: Trial, *, session_count: int | None, on_exhausted: str):
        if not self.plans and (session_count is None or session_count > 0):
            raise DatasetExhausted("dataset has 0 Sessions")
        if session_count is not None and on_exhausted == "error" and session_count > len(self.plans):
            raise DatasetExhausted(
                f"dataset has {len(self.plans)} Sessions but stop.session_count={session_count}"
            )
        plan_count = len(self.plans)

        async def generate():
            ordinal = 0
            while session_count is None or ordinal < session_count:
                if ordinal >= plan_count and on_exhausted == "stop":
                    return
                if ordinal >= plan_count and on_exhausted == "error":
                    raise DatasetExhausted("dataset exhausted while scheduling Sessions")
                plan = self.plans[ordinal % plan_count]
                yield _materialize_session(
                    plan,
                    trial,
                    ordinal,
                    cycle=ordinal // plan_count if on_exhausted == "cycle" else None,
                )
                ordinal += 1

        return generate()

    async def close(self) -> None:
        self.plans = ()
        self.turn_count = 0
        self.payloads.close()
