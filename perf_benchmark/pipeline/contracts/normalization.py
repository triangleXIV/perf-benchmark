from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import json
import math
from typing import Any

import orjson
import pyarrow as pa

from perf_benchmark.core.models import ContextMode, InputKind, Metadata
from perf_benchmark.core.profiles import RequestProfile
from perf_benchmark.core.validation import positive_int, validate_extra_headers
from perf_benchmark.pipeline.contracts.loader import SOURCE_ORDER_COLUMN
from perf_benchmark.core.causal import CausalNode


CAUSAL_SCHEMA = pa.struct([
    pa.field("graph_id", pa.string()), pa.field("node_id", pa.string()), pa.field("branch_id", pa.string()),
    pa.field("root_ns", pa.int64()), pa.field("not_before_ns", pa.int64()),
    pa.field("dependencies", pa.list_(pa.struct([pa.field("node_id", pa.string()),
             pa.field("event", pa.string()), pa.field("delay_ns", pa.int64())]))),
])

NORMALIZED_SCHEMA = pa.schema(
    [
        pa.field("source_order", pa.int64(), nullable=False),
        pa.field("session_id", pa.string(), nullable=False),
        pa.field("turn_id", pa.string(), nullable=False),
        pa.field("turn_order_key", pa.int64(), nullable=False),
        pa.field("input_kind", pa.string(), nullable=False),
        pa.field("context_mode", pa.string(), nullable=False),
        pa.field("request_payload_offset", pa.int64(), nullable=False),
        pa.field("request_payload_bytes", pa.int64(), nullable=False),
        pa.field("extra_headers_payload_offset", pa.int64()),
        pa.field("extra_headers_payload_bytes", pa.int64()),
        pa.field("recorded_response_payload_offset", pa.int64()),
        pa.field("recorded_response_payload_bytes", pa.int64()),
        pa.field("estimated_input_tokens", pa.int32(), nullable=False),
        pa.field("reference_output_tokens", pa.int32()),
        pa.field("requested_output_tokens", pa.int32()),
        pa.field("created_at", pa.timestamp("us", tz="UTC")),
        pa.field("causal", CAUSAL_SCHEMA),
        pa.field("metadata_json", pa.string(), nullable=False),
        pa.field("profile_json", pa.string()),
        pa.field("recorded_profile_json", pa.string()),
    ]
)


@dataclass(frozen=True, slots=True)
class TokenTexts:
    texts: tuple[str, ...]
    known_tokens: int = 0


@dataclass(frozen=True, slots=True)
class FixedTokens:
    value: int


TokenValue = int | TokenTexts | FixedTokens


@dataclass(frozen=True, slots=True)
class TurnDraft:
    session_id: str
    turn_id: str
    turn_order_key: int
    request: object
    estimated_input_tokens: TokenValue
    reference_output_tokens: TokenValue | None
    input_kind: InputKind = "chat"
    context_mode: ContextMode = "snapshot"
    recorded_response: object | None = None
    created_at: datetime | None = None
    metadata: Metadata = field(default_factory=dict)
    requested_output_tokens: int | None = None
    source_order: int | None = None
    source_session_id: str | None = None
    profile: RequestProfile | None = None
    recorded_profile: RequestProfile | None = None
    system_tokens: TokenValue | None = None
    developer_tokens: TokenValue | None = None
    causal: CausalNode | None = None
    extra_headers: Mapping[str, str] | None = field(default=None, repr=False)


class RecordNormalizationError(Exception):
    def __init__(self, source_order: int, error: BaseException) -> None:
        self.source_order = source_order
        self.error = error
        super().__init__(str(error))

    def __reduce__(self):
        return type(self), (self.source_order, self.error)


class NormalizationPlugin(ABC):
    def prepare(self, ctx) -> None:
        """Initialize source-specific preparation on the Host's worker thread."""

    @property
    def tokenizer_path(self) -> str | None:
        return None

    @abstractmethod
    def normalize_batch(self, batch: pa.RecordBatch) -> Iterable[TurnDraft]:
        raise NotImplementedError


class RecordNormalizationPlugin(NormalizationPlugin):
    """Convenience contract for plugins that independently map source rows."""

    def __init__(self, settings: object) -> None:
        if not isinstance(settings, Mapping):
            raise TypeError("normalization plugin requires parsed settings")
        self.settings = settings

    @property
    def tokenizer_path(self) -> str | None:
        value = self.settings.get("tokenizer_path")
        return str(value) if value is not None else None

    def normalize_batch(self, batch: pa.RecordBatch) -> Iterable[TurnDraft]:
        if SOURCE_ORDER_COLUMN not in batch.schema.names:
            raise TypeError(f"loader output is missing {SOURCE_ORDER_COLUMN}")
        json_columns = {field.name: field.type for field in batch.schema
                        if pa.types.is_nested(field.type) or isinstance(field.type, pa.BaseExtensionType)}
        for record in batch.to_pylist():
            source_order = int(record.pop(SOURCE_ORDER_COLUMN))
            try:
                for name, kind in json_columns.items():
                    record[name] = _decode_arrow_json(record[name], kind)
                headers = record.pop("headers", None)
                if headers is not None:
                    if not isinstance(headers, Mapping):
                        raise TypeError("headers must be an object")
                    headers = validate_extra_headers({key: value for key, value in headers.items() if value is not None})
                for draft in self.normalize_record(record, source_order):
                    if not isinstance(draft, TurnDraft):
                        raise TypeError("normalization plugin must yield TurnDraft values")
                    if draft.source_order not in (None, source_order):
                        raise ValueError("TurnDraft source_order does not match source row")
                    yield _dataset_tokens(
                        record,
                        _profile_draft(replace(draft, source_order=source_order, extra_headers=headers)),
                    )
            except BaseException as exc:
                if isinstance(exc, RecordNormalizationError):
                    raise
                raise RecordNormalizationError(source_order, exc) from exc

    @abstractmethod
    def normalize_record(
        self,
        record: Mapping[str, Any],
        source_order: int,
    ) -> Iterable[TurnDraft]:
        raise NotImplementedError


def parse_record_settings(
    settings: dict[str, Any],
    *,
    plugin: str,
    extra: Iterable[str] = (),
) -> dict[str, Any]:
    allowed = {"tokenizer_path", *extra}
    unknown = sorted(set(settings) - allowed)
    if unknown:
        raise ValueError(
            f"unknown {plugin} normalization settings: " + ", ".join(unknown)
        )
    parsed = dict(settings)
    tokenizer_path = parsed.get("tokenizer_path")
    if tokenizer_path is not None and (
        not isinstance(tokenizer_path, str) or not tokenizer_path
    ):
        raise TypeError("tokenizer_path must be a non-empty path")
    return parsed


def _dataset_tokens(record: Mapping[str, Any], draft: TurnDraft) -> TurnDraft:
    input_count = _declared_token(record, "prompt_tokens", "input_tokens")
    output_count = _declared_token(record, "completion_tokens", "output_tokens")
    requested = record.get("requested_output_tokens")
    if requested is None:
        requested = draft.requested_output_tokens
    return replace(
        draft,
        requested_output_tokens=requested,
        metadata={
            **draft.metadata,
            "reference_output_source": (
                "dataset_count"
                if output_count is not None
                else "tokenized_reference"
                if draft.reference_output_tokens is not None
                else "unavailable"
            ),
        },
        estimated_input_tokens=_choose_token(draft.estimated_input_tokens, input_count),
        reference_output_tokens=_choose_token(draft.reference_output_tokens, output_count),
    )


def _choose_token(fallback: TokenValue | None, declared: int | None) -> TokenValue | None:
    if isinstance(fallback, FixedTokens):
        return fallback.value
    return fallback if declared is None else declared


def _declared_token(
    record: Mapping[str, Any],
    usage_name: str,
    top_level_name: str,
) -> int | None:
    usage = record.get("usage")
    if usage is not None:
        if not isinstance(usage, Mapping):
            raise TypeError("usage must be an object")
        value = usage.get(usage_name)
        if value is not None:
            return positive_int(value, f"usage.{usage_name}", allow_zero=True)
    value = record.get(top_level_name)
    return positive_int(value, top_level_name, optional=True, allow_zero=True)


def token_plan(value: TokenValue, name: str) -> int | TokenTexts:
    if isinstance(value, FixedTokens):
        return positive_int(value.value, name, allow_zero=True)
    if isinstance(value, TokenTexts):
        positive_int(value.known_tokens, name, allow_zero=True)
        return value
    return positive_int(value, name, allow_zero=True)


def resolve_token(plan: int | TokenTexts, lengths: Iterator[int]) -> int:
    if isinstance(plan, int):
        return plan
    return plan.known_tokens + sum(next(lengths) for _ in plan.texts)


def token_texts(*values: str, known_tokens: int = 0) -> TokenTexts:
    if not all(isinstance(value, str) for value in values):
        raise TypeError("tokenizer inputs must be text")
    return TokenTexts(tuple(values), positive_int(known_tokens, "known token count", allow_zero=True))


def fixed_tokens(value: int) -> FixedTokens:
    return FixedTokens(positive_int(value, "fixed token count", allow_zero=True))


def message_tokens(message: Mapping[str, Any], fallback_text: str) -> TokenValue:
    value = message.get("token_count")
    return token_texts(fallback_text) if value is None else positive_int(
        value, "message token_count", allow_zero=True
    )


def combine_tokens(*values: TokenValue) -> TokenValue:
    known, texts = 0, []
    for value in values:
        if isinstance(value, FixedTokens):
            known += value.value
        elif isinstance(value, TokenTexts):
            known += value.known_tokens
            texts.extend(value.texts)
        else:
            known += positive_int(value, "token count", allow_zero=True)
    return TokenTexts(tuple(texts), known) if texts else known


def compact_json_text(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def compact_json_bytes(value: object) -> bytes:
    """Encode ordinary JSON quickly, preserving the stdlib fallback contract."""
    pending = [value]
    seen = set()
    while pending:
        item = pending.pop()
        kind = type(item)
        if kind is dict:
            marker = id(item)
            if marker in seen or any(type(key) is not str for key in item):
                return compact_json_text(value).encode()
            seen.add(marker)
            pending.extend(item.values())
        elif kind in (list, tuple):
            marker = id(item)
            if marker in seen:
                return compact_json_text(value).encode()
            seen.add(marker)
            pending.extend(item)
        elif kind is float:
            if not math.isfinite(item):
                return compact_json_text(value).encode()
        elif kind not in (str, int, bool, type(None)):
            return compact_json_text(value).encode()
    try:
        return orjson.dumps(value)
    except orjson.JSONEncodeError:
        return compact_json_text(value).encode()


def required_text(record: Mapping[str, Any], name: str) -> str:
    value = record.get(name)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be non-empty text")
    return value


def optional_text(record: Mapping[str, Any], name: str) -> str | None:
    value = record.get(name)
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    return value or None


def parse_created(value: object) -> datetime:
    if isinstance(value, bool):
        raise TypeError("created must be an epoch timestamp or ISO-8601 text")
    if isinstance(value, datetime):
        return (
            value.replace(tzinfo=timezone.utc)
            if value.tzinfo is None
            else value.astimezone(timezone.utc)
        )
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str) and value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("created must include a timezone")
        return parsed.astimezone(timezone.utc)
    raise TypeError("created must be an epoch timestamp or ISO-8601 text")


def message_profile(messages):
    """Count only content strings/text parts; never reasoning or tool arguments."""
    contents = []
    for message in messages:
        content = message.get("content")
        texts = [content] if isinstance(content, str) else [
            part["text"] for part in content
            if isinstance(part, Mapping) and part.get("type") in {"text", "input_text", "output_text"}
            and isinstance(part.get("text"), str)
        ] if isinstance(content, (list, tuple)) else []
        contents.append((message.get("role"), "".join(texts)))
    system = tuple(text for role, text in contents if role == "system")
    developer = tuple(text for role, text in contents if role == "developer")
    last_user = next((i for i in range(len(contents) - 1, -1, -1) if contents[i][0] == "user"), None)
    return RequestProfile(
        message_count=len(contents),
        history_message_count=(sum(role not in {"system", "developer"} for role, _ in contents[:last_user])
                               if last_user is not None else None),
        text_chars=sum(len(text) for _, text in contents),
        system_chars=sum(map(len, system)), developer_chars=sum(map(len, developer)),
        system_tokens_estimate=0 if not any(system) else None,
        developer_tokens_estimate=0 if not any(developer) else None,
    ), token_texts(*system), token_texts(*developer)


def _profile_draft(draft):
    if draft.input_kind != "chat" or draft.profile is not None:
        return draft
    request = draft.request
    messages = request.get("messages") if draft.context_mode == "snapshot" else [request]
    if not isinstance(messages, (list, tuple)) or not all(isinstance(m, Mapping) for m in messages):
        return draft
    profile, system, developer = message_profile(messages)
    tools = request.get("tools")
    recorded = message_profile([draft.recorded_response])[0] if draft.recorded_response is not None else None
    return replace(draft, profile=replace(profile, tool_count=len(tools) if tools is not None else None),
                   recorded_profile=recorded, system_tokens=system, developer_tokens=developer)


def _decode_arrow_json(value, kind):
    if value is None:
        return None
    if isinstance(kind, pa.BaseExtensionType) and kind.extension_name == "arrow.json":
        return json.loads(value)
    if pa.types.is_list(kind) or pa.types.is_large_list(kind) or pa.types.is_fixed_size_list(kind):
        for index, item in enumerate(value):
            value[index] = _decode_arrow_json(item, kind.value_type)
    elif pa.types.is_struct(kind):
        for field in kind:
            value[field.name] = _decode_arrow_json(value[field.name], field.type)
    return value
