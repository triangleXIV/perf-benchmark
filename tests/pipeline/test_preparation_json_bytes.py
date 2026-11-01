import dataclasses
import datetime
import json
import uuid

import pytest

from perf_benchmark.pipeline.contracts.normalization import compact_json_bytes, compact_json_text


@pytest.mark.parametrize("value", [
    None, True, False, {"a": 1, "b": 2**80},
    [1.0, -0.0, 1e-30, 1e20],
    {"text": "中文😀\n\u2028", "nested": [None, True, {"x": "null"}]},
    {1: "integer", False: "boolean"}, {"tuple": (1, 2)},
])
def test_payload_encoder_preserves_json_values(value):
    encoded = compact_json_bytes(value)
    assert isinstance(encoded, bytes)
    assert json.loads(encoded) == json.loads(compact_json_text(value))


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_values_keep_stdlib_encoding(value):
    assert compact_json_bytes({"value": value}) == compact_json_text({"value": value}).encode()


def test_aliases_and_subclasses_keep_stdlib_encoding():
    class Text(str):
        pass

    shared = {"text": Text("中文")}
    value = [shared, shared]
    assert compact_json_bytes(value) == compact_json_text(value).encode()


@pytest.mark.parametrize("value", [uuid.uuid4(), datetime.datetime(2026, 1, 1), object()])
def test_unsupported_objects_are_not_silently_serialized(value):
    with pytest.raises(TypeError):
        compact_json_bytes(value)


def test_dataclasses_and_cycles_keep_validation_errors():
    @dataclasses.dataclass
    class Item:
        value: int

    with pytest.raises(TypeError):
        compact_json_bytes(Item(1))
    cycle = []
    cycle.append(cycle)
    with pytest.raises(ValueError, match="Circular"):
        compact_json_bytes(cycle)


def test_swe_chat_payload_bytes_are_unchanged():
    request = {"messages": [{"role": "user", "content": "Run `echo 中文`"},
                            {"role": "tool", "content": "OK", "tool_call_id": ""}],
               "tools": [{"type": "function", "function": {"name": "Bash",
                          "parameters": {"type": "object", "properties": {"command": {"type": "string"}}}}}]}
    assert compact_json_bytes(request) == compact_json_text(request).encode()
