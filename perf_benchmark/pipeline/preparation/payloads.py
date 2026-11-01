from __future__ import annotations

from dataclasses import dataclass
import mmap
from pathlib import Path
import tempfile
import weakref

from perf_benchmark.core.validation import positive_int


_sealed_mappings = weakref.WeakSet()


def _is_sealed_mapping(value):
    return isinstance(value, mmap.mmap) and value in _sealed_mappings



@dataclass(frozen=True, slots=True)
class PayloadRef:
    offset: int
    byte_length: int


class BlobStore:
    """Append blobs once, then expose read-only mmap-backed views."""

    def __init__(self) -> None:
        self._stream = tempfile.TemporaryFile(mode="w+b")
        self._size = 0
        self._mapping: mmap.mmap | None = None
        self._sealed = False
        self._closed = False

    def use_file(self, path: Path, *, existing: bool = False) -> None:
        """Use a named preparation artifact without copying its payload bytes."""
        self._ensure_open()
        if self._size or self._sealed:
            raise RuntimeError("cannot replace a populated blob store")
        stream = path.open("rb" if existing else "x+b")
        self._stream.close()
        self._stream = stream
        self._size = path.stat().st_size
        if existing:
            self.seal()

    def append(self, data: bytes) -> PayloadRef:
        self._ensure_open()
        if self._sealed:
            raise RuntimeError("blob store is sealed")
        if not isinstance(data, bytes):
            raise TypeError("payload must be bytes")
        ref = PayloadRef(self._size, len(data))
        if data:
            self._stream.write(data)
            self._size += len(data)
        return ref

    def seal(self) -> None:
        self._ensure_open()
        if self._sealed:
            return
        self._stream.flush()
        if self._size:
            self._mapping = mmap.mmap(
                self._stream.fileno(), self._size, access=mmap.ACCESS_READ
            )
        if self._mapping is not None:
            _sealed_mappings.add(self._mapping)
        self._sealed = True

    def view(self, ref: PayloadRef) -> memoryview:
        self._ensure_open()
        if not self._sealed:
            raise RuntimeError("blob store is not sealed")
        if not isinstance(ref, PayloadRef):
            raise ValueError("invalid payload reference")
        offset = positive_int(ref.offset, "invalid payload reference offset", allow_zero=True)
        byte_length = positive_int(ref.byte_length, "invalid payload reference byte length", allow_zero=True)
        if offset + byte_length > self._size:
            raise ValueError("invalid payload reference")
        if byte_length == 0:
            return memoryview(b"")
        assert self._mapping is not None
        return memoryview(self._mapping)[
            offset:offset + byte_length
        ]

    def close(self) -> None:
        if self._closed:
            return
        if self._mapping is not None:
            self._mapping.close()
            self._mapping = None
        self._stream.close()
        self._closed = True

    def _ensure_open(self) -> None:
        if self._closed:
            raise RuntimeError("blob store is closed")
