"""Opt-in sealed SWE preparation cache, independent of run/concurrency/results."""

from collections.abc import Mapping
from dataclasses import fields, is_dataclass
import hashlib
from importlib.metadata import version
import json
import logging
import os
from pathlib import Path
import re
import shutil
import tempfile

from filelock import FileLock
import pyarrow as pa
import pyarrow.compute as pc

from perf_benchmark.pipeline.contracts.selector import SELECTED_SCHEMA

logger = logging.getLogger(__name__)
STAGES = ("loader", "normalization", "selector")


def _jsonable(value):
    if is_dataclass(value):
        return {f.name: _jsonable(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, Mapping):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_jsonable(item) for item in value]
    return value


def _digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _stat(path):
    value = path.stat()
    return {"bytes": value.st_size, "mtime_ns": value.st_mtime_ns}


class PreparedCache:
    @classmethod
    def open(cls, ctx):
        root = os.environ.get("PERF_BENCHMARK_PREPARED_CACHE")
        if not root:
            return None
        names = tuple(ctx.binding(stage).name for stage in STAGES)
        if names != ("hf-datasets", "open-swe-traces", "all"):
            raise ValueError("prepared cache requires local hf-datasets / open-swe-traces / all")
        loader = ctx.binding("loader").settings
        if loader.dataset not in {"json", "parquet"} or not isinstance(loader.data_files, str):
            raise ValueError("prepared cache requires one explicit local JSON or Parquet file")
        source = (ctx.base_dir / loader.data_files).resolve()
        package = Path(__file__).resolve().parents[2]
        # Include all preparation semantics and dependency versions; scheduler,
        # saver, output paths and concurrency deliberately do not affect reuse.
        directories = ["core", "pipeline/contracts", "pipeline/preparation",
                       "plugins/loader/hf_datasets", "plugins/normalization/open_swe_traces",
                       "plugins/selector/all"]
        code = {str(p.relative_to(package)): _digest(p)
                for directory in directories for p in sorted((package / directory).rglob("*.py"))}
        identity = {"format": 1, "seed": ctx.seed, "source": str(source),
                    "source_sha256": _digest(source), "source_stat": _stat(source),
                    "stages": {stage: {"plugin": ctx.binding(stage).name,
                                        "settings": _jsonable(ctx.binding(stage).settings)} for stage in STAGES},
                    "code": code,
                    "dependencies": {name: version(name) for name in ("pyarrow", "orjson", "datasets")}}
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        root = Path(root).resolve()
        root.mkdir(parents=True, exist_ok=True)
        cache = cls()
        cache.path, cache.identity = root / key, identity
        cache.lock = FileLock(str(root / (key + ".lock")), thread_local=False)
        cache.lock.acquire(timeout=0)
        cache.staging = None
        logger.info("prepared_cache lookup path=%s", cache.path)
        return cache

    def load(self, payloads, run_id):
        manifest = self.path / "manifest.json"
        if not manifest.exists():
            self.staging = Path(tempfile.mkdtemp(prefix=self.path.name + ".building-", dir=self.path.parent))
            payloads.use_file(self.staging / "payloads.bin")
            logger.info("prepared_cache miss; building persistent payloads path=%s", self.staging)
            return None
        data = json.loads(manifest.read_text())
        if data["identity"] != self.identity:
            raise ValueError("prepared cache identity mismatch")
        body, metadata = self.path / "payloads.bin", self.path / "selected.arrow"
        if _stat(body) != data["payload_stat"] or _digest(metadata) != data["metadata_sha256"]:
            raise ValueError("prepared cache artifact changed; refusing to reuse")
        with pa.memory_map(str(metadata), "r") as source:
            table = pa.ipc.open_file(source).read_all()
        if not table.schema.equals(SELECTED_SCHEMA) or table.num_rows != data["counts"]["selected_turns"]:
            raise ValueError("prepared cache metadata schema or row count mismatch")
        for name in ("request", "recorded_response", "extra_headers"):
            offsets, lengths = table[f"{name}_payload_offset"], table[f"{name}_payload_bytes"]
            if (pc.min(offsets).as_py() or 0) < 0 or (pc.min(lengths).as_py() or 0) < 0:
                raise ValueError("prepared cache has a negative payload reference")
            if (pc.max(pc.add(offsets, lengths)).as_py() or 0) > data["payload_stat"]["bytes"]:
                raise ValueError("prepared cache payload reference exceeds its artifact")
        # IDs in selected metadata belong to the producing Run. Rebind those
        # two columns; source IDs and immutable request bodies stay unchanged.
        for name in ("session_id", "turn_id"):
            column = pc.replace_substring_regex(table[name], pattern="^" + re.escape(data["run_id"]) + ":",
                                                replacement=run_id + ":")
            table = table.set_column(table.schema.get_field_index(name), name, column)
        table = table.cast(SELECTED_SCHEMA)
        payloads.use_file(body, existing=True)
        logger.info("prepared_cache hit sessions=%s turns=%s payload_bytes=%s path=%s",
                    data["counts"]["selected"], table.num_rows, data["payload_stat"]["bytes"], self.path)
        return tuple(table.to_batches(max_chunksize=65_536)), data["counts"]

    def publish(self, batches, counts, run_id):
        if self.staging is None:
            return
        metadata, body = self.staging / "selected.arrow", self.staging / "payloads.bin"
        with pa.OSFile(str(metadata), "wb") as stream:
            with pa.ipc.new_file(stream, SELECTED_SCHEMA, options=pa.ipc.IpcWriteOptions(compression="lz4")) as writer:
                for batch in batches:
                    writer.write_batch(batch)
        for path in (body, metadata):
            with path.open("rb") as stream:
                os.fsync(stream.fileno())
            path.chmod(0o444)
        data = {"identity": self.identity, "run_id": run_id, "counts": dict(counts),
                "payload_stat": _stat(body), "metadata_sha256": _digest(metadata)}
        manifest = self.staging / "manifest.json"
        with manifest.open("x") as stream:
            json.dump(data, stream, sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        # No cache is visible until complete metadata and the sealed body have
        # reached disk. A later client failure cannot destroy this artifact.
        os.rename(self.staging, self.path)
        self.staging = None
        descriptor = os.open(self.path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        logger.info("prepared_cache published path=%s", self.path)

    def close(self):
        try:
            if self.staging is not None:
                shutil.rmtree(self.staging)
        finally:
            self.lock.release()
