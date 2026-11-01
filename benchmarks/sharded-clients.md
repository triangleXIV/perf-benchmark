# Parallel CPU clients with pooled key metrics

`sharded_clients.py` reuses the native Executor, Scheduler and Metrics pipeline in
separate CPU processes. Each process selects `corpus.plans[index::workers]` from
one prepared corpus, so sessions are disjoint and turns within a session remain
sequential. Causal graphs are rejected. All workers use the same cache salt.

Create a **new** absolute working directory for the coordination files and a new
output directory. A `spec.json` contains:

```json
{
  "directory": "/absolute/path/to/coordination",
  "output": "/absolute/path/to/results",
  "workdir": "/absolute/path/to/project",
  "workers": 8,
  "concurrency": 7480,
  "active_users": 12000,
  "cache_salt": "fresh-random-value-shared-by-these-workers",
  "observer_base_port": 28200
}
```

In the coordination directory, write `worker-0.json` through `worker-7.json` using
your ordinary Run configuration, with these per-worker settings:

- `scheduler`: `multi-turn-closed-loop`, C935, A1500, `stop.session_count=1500`,
  `on_exhausted=error`. Other sizes must partition the selected session count and
  configured global concurrency correctly.
- `saver`: `summary_only=true`, path `output/worker-N/results.db`. Create each
  worker's output directory before starting it.
- `metrics`: `standard`; retain your existing metric/pricing settings. Allocate
  memory per process, including the prepared metadata and Metrics DuckDB budget.
- `observation`: enabled, benchmark mode, trace export disabled,
  `port=observer_base_port+N`. Use a different port for each worker.
- Preserve the same loader, normalization, selector, seed and prepared cache
  identity in every worker. A shared preparation lock can serialize cache loads;
  independent read-only aliases of the same sealed entry, with their own existing
  lock files, allow concurrent loads without copying payload bytes.

With the package and dependencies installed, start each CPU process with
`python benchmarks/sharded_clients.py --spec /absolute/path/to/spec.json --worker N`
and one observer with `--aggregate` instead of `--worker N`. Apply CPU/memory
limits in the launching environment; the example does not reserve resources or
manage serving processes. Never overlap a GPU benchmark with GPU validation jobs.

Every worker writes `worker-N.ready.json` and then waits without inference warmup.
Check that the source ordinal union is complete, disjoint, and has the intended
session/turn counts. Clear the **serving** KV, external-store and routing caches
at this boundary when measuring a cold start; retain prepared CPU request bodies.
Then atomically publish `go.json` in the coordination directory to release the
workers together. The caller owns health checks and cold-boundary evidence.

The aggregate observer prints actual summed concurrency, counts, rolling completed
output throughput, and TTFT/TPOT/E2E means pooled by valid sample counts. Missing
samples remain unknown; the log is a low-frequency preview. Summary-only workers
still compute all standard metric windows and distributions. Their small SQLite
files and final JSON summaries retain those metrics, counts and bounded errors,
without Session/Turn/Attempt detail rows.

At completion, compact numeric samples produce exact **pooled** p50/p90/p99 and
global concurrency. Averaging per-worker percentiles is incorrect. Numeric samples
contain timestamps/latencies/usage and occupancy, without request/response bodies,
headers, or request IDs. Each shard retains its complete standard maxTPM and other
window metrics. The current global summary does not reconstruct a synchronized
maxTPM window; do not sum shard peaks or present the global summary as a full
replacement for the per-shard reports.

The compact StandardMetrics tables use milliseconds for TTFT/TPOT/E2E/ITL;
the global JSON converts them to explicitly named seconds. Native per-shard
reports retain their original public units (for example TPOT/ITL in ms).
Inverse ITL is labeled events/s: a protocol output event can contain multiple
speculative tokens, so it cannot be advertised as per-user token throughput.
