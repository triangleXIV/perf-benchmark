"""Finite source DAG replay through complete Session runners and bounded admission."""
import asyncio
from time import perf_counter_ns

from perf_benchmark.core.validation import positive_int, positive_number
from perf_benchmark.pipeline.contracts import (
    GraphCoverage, LoadProfile, NextTurn, ResourceLimits, SessionAdmission,
    StopPlan, TrialPlan, TrialSchedulingFact, TurnEligibility,
)
from perf_benchmark.pipeline.scheduling.mechanics import StandardTurnStrategy, parse_common_settings
from perf_benchmark.pipeline.contracts.scheduler import SchedulerPlugin
from perf_benchmark.plugins import register_plugin


def parse_settings(settings):
    if set(settings) - {"max_active_sessions", "max_concurrency", "max_pending_requests", "time_scale"}:
        raise ValueError("causal replay accepts resource limits and time_scale only; no retries or cycling")
    parsed = dict(settings)
    for key in ("max_active_sessions", "max_concurrency"):
        positive_int(parsed.get(key), key)
    positive_int(parsed.setdefault("max_pending_requests", parsed["max_concurrency"]), "max_pending_requests")
    positive_number(parsed.setdefault("time_scale", 1.0), "time_scale")
    return parsed


class Graph:
    def __init__(self, sessions, origin_ns, scale):
        self.sessions, self.origin_ns, self.scale = sessions, origin_ns, scale
        self.nodes = {turn.causal.node_id: turn.causal for session in sessions for turn in session.turns}
        self.events, self.changed = {}, asyncio.Event()
        self.start_sources = {edge.node_id for node in self.nodes.values() for edge in node.dependencies if edge.event == "started"}
        self.blocked = set()

    def publish(self, node, event, at):
        key = (node, event)
        if key in self.events and self.events[key] != at:
            raise RuntimeError("causal event published twice with different facts")
        self.events[key] = at
        self.changed.set()

    async def wait(self, node):
        entered = perf_counter_ns()
        while True:
            self.changed.clear()
            if any((edge.node_id, edge.event) in self.events and self.events[edge.node_id, edge.event] is None for edge in node.dependencies):
                self.blocked.add(node.node_id)
                return TurnEligibility(reason="dependency_unsatisfied")
            if all((edge.node_id, edge.event) in self.events for edge in node.dependencies):
                break
            await self.changed.wait()
        source_floor = not node.dependencies or any(edge.delay_ns is None for edge in node.dependencies)
        target = self.origin_ns + (round((node.not_before_ns-node.root_ns)/self.scale) if source_floor else 0)
        readiness = []
        for edge in node.dependencies:
            at = self.events[edge.node_id, edge.event]
            readiness.append(at)
            target = max(target, at + round(edge.delay_ns/self.scale) if edge.delay_ns is not None else at)
        dependency_ready = max(entered, *readiness) if readiness else None
        await asyncio.sleep(max(0, (target-perf_counter_ns())/1e9))
        return TurnEligibility(target, dependency_ready)

    def finish_branch(self, session):
        for turn in session.turns:
            node = turn.causal.node_id
            for event in ("started", "completed"):
                if (node, event) not in self.events:
                    self.publish(node, event, None)

    def coverage(self, reason):
        first = next(iter(self.nodes.values()))
        completed = sum(self.events.get((node, "completed")) is not None for node in self.nodes)
        started = sum(self.events.get((node, "started")) is not None for node in self.nodes)
        return GraphCoverage(first.graph_id, len(self.nodes), started, completed,
                             len(self.nodes)-completed, reason if completed < len(self.nodes) else None)


class CausalStrategy(StandardTurnStrategy):
    def __init__(self, plan, settings, seed):
        common = parse_common_settings({}, allowed=set())
        super().__init__(common, plan, perf_counter_ns(), seed=seed, reuse_session_arrival=True)
        self.graphs = {}

    async def wait_turn(self, turn):
        return await self.graphs[turn.causal.graph_id].wait(turn.causal)

    async def run_turn(self, turn, control):
        graph, node = self.graphs[turn.causal.graph_id], turn.causal.node_id
        if node in graph.start_sources:
            control.observe_started(lambda at: graph.publish(node, "started", at))
        return await control.send()

    def next_turn(self, turn, result):
        graph, node = self.graphs[turn.causal.graph_id], turn.causal.node_id
        if (node, "started") not in graph.events:
            graph.publish(node, "started", result.request.started_ns)
        graph.publish(node, "completed", result.request.response_completed_ns or perf_counter_ns())
        return NextTurn(True)


@register_plugin(stage="scheduler", name="trace-causal", settings_parser=parse_settings,
                 description="Source dependency replay with atomic graph reservations")
class TraceCausalScheduler(SchedulerPlugin):
    def __init__(self, settings):
        self.settings = settings

    async def schedule(self, trials, ctx):
        settings = self.settings
        limit, concurrency = settings["max_active_sessions"], settings["max_concurrency"]
        plan = TrialPlan("trace-causal", StopPlan(), ResourceLimits(concurrency, limit, settings["max_pending_requests"]),
                         LoadProfile("trace-causal", max_active_sessions=limit, max_concurrency=concurrency, arrival_distribution="timeline"))
        strategy = CausalStrategy(plan, settings, ctx.seed)
        async with trials.open(plan, strategy) as execution:
            strategy.start_clock(perf_counter_ns())
            strategy.cancellation = ctx.cancellation
            await self._run(execution, strategy, ctx)

    async def _run(self, execution, strategy, ctx):
        runner, settings = execution.runner, self.settings
        limit, scale = settings["max_active_sessions"], settings["time_scale"]
        active, coverage = {}, []
        reserved = admitted = completed = 0
        origin, first_root = perf_counter_ns(), None
        reason = "source_exhausted"

        async def run_graph(sessions, due):
            nonlocal completed
            graph = Graph(sessions, due, scale)
            key = sessions[0].turns[0].causal.graph_id
            strategy.graphs[key] = graph
            async def branch(session):
                nonlocal completed
                try:
                    await runner.run(session, SessionAdmission(due, perf_counter_ns()))
                finally:
                    completed += int(session.status != "pending")
                    graph.finish_branch(session)
            try:
                async with asyncio.TaskGroup() as group:
                    for session in sessions:
                        group.create_task(branch(session))
            finally:
                coverage.append(graph.coverage(ctx.cancellation.reason if ctx.cancellation.is_cancelled() else "dependency_unsatisfied"))
                del strategy.graphs[key]

        async def reap():
            nonlocal reserved
            done, _ = await asyncio.wait(active, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                reserved -= active.pop(task)
                await runner.record_queue("reserved_sessions", reserved)
                await task

        async def submit(sessions):
            nonlocal reserved, admitted, first_root
            if not sessions:
                return
            if ctx.cancellation.is_cancelled():
                coverage.append(GraphCoverage(sessions[0].turns[0].causal.graph_id, sum(len(s.turns) for s in sessions),
                                              0, 0, sum(len(s.turns) for s in sessions), ctx.cancellation.reason))
                await asyncio.sleep(0)
                return
            node = sessions[0].turns[0].causal
            if len(sessions) > limit:
                raise ValueError("one graph exceeds max_active_sessions")
            if first_root is None:
                first_root = node.root_ns
            due = origin + round((node.root_ns-first_root)/scale)
            while reserved + len(sessions) > limit:
                await reap()
            if ctx.cancellation.is_cancelled():
                coverage.append(GraphCoverage(node.graph_id, sum(len(s.turns) for s in sessions),
                                              0, 0, sum(len(s.turns) for s in sessions), ctx.cancellation.reason))
                return
            reserved += len(sessions)
            admitted += len(sessions)
            await runner.record_queue("reserved_sessions", reserved)
            active[asyncio.create_task(run_graph(sessions, due))] = len(sessions)

        buffered, graph_id = [], None
        try:
            async for session in execution.sessions:
                node = session.turns[0].causal
                if node is None or any(turn.causal is None or turn.causal.graph_id != node.graph_id for turn in session.turns):
                    raise ValueError("causal Session must belong to exactly one source graph")
                if graph_id is not None and graph_id != node.graph_id:
                    await submit(buffered)
                    buffered = []
                graph_id = node.graph_id
                buffered.append(session)
            await submit(buffered)
            while active:
                await reap()
        finally:
            if active:
                for task in active:
                    task.cancel()
                await asyncio.gather(*active, return_exceptions=True)
            await runner.record_queue("reserved_sessions", 0)
            runner.record_scheduling(TrialSchedulingFact(
                offered_sessions=admitted, admitted_sessions=completed, completed_sessions=completed,
                offered_attempts=strategy.attempts.offered, admitted_attempts=strategy.attempts.admitted,
                completed_attempts=strategy.attempts.completed, offering_started_ns=origin,
                request_offering_finished_ns=strategy.attempts.last_offered_ns,
                termination_reason=ctx.cancellation.reason if ctx.cancellation.is_cancelled() else reason, demand_exhausted=not ctx.cancellation.is_cancelled(),
                graph_coverage=tuple(coverage)))
