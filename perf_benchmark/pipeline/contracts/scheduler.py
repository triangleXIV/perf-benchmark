"""Scheduler Plugin interface and typed scheduling plans."""

from __future__ import annotations

from perf_benchmark.core.validation import positive_int, positive_number

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol

from perf_benchmark.core.models import JsonValue, Session, Turn, freeze_snapshot
from perf_benchmark.pipeline.context import SchedulerContext
from perf_benchmark.pipeline.contracts.results import MetricPoint
from perf_benchmark.pipeline.contracts.sweep import SweepRecord, TrialValidity
from perf_benchmark.pipeline.contracts.facts import (
    ComparisonPoint,
    LoadProfile,
    RepetitionPlan,
    ResourceLimits,
    TrialSchedulingFact,
    ValidityRequirements,
)
from perf_benchmark.pipeline.contracts.executor import (
    AttemptPlan,
    AttemptResult,
    ExecutorTrial,
)


class SchedulingStopped(Exception):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


@dataclass(frozen=True, slots=True)
class NextTurn:
    proceed: bool
    retain_partial: bool = False
    cancel_reason: str | None = None

@dataclass(frozen=True, slots=True)
class StopPlan:
    session_count: int | None = None
    duration_s: float | None = None
    on_exhausted: str = "error"

    def __post_init__(self) -> None:
        if self.session_count is not None and self.duration_s is not None:
            raise ValueError("stop must contain exactly one of session_count or duration_s")
        positive_int(self.session_count, "stop.session_count", optional=True)
        positive_number(self.duration_s, "stop.duration_s", optional=True)
        if self.on_exhausted not in {"cycle", "error", "stop"}:
            raise ValueError("on_exhausted must be cycle, error or stop")
        if self.session_count is None and self.duration_s is None and self.on_exhausted != "error":
            raise ValueError(f"on_exhausted={self.on_exhausted} requires an explicit stop")


@dataclass(frozen=True, slots=True)
class TrialPlan:
    name: str
    stop: StopPlan
    resources: ResourceLimits
    load_profile: LoadProfile
    comparison: ComparisonPoint | None = None
    repetition: RepetitionPlan = field(default_factory=RepetitionPlan)
    request_parameters: Mapping[str, object] = field(
        default_factory=lambda: MappingProxyType({})
    )
    metadata: Mapping[str, JsonValue] = field(default_factory=lambda: MappingProxyType({}))
    validity_requirements: ValidityRequirements | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not self.name:
            raise ValueError("TrialPlan.name must not be empty")
        if not isinstance(self.request_parameters, Mapping):
            raise TypeError("TrialPlan.request_parameters must be a mapping")
        if not isinstance(self.metadata, Mapping):
            raise TypeError("TrialPlan.metadata must be a mapping")
        object.__setattr__(self, "request_parameters", freeze_snapshot(self.request_parameters))
        object.__setattr__(self, "metadata", freeze_snapshot(self.metadata))


@dataclass(frozen=True, slots=True)
class EffectiveTrial:
    id: str
    name: str
    plan: TrialPlan
    request_parameters: Mapping[str, object]
    metadata: Mapping[str, JsonValue]

    def __post_init__(self) -> None:
        if not self.id or not self.name:
            raise ValueError("EffectiveTrial id and name must not be empty")
        object.__setattr__(
            self, "request_parameters", freeze_snapshot(self.request_parameters)
        )
        object.__setattr__(self, "metadata", freeze_snapshot(self.metadata))

    def executor_trial(self) -> ExecutorTrial:
        return ExecutorTrial(self.id, self.request_parameters, self.metadata)


@dataclass(frozen=True, slots=True)
class SessionAdmission:
    target_ns: int
    offered_ns: int


@dataclass(frozen=True, slots=True)
class AttemptAdmission:
    target_ns: int
    offered_ns: int
    admitted_ns: int


class AttemptTicket(Protocol):
    target_ns: int
    offered_ns: int


class AttemptLease(Protocol):
    target_ns: int
    offered_ns: int
    admitted_ns: int

    def release(self) -> None: ...


class AttemptGate(Protocol):
    async def offer(self, preset: SessionAdmission | None = None) -> AttemptTicket: ...
    async def admit(self, ticket: AttemptTicket) -> AttemptLease: ...
    async def cancel(self, ticket: AttemptTicket) -> None: ...


class TurnControl(Protocol):
    result: AttemptResult | None
    stop_reason: str | None

    @property
    def session_ordinal(self) -> int: ...

    async def send(self, plan: AttemptPlan | None = None) -> AttemptResult | None: ...
    async def retry(
        self, delay_s: float, plan: AttemptPlan | None = None
    ) -> AttemptResult | None:
        """Resend a safe completed success/failure with the same pre-Turn history."""
        ...
    def cancel(self, reason: str) -> None: ...
    def observe_started(self, callback) -> None: ...


@dataclass(frozen=True, slots=True)
class TurnEligibility:
    target_ns: int | None = None
    dependency_ready_ns: int | None = None
    reason: str | None = None


class TurnStrategy(Protocol):
    def begin_turn(self, turn: Turn) -> str | None: ...
    async def wait_turn(self, turn: Turn) -> TurnEligibility: ...
    async def run_turn(self, turn: Turn, control: TurnControl) -> AttemptResult | None: ...
    def next_turn(self, turn: Turn, result: AttemptResult) -> NextTurn: ...
    def plan_attempt(self, turn: Turn, attempt: int, *, session_ordinal: int) -> AttemptPlan: ...
    @property
    def attempts(self) -> AttemptGate: ...


class SessionRunner(Protocol):
    async def run(self, session: Session, admission: SessionAdmission) -> None: ...
    async def cancel(self, session: Session, reason: str) -> None: ...
    async def record_queue(self, kind: str, value: int) -> None: ...
    def record_scheduling(self, fact: TrialSchedulingFact) -> None: ...
    async def worker_started(self, index: int) -> None: ...
    async def worker_stopped(self, index: int) -> None: ...


SessionStream = AsyncIterator[Session]


@dataclass(frozen=True, slots=True)
class TrialOutcome:
    trial_id: str
    status: str
    scheduling: TrialSchedulingFact
    points: tuple[MetricPoint, ...]
    validity: TrialValidity | None = None


@dataclass(slots=True)
class TrialExecution:
    sessions: SessionStream
    runner: SessionRunner
    _outcome: TrialOutcome | None = field(default=None, init=False)

    @property
    def outcome(self) -> TrialOutcome:
        if self._outcome is None:
            raise RuntimeError("Trial outcome is unavailable before successful finalization and commit")
        return self._outcome


class TrialHost(Protocol):
    async def record_sweep(self, record: SweepRecord) -> None: ...

    def open(
        self,
        plan: TrialPlan,
        strategy: TurnStrategy,
    ) -> AbstractAsyncContextManager[TrialExecution]: ...


class SchedulerPlugin(ABC):
    def validate_metrics(self, metrics) -> None:
        """Validate cross-stage requirements before any Run resources are opened."""

    @abstractmethod
    async def schedule(self, trials: TrialHost, ctx: SchedulerContext) -> None: ...
