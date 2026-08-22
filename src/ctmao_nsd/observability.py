"""Failure-isolated event delivery and deterministic live state projection."""

from __future__ import annotations

import threading
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping
from uuid import UUID, uuid4

from .normalized_events import EventImportance, NormalizedEvent

EventCallback = Callable[[NormalizedEvent], None]


class _Subscriber:
    def __init__(self, callback: EventCallback, capacity: int) -> None:
        self.callback = callback
        self.capacity = capacity
        self.items: deque[NormalizedEvent] = deque()
        self.condition = threading.Condition()
        self.stopping = False
        self.dropped = 0
        self.failures = 0
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def enqueue(self, event: NormalizedEvent) -> None:
        with self.condition:
            if self.stopping:
                return
            if len(self.items) >= self.capacity:
                incoming_rank = _importance_rank(event.importance)
                candidates = [
                    index
                    for index, item in enumerate(self.items)
                    if _importance_rank(item.importance) < incoming_rank
                ]
                removable = (
                    min(
                        candidates,
                        key=lambda index: _importance_rank(
                            self.items[index].importance
                        ),
                    )
                    if candidates
                    else None
                )
                if removable is None:
                    self.dropped += 1
                    return
                del self.items[removable]
                self.dropped += 1
            self.items.append(event)
            self.condition.notify()

    def _run(self) -> None:
        while True:
            with self.condition:
                while not self.items and not self.stopping:
                    self.condition.wait()
                if self.stopping:
                    return
                event = self.items.popleft()
            try:
                self.callback(event)
            except Exception:
                self.failures += 1

    def close(self) -> None:
        with self.condition:
            self.stopping = True
            # Unsubscription is a stop boundary, not an unbounded drain. One
            # callback already in progress may finish, but queued callbacks
            # must not continue after their subscriber has been removed.
            self.dropped += len(self.items)
            self.items.clear()
            self.condition.notify_all()
        self.thread.join(timeout=1.0)


def _importance_rank(importance: EventImportance) -> int:
    return {
        EventImportance.DEBUG: 0,
        EventImportance.ROUTINE: 1,
        EventImportance.IMPORTANT: 2,
        EventImportance.CRITICAL: 3,
    }[importance]


class EventBus:
    """Best-effort non-blocking fan-out with bounded per-subscriber queues."""

    def __init__(self, replay_capacity: int = 512) -> None:
        if replay_capacity < 1:
            raise ValueError("replay_capacity must be positive")
        self._lock = threading.Lock()
        self._replay: deque[NormalizedEvent] = deque(maxlen=replay_capacity)
        self._subscribers: dict[UUID, _Subscriber] = {}
        self._closed = False
        # Publishing is part of the orchestrator's result-dispatch path.  Keep
        # that path independent of subscriber count by handing fan-out to one
        # bounded daemon thread.
        self._fanout_items: deque[NormalizedEvent] = deque()
        self._fanout_capacity = replay_capacity
        self._fanout_condition = threading.Condition()
        self._fanout_stopping = False
        self._fanout_dropped = 0
        self._fanout_thread = threading.Thread(
            target=self._run_fanout, name="ctmao-event-fanout", daemon=True
        )
        self._fanout_thread.start()

    def subscribe(self, callback: EventCallback, capacity: int = 128) -> UUID:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        subscriber = _Subscriber(callback, capacity)
        token = uuid4()
        with self._lock:
            if self._closed:
                subscriber.close()
                raise RuntimeError("event bus is closed")
            self._subscribers[token] = subscriber
        return token

    def unsubscribe(self, token: UUID) -> None:
        with self._lock:
            subscriber = self._subscribers.pop(token, None)
        if subscriber is not None:
            subscriber.close()

    def publish(self, event: NormalizedEvent) -> None:
        with self._lock:
            if self._closed:
                return
            self._replay.append(event)
            with self._fanout_condition:
                if len(self._fanout_items) >= self._fanout_capacity:
                    incoming_rank = _importance_rank(event.importance)
                    candidates = [
                        index
                        for index, item in enumerate(self._fanout_items)
                        if _importance_rank(item.importance) < incoming_rank
                    ]
                    removable = (
                        min(
                            candidates,
                            key=lambda index: _importance_rank(
                                self._fanout_items[index].importance
                            ),
                        )
                        if candidates
                        else None
                    )
                    if removable is None:
                        self._fanout_dropped += 1
                        return
                    del self._fanout_items[removable]
                    self._fanout_dropped += 1
                self._fanout_items.append(event)
                self._fanout_condition.notify()

    def _run_fanout(self) -> None:
        while True:
            with self._fanout_condition:
                while not self._fanout_items and not self._fanout_stopping:
                    self._fanout_condition.wait()
                if self._fanout_stopping:
                    return
                event = self._fanout_items.popleft()
            with self._lock:
                subscribers = tuple(self._subscribers.values())
            for subscriber in subscribers:
                subscriber.enqueue(event)

    def replay(self, after_sequence: int = 0) -> tuple[NormalizedEvent, ...]:
        with self._lock:
            return tuple(event for event in self._replay if event.sequence > after_sequence)

    def diagnostics(self) -> Mapping[str, int]:
        with self._lock:
            subscribers = tuple(self._subscribers.values())
        return MappingProxyType(
            {
                "subscribers": len(subscribers),
                "dropped": self._fanout_dropped
                + sum(item.dropped for item in subscribers),
                "failures": sum(item.failures for item in subscribers),
            }
        )

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            subscribers = tuple(self._subscribers.values())
            self._subscribers.clear()
        with self._fanout_condition:
            self._fanout_stopping = True
            self._fanout_dropped += len(self._fanout_items)
            self._fanout_items.clear()
            self._fanout_condition.notify_all()
        self._fanout_thread.join(timeout=1.0)
        for subscriber in subscribers:
            subscriber.close()


@dataclass(frozen=True, slots=True)
class ProjectionSnapshot:
    """Detached human-facing state derived only from normalized events."""

    session_id: UUID | None
    run_id: UUID | None
    run_status: str
    latest_sequence: int
    workers: Mapping[str, str]
    tasks: Mapping[str, Mapping[str, object]]
    timeline: tuple[Mapping[str, object], ...]


class EventProjection:
    """Thread-safe deterministic projection for dashboards and transcripts."""

    def __init__(self, timeline_limit: int = 500) -> None:
        self._lock = threading.Lock()
        self._timeline_limit = timeline_limit
        self._session_id: UUID | None = None
        self._run_id: UUID | None = None
        self._run_status = "idle"
        self._latest_sequence = 0
        self._event_ids: set[UUID] = set()
        self._workers: dict[str, str] = {}
        self._worker_sequences: dict[str, int] = {}
        self._tasks: dict[str, dict[str, object]] = {}
        self._timeline: deque[dict[str, object]] = deque(maxlen=timeline_limit)

    def apply(self, event: NormalizedEvent) -> None:
        with self._lock:
            starts_new_run = event.event_type == "orchestration.run.started"
            if self._session_id is not None and event.session_id != self._session_id:
                if not starts_new_run:
                    return
                self._reset_run()
            if (
                self._run_id is not None
                and event.run_id is not None
                and event.run_id != self._run_id
            ):
                if not starts_new_run:
                    return
                self._reset_run()
            if event.event_id in self._event_ids:
                return
            self._event_ids.add(event.event_id)
            self._session_id = event.session_id
            self._run_id = event.run_id or self._run_id
            self._latest_sequence = max(self._latest_sequence, event.sequence)
            if event.event_type == "orchestration.run.started":
                self._run_status = "running"
            elif event.event_type == "orchestration.run.completed":
                self._run_status = "completed"
            elif event.event_type in (
                "orchestration.run.failed",
                "orchestration.run.cancelled",
            ):
                self._run_status = event.event_type.rsplit(".", 1)[-1]
            if event.worker_id:
                previous = self._worker_sequences.get(event.worker_id, -1)
                if event.sequence >= previous:
                    self._worker_sequences[event.worker_id] = event.sequence
                    if event.event_type.endswith("worker.started"):
                        self._workers[event.worker_id] = "ready"
                    elif event.event_type.endswith("worker.failed"):
                        self._workers[event.worker_id] = "failed"
                    elif event.event_type.endswith("worker.stopped"):
                        self._workers[event.worker_id] = "stopped"
            if event.task_id:
                task = self._tasks.setdefault(str(event.task_id), {})
                if event.sequence >= int(task.get("sequence", -1)):
                    task.update(
                        {
                            "task_id": str(event.task_id),
                            "parent_task_id": str(event.parent_task_id)
                            if event.parent_task_id
                            else None,
                            "worker_id": event.worker_id,
                            "agent_path": list(event.agent_path),
                            "summary": event.summary,
                            "status": event.status
                            or event.event_type.rsplit(".", 1)[-1],
                            "sequence": event.sequence,
                        }
                    )
            self._timeline.append(
                {
                    "sequence": event.sequence,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "occurred_at": event.occurred_at,
                    "summary": event.summary,
                    "worker_id": event.worker_id,
                    "task_id": str(event.task_id) if event.task_id else None,
                }
            )

    def _reset_run(self) -> None:
        self._run_status = "idle"
        self._latest_sequence = 0
        self._event_ids.clear()
        self._workers.clear()
        self._worker_sequences.clear()
        self._tasks.clear()
        self._timeline.clear()

    def snapshot(self) -> ProjectionSnapshot:
        with self._lock:
            return ProjectionSnapshot(
                self._session_id,
                self._run_id,
                self._run_status,
                self._latest_sequence,
                MappingProxyType(dict(self._workers)),
                MappingProxyType(
                    {
                        key: MappingProxyType(dict(value))
                        for key, value in self._tasks.items()
                    }
                ),
                tuple(MappingProxyType(dict(item)) for item in self._timeline),
            )
