"""Provider-neutral, failure-isolated asynchronous speech delivery."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .conference import ConferenceTurn, VoiceAssignment
from .normalized_events import EventImportance, redact_text


@dataclass(frozen=True, slots=True)
class SpeechAudio:
    """Opaque synthesized bytes plus their declared media type."""

    data: bytes
    media_type: str = "audio/wav"


@runtime_checkable
class SpeechProvider(Protocol):
    """Asynchronous adapter contract for text-to-speech providers."""

    async def synthesize(
        self, turn: ConferenceTurn, voice: VoiceAssignment
    ) -> SpeechAudio | None:
        """Return synthesized audio, or ``None`` when speech is disabled."""
        ...


@runtime_checkable
class AudioSink(Protocol):
    """Asynchronous destination for synthesized conference audio."""

    async def play(self, audio: SpeechAudio, turn: ConferenceTurn) -> None:
        """Play or persist one synthesized turn."""
        ...


class NullSpeechProvider:
    """Safe default provider that performs no synthesis or external I/O."""

    async def synthesize(
        self, turn: ConferenceTurn, voice: VoiceAssignment
    ) -> SpeechAudio | None:
        del turn, voice
        return None


@dataclass(frozen=True, slots=True)
class SpeechFailure:
    """Recorded provider/sink failure that never escapes the speaker worker."""

    turn_id: str
    stage: str
    error: str


@dataclass(frozen=True, slots=True)
class _SpeechJob:
    turn: ConferenceTurn
    voice: VoiceAssignment
    generation: int
    dedupe_key: str
    supersession_key: str | None
    supersession_version: int


class AsyncSpeakerQueue:
    """Bounded speech queue with stale-work invalidation and failure isolation."""

    def __init__(
        self,
        provider: SpeechProvider,
        sink: AudioSink,
        *,
        max_pending: int = 32,
        synthesis_timeout: float = 30.0,
        sink_timeout: float = 30.0,
        drain_timeout: float = 5.0,
    ) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be at least 1")
        if synthesis_timeout <= 0:
            raise ValueError("synthesis_timeout must be positive")
        if sink_timeout <= 0 or drain_timeout <= 0:
            raise ValueError("sink and drain timeouts must be positive")
        self._provider = provider
        self._sink = sink
        self._queue: asyncio.Queue[_SpeechJob] = asyncio.Queue(maxsize=max_pending)
        self._synthesis_timeout = synthesis_timeout
        self._sink_timeout = sink_timeout
        self._drain_timeout = drain_timeout
        self._generation = 0
        self._accepted_dedupe: set[str] = set()
        self._supersession_versions: dict[str, int] = {}
        self._failures: list[SpeechFailure] = []
        self._worker: asyncio.Task[None] | None = None
        self._closed = False

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def failures(self) -> tuple[SpeechFailure, ...]:
        return tuple(self._failures)

    @property
    def pending(self) -> int:
        return self._queue.qsize()

    async def start(self) -> None:
        if self._closed:
            raise RuntimeError("speaker queue is closed")
        if self._worker is None:
            self._worker = asyncio.create_task(
                self._run(), name="ctmao-conference-speaker"
            )

    def enqueue(
        self,
        turn: ConferenceTurn,
        voice: VoiceAssignment,
        *,
        dedupe_key: str | None = None,
        supersession_key: str | None = None,
    ) -> bool:
        """Queue without blocking; return false for muted, duplicate, or full work."""
        if self._closed:
            raise RuntimeError("speaker queue is closed")
        if voice.muted or voice.voice_id is None:
            return False
        key = dedupe_key or "source:" + ",".join(
            sorted(str(source_id) for source_id in turn.source_event_ids)
        )
        if key in self._accepted_dedupe:
            return False
        version = 0
        if supersession_key is not None:
            version = self._supersession_versions.get(supersession_key, 0) + 1
            self._supersession_versions[supersession_key] = version
        job = _SpeechJob(
            turn,
            voice,
            self._generation,
            key,
            supersession_key,
            version,
        )
        try:
            self._queue.put_nowait(job)
        except asyncio.QueueFull:
            if self._evict_for(job):
                self._queue.put_nowait(job)
            else:
                # A rejected item must be eligible for a later retry.
                if supersession_key is not None:
                    self._supersession_versions[supersession_key] = version - 1
                return False
        self._accepted_dedupe.add(key)
        return True

    def invalidate_generation(self) -> int:
        """Invalidate queued/in-flight speech and begin a fresh dedupe generation."""
        self._generation += 1
        self._accepted_dedupe.clear()
        self._supersession_versions.clear()
        return self._generation

    async def wait_idle(self, timeout: float | None = None) -> None:
        await asyncio.wait_for(
            self._queue.join(), timeout=timeout or self._drain_timeout
        )

    async def close(self, *, drain: bool = True) -> None:
        if self._closed:
            return
        self._closed = True
        if drain:
            if self._worker is None and self.pending:
                self._worker = asyncio.create_task(
                    self._run(), name="ctmao-conference-speaker"
                )
            try:
                await self.wait_idle()
            except asyncio.TimeoutError:
                self.invalidate_generation()
                self._discard_pending()
        else:
            self.invalidate_generation()
            self._discard_pending()
        if self._worker is not None:
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
            self._worker = None

    def _evict_for(self, incoming: _SpeechJob) -> bool:
        retained: list[_SpeechJob] = []
        removed: _SpeechJob | None = None
        while True:
            try:
                candidate = self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self._queue.task_done()
            if removed is None and self._is_stale(candidate):
                removed = candidate
                continue
            retained.append(candidate)
        if removed is None:
            incoming_rank = _importance_rank(incoming.turn.importance)
            for index, candidate in enumerate(retained):
                if _importance_rank(candidate.turn.importance) < incoming_rank:
                    removed = retained.pop(index)
                    break
        for candidate in retained:
            self._queue.put_nowait(candidate)
        return removed is not None

    def _is_stale(self, job: _SpeechJob) -> bool:
        if job.generation != self._generation:
            return True
        if job.supersession_key is None:
            return False
        return self._supersession_versions.get(job.supersession_key) != job.supersession_version

    async def _run(self) -> None:
        while True:
            job = await self._queue.get()
            try:
                if self._is_stale(job):
                    continue
                try:
                    audio = await asyncio.wait_for(
                        self._provider.synthesize(job.turn, job.voice),
                        timeout=self._synthesis_timeout,
                    )
                except Exception as exc:
                    self._record_failure(job, "provider", exc)
                    continue
                if audio is None or self._is_stale(job):
                    continue
                try:
                    await asyncio.wait_for(
                        self._sink.play(audio, job.turn), timeout=self._sink_timeout
                    )
                except Exception as exc:
                    self._record_failure(job, "sink", exc)
            finally:
                self._queue.task_done()

    def _record_failure(self, job: _SpeechJob, stage: str, exc: Exception) -> None:
        safe_error, _ = redact_text(str(exc))
        self._failures.append(
            SpeechFailure(job.turn.turn_id, stage, f"{type(exc).__name__}: {safe_error}")
        )

    def _discard_pending(self) -> None:
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            else:
                self._queue.task_done()


def _importance_rank(importance: EventImportance) -> int:
    return {
        EventImportance.DEBUG: 0,
        EventImportance.ROUTINE: 1,
        EventImportance.IMPORTANT: 2,
        EventImportance.CRITICAL: 3,
    }[importance]
