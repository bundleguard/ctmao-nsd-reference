"""Tests for bounded, failure-isolated asynchronous speech delivery."""

from __future__ import annotations

import asyncio
import unittest
from uuid import UUID

from ctmao_nsd.conference import ConferenceTurn, VoiceAssignment
from ctmao_nsd.normalized_events import EvidenceKind
from ctmao_nsd.normalized_events import EventImportance
from ctmao_nsd.speech import AsyncSpeakerQueue, NullSpeechProvider, SpeechAudio


SESSION = UUID("00000000-0000-4000-8000-000000000001")


def turn(number: int) -> ConferenceTurn:
    source = UUID(f"00000000-0000-4000-8000-{number:012d}")
    return ConferenceTurn(
        f"turn-{number}", SESSION, "worker:A", f"message {number}", (source,),
        EvidenceKind.OBSERVED, number,
    )


class RecordingProvider:
    def __init__(self, gate: asyncio.Event | None = None) -> None:
        self.turns: list[str] = []
        self.gate = gate

    async def synthesize(self, item: ConferenceTurn, voice: VoiceAssignment) -> SpeechAudio:
        self.turns.append(item.turn_id)
        if self.gate is not None:
            await self.gate.wait()
        return SpeechAudio(item.text.encode())


class RecordingSink:
    def __init__(self) -> None:
        self.turns: list[str] = []

    async def play(self, audio: SpeechAudio, item: ConferenceTurn) -> None:
        self.turns.append(item.turn_id)


class FailingProvider:
    async def synthesize(self, item: ConferenceTurn, voice: VoiceAssignment) -> SpeechAudio:
        raise RuntimeError("provider unavailable")


class HangingSink:
    async def play(self, audio: SpeechAudio, item: ConferenceTurn) -> None:
        await asyncio.Event().wait()


class SpeechTests(unittest.IsolatedAsyncioTestCase):
    async def test_null_provider_and_muted_voice_emit_no_audio(self) -> None:
        sink = RecordingSink()
        queue = AsyncSpeakerQueue(NullSpeechProvider(), sink)
        await queue.start()
        self.assertFalse(queue.enqueue(turn(1), VoiceAssignment("worker:A", None, muted=True)))
        self.assertTrue(queue.enqueue(turn(2), VoiceAssignment("worker:A", "voice")))
        await queue.wait_idle()
        await queue.close()
        self.assertEqual(sink.turns, [])

    async def test_dedupe_and_supersession(self) -> None:
        provider = RecordingProvider()
        sink = RecordingSink()
        queue = AsyncSpeakerQueue(provider, sink)
        old = turn(1)
        new = turn(2)
        self.assertTrue(queue.enqueue(old, VoiceAssignment("worker:A", "voice"), supersession_key="status"))
        self.assertFalse(queue.enqueue(old, VoiceAssignment("worker:A", "voice")))
        self.assertTrue(queue.enqueue(new, VoiceAssignment("worker:A", "voice"), supersession_key="status"))
        await queue.start()
        await queue.wait_idle()
        await queue.close()
        self.assertEqual(sink.turns, ["turn-2"])

    async def test_equal_source_sets_are_deduplicated(self) -> None:
        provider = RecordingProvider()
        sink = RecordingSink()
        queue = AsyncSpeakerQueue(provider, sink)
        first = turn(1)
        restyled = ConferenceTurn(
            "different-turn-id", SESSION, "worker:A", "same fact restyled",
            first.source_event_ids, EvidenceKind.DERIVED, 2,
        )
        self.assertTrue(queue.enqueue(first, VoiceAssignment("worker:A", "voice")))
        self.assertFalse(queue.enqueue(restyled, VoiceAssignment("worker:A", "voice")))
        await queue.start()
        await queue.close()

    async def test_generation_invalidation_suppresses_inflight_audio(self) -> None:
        gate = asyncio.Event()
        provider = RecordingProvider(gate)
        sink = RecordingSink()
        queue = AsyncSpeakerQueue(provider, sink)
        await queue.start()
        queue.enqueue(turn(1), VoiceAssignment("worker:A", "voice"))
        await asyncio.sleep(0)
        queue.invalidate_generation()
        gate.set()
        await queue.wait_idle()
        await queue.close()
        self.assertEqual(sink.turns, [])

    async def test_provider_failure_is_recorded_and_later_work_continues(self) -> None:
        sink = RecordingSink()
        queue = AsyncSpeakerQueue(FailingProvider(), sink)
        await queue.start()
        queue.enqueue(turn(1), VoiceAssignment("worker:A", "voice"))
        queue.enqueue(turn(2), VoiceAssignment("worker:A", "voice"))
        await queue.wait_idle()
        await queue.close()
        self.assertEqual(len(queue.failures), 2)
        self.assertTrue(all(failure.stage == "provider" for failure in queue.failures))

    async def test_queue_is_bounded_and_nonblocking(self) -> None:
        queue = AsyncSpeakerQueue(NullSpeechProvider(), RecordingSink(), max_pending=1)
        self.assertTrue(queue.enqueue(turn(1), VoiceAssignment("worker:A", "voice")))
        self.assertFalse(queue.enqueue(turn(2), VoiceAssignment("worker:A", "voice")))
        await queue.start()
        await queue.close()

    async def test_critical_turn_evicts_queued_routine_turn(self) -> None:
        provider = RecordingProvider()
        sink = RecordingSink()
        queue = AsyncSpeakerQueue(provider, sink, max_pending=1)
        routine = turn(1)
        critical = ConferenceTurn(
            "turn-2", SESSION, "worker:A", "failure", turn(2).source_event_ids,
            EvidenceKind.OBSERVED, 2, importance=EventImportance.CRITICAL,
        )
        self.assertTrue(queue.enqueue(routine, VoiceAssignment("worker:A", "voice")))
        self.assertTrue(queue.enqueue(critical, VoiceAssignment("worker:A", "voice")))
        await queue.start()
        await queue.wait_idle()
        await queue.close()
        self.assertEqual(sink.turns, ["turn-2"])

    async def test_hanging_sink_cannot_block_close(self) -> None:
        queue = AsyncSpeakerQueue(
            RecordingProvider(), HangingSink(), sink_timeout=0.02, drain_timeout=0.1
        )
        await queue.start()
        queue.enqueue(turn(1), VoiceAssignment("worker:A", "voice"))
        await asyncio.wait_for(queue.close(), timeout=0.5)
        self.assertEqual(queue.failures[0].stage, "sink")

    async def test_provider_failure_text_is_redacted(self) -> None:
        class SecretProvider:
            async def synthesize(self, item, voice):
                raise RuntimeError("Bearer very-secret-token")

        queue = AsyncSpeakerQueue(SecretProvider(), RecordingSink())
        await queue.start()
        queue.enqueue(turn(1), VoiceAssignment("worker:A", "voice"))
        await queue.wait_idle()
        await queue.close()
        self.assertNotIn("very-secret-token", queue.failures[0].error)


if __name__ == "__main__":
    unittest.main()
