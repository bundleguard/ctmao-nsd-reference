"""Optional OpenAI Responses API provider contract tests without network access."""

from __future__ import annotations

import json
import unittest
from uuid import uuid4

from ctmao_nsd import ModelRequest, OpenAIProviderError, OpenAIResponsesProvider


class OpenAIProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_sends_bounded_non_stored_request_and_extracts_public_text(self) -> None:
        captured = {}

        def transport(request: object, timeout: float) -> bytes:
            captured["authorization"] = request.headers["Authorization"]
            captured["payload"] = json.loads(request.data)
            captured["timeout"] = timeout
            return json.dumps(
                {
                    "status": "completed",
                    "output": [
                        {"type": "reasoning", "summary": "private"},
                        {
                            "type": "message",
                            "content": [
                                {"type": "output_text", "text": "Public answer"}
                            ],
                        },
                    ],
                }
            ).encode()

        provider = OpenAIResponsesProvider(
            "test-key",
            model="test-model",
            transport=transport,
            role_instructions={"Researcher": "Check evidence."},
        )
        response = await provider.complete(
            ModelRequest(uuid4(), "Analyze", "Objective", "Researcher", ("agent",))
        )
        self.assertEqual(response.output, "Public answer")
        self.assertEqual(response.provider, "openai")
        self.assertFalse(captured["payload"]["store"])
        self.assertNotIn("private", response.output)
        self.assertEqual(captured["authorization"], "Bearer test-key")

    async def test_invalid_or_incomplete_response_fails_without_raw_body(self) -> None:
        provider = OpenAIResponsesProvider(
            "test-key", transport=lambda request, timeout: b'{"status":"failed"}'
        )
        with self.assertRaises(OpenAIProviderError) as failed:
            await provider.complete(
                ModelRequest(uuid4(), "Analyze", None, "Researcher", ())
            )
        self.assertNotIn("test-key", str(failed.exception))


if __name__ == "__main__":
    unittest.main()
