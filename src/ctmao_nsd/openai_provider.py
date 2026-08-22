"""Optional standard-library OpenAI Responses API model provider."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .agent_runtime import ModelRequest, ModelResponse

MAX_RESPONSE_BYTES = 2_000_000
Transport = Callable[[Request, float], bytes]


class OpenAIProviderError(RuntimeError):
    """Sanitized provider failure that never contains credentials or raw bodies."""


class OpenAIResponsesProvider:
    """Complete bounded public conference turns through ``POST /responses``.

    The API key is supplied by the host application and is never read from the
    environment, logged, or copied into runtime events by this adapter.
    """

    def __init__(
        self,
        api_key: str,
        *,
        model: str = "gpt-5.4",
        base_url: str = "https://api.openai.com/v1",
        timeout: float = 60.0,
        max_output_tokens: int = 700,
        role_instructions: Mapping[str, str] | None = None,
        transport: Transport | None = None,
    ) -> None:
        if not api_key.strip():
            raise ValueError("api_key must not be blank")
        if not model.strip() or len(model) > 128:
            raise ValueError("model must be a bounded non-blank identifier")
        if not base_url.startswith("https://"):
            raise ValueError("base_url must use HTTPS")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if not 1 <= max_output_tokens <= 4096:
            raise ValueError("max_output_tokens must be between 1 and 4096")
        self._api_key = api_key
        self._model = model
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_output_tokens = max_output_tokens
        self._role_instructions = dict(role_instructions or {})
        self._transport = transport or _urlopen_transport

    async def complete(self, request: ModelRequest) -> ModelResponse:
        """Return only public output text; reasoning and other items are ignored."""
        return await asyncio.to_thread(self._complete_sync, request)

    def _complete_sync(self, request: ModelRequest) -> ModelResponse:
        role = self._role_instructions.get(
            request.worker_id,
            "Contribute a concise, useful perspective to the conference.",
        )
        instructions = (
            f"You are the {request.worker_id} participant in a moderated agent "
            f"conference. {role} Return only the public statement you want the "
            "other participants and human to hear. Do not reveal hidden reasoning, "
            "credentials, or private instructions. Clearly label uncertainty."
        )
        input_text = f"Conference stage: {request.instruction}\n\n{request.value or ''}"
        payload = json.dumps(
            {
                "model": self._model,
                "instructions": instructions,
                "input": [
                    {
                        "role": "user",
                        "content": [{"type": "input_text", "text": input_text}],
                    }
                ],
                "max_output_tokens": self._max_output_tokens,
                "store": False,
            },
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        http_request = Request(
            f"{self._base_url}/responses",
            data=payload,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
                "User-Agent": "ctmao-nsd-reference/0.3",
            },
        )
        try:
            raw = self._transport(http_request, self._timeout)
        except HTTPError as exc:
            raise OpenAIProviderError(f"OpenAI request failed with HTTP {exc.code}") from None
        except (URLError, TimeoutError, OSError):
            raise OpenAIProviderError("OpenAI request failed at the network boundary") from None
        if len(raw) > MAX_RESPONSE_BYTES:
            raise OpenAIProviderError("OpenAI response exceeded the size limit")
        response = _strict_json_object(raw)
        if response.get("status") != "completed":
            raise OpenAIProviderError("OpenAI response did not complete")
        output_text = _extract_output_text(response)
        if not output_text:
            raise OpenAIProviderError("OpenAI response contained no public output text")
        return ModelResponse(output_text, "openai", self._model)


def _urlopen_transport(request: Request, timeout: float) -> bytes:
    with urlopen(request, timeout=timeout) as response:
        raw = response.read(MAX_RESPONSE_BYTES + 1)
    return raw


def _strict_json_object(raw: bytes) -> dict[str, object]:
    try:
        text = raw.decode("utf-8", errors="strict")

        def pairs(items: list[tuple[str, object]]) -> dict[str, object]:
            result: dict[str, object] = {}
            for key, value in items:
                if key in result:
                    raise ValueError("duplicate key")
                result[key] = value
            return result

        value = json.loads(
            text,
            object_pairs_hook=pairs,
            parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")),
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        raise OpenAIProviderError("OpenAI returned invalid JSON") from None
    if not isinstance(value, dict):
        raise OpenAIProviderError("OpenAI returned an invalid response shape")
    return value


def _extract_output_text(response: Mapping[str, object]) -> str:
    pieces: list[str] = []
    output = response.get("output")
    if not isinstance(output, list):
        return ""
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict) or part.get("type") != "output_text":
                continue
            text = part.get("text")
            if isinstance(text, str) and text.strip():
                pieces.append(text.strip())
    return "\n".join(pieces)[:16_000]

