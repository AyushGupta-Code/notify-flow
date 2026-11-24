"""LLM providers behind one small interface. The default is a deterministic mock so tests,
CI and the eval harness need no key and no network."""

import asyncio
import hashlib
import json
import random
from typing import Protocol

import httpx


class LLMError(Exception):
    pass


class LLMProvider(Protocol):
    name: str
    model: str

    async def generate(self, prompt: str, max_tokens: int = 200) -> str: ...


MOCK_LINES = {
    "welcome": [
        "Hi {first_name}, welcome to your program! Your first exercise session takes about "
        "10 minutes whenever you're ready.",
        "Welcome aboard, {first_name}. A short first session is waiting for you in the app.",
    ],
    "reengage": [
        "Hi {first_name}, we missed you this week. A quick 10-minute session can help you "
        "get back on track.",
        "{first_name}, your exercises are ready when you are. Even a short session today counts.",
    ],
    "milestone": [
        "Nice work, {first_name}! You've completed {session_count} sessions. Keep it going.",
        "{first_name}, {session_count} sessions done. That consistency adds up.",
    ],
}


class MockProvider:
    """Deterministic copywriter. Reads the JSON context block in the prompt and returns
    one of a few on-brand lines. fail_rate and latency_s let drills inject faults."""

    name = "mock"

    def __init__(
        self,
        model: str = "mock-copywriter-1",
        fail_rate: float = 0.0,
        latency_s: float = 0.0,
        seed: int = 7,
    ) -> None:
        self.model = model
        self.fail_rate = fail_rate
        self.latency_s = latency_s
        self._rng = random.Random(seed)

    async def generate(self, prompt: str, max_tokens: int = 200) -> str:
        if self.latency_s:
            await asyncio.sleep(self.latency_s)
        if self.fail_rate and self._rng.random() < self.fail_rate:
            raise LLMError("mock provider injected failure")
        ctx = _context_from_prompt(prompt)
        lines = MOCK_LINES.get(str(ctx.get("campaign", "")), MOCK_LINES["reengage"])
        idx = int(hashlib.sha256(prompt.encode()).hexdigest(), 16) % len(lines)
        text = lines[idx].replace("{session_count}", str(ctx.get("session_count", 5)))
        if ctx.get("channel") == "email":
            text += " Reply STOP to opt out of reminders, or adjust them in the app."
        return text


def _context_from_prompt(prompt: str) -> dict[str, object]:
    start, end = prompt.find("<context>"), prompt.find("</context>")
    if start == -1 or end == -1:
        return {}
    try:
        data = json.loads(prompt[start + len("<context>") : end])
        return data if isinstance(data, dict) else {}
    except json.JSONDecodeError:
        return {}


class OllamaProvider:
    name = "ollama"

    def __init__(self, model: str, base_url: str, client: httpx.AsyncClient | None = None):
        self.model = model
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient()

    async def generate(self, prompt: str, max_tokens: int = 200) -> str:
        try:
            resp = await self._client.post(
                f"{self.base_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "options": {"num_predict": max_tokens, "temperature": 0.4},
                },
            )
            resp.raise_for_status()
            return str(resp.json()["response"]).strip()
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            raise LLMError(f"ollama: {type(exc).__name__}") from exc


class AnthropicProvider:
    """Claude via the Messages API. Only used when NF_LLM_PROVIDER=anthropic and a key is set;
    it is not exercised by tests or CI."""

    name = "anthropic"

    def __init__(self, model: str, api_key: str, client: httpx.AsyncClient | None = None):
        self.model = model
        self._api_key = api_key
        self._client = client or httpx.AsyncClient()

    async def generate(self, prompt: str, max_tokens: int = 200) -> str:
        try:
            resp = await self._client.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": self._api_key, "anthropic-version": "2023-06-01"},
                json={
                    "model": self.model,
                    "max_tokens": max_tokens,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            blocks = resp.json()["content"]
            return "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
        except (httpx.HTTPError, KeyError, ValueError) as exc:
            raise LLMError(f"anthropic: {type(exc).__name__}") from exc


def build_provider(
    kind: str, model: str, base_url: str, api_key: str = "", mock_fail_rate: float = 0.0
) -> LLMProvider:
    if kind == "mock":
        return MockProvider(model=model, fail_rate=mock_fail_rate)
    if kind == "ollama":
        return OllamaProvider(model=model, base_url=base_url)
    if kind == "anthropic":
        if not api_key:
            raise ValueError("NF_ANTHROPIC_API_KEY is required for the anthropic provider")
        return AnthropicProvider(model=model, api_key=api_key)
    raise ValueError(f"unknown LLM provider {kind!r}")
