"""Ollama-compatible /api/generate server backed by the deterministic mock copywriter.
Used for the LLM-outage drill: point NF_LLM_PROVIDER=ollama at it, then kill it.

    uvicorn --app-dir scripts fake_ollama:app --port 11435
"""

import asyncio
import os

from fastapi import FastAPI
from pydantic import BaseModel

from notify_flow.llm.providers import MockProvider

app = FastAPI()
mock = MockProvider(model="fake-ollama")
LATENCY_S = float(os.environ.get("FAKE_OLLAMA_LATENCY_S", "0.15"))


class GenerateIn(BaseModel):
    model: str
    prompt: str
    stream: bool = False
    options: dict | None = None


@app.post("/api/generate")
async def generate(req: GenerateIn) -> dict:
    await asyncio.sleep(LATENCY_S)  # roughly a small local model's latency for short copy
    return {"model": req.model, "response": await mock.generate(req.prompt), "done": True}
