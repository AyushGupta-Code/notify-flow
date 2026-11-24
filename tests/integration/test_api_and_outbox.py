import json

import httpx
import pytest

from notify_flow.api.app import app
from notify_flow.config import get_settings
from notify_flow.db.models import InboundEvent, Outbox
from notify_flow.relay import OutboxRelay

from .helpers import count

pytestmark = pytest.mark.integration


class MemoryPublisher:
    def __init__(self):
        self.sent = []

    async def start(self):
        pass

    async def stop(self):
        pass

    async def publish(self, key, value, headers):
        self.sent.append((key, json.loads(value), headers))


@pytest.fixture
async def client(sm):
    get_settings().api_bearer_token = "test-token"
    app.state.sm = sm
    app.state.orchestrator = None
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": "Bearer test-token"},
    ) as c:
        yield c


EVENT = {
    "event_id": "evt-api-0001",
    "member_id": "m-001",
    "event_type": "session_missed",
    "occurred_at": "2026-11-20T15:00:00Z",
    "payload": {},
}


async def test_duplicate_event_is_accepted_once(client, sm):
    r1 = await client.post("/v1/events", json=EVENT, headers={"x-correlation-id": "cid-abc"})
    r2 = await client.post("/v1/events", json=EVENT)
    assert r1.status_code == 202 and r1.json()["status"] == "accepted"
    assert r1.headers["x-correlation-id"] == "cid-abc"
    assert r2.status_code == 200 and r2.json()["status"] == "duplicate"
    assert await count(sm, InboundEvent) == 1
    assert await count(sm, Outbox) == 1


async def test_invalid_event_rejected(client):
    r = await client.post("/v1/events", json={**EVENT, "event_type": "nope"})
    assert r.status_code == 422


async def test_v1_routes_require_bearer_auth(client):
    assert (
        await client.post("/v1/events", json=EVENT, headers={"Authorization": ""})
    ).status_code == 401
    assert (
        await client.get("/v1/admin/prompts", headers={"Authorization": "Bearer wrong"})
    ).status_code == 401
    assert (await client.get("/healthz", headers={"Authorization": ""})).status_code == 200


async def test_outbox_relay_publishes_once_with_correlation_header(client, sm):
    await client.post("/v1/events", json=EVENT, headers={"x-correlation-id": "cid-xyz"})
    pub = MemoryPublisher()
    relay = OutboxRelay(sm, pub)
    assert await relay.run_once() == 1
    assert await relay.run_once() == 0
    key, body, headers = pub.sent[0]
    assert key == "m-001" and body["event_id"] == "evt-api-0001"
    assert headers["correlation_id"] == "cid-xyz"


async def test_health_ready_metrics_and_prompt_registry(client):
    assert (await client.get("/healthz")).json() == {"status": "ok"}
    assert (await client.get("/readyz")).status_code == 200
    m = await client.get("/metrics")
    assert "nf_events_ingested_total" in m.text
    reg = (await client.get("/v1/admin/prompts")).json()
    assert reg["campaigns"]["reengage"]["active_prompt"] == "reengage-v2"
    assert reg["campaigns"]["pain_checkin"]["llm_allowed"] is False
