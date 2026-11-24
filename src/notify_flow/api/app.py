"""Event-ingest and admin API. Run: uvicorn notify_flow.api.app:app"""

import hashlib
import secrets
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel
from sqlalchemy import case, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from notify_flow.config import get_settings
from notify_flow.db.models import AuditEvent, InboundEvent, Member, Notification, Outbox
from notify_flow.db.session import make_engine, make_sessionmaker
from notify_flow.events import MemberEvent
from notify_flow.llm.registry import load_registry
from notify_flow.log import configure_logging, correlation_id, get_logger, new_correlation_id
from notify_flow.metrics import EVENTS_INGESTED

log = get_logger(__name__)
HTTP_REQUESTS = Counter("nf_http_requests_total", "HTTP requests", ["route", "method", "status"])
HTTP_LATENCY = Histogram("nf_http_request_seconds", "HTTP latency", ["route", "method"])


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    s = get_settings()
    configure_logging(s.log_level, "notify-flow-api")
    engine = make_engine()
    app.state.sm = make_sessionmaker(engine)
    app.state.orchestrator = None
    yield
    await engine.dispose()


app = FastAPI(title="notify-flow", version="0.1.0", lifespan=lifespan)


async def _write_audit(request: Request, status: int, cid: str) -> None:
    if not request.url.path.startswith("/v1/") or not hasattr(request.app.state, "sm"):
        return
    token = request.headers.get("authorization", "").removeprefix("Bearer ")
    actor_hash = hashlib.sha256(token.encode()).hexdigest()
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)
    sm: async_sessionmaker[AsyncSession] = request.app.state.sm
    try:
        async with sm.begin() as audit_session:
            audit_session.add(
                AuditEvent(
                    actor_hash=actor_hash,
                    method=request.method,
                    route=path,
                    status=status,
                    correlation_id=cid,
                )
            )
    except Exception as exc:
        log.error("audit.write_failed", error=type(exc).__name__)


@app.middleware("http")
async def correlation_and_access_log(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    cid = request.headers.get("x-correlation-id") or new_correlation_id()
    correlation_id.set(cid)
    structlog.contextvars.bind_contextvars(correlation_id=cid)
    started = time.perf_counter()
    status = 500
    try:
        response = await call_next(request)
        status = response.status_code
        response.headers["x-correlation-id"] = cid
        response.headers["x-content-type-options"] = "nosniff"
        if request.url.path.startswith("/v1/"):
            response.headers["cache-control"] = "no-store"
        await _write_audit(request, status, cid)
        return response
    finally:
        route = request.scope.get("route")
        path = getattr(route, "path", "unmatched")
        elapsed = time.perf_counter() - started
        HTTP_REQUESTS.labels(path, request.method, str(status)).inc()
        HTTP_LATENCY.labels(path, request.method).observe(elapsed)
        if path != "/metrics":
            log.info(
                "http.request",
                method=request.method,
                route=path,
                status=status,
                duration_ms=round(elapsed * 1000, 2),
            )
        structlog.contextvars.unbind_contextvars("correlation_id")


async def session(request: Request) -> AsyncIterator[AsyncSession]:
    sm: async_sessionmaker[AsyncSession] = request.app.state.sm
    async with sm() as s:
        yield s


async def require_auth(authorization: str | None = Header(default=None)) -> None:
    expected = get_settings().api_bearer_token
    supplied = authorization.removeprefix("Bearer ") if authorization else ""
    if not expected or not secrets.compare_digest(supplied, expected):
        raise HTTPException(401, "invalid bearer token", headers={"WWW-Authenticate": "Bearer"})


class EventAccepted(BaseModel):
    event_id: str
    status: str  # "accepted" | "duplicate"
    correlation_id: str


@app.post("/v1/events", status_code=202, response_model=EventAccepted)
async def ingest_event(
    event: MemberEvent,
    response: Response,
    s: AsyncSession = Depends(session),
    _auth: None = Depends(require_auth),
) -> EventAccepted:
    cid = correlation_id.get() or new_correlation_id()
    settings = get_settings()
    async with s.begin():
        inserted = (
            await s.execute(
                insert(InboundEvent)
                .values(
                    event_id=event.event_id,
                    member_id=event.member_id,
                    event_type=event.event_type,
                    schema_version=event.schema_version,
                    payload=event.payload,
                    occurred_at=event.occurred_at,
                    correlation_id=cid,
                )
                .on_conflict_do_nothing(index_elements=["event_id"])
                .returning(InboundEvent.event_id)
            )
        ).scalar_one_or_none()
        if inserted is None:
            EVENTS_INGESTED.labels(event.event_type, "duplicate").inc()
            response.status_code = 200
            return EventAccepted(event_id=event.event_id, status="duplicate", correlation_id=cid)
        # Same transaction as the event row: the outbox relay publishes it later.
        s.add(
            Outbox(
                event_id=event.event_id,
                topic=settings.kafka_topic,
                key=event.member_id,
                payload=event.model_dump(mode="json"),
                headers={"correlation_id": cid},
            )
        )
    EVENTS_INGESTED.labels(event.event_type, "accepted").inc()
    log.info("event.accepted", event_id=event.event_id, event_type=event.event_type)
    return EventAccepted(event_id=event.event_id, status="accepted", correlation_id=cid)


class MemberIn(BaseModel):
    id: str
    first_name: str
    email: str | None = None
    phone: str | None = None
    timezone: str = "America/New_York"
    program_week: int = 1
    sessions_last_7d: int = 0
    days_since_last_session: int = 0
    preferred_channel: str | None = None
    sms_opt_in: bool = True
    push_enabled: bool = True
    email_opt_in: bool = True


@app.put("/v1/admin/members/{member_id}", status_code=204)
async def upsert_member(
    member_id: str,
    m: MemberIn,
    s: AsyncSession = Depends(session),
    _auth: None = Depends(require_auth),
) -> None:
    if m.id != member_id:
        raise HTTPException(400, "id mismatch")
    values = m.model_dump()
    async with s.begin():
        await s.execute(
            insert(Member)
            .values(**values)
            .on_conflict_do_update(
                index_elements=["id"], set_={k: v for k, v in values.items() if k != "id"}
            )
        )


@app.get("/v1/members/{member_id}/notifications")
async def list_notifications(
    member_id: str,
    s: AsyncSession = Depends(session),
    _auth: None = Depends(require_auth),
) -> list[dict[str, Any]]:
    rows = (
        (
            await s.execute(
                select(Notification)
                .where(Notification.member_id == member_id)
                .order_by(Notification.id)
            )
        )
        .scalars()
        .all()
    )
    return [
        {
            "id": r.id,
            "event_id": r.event_id,
            "campaign": r.campaign,
            "step": r.step,
            "channel": r.channel,
            "status": r.status,
            "copy_source": r.copy_source,
            "copy_reason": r.copy_reason,
            "prompt_version": r.prompt_version,
            "llm_model": r.llm_model,
            "template_version": r.template_version,
            "channel_model_version": r.channel_model_version,
            "decision_source": r.decision_source,
            "sent_at": r.sent_at.isoformat() if r.sent_at else None,
        }
        for r in rows
    ]


@app.post("/v1/notifications/{notification_id}/engagement", status_code=202)
async def record_engagement(
    notification_id: int,
    request: Request,
    s: AsyncSession = Depends(session),
    _auth: None = Depends(require_auth),
) -> dict[str, str]:
    async with s.begin():
        n = await s.get(Notification, notification_id, with_for_update=True)
        if n is None:
            raise HTTPException(404, "notification not found")
        if n.engaged_at is None:
            n.engaged_at = datetime.now(UTC)
        workflow_id = n.workflow_id
    orch = request.app.state.orchestrator
    if orch is None:
        from notify_flow.wiring import build_orchestrator

        orch = request.app.state.orchestrator = await build_orchestrator(get_settings())
    await orch.signal_engagement(workflow_id)
    return {"workflow_id": workflow_id, "status": "signalled"}


@app.get("/v1/admin/prompts")
async def prompt_registry(_auth: None = Depends(require_auth)) -> dict[str, Any]:
    r = load_registry(get_settings().prompt_registry_path)
    return {
        "registry_version": r.registry_version,
        "campaigns": {
            k: {
                "llm_allowed": c.llm_allowed,
                "active_prompt": c.active_prompt,
                "prompt_versions": sorted(c.prompts),
                "template_version": c.template_version,
            }
            for k, c in r.campaigns.items()
        },
    }


@app.get("/v1/admin/channel-experiment")
async def channel_experiment(
    s: AsyncSession = Depends(session), _auth: None = Depends(require_auth)
) -> dict[str, Any]:
    rows = (
        await s.execute(
            select(
                Notification.decision_source,
                func.count(Notification.id),
                func.sum(case((Notification.engaged_at.is_not(None), 1), else_=0)),
            )
            .where(Notification.step == 1, Notification.status == "sent")
            .group_by(Notification.decision_source)
        )
    ).all()
    return {
        "variants": {
            source: {
                "sent": sent,
                "engaged": engaged,
                "engagement_rate": round(engaged / sent, 4) if sent else 0.0,
            }
            for source, sent, engaged in rows
        }
    }


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/readyz")
async def readyz(s: AsyncSession = Depends(session)) -> dict[str, str]:
    try:
        await s.execute(text("select 1"))
    except Exception as exc:
        raise HTTPException(503, "database unavailable") from exc
    return {"status": "ready"}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
