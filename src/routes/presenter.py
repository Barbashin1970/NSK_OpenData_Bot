"""Presenter mode: dual-screen SSE (phone → display).

Phone sends queries via POST, display receives results via SSE stream.
Sessions linked by short token, QR code on display for easy connection.
"""

import asyncio
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from pathlib import Path
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

log = logging.getLogger(__name__)

router = APIRouter(tags=["Презентация"])

_STATIC = Path(__file__).parent.parent / "static"

# ── In-memory session store ──────────────────────────────────────────────────

SESSION_TTL = 8 * 3600  # 8 hours


@dataclass
class Session:
    sid: str
    created: float = field(default_factory=time.monotonic)
    last_active: float = field(default_factory=time.monotonic)
    queues: list[asyncio.Queue] = field(default_factory=list)
    last_result: dict | None = None


_sessions: dict[str, Session] = {}


def _cleanup_sessions() -> int:
    now = time.monotonic()
    expired = [sid for sid, s in _sessions.items() if now - s.last_active > SESSION_TTL]
    for sid in expired:
        del _sessions[sid]
    return len(expired)


def _get_session(sid: str) -> Session | None:
    s = _sessions.get(sid)
    if s:
        s.last_active = time.monotonic()
    return s


# ── HTML pages ───────────────────────────────────────────────────────────────

@router.get("/display", response_class=HTMLResponse, include_in_schema=False)
def get_display():
    p = _STATIC / "display.html"
    if not p.exists():
        return HTMLResponse("<h1>display.html not found</h1>")
    return HTMLResponse(p.read_text("utf-8"), headers={"Cache-Control": "no-store"})


@router.get("/mobile", response_class=HTMLResponse, include_in_schema=False)
def get_mobile():
    p = _STATIC / "mobile.html"
    if not p.exists():
        return HTMLResponse("<h1>mobile.html not found</h1>")
    return HTMLResponse(p.read_text("utf-8"), headers={"Cache-Control": "no-store"})


# ── Session API ──────────────────────────────────────────────────────────────

@router.post(
    "/session/create",
    tags=["Презентация"],
    summary="Создать сессию презентации",
)
def create_session():
    _cleanup_sessions()
    sid = secrets.token_urlsafe(6)  # ~8 chars, URL-safe
    _sessions[sid] = Session(sid=sid)
    log.info("Presenter session created: %s", sid)
    return {"sid": sid}


@router.get(
    "/session/{sid}/info",
    tags=["Презентация"],
    summary="Проверить существование сессии",
)
def session_info(sid: str):
    s = _get_session(sid)
    if not s:
        return JSONResponse(status_code=404, content={"error": "session not found"})
    return {
        "sid": sid,
        "displays": len(s.queues),
        "has_result": s.last_result is not None,
    }


@router.get(
    "/session/{sid}/stream",
    tags=["Презентация"],
    summary="SSE-поток для display (long-lived)",
)
async def session_stream(sid: str):
    s = _get_session(sid)
    if not s:
        return JSONResponse(status_code=404, content={"error": "session not found"})

    queue: asyncio.Queue = asyncio.Queue()
    s.queues.append(queue)

    async def _generator():
        try:
            # Send connected event
            yield f"data: {json.dumps({'type': 'connected', 'sid': sid})}\n\n"
            # If there's a previous result, replay it
            if s.last_result:
                yield f"data: {json.dumps(s.last_result, ensure_ascii=False, default=str)}\n\n"
            while True:
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=25)
                    yield f"data: {json.dumps(msg, ensure_ascii=False, default=str)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            if queue in s.queues:
                s.queues.remove(queue)

    return StreamingResponse(
        _generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.post(
    "/session/{sid}/ask",
    tags=["Презентация"],
    summary="Отправить запрос из mobile → display",
)
async def session_ask(sid: str, request: Request):
    s = _get_session(sid)
    if not s:
        return JSONResponse(status_code=404, content={"error": "session not found"})

    body = await request.json()
    q = (body.get("q") or "").strip()
    if not q:
        return JSONResponse(status_code=400, content={"error": "empty query"})

    # Push query event to all displays (iframe will call /ask itself)
    query_msg = {"type": "query", "query": q}
    s.last_result = query_msg
    for queue in s.queues:
        await queue.put(query_msg)

    return {"ok": True, "query": q}


@router.post(
    "/session/{sid}/set-city",
    tags=["Презентация"],
    summary="Сменить город из mobile → display",
)
async def session_set_city(sid: str, request: Request):
    s = _get_session(sid)
    if not s:
        return JSONResponse(status_code=404, content={"error": "session not found"})

    body = await request.json()
    city_id = (body.get("city_id") or "").strip()
    if not city_id:
        return JSONResponse(status_code=400, content={"error": "empty city_id"})

    # Actually switch the city on the server
    from .studio import studio_set_active_city
    studio_set_active_city({"city_id": city_id})

    # Get city name for display
    city_name = city_id
    try:
        from ..city_config import get_city_name
        city_name = get_city_name()
    except Exception:
        pass

    # Notify all displays
    msg = {"type": "set-city", "city_id": city_id, "city_name": city_name}
    for queue in s.queues:
        await queue.put(msg)

    return {"ok": True, "city_id": city_id}


@router.post(
    "/session/{sid}/set-role",
    tags=["Презентация"],
    summary="Сменить роль из mobile → display",
)
async def session_set_role(sid: str, request: Request):
    s = _get_session(sid)
    if not s:
        return JSONResponse(status_code=404, content={"error": "session not found"})

    body = await request.json()
    role = (body.get("role") or "").strip()
    if not role:
        return JSONResponse(status_code=400, content={"error": "empty role"})

    msg = {"type": "set-role", "role": role, "district": body.get("district", "")}
    for queue in s.queues:
        await queue.put(msg)

    return {"ok": True, "role": role}


@router.post(
    "/session/{sid}/end",
    tags=["Презентация"],
    summary="Завершить сессию презентации",
)
async def session_end(sid: str):
    s = _sessions.pop(sid, None)
    if not s:
        return JSONResponse(status_code=404, content={"error": "session not found"})

    # Notify all displays about session end
    msg = {"type": "session-end"}
    for queue in s.queues:
        await queue.put(msg)

    log.info("Presenter session ended: %s", sid)
    return {"ok": True}
