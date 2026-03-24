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

router = APIRouter(tags=["Presenter"])

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
    tags=["Presenter"],
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
    tags=["Presenter"],
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
    tags=["Presenter"],
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
    tags=["Presenter"],
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
