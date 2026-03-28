"""Vocabulary management API endpoints for Studio."""

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from ..vocabulary import (
    load_vocabulary, get_terms, add_term, remove_term,
    ALL_TOPICS, get_extra_keywords,
)
from ..query_log import get_unknown_queries, remove_unknown_query

log = logging.getLogger(__name__)

router = APIRouter(tags=["Словарь"])


@router.get(
    "/api/vocabulary",
    summary="Текущий пользовательский словарь",
)
def api_get_vocabulary():
    terms = get_terms()
    return {
        "terms": terms,
        "count": len(terms),
        "topics": ALL_TOPICS,
    }


@router.post(
    "/api/vocabulary",
    summary="Добавить термин в словарь",
)
async def api_add_term(request: Request):
    body = await request.json()
    term = (body.get("term") or "").strip()
    topic = (body.get("topic") or "").strip()
    added_by = (body.get("added_by") or "оператор").strip()

    if not term:
        return JSONResponse(status_code=400, content={"error": "empty term"})
    if not topic:
        return JSONResponse(status_code=400, content={"error": "empty topic"})
    if topic not in ALL_TOPICS:
        return JSONResponse(status_code=400, content={"error": f"unknown topic: {topic}"})

    entry = add_term(term, topic, added_by)

    # Убрать из unknown_queries если был там
    remove_unknown_query(term)

    log.info("Vocabulary: added '%s' → %s", term, topic)
    return {"ok": True, "entry": entry}


@router.delete(
    "/api/vocabulary",
    summary="Удалить термин из словаря",
)
async def api_remove_term(request: Request):
    body = await request.json()
    term = (body.get("term") or "").strip().lower()
    topic = (body.get("topic") or "").strip() or None

    if not term:
        return JSONResponse(status_code=400, content={"error": "empty term"})

    removed = remove_term(term, topic)
    if not removed:
        return JSONResponse(status_code=404, content={"error": "term not found"})

    log.info("Vocabulary: removed '%s'", term)
    return {"ok": True}


@router.delete(
    "/api/unknown-query",
    summary="Удалить нераспознанный запрос",
)
async def api_remove_unknown(request: Request):
    body = await request.json()
    query = (body.get("query") or "").strip()
    if not query:
        return JSONResponse(status_code=400, content={"error": "empty query"})
    removed = remove_unknown_query(query)
    return {"ok": removed}


@router.get(
    "/api/unknown-queries",
    summary="Топ нераспознанных запросов",
)
def api_unknown_queries():
    return {"queries": get_unknown_queries(limit=30)}


@router.post(
    "/api/vocabulary/reload",
    summary="Перезагрузить словарь из YAML",
)
def api_reload_vocabulary():
    terms = load_vocabulary()
    return {"ok": True, "count": len(terms)}
