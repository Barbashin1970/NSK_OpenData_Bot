"""Query history API endpoints for Studio."""

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ..query_log import get_history, get_stats, clear_log

router = APIRouter(tags=["История запросов"])


@router.get(
    "/api/query-history",
    summary="Получить историю запросов",
)
def api_query_history(
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    topic: str | None = Query(None),
    city_id: str | None = Query(None),
    search: str | None = Query(None),
):
    return get_history(limit=limit, offset=offset, topic=topic, city_id=city_id, search=search)


@router.get(
    "/api/query-stats",
    summary="Агрегированная статистика по запросам",
)
def api_query_stats():
    return get_stats()


@router.delete(
    "/api/query-history",
    summary="Очистить историю запросов",
)
def api_clear_history():
    deleted = clear_log()
    return {"deleted": deleted}
