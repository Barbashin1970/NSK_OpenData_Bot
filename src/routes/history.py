"""Query history API endpoints for Studio."""

import logging

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ..query_log import get_history, get_stats, clear_log, log_query

log = logging.getLogger(__name__)

router = APIRouter(tags=["История запросов"])

# ── Тестовые запросы для демонстрации ──────────────────────────────────────────
_TEST_QUERIES = [
    "сколько школ в Новосибирске",
    "парковки по районам",
    "библиотеки в Советском районе",
    "спортивные площадки в городе",
    "аптеки на улице Ленина",
    "отключения электричества сейчас",
    "детские сады в Академгородке",
    "парки в Октябрьском районе",
    "остановки в Калининском районе",
    "горячая вода отключения",
    "количество аптек по районам",
    "активные стройки",
    "ввод в эксплуатацию в Кировском районе",
    "метро Новосибирск",
    "спортивные секции в Ленинском",
    "топ-5 парковок",
    "покажи парковки в центре",
    "плановые отключения отопления",
    "экология воздух Новосибирск",
    "школы в Дзержинском районе",
]


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
    source: str | None = Query(None),
):
    return get_history(limit=limit, offset=offset, topic=topic, city_id=city_id, search=search, source=source)


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


@router.post(
    "/api/query-history/test",
    summary="Прогнать тестовый набор запросов через роутер",
)
def api_run_test_queries():
    """Run ~20 test queries through router+planner and log with source='test'."""
    from ..router import best_topic
    from ..planner import make_plan

    results = []
    for q in _TEST_QUERIES:
        try:
            route_result = best_topic(q)
            topic = route_result.topic if route_result else None
            plan = make_plan(q, topic) if topic else None
            log_query(
                query=q,
                topic=topic,
                topic_name=route_result.name if route_result else None,
                confidence=route_result.confidence if route_result else None,
                operation=plan.operation if plan else "UNKNOWN",
                district=plan.district if plan else None,
                sub_district=plan.sub_district if plan else None,
                street=plan.street if plan else None,
                extra_filters=plan.extra_filters if plan else None,
                matched_keywords=route_result.matched_keywords if route_result else None,
                utility_type=route_result.utility_type if route_result else None,
                source="test",
            )
            results.append({
                "query": q,
                "topic": topic,
                "confidence": route_result.confidence if route_result else None,
                "operation": plan.operation if plan else "UNKNOWN",
            })
        except Exception as e:
            log.warning("test query failed: %s — %s", q, e)
            results.append({"query": q, "error": str(e)})

    return {"count": len(results), "results": results}
