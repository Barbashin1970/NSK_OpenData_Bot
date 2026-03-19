"""Transport endpoints: transit routes, traffic index, Yandex traffic."""

import logging
import re as _re

from fastapi import APIRouter, Query

from ..city_config import get_city_name, get_city_slug, get_opendata_base_url

log = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/transit",
    tags=["Запросы"],
    summary="Маршруты между районами (открытые данные мэрии)",
    response_description="Общие маршруты наземного транспорта между двумя районами.",
)
def get_transit(
    from_district: str = Query(
        ...,
        description="Район отправления. Например: `Советский район`",
        examples={"default": {"value": "Советский район"}},
    ),
    to_district: str = Query(
        ...,
        description="Район назначения. Например: `Дзержинский район`",
        examples={"default": {"value": "Дзержинский район"}},
    ),
) -> dict:
    """
    Находит общие маршруты наземного транспорта между двумя районами Новосибирска.

    Использует данные об остановках из [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru)
    (тема `stops`, TTL 24ч). **Ключ API не требуется.**

    Для каждого общего маршрута возвращает примеры остановок в районе отправления и назначения.
    Также включает ссылку для построения точного маршрута в 2ГИС.

    > ⚠️ Данные о маршрутах из открытых данных мэрии могут быть неполными.
    > Для точного маршрута используйте 2ГИС или Яндекс.Транспорт.
    """
    from ..cache import _get_conn, table_exists
    from ..transport_api import DISTRICT_COORDS

    if not table_exists("stops"):
        return {
            "error": "Данные об остановках не загружены",
            "hint": "POST /update?topic=stops",
            "connections": [],
        }

    def split_routes(marshryt: str) -> list[str]:
        if not marshryt:
            return []
        return _re.findall(r"\b\d+[а-яёa-z]?\b", marshryt)

    conn = _get_conn()
    try:
        from_kw = from_district.split()[0]
        to_kw = to_district.split()[0]

        def get_route_stops(kw: str) -> dict[str, list[str]]:
            rows = conn.execute(
                "SELECT OstName, Marshryt FROM topic_stops "
                "WHERE AdrDistr ILIKE ? AND Marshryt IS NOT NULL AND Marshryt != ''",
                [f"%{kw}%"],
            ).fetchall()
            result: dict[str, list[str]] = {}
            for stop_name, marshryt in rows:
                for route in split_routes(marshryt or ""):
                    if route not in result:
                        result[route] = []
                    if stop_name and stop_name not in result[route]:
                        result[route].append(stop_name)
            return result

        from_routes = get_route_stops(from_kw)
        to_routes = get_route_stops(to_kw)
        common = sorted(set(from_routes) & set(to_routes))

        connections = [
            {
                "route": r,
                "from_stops": from_routes[r][:3],
                "to_stops": to_routes[r][:3],
            }
            for r in common[:20]
        ]

        from_coords = DISTRICT_COORDS.get(from_district)
        to_coords = DISTRICT_COORDS.get(to_district)
        hint = None
        if from_coords and to_coords:
            hint = (
                f"https://2gis.ru/{get_city_slug()}/routeSearch/rsType/publictransport/"
                f"from/{from_coords[0]},{from_coords[1]}/to/{to_coords[0]},{to_coords[1]}"
            )

        _od = get_opendata_base_url() or "opendata"
        return {
            "from": from_district,
            "to": to_district,
            "common_routes_count": len(common),
            "connections": connections,
            "hint": hint,
            "notice": (
                f"⚠️ Данные о маршрутах взяты из открытых данных {get_city_name('genitive')} "
                f"({_od}) и могут быть неполными или устаревшими. "
                "Для построения точного маршрута воспользуйтесь приложением 2ГИС или Яндекс.Транспорт."
            ),
            "source": f"{_od} · остановки наземного транспорта (TTL 24ч)",
        }
    except Exception as e:
        log.error(f"Ошибка /transit: {e}")
        return {"error": str(e), "connections": []}
    finally:
        conn.close()


@router.get(
    "/transit/districts",
    tags=["Запросы"],
    summary="Транспортная инфраструктура по районам (без ключа API)",
    response_description="Число остановок наземного транспорта по районам",
)
def get_transit_districts() -> dict:
    """
    Возвращает число остановок наземного пассажирского транспорта по районам
    из кэша [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru).

    **Ключ API не требуется.** Данные берутся из темы `stops` (TTL 24ч).
    """
    from ..cache import _get_conn, table_exists

    if not table_exists("stops"):
        return {
            "error": "Данные об остановках не загружены",
            "hint": "POST /update?topic=stops",
            "rows": [],
            "total_stops": 0,
            "count": 0,
        }

    conn = _get_conn()
    try:
        cursor = conn.execute("""
            SELECT
                AdrDistr AS district,
                COUNT(*) AS stops_count
            FROM topic_stops
            WHERE AdrDistr IS NOT NULL AND AdrDistr != ''
            GROUP BY AdrDistr
            ORDER BY stops_count DESC
        """)
        cols = [d[0] for d in cursor.description]
        rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        total = sum(r["stops_count"] for r in rows)
        return {
            "operation": "TRANSIT_DISTRICTS",
            "count": len(rows),
            "total_stops": total,
            "rows": rows,
            "columns": cols,
            "source": "opendata.novo-sibirsk.ru · остановки наземного транспорта",
        }
    except Exception as e:
        log.error(f"Ошибка /transit/districts: {e}")
        return {"error": str(e), "rows": [], "total_stops": 0, "count": 0}
    finally:
        conn.close()


@router.get(
    "/traffic-index",
    tags=["Запросы"],
    summary="Индекс дорожной нагрузки (аналитика)",
    response_description="Синтетический индекс пробок 0–10 с факторами и рекомендациями.",
)
def get_traffic_index() -> dict:
    """
    Синтетический **индекс дорожной нагрузки** для Новосибирска (0 = пусто, 10 = коллапс).

    Алгоритм учитывает:
    - **Время суток** — утренний (07:30–09:30) и вечерний (16:30–19:00) час пик
    - **День недели** — понедельничный эффект (+0.8), пятничный исход (+0.7)
    - **Официальные праздники 2025–2027** — нерабочие дни, предпраздничные укороченные дни
    - **Погоду** — снег (+1.5/+2.5), первый осенний снег (+3.0), дождь (+0.8), гололедица (+2.5)
    - **Городские события** — 1 сентября (+2.0), предновогодняя суета (+2.0)
    - **Экстремальный мороз** (< −20°C) — снижает трафик (машины не заводятся)

    Погодные данные берутся из кэша Open-Meteo (обновляется каждые 15 мин).

    > ⚠️ Реальные данные о пробках отсутствуют — только аналитическая модель.
    """
    from ..traffic_index import get_traffic_index_with_weather
    return get_traffic_index_with_weather()


@router.get(
    "/yandex-traffic",
    tags=["Запросы"],
    summary="Яндекс.Пробки — реальный индекс",
    response_description="Текущий уровень пробок из Яндекс.Карт (0–10).",
)
def get_yandex_traffic() -> dict:
    """
    Реальный **индекс пробок** из Яндекс.Карт для текущего города.

    Публичный XML API виджета Яндекса, без ключа.
    Данные обновляются каждые ~2 мин на стороне Яндекса, наш TTL кэша — 3 мин.

    Возвращает: level (0–10), hint (текст), emoji, level_label, city, url.
    """
    from ..yandex_traffic import fetch_yandex_traffic
    data = fetch_yandex_traffic()
    if not data:
        return {"error": "Яндекс.Пробки недоступны для этого города", "available": False}
    return {**data, "available": True}
