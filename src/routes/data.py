"""Core data endpoints: /ask, /topics, /update, /power/update."""

import logging
import re as _re

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from ..city_config import (
    get_city_name, get_city_slug, get_districts,
    get_feature, get_opendata_base_url,
)
from ..registry import load_registry
from ..router import route, best_topic
from ..planner import make_plan, INFO_PATTERNS, DISTRICTS_PATTERNS
from ..executor import execute_plan
from ..fetcher import load_meta, is_stale
from ..cache import get_table_info, table_exists

log = logging.getLogger(__name__)

router = APIRouter()


# ── Метро-подсказки для транзитных маршрутов ──────────────────────────────────
_METRO_DISTRICT_ENTRY: dict[str, tuple[str, str]] = {
    "Заельцовский":    ("Заельцовская", "1"),
    "Калининский":     ("Гагаринская", "1"),
    "Октябрьский":     ("Площадь Маркса", "1"),
    "Дзержинский":     ("Золотая нива", "2"),
    "Железнодорожный": ("Площадь Гарина-Михайловского", "2"),
}
_METRO_NO_SERVICE = frozenset({"Советский", "Кировский", "Ленинский", "Первомайский", "Кольцово"})


def _metro_route_hint(from_d: str, to_d: str) -> dict:
    """Подсказка маршрута на метро между двумя районами НСК."""
    def _base(d: str) -> str:
        return d.replace(" район", "").strip()

    fb, tb = _base(from_d), _base(to_d)

    if fb in _METRO_NO_SERVICE:
        return {"available": False, "reason": f"{fb} район — станций метро нет"}
    if tb in _METRO_NO_SERVICE:
        return {"available": False, "reason": f"{tb} район — станций метро нет"}

    is_from_central = (fb == "Центральный")
    is_to_central   = (tb == "Центральный")

    if not is_from_central and fb not in _METRO_DISTRICT_ENTRY:
        return {"available": False}
    if not is_to_central and tb not in _METRO_DISTRICT_ENTRY:
        return {"available": False}

    if is_to_central:
        to_station, to_line = None, None
    else:
        to_station, to_line = _METRO_DISTRICT_ENTRY[tb]

    if is_from_central:
        target = to_line or "1"
        from_station = "Площадь Ленина" if target == "1" else "Сибирская"
        from_line    = target
    else:
        from_station, from_line = _METRO_DISTRICT_ENTRY[fb]

    if is_to_central:
        to_station = "Площадь Ленина" if from_line == "1" else "Сибирская"
        to_line    = from_line

    transfer = (from_line != to_line)
    result: dict = {
        "available":    True,
        "from_station": from_station,
        "from_line":    from_line,
        "to_station":   to_station,
        "to_line":      to_line,
        "transfer":     transfer,
    }
    if transfer:
        if from_line == "1":
            result["transfer_exit"]  = "Площадь Ленина"
            result["transfer_enter"] = "Сибирская"
        else:
            result["transfer_exit"]  = "Сибирская"
            result["transfer_enter"] = "Площадь Ленина"
    return result


@router.get(
    "/topics",
    tags=["Данные"],
    summary="Список доступных тем",
    response_description="Массив `topics` с метаданными и состоянием кэша каждой темы",
)
def get_topics() -> dict:
    """
    Возвращает все поддерживаемые темы открытых данных с информацией о состоянии кэша.

    ### Поля каждой темы

    | Поле | Тип | Описание |
    |---|---|---|
    | `id` | string | Идентификатор для `POST /update?topic=<id>` |
    | `name` | string | Русское название |
    | `description` | string | Краткое описание набора данных |
    | `rows` | int or null | Число строк в кэше (`null` — данные не загружены) |
    | `last_updated` | string or null | Дата последней загрузки (ISO 8601) |
    | `stale` | bool | `true` если TTL истёк и рекомендуется обновление |
    | `passport_url` | string | Паспорт набора на opendata.novo-sibirsk.ru |

    Для обновления данных используйте `POST /update?topic=<id>` или `bot update --all` в CLI.
    """
    registry = load_registry()
    meta = load_meta()
    result = []
    for tid, ds in registry.items():
        m = meta.get(tid, {})
        result.append({
            "id": tid,
            "name": ds.get("name"),
            "description": ds.get("description"),
            "rows": m.get("rows"),
            "last_updated": m.get("last_updated"),
            "stale": is_stale(tid, ds.get("ttl_hours", 24)),
            "passport_url": ds.get("passport_url"),
        })
    return {"topics": result}


@router.get(
    "/ask",
    tags=["Запросы"],
    summary="Задать вопрос на русском языке",
    response_description="Результат: operation, rows, count + метаданные темы и кэша",
    responses={
        200: {
            "content": {
                "application/json": {
                    "examples": {
                        "count": {
                            "summary": "COUNT — «сколько школ»",
                            "value": {
                                "query": "сколько школ",
                                "topic": "schools",
                                "topic_name": "Школы",
                                "confidence": 0.857,
                                "operation": "COUNT",
                                "district": None,
                                "street": None,
                                "count": 214,
                                "rows": [],
                                "columns": [],
                                "cache": {"last_updated": "2026-03-04T10:00:00", "rows": 214},
                            },
                        },
                        "group": {
                            "summary": "GROUP — «парковки по районам»",
                            "value": {
                                "query": "парковки по районам",
                                "topic": "parking",
                                "topic_name": "Парковки",
                                "confidence": 0.75,
                                "operation": "GROUP",
                                "district": None,
                                "count": 2360,
                                "rows": [
                                    {"район": "Центральный район", "количество": 456},
                                    {"район": "Советский район", "количество": 312},
                                ],
                                "columns": ["район", "количество"],
                            },
                        },
                        "filter": {
                            "summary": "FILTER — «библиотеки для детей»",
                            "value": {
                                "query": "библиотеки для детей",
                                "topic": "libraries",
                                "operation": "FILTER",
                                "extra_filters": {"audience": "children"},
                                "count": 7,
                                "rows": [{"BiblName": "Детская библиотека № 1", "AdrDistr": "Ленинский район"}],
                                "columns": ["BiblName", "AdrDistr", "AdrStreet", "AdrDom", "Phone", "Site"],
                            },
                        },
                        "power": {
                            "summary": "POWER_STATUS — «отключения электричества сейчас»",
                            "value": {
                                "query": "отключения электричества сейчас",
                                "topic": "power_outages",
                                "topic_name": "Отключения электроснабжения",
                                "operation": "POWER_STATUS",
                                "district": None,
                                "count": 2,
                                "rows": [
                                    {"utility": "Электроснабжение", "group_type": "active",
                                     "district": "Ленинский район", "houses": "14",
                                     "scraped_at": "2026-03-04T09:30:00"},
                                ],
                                "columns": ["utility", "group_type", "district", "houses", "scraped_at"],
                                "power_meta": {"last_scraped": "2026-03-04T09:30:00", "active_houses": 14},
                            },
                        },
                    }
                }
            }
        }
    },
)
def get_ask(
    q: str = Query(
        ...,
        description=(
            "Вопрос на русском языке. Примеры:\n"
            "- `сколько парковок по районам`\n"
            "- `школы в Советском районе`\n"
            "- `топ-5 аптек в центре`\n"
            "- `отключения электричества сейчас`\n"
            "- `плановые отключения света на неделю`\n"
            "- `библиотеки для детей`\n"
            "- `детские спортивные организации по районам`"
        ),
        examples={"default": {"summary": "Группировка по районам", "value": "сколько парковок по районам"}},
        min_length=2,
    ),
    with_coords: bool = Query(
        False,
        description=(
            "Обогатить строки результата координатами (_lat, _lon) через 2GIS Geocoder. "
            "Работает только для операций FILTER и TOP_N с адресными данными. "
            "Требует настроенный 2GIS API ключ; без ключа строки возвращаются без изменений."
        ),
    ),
    offset: int = Query(
        0, ge=0,
        description="Смещение строк для пагинации (0-based). Используется с операцией FILTER.",
    ),
    page_size: int = Query(
        20, ge=1, le=200,
        description="Размер страницы для операции FILTER (по умолчанию 20, макс. 200).",
    ),
) -> dict:
    """
    Основной endpoint. Принимает запрос на русском языке, автоматически определяет
    тему и тип операции, возвращает структурированный результат.

    ### Как работает

    1. **Маршрутизация** — определяет тему по ключевым словам (`router.py`)
    2. **Планирование** — определяет операцию COUNT / GROUP / TOP_N / FILTER / POWER_* (`planner.py`)
    3. **Выполнение** — SQL к DuckDB или скрапинг `051.novo-sibirsk.ru` (`executor.py`)

    ### Поля ответа

    | Поле | Тип | Описание |
    |---|---|---|
    | `query` | string | Исходный запрос |
    | `topic` | string | ID темы (`parking`, `schools`, `power_outages`, …) |
    | `topic_name` | string | Русское название темы |
    | `confidence` | float | Уверенность маршрутизатора (0–1) |
    | `operation` | string | COUNT / GROUP / TOP_N / FILTER / POWER_* |
    | `district` | string or null | Распознанный район (если указан в запросе) |
    | `street` | string or null | Распознанная улица (если указана) |
    | `extra_filters` | object | Доп. фильтры (`audience: children/adults`) |
    | `rows` | array | Строки результата |
    | `columns` | array | Названия колонок |
    | `count` | int | Общее число совпадений (без учёта лимита) |
    | `cache` | object | `last_updated` и `rows` — состояние кэша |

    ### Особые случаи

    - Если тема не определена → поле `error` + список `available_topics`
    - Если данные не загружены → `error` с инструкцией запустить `POST /update`
    - Запросы об отключениях ЖКХ автоматически обновляют кэш если TTL > 30 мин
    """
    route_result = best_topic(q)

    if not route_result:
        q_lower = q.lower()
        if DISTRICTS_PATTERNS.search(q_lower):
            from ..router import DISTRICTS
            return {
                "query": q,
                "operation": "DISTRICTS",
                "rows": list(DISTRICTS.keys()),
                "count": len(DISTRICTS),
            }
        if INFO_PATTERNS.search(q_lower):
            registry = load_registry()
            topics_list = [
                {"id": tid, "name": ds.get("name"), "description": ds.get("description")}
                for tid, ds in registry.items()
            ]
            return {"query": q, "operation": "INFO", "topics": topics_list}
        log.info("UNKNOWN_QUERY: %s", q)
        return {
            "query": q,
            "operation": "UNKNOWN",
        }

    topic = route_result.topic
    plan = make_plan(q, topic)
    plan.offset = offset
    if page_size != 20 or plan.limit is None:
        plan.limit = page_size

    # ── Строительство ─────────────────────────────────────────────────────────
    if topic == "construction":
        from ..executor import execute_construction
        from ..construction_opendata import get_construction_meta, permits_available

        if not permits_available():
            log.info("Lazy load: construction_permits/commissioned не загружены, подгружаю")
            from ..updater import ensure_fresh
            ensure_fresh("construction_permits")
            ensure_fresh("construction_commissioned")

        if not permits_available():
            return JSONResponse(
                status_code=503,
                content={
                    "error": "Данные о строительстве не загружены",
                    "hint": "POST /update?topic=construction_permits и POST /update?topic=construction_commissioned",
                },
            )

        result = execute_construction(plan)

        if with_coords and plan.operation in ("CONSTRUCTION_ACTIVE", "CONSTRUCTION_COMMISSIONED") and result.get("rows"):
            from ..geocoder import geocode_rows
            result["rows"] = geocode_rows(result["rows"])
            result["coords_enriched"] = True
            result["coords_source"] = "2GIS Geocoder (кеш + API)"

        meta = get_construction_meta()
        return {
            "query": q,
            "topic": "construction",
            "operation": plan.operation,
            "district": plan.district,
            "meta": {
                "permits_total": meta.get("permits_total", 0),
                "commissioned_total": meta.get("commissioned_total", 0),
                "active_total": meta.get("active_total", 0),
                "permits_updated": meta.get("permits_updated", ""),
                "commissioned_updated": meta.get("commissioned_updated", ""),
            },
            **result,
        }

    # ── Метро ─────────────────────────────────────────────────────────────────
    if topic == "metro":
        from ..executor import execute_metro
        result = execute_metro(plan)
        return {
            "query": q,
            "topic": "metro",
            "topic_name": f"{get_city_name()} метрополитен",
            "confidence": route_result.confidence,
            "operation": result.get("operation", plan.operation),
            "district": plan.district,
            **result,
        }

    # ── Аэропорт ──────────────────────────────────────────────────────────────
    if topic == "airport":
        from ..executor import execute_airport
        result = execute_airport(plan)
        return {
            "query": q,
            "topic": "airport",
            "topic_name": f"{get_feature('airport_name', 'Аэропорт')} ({get_feature('airport_iata', '')})",
            "confidence": route_result.confidence,
            "operation": result.get("operation", plan.operation),
            **result,
        }

    # ── Экология и метеорология ───────────────────────────────────────────────
    if topic == "ecology":
        from ..executor import execute_ecology
        from ..ecology_cache import (
            is_ecology_stale, get_ecology_meta, upsert_stations, upsert_measurements,
            is_forecast_stale, upsert_forecast,
        )
        from ..ecology_fetcher import fetch_all_ecology, fetch_all_forecast

        if is_ecology_stale():
            upsert_stations()
            upsert_measurements(fetch_all_ecology())

        if is_forecast_stale():
            upsert_forecast(fetch_all_forecast())

        result = execute_ecology(plan)
        meta = get_ecology_meta()
        if plan.operation == "ECO_STATUS":
            from ..ecology_cache import query_risks
            result["risks"] = query_risks(district_filter=plan.district)
        return {
            "query": q,
            "topic": topic,
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation": plan.operation,
            "district": plan.district,
            "sub_district": plan.sub_district,
            "ecology_meta": {k: str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v
                             for k, v in meta.items()},
            **result,
        }

    # ── Отключения ЖКХ ───────────────────────────────────────────────────────
    if topic == "power_outages":
        from ..executor import execute_power
        from ..power_cache import is_power_stale, get_power_meta, upsert_outages
        from ..power_scraper import fetch_all_outages

        if is_power_stale():
            upsert_outages(fetch_all_outages())

        result = execute_power(plan)
        _raw = plan.extra_filters.get("utility", None)
        _uf = "электроснабж" if _raw is None else (_raw or None)
        meta = get_power_meta(utility_filter=_uf, district_filter=plan.district)
        return {
            "query": q,
            "topic": topic,
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation": plan.operation,
            "district": plan.district,
            "sub_district": plan.sub_district,
            "power_meta": {k: str(v) if not isinstance(v, (int, float, str, bool, type(None))) else v
                           for k, v in meta.items()},
            **result,
        }

    # ── Индекс дорожной нагрузки ──────────────────────────────────────────────
    if topic == "traffic_index":
        from ..traffic_index import get_traffic_index_with_weather
        ti = get_traffic_index_with_weather()
        return {
            "query":      q,
            "topic":      "traffic_index",
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation":  "TRAFFIC_INDEX",
            **ti,
        }

    # ── Маршруты общественного транспорта (из кэша остановок) ────────────────
    if topic == "transit":
        from ..cache import _get_conn, table_exists as _table_exists
        from ..transport_api import DISTRICT_COORDS

        from_district = plan.extra_filters.get("from_district") or ""
        to_district   = plan.extra_filters.get("to_district") or ""

        if not _table_exists("stops"):
            return {
                "query": q, "topic": "transit", "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "TRANSIT_ROUTE",
                "error": "Данные об остановках не загружены",
                "hint": "POST /update?topic=stops",
                "connections": [],
            }

        def _split_routes(marshryt: str) -> list[str]:
            if not marshryt:
                return []
            return _re.findall(r"\b\d+[а-яёa-z]?\b", marshryt)

        conn = _get_conn()
        try:
            def _get_route_stops(kw: str) -> dict[str, list[str]]:
                if not kw:
                    return {}
                rows = conn.execute(
                    "SELECT OstName, Marshryt FROM topic_stops "
                    "WHERE AdrDistr ILIKE ? AND Marshryt IS NOT NULL AND Marshryt != ''",
                    [f"%{kw.split()[0]}%"],
                ).fetchall()
                result: dict[str, list[str]] = {}
                for stop_name, marshryt in rows:
                    for route in _split_routes(marshryt or ""):
                        if route not in result:
                            result[route] = []
                        if stop_name and stop_name not in result[route]:
                            result[route].append(stop_name)
                return result

            from_routes = _get_route_stops(from_district)
            to_routes   = _get_route_stops(to_district)
            common = sorted(set(from_routes) & set(to_routes))
            connections = [
                {"route": r, "from_stops": from_routes[r][:3], "to_stops": to_routes[r][:3]}
                for r in common[:20]
            ]

            from_coords = DISTRICT_COORDS.get(from_district)
            to_coords   = DISTRICT_COORDS.get(to_district)
            hint = None
            if from_coords and to_coords:
                hint = (
                    f"https://2gis.ru/{get_city_slug()}/routeSearch/rsType/publictransport/"
                    f"from/{from_coords[0]},{from_coords[1]}/to/{to_coords[0]},{to_coords[1]}"
                )

            _opendata_url = get_opendata_base_url() or "opendata"
            return {
                "query": q, "topic": "transit", "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "TRANSIT_ROUTE",
                "from": from_district, "to": to_district,
                "common_routes_count": len(common),
                "connections": connections,
                "metro_route": _metro_route_hint(from_district, to_district),
                "hint": hint,
                "notice": (
                    f"⚠️ Данные о маршрутах взяты из открытых данных {get_city_name('genitive')} "
                    f"({_opendata_url}) и могут быть неполными или устаревшими. "
                    "Для построения точного маршрута воспользуйтесь приложением 2ГИС или Яндекс.Транспорт."
                ),
                "source": f"{_opendata_url} · остановки наземного транспорта (TTL 24ч)",
            }
        except Exception as e:
            log.error(f"Ошибка /ask transit: {e}")
            return {"query": q, "topic": "transit", "error": str(e), "connections": []}
        finally:
            conn.close()

    # ── Камеры фиксации нарушений ПДД (OSM Overpass) ─────────────────────────
    if topic == "cameras":
        from ..cameras_cache import (
            query_cameras, count_cameras, get_cameras_meta,
            upsert_cameras, is_cameras_stale,
        )
        from ..cameras_fetcher import fetch_cameras

        if is_cameras_stale():
            fetched = fetch_cameras()
            if fetched:
                upsert_cameras(fetched)

        op = plan.operation
        meta = get_cameras_meta()
        district = plan.district
        total = count_cameras(district_filter=district)

        if op == "COUNT":
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "COUNT",
                "count": total,
                "rows": [],
                "columns": [],
                "cameras_meta": {
                    "last_updated": str(meta.get("last_updated") or ""),
                    "total_rows": meta.get("total_rows", 0),
                    "source": "OpenStreetMap · Overpass API",
                },
            }
        else:
            lim = plan.limit or 20
            off = plan.offset or 0
            rows = query_cameras(limit=lim, offset=off, district_filter=district)
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "FILTER",
                "district": district or "",
                "count": total,
                "rows": rows,
                "columns": ["osm_id", "_lat", "_lon", "maxspeed", "name", "direction", "ref", "district"],
                "coords_enriched": True,
                "coords_source": "OpenStreetMap (предзагружены)",
                "cameras_meta": {
                    "last_updated": str(meta.get("last_updated") or ""),
                    "total_rows": meta.get("total_rows", 0),
                    "source": "OpenStreetMap · Overpass API",
                },
            }

    # ── Медицинские учреждения (OSM Overpass, TTL 72ч) ────────────────────────
    if topic == "medical":
        from ..medical_cache import (
            query_medical, count_medical, group_by_district as _medical_group,
            get_medical_meta, upsert_medical, is_medical_stale,
        )
        from ..medical_fetcher import fetch_medical

        if is_medical_stale():
            fetched = fetch_medical()
            if fetched:
                upsert_medical(fetched)

        op = plan.operation
        meta = get_medical_meta()
        district = plan.district
        facility_type = plan.extra_filters.get("facility_type", "") or None
        emergency_only = plan.extra_filters.get("emergency_only", "") == "1"

        _medical_source = {
            "last_updated": str(meta.get("last_updated") or ""),
            "total_rows": meta.get("total_rows", 0),
            "source": "OpenStreetMap · Overpass API · ODbL",
            "ttl_hours": 72,
        }

        if op == "MEDICAL_COUNT":
            total = count_medical(
                district_filter=district,
                facility_type=facility_type,
                emergency_only=emergency_only,
            )
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "COUNT",
                "count": total,
                "rows": [],
                "columns": [],
                "medical_meta": _medical_source,
            }
        elif op == "MEDICAL_GROUP":
            rows = _medical_group()
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "GROUP",
                "rows": rows,
                "columns": ["район", "количество", "больниц", "поликлиник"],
                "count": sum(r.get("количество", 0) for r in rows),
                "medical_meta": _medical_source,
            }
        else:  # MEDICAL_LIST (FILTER)
            lim = plan.limit or 20
            off = plan.offset or 0
            rows = query_medical(
                limit=lim,
                offset=off,
                district_filter=district,
                facility_type=facility_type,
                emergency_only=emergency_only,
            )
            total = count_medical(
                district_filter=district,
                facility_type=facility_type,
                emergency_only=emergency_only,
            )
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "FILTER",
                "district": district or "",
                "count": total,
                "rows": rows,
                "columns": ["name", "type_label", "emergency", "address",
                            "phone", "district", "_lat", "_lon"],
                "coords_enriched": True,
                "coords_source": "OpenStreetMap (предзагружены)",
                "medical_meta": _medical_source,
            }

    # ── Тепловые источники (статический GeoJSON) ──────────────────────────────
    if topic == "heat_sources":
        from ..heat_sources import (
            query_heat_sources,
            count_heat_sources,
            get_heat_metadata,
            TABLE_COLUMNS,
        )

        extra = plan.extra_filters or {}
        operator_group = extra.get("operator_group", "")
        pilot_only = extra.get("pilot_only", False)

        rows = query_heat_sources(operator_group=operator_group, pilot_only=pilot_only)
        total = len(rows)

        if plan.operation == "COUNT":
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "COUNT",
                "count": total,
                "rows": [],
                "columns": [],
            }

        heat_meta = get_heat_metadata()
        return {
            "query": q,
            "topic": topic,
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation": "FILTER",
            "count": total,
            "rows": rows,
            "columns": TABLE_COLUMNS,
            "coords_enriched": True,
            "coords_source": "GeoJSON (статические координаты)",
            "_skipDistBar": True,
            "_heatMeta": {
                "note": heat_meta.get("note", ""),
                "insights": heat_meta.get("insights", []),
                "eco_comparison": heat_meta.get("eco_comparison", None),
            },
        }

    # ── Выбросы в атмосферу 2-ТП Воздух (статический JSON) ──────────────────
    if topic == "emissions":
        from ..emissions import query_emissions, get_emissions_meta, TABLE_COLUMNS as EMIT_COLS

        extra = plan.extra_filters or {}
        top_n = plan.limit if plan.operation == "TOP_N" else 0
        rows = query_emissions(top_n=top_n)
        meta = get_emissions_meta()
        total = len(rows)

        if plan.operation == "COUNT":
            return {
                "query": q,
                "topic": topic,
                "topic_name": route_result.name,
                "confidence": round(route_result.confidence, 3),
                "operation": "COUNT",
                "count": total,
                "rows": [],
                "columns": [],
            }

        return {
            "query": q,
            "topic": topic,
            "topic_name": route_result.name,
            "confidence": round(route_result.confidence, 3),
            "operation": "FILTER",
            "count": total,
            "rows": rows,
            "columns": EMIT_COLS,
            "coords_enriched": True,
            "coords_source": f"2-ТП Воздух {meta.get('year', 2024)} (центроиды МО)",
            "_skipDistBar": True,
            "_emissionsView": True,
            "meta": meta,
        }

    # ── Стандартные темы opendata ─────────────────────────────────────────────
    if not get_feature("opendata_csv_enabled", False):
        return {
            "query": q,
            "topic": topic,
            "error": (
                f"Данные по теме «{topic}» недоступны для {get_city_name(case='genitive')}: "
                "портал открытых данных этого города не подключён."
            ),
        }

    if not table_exists(topic):
        log.info(f"Lazy load: тема '{topic}' не загружена, начинаю подгрузку")
        from ..updater import ensure_fresh
        if not ensure_fresh(topic):
            return {
                "query": q,
                "topic": topic,
                "error": "Не удалось загрузить данные. Проверьте доступность opendata.novo-sibirsk.ru",
            }

    result = execute_plan(plan)

    # ── Геокодирование (опционально) ──────────────────────────────────────────
    if with_coords and plan.operation in ("FILTER", "TOP_N", "CONSTRUCTION_ACTIVE", "CONSTRUCTION_COMMISSIONED") and result.get("rows"):
        from ..geocoder import geocode_rows
        result["rows"] = geocode_rows(result["rows"])
        result["coords_enriched"] = True
        result["coords_source"] = "2GIS Geocoder (кеш + API)"

    cache_info = load_meta().get(topic, {})
    cache_info.update(get_table_info(topic))

    return {
        "query": q,
        "topic": topic,
        "topic_name": route_result.name,
        "confidence": round(route_result.confidence, 3),
        "operation": plan.operation,
        "district": plan.district,
        "sub_district": plan.sub_district,
        "street": plan.street,
        "extra_filters": plan.extra_filters,
        "cache": {
            "last_updated": cache_info.get("last_updated"),
            "rows": cache_info.get("rows"),
        },
        **result,
    }


# ── Update & Power update ────────────────────────────────────────────────────

@router.post(
    "/update",
    tags=["Управление"],
    summary="Обновить данные из источника",
    response_description="Статус обновления по каждой теме: `rows` и `success`",
    responses={
        200: {
            "content": {
                "application/json": {
                    "example": {
                        "updated": {
                            "parking": {"rows": 2360, "success": True},
                            "schools": {"rows": 214, "success": True},
                            "pharmacies": {"rows": 27, "success": True},
                        }
                    }
                }
            }
        }
    },
)
def post_update(
    topic: str | None = Query(
        None,
        description=(
            "ID темы для обновления. Если не указан — обновляются все 10 тем (~1–2 мин).\n\n"
            "Доступные ID: `parking`, `stops`, `schools`, `kindergartens`, "
            "`libraries`, `pharmacies`, `sport_grounds`, `sport_orgs`, `culture`, "
            "`construction_permits`, `construction_commissioned`"
        ),
        examples={"default": {"summary": "Конкретная тема", "value": "parking"}},
    ),
) -> dict:
    """
    Загружает или обновляет данные с [opendata.novo-sibirsk.ru](http://opendata.novo-sibirsk.ru).

    - **Без `topic`** — обновляет все 12 тем (~1–2 минуты, ~4 МБ)
    - **С `topic`** — обновляет только указанную тему (несколько секунд)

    Данные сохраняются в локальный DuckDB-кэш (`DATA/cache.db`).
    TTL по умолчанию — 24 часа.
    """
    from ..cli import _do_update
    from ..registry import list_topics

    topics_to_update = [topic] if topic else list_topics()
    results = {}
    for t in topics_to_update:
        rows = _do_update(t)
        results[t] = {"rows": rows, "success": rows > 0}

    return {"updated": results}


@router.post(
    "/power/update",
    tags=["Управление"],
    summary="Обновить данные об отключениях ЖКХ",
    response_description="Статус обновления: количество загруженных записей",
)
def post_power_update() -> dict:
    """
    Принудительно загружает актуальные данные об отключениях ЖКХ.

    В штатном режиме обновление происходит автоматически при запросах через `/ask`
    (тема `power_outages`) если TTL (30 мин) истёк.
    """
    from ..power_scraper import fetch_all_outages
    from ..power_cache import upsert_outages, get_power_meta

    records = fetch_all_outages()
    count = upsert_outages(records)
    meta = get_power_meta()
    return {
        "success": count > 0,
        "records_loaded": count,
        "active_houses": meta.get("active_houses", 0),
        "planned_houses": meta.get("planned_houses", 0),
        "last_scraped": meta.get("last_scraped", ""),
        "source": get_feature("power_outages_url", ""),
    }
