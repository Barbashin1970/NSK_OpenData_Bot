"""Построение и выполнение SQL-запросов к DuckDB.

Маппинг: Plan → SQL → список записей.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cache import query, table_name
from .city_config import get_city_name as _get_city_name
from .planner import Plan
from .registry import get_dataset

log = logging.getLogger(__name__)


def _quote(val: str) -> str:
    """Экранирует одинарные кавычки для SQL."""
    return val.replace("'", "''")


def _district_condition(district: str, district_col: str) -> tuple[str, str]:
    """Возвращает (условие SQL, краткое описание) для фильтра по району."""
    d = _quote(district)
    # Некоторые записи содержат 'Центральный район', другие — просто 'Центральный'
    cond = f'("{district_col}" ILIKE \'%{d.split()[0]}%\')'
    return cond, district


def execute_plan(plan: Plan) -> dict[str, Any]:
    """Выполняет план и возвращает dict с результатами."""
    if not plan.topic:
        return {"error": "Тема не определена"}

    ds = get_dataset(plan.topic)
    if not ds:
        return {"error": f"Тема '{plan.topic}' не найдена в реестре"}

    tbl = table_name(plan.topic)
    fields = ds.get("fields", {})
    district_col = fields.get("district_col", "_district")
    street_col = fields.get("street_col", "_street")
    name_col = fields.get("name_col", "_name")
    display_cols = fields.get("display_cols", [district_col, street_col, name_col])
    count_col = fields.get("count_col")

    # Базовый WHERE
    where_parts = []
    params = []

    if plan.district:
        cond, _ = _district_condition(plan.district, district_col)
        where_parts.append(cond)

    if plan.street:
        s = _quote(plan.street)
        where_parts.append(f'("{street_col}" ILIKE \'%{s}%\')')

    audience = plan.extra_filters.get("audience")
    audience_unsupported = False
    if audience:
        if plan.topic == "libraries":
            if audience == "children":
                where_parts.append(
                    '("BiblName" ILIKE \'%детск%\' OR "BiblFName" ILIKE \'%детск%\')'
                )
            elif audience == "adults":
                where_parts.append(
                    '("BiblName" NOT ILIKE \'%детск%\' AND "BiblFName" NOT ILIKE \'%детск%\')'
                )
        elif plan.topic == "sport_orgs":
            if audience == "children":
                # МБУ ДО / МАУ ДО = учреждения дополнительного образования (детские спортшколы)
                where_parts.append(
                    '("NazvUch" LIKE \'МБУ ДО%\' OR "NazvUch" LIKE \'МАУ ДО%\')'
                )
            elif audience == "adults":
                where_parts.append(
                    '("NazvUch" NOT LIKE \'МБУ ДО%\' AND "NazvUch" NOT LIKE \'МАУ ДО%\''
                    ' AND length(trim("NazvUch")) > 3)'
                )
        elif plan.topic == "sport_grounds":
            # В данных нет разделения по возрастным группам
            audience_unsupported = True

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

    note = None
    if audience_unsupported:
        note = "В данных нет разделения на детские/взрослые объекты. Показаны все записи."

    try:
        if plan.operation == "COUNT":
            sql = f'SELECT COUNT(*) AS cnt FROM {tbl} {where_sql}'
            rows = query(sql)
            count = rows[0]["cnt"] if rows else 0
            result: dict = {
                "operation": "COUNT",
                "count": count,
                "rows": [],
                "columns": [],
            }

        elif plan.operation == "GROUP":
            sql = f'''
                SELECT "{district_col}" AS район, COUNT(*) AS количество
                FROM {tbl}
                {where_sql}
                GROUP BY "{district_col}"
                ORDER BY количество DESC
            '''
            rows = query(sql)
            result = {
                "operation": "GROUP",
                "rows": rows,
                "columns": ["район", "количество"],
                "count": sum(r.get("количество", 0) for r in rows),
            }

        elif plan.operation == "TOP_N":
            lim = plan.limit or 10
            # Если есть числовая колонка (кол-во мест и т.п.) — сортируем по ней
            if count_col:
                order_sql = f'TRY_CAST("{count_col}" AS INTEGER) DESC NULLS LAST'
            else:
                order_sql = f'"{name_col}"'

            sel_cols = ", ".join(f'"{c}"' for c in display_cols)
            sql = f'''
                SELECT {sel_cols}
                FROM {tbl}
                {where_sql}
                ORDER BY {order_sql}
                LIMIT {lim}
            '''
            rows = query(sql)
            result = {
                "operation": "TOP_N",
                "rows": rows,
                "columns": display_cols,
                "limit": lim,
                "count": len(rows),
            }

        elif plan.operation == "FILTER":
            lim = plan.limit or 20
            off = plan.offset or 0
            sel_cols = ", ".join(f'"{c}"' for c in display_cols)
            sql = f'''
                SELECT {sel_cols}
                FROM {tbl}
                {where_sql}
                ORDER BY "{district_col}", "{name_col}"
                LIMIT {lim} OFFSET {off}
            '''
            rows = query(sql)

            # Общее кол-во (без лимита)
            count_sql = f'SELECT COUNT(*) AS cnt FROM {tbl} {where_sql}'
            count_rows = query(count_sql)
            total = count_rows[0]["cnt"] if count_rows else len(rows)

            result = {
                "operation": "FILTER",
                "rows": rows,
                "columns": display_cols,
                "count": total,
                "limit": lim,
            }

        else:
            return {"error": f"Неизвестная операция: {plan.operation}"}

        if note:
            result["note"] = note
        return result

    except Exception as e:
        log.error(f"Ошибка выполнения плана: {e}")
        return {"error": str(e)}


def execute_ecology(plan: Plan) -> dict[str, Any]:
    """Выполняет запросы к таблицам экологии (dim_stations + fact_measurements).

    Поддерживаемые операции:
      ECO_STATUS   — текущие показатели (последний снимок, по районам)
      ECO_PDK      — превышения ПДК PM2.5 > 35 мкг/м³ за сегодня
      ECO_HISTORY  — история по дням за N дней
      ECO_RISKS    — прескриптивная аналитика: карточки рисков + рекомендации
      ECO_FORECAST — 7-дневный прогноз погоды (Open-Meteo daily forecast)
    """
    from .ecology_cache import (
        query_current, query_pdk_exceedances, query_history, get_ecology_meta,
        query_risks, query_forecast,
    )

    district = plan.district
    op = plan.operation

    try:
        if op == "ECO_STATUS":
            rows = query_current(district_filter=district)
            cols = ["district", "address", "pm25", "pm10", "no2", "aqi",
                    "temperature_c", "wind_speed_ms", "humidity_pct", "source", "measured_at"]
            return {
                "operation": op,
                "rows": [{k: r.get(k) for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
            }

        elif op == "ECO_PDK":
            rows = query_pdk_exceedances(district_filter=district)
            cols = ["district", "pm25_max", "pm25_avg", "измерений", "последнее"]
            return {
                "operation": op,
                "rows": [{k: r.get(k) for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
                "threshold": 35.0,
                "note": "ПДК WHO: PM2.5 > 35 мкг/м³ (среднесуточный порог)",
            }

        elif op == "ECO_HISTORY":
            days = plan.limit if plan.limit and plan.limit <= 30 else 7
            rows = query_history(district_filter=district, days=days)
            cols = ["день", "район", "pm25_ср", "pm25_макс", "aqi_ср",
                    "темп_ср", "ветер_ср", "снимков"]
            return {
                "operation": op,
                "rows": [{k: r.get(k) for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
                "days": days,
            }

        elif op == "ECO_RISKS":
            risks = query_risks(district_filter=district)
            return {
                "operation": op,
                "rows": risks,
                "columns": ["id", "scenario", "severity", "icon", "title",
                            "metrics", "citizen", "official"],
                "count": len(risks),
            }

        elif op == "ECO_FORECAST":
            days = min(plan.limit, 7) if plan.limit and plan.limit > 0 else 7
            rows = query_forecast(district_filter=district, days=days)
            cols = ["forecast_date", "day_name", "temp_max", "temp_min",
                    "wind_max", "precipitation", "snowfall_cm", "weathercode",
                    "weather_icon", "weather_desc",
                    "ice_risk", "cold_risk", "snow_risk", "snow_impact"]
            return {
                "operation": op,
                "rows": [{k: r.get(k) for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
                "days": days,
            }

        else:
            return {"error": f"Неизвестная ecology-операция: {op}"}

    except Exception as e:
        log.error(f"Ошибка execute_ecology: {e}")
        return {"error": str(e)}


def execute_power(plan: Plan) -> dict[str, Any]:
    """Выполняет запросы к таблице power_outages (отключения электроснабжения).

    Поддерживаемые операции:
      POWER_STATUS  — текущий статус (последний скрап)
      POWER_TODAY   — статус за сегодня
      POWER_PLANNED — плановые отключения
      POWER_HISTORY — история за N дней
    """
    from .power_cache import (
        query_power, get_history_by_day, get_power_meta, is_power_stale
    )

    district = plan.district
    op = plan.operation

    # Извлекаем фильтр по типу утилиты (электро / тепло / вода / газ / all)
    # Если ключ не задан (старые вызовы) — дефолт электроснабжение.
    # Если ключ есть, но значение пустое — значит «все типы» (utility_filter=None).
    _utility_raw = plan.extra_filters.get("utility", None)
    if _utility_raw is None:
        utility_filter: str | None = "электроснабж"
    elif _utility_raw == "":
        utility_filter = None   # запрос по всем типам ЖКХ
    else:
        utility_filter = _utility_raw

    try:
        if op == "POWER_STATUS":
            rows = query_power(
                utility_filter=utility_filter,
                district_filter=district,
                latest_only=True,
            )
            cols = ["utility", "group_type", "district", "houses", "scraped_at"]
            return {
                "operation": op,
                "rows": [{k: r.get(k, "") for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
            }

        elif op == "POWER_TODAY":
            now = datetime.now(timezone.utc)
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
            rows = query_power(
                utility_filter=utility_filter,
                district_filter=district,
                date_from=day_start,
                latest_only=False,
            )
            # Берём последний снимок сегодня
            if rows:
                last_ts = max(r["scraped_at"] for r in rows)
                rows = [r for r in rows if r["scraped_at"] == last_ts]

            cols = ["utility", "group_type", "district", "houses", "scraped_at"]
            return {
                "operation": op,
                "rows": [{k: r.get(k, "") for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
            }

        elif op == "POWER_PLANNED":
            rows = query_power(
                utility_filter=utility_filter,
                district_filter=district,
                group_filter="planned",
                latest_only=True,
            )
            cols = ["utility", "district", "houses", "scraped_at"]
            return {
                "operation": op,
                "rows": [{k: r.get(k, "") for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
            }

        elif op == "POWER_HISTORY":
            days = plan.limit if plan.limit and plan.limit <= 30 else 7
            rows = get_history_by_day(
                utility_filter=utility_filter,
                district_filter=district,
                days=days,
            )
            cols = ["day", "group_type", "total_houses", "snapshots"]
            return {
                "operation": op,
                "rows": [{k: r.get(k, "") for k in cols} for r in rows],
                "columns": cols,
                "count": len(rows),
            }

        else:
            return {"error": f"Неизвестная power-операция: {op}"}

    except Exception as e:
        log.error(f"Ошибка execute_power: {e}")
        return {"error": str(e)}


def execute_construction(plan: Plan) -> dict[str, Any]:
    """Выполняет запросы к таблицам строительства.

    Операции:
      CONSTRUCTION_ACTIVE      — активные стройки (разрешения − введённые в эксплуатацию)
      CONSTRUCTION_PERMITS     — список разрешений на строительство
      CONSTRUCTION_COMMISSIONED — список введённых в эксплуатацию объектов
      CONSTRUCTION_COUNT       — подсчёт
      CONSTRUCTION_GROUP       — группировка по районам
    """
    from .construction_opendata import (
        query_active, query_permits_list, count_construction,
        group_by_district, get_construction_meta,
        permits_available, commissioned_available,
    )

    op = plan.operation
    district = plan.district
    limit = plan.limit or 20
    offset = plan.offset or 0
    permit_type = plan.extra_filters.get("permit_type", "active")

    _COLS = ["NomRazr", "DatRazr", "Zastr", "NameOb", "AdrOr", "district", "KadNom"]
    _COL_LABELS = {
        "NomRazr": "Номер разрешения",
        "DatRazr": "Дата выдачи",
        "Zastr": "Застройщик",
        "NameOb": "Объект",
        "AdrOr": "Адрес",
        "district": "Район",
        "KadNom": "Кадастровый номер",
    }

    try:
        if op in ("CONSTRUCTION_ACTIVE", "CONSTRUCTION_PERMITS", "CONSTRUCTION_COMMISSIONED"):
            if op == "CONSTRUCTION_ACTIVE":
                rows, total = query_active(
                    district_filter=district,
                    limit=limit,
                    offset=offset,
                )
                source_note = (
                    "Активные стройки = разрешения на строительство, "
                    "объекты которых ещё не введены в эксплуатацию"
                )
                if not commissioned_available():
                    source_note += " (датасет 125 не загружен — показаны все разрешения)"
            elif op == "CONSTRUCTION_COMMISSIONED":
                rows, total = query_permits_list(
                    permit_type="commissioned",
                    district_filter=district,
                    limit=limit,
                    offset=offset,
                )
                source_note = "Ввод в эксплуатацию (датасет 125)"
            else:  # CONSTRUCTION_PERMITS
                rows, total = query_permits_list(
                    permit_type="permits",
                    district_filter=district,
                    limit=limit,
                    offset=offset,
                )
                source_note = "Разрешения на строительство (датасет 124)"

            return {
                "operation": op,
                "rows": rows,
                "columns": _COLS,
                "col_labels": _COL_LABELS,
                "count": total,
                "shown": len(rows),
                "limit": limit,
                "note": source_note,
            }

        elif op == "CONSTRUCTION_COUNT":
            n = count_construction(permit_type=permit_type, district_filter=district)
            label = {
                "active": "активных строек",
                "permits": "разрешений на строительство",
                "commissioned": "введённых в эксплуатацию объектов",
            }.get(permit_type, "объектов")
            return {
                "operation": op,
                "count": n,
                "label": label,
                "district": district,
            }

        elif op == "CONSTRUCTION_GROUP":
            rows = group_by_district(permit_type=permit_type)
            total = sum(r.get("количество", 0) for r in rows)
            return {
                "operation": op,
                "rows": rows,
                "columns": ["район", "количество"],
                "count": total,
                "permit_type": permit_type,
            }

        else:
            return {"error": f"Неизвестная construction-операция: {op}"}

    except Exception as e:
        log.error(f"Ошибка execute_construction: {e}")
        return {"error": str(e)}


def _enrich_metro_coords(stations: list[dict]) -> list[dict]:
    """Обогащает координаты станций из geocode_cache (только кеш, без live API-вызовов).

    Точные 2GIS-координаты кешируются фоновым заданием при старте сервера
    (_geocode_metro_bg). Если кеш пуст — возвращает статические координаты.
    """
    try:
        from .geocoder import _get_cached, _address_key
    except Exception:
        return stations

    enriched = []
    for s in stations:
        name = s.get("name", "")
        cache_key = _address_key(f"{_get_city_name()}, метро {name}")
        cached = _get_cached(cache_key)
        if cached and cached.get("lat") and cached.get("lon"):
            s = {**s, "_lat": cached["lat"], "_lon": cached["lon"]}
        enriched.append(s)
    return enriched


def _geocode_metro_bg() -> None:
    """Геокодирует все 13 станций метро и сохраняет результат в geocode_cache.

    Вызывается в фоновом потоке при старте сервера.
    Делает live-запросы к 2GIS API; при отсутствии ключа — пропускает.
    """
    try:
        from .geocoder import geocode
        from .metro_data import get_stations
        for s in get_stations():
            geocode(f"метро {s['name']}")
    except Exception as e:
        log.warning(f"metro geocoding bg: {e}")


def execute_metro(plan: Plan) -> dict[str, Any]:
    """Выполняет запросы к статическим данным метрополитена.

    Операции:
      METRO_INFO     — обзорная карточка: 2 линии, 13 станций, статистика
      METRO_STATIONS — список станций с фильтрами по линии / району

    Координаты обогащаются через 2GIS-геокодер (если ключ задан), иначе
    используются статические координаты из metro_data.py.
    """
    from .metro_data import get_metro_info, get_stations
    _metro_full = get_metro_info()
    METRO_LINES = _metro_full.get("lines", [])
    METRO_INFO = {k: v for k, v in _metro_full.items() if k not in ("lines", "stations")}

    op = plan.operation
    line_filter = plan.extra_filters.get("line") or None
    district_filter = plan.district

    try:
        if op == "METRO_STATIONS":
            stations = get_stations(line_filter=line_filter, district_filter=district_filter)
            stations = _enrich_metro_coords(stations)
            return {
                "operation": op,
                "info": {**METRO_INFO, "lines": METRO_LINES},
                "rows": stations,
                "columns": ["name", "line", "address", "district", "_lon", "_lat", "interchange_with", "note", "passengers_day"],
                "count": len(stations),
                "lines": METRO_LINES,
            }
        else:  # METRO_INFO
            info = get_metro_info()
            enriched_stations = _enrich_metro_coords(list(info["stations"]))
            enriched_info = {**info, "stations": enriched_stations}
            return {
                "operation": "METRO_INFO",
                "info": enriched_info,
                "rows": enriched_stations,
                "columns": ["name", "line", "address", "district", "_lon", "_lat"],
                "count": info["stations_count"],
                "lines": METRO_LINES,
            }

    except Exception as e:
        log.error(f"Ошибка execute_metro: {e}")
        return {"error": str(e)}


def execute_airport(plan: Plan) -> dict[str, Any]:
    """Выполняет запросы к статическим данным аэропорта Толмачёво.

    Операция:
      AIRPORT_INFO — полная информационная карточка аэропорта
    """
    from .airport_data import get_airport_info

    try:
        info = get_airport_info()
        return {
            "operation": "AIRPORT_INFO",
            "info": info,
            # Единственная строка — сам аэропорт (для отметки на карте)
            "rows": [{
                "_lon": info["_lon"],
                "_lat": info["_lat"],
                "name": info["short_name"],
                "iata": info["iata"],
            }],
            "count": 1,
        }

    except Exception as e:
        log.error(f"Ошибка execute_airport: {e}")
        return {"error": str(e)}
