"""Построение и выполнение SQL-запросов к DuckDB.

Маппинг: Plan → SQL → список записей.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cache import query, table_name
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
      ECO_STATUS  — текущие показатели (последний снимок, по районам)
      ECO_PDK     — превышения ПДК PM2.5 > 35 мкг/м³ за сегодня
      ECO_HISTORY — история по дням за N дней
      ECO_RISKS   — прескриптивная аналитика: карточки рисков + рекомендации
    """
    from .ecology_cache import (
        query_current, query_pdk_exceedances, query_history, get_ecology_meta,
        query_risks,
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
