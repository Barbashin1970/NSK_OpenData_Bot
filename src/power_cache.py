"""Хранилище данных об отключениях ЖКХ в DuckDB.

Таблица power_outages — скользящее окно:
  - история за последние POWER_HISTORY_DAYS дней
  - данные хранятся как временные снимки (scraped_at = timestamp)

Каждый запуск fetch_all_outages() добавляет новую группу записей —
накапливается история изменений состояния систем ЖКХ.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cache import _get_conn          # единственный _get_conn на весь проект
from .constants import (
    DATA_DIR, POWER_HISTORY_DAYS, POWER_TTL_MINUTES
)

log = logging.getLogger(__name__)

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS power_outages (
    id            VARCHAR,
    utility       VARCHAR,
    utility_id    VARCHAR,
    group_type    VARCHAR,
    district      VARCHAR,
    district_href VARCHAR,
    houses        INTEGER,
    scraped_at    VARCHAR,
    source_url    VARCHAR
)
"""


def init_power_table() -> None:
    """Создаёт таблицу power_outages если её нет."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    try:
        conn.execute(_TABLE_DDL)
    finally:
        conn.close()


def upsert_outages(records: list[dict[str, Any]]) -> int:
    """Вставляет записи в power_outages с автоматической очисткой старых данных.

    Возвращает количество добавленных записей.
    """
    if not records:
        return 0

    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (
            datetime.now(timezone.utc) - timedelta(days=POWER_HISTORY_DAYS)
        ).isoformat()
        conn.execute("DELETE FROM power_outages WHERE scraped_at < ?", [cutoff])

        rows = [
            (
                r["id"],
                r["utility"],
                r["utility_id"],
                r["group_type"],
                r["district"],
                r.get("district_href", ""),
                int(r.get("houses", 0)),
                r["scraped_at"],
                r.get("source_url", ""),
            )
            for r in records
        ]
        conn.executemany("INSERT INTO power_outages VALUES (?,?,?,?,?,?,?,?,?)", rows)
        log.info(f"Добавлено {len(rows)} записей в power_outages")
        return len(rows)
    finally:
        conn.close()


def is_power_stale(ttl_minutes: int = POWER_TTL_MINUTES) -> bool:
    """Возвращает True если данные устарели или таблицы нет."""
    try:
        init_power_table()
        conn = _get_conn()
        try:
            result = conn.execute("SELECT MAX(scraped_at) FROM power_outages").fetchone()
            last = result[0] if result else None
            if not last:
                return True
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return datetime.now(timezone.utc) - last_dt > timedelta(minutes=ttl_minutes)
        finally:
            conn.close()
    except Exception:
        return True


def get_power_meta() -> dict:
    """Возвращает метаданные: последнее обновление, кол-во записей, активных/плановых домов."""
    try:
        init_power_table()
        conn = _get_conn()
        try:
            last = conn.execute("SELECT MAX(scraped_at) FROM power_outages").fetchone()[0] or ""
            total = conn.execute("SELECT COUNT(*) FROM power_outages").fetchone()[0]
            latest_sql = (
                "WHERE scraped_at = (SELECT MAX(scraped_at) FROM power_outages)"
            )
            active = conn.execute(
                f"SELECT COALESCE(SUM(houses), 0) FROM power_outages {latest_sql} AND group_type='active'"
            ).fetchone()[0]
            planned = conn.execute(
                f"SELECT COALESCE(SUM(houses), 0) FROM power_outages {latest_sql} AND group_type='planned'"
            ).fetchone()[0]
            return {
                "last_scraped": last,
                "total_records": total,
                "active_houses": int(active),
                "planned_houses": int(planned),
            }
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Ошибка get_power_meta: {e}")
        return {"last_scraped": "", "total_records": 0, "active_houses": 0, "planned_houses": 0}


def query_power(
    utility_filter: str | None = None,
    district_filter: str | None = None,
    group_filter: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
    latest_only: bool = False,
) -> list[dict]:
    """Запрос к power_outages с фильтрами."""
    init_power_table()
    conn = _get_conn()
    try:
        wheres: list[str] = []
        params: list = []

        if latest_only:
            wheres.append("scraped_at = (SELECT MAX(scraped_at) FROM power_outages)")
        if utility_filter:
            wheres.append("utility ILIKE ?")
            params.append(f"%{utility_filter}%")
        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter}%")
        if group_filter:
            wheres.append("group_type = ?")
            params.append(group_filter)
        if date_from:
            wheres.append("scraped_at >= ?")
            params.append(date_from)
        if date_to:
            wheres.append("scraped_at <= ?")
            params.append(date_to)

        where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        sql = f"""
            SELECT utility, utility_id, group_type, district, houses, scraped_at, source_url
            FROM power_outages
            {where_sql}
            ORDER BY scraped_at DESC, utility, district
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_power: {e}")
        return []
    finally:
        conn.close()


def get_history_by_day(
    utility_filter: str | None = None,
    district_filter: str | None = None,
    days: int = 7,
) -> list[dict]:
    """Сводная история по дням за последние N дней (пик по домам в день)."""
    init_power_table()
    conn = _get_conn()
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        wheres = ["scraped_at >= ?"]
        params: list = [cutoff]

        if utility_filter:
            wheres.append("utility ILIKE ?")
            params.append(f"%{utility_filter}%")
        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter}%")

        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
            SELECT
                STRFTIME(CAST(scraped_at AS TIMESTAMP), '%Y-%m-%d') AS day,
                group_type,
                SUM(houses)              AS total_houses,
                COUNT(DISTINCT scraped_at) AS snapshots
            FROM power_outages
            {where_sql}
            GROUP BY day, group_type
            ORDER BY day DESC, group_type
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка get_history_by_day: {e}")
        return []
    finally:
        conn.close()


def get_current_status() -> list[dict]:
    """Текущий статус по всем утилитам из последнего скрапа."""
    return query_power(latest_only=True)


def get_electricity_status(district_filter: str | None = None) -> list[dict]:
    """Статус электроснабжения из последнего скрапа."""
    return query_power(
        utility_filter="электроснабж",
        district_filter=district_filter,
        latest_only=True,
    )
