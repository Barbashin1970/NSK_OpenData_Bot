"""Хранилище разрешений на строительство в DuckDB.

Таблица construction_permits:
  - TTL = 24 часа (обновляется через `bot update --construction`)
  - Хранит разрешения на строительство и ввод в эксплуатацию
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from .cache import _get_conn
from .constants import DATA_DIR

log = logging.getLogger(__name__)

CONSTRUCTION_TTL_HOURS = 24

_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS construction_permits (
    id           VARCHAR,
    permit_type  VARCHAR,
    number       VARCHAR,
    address      VARCHAR,
    object_name  VARCHAR,
    developer    VARCHAR,
    issue_date   VARCHAR,
    valid_until  VARCHAR,
    district     VARCHAR,
    raw          VARCHAR,
    scraped_at   VARCHAR,
    source_url   VARCHAR
)
"""


def init_construction_table() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = _get_conn()
    try:
        conn.execute(_TABLE_DDL)
    finally:
        conn.close()


def upsert_permits(records: list[dict[str, Any]]) -> int:
    """Полная замена записей (truncate + insert) для свежих данных."""
    if not records:
        return 0
    init_construction_table()
    conn = _get_conn()
    try:
        # Удаляем всё старше TTL
        cutoff = (
            datetime.now(timezone.utc) - timedelta(hours=CONSTRUCTION_TTL_HOURS)
        ).isoformat()
        conn.execute("DELETE FROM construction_permits WHERE scraped_at < ?", [cutoff])

        rows = [
            (
                r["id"], r["permit_type"], r.get("number", ""),
                r.get("address", ""), r.get("object_name", ""),
                r.get("developer", ""), r.get("issue_date", ""),
                r.get("valid_until", ""), r.get("district", ""),
                r.get("raw", ""), r["scraped_at"], r.get("source_url", ""),
            )
            for r in records
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO construction_permits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )
        log.info(f"Добавлено {len(rows)} записей в construction_permits")
        return len(rows)
    finally:
        conn.close()


def is_construction_stale() -> bool:
    """Возвращает True если данные устарели или таблицы нет."""
    try:
        init_construction_table()
        conn = _get_conn()
        try:
            result = conn.execute(
                "SELECT MAX(scraped_at) FROM construction_permits"
            ).fetchone()
            last = result[0] if result else None
            if not last:
                return True
            last_dt = datetime.fromisoformat(last)
            if last_dt.tzinfo is None:
                last_dt = last_dt.replace(tzinfo=timezone.utc)
            return (
                datetime.now(timezone.utc) - last_dt
                > timedelta(hours=CONSTRUCTION_TTL_HOURS)
            )
        finally:
            conn.close()
    except Exception:
        return True


def query_permits(
    permit_type: str | None = None,
    district_filter: str | None = None,
    address_filter: str | None = None,
    developer_filter: str | None = None,
    limit: int = 50,
) -> list[dict]:
    """Запрос к construction_permits с фильтрами."""
    init_construction_table()
    conn = _get_conn()
    try:
        wheres: list[str] = []
        params: list = []

        if permit_type:
            wheres.append("permit_type = ?")
            params.append(permit_type)
        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter}%")
        if address_filter:
            wheres.append("(address ILIKE ? OR object_name ILIKE ?)")
            params.extend([f"%{address_filter}%", f"%{address_filter}%"])
        if developer_filter:
            wheres.append("developer ILIKE ?")
            params.append(f"%{developer_filter}%")

        where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        sql = f"""
            SELECT permit_type, number, address, object_name, developer,
                   issue_date, valid_until, district, scraped_at
            FROM construction_permits
            {where_sql}
            ORDER BY issue_date DESC, address
            LIMIT {limit}
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка query_permits: {e}")
        return []
    finally:
        conn.close()


def count_permits(
    permit_type: str | None = None,
    district_filter: str | None = None,
) -> int:
    """Подсчёт разрешений с фильтрами."""
    init_construction_table()
    conn = _get_conn()
    try:
        wheres: list[str] = []
        params: list = []
        if permit_type:
            wheres.append("permit_type = ?")
            params.append(permit_type)
        if district_filter:
            wheres.append("district ILIKE ?")
            params.append(f"%{district_filter}%")
        where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        result = conn.execute(
            f"SELECT COUNT(*) FROM construction_permits {where_sql}", params
        ).fetchone()
        return result[0] if result else 0
    except Exception as e:
        log.error(f"Ошибка count_permits: {e}")
        return 0
    finally:
        conn.close()


def group_permits_by_district(permit_type: str | None = None) -> list[dict]:
    """Группировка разрешений по районам."""
    init_construction_table()
    conn = _get_conn()
    try:
        wheres: list[str] = ["district != ''"]
        params: list = []
        if permit_type:
            wheres.append("permit_type = ?")
            params.append(permit_type)
        where_sql = "WHERE " + " AND ".join(wheres)
        sql = f"""
            SELECT district AS район, COUNT(*) AS количество
            FROM construction_permits
            {where_sql}
            GROUP BY district
            ORDER BY количество DESC
        """
        cursor = conn.execute(sql, params)
        cols = [d[0] for d in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка group_permits_by_district: {e}")
        return []
    finally:
        conn.close()


def get_construction_meta() -> dict:
    """Метаданные: последнее обновление, количество записей."""
    try:
        init_construction_table()
        conn = _get_conn()
        try:
            last = conn.execute(
                "SELECT MAX(scraped_at) FROM construction_permits"
            ).fetchone()[0] or ""
            total = conn.execute(
                "SELECT COUNT(*) FROM construction_permits"
            ).fetchone()[0]
            by_type = conn.execute(
                "SELECT permit_type, COUNT(*) FROM construction_permits GROUP BY permit_type"
            ).fetchall()
            return {
                "last_scraped": last,
                "total": total,
                "by_type": {row[0]: row[1] for row in by_type},
            }
        finally:
            conn.close()
    except Exception as e:
        log.error(f"Ошибка get_construction_meta: {e}")
        return {"last_scraped": "", "total": 0, "by_type": {}}
