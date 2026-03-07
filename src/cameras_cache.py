"""DuckDB-хранилище для данных о камерах фиксации нарушений ПДД.

Таблица: cameras
TTL: 7 дней (данные обновляются редко, нет смысла обновлять чаще)
"""

import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import duckdb

log = logging.getLogger(__name__)

_DB_PATH = Path(__file__).parent.parent / "data" / "cache.db"
_TABLE = "cameras"
_TTL_DAYS = 7


def _conn():
    return duckdb.connect(str(_DB_PATH))


def _ensure_table() -> None:
    conn = _conn()
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                osm_id    TEXT PRIMARY KEY,
                _lat      DOUBLE,
                _lon      DOUBLE,
                maxspeed  TEXT,
                name      TEXT,
                direction TEXT,
                ref       TEXT,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cameras_meta (
                id           INTEGER PRIMARY KEY DEFAULT 1,
                last_updated TIMESTAMP,
                total_rows   INTEGER
            )
        """)
    finally:
        conn.close()


def upsert_cameras(cameras: list[dict[str, Any]]) -> int:
    """Сохраняет список камер в DuckDB. Возвращает количество записей."""
    if not cameras:
        return 0
    _ensure_table()
    conn = _conn()
    try:
        conn.execute(f"DELETE FROM {_TABLE}")
        for cam in cameras:
            conn.execute(
                f"""
                INSERT INTO {_TABLE} (osm_id, _lat, _lon, maxspeed, name, direction, ref)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    cam.get("osm_id", ""),
                    cam.get("_lat"),
                    cam.get("_lon"),
                    cam.get("maxspeed", ""),
                    cam.get("name", ""),
                    cam.get("direction", ""),
                    cam.get("ref", ""),
                ],
            )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT OR REPLACE INTO cameras_meta (id, last_updated, total_rows)
            VALUES (1, ?, ?)
        """, [now, len(cameras)])
        log.info("cameras_cache: сохранено %d камер", len(cameras))
        return len(cameras)
    finally:
        conn.close()


def query_cameras(
    limit: int = 50,
    maxspeed_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Возвращает список камер из кеша."""
    _ensure_table()
    conn = _conn()
    try:
        where = ""
        if maxspeed_filter:
            where = f"WHERE maxspeed = '{maxspeed_filter}'"
        rows = conn.execute(
            f"SELECT osm_id, _lat, _lon, maxspeed, name, direction, ref "
            f"FROM {_TABLE} {where} ORDER BY osm_id LIMIT {limit}"
        ).fetchall()
        cols = ["osm_id", "_lat", "_lon", "maxspeed", "name", "direction", "ref"]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def count_cameras() -> int:
    """Возвращает общее количество камер в кеше."""
    _ensure_table()
    conn = _conn()
    try:
        row = conn.execute(f"SELECT COUNT(*) FROM {_TABLE}").fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def get_cameras_meta() -> dict[str, Any]:
    """Метаданные кеша камер."""
    _ensure_table()
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT last_updated, total_rows FROM cameras_meta WHERE id = 1"
        ).fetchone()
        if row:
            return {"last_updated": row[0], "total_rows": row[1]}
        return {"last_updated": None, "total_rows": 0}
    except Exception:
        return {"last_updated": None, "total_rows": 0}
    finally:
        conn.close()


def is_cameras_stale() -> bool:
    """Возвращает True если данные устарели (> TTL_DAYS дней) или отсутствуют."""
    meta = get_cameras_meta()
    last = meta.get("last_updated")
    if not last:
        return True
    try:
        if isinstance(last, str):
            last = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - last > timedelta(days=_TTL_DAYS)
    except Exception:
        return True
