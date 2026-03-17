"""DuckDB-хранилище для данных о камерах фиксации нарушений ПДД.

Таблица: cameras
TTL: 7 дней (данные обновляются редко, нет смысла обновлять чаще)
"""

import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import duckdb

from .city_config import get_ecology_stations as _get_ecology_stations, get_bbox_dict as _get_bbox_dict

log = logging.getLogger(__name__)

_TABLE = "cameras"
_TTL_DAYS = 7

# Bounding box читается из city_profile.yaml через city_config
def _bbox():
    bb = _get_bbox_dict()
    return bb["lat_min"], bb["lat_max"], bb["lon_min"], bb["lon_max"]


def _classify_district(lat: float | None, lon: float | None) -> str:
    """Определяет район города по координатам (ближайший центроид станции мониторинга).

    Если точка за пределами bbox города — возвращает 'Прочие'.
    """
    if lat is None or lon is None:
        return "Прочие"
    lat_min, lat_max, lon_min, lon_max = _bbox()
    if not (lat_min <= lat <= lat_max and lon_min <= lon <= lon_max):
        return "Прочие"
    best_dist = float("inf")
    best_district = "Прочие"
    for st in _get_ecology_stations():
        d = (lat - st["latitude"]) ** 2 + (lon - st["longitude"]) ** 2
        if d < best_dist:
            best_dist = d
            best_district = st["district"]
    return best_district


def _conn():
    from .city_config import get_db_path
    return duckdb.connect(str(get_db_path()))


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
                district  TEXT DEFAULT '',
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Миграция: добавляем district если таблица существовала без него
        try:
            conn.execute(f"ALTER TABLE {_TABLE} ADD COLUMN district TEXT DEFAULT ''")
        except Exception:
            pass  # Колонка уже существует
        conn.execute("""
            CREATE TABLE IF NOT EXISTS cameras_meta (
                id           INTEGER PRIMARY KEY DEFAULT 1,
                last_updated TIMESTAMP,
                total_rows   INTEGER
            )
        """)
        # Бэкфилл: заполняем district для камер у которых он пустой
        try:
            rows = conn.execute(
                f"SELECT osm_id, _lat, _lon FROM {_TABLE} WHERE district IS NULL OR district = ''"
            ).fetchall()
            for osm_id, lat, lon in rows:
                d = _classify_district(lat, lon)
                conn.execute(
                    f"UPDATE {_TABLE} SET district = ? WHERE osm_id = ?", [d, osm_id]
                )
            if rows:
                log.info("cameras_cache: backfilled district for %d cameras", len(rows))
        except Exception as e:
            log.warning("cameras_cache: district backfill failed: %s", e)
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
            district = _classify_district(cam.get("_lat"), cam.get("_lon"))
            conn.execute(
                f"""
                INSERT INTO {_TABLE} (osm_id, _lat, _lon, maxspeed, name, direction, ref, district)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    cam.get("osm_id", ""),
                    cam.get("_lat"),
                    cam.get("_lon"),
                    cam.get("maxspeed", ""),
                    cam.get("name", ""),
                    cam.get("direction", ""),
                    cam.get("ref", ""),
                    district,
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
    offset: int = 0,
    maxspeed_filter: str | None = None,
    district_filter: str | None = None,
) -> list[dict[str, Any]]:
    """Возвращает список камер из кеша с опциональным фильтром по району."""
    _ensure_table()
    conn = _conn()
    try:
        wheres = []
        if maxspeed_filter:
            wheres.append(f"maxspeed = '{maxspeed_filter}'")
        if district_filter:
            d = district_filter.split()[0]
            wheres.append(f"district ILIKE '%{d}%'")
        where = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        rows = conn.execute(
            f"SELECT osm_id, _lat, _lon, maxspeed, name, direction, ref, district "
            f"FROM {_TABLE} {where} ORDER BY district, osm_id LIMIT {limit} OFFSET {offset}"
        ).fetchall()
        cols = ["osm_id", "_lat", "_lon", "maxspeed", "name", "direction", "ref", "district"]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def count_cameras(district_filter: str | None = None) -> int:
    """Возвращает количество камер в кеше (с опциональным фильтром по району)."""
    _ensure_table()
    conn = _conn()
    try:
        where = ""
        if district_filter:
            d = district_filter.split()[0]
            where = f"WHERE district ILIKE '%{d}%'"
        row = conn.execute(f"SELECT COUNT(*) FROM {_TABLE} {where}").fetchone()
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
