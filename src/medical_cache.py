"""DuckDB-хранилище для данных о медицинских учреждениях Новосибирска.

Таблица: medical_facilities
TTL: 72 часа (больницы и поликлиники меняются редко)
"""

import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import duckdb

from .constants import NSK_ECOLOGY_STATIONS

log = logging.getLogger(__name__)

_DB_PATH = Path(__file__).parent.parent / "data" / "cache.db"
_TABLE = "medical_facilities"
_TTL_HOURS = 72

_NSK_LAT_MIN, _NSK_LAT_MAX = 54.70, 55.15
_NSK_LON_MIN, _NSK_LON_MAX = 82.65, 83.35


def _classify_district(lat: float | None, lon: float | None) -> str:
    """Определяет район города по координатам (ближайший центроид станции мониторинга)."""
    if lat is None or lon is None:
        return "Прочие"
    if not (_NSK_LAT_MIN <= lat <= _NSK_LAT_MAX and _NSK_LON_MIN <= lon <= _NSK_LON_MAX):
        return "Прочие"
    best_dist = float("inf")
    best_district = "Прочие"
    for st in NSK_ECOLOGY_STATIONS:
        d = (lat - st["latitude"]) ** 2 + (lon - st["longitude"]) ** 2
        if d < best_dist:
            best_dist = d
            best_district = st["district"]
    return best_district


def _conn():
    return duckdb.connect(str(_DB_PATH))


def _ensure_table() -> None:
    conn = _conn()
    try:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {_TABLE} (
                osm_id        TEXT PRIMARY KEY,
                name          TEXT,
                facility_type TEXT,
                type_label    TEXT,
                emergency     TEXT,
                phone         TEXT,
                address       TEXT,
                district      TEXT DEFAULT '',
                _lat          DOUBLE,
                _lon          DOUBLE,
                loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS medical_meta (
                id           INTEGER PRIMARY KEY DEFAULT 1,
                last_updated TIMESTAMP,
                total_rows   INTEGER
            )
        """)
    finally:
        conn.close()


def upsert_medical(facilities: list[dict[str, Any]]) -> int:
    """Сохраняет список медучреждений в DuckDB. Возвращает количество записей."""
    if not facilities:
        return 0
    _ensure_table()
    conn = _conn()
    try:
        conn.execute(f"DELETE FROM {_TABLE}")
        for f in facilities:
            district = _classify_district(f.get("_lat"), f.get("_lon"))
            conn.execute(
                f"""
                INSERT INTO {_TABLE}
                    (osm_id, name, facility_type, type_label, emergency,
                     phone, address, district, _lat, _lon)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    f.get("osm_id", ""),
                    f.get("name", ""),
                    f.get("facility_type", ""),
                    f.get("type_label", ""),
                    f.get("emergency", ""),
                    f.get("phone", ""),
                    f.get("address", ""),
                    district,
                    f.get("_lat"),
                    f.get("_lon"),
                ],
            )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("""
            INSERT OR REPLACE INTO medical_meta (id, last_updated, total_rows)
            VALUES (1, ?, ?)
        """, [now, len(facilities)])
        log.info("medical_cache: сохранено %d учреждений", len(facilities))
        return len(facilities)
    finally:
        conn.close()


def query_medical(
    limit: int = 50,
    offset: int = 0,
    district_filter: str | None = None,
    facility_type: str | None = None,
    emergency_only: bool = False,
) -> list[dict[str, Any]]:
    """Возвращает список медучреждений с опциональными фильтрами."""
    _ensure_table()
    conn = _conn()
    try:
        wheres = []
        if district_filter:
            d = district_filter.split()[0]
            wheres.append(f"district ILIKE '%{d}%'")
        if facility_type:
            wheres.append(f"facility_type = '{facility_type}'")
        if emergency_only:
            wheres.append("emergency = 'yes'")
        where = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        rows = conn.execute(
            f"""SELECT osm_id, name, facility_type, type_label, emergency,
                       phone, address, district, _lat, _lon
                FROM {_TABLE} {where}
                ORDER BY district, facility_type, name
                LIMIT {limit} OFFSET {offset}"""
        ).fetchall()
        cols = ["osm_id", "name", "facility_type", "type_label", "emergency",
                "phone", "address", "district", "_lat", "_lon"]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def count_medical(
    district_filter: str | None = None,
    facility_type: str | None = None,
    emergency_only: bool = False,
) -> int:
    """Количество медучреждений в кеше."""
    _ensure_table()
    conn = _conn()
    try:
        wheres = []
        if district_filter:
            d = district_filter.split()[0]
            wheres.append(f"district ILIKE '%{d}%'")
        if facility_type:
            wheres.append(f"facility_type = '{facility_type}'")
        if emergency_only:
            wheres.append("emergency = 'yes'")
        where = ("WHERE " + " AND ".join(wheres)) if wheres else ""
        row = conn.execute(f"SELECT COUNT(*) FROM {_TABLE} {where}").fetchone()
        return row[0] if row else 0
    finally:
        conn.close()


def group_by_district() -> list[dict[str, Any]]:
    """Группировка медучреждений по районам с разбивкой по типу."""
    _ensure_table()
    conn = _conn()
    try:
        rows = conn.execute(f"""
            SELECT district,
                   COUNT(*) AS количество,
                   SUM(CASE WHEN facility_type = 'hospital' THEN 1 ELSE 0 END) AS больниц,
                   SUM(CASE WHEN facility_type = 'clinic'   THEN 1 ELSE 0 END) AS поликлиник
            FROM {_TABLE}
            WHERE district != 'Прочие'
            GROUP BY district
            ORDER BY количество DESC
        """).fetchall()
        cols = ["район", "количество", "больниц", "поликлиник"]
        return [dict(zip(cols, row)) for row in rows]
    finally:
        conn.close()


def get_medical_meta() -> dict[str, Any]:
    """Метаданные кеша медучреждений."""
    _ensure_table()
    conn = _conn()
    try:
        row = conn.execute(
            "SELECT last_updated, total_rows FROM medical_meta WHERE id = 1"
        ).fetchone()
        if row:
            return {"last_updated": row[0], "total_rows": row[1]}
        return {"last_updated": None, "total_rows": 0}
    except Exception:
        return {"last_updated": None, "total_rows": 0}
    finally:
        conn.close()


def is_medical_stale() -> bool:
    """True если данные устарели (> TTL_HOURS часов) или отсутствуют."""
    meta = get_medical_meta()
    last = meta.get("last_updated")
    if not last:
        return True
    try:
        if isinstance(last, str):
            last = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - last > timedelta(hours=_TTL_HOURS)
    except Exception:
        return True
