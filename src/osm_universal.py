"""Универсальный OSM-источник для городских тем (школы, аптеки, остановки и др.)

Позволяет получать данные для ЛЮБОГО города через Overpass API,
не привязываясь к CSV-порталу opendata.novo-sibirsk.ru.

Все города (включая Новосибирск) используют OSM Overpass для стандартных тем.
CSV с opendata.novo-sibirsk.ru оставлен только для строительства (датасеты 124/125).

Архитектура:
  1. OSM_TOPICS — конфиг каждой темы (тег, поля, маппинг)
  2. fetch_osm_topic() — Overpass запрос по bbox города
  3. upsert_osm_topic() — сохранение в DuckDB (topic_osm_{topic})
  4. query_osm_topic() — чтение из DuckDB с фильтрами
  5. Изолированная работа: каждая функция принимает db_path/bbox явно
"""

import json
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import duckdb

from .cache import _get_conn  # единый пул соединений DuckDB на весь процесс

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent

# ── Конфигурация тем ──────────────────────────────────────────────────────

OSM_TOPICS: dict[str, dict] = {
    "schools": {
        "overpass_tags": '["amenity"="school"]',
        "name_col": "name",
        "type_label": "Школа",
        "display_cols": ["name", "address", "district", "phone", "website"],
        "ttl_days": 14,
    },
    "kindergartens": {
        "overpass_tags": '["amenity"="kindergarten"]',
        "name_col": "name",
        "type_label": "Детский сад",
        "display_cols": ["name", "address", "district", "phone", "website"],
        "ttl_days": 14,
    },
    "pharmacies": {
        "overpass_tags": '["amenity"="pharmacy"]',
        "name_col": "name",
        "type_label": "Аптека",
        "display_cols": ["name", "address", "district", "phone", "opening_hours"],
        "ttl_days": 14,
    },
    "stops": {
        "overpass_tags": '["highway"="bus_stop"]',
        "name_col": "name",
        "type_label": "Остановка",
        "display_cols": ["name", "district", "routes"],
        "ttl_days": 30,
    },
    "libraries": {
        "overpass_tags": '["amenity"="library"]',
        "name_col": "name",
        "type_label": "Библиотека",
        "display_cols": ["name", "address", "district", "phone", "website"],
        "ttl_days": 30,
    },
    "culture": {
        "overpass_tags": '["amenity"~"theatre|cinema|museum|arts_centre"]',
        "name_col": "name",
        "type_label": "Культура",
        "display_cols": ["name", "type_label", "address", "district", "phone", "website"],
        "ttl_days": 30,
    },
    "parks": {
        "overpass_tags": '["leisure"="park"]',
        "name_col": "name",
        "type_label": "Парк",
        "display_cols": ["name", "district"],
        "ttl_days": 30,
    },
    "sport_grounds": {
        "overpass_tags": '["leisure"~"pitch|playground|sports_centre"]',
        "name_col": "name",
        "type_label": "Спортплощадка",
        "display_cols": ["name", "sport", "district", "address"],
        "ttl_days": 30,
    },
    "parking": {
        "overpass_tags": '["amenity"="parking"]',
        "name_col": "name",
        "type_label": "Парковка",
        "display_cols": ["name", "parking_type", "capacity", "district", "address"],
        "ttl_days": 14,
    },
    "sport_orgs": {
        "overpass_tags": '["leisure"~"fitness_centre|sports_centre|swimming_pool"]',
        "name_col": "name",
        "type_label": "Спорт. организация",
        "display_cols": ["name", "sport", "address", "district", "phone"],
        "ttl_days": 30,
    },
}

# Маппинг amenity → читаемый тип
_AMENITY_LABELS = {
    "school": "Школа", "kindergarten": "Детский сад", "pharmacy": "Аптека",
    "library": "Библиотека", "theatre": "Театр", "cinema": "Кинотеатр",
    "museum": "Музей", "arts_centre": "Центр искусств",
    "fitness_centre": "Фитнес-центр", "sports_centre": "Спорткомплекс",
    "swimming_pool": "Бассейн",
}

_LEISURE_LABELS = {
    "pitch": "Спортплощадка", "playground": "Детская площадка",
    "sports_centre": "Спорткомплекс", "park": "Парк",
    "fitness_centre": "Фитнес-центр", "swimming_pool": "Бассейн",
}

_TABLE_PREFIX = "topic_osm_"

# Зеркала Overpass с ПЛАНЕТАРНЫМ покрытием.
#
# Важно при добавлении новых: часть публичных инстансов держит только
# региональный экстракт. Так, overpass.osm.ch отвечает 200 и выглядит
# исправным, но содержит лишь Швейцарию — по Новосибирску честно вернёт
# ноль объектов. Такое зеркало хуже недоступного: оно молча выдаёт пустой
# результат, который неотличим от «данных нет». Проверять новые адреса
# нужно через GET /osm/diagnose — там пробный запрос считает объекты
# именно в городе, а не абстрактно проверяет живость.
_OVERPASS_MIRRORS = [
    # Порядок = порядок проверки. Первым идёт зеркало, реально доступное из
    # сети прода: по данным GET /osm/diagnose с Railway (Амстердам) основные
    # немецкие инстансы отклоняют соединение, kumi.systems и private.coffee
    # отвечают 500, японское и тайваньское недоступны — работает только это.
    "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
    "https://overpass.osm.jp/api/interpreter",
    "https://overpass.nchc.org.tw/api/interpreter",
]

# Причина последнего отказа Overpass по темам — отдаётся в /osm/update, чтобы
# «не обновилось» не приходилось выяснять по логам контейнера.
_LAST_OVERPASS_ERRORS: dict[str, list[str]] = {}
# Диагностика последнего обращения: сколько объектов отдал Overpass, сколько
# осталось после фильтров и какой запрос уходил. Без этого «не обновилось»
# неотличимо от «объектов нет».
_LAST_OVERPASS_STATS: dict[str, dict] = {}


def diagnose_overpass(timeout: int = 20) -> list[dict]:
    """Проверяет каждое зеркало Overpass: доступность канала и живой ответ.

    Нужно, чтобы отличать «сервер недоступен из нашей сети» от «сервер
    ответил, но данных нет»: по логам эти случаи выглядели одинаково.
    """
    import time
    import requests
    from .constants import HEADERS

    probe = '[out:json][timeout:15];node["amenity"="pharmacy"](54.98,82.88,55.00,82.92);out count;'
    out = []
    for url in _OVERPASS_MIRRORS:
        host = url.split("/")[2]
        row = {"mirror": host}
        # 1. Сервисный статус — показывает лимиты и что канал вообще есть.
        #    Путь к нему выводим из адреса интерпретатора: у части зеркал
        #    он лежит не в корне (например, /osm/tools/overpass/api/…).
        status_url = url.replace("/interpreter", "/status")
        t0 = time.time()
        try:
            r = requests.get(status_url, headers=HEADERS, timeout=timeout)
            row["status_code"] = r.status_code
            row["status_head"] = r.text[:160].replace("\n", " | ")
        except Exception as e:
            row["status_code"] = None
            row["status_error"] = str(e)[:160]
        row["status_ms"] = round((time.time() - t0) * 1000)

        # 2. Минимальный реальный запрос — считает аптеки в одном квартале
        t0 = time.time()
        try:
            r = requests.post(url, data={"data": probe},
                              headers={**HEADERS, "Accept": "application/json"}, timeout=timeout)
            row["probe_code"] = r.status_code
            try:
                js = r.json()
                els = js.get("elements") or []
                row["probe_count"] = (els[0].get("tags", {}).get("total") if els else None)
                row["probe_remark"] = js.get("remark")
            except Exception:
                row["probe_body"] = r.text[:160]
        except Exception as e:
            row["probe_code"] = None
            row["probe_error"] = str(e)[:160]
        row["probe_ms"] = round((time.time() - t0) * 1000)
        out.append(row)
    return out


def last_overpass_stats(topic: str | None = None) -> dict:
    """Диагностика последнего обращения к Overpass по темам."""
    if topic:
        return _LAST_OVERPASS_STATS.get(topic, {})
    return dict(_LAST_OVERPASS_STATS)


def last_overpass_errors(topic: str | None = None) -> dict[str, list[str]]:
    """Ошибки последнего неудачного обращения к Overpass."""
    if topic:
        return {topic: _LAST_OVERPASS_ERRORS.get(topic, [])}
    return dict(_LAST_OVERPASS_ERRORS)


# ── Overpass fetch ────────────────────────────────────────────────────────

def fetch_osm_topic(
    topic: str,
    bbox_overpass: str,
    bbox_dict: dict,
    boundaries: list | None = None,
    ecology_stations: list | None = None,
) -> list[dict[str, Any]]:
    """Загружает объекты из Overpass API для указанной темы.

    Args:
        topic: ключ из OSM_TOPICS
        bbox_overpass: строка "(lat_min,lon_min,lat_max,lon_max)"
        bbox_dict: словарь {lat_min, lat_max, lon_min, lon_max}
        boundaries: полигоны границ для classify_district (или None)
        ecology_stations: станции для centroid fallback (или None)

    Returns:
        Список словарей с полями: name, address, district, _lat, _lon, ...
    """
    import requests

    cfg = OSM_TOPICS.get(topic)
    if not cfg:
        return []

    tags = cfg["overpass_tags"]
    query = f"""[out:json][timeout:60];
(
  node{tags}{bbox_overpass};
  way{tags}{bbox_overpass};
);
out center tags;"""

    # Overpass требует идентификации клиента: запрос без User-Agent (а requests
    # по умолчанию шлёт «python-requests/x.y») зеркала штатно отбивают кодом
    # 406/429. Берём общий UA проекта — он называет систему и контакт.
    from .constants import HEADERS
    headers = {**HEADERS, "Accept": "application/json"}

    data = None
    used_mirror = None
    raw_head = None
    errors: list[str] = []
    for url in _OVERPASS_MIRRORS:
        try:
            resp = requests.post(url, data={"data": query}, headers=headers, timeout=65)
            resp.raise_for_status()
            data = resp.json()
            used_mirror = url.split("/")[2]
            raw_head = resp.text[:300]
            # Пустой ответ — не повод останавливаться: зеркало могло отдать 200
            # с нулевой выдачей из-за перегрузки. Пробуем следующее.
            if not data.get("elements"):
                errors.append(f"{used_mirror}: 200, но elements=0")
                log.warning("Overpass %s [%s]: 200, но 0 объектов — пробую следующее зеркало",
                            topic, used_mirror)
                data = None
                continue
            break
        except Exception as e:
            host = url.split("/")[2]
            errors.append(f"{host}: {e}")
            # WARNING, а не DEBUG: раньше причина отказа Overpass не попадала
            # в лог, и наружу это выглядело как «данных нет» — темы молча
            # оставались устаревшими месяцами, никто не видел почему.
            log.warning("Overpass %s [%s]: %s", topic, host, e)
            continue

    if not data:
        log.error("Overpass %s: ни одно зеркало не дало данных — %s", topic, "; ".join(errors))
        _LAST_OVERPASS_ERRORS[topic] = errors
        _LAST_OVERPASS_STATS[topic] = {
            "mirrors_failed": errors, "query": query,
            "last_mirror": used_mirror, "raw_head": raw_head,
        }
        return []
    _LAST_OVERPASS_ERRORS.pop(topic, None)
    _LAST_OVERPASS_STATS[topic] = {
        "elements": len(data.get("elements", [])),
        "remark": data.get("remark"),   # Overpass кладёт сюда таймауты и ошибки запроса
        "mirror": used_mirror,
        "query": query,
    }

    from .district_classifier import _point_in_polygon

    results = []
    for el in data.get("elements", []):
        et = el.get("type")
        tags_el = el.get("tags", {})
        if et == "node":
            lat, lon = el.get("lat"), el.get("lon")
        elif et == "way":
            c = el.get("center", {})
            lat, lon = c.get("lat"), c.get("lon")
        else:
            continue
        if lat is None or lon is None:
            continue

        name = tags_el.get("name", "").strip()
        if not name and topic not in ("stops", "parking", "sport_grounds"):
            continue  # безымянные пропускаем (кроме остановок/парковок/площадок)

        # Classify district
        district = "Прочие"
        if bbox_dict["lat_min"] <= lat <= bbox_dict["lat_max"] and \
           bbox_dict["lon_min"] <= lon <= bbox_dict["lon_max"]:
            if boundaries:
                for b in boundaries:
                    if _point_in_polygon(lat, lon, b["polygon"]):
                        district = b["district"]
                        break
                # Bbox-фоллбек: если вне полигонов, но в bbox одного района
                if district == "Прочие":
                    bbox_hits: set[str] = set()
                    for b in boundaries:
                        poly = b["polygon"]
                        lons = [p[0] for p in poly]
                        lats = [p[1] for p in poly]
                        if min(lats) <= lat <= max(lats) and min(lons) <= lon <= max(lons):
                            bbox_hits.add(b["district"])
                    if len(bbox_hits) == 1:
                        district = next(iter(bbox_hits))
                    elif bbox_hits and ecology_stations:
                        best_d = float("inf")
                        for st in ecology_stations:
                            if st["district"] in bbox_hits:
                                d = (lat - st.get("lat", st.get("latitude", 0))) ** 2 + \
                                    (lon - st.get("lon", st.get("longitude", 0))) ** 2
                                if d < best_d:
                                    best_d = d
                                    district = st["district"]
            if district == "Прочие" and ecology_stations:
                # Центроидный фоллбек с порогом ~5 км
                _MAX_DEG2 = 0.045 ** 2 + 0.045 ** 2
                best_d = float("inf")
                for st in ecology_stations:
                    d = (lat - st.get("lat", st.get("latitude", 0))) ** 2 + \
                        (lon - st.get("lon", st.get("longitude", 0))) ** 2
                    if d < best_d:
                        best_d = d
                        district = st["district"]
                if best_d > _MAX_DEG2:
                    district = "Прочие"

        # Build row
        street = tags_el.get("addr:street", "")
        house = tags_el.get("addr:housenumber", "")
        address = f"{street} {house}".strip()

        amenity = tags_el.get("amenity", "")
        leisure = tags_el.get("leisure", "")
        type_label = _AMENITY_LABELS.get(amenity, "") or \
                     _LEISURE_LABELS.get(leisure, "") or \
                     cfg.get("type_label", "")

        row: dict[str, Any] = {
            "osm_id": f"{et[0]}{el['id']}",
            "name": name or type_label or f"({topic})",
            "type_label": type_label,
            "address": address,
            "district": district,
            "phone": tags_el.get("phone", tags_el.get("contact:phone", "")),
            "website": tags_el.get("website", tags_el.get("contact:website", "")),
            "opening_hours": tags_el.get("opening_hours", ""),
            "_lat": float(lat),
            "_lon": float(lon),
        }

        # Topic-specific fields
        if topic == "stops":
            row["routes"] = tags_el.get("route_ref", tags_el.get("route_ref:bus", ""))
        elif topic in ("sport_grounds", "sport_orgs"):
            row["sport"] = tags_el.get("sport", "")
        elif topic == "parking":
            row["parking_type"] = tags_el.get("parking", "")
            row["capacity"] = tags_el.get("capacity", "")

        results.append(row)

    _LAST_OVERPASS_STATS.setdefault(topic, {})["kept"] = len(results)
    log.info("OSM [%s]: загружено %d объектов (bbox %s)", topic, len(results), bbox_overpass)
    return results


# ── DuckDB storage ────────────────────────────────────────────────────────

def _table_name(topic: str) -> str:
    return f"{_TABLE_PREFIX}{topic}"


def upsert_osm_topic(
    topic: str,
    rows: list[dict[str, Any]],
    db_path: Path | str | None = None,
) -> int:
    """Сохраняет данные OSM-темы в DuckDB.

    Args:
        topic: ключ из OSM_TOPICS
        rows: результат fetch_osm_topic()
        db_path: путь к cache.db (если None — берёт из city_config)
    """
    if not rows:
        return 0

    if db_path is None:
        from .city_config import get_db_path
        db_path = get_db_path()

    table = _table_name(topic)
    conn = _get_conn(db_path)
    try:
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {table} (
            osm_id TEXT PRIMARY KEY,
            name TEXT,
            type_label TEXT,
            address TEXT,
            district TEXT DEFAULT '',
            phone TEXT,
            website TEXT,
            opening_hours TEXT,
            sport TEXT DEFAULT '',
            routes TEXT DEFAULT '',
            parking_type TEXT DEFAULT '',
            capacity TEXT DEFAULT '',
            _lat DOUBLE,
            _lon DOUBLE,
            loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )""")
        conn.execute(f"""CREATE TABLE IF NOT EXISTS {table}_meta (
            id INTEGER PRIMARY KEY DEFAULT 1,
            last_updated TIMESTAMP,
            total_rows INTEGER
        )""")

        conn.execute(f"DELETE FROM {table}")
        for r in rows:
            conn.execute(
                f"""INSERT INTO {table}
                (osm_id,name,type_label,address,district,phone,website,
                 opening_hours,sport,routes,parking_type,capacity,_lat,_lon)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                [r.get("osm_id", ""), r.get("name", ""), r.get("type_label", ""),
                 r.get("address", ""), r.get("district", ""),
                 r.get("phone", ""), r.get("website", ""),
                 r.get("opening_hours", ""), r.get("sport", ""),
                 r.get("routes", ""), r.get("parking_type", ""),
                 r.get("capacity", ""), r.get("_lat"), r.get("_lon")],
            )

        now = datetime.now(timezone.utc).isoformat()
        conn.execute(f"INSERT OR REPLACE INTO {table}_meta (id,last_updated,total_rows) VALUES (1,?,?)",
                      [now, len(rows)])
        return len(rows)
    finally:
        conn.close()


def is_osm_topic_stale(topic: str, db_path: Path | str | None = None) -> bool:
    """True если OSM-данные устарели или отсутствуют."""
    cfg = OSM_TOPICS.get(topic)
    if not cfg:
        return True
    ttl_days = cfg.get("ttl_days", 14)

    if db_path is None:
        from .city_config import get_db_path
        db_path = get_db_path()

    table = _table_name(topic)
    try:
        conn = _get_conn(db_path)
        try:
            row = conn.execute(
                f"SELECT last_updated FROM {table}_meta WHERE id = 1"
            ).fetchone()
        finally:
            conn.close()
        if not row or not row[0]:
            return True
        last = row[0]
        if isinstance(last, str):
            last = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if hasattr(last, 'tzinfo') and last.tzinfo is None:
            last = last.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - last > timedelta(days=ttl_days)
    except Exception:
        return True


def osm_topic_available(topic: str, db_path: Path | str | None = None) -> bool:
    """True если в кэше есть данные для этой темы."""
    if db_path is None:
        from .city_config import get_db_path
        db_path = get_db_path()
    table = _table_name(topic)
    try:
        conn = _get_conn(db_path)
        try:
            row = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            return row[0] > 0 if row else False
        finally:
            conn.close()
    except Exception:
        return False


def query_osm_topic(
    topic: str,
    limit: int = 20,
    offset: int = 0,
    district_filter: str | None = None,
    db_path: Path | str | None = None,
) -> tuple[list[dict], int]:
    """Запрос данных OSM-темы. Возвращает (rows, total_count)."""
    cfg = OSM_TOPICS.get(topic)
    if not cfg:
        return [], 0

    if db_path is None:
        from .city_config import get_db_path
        db_path = get_db_path()

    table = _table_name(topic)
    display_cols = cfg["display_cols"]
    # Всегда включаем координаты
    select_cols = list(display_cols) + ["_lat", "_lon"]
    # Убираем дубли
    select_cols = list(dict.fromkeys(select_cols))

    conn = _get_conn(db_path)
    try:
        where = ""
        if district_filter:
            d = district_filter.split()[0]
            where = f"WHERE district ILIKE '%{d}%'"

        total = conn.execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()[0]

        cols_sql = ", ".join(f'"{c}"' for c in select_cols)
        cursor = conn.execute(
            f'SELECT {cols_sql} FROM {table} {where} ORDER BY district, name LIMIT {limit} OFFSET {offset}'
        )
        rows = [dict(zip(select_cols, row)) for row in cursor.fetchall()]
        return rows, total
    except Exception as e:
        log.warning("query_osm_topic [%s]: %s", topic, e)
        return [], 0
    finally:
        conn.close()


def group_osm_topic(
    topic: str,
    db_path: Path | str | None = None,
) -> list[dict]:
    """Группировка по районам."""
    if db_path is None:
        from .city_config import get_db_path
        db_path = get_db_path()
    table = _table_name(topic)
    try:
        conn = _get_conn(db_path)
        try:
            rows = conn.execute(f"""
                SELECT district AS район, COUNT(*) AS количество
                FROM {table}
                WHERE district != 'Прочие' AND district != ''
                GROUP BY district ORDER BY количество DESC
            """).fetchall()
            return [{"район": r[0], "количество": r[1]} for r in rows]
        finally:
            conn.close()
    except Exception:
        return []


def count_osm_topic(
    topic: str,
    district_filter: str | None = None,
    db_path: Path | str | None = None,
) -> int:
    """Подсчёт записей."""
    if db_path is None:
        from .city_config import get_db_path
        db_path = get_db_path()
    table = _table_name(topic)
    try:
        conn = _get_conn(db_path)
        try:
            where = ""
            if district_filter:
                d = district_filter.split()[0]
                where = f"WHERE district ILIKE '%{d}%'"
            return conn.execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()[0]
        finally:
            conn.close()
    except Exception:
        return 0


def get_osm_meta(topic: str, db_path: Path | str | None = None) -> dict:
    """Метаданные OSM-темы."""
    if db_path is None:
        from .city_config import get_db_path
        db_path = get_db_path()
    table = _table_name(topic)
    try:
        conn = _get_conn(db_path)
        try:
            row = conn.execute(
                f"SELECT last_updated, total_rows FROM {table}_meta WHERE id = 1"
            ).fetchone()
            if row:
                return {"last_updated": row[0], "total_rows": row[1]}
        finally:
            conn.close()
    except Exception:
        pass
    return {"last_updated": None, "total_rows": 0}
