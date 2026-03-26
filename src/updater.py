"""Тихое обновление данных без CLI-рендера.

Используется тремя механизмами:
  1. Lazy fallback в api.py — когда пользователь запросил тему, которой нет в БД.
  2. Background preloader — стартует через 15 сек после запуска, грузит все темы
     поочерёдно с паузами, чтобы не нагружать сервер и не мешать health check.
  3. Multi-city updater — фоново обновляет экологию, медицину и камеры для ВСЕХ
     городов (не только активного), чтобы данные были готовы при переключении.
"""

import asyncio
import logging
import os
from pathlib import Path

from .cache import table_exists
from .fetcher import is_stale
from .registry import list_topics

log = logging.getLogger(__name__)

# Порядок фоновой загрузки: от самых популярных к редким.
# Первые темы загрузятся раньше — пользователи не ждут.
PRELOAD_ORDER: list[str] = [
    "stops",
    "schools",
    "kindergartens",
    "pharmacies",
    "libraries",
    "parking",
    "sport_grounds",
    "sport_orgs",
    "culture",
    "construction_permits",
    "construction_commissioned",
]

# Пауза между темами при фоновой загрузке (секунды)
_PRELOAD_INTERVAL = 5

# Темы, которые сейчас грузятся (защита от параллельных вызовов)
_loading: set[str] = set()


def refresh_topic(topic: str, force: bool = False) -> int:
    """Скачивает и загружает одну тему в DuckDB без CLI-рендера.

    Возвращает:
      > 0 — количество загруженных строк
      -1  — данные уже свежие, загрузка не нужна
       0  — ошибка или нет данных
    """
    from .fetcher import fetch_csv
    from .parser import read_csv
    from .cache import load_into_db
    from .registry import get_dataset

    ds = get_dataset(topic)
    if not ds:
        return 0

    ttl = int(ds.get("ttl_hours", 24))
    if not force and not is_stale(topic, ttl) and table_exists(topic):
        return -1  # данные свежие

    url = ds.get("data_url", "")
    if not url:
        return 0

    try:
        path = fetch_csv(topic, url, ttl_hours=ttl, force=force)
        if not path:
            return 0
        rows = read_csv(path, ds)
        if not rows:
            return 0
        n = load_into_db(topic, rows, ds)
        log.info(f"refresh_topic({topic}): загружено {n} строк")
        return n
    except Exception as e:
        log.error(f"refresh_topic({topic}): {e}")
        return 0


def ensure_fresh(topic: str) -> bool:
    """Гарантирует наличие данных для темы. Блокирует до завершения загрузки.

    Безопасно вызывать на каждый запрос: если данные свежие — мгновенно (только
    чтение meta.json + проверка DuckDB information_schema).

    Возвращает True если данные доступны после вызова.
    """
    if table_exists(topic) and not is_stale(topic):
        return True  # быстрый путь: данные есть, TTL не истёк

    if topic in _loading:
        # Фоновый загрузчик уже тащит эту тему — ждём чуть-чуть
        log.info(f"ensure_fresh({topic}): уже загружается фоном, ждём")
        return table_exists(topic)  # вернём что есть прямо сейчас

    _loading.add(topic)
    try:
        n = refresh_topic(topic)
        return n != 0  # -1 = было свежим, >0 = загружено
    finally:
        _loading.discard(topic)


async def preload_all_async(delay_start: float = 15.0) -> None:
    """Фоновый preloader: стартует после задержки, грузит темы одну за одной.

    Запускать через asyncio.create_task() при старте API-сервера.
    Не трогает темы с актуальными данными (TTL не истёк).
    """
    await asyncio.sleep(delay_start)
    log.info("Фоновый preloader: старт")

    for topic in PRELOAD_ORDER:
        if table_exists(topic) and not is_stale(topic):
            log.debug(f"preload: {topic} — уже свежий, пропуск")
            await asyncio.sleep(0)  # уступаем event loop
            continue

        log.info(f"preload: загрузка {topic}...")
        try:
            n = await asyncio.to_thread(refresh_topic, topic)
            if n > 0:
                log.info(f"preload: {topic} готов ({n} строк)")
            elif n == -1:
                log.debug(f"preload: {topic} — уже свежий")
        except Exception as e:
            log.warning(f"preload: ошибка {topic}: {e}")

        await asyncio.sleep(_PRELOAD_INTERVAL)

    log.info("Фоновый preloader: все темы обработаны")


# ── Периодический авто-рефреш (каждые N часов) ──────────────────────────────
# На Railway (и любом сервере) данные устаревают если нет пользователей.
# Этот цикл каждые 12 часов проверяет TTL и обновляет устаревшие темы,
# чтобы чипы на главной всегда показывали «актуален».

_REFRESH_INTERVAL_HOURS = 12


async def periodic_refresh_loop(interval_hours: float = _REFRESH_INTERVAL_HOURS) -> None:
    """Бесконечный цикл: каждые interval_hours обновляет устаревшие CSV-темы.

    Запускать через asyncio.create_task() при старте API-сервера.
    Первая итерация начинается через interval_hours после preloader'а.
    """
    interval_sec = interval_hours * 3600
    log.info("periodic_refresh_loop: старт (интервал %.0f ч)", interval_hours)

    while True:
        await asyncio.sleep(interval_sec)
        log.info("periodic_refresh_loop: проверка устаревших тем…")
        refreshed = 0
        for topic in PRELOAD_ORDER:
            try:
                if is_stale(topic) or not table_exists(topic):
                    n = await asyncio.to_thread(refresh_topic, topic, True)
                    if n > 0:
                        refreshed += 1
                        log.info("periodic_refresh: %s — обновлено (%d строк)", topic, n)
                    await asyncio.sleep(_PRELOAD_INTERVAL)
            except Exception as e:
                log.warning("periodic_refresh: ошибка %s: %s", topic, e)
        log.info("periodic_refresh_loop: завершено, обновлено %d тем", refreshed)


# ── Мульти-город: изолированное обновление всех городов ────────────────────
#
# ВАЖНО: НЕ меняет глобальный CITY_PROFILE!
# Читает профиль YAML напрямую, строит bbox, подключается к db_path каждого
# города изолированно. Это предотвращает гонку потоков, из-за которой данные
# одного города попадали в кэш другого.

_CONFIG_DIR = Path(__file__).parent.parent / "config"
_PROJECT_ROOT = Path(__file__).parent.parent

# Пауза между городами (секунды) — бережём API лимиты
_CITY_INTERVAL = 30
# Интервал полного цикла (часы)
_MULTI_CITY_INTERVAL_HOURS = 6


def _list_city_profiles() -> list[dict]:
    """Возвращает [{profile_name, city_id, city_name, profile}, ...] для всех городов."""
    import yaml as _yaml

    cities = []
    for p in sorted(_CONFIG_DIR.glob("city_profile*.yaml")):
        try:
            with open(p, encoding="utf-8") as f:
                d = _yaml.safe_load(f)
            if d and "city" in d:
                cities.append({
                    "profile_name": p.stem,
                    "city_id": d["city"].get("id", ""),
                    "city_name": d["city"].get("name", ""),
                    "profile": d,
                })
        except Exception:
            continue
    return cities


def _city_db_path(city_id: str) -> Path:
    """Путь к cache.db конкретного города (без глобального переключения)."""
    db_dir = _PROJECT_ROOT / "data" / "cities" / city_id
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "cache.db"


def _city_bbox_overpass(profile: dict) -> str:
    """Формирует bbox в формате Overpass из профиля."""
    bb = profile["city"]["bbox"]
    def _fmt(v: float) -> str:
        s = f"{v:.10f}".rstrip("0")
        if "." not in s:
            return s + ".00"
        ip, dp = s.split(".")
        if len(dp) < 2:
            dp = dp.ljust(2, "0")
        return f"{ip}.{dp}"
    return f"({_fmt(bb['lat_min'])},{_fmt(bb['lon_min'])},{_fmt(bb['lat_max'])},{_fmt(bb['lon_max'])})"


def _city_boundaries_path(city_id: str) -> Path:
    return _PROJECT_ROOT / "data" / "cities" / city_id / "district_boundaries.geojson"


# ── Изолированные Overpass-запросы (без глобального контекста) ─────────────

_OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]


def _overpass_fetch(query: str, timeout: int = 60) -> dict | None:
    """Выполняет запрос к Overpass API с fallback на зеркала."""
    import requests
    for url in _OVERPASS_MIRRORS:
        try:
            resp = requests.post(url, data={"data": query}, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.debug("Overpass %s: %s", url, e)
            continue
    return None


def _classify_point(lat: float, lon: float, boundaries: list | None,
                    ecology_stations: list, bbox: dict) -> str:
    """Классифицирует точку по району — изолированно от глобального контекста."""
    if lat is None or lon is None:
        return "Прочие"
    if not (bbox["lat_min"] <= lat <= bbox["lat_max"] and
            bbox["lon_min"] <= lon <= bbox["lon_max"]):
        return "Прочие"
    # Point-in-polygon
    if boundaries:
        from .district_classifier import _point_in_polygon
        for b in boundaries:
            if _point_in_polygon(lat, lon, b["polygon"]):
                return b["district"]
        # Bbox-фоллбек: если точка в bbox только одного района — берём его
        bbox_hits: set[str] = set()
        for b in boundaries:
            poly = b["polygon"]
            lons = [p[0] for p in poly]
            lats = [p[1] for p in poly]
            if min(lats) <= lat <= max(lats) and min(lons) <= lon <= max(lons):
                bbox_hits.add(b["district"])
        if len(bbox_hits) == 1:
            return next(iter(bbox_hits))
        if bbox_hits:
            # Предпочитаем центроиды из районов, чей bbox содержит точку
            best_dist = float("inf")
            best_district = next(iter(bbox_hits))
            for st in ecology_stations:
                if st["district"] in bbox_hits:
                    d = (lat - st.get("lat", st.get("latitude", 0))) ** 2 + \
                        (lon - st.get("lon", st.get("longitude", 0))) ** 2
                    if d < best_dist:
                        best_dist = d
                        best_district = st["district"]
            return best_district
    # Centroid fallback
    best_dist = float("inf")
    best_district = "Прочие"
    for st in ecology_stations:
        d = (lat - st.get("lat", st.get("latitude", 0))) ** 2 + \
            (lon - st.get("lon", st.get("longitude", 0))) ** 2
        if d < best_dist:
            best_dist = d
            best_district = st["district"]
    return best_district


def _load_boundaries_from_file(city_id: str) -> list | None:
    """Загружает полигоны границ напрямую из файла (без lru_cache)."""
    import json as _json
    path = _city_boundaries_path(city_id)
    if not path.exists():
        return None
    try:
        data = _json.loads(path.read_text(encoding="utf-8"))
        result = []
        for f in data.get("features", []):
            district = f.get("properties", {}).get("district", "")
            geom = f.get("geometry", {})
            if geom.get("type") == "Polygon":
                coords = geom.get("coordinates", [[]])[0]
                result.append({"district": district, "polygon": coords})
            elif geom.get("type") == "MultiPolygon":
                for poly in geom.get("coordinates", []):
                    if poly:
                        result.append({"district": district, "polygon": poly[0]})
        return result if result else None
    except Exception:
        return None


# ── Изолированные refresh-функции для каждого типа данных ─────────────────

_TYPE_LABELS = {"hospital": "Больница", "clinic": "Поликлиника"}


def _refresh_medical_isolated(profile: dict, city_id: str) -> int:
    """Обновляет медучреждения для города без переключения глобального контекста."""
    import duckdb
    from datetime import datetime, timezone, timedelta

    db_path = _city_db_path(city_id)
    bbox_str = _city_bbox_overpass(profile)
    bbox = profile["city"]["bbox"]
    boundaries = _load_boundaries_from_file(city_id)
    eco_stations = profile.get("ecology_stations", [])

    # Проверяем stale
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS medical_meta (
            id INTEGER PRIMARY KEY DEFAULT 1, last_updated TIMESTAMP, total_rows INTEGER)""")
        row = conn.execute("SELECT last_updated FROM medical_meta WHERE id = 1").fetchone()
        if row and row[0]:
            last = row[0] if not isinstance(row[0], str) else datetime.fromisoformat(row[0].replace("Z", "+00:00"))
            if hasattr(last, 'tzinfo') and last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - last < timedelta(hours=72):
                return -1  # свежие
    except Exception:
        pass
    finally:
        conn.close()

    # Fetch из Overpass
    query = f"""[out:json][timeout:55];
(node["amenity"~"hospital|clinic"]{bbox_str};way["amenity"~"hospital|clinic"]{bbox_str};);
out center tags;"""

    data = _overpass_fetch(query)
    if not data:
        return 0

    facilities = []
    for el in data.get("elements", []):
        et = el.get("type")
        tags = el.get("tags", {})
        if et == "node":
            lat, lon = el.get("lat"), el.get("lon")
        elif et == "way":
            c = el.get("center", {})
            lat, lon = c.get("lat"), c.get("lon")
        else:
            continue
        if lat is None or lon is None:
            continue
        name = tags.get("name", "").strip()
        if not name:
            continue
        amenity = tags.get("amenity", "")
        street = tags.get("addr:street", "")
        house = tags.get("addr:housenumber", "")
        district = _classify_point(lat, lon, boundaries, eco_stations, bbox)
        facilities.append((
            f"{et[0]}{el['id']}", name, amenity,
            _TYPE_LABELS.get(amenity, amenity),
            tags.get("emergency", ""),
            tags.get("phone", tags.get("contact:phone", "")),
            f"{street} {house}".strip(),
            district, float(lat), float(lon),
        ))

    if not facilities:
        return 0

    # Store
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS medical_facilities (
            osm_id TEXT PRIMARY KEY, name TEXT, facility_type TEXT, type_label TEXT,
            emergency TEXT, phone TEXT, address TEXT, district TEXT DEFAULT '',
            _lat DOUBLE, _lon DOUBLE, loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("DELETE FROM medical_facilities")
        conn.executemany(
            "INSERT INTO medical_facilities (osm_id,name,facility_type,type_label,emergency,phone,address,district,_lat,_lon) VALUES (?,?,?,?,?,?,?,?,?,?)",
            facilities,
        )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("INSERT OR REPLACE INTO medical_meta (id, last_updated, total_rows) VALUES (1, ?, ?)",
                      [now, len(facilities)])
        log.info("multi-city [%s]: медицина — %d записей", city_id, len(facilities))
        return len(facilities)
    finally:
        conn.close()


def _refresh_cameras_isolated(profile: dict, city_id: str) -> int:
    """Обновляет камеры для города без переключения глобального контекста."""
    import duckdb
    from datetime import datetime, timezone, timedelta

    db_path = _city_db_path(city_id)
    bbox_str = _city_bbox_overpass(profile)
    bbox = profile["city"]["bbox"]
    boundaries = _load_boundaries_from_file(city_id)
    eco_stations = profile.get("ecology_stations", [])

    # Проверяем stale (TTL 7 дней)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS cameras_meta (
            id INTEGER PRIMARY KEY DEFAULT 1, last_updated TIMESTAMP, total_rows INTEGER)""")
        row = conn.execute("SELECT last_updated FROM cameras_meta WHERE id = 1").fetchone()
        if row and row[0]:
            last = row[0] if not isinstance(row[0], str) else datetime.fromisoformat(row[0].replace("Z", "+00:00"))
            if hasattr(last, 'tzinfo') and last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - last < timedelta(days=7):
                return -1
    except Exception:
        pass
    finally:
        conn.close()

    query = f"""[out:json][timeout:40];
node["highway"="speed_camera"]{bbox_str};
out body;"""

    data = _overpass_fetch(query, timeout=45)
    if not data:
        return 0

    cameras = []
    for node in data.get("elements", []):
        if node.get("type") != "node":
            continue
        tags = node.get("tags", {})
        lat, lon = float(node["lat"]), float(node["lon"])
        district = _classify_point(lat, lon, boundaries, eco_stations, bbox)
        cameras.append((
            str(node["id"]), lat, lon,
            tags.get("maxspeed", ""), tags.get("name", ""),
            tags.get("direction", ""),
            tags.get("ref", tags.get("int_ref", "")),
            district,
        ))

    if not cameras:
        return 0

    conn = duckdb.connect(str(db_path))
    try:
        conn.execute("""CREATE TABLE IF NOT EXISTS cameras (
            osm_id TEXT PRIMARY KEY, _lat DOUBLE, _lon DOUBLE,
            maxspeed TEXT, name TEXT, direction TEXT, ref TEXT,
            district TEXT DEFAULT '', loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)""")
        conn.execute("DELETE FROM cameras")
        conn.executemany(
            "INSERT INTO cameras (osm_id,_lat,_lon,maxspeed,name,direction,ref,district) VALUES (?,?,?,?,?,?,?,?)",
            cameras,
        )
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("INSERT OR REPLACE INTO cameras_meta (id, last_updated, total_rows) VALUES (1, ?, ?)",
                      [now, len(cameras)])
        log.info("multi-city [%s]: камеры — %d записей", city_id, len(cameras))
        return len(cameras)
    finally:
        conn.close()


def _refresh_osm_topics_isolated(profile: dict, city_id: str) -> dict[str, int]:
    """Обновляет все OSM-темы для города (изолированно от глобального контекста)."""
    from .osm_universal import OSM_TOPICS, fetch_osm_topic, upsert_osm_topic, is_osm_topic_stale

    db_path = _city_db_path(city_id)
    bbox_str = _city_bbox_overpass(profile)
    bbox = profile["city"]["bbox"]
    boundaries = _load_boundaries_from_file(city_id)
    eco_stations = profile.get("ecology_stations", [])

    results = {}
    for topic in OSM_TOPICS:
        try:
            if not is_osm_topic_stale(topic, db_path=db_path):
                results[topic] = -1  # свежие
                continue
            rows = fetch_osm_topic(topic, bbox_str, bbox, boundaries, eco_stations)
            if rows:
                n = upsert_osm_topic(topic, rows, db_path=db_path)
                results[topic] = n
                log.info("multi-city [%s]: OSM %s — %d записей", city_id, topic, n)
            else:
                results[topic] = 0
        except Exception as e:
            log.warning("multi-city [%s] OSM %s: %s", city_id, topic, e)
            results[topic] = 0
        # Пауза между Overpass-запросами (rate limit)
        import time
        time.sleep(5)
    return results


def _refresh_one_city(city: dict) -> dict:
    """Обновляет все бесплатные источники для одного города (изолированно)."""
    city_id = city["city_id"]
    city_name = city["city_name"]
    profile = city["profile"]
    results = {}

    try:
        results["medical"] = _refresh_medical_isolated(profile, city_id)
    except Exception as e:
        log.warning("multi-city [%s] medical: %s", city_name, e)
        results["medical"] = 0

    try:
        results["cameras"] = _refresh_cameras_isolated(profile, city_id)
    except Exception as e:
        log.warning("multi-city [%s] cameras: %s", city_name, e)
        results["cameras"] = 0

    try:
        osm_results = _refresh_osm_topics_isolated(profile, city_id)
        results["osm"] = osm_results
    except Exception as e:
        log.warning("multi-city [%s] osm: %s", city_name, e)
        results["osm"] = {}

    return results


async def multi_city_refresh_loop(
    interval_hours: float = _MULTI_CITY_INTERVAL_HOURS,
    initial_delay: float = 120.0,
) -> None:
    """Фоновый цикл: обновляет экологию/медицину/камеры для ВСЕХ городов.

    НЕ меняет глобальный CITY_PROFILE — каждый город обрабатывается
    изолированно (прямые подключения к db, прямые Overpass-запросы).
    Это предотвращает гонку потоков с пользовательскими запросами.

    Все источники бесплатные (Open-Meteo, OSM Overpass).
    """
    await asyncio.sleep(initial_delay)
    interval_sec = interval_hours * 3600
    log.info("multi_city_refresh_loop: старт (интервал %.0f ч, %d городов)",
             interval_hours, len(_list_city_profiles()))

    while True:
        cities = _list_city_profiles()
        active_id = os.environ.get("CITY_PROFILE", "city_profile")
        updated = 0

        for city in cities:
            # Активный город уже обновляется основным preloader'ом
            if city["profile_name"] == active_id:
                continue

            try:
                results = await asyncio.to_thread(_refresh_one_city, city)
                if any(v > 0 for v in results.values()):
                    updated += 1
            except Exception as e:
                log.warning("multi-city [%s]: ошибка — %s", city["city_name"], e)

            await asyncio.sleep(_CITY_INTERVAL)

        log.info("multi_city_refresh_loop: завершено, обновлено %d городов", updated)
        await asyncio.sleep(interval_sec)
