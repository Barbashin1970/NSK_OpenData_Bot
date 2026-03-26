"""Классификатор района/округа по координатам на основе полигонов границ.

Использует GeoJSON-файл с границами административных единиц:
    data/cities/{city_id}/district_boundaries.geojson

Если файл отсутствует — фолбэк на ближайший центроид (старое поведение).

Обновление границ:
    GET /admin/update-boundaries  (в api.py)
    или вызов fetch_and_cache_boundaries() из кода

Источник данных: OpenStreetMap (Overpass API)
Лицензия: ODbL (OpenStreetMap contributors)
"""

import json
import logging
import time
from functools import lru_cache
from pathlib import Path
from typing import Any

import requests

from .city_config import (
    get_bbox_dict,
    get_bbox_overpass,
    get_city_id,
    get_city_name,
    get_districts,
    get_ecology_stations,
)

log = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).parent.parent

_OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]


# ── Путь к файлу границ ──────────────────────────────────────────────────────

def _boundaries_path() -> Path:
    city_id = get_city_id()
    return _PROJECT_ROOT / "data" / "cities" / city_id / "district_boundaries.geojson"


# ── Загрузка границ (кэш) ────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _load_boundaries() -> list[dict] | None:
    """Загружает границы из GeoJSON.

    Возвращает список {district, polygon: [(lon,lat), ...]} или None.
    """
    path = _boundaries_path()
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        features = data.get("features", [])
        result = []
        for f in features:
            district = f.get("properties", {}).get("district", "")
            geom = f.get("geometry", {})
            geom_type = geom.get("type", "")
            if geom_type == "Polygon":
                # Берём внешнее кольцо (первый массив координат)
                coords = geom.get("coordinates", [[]])[0]
                result.append({"district": district, "polygon": coords})
            elif geom_type == "MultiPolygon":
                # Каждый полигон — отдельная запись с тем же районом
                for poly in geom.get("coordinates", []):
                    if poly:
                        result.append({"district": district, "polygon": poly[0]})
        log.info("district_classifier: загружено %d полигонов из %s", len(result), path)
        return result if result else None
    except Exception as e:
        log.warning("district_classifier: ошибка чтения %s — %s", path, e)
        return None


def reload_boundaries() -> None:
    """Сбросить кэш загруженных границ (после обновления файла)."""
    _load_boundaries.cache_clear()


# ── Point-in-polygon (Ray Casting) ───────────────────────────────────────────

def _point_in_polygon(lat: float, lon: float, polygon: list[list[float]]) -> bool:
    """Ray casting algorithm. polygon — список [lon, lat] пар."""
    n = len(polygon)
    if n < 3:
        return False
    inside = False
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i][0], polygon[i][1]   # lon, lat
        xj, yj = polygon[j][0], polygon[j][1]
        if ((yi > lat) != (yj > lat)) and \
           (lon < (xj - xi) * (lat - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


def _districts_by_polygon_bbox(lat: float, lon: float, boundaries: list[dict]) -> list[str]:
    """Возвращает список районов, чьи полигоны содержат точку в своём bbox."""
    hits: set[str] = set()
    for b in boundaries:
        poly = b["polygon"]
        lons = [p[0] for p in poly]
        lats = [p[1] for p in poly]
        if min(lats) <= lat <= max(lats) and min(lons) <= lon <= max(lons):
            hits.add(b["district"])
    return sorted(hits)


# ── Основная функция классификации ───────────────────────────────────────────

def classify_district(lat: float | None, lon: float | None) -> str:
    """Определяет район/округ по координатам.

    1. Проверяет bbox города
    2. Пробует point-in-polygon по GeoJSON границам
    3. Фолбэк на ближайший центроид экологической станции

    Возвращает название района ("Октябрьский округ") или "Прочие".
    """
    if lat is None or lon is None:
        return "Прочие"

    bb = get_bbox_dict()
    if not (bb["lat_min"] <= lat <= bb["lat_max"] and
            bb["lon_min"] <= lon <= bb["lon_max"]):
        return "Прочие"

    # Попытка 1: границы-полигоны
    boundaries = _load_boundaries()
    if boundaries:
        for b in boundaries:
            if _point_in_polygon(lat, lon, b["polygon"]):
                return b["district"]
        # Точка внутри bbox города, но вне всех полигонов.
        # Попытка 1.5: если точка попадает в bbox только одного района — берём его.
        # Это корректирует ошибки у краёв полигонов (река, граница и т.п.)
        bbox_districts = _districts_by_polygon_bbox(lat, lon, boundaries)
        if len(bbox_districts) == 1:
            log.debug("classify_district: (%.4f, %.4f) в bbox единственного района %s",
                       lat, lon, bbox_districts[0])
            return bbox_districts[0]
        # Если попадает в bbox нескольких районов — предпочитаем центроиды из этих районов
        if bbox_districts:
            log.debug("classify_district: (%.4f, %.4f) в bbox районов %s, фолбэк по центроидам этой группы",
                       lat, lon, bbox_districts)
            best_dist = float("inf")
            best_district = bbox_districts[0]
            for st in get_ecology_stations():
                if st["district"] in bbox_districts:
                    d = (lat - st["latitude"]) ** 2 + (lon - st["longitude"]) ** 2
                    if d < best_dist:
                        best_dist = d
                        best_district = st["district"]
            return best_district
        log.debug("classify_district: (%.4f, %.4f) вне всех полигонов и bbox, фолбэк", lat, lon)

    # Попытка 2 (фолбэк): ближайший центроид
    best_dist = float("inf")
    best_district = "Прочие"
    for st in get_ecology_stations():
        d = (lat - st["latitude"]) ** 2 + (lon - st["longitude"]) ** 2
        if d < best_dist:
            best_dist = d
            best_district = st["district"]
    return best_district


# ── Загрузка границ из Overpass API ──────────────────────────────────────────

def _close_enough(p1: list[float], p2: list[float], eps: float = 1e-6) -> bool:
    return abs(p1[0] - p2[0]) < eps and abs(p1[1] - p2[1]) < eps


def _stitch_outer_ring(members: list[dict]) -> list[list[float]]:
    """Сшивает сегменты outer-кольца из членов OSM-relation в замкнутый полигон."""
    segments: list[list[list[float]]] = []
    for m in members:
        if m.get("role") != "outer" or "geometry" not in m:
            continue
        coords = [[p["lon"], p["lat"]] for p in m["geometry"]]
        if len(coords) >= 2:
            segments.append(coords)

    if not segments:
        return []

    # Greedy stitching
    ring = list(segments.pop(0))
    changed = True
    while changed and segments:
        changed = False
        for i, seg in enumerate(segments):
            if _close_enough(ring[-1], seg[0]):
                ring.extend(seg[1:])
                segments.pop(i)
                changed = True
                break
            elif _close_enough(ring[-1], seg[-1]):
                ring.extend(list(reversed(seg))[1:])
                segments.pop(i)
                changed = True
                break
            elif _close_enough(ring[0], seg[-1]):
                ring = seg[:-1] + ring
                segments.pop(i)
                changed = True
                break
            elif _close_enough(ring[0], seg[0]):
                ring = list(reversed(seg))[:-1] + ring
                segments.pop(i)
                changed = True
                break

    return ring


def _match_district_name(osm_name: str, districts: dict[str, list[str]]) -> str | None:
    """Сопоставляет OSM-имя с районами из профиля города."""
    name_lower = osm_name.lower()
    for dist_name, stems in districts.items():
        for stem in stems:
            if stem.lower() in name_lower:
                return dist_name
    return None


def fetch_and_cache_boundaries() -> dict:
    """Загружает границы районов из Overpass API и сохраняет GeoJSON.

    Returns:
        {"ok": True, "districts": N, "path": str}  при успехе
        {"ok": False, "error": str}  при ошибке
    """
    city_name = get_city_name()
    bbox = get_bbox_overpass()
    districts = get_districts()

    # Запрос: все admin boundaries внутри bbox
    # admin_level 8 и 9 — районы/округа городов России
    query = f"""
[out:json][timeout:90];
(
  relation["boundary"="administrative"]["admin_level"~"^(8|9)$"]{bbox};
);
out geom;
"""
    data = None
    for url in _OVERPASS_MIRRORS:
        try:
            log.info("Overpass boundaries: запрос к %s", url)
            resp = requests.post(url, data={"data": query}, timeout=90)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            log.warning("Overpass boundaries: %s — %s", url, e)
            continue

    if data is None:
        return {"ok": False, "error": "Все зеркала Overpass недоступны"}

    elements = data.get("elements", [])
    log.info("Overpass boundaries: получено %d relations", len(elements))

    features = []
    matched_districts: set[str] = set()

    for el in elements:
        if el.get("type") != "relation":
            continue
        tags = el.get("tags", {})
        osm_name = tags.get("name", "")

        district = _match_district_name(osm_name, districts)
        if not district or district in matched_districts:
            continue

        ring = _stitch_outer_ring(el.get("members", []))
        if len(ring) < 3:
            log.warning("Boundaries: %s (%s) — не удалось собрать полигон", osm_name, district)
            continue

        # Замыкаем кольцо если нужно
        if not _close_enough(ring[0], ring[-1]):
            ring.append(ring[0])

        features.append({
            "type": "Feature",
            "properties": {
                "district": district,
                "osm_name": osm_name,
                "admin_level": tags.get("admin_level", ""),
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [ring],
            },
        })
        matched_districts.add(district)

    if not features:
        return {"ok": False, "error": f"Не найдено подходящих границ для {city_name} "
                f"(получено {len(elements)} relations, совпало 0 из {len(districts)} районов)"}

    geojson = {
        "type": "FeatureCollection",
        "properties": {
            "city": city_name,
            "city_id": get_city_id(),
            "source": "OpenStreetMap (Overpass API)",
            "license": "ODbL",
            "fetched_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "features": features,
    }

    path = _boundaries_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(geojson, ensure_ascii=False, indent=2), encoding="utf-8")
    reload_boundaries()

    missing = set(districts.keys()) - matched_districts
    result: dict[str, Any] = {
        "ok": True,
        "districts": len(features),
        "total_expected": len(districts),
        "path": str(path),
    }
    if missing:
        result["missing"] = sorted(missing)

    log.info("district_classifier: сохранено %d/%d границ → %s",
             len(features), len(districts), path)
    return result
