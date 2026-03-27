"""2GIS Public Transport API — pass-through без хранения данных.

Лицензия 2ГИС (law.2gis.ru/api-rules) запрещает сохранять данные.
Этот модуль реализует ТОЛЬКО real-time проксирование:
- Запрос от пользователя → 2GIS API → ответ пользователю
- Никакие данные не сохраняются в БД

Стратегия (две ступени):
1. Пробуем 2GIS Public Transport Routing API (POST routing.api.2gis.com).
   Если ключ не поддерживает routing (HTTP 403/404) — переходим к шагу 2.
2. Fallback: Catalog API (catalog.api.2gis.com/3.0/items) находит остановки
   рядом с точками отправления и назначения + оценка времени в пути.
   Это работает с любым бесплатным ключом 2GIS.

Требует переменную окружения: TWOGIS_API_KEY
Получить ключ: https://platform.2gis.ru
"""

import logging
import math
import os
from typing import Any

import requests

from .city_config import get_district_coords, get_city_slug as _get_city_slug
from .constants import TIMEOUT

log = logging.getLogger(__name__)

_ROUTING_URL  = "https://routing.api.2gis.com/public_transport/3.0.0/"
_CATALOG_URL  = "https://catalog.api.2gis.com/3.0/items"

# Координаты центров районов (lng, lat) — из city_profile.yaml текущего города.
# Вызываем get_district_coords() каждый раз, чтобы при смене города данные обновлялись.
def _get_district_coords() -> dict[str, tuple[float, float]]:
    return get_district_coords()

# Обратная совместимость: DISTRICT_COORDS как property-like, но для простоты
# импортёры вызывают get_district_coords() напрямую или _get_district_coords().
DISTRICT_COORDS = _get_district_coords()  # legacy, prefer _get_district_coords()


def _get_api_key() -> str | None:
    return os.environ.get("TWOGIS_API_KEY", "").strip() or None


def _haversine_km(p1: tuple[float, float], p2: tuple[float, float]) -> float:
    """Расстояние по прямой между двумя точками (lng, lat) в км."""
    lon1, lat1 = p1
    lon2, lat2 = p2
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    return round(R * 2 * math.asin(math.sqrt(a)), 1)


def _get_stops_near(api_key: str, lng: float, lat: float, radius: int = 700) -> list[dict]:
    """Возвращает ближайшие остановки через 2GIS Catalog API (бесплатный доступ)."""
    try:
        resp = requests.get(
            _CATALOG_URL,
            params={
                "key":       api_key,
                "point":     f"{lng},{lat}",
                "radius":    radius,
                "type":      "station,stop",
                "fields":    "items.point,items.name,items.transit",
                "page_size": 6,
                "locale":    "ru",
            },
            timeout=8,
        )
        resp.raise_for_status()
        items = resp.json().get("result", {}).get("items", [])
        stops = []
        for item in items:
            routes = [
                r.get("name", "")
                for r in item.get("transit", {}).get("routes", [])[:8]
                if r.get("name")
            ]
            stops.append({
                "name":   item.get("name", ""),
                "routes": routes,
            })
        return stops
    except Exception as e:
        log.warning("_get_stops_near (%s,%s): %s", lng, lat, e)
        return []


def _try_routing_api(
    api_key: str,
    from_point: tuple[float, float],
    to_point: tuple[float, float],
) -> dict[str, Any]:
    """Пробует построить маршрут через 2GIS Public Transport Routing API.

    Возвращает dict с ключом 'routes' (список маршрутов) или 'routing_error'.
    HTTP 403/404 означает, что ключ не поддерживает routing — нужен fallback.
    """
    payload = {
        "locale": "ru",
        "points": [
            {"lon": from_point[0], "lat": from_point[1], "type": "stop"},
            {"lon": to_point[0],   "lat": to_point[1],   "type": "stop"},
        ],
    }
    try:
        resp = requests.post(
            _ROUTING_URL,
            params={"key": api_key},
            json=payload,
            timeout=TIMEOUT,
        )
        log.info("2GIS routing: HTTP %s from %s", resp.status_code, _ROUTING_URL)
        if resp.status_code in (403, 404):
            return {
                "routes": [],
                "routing_error": (
                    f"HTTP {resp.status_code}: ключ не поддерживает Routing API. "
                    "Используется расширенный fallback (остановки + оценка времени)."
                ),
            }
        resp.raise_for_status()
        data = resp.json()
        return _parse_routing_response(data)
    except requests.exceptions.Timeout:
        return {"routes": [], "routing_error": "Таймаут Routing API"}
    except requests.exceptions.HTTPError as e:
        return {"routes": [], "routing_error": f"HTTP ошибка: {e}"}
    except Exception as e:
        log.warning("2GIS routing error: %s", e)
        return {"routes": [], "routing_error": str(e)}


def _parse_routing_response(data: dict) -> dict[str, Any]:
    """Нормализует ответ 2GIS Routing API в читаемый формат."""
    result = data.get("result", {})
    routes_raw = result.get("routes", [])

    routes: list[dict] = []
    for r in routes_raw[:3]:
        legs = r.get("legs", [])
        segments: list[dict] = []
        for leg in legs:
            for step in leg.get("steps", []):
                mode = step.get("travel_mode", "unknown")
                line = step.get("line", {})
                segments.append({
                    "mode":         mode,
                    "line":         line.get("name", ""),
                    "duration_sec": step.get("duration", 0),
                    "distance_m":   step.get("distance", 0),
                })
        routes.append({
            "total_duration_min": round(r.get("total_duration", 0) / 60, 1),
            "total_distance_m":   r.get("total_distance", 0),
            "transfers":          r.get("transfers_count", 0),
            "segments":           segments,
        })

    return {
        "routes": routes,
        "count":  len(routes),
        "routing_available": True,
        "source": "2GIS Public Transport Routing API (real-time, данные не сохраняются)",
    }


def transit_route(
    from_point: tuple[float, float],  # (lng, lat)
    to_point: tuple[float, float],
    from_name: str = "",
    to_name: str = "",
) -> dict[str, Any]:
    """Маршрут на общественном транспорте между двумя точками.

    Данные НЕ сохраняются (pass-through согласно лицензии 2ГИС).

    Стратегия:
    1. Пробует 2GIS Routing API (routing.api.2gis.com)
    2. Если недоступен — возвращает остановки рядом с точками + оценку времени
       через бесплатный Catalog API

    Returns:
        dict с полями: routes, origin_stops, destination_stops,
                       straight_line_km, estimated_transit_min,
                       routing_available, source, hint
    """
    api_key = _get_api_key()
    if not api_key:
        return {
            "error": "TWOGIS_API_KEY не задан. Получите ключ на platform.2gis.ru",
            "routes": [],
            "routing_available": False,
        }

    dist_km = _haversine_km(from_point, to_point)
    # Оценка времени в пути: ~18 км/ч средняя скорость наземного транспорта в НСК
    estimated_min = max(5, round(dist_km / 18 * 60))

    # ── Шаг 1: пробуем Routing API ───────────────────────────────────────────
    routing = _try_routing_api(api_key, from_point, to_point)

    # ── Шаг 2: Catalog API — остановки вблизи точек (всегда) ────────────────
    origin_stops      = _get_stops_near(api_key, from_point[0], from_point[1])
    destination_stops = _get_stops_near(api_key, to_point[0],   to_point[1])

    result: dict[str, Any] = {
        "straight_line_km":    dist_km,
        "estimated_transit_min": estimated_min,
        "origin_stops":        origin_stops,
        "destination_stops":   destination_stops,
        "hint": (
            "Для точного пошагового маршрута откройте 2ГИС: "
            f"https://2gis.ru/{_get_city_slug()}/routeSearch/rsType/publictransport/"
            f"from/{from_point[0]},{from_point[1]}/to/{to_point[0]},{to_point[1]}"
        ),
        "source": "2GIS Catalog API (real-time, данные не сохраняются)",
    }

    if routing.get("routing_available"):
        result.update(routing)
    else:
        result["routes"] = []
        result["routing_available"] = False
        if routing.get("routing_error"):
            result["routing_note"] = routing["routing_error"]

    return result


def transit_stops_near(
    lng: float,
    lat: float,
    radius_m: int = 500,
) -> dict[str, Any]:
    """Ближайшие остановки вокруг точки через 2GIS Catalog API.

    Данные НЕ сохраняются.
    """
    api_key = _get_api_key()
    if not api_key:
        return {"error": "TWOGIS_API_KEY не задан", "stops": []}

    stops = _get_stops_near(api_key, lng, lat, radius=radius_m)
    return {
        "stops":  stops,
        "count":  len(stops),
        "source": "2GIS Catalog API (real-time, данные не сохраняются)",
    }
