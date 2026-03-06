"""2GIS Public Transport API — pass-through без хранения данных.

Лицензия 2ГИС (law.2gis.ru/api-rules) запрещает сохранять данные.
Этот модуль реализует ТОЛЬКО real-time проксирование:
- Запрос от пользователя → 2GIS API → ответ пользователю
- Никакие данные не сохраняются в БД

Требует переменную окружения: TWOGIS_API_KEY
Получить ключ: https://dev.2gis.com/api (demo-ключ бесплатен для тестирования)

Поддерживаемые операции:
  TRANSIT_ROUTE    — построить маршрут на общественном транспорте
  TRANSIT_STOPS    — найти ближайшие остановки
"""

import logging
import os
from typing import Any

import requests

from .constants import TIMEOUT

log = logging.getLogger(__name__)

_2GIS_BASE = "https://routing.api.2gis.com"
_2GIS_TRANSPORT_URL = f"{_2GIS_BASE}/public_transport"
_2GIS_DIRECTIONS_URL = f"{_2GIS_BASE}/directions/points"

# Координаты центров районов Новосибирска (lng, lat)
DISTRICT_COORDS: dict[str, tuple[float, float]] = {
    "Центральный район":      (82.9090, 54.9884),
    "Советский район":        (83.1091, 54.8441),
    "Дзержинский район":      (82.9650, 54.9823),
    "Железнодорожный район":  (82.8860, 54.9954),
    "Заельцовский район":     (82.9550, 54.9893),
    "Калининский район":      (82.8880, 55.0190),
    "Кировский район":        (82.8190, 54.9600),
    "Ленинский район":        (82.8800, 54.9460),
    "Октябрьский район":      (82.8190, 54.9940),
    "Первомайский район":     (82.8710, 54.8780),
    "Кольцово":               (83.1818, 54.9394),
}


def _get_api_key() -> str | None:
    return os.environ.get("TWOGIS_API_KEY", "").strip() or None


def transit_route(
    from_point: tuple[float, float],  # (lng, lat)
    to_point: tuple[float, float],
) -> dict[str, Any]:
    """Маршрут на общественном транспорте между двумя точками.

    Данные НЕ сохраняются (pass-through согласно лицензии 2ГИС).

    Returns:
        dict с полями: routes, total_duration, transfers, error
    """
    api_key = _get_api_key()
    if not api_key:
        return {
            "error": "TWOGIS_API_KEY не задан. Получите ключ на dev.2gis.com",
            "routes": [],
        }

    params = {
        "key": api_key,
        "points": f"{from_point[0]},{from_point[1]}|{to_point[0]},{to_point[1]}",
        "transport": "public_transport",
        "locale": "ru",
    }

    try:
        resp = requests.get(
            _2GIS_DIRECTIONS_URL,
            params=params,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as e:
        log.warning(f"2GIS HTTP error: {e}")
        return {"error": f"Ошибка 2GIS API: {e}", "routes": []}
    except Exception as e:
        log.error(f"2GIS transit_route error: {e}")
        return {"error": str(e), "routes": []}

    return _parse_transit_response(data)


def _parse_transit_response(data: dict) -> dict[str, Any]:
    """Нормализует ответ 2GIS transit API в читаемый формат."""
    result = data.get("result", {})
    routes_raw = result.get("routes", [])

    routes: list[dict] = []
    for r in routes_raw[:3]:  # топ-3 маршрута
        legs = r.get("legs", [])
        segments: list[dict] = []
        for leg in legs:
            steps = leg.get("steps", [])
            for step in steps:
                mode = step.get("travel_mode", "unknown")
                transport = step.get("line", {})
                segments.append({
                    "mode": mode,
                    "line": transport.get("name", ""),
                    "duration_sec": step.get("duration", 0),
                    "distance_m": step.get("distance", 0),
                })
        routes.append({
            "total_duration_min": round(r.get("total_duration", 0) / 60, 1),
            "total_distance_m": r.get("total_distance", 0),
            "transfers": r.get("transfers_count", 0),
            "segments": segments,
        })

    return {
        "routes": routes,
        "count": len(routes),
        "source": "2GIS Public Transport API (real-time, данные не сохраняются)",
    }


def transit_stops_near(
    lng: float,
    lat: float,
    radius_m: int = 500,
) -> dict[str, Any]:
    """Ближайшие остановки вокруг точки через 2GIS Places API.

    Данные НЕ сохраняются.
    """
    api_key = _get_api_key()
    if not api_key:
        return {
            "error": "TWOGIS_API_KEY не задан",
            "stops": [],
        }

    params = {
        "key": api_key,
        "q": "остановка",
        "point": f"{lng},{lat}",
        "radius": radius_m,
        "type": "station,stop",
        "locale": "ru",
        "fields": "items.point,items.name,items.transit",
        "page_size": 20,
    }

    try:
        resp = requests.get(
            "https://catalog.api.2gis.com/3.0/items",
            params=params,
            timeout=TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        log.error(f"2GIS stops error: {e}")
        return {"error": str(e), "stops": []}

    items = data.get("result", {}).get("items", [])
    stops = [
        {
            "name": item.get("name", ""),
            "lng": item.get("point", {}).get("lon"),
            "lat": item.get("point", {}).get("lat"),
            "routes": [
                r.get("name", "") for r in item.get("transit", {}).get("routes", [])
            ],
        }
        for item in items
    ]
    return {
        "stops": stops,
        "count": len(stops),
        "source": "2GIS Places API (real-time, данные не сохраняются)",
    }
