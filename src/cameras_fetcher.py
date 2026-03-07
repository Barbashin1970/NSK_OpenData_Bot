"""Загрузка данных о стационарных камерах фиксации нарушений ПДД из OpenStreetMap.

Источник: Overpass API (overpass-api.de)
Запрос: node["highway"="speed_camera"] в bbox Новосибирска (54.70, 82.60, 55.25, 83.40)
Лицензия данных: ODbL (OpenStreetMap contributors)

Возвращает список записей с полями:
  osm_id, _lat, _lon, maxspeed, name, direction
"""

import logging
from typing import Any

import requests

log = logging.getLogger(__name__)

_OVERPASS_URL = "https://overpass-api.de/api/interpreter"
_NSK_BBOX = "(54.70,82.60,55.25,83.40)"  # Новосибирск
_REQUEST_TIMEOUT = 30

_OVERPASS_QUERY = f"""
[out:json][timeout:25];
node["highway"="speed_camera"]{_NSK_BBOX};
out body;
"""


def fetch_cameras() -> list[dict[str, Any]]:
    """Загружает список камер фиксации из OSM Overpass API.

    Возвращает список записей или [] при ошибке.
    """
    try:
        resp = requests.post(
            _OVERPASS_URL,
            data={"data": _OVERPASS_QUERY},
            timeout=_REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.Timeout:
        log.warning("Overpass API: таймаут при загрузке камер")
        return []
    except Exception as e:
        log.error("Overpass API error: %s", e)
        return []

    cameras = []
    for node in data.get("elements", []):
        if node.get("type") != "node":
            continue
        tags = node.get("tags", {})
        cameras.append({
            "osm_id":    str(node["id"]),
            "_lat":      float(node["lat"]),
            "_lon":      float(node["lon"]),
            "maxspeed":  tags.get("maxspeed", ""),
            "name":      tags.get("name", ""),
            "direction": tags.get("direction", ""),
            "ref":       tags.get("ref", tags.get("int_ref", "")),
        })

    log.info("Overpass API: загружено %d камер в Новосибирске", len(cameras))
    return cameras
