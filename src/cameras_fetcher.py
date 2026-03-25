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

from .city_config import get_bbox_overpass

log = logging.getLogger(__name__)

# Публичные зеркала Overpass API — пробуем по порядку при ошибке
_OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
_REQUEST_TIMEOUT = 45


def _build_cameras_query() -> str:
    bbox = get_bbox_overpass()
    return f"""
[out:json][timeout:40];
node["highway"="speed_camera"]{bbox};
out body;
"""


def fetch_cameras() -> list[dict[str, Any]]:
    """Загружает список камер фиксации из OSM Overpass API.

    Пробует несколько зеркал при недоступности основного сервера.
    Bbox берётся из активного city_profile при каждом вызове.
    Возвращает список записей или [] при ошибке.
    """
    query = _build_cameras_query()
    last_err: Exception | None = None
    for url in _OVERPASS_MIRRORS:
        try:
            log.info("Overpass: запрос к %s", url)
            resp = requests.post(
                url,
                data={"data": query},
                timeout=_REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.Timeout as e:
            log.warning("Overpass API: таймаут %s — %s", url, e)
            last_err = e
            continue
        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response is not None else None
            log.warning("Overpass API: HTTP %s от %s — %s", status, url, e)
            last_err = e
            continue
        except Exception as e:
            log.warning("Overpass API: ошибка %s — %s", url, e)
            last_err = e
            continue

        elements = data.get("elements", [])
        cameras = []
        for node in elements:
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

        log.info("Overpass API (%s): загружено %d камер", url, len(cameras))
        return cameras

    log.error("Overpass API: все зеркала недоступны. Последняя ошибка: %s", last_err)
    return []
