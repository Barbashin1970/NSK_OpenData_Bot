"""Загрузка данных о медицинских учреждениях Новосибирска из OpenStreetMap.

Источник: Overpass API (overpass-api.de)
Запрос: amenity=hospital|clinic в bbox Новосибирска (54.70, 82.60, 55.25, 83.40)
Лицензия данных: ODbL (OpenStreetMap contributors)
TTL: 72 часа (больницы/поликлиники меняются редко)
"""

import logging
from typing import Any

import requests

from .city_config import get_bbox_overpass

log = logging.getLogger(__name__)

_OVERPASS_MIRRORS = [
    "https://overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://z.overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]
_REQUEST_TIMEOUT = 60


def _build_medical_query() -> str:
    bbox = get_bbox_overpass()
    return f"""
[out:json][timeout:55];
(
  node["amenity"~"hospital|clinic"]{bbox};
  way["amenity"~"hospital|clinic"]{bbox};
);
out center tags;
"""

_TYPE_LABELS: dict[str, str] = {
    "hospital": "Больница",
    "clinic":   "Поликлиника",
    "doctors":  "Медицинский кабинет",
}


def fetch_medical() -> list[dict[str, Any]]:
    """Загружает список медучреждений из OSM Overpass API.

    Пробует несколько зеркал при недоступности основного сервера.
    Bbox берётся из активного city_profile при каждом вызове.
    Возвращает список записей или [] при ошибке.
    """
    query = _build_medical_query()
    last_err: Exception | None = None
    for url in _OVERPASS_MIRRORS:
        try:
            log.info("Overpass (medical): запрос к %s", url)
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
        result = []
        for el in elements:
            el_type = el.get("type")
            tags = el.get("tags", {})
            # node: координаты в элементе; way: в поле center
            if el_type == "node":
                lat = el.get("lat")
                lon = el.get("lon")
            elif el_type == "way":
                center = el.get("center", {})
                lat = center.get("lat")
                lon = center.get("lon")
            else:
                continue
            if lat is None or lon is None:
                continue

            name = tags.get("name", "").strip()
            if not name:
                continue  # безымянные объекты пропускаем

            amenity = tags.get("amenity", "")
            street = tags.get("addr:street", "")
            housenumber = tags.get("addr:housenumber", "")
            address = f"{street} {housenumber}".strip()

            result.append({
                "osm_id":        f"{el_type[0]}{el['id']}",  # n12345 / w12345
                "name":          name,
                "facility_type": amenity,
                "type_label":    _TYPE_LABELS.get(amenity, amenity),
                "emergency":     tags.get("emergency", ""),
                "phone":         tags.get("phone", tags.get("contact:phone", "")),
                "address":       address,
                "_lat":          float(lat),
                "_lon":          float(lon),
            })

        log.info("Overpass medical (%s): загружено %d объектов", url, len(result))
        return result

    log.error("Overpass API (medical): все зеркала недоступны. Последняя ошибка: %s", last_err)
    return []
