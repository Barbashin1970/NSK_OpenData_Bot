"""
Справочник тепловых источников Новосибирской области.
Данные загружаются из GeoJSON один раз при первом обращении.
"""
import json
import logging
from functools import lru_cache
from pathlib import Path

log = logging.getLogger(__name__)

_GEOJSON_PATH = Path(__file__).parent.parent / "data" / "nsk_heat_sources_v1.geojson"

# Поля, которые возвращаем в таблицу (без _lat/_lon — они в rows для карты, но не в columns)
TABLE_COLUMNS = [
    "short_name",
    "type",
    "fuel",
    "district",
    "thermal_gcal_h",
    "electric_mw",
    "operator_group",
    "digitalization_status",
    "_lat",
    "_lon",
]


@lru_cache(maxsize=1)
def load_heat_sources() -> list[dict]:
    """Загружает GeoJSON, возвращает список объектов с полями properties + _lat/_lon."""
    with open(_GEOJSON_PATH, encoding="utf-8") as f:
        fc = json.load(f)

    sources = []
    for feat in fc["features"]:
        props = feat["properties"].copy()
        lon, lat = feat["geometry"]["coordinates"]  # GeoJSON: [lon, lat]
        props["_lon"] = lon
        props["_lat"] = lat
        sources.append(props)

    log.info(f"Загружено {len(sources)} тепловых источников")
    return sources


def query_heat_sources(
    operator_group: str = "",
    source_type: str = "",
    pilot_only: bool = False,
) -> list[dict]:
    """Возвращает отфильтрованный список источников."""
    sources = load_heat_sources()
    result = sources

    if operator_group:
        result = [s for s in result if s.get("operator_group", "") == operator_group]
    if source_type:
        result = [s for s in result if source_type.lower() in s.get("type", "").lower()]
    if pilot_only:
        result = [s for s in result if s.get("digitalization_status") == "пилот"]

    return result


def get_source_by_id(source_id: str) -> dict | None:
    return next((s for s in load_heat_sources() if s.get("id") == source_id), None)


def count_heat_sources(**kwargs) -> int:
    return len(query_heat_sources(**kwargs))
