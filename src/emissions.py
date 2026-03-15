"""
Справочник выбросов загрязняющих веществ в атмосферу по МО НСО.
Данные по форме 2-ТП Воздух за 2024 год (rpn.gov.ru).
Загружаются из JSON один раз при первом обращении.
"""
import json
import logging
from functools import lru_cache

from .city_config import get_emissions_path

log = logging.getLogger(__name__)

# Колонки для таблицы (без _lat/_lon — они в rows для карты, но не в таблице)
TABLE_COLUMNS = [
    "name",
    "vsego_t",
    "so2_t",
    "nox_t",
    "co_t",
    "data_status",
    "_lat",
    "_lon",
]


@lru_cache(maxsize=1)
def load_emissions() -> list[dict]:
    """Загружает JSON, возвращает список записей с полями + _lat/_lon."""
    path = get_emissions_path()
    if path is None:
        raise FileNotFoundError("Данные выбросов недоступны для этого города (emissions не настроен в city_profile)")
    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    records = []
    for m in data["municipalities"]:
        row = {k: m.get(k) for k in (
            "id", "name", "short", "type",
            "vsego_t", "tverdye_t", "so2_t", "co_t", "nox_t", "los_t", "prochie_t",
            "main_sources", "data_status",
        )}
        row["_lat"] = m["lat"]
        row["_lon"] = m["lon"]
        row["_year"] = data.get("year", 2024)
        records.append(row)

    log.info(f"Загружено {len(records)} записей выбросов 2-ТП Воздух")
    return records


def get_emissions_meta() -> dict:
    path = get_emissions_path()
    if path is None:
        return {}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return {
        "year": data.get("year"),
        "form": data.get("form"),
        "source": data.get("source"),
        "published": data.get("published"),
        "note": data.get("note"),
        "total_municipalities": len(data.get("municipalities", [])),
    }


def query_emissions(municipality: str = "", top_n: int = 0) -> list[dict]:
    """Возвращает отфильтрованный список МО."""
    records = load_emissions()
    result = records

    if municipality:
        result = [r for r in result if municipality.lower() in r.get("name", "").lower()]

    result = sorted(result, key=lambda r: r.get("vsego_t") or 0, reverse=True)

    if top_n > 0:
        result = result[:top_n]

    return result


def count_emissions(**kwargs) -> int:
    return len(query_emissions(**kwargs))
