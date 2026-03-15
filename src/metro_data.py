"""Данные метрополитена — загружаются из data/cities/<city>/metro.json.

Публичный API не изменился:
    get_metro_info()  → dict с ключами info, lines, stations
    get_stations()    → list[dict] с опциональными фильтрами
"""
import json
import logging
from functools import lru_cache

from .city_config import get_metro_path

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _load() -> dict:
    """Загружает metro.json один раз. Raises FileNotFoundError если недоступен."""
    path = get_metro_path()
    if path is None:
        raise FileNotFoundError(
            "Данные метро недоступны для этого города (metro не настроен в city_profile)"
        )
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"Загружено {len(data.get('stations', []))} станций метро из {path.name}")
    return data


def get_metro_info() -> dict:
    """Полная информация о метро: мета + линии + список станций."""
    d = _load()
    return {**d["info"], "lines": d["lines"], "stations": d["stations"]}


def get_stations(
    line_filter: str | None = None,
    district_filter: str | None = None,
) -> list[dict]:
    """Список станций с опциональным фильтром по линии или району."""
    stations = _load()["stations"]
    if line_filter:
        stations = [s for s in stations if s["line"] == line_filter]
    if district_filter:
        df = district_filter.lower()
        stations = [s for s in stations if df in s["district"].lower()]
    return stations
