"""Данные аэропорта — загружаются из data/cities/<city>/airport.json.

Публичный API не изменился:
    get_airport_info() → dict
"""
import json
import logging
from functools import lru_cache

from .city_config import get_airport_path

log = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _load() -> dict:
    """Загружает airport.json один раз. Raises FileNotFoundError если недоступен."""
    path = get_airport_path()
    if path is None:
        raise FileNotFoundError(
            "Данные аэропорта недоступны для этого города (airport не настроен в city_profile)"
        )
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    log.info(f"Загружены данные аэропорта {data.get('iata', '')} из {path.name}")
    return data


def get_airport_info() -> dict:
    """Полная информация об аэропорте."""
    return _load()
