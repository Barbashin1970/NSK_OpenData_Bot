"""Получение реального индекса пробок из Яндекс.Карты (публичный XML API).

Источник: https://export.yandex.ru/bar/reginfo.xml?region=<id>
Возвращает уровень пробок 0–10, текстовое описание и цвет.

Нет авторизации, нет ключа. Публичный виджет Яндекса.
Данные обновляются каждые ~2 минуты на стороне Яндекса.
TTL кэша в нашем коде: 3 минуты (не чаще 1 запроса в 3 мин).
"""

import logging
import re
import time
from typing import Any

import requests

from .city_config import get_yandex_region_id, get_city_name

log = logging.getLogger(__name__)

_YANDEX_URL = "https://export.yandex.ru/bar/reginfo.xml"
_REQUEST_TIMEOUT = 10
_CACHE_TTL = 180  # 3 минуты

# Простой in-memory кэш (region_id → (timestamp, data))
_cache: dict[int, tuple[float, dict[str, Any]]] = {}

# Яндекс цвета → emoji
_ICON_MAP = {
    "green":  "🟢",
    "yellow": "🟡",
    "red":    "🔴",
}


def _classify_level(level: int) -> tuple[str, str]:
    """Яндекс level → текстовый уровень и emoji."""
    if level <= 1:
        return "Свободно", "🟢"
    if level <= 3:
        return "Умеренно", "🟡"
    if level <= 5:
        return "Затруднено", "🟠"
    if level <= 7:
        return "Сложно", "🔴"
    if level <= 9:
        return "Очень сложно", "🔴"
    return "Коллапс", "⛔"


def fetch_yandex_traffic(region_id: int | None = None) -> dict[str, Any] | None:
    """Получает текущий уровень пробок из Яндекс.Карт.

    Args:
        region_id: Яндекс region ID. Если None — берётся из city_profile.

    Returns:
        dict с ключами: level, hint, icon, emoji, level_label, time, city, url
        или None при ошибке.
    """
    if region_id is None:
        region_id = get_yandex_region_id()
    if not region_id:
        return None

    # Проверяем кэш
    now = time.time()
    cached = _cache.get(region_id)
    if cached and (now - cached[0]) < _CACHE_TTL:
        return cached[1]

    try:
        resp = requests.get(
            _YANDEX_URL,
            params={"region": region_id},
            timeout=_REQUEST_TIMEOUT,
            headers={"User-Agent": "NSK-OpenData-Bot/1.0"},
        )
        resp.raise_for_status()
        xml = resp.text
    except Exception as e:
        log.warning("yandex_traffic: ошибка запроса region=%s: %s", region_id, e)
        return None

    # Парсим XML регулярками (избегаем зависимости от xml.etree для простоты)
    level_m = re.search(r"<level>(\d+)</level>", xml)
    hint_m = re.search(r'<hint lang="ru">([^<]+)</hint>', xml)
    icon_m = re.search(r"<icon>(\w+)</icon>", xml)
    time_m = re.search(r"<time>([^<]+)</time>", xml)
    url_m = re.search(r"<url>([^<]+)</url>", xml)
    title_m = re.search(r"<title>([^<]+)</title>", xml)

    if not level_m:
        log.warning("yandex_traffic: не найден <level> в ответе region=%s", region_id)
        return None

    level = int(level_m.group(1))
    level_label, emoji = _classify_level(level)

    result: dict[str, Any] = {
        "level": level,
        "hint": hint_m.group(1) if hint_m else "",
        "icon": icon_m.group(1) if icon_m else "",
        "emoji": emoji,
        "level_label": level_label,
        "time": time_m.group(1) if time_m else "",
        "city": title_m.group(1) if title_m else "",
        "url": url_m.group(1) if url_m else "",
        "region_id": region_id,
        "source": "Яндекс.Карты",
    }

    _cache[region_id] = (now, result)
    log.info(
        "yandex_traffic: %s — уровень %d (%s) %s",
        result["city"], level, level_label, emoji,
    )
    return result
