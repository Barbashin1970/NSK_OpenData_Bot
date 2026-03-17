"""Скрапер отключений ЖКХ для Омска — АО «Омск РТС» (omskrts.ru).

API omskrts.ru — два POST-вызова на страницу /users/people/disconnections/:
  action=findAddress, mask=''   → JSON-список всех адресов с текущими отключениями
  action=info, mask=<адрес>     → HTML с деталями: компания, период, статус

Схема возвращаемых записей совместима с power_scraper.py:
  id, utility, utility_id, group_type, district, district_href,
  houses, date_from, date_to, company, scraped_at, source_url

Отличие от НСК-скрапера:
  - district всегда "all" (omskrts не даёт срезы по районам)
  - houses = количество адресов в группе (utility + period)
  - дополнительные поля: date_from, date_to, company
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from .city_config import get_feature as _get_city_feature
from .constants import SCRAPER_HEADERS, SCRAPER_TIMEOUT

log = logging.getLogger(__name__)

# Regex для парсинга HTML-ответа info
_DATE_RE    = re.compile(r'c\s+(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})\s+по\s+(\d{2}\.\d{2}\.\d{4}\s+\d{2}:\d{2})')
_STATUS_RE  = re.compile(r'[Сс]татус:\s*([^<\n]+)')
_COMPANY_RE = re.compile(r'Ответственная компания:\s*([^<\n]+)')

# Маппинг статусов → канонические названия ресурсов (как в НСК-скрапере)
_UTILITY_MAP: list[tuple[str, str]] = [
    ("без теплоснабжения и гвс",   "Теплоснабжение / ГВС"),
    ("без гвс",                    "Горячее водоснабжение"),
    ("без горячей воды",           "Горячее водоснабжение"),
    ("без теплоснабжения",         "Теплоснабжение"),
    ("без отопления",              "Теплоснабжение"),
    ("без холодной воды",          "Холодное водоснабжение"),
    ("без хвс",                    "Холодное водоснабжение"),
    ("без электроснабжения",       "Электроснабжение"),
    ("без электричества",          "Электроснабжение"),
]


def _make_id(*parts: str) -> str:
    key = "|".join(parts)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_utility(status_text: str) -> str:
    key = status_text.lower().strip()
    for pattern, canonical in _UTILITY_MAP:
        if pattern in key:
            return canonical
    return status_text.strip() or "ЖКХ"


def _parse_group_type(date_from_str: str, date_to_str: str) -> str:
    """'active' если сейчас между from и to, иначе 'planned'."""
    fmt = "%d.%m.%Y %H:%M"
    try:
        dt_from = datetime.strptime(date_from_str.strip(), fmt)
        dt_to   = datetime.strptime(date_to_str.strip(), fmt)
        now = datetime.now()
        return "active" if dt_from <= now <= dt_to else "planned"
    except Exception:
        return "planned"


def _fetch_all_addresses() -> list[str]:
    """POST action=findAddress, mask='' → список всех адресов с отключениями."""
    url = _get_city_feature("power_outages_url", "")
    if not url:
        log.warning("omskrts: power_outages_url не задан в city_profile")
        return []
    try:
        resp = requests.post(
            url,
            data={"mask": "", "action": "findAddress", "len": 500},
            headers=SCRAPER_HEADERS,
            timeout=SCRAPER_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return [str(a).strip() for a in data if a]
    except Exception as e:
        log.error(f"omskrts: ошибка получения списка адресов: {e}")
    return []


def _fetch_address_info(url: str, address: str) -> dict | None:
    """POST action=info, mask=<address> → dict с деталями или None."""
    try:
        resp = requests.post(
            url,
            data={"mask": address, "action": "info"},
            headers=SCRAPER_HEADERS,
            timeout=SCRAPER_TIMEOUT,
        )
        resp.raise_for_status()
        html = resp.text

        company_m = _COMPANY_RE.search(html)
        date_m    = _DATE_RE.search(html)
        status_m  = _STATUS_RE.search(html)

        if not date_m or not status_m:
            return None

        date_from  = date_m.group(1).strip()
        date_to    = date_m.group(2).strip()
        status_raw = status_m.group(1).strip()

        return {
            "address":    address,
            "company":    company_m.group(1).strip() if company_m else "",
            "date_from":  date_from,
            "date_to":    date_to,
            "status_raw": status_raw,
            "utility":    _parse_utility(status_raw),
            "group_type": _parse_group_type(date_from, date_to),
        }
    except Exception as e:
        log.debug(f"omskrts: ошибка info для {address!r}: {e}")
        return None


def scrape_summary() -> list[dict[str, Any]]:
    """Сводка по всем активным отключениям omskrts.ru.

    Алгоритм:
    1. Получаем список всех адресов с отключениями (1 запрос).
    2. Для каждого адреса получаем детали (N запросов, N ≈ 50–150).
    3. Группируем по (utility, group_type, date_from, date_to).
    4. houses = кол-во адресов в группе.

    Возвращает список записей в формате, совместимом с power_scraper.py.
    """
    scraped_at = datetime.now(timezone.utc).isoformat()
    url = _get_city_feature("power_outages_url", "")

    addresses = _fetch_all_addresses()
    if not addresses:
        log.info("omskrts: отключений не обнаружено (список адресов пуст)")
        return []

    log.info(f"omskrts: {len(addresses)} адресов с отключениями, запрашиваем детали…")

    infos: list[dict] = []
    for addr in addresses:
        info = _fetch_address_info(url, addr)
        if info:
            infos.append(info)

    if not infos:
        return []

    # Группируем по ключу
    groups: dict[tuple, dict] = {}
    for info in infos:
        key = (info["utility"], info["group_type"], info["date_from"], info["date_to"])
        if key not in groups:
            groups[key] = {
                "utility":    info["utility"],
                "group_type": info["group_type"],
                "date_from":  info["date_from"],
                "date_to":    info["date_to"],
                "company":    info["company"],
                "houses":     0,
            }
        groups[key]["houses"] += 1

    records: list[dict] = []
    for key, g in groups.items():
        utility, group_type, date_from, date_to = key
        records.append({
            "id":           _make_id(scraped_at, utility, group_type, date_from),
            "utility":      utility,
            "utility_id":   re.sub(r"[^a-z0-9]", "_", utility.lower())[:20],
            "group_type":   group_type,
            "district":     "all",
            "district_href": "",
            "houses":       g["houses"],
            "date_from":    date_from,
            "date_to":      date_to,
            "company":      g["company"],
            "scraped_at":   scraped_at,
            "source_url":   url,
        })

    log.info(
        f"omskrts: {len(records)} групп отключений, "
        f"{sum(r['houses'] for r in records)} домов суммарно"
    )
    return records


def fetch_outages_detail(system_id: str, district_href: str) -> list[dict[str, Any]]:
    """Не реализовано для omskrts.ru — API не поддерживает детальный просмотр по системе/району."""
    return []


def fetch_all_outages() -> list[dict[str, Any]]:
    """Основная точка входа — возвращает сводку."""
    return scrape_summary()
