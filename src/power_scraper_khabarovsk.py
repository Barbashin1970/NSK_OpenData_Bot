"""Скрапер отключений ЖКХ для Хабаровска — dvhab.ru.

Источник: https://www.dvhab.ru/city/отключения/
HTML-страница с семантической разметкой:
  li.outages__service       — каждое отключение
  .outages__service-title   — дата + тип ресурса
  .outages__service-cause   — причина
  .outages__service-responsible — ответственная компания
  .outages__service-addresses-streets div — улицы + дома

Схема возвращаемых записей совместима с power_scraper.py:
  id, utility, utility_id, group_type, district, district_href,
  houses, date_from, date_to, company, scraped_at, source_url
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

# Маппинг фраз из заголовков → канонические названия ресурсов
_UTILITY_MAP: list[tuple[str, str]] = [
    ("холодной воды",       "Холодное водоснабжение"),
    ("холодного водоснабж", "Холодное водоснабжение"),
    ("горячей воды",        "Горячее водоснабжение"),
    ("горячего водоснабж",  "Горячее водоснабжение"),
    ("отопления",           "Теплоснабжение"),
    ("теплоснабж",          "Теплоснабжение"),
    ("электричества",       "Электроснабжение"),
    ("электроснабж",        "Электроснабжение"),
    ("электроэнерг",        "Электроснабжение"),
    ("света",               "Электроснабжение"),
    ("газоснабж",            "Газоснабжение"),
    ("газа",                 "Газоснабжение"),
    ("водоотвед",            "Водоотведение"),
    ("канализац",            "Водоотведение"),
]

# Regex для даты в заголовке: "С 19.03 09:40" или "С 19.03.2026 09:40"
_TITLE_DATE_RE = re.compile(
    r"[Сс]\s+(\d{1,2}\.\d{2}(?:\.\d{2,4})?)\s+(\d{2}:\d{2})"
)

# Regex для периода: "до 19.03 17:00" или "по 19.03 17:00"
_TITLE_END_RE = re.compile(
    r"(?:до|по)\s+(\d{1,2}\.\d{2}(?:\.\d{2,4})?)\s+(\d{2}:\d{2})"
)


def _make_id(*parts: str) -> str:
    key = "|".join(parts)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_utility(title_text: str) -> str:
    """Определяет тип ресурса по тексту заголовка."""
    low = title_text.lower()
    for pattern, canonical in _UTILITY_MAP:
        if pattern in low:
            return canonical
    return "ЖКХ"


def _parse_date_from_title(title_text: str) -> tuple[str, str]:
    """Извлекает дату начала и окончания из заголовка.

    Возвращает (date_from, date_to) в формате "DD.MM.YYYY HH:MM".
    Если год не указан — подставляет текущий.
    """
    year = datetime.now().year

    date_from = ""
    m = _TITLE_DATE_RE.search(title_text)
    if m:
        date_part = m.group(1)
        time_part = m.group(2)
        # Дополняем год если нет
        if len(date_part) <= 5:  # "19.03"
            date_part = f"{date_part}.{year}"
        date_from = f"{date_part} {time_part}"

    date_to = ""
    m2 = _TITLE_END_RE.search(title_text)
    if m2:
        date_part2 = m2.group(1)
        time_part2 = m2.group(2)
        if len(date_part2) <= 5:
            date_part2 = f"{date_part2}.{year}"
        date_to = f"{date_part2} {time_part2}"

    return date_from, date_to


def _count_houses(addr_divs: list) -> int:
    """Считает количество домов из div-ов с адресами."""
    count = 0
    for div in addr_divs:
        # Каждая ссылка <a> = один дом
        links = div.find_all("a")
        count += max(len(links), 1)
    return count


def scrape_summary() -> list[dict[str, Any]]:
    """Скрапит текущие отключения с dvhab.ru.

    Возвращает список записей в формате, совместимом с power_scraper.py.
    """
    url = _get_city_feature("power_outages_url", "")
    if not url:
        log.warning("dvhab: power_outages_url не задан в city_profile")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()

    try:
        resp = requests.get(
            url,
            headers=SCRAPER_HEADERS,
            timeout=SCRAPER_TIMEOUT,
        )
        resp.raise_for_status()
    except Exception as e:
        log.error("dvhab: ошибка загрузки %s — %s", url, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    items = soup.select("li.outages__service")

    if not items:
        log.info("dvhab: отключений не обнаружено (список пуст)")
        return []

    records: list[dict[str, Any]] = []
    for item in items:
        # Заголовок: "С 19.03 09:40 отключение холодной воды"
        title_el = item.select_one(".outages__service-title")
        title_text = title_el.get_text(strip=True) if title_el else ""

        utility = _parse_utility(title_text)
        date_from, date_to = _parse_date_from_title(title_text)

        # Причина
        cause_el = item.select_one(".outages__service-cause")
        cause = cause_el.get_text(strip=True) if cause_el else ""

        # Ответственная компания
        resp_el = item.select_one(".outages__service-responsible")
        company = ""
        if resp_el:
            company = resp_el.get_text(strip=True)
            # Убираем префикс "Ответственный:"
            company = re.sub(r"^Ответственн\w*:\s*", "", company).strip()

        # Адреса — считаем дома
        addr_divs = item.select(".outages__service-addresses-streets div")
        houses = _count_houses(addr_divs)

        # group_type: active если дата начала в прошлом
        group_type = "active"
        if date_from:
            try:
                fmt = "%d.%m.%Y %H:%M"
                dt_from = datetime.strptime(date_from, fmt)
                now = datetime.now()
                if dt_from > now:
                    group_type = "planned"
            except ValueError:
                pass

        records.append({
            "id":            _make_id(scraped_at, utility, date_from, title_text[:30]),
            "utility":       utility,
            "utility_id":    re.sub(r"[^a-z0-9]", "_", utility.lower())[:20],
            "group_type":    group_type,
            "district":      "all",
            "district_href": "",
            "houses":        houses,
            "date_from":     date_from,
            "date_to":       date_to,
            "company":       company,
            "cause":         cause,
            "scraped_at":    scraped_at,
            "source_url":    url,
        })

    log.info(
        "dvhab: %d отключений, %d домов суммарно",
        len(records), sum(r["houses"] for r in records),
    )
    return records


def fetch_outages_detail(system_id: str, district_href: str) -> list[dict[str, Any]]:
    """Не реализовано для dvhab.ru — страница не поддерживает детальный просмотр."""
    return []


def fetch_all_outages() -> list[dict[str, Any]]:
    """Основная точка входа — возвращает сводку."""
    return scrape_summary()
