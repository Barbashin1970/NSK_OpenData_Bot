"""Универсальный скрапер отключений ЖКХ через gogov.ru.

gogov.ru/hotwater/{city_slug} — агрегатор данных об отключениях ГВС
по всем крупным городам России. HTML-сайт без публичного API.

Особенности:
- Защита CAPTCHA (reCAPTCHA) при частых запросах
- Данные только по горячему водоснабжению (ГВС)
- Структура: адреса, даты, причины отключений

Использует city_profile.yaml → features.power_outages_gogov_slug
для определения города.

Схема возвращаемых записей совместима с power_scraper.py:
  id, utility, utility_id, group_type, district, district_href,
  houses, date_from, date_to, scraped_at, source_url
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

_GOGOV_BASE = "https://gogov.ru/hotwater"

# Маппинг текстовых меток → канонические названия ресурсов
_UTILITY_MAP: list[tuple[str, str]] = [
    ("горячей воды",         "Горячее водоснабжение"),
    ("горячего водоснабж",   "Горячее водоснабжение"),
    ("гвс",                 "Горячее водоснабжение"),
    ("холодной воды",        "Холодное водоснабжение"),
    ("холодного водоснабж",  "Холодное водоснабжение"),
    ("хвс",                 "Холодное водоснабжение"),
    ("отопления",           "Теплоснабжение"),
    ("теплоснабж",          "Теплоснабжение"),
    ("электричества",       "Электроснабжение"),
    ("электроснабж",        "Электроснабжение"),
    ("водоснабж",           "Горячее водоснабжение"),  # gogov = ГВС по умолчанию
]

# Regex для дат вида "28.03.2026" или "28.03.2026 08:00"
_DATE_RE = re.compile(r"(\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?)")
_PERIOD_RE = re.compile(
    r"[сc]\s+(\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?)"
    r"\s+по\s+(\d{2}\.\d{2}\.\d{4}(?:\s+\d{2}:\d{2})?)",
    re.IGNORECASE,
)


def _make_id(*parts: str) -> str:
    key = "|".join(str(p) for p in parts)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _detect_utility(text: str) -> str:
    lower = text.lower()
    for pattern, canonical in _UTILITY_MAP:
        if pattern in lower:
            return canonical
    return "Горячее водоснабжение"  # gogov.ru = ГВС по умолчанию


def _parse_group_type(date_from_str: str, date_to_str: str) -> str:
    """'active' если сейчас между from и to, иначе 'planned'."""
    for fmt in ("%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            dt_from = datetime.strptime(date_from_str.strip(), fmt)
            dt_to = datetime.strptime(date_to_str.strip(), fmt)
            now = datetime.now()
            return "active" if dt_from <= now <= dt_to else "planned"
        except ValueError:
            continue
    return "planned"


def _scrape_gogov(city_slug: str) -> list[dict[str, Any]]:
    """Парсит gogov.ru/hotwater/{city_slug}.

    Возвращает список записей об отключениях.
    При CAPTCHA/ошибке возвращает пустой список.
    """
    url = f"{_GOGOV_BASE}/{city_slug}"
    try:
        resp = requests.get(
            url,
            headers=SCRAPER_HEADERS,
            timeout=SCRAPER_TIMEOUT,
        )
        resp.raise_for_status()
    except Exception as e:
        log.error(f"gogov.ru/{city_slug}: ошибка HTTP: {e}")
        return []

    # Проверяем CAPTCHA
    if "recaptcha" in resp.text.lower() or "Секундочку" in resp.text:
        log.warning(
            f"gogov.ru/{city_slug}: CAPTCHA — данные временно недоступны. "
            "Слишком частые запросы к gogov.ru."
        )
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()
    soup = BeautifulSoup(resp.text, "html.parser")
    records: list[dict] = []

    # Стратегия 1: ищем карточки отключений (div/article с адресами и датами)
    # gogov.ru использует различные CSS-классы в зависимости от версии
    for card in soup.find_all(["div", "article", "section", "li"],
                              class_=re.compile(r"event|card|item|outage|shutdown|block")):
        text = card.get_text(" ", strip=True)
        if len(text) < 20:
            continue

        # Ищем период
        period_m = _PERIOD_RE.search(text)
        date_from = period_m.group(1) if period_m else ""
        date_to = period_m.group(2) if period_m else ""

        if not date_from:
            dates = _DATE_RE.findall(text)
            if dates:
                date_from = dates[0]
                date_to = dates[1] if len(dates) > 1 else dates[0]

        if not date_from:
            continue

        utility = _detect_utility(text)
        group_type = _parse_group_type(date_from, date_to)

        # Адрес — берём первые строки текста до даты
        addr_text = text.split(date_from)[0].strip()
        addr_text = re.sub(r"\s+", " ", addr_text).strip(" —:,.")

        records.append({
            "id": _make_id(scraped_at, city_slug, date_from, addr_text[:50]),
            "utility": utility,
            "utility_id": "hotwater",
            "group_type": group_type,
            "district": "all",
            "district_href": "",
            "houses": 1,
            "date_from": date_from,
            "date_to": date_to,
            "scraped_at": scraped_at,
            "source_url": url,
        })

    # Стратегия 2: ищем таблицу с данными
    if not records:
        for table in soup.find_all("table"):
            rows = table.find_all("tr")
            if len(rows) < 2:
                continue

            headers = [th.get_text(strip=True).lower()
                       for th in rows[0].find_all(["th", "td"])]

            for row in rows[1:]:
                cells = [td.get_text(strip=True) for td in row.find_all("td")]
                if not cells or len(cells) < 2:
                    continue

                row_text = " ".join(cells)
                dates = _DATE_RE.findall(row_text)
                if not dates:
                    continue

                date_from = dates[0]
                date_to = dates[1] if len(dates) > 1 else dates[0]

                # Ищем адрес в ячейках
                address = ""
                for i, h in enumerate(headers):
                    if i < len(cells) and any(k in h for k in
                                              ["адрес", "улица", "дом", "район"]):
                        address = cells[i]
                        break
                if not address and cells:
                    address = cells[0]

                utility = _detect_utility(row_text)
                group_type = _parse_group_type(date_from, date_to)

                records.append({
                    "id": _make_id(scraped_at, city_slug, date_from, address[:50]),
                    "utility": utility,
                    "utility_id": "hotwater",
                    "group_type": group_type,
                    "district": "all",
                    "district_href": "",
                    "houses": 1,
                    "date_from": date_from,
                    "date_to": date_to,
                    "scraped_at": scraped_at,
                    "source_url": url,
                })

    # Стратегия 3: ищем любые блоки с датами (свободный парсинг)
    if not records:
        for el in soup.find_all(["p", "div", "span"]):
            text = el.get_text(" ", strip=True)
            if len(text) < 30 or len(text) > 500:
                continue

            period_m = _PERIOD_RE.search(text)
            if not period_m:
                continue

            date_from = period_m.group(1)
            date_to = period_m.group(2)
            utility = _detect_utility(text)
            group_type = _parse_group_type(date_from, date_to)

            records.append({
                "id": _make_id(scraped_at, city_slug, date_from, text[:50]),
                "utility": utility,
                "utility_id": "hotwater",
                "group_type": group_type,
                "district": "all",
                "district_href": "",
                "houses": 1,
                "date_from": date_from,
                "date_to": date_to,
                "scraped_at": scraped_at,
                "source_url": url,
            })

    if records:
        log.info(f"gogov.ru/{city_slug}: получено {len(records)} записей")
    else:
        log.info(f"gogov.ru/{city_slug}: данных об отключениях не найдено")

    return records


def _try_direct_source() -> list[dict[str, Any]]:
    """Пробует прямой источник из power_outages_url (если задан и отличен от gogov)."""
    url = _get_city_feature("power_outages_url", "")
    if not url or "gogov.ru" in url:
        return []

    try:
        resp = requests.get(url, headers=SCRAPER_HEADERS, timeout=SCRAPER_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        log.debug(f"Прямой источник {url}: {e}")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()
    soup = BeautifulSoup(resp.text, "html.parser")
    records: list[dict] = []

    # Универсальный парсинг HTML-таблиц
    for table in soup.find_all("table"):
        rows = table.find_all("tr")
        if len(rows) < 2:
            continue

        headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
        headers_lower = [h.lower() for h in headers]

        for row in rows[1:]:
            cells = [td.get_text(strip=True) for td in row.find_all("td")]
            if not cells or len(cells) < 2:
                continue

            row_dict: dict[str, str] = {}
            for i, h in enumerate(headers):
                if i < len(cells):
                    row_dict[h] = cells[i]

            # Ищем адрес
            address = ""
            for i, h in enumerate(headers_lower):
                if i < len(cells) and any(k in h for k in
                                          ["адрес", "улица", "населённый", "нас. пункт"]):
                    address = cells[i]
                    break
            if not address and cells:
                address = cells[0]

            # Ищем даты
            date_from = ""
            date_to = ""
            for i, h in enumerate(headers_lower):
                if i < len(cells):
                    if any(k in h for k in ["начал", "с ", "дата нач", "от"]):
                        date_from = cells[i]
                    elif any(k in h for k in ["оконч", "по ", "дата ок", "до"]):
                        date_to = cells[i]

            if not date_from:
                row_text = " ".join(cells)
                dates = _DATE_RE.findall(row_text)
                if dates:
                    date_from = dates[0]
                    date_to = dates[1] if len(dates) > 1 else ""

            if not address or not date_from:
                continue

            row_text = " ".join(cells)
            utility = _detect_utility(row_text)
            group_type = _parse_group_type(date_from, date_to) if date_to else "planned"

            records.append({
                "id": _make_id(scraped_at, address[:50], date_from),
                "utility": utility,
                "utility_id": re.sub(r"[^a-z0-9]", "_", utility.lower())[:20],
                "group_type": group_type,
                "district": "all",
                "district_href": "",
                "houses": 1,
                "date_from": date_from,
                "date_to": date_to,
                "scraped_at": scraped_at,
                "source_url": url,
            })

    if records:
        log.info(f"Прямой источник {url}: получено {len(records)} записей")
    return records


def scrape_summary() -> list[dict[str, Any]]:
    """Сводка: сначала прямой источник, затем gogov.ru как fallback."""
    # 1. Пробуем прямой источник (power_outages_url)
    records = _try_direct_source()
    if records:
        return records

    # 2. Fallback: gogov.ru
    slug = _get_city_feature("power_outages_gogov_slug", "")
    if slug:
        return _scrape_gogov(slug)

    log.warning("gogov scraper: нет ни прямого источника, ни gogov_slug")
    return []


def fetch_outages_detail(system_id: str, district_href: str) -> list[dict[str, Any]]:
    """Детальный просмотр не поддерживается для gogov.ru."""
    return []


def fetch_all_outages() -> list[dict[str, Any]]:
    """Основная точка входа — возвращает сводку."""
    return scrape_summary()
