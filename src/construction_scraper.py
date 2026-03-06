"""Скрапер разрешений на строительство/ввод в эксплуатацию.

Источник: portal.novo-sibirsk.ru/dsa/
  - acception.aspx    — разрешения на строительство
  - commissioning.aspx — разрешения на ввод в эксплуатацию

Страницы содержат HTML-таблицы, доступные без авторизации.
Обновляются ежедневно по мере выдачи разрешений.
TTL кэша: 24 ч.
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from .constants import SCRAPER_HEADERS, SCRAPER_TIMEOUT

log = logging.getLogger(__name__)

_DSA_BASE = "http://portal.novo-sibirsk.ru"
_ACCEPTION_URL = f"{_DSA_BASE}/dsa/acception.aspx"
_COMMISSIONING_URL = f"{_DSA_BASE}/dsa/commissioning.aspx"

# Соответствие типа документа
PERMIT_TYPES = {
    "construction": "Разрешение на строительство",
    "commissioning": "Разрешение на ввод в эксплуатацию",
}


def _make_id(permit_type: str, number: str, address: str) -> str:
    key = f"{permit_type}|{number}|{address}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _scrape_table(url: str, permit_type: str) -> list[dict[str, Any]]:
    """Скрапит HTML-таблицу разрешений с указанного URL."""
    try:
        resp = requests.get(
            url, headers=SCRAPER_HEADERS, timeout=SCRAPER_TIMEOUT, verify=False
        )
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
    except Exception as e:
        log.error(f"Ошибка получения {url}: {e}")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()
    soup = BeautifulSoup(resp.text, "html.parser")
    records: list[dict] = []

    # Ищем первую таблицу с данными
    table = soup.find("table")
    if not table:
        log.warning(f"Таблица не найдена на странице {url}")
        return []

    rows = table.find_all("tr")
    if not rows:
        return []

    # Заголовки из первой строки
    headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]

    for row in rows[1:]:
        cells = [td.get_text(" ", strip=True) for td in row.find_all("td")]
        if not cells or all(not c for c in cells):
            continue

        row_dict: dict[str, str] = {}
        for i, h in enumerate(headers):
            if i < len(cells):
                row_dict[h] = cells[i]

        # Нормируем поля (разные названия колонок на разных страницах)
        number = (
            row_dict.get("Номер разрешения", "")
            or row_dict.get("№ разрешения", "")
            or row_dict.get("Номер", "")
            or cells[0] if cells else ""
        )
        address = (
            row_dict.get("Адрес объекта", "")
            or row_dict.get("Адрес", "")
            or row_dict.get("Местоположение", "")
        )
        developer = (
            row_dict.get("Застройщик", "")
            or row_dict.get("Заявитель", "")
            or row_dict.get("Организация", "")
        )
        object_name = (
            row_dict.get("Наименование объекта", "")
            or row_dict.get("Объект", "")
            or row_dict.get("Наименование", "")
        )
        issue_date = (
            row_dict.get("Дата выдачи", "")
            or row_dict.get("Дата", "")
            or row_dict.get("Дата разрешения", "")
        )
        valid_until = (
            row_dict.get("Срок действия", "")
            or row_dict.get("Действует до", "")
            or row_dict.get("Дата окончания", "")
        )
        # Попытка извлечь район из адреса
        district = _extract_district_from_address(address or object_name)

        if number or address:
            records.append({
                "id": _make_id(permit_type, number, address),
                "permit_type": permit_type,
                "number": number,
                "address": address,
                "object_name": object_name,
                "developer": developer,
                "issue_date": issue_date,
                "valid_until": valid_until,
                "district": district,
                "raw": str(row_dict),
                "scraped_at": scraped_at,
                "source_url": url,
            })

    log.info(f"Получено {len(records)} записей разрешений ({permit_type}) с {url}")
    return records


_DISTRICT_PATTERNS = {
    "Дзержинский": re.compile(r"дзержинск", re.I),
    "Железнодорожный": re.compile(r"железнодорожн", re.I),
    "Заельцовский": re.compile(r"заельцовск", re.I),
    "Калининский": re.compile(r"калининск", re.I),
    "Кировский": re.compile(r"кировск", re.I),
    "Ленинский": re.compile(r"ленинск", re.I),
    "Октябрьский": re.compile(r"октябрьск", re.I),
    "Первомайский": re.compile(r"первомайск", re.I),
    "Советский": re.compile(r"советск", re.I),
    "Центральный": re.compile(r"централь|центр", re.I),
}


def _extract_district_from_address(text: str) -> str:
    """Пытается определить район по адресу объекта."""
    if not text:
        return ""
    for name, pat in _DISTRICT_PATTERNS.items():
        if pat.search(text):
            return f"{name} район"
    return ""


def scrape_construction_permits() -> list[dict[str, Any]]:
    """Получает разрешения на строительство."""
    return _scrape_table(_ACCEPTION_URL, "construction")


def scrape_commissioning_permits() -> list[dict[str, Any]]:
    """Получает разрешения на ввод в эксплуатацию."""
    return _scrape_table(_COMMISSIONING_URL, "commissioning")


def fetch_all_permits() -> list[dict[str, Any]]:
    """Получает оба типа разрешений."""
    construction = scrape_construction_permits()
    commissioning = scrape_commissioning_permits()
    return construction + commissioning
