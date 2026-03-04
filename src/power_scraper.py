"""Скрапер данных об отключениях систем жизнеобеспечения Новосибирска.

Источники:
1. http://051.novo-sibirsk.ru/sitepages/off.aspx — официальный портал мэрии.
   Даёт сводку (плановые + текущие) по видам систем и районам.
   Обновляется в реальном времени. Работает без JS.

Схема данных:
- utility:    Электроснабжение / Теплоснабжение / Горячее водоснабжение / ...
- group_type: planned (запланировано сегодня) | active (отключено сейчас)
- district:   название района ("Советский район") или "all" если нет данных
- houses:     количество отключённых домов (int)
- scraped_at: ISO timestamp момента получения данных
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

import requests
from bs4 import BeautifulSoup

from .constants import NSK_051_URL, SCRAPER_HEADERS, SCRAPER_TIMEOUT

log = logging.getLogger(__name__)

# Соответствие номеров блоков → групп
# cult_off_block_1-X = плановые (Запланировано к отключению)
# cult_off_block_2-X = текущие  (Отключено домов)
_GROUP_LABELS = {
    "1": "planned",   # Запланировано к отключению
    "2": "active",    # Отключено сейчас
}


def _make_id(scraped_at: str, group_type: str, utility: str, district: str) -> str:
    key = f"{scraped_at}|{group_type}|{utility}|{district}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_district_id(href: str) -> str:
    """Извлекает District ID из href вида /SitePages/offfull.aspx?System=9&District=..."""
    m = re.search(r"District=([^&#]+)", href)
    return m.group(1) if m else ""


def scrape_summary() -> list[dict[str, Any]]:
    """Парсит сводную страницу 051.novo-sibirsk.ru.

    Возвращает список записей:
    {
        id, utility, utility_id, group_type, district, district_href,
        houses, scraped_at, source_url
    }
    """
    try:
        resp = requests.get(NSK_051_URL, headers=SCRAPER_HEADERS, timeout=SCRAPER_TIMEOUT)
        resp.raise_for_status()
    except Exception as e:
        log.error(f"Ошибка получения 051.novo-sibirsk.ru: {e}")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()
    soup = BeautifulSoup(resp.text, "html.parser")
    records: list[dict] = []

    # Находим все блоки cult_off_block_N-M
    # N = группа (1=плановые, 2=текущие), M = system_id
    block_pattern = re.compile(r"cult_off_block_(\d+)-(\d+)")

    for div in soup.find_all("div", id=block_pattern):
        div_id = div.get("id", "")
        m = block_pattern.match(div_id)
        if not m:
            continue

        group_num = m.group(1)   # "1" или "2"
        system_id = m.group(2)   # "9" для электричества
        group_type = _GROUP_LABELS.get(group_num, f"group{group_num}")

        # Название утилиты
        p_tag = div.find("p")
        utility = p_tag.get_text(strip=True) if p_tag else f"system_{system_id}"

        # Проверяем "Нет отключений"
        no_off_div = div.find("div", class_="cult_off_block_no")
        if no_off_div:
            records.append({
                "id": _make_id(scraped_at, group_type, utility, "all"),
                "utility": utility,
                "utility_id": system_id,
                "group_type": group_type,
                "district": "all",
                "district_href": "",
                "houses": 0,
                "scraped_at": scraped_at,
                "source_url": NSK_051_URL,
            })
            continue

        # Парсим строки с районами и счётчиками
        for row in div.find_all("tr"):
            name_td = row.find("td", class_="cult_off_block_district_name")
            val_td = row.find("td", class_="cult_off_block_district_value")
            if not name_td or not val_td:
                continue

            # Пропускаем строку "итого"
            if "cult_off_total" in (name_td.get("class") or []):
                continue

            link = name_td.find("a")
            district_name = name_td.get_text(strip=True).rstrip(" —").strip()
            district_href = link["href"] if link else ""

            try:
                houses = int(val_td.get_text(strip=True))
            except ValueError:
                houses = 0

            if district_name:
                records.append({
                    "id": _make_id(scraped_at, group_type, utility, district_name),
                    "utility": utility,
                    "utility_id": system_id,
                    "group_type": group_type,
                    "district": district_name,
                    "district_href": district_href,
                    "houses": houses,
                    "scraped_at": scraped_at,
                    "source_url": NSK_051_URL,
                })

    log.info(f"Получено {len(records)} записей с 051.novo-sibirsk.ru")
    return records


def fetch_all_outages() -> list[dict[str, Any]]:
    """Основная функция получения данных об отключениях."""
    return scrape_summary()
