"""Скрапер отключений электроснабжения для Санкт-Петербурга — rosseti-lenenergo.ru.

Источник: https://rosseti-lenenergo.ru/planned_work/?reg=343
HTML-таблица class="tableous_facts funds":
  <tr data-record-id="...">
    td[0] — Регион
    td[1] — Административный район / Населённый пункт
    td[2] — Адрес (class="rowStreets", <span> на каждый дом)
    td[3] — Дата начала отключения (DD-MM-YYYY)
    td[4] — Время начала (HH:MM)
    td[5] — Дата восстановления (DD-MM-YYYY)
    td[6] — Время восстановления (HH:MM)
    td[7] — Филиал
    td[8] — РЭС
    td[9] — Комментарий

Схема возвращаемых записей совместима с power_scraper.py:
  id, utility, utility_id, group_type, district, district_href,
  houses, date_from, date_to, company, scraped_at, source_url
"""

import hashlib
import logging
import re
from datetime import datetime, timezone
from typing import Any

import urllib3
import requests
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from .city_config import get_feature as _get_city_feature
from .constants import SCRAPER_HEADERS, SCRAPER_TIMEOUT

log = logging.getLogger(__name__)

# reg=343 — Санкт-Петербург в справочнике Россети Ленэнерго
_SPB_REG_ID = "343"

# Населённые пункты СПб → административный район
_LOCALITY_TO_DISTRICT: dict[str, str] = {
    "зеленогорск":  "Курортный район",
    "сестрорецк":   "Курортный район",
    "комарово":     "Курортный район",
    "репино":       "Курортный район",
    "солнечное":    "Курортный район",
    "ушково":       "Курортный район",
    "песочный":     "Курортный район",
    "белоостров":   "Курортный район",
    "колпино":      "Колпинский район",
    "металлострой": "Колпинский район",
    "понтонный":    "Колпинский район",
    "петергоф":     "Петродворцовый район",
    "стрельна":     "Петродворцовый район",
    "ломоносов":    "Петродворцовый район",
    "кронштадт":    "Кронштадтский район",
    "пушкин":       "Пушкинский район",
    "павловск":     "Пушкинский район",
    "шушары":       "Пушкинский район",
    "левашово":     "Выборгский район",
    "парголово":    "Выборгский район",
}

# Карта РЭС → административный район (фолбэк)
_RES_TO_DISTRICT: dict[str, str] = {
    "невский рэс":        "Невский район",
    "восточный рэс":      "Красногвардейский район",
    "центральный рэс":    "Центральный район",
    "курортный рэс":      "Курортный район",
    "южный рэс":          "Московский район",
    "пригородный рэс":    "Пушкинский район",
}

# Исправление опечаток в данных Россети
_DISTRICT_TYPOS: dict[str, str] = {
    "Краносельский район": "Красносельский район",
}


def _make_id(*parts: str) -> str:
    key = "|".join(parts)
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _extract_district(district_cell_text: str) -> str:
    """Извлекает название района из ячейки 'Административный район'.

    Пример: 'Санкт-Петербург  Калининский район' → 'Калининский район'
    'Санкт-Петербург  п.Репино' → 'Курортный район' (через _LOCALITY_TO_DISTRICT)
    """
    text = " ".join(district_cell_text.split())
    # Ищем "XXXский район"
    m = re.search(r"([А-ЯЁа-яё]+(?:ий|ой|ый)\s+район)", text)
    if m:
        district = m.group(1)
        return _DISTRICT_TYPOS.get(district, district)
    # Населённый пункт: "п.Репино", "г.Колпино", "г Зеленогорск"
    m2 = re.search(r"(?:п\.|пос\.|г\.?)\s*([А-ЯЁа-яё]+)", text)
    if m2:
        locality = m2.group(1).lower()
        if locality in _LOCALITY_TO_DISTRICT:
            return _LOCALITY_TO_DISTRICT[locality]
    # Попробуем найти любое известное название населённого пункта
    text_lower = text.lower()
    for loc, district in _LOCALITY_TO_DISTRICT.items():
        if loc in text_lower:
            return district
    return text.replace("Санкт-Петербург", "").strip() or "all"


def _count_houses(streets_cell) -> int:
    """Считает количество адресов в ячейке rowStreets."""
    spans = streets_cell.find_all("span")
    return max(len(spans), 1) if spans else 1


def _build_url(days_ahead: int = 7) -> str:
    """Строит URL для запроса плановых отключений на ближайшие N дней."""
    base = _get_city_feature("power_outages_url", "")
    if not base:
        return ""
    now = datetime.now()
    date_start = now.strftime("%d.%m.%Y")
    future = datetime(now.year, now.month, now.day)
    from datetime import timedelta
    future = future + timedelta(days=days_ahead)
    date_finish = future.strftime("%d.%m.%Y")
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}reg={_SPB_REG_ID}&date_start={date_start}&date_finish={date_finish}"


def scrape_summary() -> list[dict[str, Any]]:
    """Скрапит плановые отключения электроснабжения с rosseti-lenenergo.ru.

    Возвращает список записей в формате, совместимом с power_scraper.py.
    """
    url = _build_url(days_ahead=7)
    if not url:
        log.warning("spb: power_outages_url не задан в city_profile")
        return []

    scraped_at = datetime.now(timezone.utc).isoformat()

    try:
        resp = requests.get(
            url,
            headers=SCRAPER_HEADERS,
            timeout=SCRAPER_TIMEOUT,
            verify=False,  # rosseti-lenenergo.ru иногда имеет проблемы с сертификатом
        )
        resp.raise_for_status()
    except Exception as e:
        log.error("spb rosseti: ошибка загрузки %s — %s", url, e)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    table = soup.find("table", class_="tableous_facts")
    if not table:
        log.info("spb rosseti: таблица отключений не найдена")
        return []

    tbody = table.find("tbody") or table
    rows = tbody.find_all("tr", attrs={"data-record-id": True})

    if not rows:
        log.info("spb rosseti: отключений не обнаружено (таблица пуста)")
        return []

    records: list[dict[str, Any]] = []
    now = datetime.now()

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 7:
            continue

        # Район
        district_text = cells[1].get_text() if len(cells) > 1 else ""
        district = _extract_district(district_text)

        # Адреса
        streets_cell = cells[2] if len(cells) > 2 else None
        houses = _count_houses(streets_cell) if streets_cell else 0

        # Даты: DD-MM-YYYY HH:MM
        date_start_raw = cells[3].get_text(strip=True) if len(cells) > 3 else ""
        time_start_raw = cells[4].get_text(strip=True) if len(cells) > 4 else ""
        date_end_raw = cells[5].get_text(strip=True) if len(cells) > 5 else ""
        time_end_raw = cells[6].get_text(strip=True) if len(cells) > 6 else ""

        # Нормализация дат: DD-MM-YYYY → DD.MM.YYYY
        date_from = ""
        if date_start_raw:
            d = date_start_raw.replace("-", ".")
            date_from = f"{d} {time_start_raw}" if time_start_raw else d

        date_to = ""
        if date_end_raw:
            d = date_end_raw.replace("-", ".")
            date_to = f"{d} {time_end_raw}" if time_end_raw else d

        # Филиал и РЭС
        branch = cells[7].get_text(strip=True) if len(cells) > 7 else ""
        res = cells[8].get_text(strip=True) if len(cells) > 8 else ""
        company = f"{branch}, {res}" if branch and res else branch or res

        # Комментарий (причина)
        comment = cells[9].get_text(strip=True) if len(cells) > 9 else ""

        # Если район не определён из ячейки — попробуем по РЭС
        if district == "all" and res:
            res_lower = res.lower().strip()
            district = _RES_TO_DISTRICT.get(res_lower, "all")

        # group_type: active если дата начала в прошлом
        group_type = "planned"
        if date_from:
            try:
                dt_from = datetime.strptime(date_from, "%d.%m.%Y %H:%M")
                if dt_from <= now:
                    group_type = "active"
            except ValueError:
                pass

        records.append({
            "id":            _make_id(scraped_at, district, date_from, str(houses)),
            "utility":       "Электроснабжение",
            "utility_id":    "electricity",
            "group_type":    group_type,
            "district":      district,
            "district_href": "",
            "houses":        houses,
            "date_from":     date_from,
            "date_to":       date_to,
            "company":       company,
            "cause":         comment,
            "scraped_at":    scraped_at,
            "source_url":    url,
        })

    log.info(
        "spb rosseti: %d отключений, %d адресов суммарно",
        len(records), sum(r["houses"] for r in records),
    )
    return records


def fetch_outages_detail(system_id: str, district_href: str) -> list[dict[str, Any]]:
    """Не реализовано — rosseti-lenenergo.ru не имеет детальных страниц."""
    return []


def fetch_all_outages() -> list[dict[str, Any]]:
    """Основная точка входа — возвращает сводку."""
    return scrape_summary()
