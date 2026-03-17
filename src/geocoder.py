"""Геокодирование адресов через 2GIS Catalog API с кешированием в DuckDB.

Лицензия 2ГИС (law.2gis.ru/api-rules):
- Кешировать результаты геокодирования разрешено.
- Данные хранятся в таблице geocode_cache в data/cache.db.

Требует TWOGIS_API_KEY (ENV или data/api_keys.json).
Если ключ не задан — возвращает None без ошибки (graceful degradation).
"""

import hashlib
import logging
import os
import re
from pathlib import Path

import duckdb
import requests

from .city_config import get_city_name

log = logging.getLogger(__name__)

_GEOCODE_URL = "https://catalog.api.2gis.com/3.0/items/geocode"
_DEFAULT_CITY = get_city_name()  # из city_profile.yaml
_REQUEST_TIMEOUT = 8


def _get_key() -> str | None:
    return os.environ.get("TWOGIS_API_KEY", "").strip() or None


def _conn():
    from .city_config import get_db_path
    return duckdb.connect(str(get_db_path()))


def _ensure_table() -> None:
    conn = _conn()
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS geocode_cache (
                address_key  TEXT PRIMARY KEY,
                address_raw  TEXT,
                lat          DOUBLE,
                lon          DOUBLE,
                full_name    TEXT,
                created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    finally:
        conn.close()


def _address_key(address: str) -> str:
    return hashlib.md5(address.lower().strip().encode()).hexdigest()


def _get_cached(address_key: str) -> dict | None:
    conn = _conn()
    try:
        rows = conn.execute(
            "SELECT lat, lon, full_name FROM geocode_cache WHERE address_key = ?",
            [address_key],
        ).fetchall()
        if rows:
            return {"lat": rows[0][0], "lon": rows[0][1], "full_name": rows[0][2]}
    finally:
        conn.close()
    return None


def _save_cache(address_key: str, address_raw: str, lat: float, lon: float, full_name: str) -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO geocode_cache (address_key, address_raw, lat, lon, full_name)
            VALUES (?, ?, ?, ?, ?)
            """,
            [address_key, address_raw, lat, lon, full_name],
        )
    finally:
        conn.close()


def geocode(address: str, city: str = _DEFAULT_CITY) -> dict | None:
    """Геокодирует адрес. Возвращает {lat, lon, full_name, source} или None.

    source = "cache" — из кеша (не требует ключа)
    source = "api"   — из 2GIS Geocoder API
    None             — ключ не задан, адрес не найден или ошибка сети
    """
    query = f"{city}, {address}" if city and city.lower() not in address.lower() else address
    key = _address_key(query)

    _ensure_table()
    cached = _get_cached(key)
    if cached:
        return {**cached, "source": "cache"}

    api_key = _get_key()
    if not api_key:
        return None

    try:
        resp = requests.get(
            _GEOCODE_URL,
            params={"q": query, "fields": "items.point,items.full_name", "key": api_key},
            timeout=_REQUEST_TIMEOUT,
        )
        if resp.status_code == 403:
            log.warning("2GIS Geocoder: ключ недействителен (403)")
            return None
        if resp.status_code != 200:
            log.warning("2GIS Geocoder: HTTP %s для %r", resp.status_code, query)
            return None

        items = resp.json().get("result", {}).get("items", [])
        if not items:
            return None

        point = items[0].get("point", {})
        lat = point.get("lat")
        lon = point.get("lon")
        full_name = items[0].get("full_name", query)

        if lat is None or lon is None:
            return None

        _save_cache(key, query, lat, lon, full_name)
        return {"lat": lat, "lon": lon, "full_name": full_name, "source": "api"}

    except requests.exceptions.Timeout:
        log.warning("2GIS Geocoder: таймаут для %r", query)
        return None
    except Exception as e:
        log.error("2GIS Geocoder error for %r: %s", query, e)
        return None


def geocode_stats() -> dict:
    """Статистика кеша геокодирования."""
    try:
        _ensure_table()
        conn = _conn()
        try:
            count = conn.execute("SELECT COUNT(*) FROM geocode_cache").fetchone()[0]
            from .city_config import get_db_path
            return {"cached_addresses": count, "db_path": str(get_db_path())}
        finally:
            conn.close()
    except Exception:
        return {"cached_addresses": 0}


# ── Вспомогательные функции для работы с датасетами ─────────────────────────

_STREET_FIELDS = ("AdrStreet", "AdrStr", "Ulica", "Street")
_HOUSE_FIELDS = ("AdrDom", "Dom", "House")
_DISTRICT_FIELDS = ("AdrDistr", "Rayon", "District")

# Признаки улицы/места в строке AdrOr (строительный датасет — "Район, ул. Название")
_STREET_IN_ADDR = re.compile(
    r"\bул\.|\bулиц[аы]\b"                         # ул. / улица / улицы
    r"|\bпр[\.-]|\bпросп\b|\bпроспект\b"           # пр. / просп / проспект
    r"|\bпер\.|\bпереулок\b"                        # пер. / переулок
    r"|\bпроезд\b|\bшоссе\b|\bнаб\.|\bнабережн\w*" # проезд / шоссе / наб.
    r"|\bб-р\b|\bбульвар\b|\bтракт\b|\bлиния\b"    # бульвар / тракт / линия
    r"|\bспуск\b|\bтупик\b|\bплощадь\b|\bпл\."     # спуск / тупик / площадь
    r"|\bпос\.|\bпосел\w+\b"                        # пос. / поселок
    r"|\bмкр\.?\b|\bмикрорайон\b",                 # мкр / микрорайон
    re.IGNORECASE,
)

# Срезает "Район, " из строки AdrOr (для fallback-геокодирования хвоста)
_DISTRICT_STRIP = re.compile(
    r"^(?:Дзержинский|Железнодорожный|Заельцовский|Калининский|Кировский|"
    r"Ленинский|Октябрьский|Первомайский|Советский|Центральный)\s+район\s*,\s*",
    re.IGNORECASE,
)


def extract_address(row: dict) -> str | None:
    """Извлекает адресную строку из строки датасета.

    Ищет поля улицы и номера дома по известным именам колонок.
    Fallback: поле AdrOr (датасет стройки) — если содержит улицу.
    Возвращает строку вида "ул. Красный проспект, 25" или None.
    """
    street = next((row[f] for f in _STREET_FIELDS if row.get(f)), None)
    house = next((row[f] for f in _HOUSE_FIELDS if row.get(f)), None)
    if not street:
        addr_or = (row.get("AdrOr") or "").strip()
        if not addr_or:
            return None
        if _STREET_IN_ADDR.search(addr_or):
            return addr_or  # geocoder добавит "Новосибирск, " prefix
        # Fallback: срезаем "Район, " и пробуем геокодировать хвост
        # (поселки, жилые массивы, улицы без стандартного сокращения)
        tail = _DISTRICT_STRIP.sub("", addr_or).strip()
        if tail and tail != addr_or and not re.search(r"\bрайон\w*", tail, re.IGNORECASE):
            return tail
        return None
    return f"{street}, {house}" if house else street


def geocode_rows(rows: list[dict], max_rows: int = 50) -> list[dict]:
    """Обогащает строки датасета координатами.

    Добавляет поля _lat, _lon к каждой строке (или None если не удалось).
    Обрабатывает не более max_rows строк.
    Сначала использует кеш (быстро), потом API (медленно).
    """
    _ensure_table()
    enriched = []
    for row in rows[:max_rows]:
        addr = extract_address(row)
        result_row = dict(row)
        if addr:
            geo = geocode(addr)
            result_row["_lat"] = geo["lat"] if geo else None
            result_row["_lon"] = geo["lon"] if geo else None
        else:
            result_row["_lat"] = None
            result_row["_lon"] = None
        enriched.append(result_row)
    return enriched
