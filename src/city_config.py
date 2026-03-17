"""Загрузчик профиля города — единственная точка входа для городских данных.

Все модули, которым нужны городские константы (районы, координаты, bbox,
timezone и т.д.), ДОЛЖНЫ использовать функции из этого модуля.
Прямое чтение config/city_profile.yaml в других модулях запрещено.

Переключение города:
    CITY_PROFILE=city_profile_omsk bot serve
    (по умолчанию: config/city_profile.yaml)

Кэширование:
    Профиль загружается один раз при первом обращении (lru_cache).
    Повторные вызовы не читают файл с диска.
    При смене CITY_PROFILE нужен перезапуск процесса.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml

# ── Путь к профилю ─────────────────────────────────────────────────────────────

def _profile_path() -> Path:
    """Определяет путь к YAML-профилю по env-переменной CITY_PROFILE.

    CITY_PROFILE может быть:
      - именем файла без расширения: 'city_profile_omsk'
        → config/city_profile_omsk.yaml
      - абсолютным путём: '/etc/mybot/omsk.yaml'
        → используется как есть
    """
    name = os.getenv("CITY_PROFILE", "city_profile")
    p = Path(name)
    if p.is_absolute():
        return p
    # Относительный путь — ищем в config/
    config_dir = Path(__file__).parent.parent / "config"
    candidate = config_dir / (name if name.endswith(".yaml") else f"{name}.yaml")
    if not candidate.exists():
        raise FileNotFoundError(
            f"City profile not found: {candidate}\n"
            f"Set CITY_PROFILE env var to point to a valid city_profile YAML."
        )
    return candidate


# ── Загрузчик (lru_cache — одна загрузка на весь жизненный цикл процесса) ──────

@lru_cache(maxsize=1)
def get_city_profile() -> dict:
    """Возвращает полный словарь профиля города.

    Кэшируется. Для сброса кэша (тесты): get_city_profile.cache_clear().
    """
    path = _profile_path()
    with open(path, encoding="utf-8") as f:
        profile = yaml.safe_load(f)
    if not profile or "city" not in profile:
        raise ValueError(f"Invalid city profile at {path}: missing 'city' key")
    return profile


# ── Базовые параметры города ───────────────────────────────────────────────────

def get_city_id() -> str:
    """Короткий ASCII-идентификатор города: 'novosibirsk', 'omsk'."""
    return get_city_profile()["city"]["id"]


def get_city_name(case: str = "nominative") -> str:
    """Название города в нужном падеже.

    Args:
        case: 'nominative'     → 'Новосибирск'
              'genitive'        → 'Новосибирска'
              'prepositional'   → 'Новосибирске'
    """
    city = get_city_profile()["city"]
    return {
        "nominative":   city["name"],
        "genitive":     city.get("name_genitive",     city["name"]),
        "prepositional": city.get("name_prepositional", city["name"]),
    }.get(case, city["name"])


def get_city_slug() -> str:
    """Слаг для 2GIS deep-link URL: 'novosibirsk'."""
    return get_city_profile()["city"].get("slug", get_city_id())


def get_timezone() -> str:
    """IANA timezone: 'Asia/Novosibirsk'."""
    return get_city_profile()["city"]["timezone"]


def get_utc_offset() -> int:
    """UTC offset в часах: 7 для Новосибирска."""
    return get_city_profile()["city"]["utc_offset"]


# ── География ──────────────────────────────────────────────────────────────────

def get_bbox_overpass() -> str:
    """Bbox в формате Overpass API: '(lat_min,lon_min,lat_max,lon_max)'.

    Всегда минимум 2 знака после точки (54.70, не 54.7) —
    совместимо со значением _NSK_BBOX в medical_fetcher.py и cameras_fetcher.py.
    """
    bb = get_city_profile()["city"]["bbox"]
    def _fmt(v: float) -> str:
        s = f"{v:.10f}".rstrip("0")
        # гарантируем минимум 2 знака после точки
        if "." not in s:
            return s + ".00"
        int_part, dec_part = s.split(".")
        if len(dec_part) < 2:
            dec_part = dec_part.ljust(2, "0")
        return f"{int_part}.{dec_part}"
    return f"({_fmt(bb['lat_min'])},{_fmt(bb['lon_min'])},{_fmt(bb['lat_max'])},{_fmt(bb['lon_max'])})"


def get_bbox_dict() -> dict:
    """Bbox как словарь: {lat_min, lat_max, lon_min, lon_max}."""
    return get_city_profile()["city"]["bbox"]


def get_city_center() -> tuple[float, float]:
    """Координаты центра города: (lat, lon)."""
    c = get_city_profile()["city"]["center"]
    return c["lat"], c["lon"]


# ── Административные районы ────────────────────────────────────────────────────

def get_districts() -> dict[str, list[str]]:
    """Словарь район → список стемов для сопоставления.

    Возвращает ту же структуру, что router.py DISTRICTS.
    """
    return get_city_profile()["districts"]


def get_sub_districts_compiled() -> list[tuple[re.Pattern, str, str]]:
    """Скомпилированные паттерны подрайонов.

    Возвращает список кортежей (pattern, parent_district, display_name) —
    точно такой же формат, как _SUB_DISTRICTS в router.py.
    Каждый подрайон может иметь несколько паттернов — они разворачиваются
    в отдельные строки, сохраняя порядок из YAML.
    """
    result: list[tuple[re.Pattern, str, str]] = []
    for sd in get_city_profile().get("sub_districts", []):
        parent = sd["parent"]
        name   = sd["name"]
        for pat_str in sd.get("patterns", []):
            result.append((re.compile(pat_str), parent, name))
    return result


def get_sub_districts_info() -> dict[str, tuple[str, list[str]]]:
    """Словарь для документации и рендера.

    Возвращает ту же структуру, что SUB_DISTRICTS_INFO в router.py:
    {display_name: (parent_district, [примеры написания])}
    """
    info: dict[str, tuple[str, list[str]]] = {}
    for sd in get_city_profile().get("sub_districts", []):
        info[sd["name"]] = (sd["parent"], sd.get("examples", []))
    return info


def get_district_coords() -> dict[str, tuple[float, float]]:
    """Координаты центров районов в формате 2GIS: {район: (lon, lat)}.

    Возвращает ту же структуру, что DISTRICT_COORDS в transport_api.py.
    """
    raw = get_city_profile().get("district_coords", {})
    return {name: (v["lon"], v["lat"]) for name, v in raw.items()}


@lru_cache(maxsize=1)
def get_district_strip_re() -> re.Pattern:
    """Regex для удаления названий районов из адресной строки.

    Используется в geocoder.py для нормализации адресов.
    Пример: 'ул. Ленина, Советский район' → 'ул. Ленина'
    """
    names = "|".join(
        re.escape(d.replace(" район", "").replace(" округ", ""))
        for d in get_districts()
    )
    return re.compile(
        rf"\b({names})\s+(район|округ|р-н)\b",
        re.IGNORECASE,
    )


def get_city_stopwords() -> set[str]:
    """Стоп-слова специфичные для города (для геокодера и роутера).

    НЕ включает общие стоп-слова (район, тип, количество…) — они в router.py.
    """
    return set(get_city_profile().get("city_stopwords", []))


# ── Возможности города ─────────────────────────────────────────────────────────

def get_feature(key: str, default: Any = None) -> Any:
    """Читает флаг или значение из секции features профиля.

    Примеры:
        get_feature('has_metro')               → True
        get_feature('airport_name')            → 'Аэропорт Толмачёво'
        get_feature('has_tram', default=False) → False (если не задано)
    """
    return get_city_profile().get("features", {}).get(key, default)


# ── Экология ───────────────────────────────────────────────────────────────────

def get_ecology_stations() -> list[dict]:
    """Список станций мониторинга с полями station_id, district, address, lat, lon.

    Заменяет NSK_ECOLOGY_STATIONS из constants.py.
    Формат совместим: ecology_fetcher.py и medical_cache.py читают те же поля.
    """
    # Нормализуем: добавляем 'latitude'/'longitude' как алиасы для обратной совместимости
    stations = []
    for s in get_city_profile().get("ecology_stations", []):
        entry = dict(s)
        entry.setdefault("latitude",  s["lat"])
        entry.setdefault("longitude", s["lon"])
        stations.append(entry)
    return stations


# ── Открытые данные ────────────────────────────────────────────────────────────

def get_opendata_base_url() -> str:
    """Базовый URL портала открытых данных города."""
    return get_city_profile().get("opendata_base_url", "")


# ── База данных ────────────────────────────────────────────────────────────────

# _PROJECT_ROOT нужен get_db_path() раньше, чем объявлен ниже — объявим здесь
_PROJECT_ROOT = Path(__file__).parent.parent


def get_db_path() -> Path:
    """Путь к DuckDB-файлу для текущего активного города.

    Каждый город хранит данные изолированно:
        data/cities/novosibirsk/cache.db
        data/cities/omsk/cache.db
        ...

    Директория создаётся автоматически при первом обращении.
    НЕ кэшируется — переключение города через set-active-city работает
    мгновенно без перезапуска сервера.
    """
    city_id = get_city_id()
    db_dir = _PROJECT_ROOT / "data" / "cities" / city_id
    db_dir.mkdir(parents=True, exist_ok=True)
    return db_dir / "cache.db"


# ── Статические датасеты ───────────────────────────────────────────────────────

# _PROJECT_ROOT уже объявлен выше (нужен get_db_path)

# Заглушка — возвращается когда датасет не настроен для города
_DATASET_STUB: dict = {
    "enabled": False,
    "file": None,
    "note": "Данные для этого города ещё не добавлены",
}


def get_static_dataset(name: str) -> dict:
    """Конфиг статического датасета по имени.

    Args:
        name: 'emissions', 'heat_sources', 'metro', 'airport'

    Returns:
        Словарь из city_profile.yaml → static_datasets.<name>,
        или заглушку {'enabled': False, 'file': None, 'note': '...'}.
    """
    datasets = get_city_profile().get("static_datasets", {})
    return datasets.get(name, dict(_DATASET_STUB))


def is_dataset_available(name: str) -> bool:
    """True если датасет включён и файл данных существует на диске."""
    ds = get_static_dataset(name)
    if not ds.get("enabled") or not ds.get("file"):
        return False
    return (_PROJECT_ROOT / ds["file"]).exists()


def get_dataset_path(name: str) -> Path | None:
    """Абсолютный путь к файлу датасета или None если недоступен.

    Не бросает исключений — безопасно вызывать без проверки.
    """
    ds = get_static_dataset(name)
    if not ds.get("enabled") or not ds.get("file"):
        return None
    p = _PROJECT_ROOT / ds["file"]
    return p if p.exists() else None


# Типизированные геттеры для конкретных датасетов
# (используются emissions.py и heat_sources.py в Фазе 1)

def get_emissions_path() -> Path | None:
    """Путь к JSON-файлу выбросов 2-ТП Воздух или None."""
    return get_dataset_path("emissions")


def get_emissions_meta_from_profile() -> dict:
    """Метаданные датасета выбросов из профиля города (без чтения JSON)."""
    ds = get_static_dataset("emissions")
    return {
        "scope":    ds.get("scope", ""),
        "year":     ds.get("year"),
        "form":     ds.get("form", ""),
        "source":   ds.get("source", ""),
        "published": ds.get("published", ""),
        "municipalities_count": ds.get("municipalities_count", 0),
        "available": is_dataset_available("emissions"),
    }


def get_heat_sources_path() -> Path | None:
    """Путь к GeoJSON-файлу тепловых источников или None."""
    return get_dataset_path("heat_sources")


def get_metro_path() -> Path | None:
    """Путь к JSON-файлу данных метро или None."""
    return get_dataset_path("metro")


def get_airport_path() -> Path | None:
    """Путь к JSON-файлу данных аэропорта или None."""
    return get_dataset_path("airport")
