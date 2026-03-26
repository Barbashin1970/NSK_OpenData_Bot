"""Тихое обновление данных без CLI-рендера.

Используется тремя механизмами:
  1. Lazy fallback в api.py — когда пользователь запросил тему, которой нет в БД.
  2. Background preloader — стартует через 15 сек после запуска, грузит все темы
     поочерёдно с паузами, чтобы не нагружать сервер и не мешать health check.
  3. Multi-city updater — фоново обновляет экологию, медицину и камеры для ВСЕХ
     городов (не только активного), чтобы данные были готовы при переключении.
"""

import asyncio
import logging
import os
import threading
from contextlib import contextmanager
from pathlib import Path

from .cache import table_exists
from .fetcher import is_stale
from .registry import list_topics

log = logging.getLogger(__name__)

# Порядок фоновой загрузки: от самых популярных к редким.
# Первые темы загрузятся раньше — пользователи не ждут.
PRELOAD_ORDER: list[str] = [
    "stops",
    "schools",
    "kindergartens",
    "pharmacies",
    "libraries",
    "parking",
    "sport_grounds",
    "sport_orgs",
    "culture",
    "construction_permits",
    "construction_commissioned",
]

# Пауза между темами при фоновой загрузке (секунды)
_PRELOAD_INTERVAL = 5

# Темы, которые сейчас грузятся (защита от параллельных вызовов)
_loading: set[str] = set()


def refresh_topic(topic: str, force: bool = False) -> int:
    """Скачивает и загружает одну тему в DuckDB без CLI-рендера.

    Возвращает:
      > 0 — количество загруженных строк
      -1  — данные уже свежие, загрузка не нужна
       0  — ошибка или нет данных
    """
    from .fetcher import fetch_csv
    from .parser import read_csv
    from .cache import load_into_db
    from .registry import get_dataset

    ds = get_dataset(topic)
    if not ds:
        return 0

    ttl = int(ds.get("ttl_hours", 24))
    if not force and not is_stale(topic, ttl) and table_exists(topic):
        return -1  # данные свежие

    url = ds.get("data_url", "")
    if not url:
        return 0

    try:
        path = fetch_csv(topic, url, ttl_hours=ttl, force=force)
        if not path:
            return 0
        rows = read_csv(path, ds)
        if not rows:
            return 0
        n = load_into_db(topic, rows, ds)
        log.info(f"refresh_topic({topic}): загружено {n} строк")
        return n
    except Exception as e:
        log.error(f"refresh_topic({topic}): {e}")
        return 0


def ensure_fresh(topic: str) -> bool:
    """Гарантирует наличие данных для темы. Блокирует до завершения загрузки.

    Безопасно вызывать на каждый запрос: если данные свежие — мгновенно (только
    чтение meta.json + проверка DuckDB information_schema).

    Возвращает True если данные доступны после вызова.
    """
    if table_exists(topic) and not is_stale(topic):
        return True  # быстрый путь: данные есть, TTL не истёк

    if topic in _loading:
        # Фоновый загрузчик уже тащит эту тему — ждём чуть-чуть
        log.info(f"ensure_fresh({topic}): уже загружается фоном, ждём")
        return table_exists(topic)  # вернём что есть прямо сейчас

    _loading.add(topic)
    try:
        n = refresh_topic(topic)
        return n != 0  # -1 = было свежим, >0 = загружено
    finally:
        _loading.discard(topic)


async def preload_all_async(delay_start: float = 15.0) -> None:
    """Фоновый preloader: стартует после задержки, грузит темы одну за одной.

    Запускать через asyncio.create_task() при старте API-сервера.
    Не трогает темы с актуальными данными (TTL не истёк).
    """
    await asyncio.sleep(delay_start)
    log.info("Фоновый preloader: старт")

    for topic in PRELOAD_ORDER:
        if table_exists(topic) and not is_stale(topic):
            log.debug(f"preload: {topic} — уже свежий, пропуск")
            await asyncio.sleep(0)  # уступаем event loop
            continue

        log.info(f"preload: загрузка {topic}...")
        try:
            n = await asyncio.to_thread(refresh_topic, topic)
            if n > 0:
                log.info(f"preload: {topic} готов ({n} строк)")
            elif n == -1:
                log.debug(f"preload: {topic} — уже свежий")
        except Exception as e:
            log.warning(f"preload: ошибка {topic}: {e}")

        await asyncio.sleep(_PRELOAD_INTERVAL)

    log.info("Фоновый preloader: все темы обработаны")


# ── Периодический авто-рефреш (каждые N часов) ──────────────────────────────
# На Railway (и любом сервере) данные устаревают если нет пользователей.
# Этот цикл каждые 12 часов проверяет TTL и обновляет устаревшие темы,
# чтобы чипы на главной всегда показывали «актуален».

_REFRESH_INTERVAL_HOURS = 12


async def periodic_refresh_loop(interval_hours: float = _REFRESH_INTERVAL_HOURS) -> None:
    """Бесконечный цикл: каждые interval_hours обновляет устаревшие CSV-темы.

    Запускать через asyncio.create_task() при старте API-сервера.
    Первая итерация начинается через interval_hours после preloader'а.
    """
    interval_sec = interval_hours * 3600
    log.info("periodic_refresh_loop: старт (интервал %.0f ч)", interval_hours)

    while True:
        await asyncio.sleep(interval_sec)
        log.info("periodic_refresh_loop: проверка устаревших тем…")
        refreshed = 0
        for topic in PRELOAD_ORDER:
            try:
                if is_stale(topic) or not table_exists(topic):
                    n = await asyncio.to_thread(refresh_topic, topic, True)
                    if n > 0:
                        refreshed += 1
                        log.info("periodic_refresh: %s — обновлено (%d строк)", topic, n)
                    await asyncio.sleep(_PRELOAD_INTERVAL)
            except Exception as e:
                log.warning("periodic_refresh: ошибка %s: %s", topic, e)
        log.info("periodic_refresh_loop: завершено, обновлено %d тем", refreshed)


# ── Мульти-город: фоновое обновление всех городов ─────────────────────────

_city_switch_lock = threading.Lock()
_CONFIG_DIR = Path(__file__).parent.parent / "config"

# Пауза между городами (секунды) — бережём API лимиты
_CITY_INTERVAL = 30
# Интервал полного цикла (часы)
_MULTI_CITY_INTERVAL_HOURS = 6


@contextmanager
def _temporary_city(profile_name: str):
    """Переключает активный город на время выполнения блока, затем возвращает обратно.

    Защищено lock'ом — не конфликтует с пользовательскими запросами.
    Время удержания lock'а: ~10-30 сек на город (сетевые запросы внутри).
    """
    from .city_config import get_city_profile, get_district_strip_re

    def _clear_caches():
        get_city_profile.cache_clear()
        try:
            get_district_strip_re.cache_clear()
        except Exception:
            pass
        try:
            from .district_classifier import reload_boundaries
            reload_boundaries()
        except Exception:
            pass

    with _city_switch_lock:
        old_profile = os.environ.get("CITY_PROFILE", "city_profile")
        os.environ["CITY_PROFILE"] = profile_name
        _clear_caches()
        try:
            yield
        finally:
            os.environ["CITY_PROFILE"] = old_profile
            _clear_caches()


def _list_city_profiles() -> list[dict]:
    """Возвращает [{profile_name, city_id, city_name}, ...] для всех городов."""
    import yaml as _yaml

    cities = []
    for p in sorted(_CONFIG_DIR.glob("city_profile*.yaml")):
        try:
            with open(p, encoding="utf-8") as f:
                d = _yaml.safe_load(f)
            if d and "city" in d:
                cities.append({
                    "profile_name": p.stem,
                    "city_id": d["city"].get("id", ""),
                    "city_name": d["city"].get("name", ""),
                })
        except Exception:
            continue
    return cities


def _refresh_city_ecology() -> int:
    """Обновляет экологию для текущего активного города. Возвращает кол-во записей."""
    try:
        from .ecology_cache import upsert_stations, upsert_measurements, is_ecology_stale
        from .ecology_fetcher import fetch_all_ecology

        if not is_ecology_stale():
            return -1

        upsert_stations()
        records = fetch_all_ecology()
        return upsert_measurements(records)
    except Exception as e:
        log.warning("ecology refresh: %s", e)
        return 0


def _refresh_city_medical() -> int:
    """Обновляет медучреждения (OSM) для текущего города. Возвращает кол-во записей."""
    try:
        from .medical_cache import is_medical_stale, upsert_medical
        from .medical_fetcher import fetch_medical

        if not is_medical_stale():
            return -1

        data = fetch_medical()
        if data:
            return upsert_medical(data)
        return 0
    except Exception as e:
        log.warning("medical refresh: %s", e)
        return 0


def _refresh_city_cameras() -> int:
    """Обновляет камеры (OSM) для текущего города. Возвращает кол-во записей."""
    try:
        from .cameras_cache import is_cameras_stale, upsert_cameras
        from .cameras_fetcher import fetch_cameras

        if not is_cameras_stale():
            return -1

        data = fetch_cameras()
        if data:
            return upsert_cameras(data)
        return 0
    except Exception as e:
        log.warning("cameras refresh: %s", e)
        return 0


def _refresh_one_city(profile_name: str, city_name: str) -> dict:
    """Обновляет все бесплатные источники (экология, медицина, камеры) для одного города."""
    results = {}

    with _temporary_city(profile_name):
        eco = _refresh_city_ecology()
        results["ecology"] = eco
        if eco > 0:
            log.info("multi-city [%s]: экология — %d записей", city_name, eco)

        med = _refresh_city_medical()
        results["medical"] = med
        if med > 0:
            log.info("multi-city [%s]: медицина — %d записей", city_name, med)

        cam = _refresh_city_cameras()
        results["cameras"] = cam
        if cam > 0:
            log.info("multi-city [%s]: камеры — %d записей", city_name, cam)

    return results


async def multi_city_refresh_loop(
    interval_hours: float = _MULTI_CITY_INTERVAL_HOURS,
    initial_delay: float = 120.0,
) -> None:
    """Фоновый цикл: обновляет экологию/медицину/камеры для ВСЕХ городов.

    Запускать через asyncio.create_task() при старте API-сервера.
    Первый цикл стартует через initial_delay секунд (после основного preloader'а).
    Повторяется каждые interval_hours часов.

    Все источники бесплатные (Open-Meteo, OSM Overpass) — нет расхода ресурсов.
    """
    await asyncio.sleep(initial_delay)
    interval_sec = interval_hours * 3600
    log.info("multi_city_refresh_loop: старт (интервал %.0f ч, %d городов)",
             interval_hours, len(_list_city_profiles()))

    while True:
        cities = _list_city_profiles()
        active_id = os.environ.get("CITY_PROFILE", "city_profile")
        updated = 0

        for city in cities:
            # Активный город уже обновляется основным preloader'ом
            if city["profile_name"] == active_id:
                continue

            try:
                results = await asyncio.to_thread(
                    _refresh_one_city, city["profile_name"], city["city_name"]
                )
                if any(v > 0 for v in results.values()):
                    updated += 1
            except Exception as e:
                log.warning("multi-city [%s]: ошибка — %s", city["city_name"], e)

            await asyncio.sleep(_CITY_INTERVAL)

        log.info("multi_city_refresh_loop: завершено, обновлено %d городов", updated)
        await asyncio.sleep(interval_sec)
