"""Тихое обновление данных без CLI-рендера.

Используется двумя механизмами:
  1. Lazy fallback в api.py — когда пользователь запросил тему, которой нет в БД.
  2. Background preloader — стартует через 15 сек после запуска, грузит все темы
     поочерёдно с паузами, чтобы не нагружать сервер и не мешать health check.
"""

import asyncio
import logging

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
