"""Обновление устаревших данных без ожидания в запросе человека (BEKLOG № 12).

Раньше /ask при устаревшей теме OSM сам перекачивал её через Overpass и ждал: парковки,
детские сады, спортплощадки держали ответ больше 90 с (замер 2026-11-17), а при неудаче
Overpass ждал и следующий запрос. Теперь (разбор — docs/OSM-OBNOVLENIE-RAZBOR.md):

* данные есть, но устарели — ответ сразу из кэша, обновление в фоновом потоке;
* данных нет совсем — обновление в запросе (отвечать нечем), как раньше;
* одновременно — не больше одного обновления на ключ; после неудачи — пауза
  COOLDOWN_SECONDS, в том числе для обновления в запросе.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Callable

log = logging.getLogger(__name__)

COOLDOWN_SECONDS = 30 * 60

_lock = threading.Lock()
_state: dict[str, dict] = {}          # ключ → {"running": bool, "failed_at": float | None}


def refresh_if_stale(
    key: str,
    is_stale: Callable[[], bool],
    has_data: Callable[[], bool],
    refresh: Callable[[], int | None],
) -> str:
    """Обновить данные ключа, если срок вышел, не держа запрос, когда отвечать уже есть чем.

    refresh() возвращает число записанных записей; 0, None или исключение — неудача.
    Возвращает, что сделано: "fresh" | "running" | "cooldown" | "background" | "sync".
    """
    if not is_stale():
        return "fresh"
    with _lock:
        st = _state.setdefault(key, {"running": False, "failed_at": None})
        if st["running"]:
            return "running"
        if st["failed_at"] is not None and time.monotonic() - st["failed_at"] < COOLDOWN_SECONDS:
            return "cooldown"
        st["running"] = True

    def run() -> None:
        ok = False
        try:
            ok = bool(refresh())
        except Exception as exc:                       # неудача обновления не роняет ответ
            log.warning("stale_refresh: %s — обновление не удалось: %s", key, exc)
        finally:
            with _lock:
                st["running"] = False
                st["failed_at"] = None if ok else time.monotonic()
        if not ok:
            log.warning("stale_refresh: %s — данных не получено, следующая попытка через %d мин",
                        key, COOLDOWN_SECONDS // 60)

    try:
        cached = has_data()
    except Exception:
        cached = False
    if cached:
        threading.Thread(target=run, name=f"stale-refresh:{key}", daemon=True).start()
        return "background"
    run()
    return "sync"


def reset_state() -> None:
    """Для тестов: забыть идущие обновления и паузы."""
    with _lock:
        _state.clear()
