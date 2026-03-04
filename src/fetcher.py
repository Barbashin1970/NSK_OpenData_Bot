"""HTTP-загрузка CSV-файлов с opendata.novo-sibirsk.ru.

Логирует запросы в data/logs/fetch.log.
Сохраняет сырые файлы в data/raw/<topic>/<timestamp>.csv.
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from .constants import DATA_DIR, HEADERS, LOGS_DIR, META_FILE, RAW_DIR, TIMEOUT

log = logging.getLogger(__name__)


def _ensure_dirs(topic: str) -> Path:
    topic_dir = RAW_DIR / topic
    topic_dir.mkdir(parents=True, exist_ok=True)
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return topic_dir


def _log_request(url: str, status: int, size: int, elapsed: float) -> None:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).isoformat()
    line = f"{ts} | status={status} | size={size}B | elapsed={elapsed:.2f}s | url={url}\n"
    with open(LOGS_DIR / "fetch.log", "a", encoding="utf-8") as f:
        f.write(line)
    log.info(line.strip())


def load_meta() -> dict:
    """Загружает meta.json с датами обновления."""
    if META_FILE.exists():
        with open(META_FILE, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_meta(meta: dict) -> None:
    """Сохраняет meta.json."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(META_FILE, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


def is_stale(topic: str, ttl_hours: int = 24) -> bool:
    """Проверяет, устарел ли кэш для темы."""
    meta = load_meta()
    last = meta.get(topic, {}).get("last_updated")
    if not last:
        return True
    last_dt = datetime.fromisoformat(last)
    if last_dt.tzinfo is None:
        last_dt = last_dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc) - last_dt > timedelta(hours=ttl_hours)


def fetch_csv(topic: str, url: str, ttl_hours: int = 24, force: bool = False) -> Path | None:
    """Скачивает CSV-файл для темы. Возвращает путь к сохранённому файлу.

    Если кэш актуален и force=False — возвращает существующий файл без запроса.
    При ошибке — возвращает последний имеющийся файл (если есть).
    """
    topic_dir = _ensure_dirs(topic)
    existing = sorted(topic_dir.glob("*.csv"), reverse=True) + \
               sorted(topic_dir.glob("*.CSV"), reverse=True)

    if not force and not is_stale(topic, ttl_hours) and existing:
        log.info(f"Кэш актуален для '{topic}', используем: {existing[0]}")
        return existing[0]

    ts_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    suffix = Path(url).suffix.lower() or ".csv"
    out_file = topic_dir / f"{ts_str}{suffix}"

    start = time.monotonic()
    try:
        resp = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        elapsed = time.monotonic() - start
        _log_request(url, resp.status_code, len(resp.content), elapsed)

        if resp.status_code != 200:
            log.warning(f"HTTP {resp.status_code} при загрузке {url}")
            return existing[0] if existing else None

        out_file.write_bytes(resp.content)

        meta = load_meta()
        meta[topic] = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "source_url": url,
            "file": str(out_file),
            "file_hash": hashlib.md5(resp.content).hexdigest(),
            "size_bytes": len(resp.content),
        }
        save_meta(meta)

        log.info(f"Скачан файл для '{topic}': {out_file} ({len(resp.content)} байт)")
        return out_file

    except Exception as exc:
        elapsed = time.monotonic() - start
        _log_request(url, 0, 0, elapsed)
        log.error(f"Ошибка загрузки {url}: {exc}")
        if existing:
            log.warning(f"Используем последний кэш: {existing[0]}")
            return existing[0]
        return None
