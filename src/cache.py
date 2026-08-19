"""Управление локальной базой данных DuckDB.

Таблица для каждой темы: topic_<name>
Мета-информация: meta.json (даты обновления, кол-во строк)
"""

import logging
import threading
from typing import Any

import duckdb

from .constants import DATA_DIR
from .fetcher import load_meta, save_meta

log = logging.getLogger(__name__)

# Одно корневое соединение на файл БД, дальше — курсоры.
#
# DuckDB запрещает открывать один файл несколькими соединениями внутри одного
# процесса: второй duckdb.connect() к тому же пути падает с
#   Binder Error: Unique file handle conflict: Cannot attach "cache" —
#   the database file ... is already attached by database "cache"
# Страница шлёт запросы пачками (Promise.all), FastAPI выполняет sync-роуты в
# threadpool — и часть из них разбивалась об этот конфликт. Роуты глушили
# исключение и отдавали 200 с пустым списком, поэтому наружу это выглядело не
# как ошибка, а как «данных нет»: пустой рейтинг, пропавшие фильтры, график
# целиком из «чистых» дней.
#
# cursor() создаёт независимый дескриптор к уже открытой БД: у него свои
# транзакции, close() закрывает только курсор, и параллельные запросы больше
# не конфликтуют. Ключ — путь к файлу, поэтому переключение города через
# /studio по-прежнему работает без перезапуска.
_ROOT_CONNS: dict[str, duckdb.DuckDBPyConnection] = {}
_ROOT_LOCK = threading.Lock()


def _get_conn() -> duckdb.DuckDBPyConnection:
    """Курсор к БД текущего активного города.

    Путь: data/cities/{city_id}/cache.db — каждый город изолирован.
    Вызывающий код закрывает курсор как обычное соединение; корневое
    соединение живёт до конца процесса.
    """
    from .city_config import get_db_path
    path = str(get_db_path())

    with _ROOT_LOCK:
        root = _ROOT_CONNS.get(path)
        if root is None:
            root = duckdb.connect(path)
            _ROOT_CONNS[path] = root
        try:
            return root.cursor()
        except Exception as e:
            # Корневое соединение могло быть закрыто извне — переоткрываем
            log.warning("DuckDB: пересоздаю соединение к %s (%s)", path, e)
            root = duckdb.connect(path)
            _ROOT_CONNS[path] = root
            return root.cursor()


def table_name(topic: str) -> str:
    return f"topic_{topic}"


def load_into_db(topic: str, rows: list[dict[str, Any]], dataset_config: dict) -> int:
    """Загружает список строк в DuckDB-таблицу. Возвращает кол-во загруженных строк."""
    if not rows:
        log.warning(f"Нет данных для загрузки в таблицу {topic}")
        return 0

    tbl = table_name(topic)
    all_cols: set[str] = set()
    for row in rows:
        all_cols.update(row.keys())
    cols = sorted(all_cols)

    conn = _get_conn()
    try:
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        col_defs = ", ".join(f'"{c}" VARCHAR' for c in cols)
        conn.execute(f"CREATE TABLE {tbl} ({col_defs})")

        placeholders = ", ".join(["?"] * len(cols))
        insert_sql = f"INSERT INTO {tbl} VALUES ({placeholders})"

        batch: list[list] = []
        for row in rows:
            batch.append([row.get(c, "") for c in cols])
            if len(batch) >= 1000:
                conn.executemany(insert_sql, batch)
                batch = []
        if batch:
            conn.executemany(insert_sql, batch)

        meta = load_meta()
        meta.setdefault(topic, {}).update({"rows": len(rows), "cols": len(cols)})
        save_meta(meta)

        log.info(f"Загружено {len(rows)} строк в таблицу '{tbl}'")
        return len(rows)
    finally:
        conn.close()


def query(sql: str, params: list | None = None) -> list[dict]:
    """Выполняет SQL-запрос и возвращает список словарей."""
    conn = _get_conn()
    try:
        cursor = conn.execute(sql, params) if params else conn.execute(sql)
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    except Exception as e:
        log.error(f"Ошибка SQL: {e}\nЗапрос: {sql}")
        raise
    finally:
        conn.close()


def table_exists(topic: str) -> bool:
    """Проверяет, существует ли таблица темы в DuckDB."""
    tbl = table_name(topic)
    conn = _get_conn()
    try:
        result = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [tbl],
        ).fetchone()
        return bool(result and result[0] > 0)
    except Exception:
        return False
    finally:
        conn.close()


def get_table_info(topic: str) -> dict:
    """Возвращает информацию о таблице (кол-во строк, колонок)."""
    tbl = table_name(topic)
    info = load_meta().get(topic, {})
    if not table_exists(topic):
        return info
    conn = _get_conn()
    try:
        count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        cols_result = conn.execute(f"PRAGMA table_info({tbl})").fetchall()
        info["rows"] = count
        info["cols"] = len(cols_result)
    except Exception:
        pass
    finally:
        conn.close()
    return info
