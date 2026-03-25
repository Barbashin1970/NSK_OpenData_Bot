"""Query log: stores every /ask request with routing metadata.

Uses a single DuckDB file at data/query_log.db (shared across cities).
Designed to survive Volume mounts on Railway.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any

import duckdb

from .constants import DATA_DIR

log = logging.getLogger(__name__)

_DB_PATH = DATA_DIR / "query_log.db"

_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS query_log (
    id          INTEGER PRIMARY KEY,
    ts          TIMESTAMP DEFAULT current_timestamp,
    query       VARCHAR NOT NULL,
    topic       VARCHAR,
    topic_name  VARCHAR,
    confidence  DOUBLE,
    operation   VARCHAR,
    district    VARCHAR,
    sub_district VARCHAR,
    street      VARCHAR,
    extra_filters VARCHAR,
    matched_keywords VARCHAR,
    utility_type VARCHAR,
    result_count INTEGER,
    city_id     VARCHAR,
    source      VARCHAR DEFAULT 'web'
)
"""

_SEQ = "CREATE SEQUENCE IF NOT EXISTS query_log_seq START 1"

_UNKNOWN_SQL = """
CREATE TABLE IF NOT EXISTS unknown_queries (
    query       VARCHAR PRIMARY KEY,
    count       INTEGER DEFAULT 1,
    last_seen   TIMESTAMP DEFAULT current_timestamp,
    city_id     VARCHAR
)
"""


def _conn() -> duckdb.DuckDBPyConnection:
    _DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    c = duckdb.connect(str(_DB_PATH))
    c.execute(_SEQ)
    c.execute(_CREATE_SQL)
    c.execute(_UNKNOWN_SQL)
    return c


def log_query(
    query: str,
    topic: str | None = None,
    topic_name: str | None = None,
    confidence: float | None = None,
    operation: str | None = None,
    district: str | None = None,
    sub_district: str | None = None,
    street: str | None = None,
    extra_filters: dict | None = None,
    matched_keywords: list[str] | None = None,
    utility_type: str | None = None,
    result_count: int | None = None,
    city_id: str | None = None,
    source: str = "web",
) -> None:
    """Record a query to the log. Fire-and-forget, never raises."""
    try:
        conn = _conn()
        conn.execute(
            """INSERT INTO query_log
               (id, query, topic, topic_name, confidence, operation,
                district, sub_district, street, extra_filters,
                matched_keywords, utility_type, result_count, city_id, source)
            VALUES (nextval('query_log_seq'), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                query,
                topic,
                topic_name,
                confidence,
                operation,
                district,
                sub_district,
                street,
                str(extra_filters) if extra_filters else None,
                ", ".join(matched_keywords) if matched_keywords else None,
                utility_type,
                result_count,
                city_id,
                source,
            ],
        )
        # Нераспознанные запросы (confidence < 0.35 или UNKNOWN) → unknown_queries
        if (confidence is not None and confidence < 0.35) or operation == "UNKNOWN":
            q_lower = query.strip().lower()
            existing = conn.execute(
                "SELECT count FROM unknown_queries WHERE query = ?", [q_lower]
            ).fetchone()
            if existing:
                conn.execute(
                    "UPDATE unknown_queries SET count = count + 1, last_seen = current_timestamp WHERE query = ?",
                    [q_lower],
                )
            else:
                conn.execute(
                    "INSERT INTO unknown_queries (query, count, city_id) VALUES (?, 1, ?)",
                    [q_lower, city_id],
                )
        conn.close()
    except Exception as e:
        log.warning("query_log write failed: %s", e)


def get_unknown_queries(limit: int = 20) -> list[dict[str, Any]]:
    """Топ нераспознанных запросов по частоте."""
    try:
        conn = _conn()
        rows = conn.execute(
            """SELECT query, count, last_seen, city_id
               FROM unknown_queries
               ORDER BY count DESC, last_seen DESC
               LIMIT ?""",
            [limit],
        ).fetchall()
        conn.close()
        return [
            {"query": r[0], "count": r[1], "last_seen": str(r[2]), "city_id": r[3]}
            for r in rows
        ]
    except Exception as e:
        log.warning("unknown_queries read failed: %s", e)
        return []


def remove_unknown_query(query: str) -> bool:
    """Удалить запрос из unknown_queries (после добавления в словарь)."""
    try:
        conn = _conn()
        conn.execute("DELETE FROM unknown_queries WHERE query = ?", [query.strip().lower()])
        conn.close()
        return True
    except Exception as e:
        log.warning("unknown_queries remove failed: %s", e)
        return False


def get_history(
    limit: int = 100,
    offset: int = 0,
    topic: str | None = None,
    city_id: str | None = None,
    search: str | None = None,
    source: str | None = None,
) -> dict[str, Any]:
    """Retrieve query history with optional filters."""
    try:
        conn = _conn()
        where_parts = []
        params: list = []

        if topic:
            where_parts.append("topic = ?")
            params.append(topic)
        if city_id:
            where_parts.append("city_id = ?")
            params.append(city_id)
        if search:
            where_parts.append("query ILIKE ?")
            params.append(f"%{search}%")
        if source:
            where_parts.append("source = ?")
            params.append(source)

        where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""

        total = conn.execute(
            f"SELECT COUNT(*) FROM query_log {where_sql}", params
        ).fetchone()[0]

        rows = conn.execute(
            f"""SELECT id, ts, query, topic, topic_name, confidence,
                       operation, district, sub_district, street,
                       extra_filters, matched_keywords, utility_type,
                       result_count, city_id, source
                FROM query_log {where_sql}
                ORDER BY ts DESC
                LIMIT ? OFFSET ?""",
            params + [limit, offset],
        ).fetchall()

        columns = [
            "id", "ts", "query", "topic", "topic_name", "confidence",
            "operation", "district", "sub_district", "street",
            "extra_filters", "matched_keywords", "utility_type",
            "result_count", "city_id", "source",
        ]
        result = []
        for row in rows:
            d = dict(zip(columns, row))
            if d["ts"]:
                d["ts"] = str(d["ts"])
            result.append(d)

        conn.close()
        return {"total": total, "rows": result, "limit": limit, "offset": offset}

    except Exception as e:
        log.warning("query_log read failed: %s", e)
        return {"total": 0, "rows": [], "limit": limit, "offset": offset, "error": str(e)}


def get_stats() -> dict[str, Any]:
    """Aggregate statistics for the query log."""
    try:
        conn = _conn()
        total = conn.execute("SELECT COUNT(*) FROM query_log").fetchone()[0]
        by_topic = conn.execute(
            "SELECT topic, COUNT(*) as cnt FROM query_log WHERE topic IS NOT NULL GROUP BY topic ORDER BY cnt DESC"
        ).fetchall()
        by_operation = conn.execute(
            "SELECT operation, COUNT(*) as cnt FROM query_log WHERE operation IS NOT NULL GROUP BY operation ORDER BY cnt DESC"
        ).fetchall()
        unknown = conn.execute(
            "SELECT COUNT(*) FROM query_log WHERE operation = 'UNKNOWN'"
        ).fetchone()[0]
        avg_conf = conn.execute(
            "SELECT AVG(confidence) FROM query_log WHERE confidence IS NOT NULL"
        ).fetchone()[0]
        low_conf = conn.execute(
            "SELECT COUNT(*) FROM query_log WHERE confidence IS NOT NULL AND confidence < 0.5"
        ).fetchone()[0]

        conn.close()
        return {
            "total_queries": total,
            "unknown_queries": unknown,
            "low_confidence": low_conf,
            "avg_confidence": round(avg_conf, 3) if avg_conf else None,
            "by_topic": [{"topic": t, "count": c} for t, c in by_topic],
            "by_operation": [{"operation": o, "count": c} for o, c in by_operation],
        }
    except Exception as e:
        log.warning("query_log stats failed: %s", e)
        return {"total_queries": 0, "error": str(e)}


def clear_log() -> int:
    """Delete all entries. Returns count deleted."""
    try:
        conn = _conn()
        total = conn.execute("SELECT COUNT(*) FROM query_log").fetchone()[0]
        conn.execute("DELETE FROM query_log")
        conn.close()
        return total
    except Exception as e:
        log.warning("query_log clear failed: %s", e)
        return 0
