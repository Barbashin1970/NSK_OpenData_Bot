"""Хранилище Пространства задач в DuckDB.

Таблицы:
  - ts_contractors  — справочник контрагентов (аварийные службы, МУПы и т.д.)
  - ts_initiatives  — инициативы (аналог Portfolio/Project)
  - ts_tasks         — задачи, привязанные к инициативам и контрагентам
  - ts_comments      — комментарии к задачам

Все таблицы используют префикс ts_ (Task Space) для изоляции от остальных данных.
"""

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from .cache import _get_conn

log = logging.getLogger(__name__)


# ── DDL ──────────────────────────────────────────────────────────────────────

_CONTRACTORS_DDL = """
CREATE TABLE IF NOT EXISTS ts_contractors (
    contractor_id   VARCHAR PRIMARY KEY,
    category        VARCHAR,
    org_name        VARCHAR,
    duty_phone      VARCHAR,
    work_hours      VARCHAR,
    head_name       VARCHAR,
    head_phone      VARCHAR,
    email           VARCHAR,
    channel_type    VARCHAR DEFAULT 'email_only',
    created_at      VARCHAR
)
"""

_INITIATIVES_DDL = """
CREATE TABLE IF NOT EXISTS ts_initiatives (
    initiative_id   VARCHAR PRIMARY KEY,
    title           VARCHAR NOT NULL,
    direction       VARCHAR,
    description     VARCHAR,
    status          VARCHAR DEFAULT 'draft',
    period          VARCHAR,
    created_at      VARCHAR,
    updated_at      VARCHAR
)
"""

_TASKS_DDL = """
CREATE TABLE IF NOT EXISTS ts_tasks (
    task_id         VARCHAR PRIMARY KEY,
    title           VARCHAR NOT NULL,
    description     VARCHAR,
    initiative_id   VARCHAR,
    department      VARCHAR,
    contractor_id   VARCHAR,
    assignee        VARCHAR,
    priority        VARCHAR DEFAULT 'P3',
    status          VARCHAR DEFAULT 'todo',
    due_date        VARCHAR,
    parent_task_id  VARCHAR,
    tags            VARCHAR,
    created_at      VARCHAR,
    updated_at      VARCHAR
)
"""

_COMMENTS_DDL = """
CREATE TABLE IF NOT EXISTS ts_comments (
    comment_id      VARCHAR PRIMARY KEY,
    task_id         VARCHAR NOT NULL,
    author          VARCHAR,
    text            VARCHAR,
    created_at      VARCHAR
)
"""


# ── Инициализация ────────────────────────────────────────────────────────────

def init_task_tables() -> None:
    """Создаёт все таблицы Пространства задач."""
    conn = _get_conn()
    try:
        conn.execute(_CONTRACTORS_DDL)
        conn.execute(_INITIATIVES_DDL)
        conn.execute(_TASKS_DDL)
        conn.execute(_COMMENTS_DDL)
    finally:
        conn.close()
    log.info("Task Space: таблицы инициализированы")


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _new_id() -> str:
    return str(uuid.uuid4())[:8]


# ── Контрагенты ──────────────────────────────────────────────────────────────

def upsert_contractor(row: dict) -> str:
    """Вставляет или обновляет контрагента. Возвращает contractor_id."""
    conn = _get_conn()
    try:
        cid = row.get("contractor_id") or _new_id()
        now = _now_iso()
        conn.execute("""
            INSERT OR REPLACE INTO ts_contractors
            (contractor_id, category, org_name, duty_phone, work_hours,
             head_name, head_phone, email, channel_type, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            cid,
            row.get("category", ""),
            row.get("org_name", ""),
            row.get("duty_phone", ""),
            row.get("work_hours", ""),
            row.get("head_name", ""),
            row.get("head_phone", ""),
            row.get("email", ""),
            row.get("channel_type", "email_only"),
            now,
        ])
        return cid
    finally:
        conn.close()


def get_contractors() -> list[dict]:
    """Возвращает всех контрагентов."""
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM ts_contractors ORDER BY category, org_name"
        ).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def contractors_count() -> int:
    conn = _get_conn()
    try:
        return conn.execute("SELECT COUNT(*) FROM ts_contractors").fetchone()[0]
    except Exception:
        return 0
    finally:
        conn.close()


# ── Инициативы ───────────────────────────────────────────────────────────────

def create_initiative(data: dict) -> dict:
    """Создаёт инициативу. Возвращает созданный объект."""
    conn = _get_conn()
    try:
        iid = _new_id()
        now = _now_iso()
        conn.execute("""
            INSERT INTO ts_initiatives
            (initiative_id, title, direction, description, status, period,
             created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            iid,
            data.get("title", "Без названия"),
            data.get("direction", ""),
            data.get("description", ""),
            data.get("status", "draft"),
            data.get("period", ""),
            now, now,
        ])
        return {"initiative_id": iid, "title": data.get("title"), "status": "draft"}
    finally:
        conn.close()


def get_initiatives(status: str | None = None) -> list[dict]:
    conn = _get_conn()
    try:
        sql = "SELECT * FROM ts_initiatives"
        params: list = []
        if status:
            sql += " WHERE status = ?"
            params.append(status)
        sql += " ORDER BY created_at DESC"
        rows = conn.execute(sql, params).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def update_initiative(initiative_id: str, data: dict) -> dict | None:
    conn = _get_conn()
    try:
        fields = []
        vals: list = []
        for key in ("title", "direction", "description", "status", "period"):
            if key in data:
                fields.append(f"{key} = ?")
                vals.append(data[key])
        if not fields:
            return None
        fields.append("updated_at = ?")
        vals.append(_now_iso())
        vals.append(initiative_id)
        conn.execute(
            f"UPDATE ts_initiatives SET {', '.join(fields)} WHERE initiative_id = ?",
            vals,
        )
        return {"initiative_id": initiative_id, "updated": True}
    finally:
        conn.close()


def delete_initiative(initiative_id: str) -> bool:
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM ts_initiatives WHERE initiative_id = ?", [initiative_id])
        return True
    finally:
        conn.close()


# ── Задачи ───────────────────────────────────────────────────────────────────

_TASK_STATUSES = ("todo", "in_progress", "review", "done", "cancelled")
_TASK_PRIORITIES = ("P1", "P2", "P3", "P4")

_DIRECTIONS = [
    "Транспорт", "ЖКХ", "Экология", "Безопасность",
    "Цифровизация", "Благоустройство", "Образование",
    "Здравоохранение", "Культура", "Другое",
]


def create_task(data: dict) -> dict:
    """Создаёт задачу. Возвращает созданный объект."""
    conn = _get_conn()
    try:
        tid = _new_id()
        now = _now_iso()
        status = data.get("status", "todo")
        if status not in _TASK_STATUSES:
            status = "todo"
        priority = data.get("priority", "P3")
        if priority not in _TASK_PRIORITIES:
            priority = "P3"
        conn.execute("""
            INSERT INTO ts_tasks
            (task_id, title, description, initiative_id, department,
             contractor_id, assignee, priority, status, due_date,
             parent_task_id, tags, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            tid,
            data.get("title", "Без названия"),
            data.get("description", ""),
            data.get("initiative_id", ""),
            data.get("department", ""),
            data.get("contractor_id", ""),
            data.get("assignee", ""),
            priority, status,
            data.get("due_date", ""),
            data.get("parent_task_id", ""),
            data.get("tags", ""),
            now, now,
        ])
        return {
            "task_id": tid, "title": data.get("title"),
            "status": status, "priority": priority,
        }
    finally:
        conn.close()


def get_tasks(
    status: str | None = None,
    priority: str | None = None,
    initiative_id: str | None = None,
    department: str | None = None,
    contractor_id: str | None = None,
) -> list[dict]:
    """Возвращает задачи с фильтрами."""
    conn = _get_conn()
    try:
        sql = "SELECT * FROM ts_tasks WHERE 1=1"
        params: list = []
        if status:
            sql += " AND status = ?"
            params.append(status)
        if priority:
            sql += " AND priority = ?"
            params.append(priority)
        if initiative_id:
            sql += " AND initiative_id = ?"
            params.append(initiative_id)
        if department:
            sql += " AND department = ?"
            params.append(department)
        if contractor_id:
            sql += " AND contractor_id = ?"
            params.append(contractor_id)
        sql += " ORDER BY CASE priority WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 WHEN 'P3' THEN 3 ELSE 4 END, created_at DESC"
        rows = conn.execute(sql, params).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


def get_task(task_id: str) -> dict | None:
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM ts_tasks WHERE task_id = ?", [task_id]
        ).fetchall()
        if not rows:
            return None
        cols = [d[0] for d in conn.description]
        return dict(zip(cols, rows[0]))
    except Exception:
        return None
    finally:
        conn.close()


def update_task(task_id: str, data: dict) -> dict | None:
    conn = _get_conn()
    try:
        allowed = (
            "title", "description", "initiative_id", "department",
            "contractor_id", "assignee", "priority", "status",
            "due_date", "parent_task_id", "tags",
        )
        fields = []
        vals: list = []
        for key in allowed:
            if key in data:
                val = data[key]
                if key == "status" and val not in _TASK_STATUSES:
                    continue
                if key == "priority" and val not in _TASK_PRIORITIES:
                    continue
                fields.append(f"{key} = ?")
                vals.append(val)
        if not fields:
            return None
        fields.append("updated_at = ?")
        vals.append(_now_iso())
        vals.append(task_id)
        conn.execute(
            f"UPDATE ts_tasks SET {', '.join(fields)} WHERE task_id = ?",
            vals,
        )
        return {"task_id": task_id, "updated": True}
    finally:
        conn.close()


def delete_task(task_id: str) -> bool:
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM ts_comments WHERE task_id = ?", [task_id])
        conn.execute("DELETE FROM ts_tasks WHERE task_id = ?", [task_id])
        return True
    finally:
        conn.close()


# ── Комментарии ──────────────────────────────────────────────────────────────

def add_comment(task_id: str, text: str, author: str = "") -> dict:
    conn = _get_conn()
    try:
        cid = _new_id()
        now = _now_iso()
        conn.execute("""
            INSERT INTO ts_comments (comment_id, task_id, author, text, created_at)
            VALUES (?, ?, ?, ?, ?)
        """, [cid, task_id, author, text, now])
        return {"comment_id": cid, "task_id": task_id}
    finally:
        conn.close()


def get_comments(task_id: str) -> list[dict]:
    conn = _get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM ts_comments WHERE task_id = ? ORDER BY created_at",
            [task_id],
        ).fetchall()
        cols = [d[0] for d in conn.description]
        return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []
    finally:
        conn.close()


# ── Статистика ───────────────────────────────────────────────────────────────

def get_task_stats() -> dict:
    """Агрегированная статистика по задачам."""
    conn = _get_conn()
    try:
        total = conn.execute("SELECT COUNT(*) FROM ts_tasks").fetchone()[0]
        by_status = {}
        for row in conn.execute(
            "SELECT status, COUNT(*) FROM ts_tasks GROUP BY status"
        ).fetchall():
            by_status[row[0]] = row[1]
        by_priority = {}
        for row in conn.execute(
            "SELECT priority, COUNT(*) FROM ts_tasks GROUP BY priority"
        ).fetchall():
            by_priority[row[0]] = row[1]
        overdue = 0
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        try:
            overdue = conn.execute(
                "SELECT COUNT(*) FROM ts_tasks WHERE due_date < ? AND status NOT IN ('done', 'cancelled') AND due_date != ''",
                [today],
            ).fetchone()[0]
        except Exception:
            pass
        return {
            "total": total,
            "by_status": by_status,
            "by_priority": by_priority,
            "overdue": overdue,
            "initiatives": conn.execute("SELECT COUNT(*) FROM ts_initiatives").fetchone()[0],
            "contractors": conn.execute("SELECT COUNT(*) FROM ts_contractors").fetchone()[0],
        }
    except Exception:
        return {"total": 0, "by_status": {}, "by_priority": {}, "overdue": 0, "initiatives": 0, "contractors": 0}
    finally:
        conn.close()


# Экспорт списков для UI
TASK_STATUSES = _TASK_STATUSES
TASK_PRIORITIES = _TASK_PRIORITIES
DIRECTIONS = _DIRECTIONS
