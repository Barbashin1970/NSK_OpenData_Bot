"""Backup / Restore API — экспорт и импорт данных Task Space.

Endpoints:
  GET  /api/backup/export     — скачать ZIP-архив с JSON-дампами таблиц
  POST /api/backup/import     — загрузить ZIP-архив и восстановить данные
  POST /api/backup/snapshot   — сохранить снимок на сервере (data/backups/)
  GET  /api/backup/snapshots  — список снимков на сервере
  GET  /api/backup/download/{name} — скачать конкретный снимок
  DELETE /api/backup/snapshot/{name} — удалить снимок
  GET  /api/version/check     — проверка наличия новой версии
"""

import io
import json
import logging
import zipfile
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, File, Query, UploadFile
from fastapi.responses import JSONResponse, StreamingResponse

from ..cache import _get_conn

log = logging.getLogger(__name__)

router = APIRouter()

_BACKUPS_DIR = Path(__file__).parent.parent.parent / "data" / "backups"

# Таблицы Task Space для экспорта
_TS_TABLES = [
    "ts_tasks",
    "ts_initiatives",
    "ts_comments",
    "ts_contractors",
    "ts_users",
]


def _table_exists(conn, table_name: str) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [table_name],
    ).fetchall()
    return len(rows) > 0


def _json_default(obj):
    """Сериализация нестандартных типов DuckDB (datetime, Decimal и т.д.)."""
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    if isinstance(obj, (bytes, bytearray)):
        return obj.hex()
    try:
        return str(obj)
    except Exception:
        return None


def _dump_table(conn, table_name: str) -> list[dict]:
    """Дамп таблицы в список словарей."""
    if not _table_exists(conn, table_name):
        return []
    cols = [
        row[0]
        for row in conn.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}' ORDER BY ordinal_position"
        ).fetchall()
    ]
    rows = conn.execute(f"SELECT * FROM {table_name}").fetchall()
    return [dict(zip(cols, row)) for row in rows]


def _create_backup_zip(scope: str = "tasks") -> io.BytesIO:
    """Создаёт ZIP-архив с JSON-дампами."""
    conn = _get_conn()
    errors = []
    try:
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            manifest = {
                "version": "1.0",
                "scope": scope,
                "created_at": datetime.now().isoformat(),
                "tables": {},
                "errors": [],
            }

            tables = list(_TS_TABLES)
            if scope == "all":
                topic_tables = conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name LIKE 'topic_%' ORDER BY table_name"
                ).fetchall()
                tables += [r[0] for r in topic_tables]
                # Также экологические и другие служебные таблицы
                extra = conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_name NOT LIKE 'topic_%' "
                    "AND table_name NOT LIKE 'ts_%' "
                    "AND table_name NOT LIKE 'information_%' "
                    "ORDER BY table_name"
                ).fetchall()
                tables += [r[0] for r in extra if r[0] not in tables]

            for tbl in tables:
                try:
                    data = _dump_table(conn, tbl)
                    if data:
                        fname = f"{tbl}.json"
                        zf.writestr(
                            fname,
                            json.dumps(data, ensure_ascii=False, default=_json_default),
                        )
                        manifest["tables"][tbl] = len(data)
                except Exception as e:
                    err_msg = f"{tbl}: {e}"
                    manifest["errors"].append(err_msg)
                    log.warning("backup export skip %s: %s", tbl, e)

            zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False, indent=2))

        buf.seek(0)
        return buf
    finally:
        conn.close()


# Глобальный флаг — фоновые задачи проверяют его перед записью
import_in_progress = False


def _restore_from_zip(zip_bytes: bytes) -> dict:
    """Восстанавливает данные из ZIP-архива в единой транзакции."""
    global import_in_progress
    import_in_progress = True
    log.info("backup import: START, фоновые записи приостановлены")

    conn = _get_conn()
    result = {"restored": {}, "errors": [], "zip_size": len(zip_bytes)}
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes), "r") as zf:
            names = zf.namelist()
            result["files_in_zip"] = len(names)
            log.info("backup import: ZIP %d bytes, %d files: %s", len(zip_bytes), len(names), names[:10])

            # Единая транзакция — DELETE+INSERT атомарны
            conn.execute("BEGIN TRANSACTION")
            try:
                for name in names:
                    if name == "manifest.json" or not name.endswith(".json"):
                        continue

                    table_name = name.replace(".json", "")

                    # Безопасность: запрещаем системные таблицы
                    if table_name.startswith("information_") or table_name.startswith("pg_"):
                        result["errors"].append(f"Пропущена таблица {table_name}: системная")
                        continue

                    try:
                        data = json.loads(zf.read(name))
                        if not data or not isinstance(data, list):
                            continue

                        cols = list(data[0].keys())
                        quoted_cols = [f'"{c}"' for c in cols]
                        col_list = ", ".join(quoted_cols)
                        placeholders = ", ".join(["?"] * len(cols))

                        # Создаём таблицу если не существует
                        if not _table_exists(conn, table_name):
                            col_defs = ", ".join(f'"{c}" VARCHAR' for c in cols)
                            conn.execute(f"CREATE TABLE {table_name} ({col_defs})")

                        # Очищаем и заливаем (внутри транзакции, batch)
                        conn.execute(f"DELETE FROM {table_name}")
                        all_vals = [[row.get(c) for c in cols] for row in data]
                        conn.executemany(
                            f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})",
                            all_vals,
                        )

                        result["restored"][table_name] = len(data)
                        log.info("backup import: %s — %d строк", table_name, len(data))

                    except Exception as e:
                        result["errors"].append(f"{table_name}: {e}")
                        log.warning("backup import error %s: %s", table_name, e)

                conn.execute("COMMIT")
                log.info("backup import: COMMIT OK, %d таблиц восстановлено", len(result["restored"]))
            except Exception as e:
                conn.execute("ROLLBACK")
                result["errors"].append(f"Транзакция откачена: {e}")
                log.error("backup import: ROLLBACK — %s", e)

    finally:
        conn.close()
        import_in_progress = False
        log.info("backup import: DONE, фоновые записи возобновлены")

    return result


# ── Endpoints ────────────────────────────────────────────────────────────────


@router.get(
    "/api/backup/export",
    tags=["Backup"],
    summary="Скачать архив данных",
)
def backup_export(
    scope: str = Query("tasks", description="tasks = только Task Space, all = + кеши opendata"),
):
    """Экспорт данных в ZIP-архив с JSON-файлами."""
    buf = _create_backup_zip(scope)
    ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    filename = f"sigma-backup-{scope}-{ts}.zip"
    return StreamingResponse(
        buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post(
    "/api/backup/import",
    tags=["Backup"],
    summary="Восстановить данные из архива",
)
async def backup_import(file: UploadFile = File(...)):
    """Загрузить ZIP-архив и восстановить данные."""
    if not file.filename.endswith(".zip"):
        return JSONResponse(status_code=400, content={"error": "Нужен ZIP-файл"})

    content = await file.read()
    result = _restore_from_zip(content)
    return result


@router.post(
    "/api/backup/snapshot",
    tags=["Backup"],
    summary="Сохранить снимок на сервере",
)
def backup_snapshot(
    scope: str = Query("tasks", description="tasks | all"),
    name: str | None = Query(None, description="Имя снимка (опционально)"),
):
    """Создаёт ZIP-снимок в data/backups/."""
    _BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y-%m-%d-%H%M")
    fname = f"{name or 'sigma-backup'}-{scope}-{ts}.zip"
    path = _BACKUPS_DIR / fname

    buf = _create_backup_zip(scope)
    path.write_bytes(buf.read())

    size_kb = path.stat().st_size / 1024
    log.info("backup snapshot: %s (%.1f KB)", fname, size_kb)
    return {"name": fname, "size_kb": round(size_kb, 1), "path": str(path)}


@router.get(
    "/api/backup/snapshots",
    tags=["Backup"],
    summary="Список снимков на сервере",
)
def backup_snapshots():
    """Список ZIP-файлов в data/backups/."""
    if not _BACKUPS_DIR.exists():
        return {"snapshots": []}

    result = []
    for f in sorted(_BACKUPS_DIR.glob("*.zip"), reverse=True):
        stat = f.stat()
        result.append({
            "name": f.name,
            "size_kb": round(stat.st_size / 1024, 1),
            "created": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        })
    return {"snapshots": result}


@router.get(
    "/api/backup/download/{name}",
    tags=["Backup"],
    summary="Скачать конкретный снимок",
)
def backup_download(name: str):
    """Скачать ранее сохранённый снимок."""
    path = _BACKUPS_DIR / name
    if not path.exists() or not path.suffix == ".zip":
        return JSONResponse(status_code=404, content={"error": "Снимок не найден"})

    return StreamingResponse(
        open(path, "rb"),
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{name}"'},
    )


@router.delete(
    "/api/backup/snapshot/{name}",
    tags=["Backup"],
    summary="Удалить снимок",
)
def backup_delete(name: str):
    """Удалить конкретный снимок."""
    path = _BACKUPS_DIR / name
    if not path.exists():
        return JSONResponse(status_code=404, content={"error": "Снимок не найден"})

    path.unlink()
    log.info("backup delete: %s", name)
    return {"deleted": name}


# ── Version check ────────────────────────────────────────────────────────────

_GITHUB_REPO = "Barbashin1970/NSK_OpenData_Bot"
_VERSION_CACHE: dict = {}


@router.get(
    "/api/version/check",
    tags=["Backup"],
    summary="Проверить наличие новой версии",
)
def version_check():
    """Сравнивает локальную версию с последней на GitHub."""
    import importlib.metadata
    import urllib.request

    try:
        local_version = importlib.metadata.version("nsk-opendata-bot")
    except Exception:
        local_version = "unknown"

    # Cache: check GitHub max once per hour
    now = datetime.now()
    if _VERSION_CACHE.get("checked_at") and (now - _VERSION_CACHE["checked_at"]).seconds < 3600:
        return {
            "local": local_version,
            "remote": _VERSION_CACHE.get("remote", "unknown"),
            "update_available": _VERSION_CACHE.get("update_available", False),
            "cached": True,
        }

    remote_version = "unknown"
    update_available = False
    try:
        url = f"https://raw.githubusercontent.com/{_GITHUB_REPO}/main/pyproject.toml"
        req = urllib.request.Request(url, headers={"User-Agent": "SIGMA-Bot"})
        with urllib.request.urlopen(req, timeout=5) as resp:
            text = resp.read().decode()
            for line in text.splitlines():
                if line.startswith("version"):
                    remote_version = line.split('"')[1]
                    break
        update_available = remote_version != "unknown" and remote_version != local_version
    except Exception as e:
        log.debug("version check failed: %s", e)

    _VERSION_CACHE["remote"] = remote_version
    _VERSION_CACHE["update_available"] = update_available
    _VERSION_CACHE["checked_at"] = now

    return {
        "local": local_version,
        "remote": remote_version,
        "update_available": update_available,
        "cached": False,
    }
