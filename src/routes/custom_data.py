"""No-Code аналитический конструктор — загрузка произвольных CSV-отчётов.

Пользователь загружает CSV через Студию → система создаёт отчёт →
доступен через чип на главном экране.

Ограничения:
  - Макс. 5 отчётов
  - Макс. 5 МБ / 10 000 строк на файл
  - Только CSV (UTF-8 или CP-1251)
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from ..constants import DATA_DIR

log = logging.getLogger(__name__)
router = APIRouter(tags=["Аналитические отчёты"])

_CUSTOM_DIR = Path(DATA_DIR) / "custom"
_REGISTRY = _CUSTOM_DIR / "registry.json"
_MAX_REPORTS = 5
_MAX_FILE_SIZE = 5 * 1024 * 1024  # 5 MB
_MAX_ROWS = 10_000


def _load_registry() -> list[dict]:
    if not _REGISTRY.exists():
        return []
    try:
        return json.loads(_REGISTRY.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save_registry(items: list[dict]) -> None:
    _CUSTOM_DIR.mkdir(parents=True, exist_ok=True)
    _REGISTRY.write_text(
        json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8"
    )


@router.get("/custom-reports")
def list_reports() -> dict:
    """Список всех пользовательских отчётов."""
    return {"reports": _load_registry(), "max": _MAX_REPORTS}


@router.post("/custom-reports/upload")
async def upload_report(
    file: UploadFile = File(...),
    name: str = Form(...),
    icon: str = Form("📊"),
    description: str = Form(""),
    city_id: str = Form(""),  # "" = все города
) -> dict:
    """Загрузить CSV и создать отчёт."""
    registry = _load_registry()
    if len(registry) >= _MAX_REPORTS:
        raise HTTPException(400, f"Максимум {_MAX_REPORTS} отчётов. Удалите старый перед добавлением.")

    name = name.strip()[:80]
    if not name:
        raise HTTPException(400, "Название отчёта обязательно")

    # Генерируем slug
    import re
    slug = re.sub(r"[^a-zA-Z0-9а-яёА-ЯЁ]", "_", name.lower())[:40]
    slug = re.sub(r"_+", "_", slug).strip("_")
    if not slug:
        slug = f"report_{len(registry) + 1}"

    # Проверяем дубль slug
    if any(r["slug"] == slug for r in registry):
        slug = f"{slug}_{len(registry) + 1}"

    # Читаем файл
    content = await file.read()
    if len(content) > _MAX_FILE_SIZE:
        raise HTTPException(400, f"Файл слишком большой (макс. {_MAX_FILE_SIZE // 1024 // 1024} МБ)")

    # Определяем кодировку
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError:
        try:
            text = content.decode("cp1251")
        except UnicodeDecodeError:
            raise HTTPException(400, "Не удалось определить кодировку файла. Используйте UTF-8.")

    # Парсим CSV
    lines = text.strip().split("\n")
    if len(lines) < 2:
        raise HTTPException(400, "CSV должен содержать заголовок и хотя бы 1 строку данных")

    # Определяем разделитель
    sniffer = csv.Sniffer()
    try:
        dialect = sniffer.sniff(lines[0], delimiters=",;\t|")
    except csv.Error:
        dialect = csv.excel
        dialect.delimiter = ";"

    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    rows: list[dict[str, Any]] = []
    columns = reader.fieldnames or []
    if not columns:
        raise HTTPException(400, "Не удалось определить колонки CSV")

    for i, row in enumerate(reader):
        if i >= _MAX_ROWS:
            break
        # Приводим числа
        clean = {}
        for k, v in row.items():
            if v is None:
                clean[k] = ""
                continue
            v = v.strip()
            try:
                clean[k] = int(v) if v.isdigit() else float(v.replace(",", "."))
            except (ValueError, AttributeError):
                clean[k] = v
        rows.append(clean)

    if not rows:
        raise HTTPException(400, "CSV не содержит данных")

    # Сохраняем CSV как JSON (для быстрого чтения DuckDB не нужен)
    _CUSTOM_DIR.mkdir(parents=True, exist_ok=True)
    data_path = _CUSTOM_DIR / f"{slug}.json"
    data_path.write_text(
        json.dumps({"columns": columns, "rows": rows}, ensure_ascii=False),
        encoding="utf-8",
    )

    # Определяем типы колонок
    col_types = {}
    for col in columns:
        sample = [r.get(col) for r in rows[:20] if r.get(col) not in (None, "")]
        if all(isinstance(v, (int, float)) for v in sample) and sample:
            col_types[col] = "number"
        elif col.lower() in ("_lat", "_lon", "lat", "lon", "latitude", "longitude"):
            col_types[col] = "coord"
        elif any(k in col.lower() for k in ("район", "district", "округ")):
            col_types[col] = "district"
        else:
            col_types[col] = "text"

    # Добавляем в реестр
    entry = {
        "slug": slug,
        "name": name,
        "icon": icon.strip()[:4] or "📊",
        "description": description.strip()[:200],
        "city_id": city_id.strip(),  # "" = все города
        "columns": columns,
        "col_types": col_types,
        "row_count": len(rows),
        "file": str(data_path.relative_to(Path(DATA_DIR).parent)),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    registry.append(entry)
    _save_registry(registry)

    log.info("custom report '%s': %d rows, %d columns", name, len(rows), len(columns))
    return {"ok": True, "slug": slug, "rows": len(rows), "columns": len(columns)}


@router.get("/custom-reports/{slug}")
def get_report(slug: str) -> dict:
    """Получить данные отчёта."""
    registry = _load_registry()
    entry = next((r for r in registry if r["slug"] == slug), None)
    if not entry:
        raise HTTPException(404, f"Отчёт '{slug}' не найден")

    data_path = Path(DATA_DIR).parent / entry["file"]
    if not data_path.exists():
        raise HTTPException(404, "Файл данных не найден")

    data = json.loads(data_path.read_text(encoding="utf-8"))
    return {
        "meta": entry,
        "columns": data["columns"],
        "rows": data["rows"],
    }


@router.delete("/custom-reports/{slug}")
def delete_report(slug: str) -> dict:
    """Удалить отчёт."""
    registry = _load_registry()
    entry = next((r for r in registry if r["slug"] == slug), None)
    if not entry:
        raise HTTPException(404, f"Отчёт '{slug}' не найден")

    # Удаляем файл данных
    data_path = Path(DATA_DIR).parent / entry["file"]
    if data_path.exists():
        data_path.unlink()

    registry = [r for r in registry if r["slug"] != slug]
    _save_registry(registry)
    return {"ok": True}
