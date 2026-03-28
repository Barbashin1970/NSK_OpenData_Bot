"""Сервис обратной связи — Жалобы и предложения.

Хранит сообщения в JSON-файле на диске.
Голосовой ввод через Web Speech API (клиентская сторона).
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..constants import DATA_DIR

log = logging.getLogger(__name__)
router = APIRouter(tags=["Обратная связь"])

_FEEDBACK_FILE = Path(DATA_DIR) / "feedback.json"


class FeedbackIn(BaseModel):
    text: str
    category: str = "suggestion"  # bug | suggestion | question
    source: str = "web"           # web | voice


def _load() -> list[dict]:
    if not _FEEDBACK_FILE.exists():
        return []
    try:
        return json.loads(_FEEDBACK_FILE.read_text(encoding="utf-8"))
    except Exception:
        return []


def _save(items: list[dict]) -> None:
    _FEEDBACK_FILE.parent.mkdir(parents=True, exist_ok=True)
    _FEEDBACK_FILE.write_text(
        json.dumps(items, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


@router.post("/feedback")
def submit_feedback(body: FeedbackIn) -> dict:
    """Отправить отзыв/баг/предложение."""
    text = body.text.strip()
    if not text or len(text) < 5:
        raise HTTPException(400, "Сообщение слишком короткое (мин. 5 символов)")
    if len(text) > 2000:
        raise HTTPException(400, "Сообщение слишком длинное (макс. 2000 символов)")

    items = _load()
    entry: dict[str, Any] = {
        "id": len(items) + 1,
        "text": text,
        "category": body.category,
        "source": body.source,
        "status": "new",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    items.append(entry)
    _save(items)
    log.info("feedback #%d: %s [%s]", entry["id"], body.category, body.source)
    return {"ok": True, "id": entry["id"]}


@router.get("/feedback")
def list_feedback() -> dict:
    """Список всех отзывов (для Студии)."""
    items = _load()
    return {
        "total": len(items),
        "items": list(reversed(items)),  # новые сверху
    }


@router.patch("/feedback/{item_id}")
def update_feedback(item_id: int, status: str = "resolved") -> dict:
    """Изменить статус (new → in_progress → resolved → rejected)."""
    allowed = {"new", "in_progress", "resolved", "rejected"}
    if status not in allowed:
        raise HTTPException(400, f"Статус должен быть одним из: {allowed}")
    items = _load()
    for item in items:
        if item["id"] == item_id:
            item["status"] = status
            _save(items)
            return {"ok": True}
    raise HTTPException(404, f"Отзыв #{item_id} не найден")
