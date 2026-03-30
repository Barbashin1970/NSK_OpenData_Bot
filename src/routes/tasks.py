"""Task Space API — Пространство задач.

Эндпоинты для управления инициативами, задачами и контрагентами.
MVP v1: без авторизации, все операции открыты.
"""

import base64
import logging
from pathlib import Path

from fastapi import APIRouter, Query, Request, HTTPException, UploadFile, File, Form
from fastapi.responses import HTMLResponse

from ..task_store import (
    init_task_tables,
    create_initiative, get_initiatives, update_initiative, delete_initiative,
    create_task, get_tasks, get_task, update_task, delete_task,
    add_comment, get_comments,
    get_contractors, get_contractor, create_contractor, update_contractor,
    get_contractor_categories,
    get_task_stats,
    TASK_STATUSES, TASK_PRIORITIES, DIRECTIONS,
)

log = logging.getLogger(__name__)

router = APIRouter()

_STATIC = Path(__file__).parent.parent / "static"


# ── Страница UI ──────────────────────────────────────────────────────────────

@router.get("/tasks", include_in_schema=False)
def tasks_page():
    """Отдаёт HTML-страницу Пространства задач."""
    html_file = _STATIC / "tasks.html"
    if not html_file.exists():
        return HTMLResponse("<h1>tasks.html not found</h1>")
    return HTMLResponse(
        html_file.read_text(encoding="utf-8"),
        headers={"Cache-Control": "no-store"},
    )


# ── Справочные данные ────────────────────────────────────────────────────────

@router.get(
    "/api/tasks/meta",
    tags=["Пространство задач"],
    summary="Справочники: статусы, приоритеты, направления",
)
def api_tasks_meta():
    """Возвращает справочные данные для построения форм."""
    return {
        "statuses": list(TASK_STATUSES),
        "priorities": list(TASK_PRIORITIES),
        "directions": DIRECTIONS,
        "status_labels": {
            "todo": "К выполнению",
            "in_progress": "В работе",
            "review": "На проверке",
            "done": "Готово",
            "cancelled": "Отменена",
        },
        "priority_labels": {
            "P1": "P1 — Критично",
            "P2": "P2 — Высокий",
            "P3": "P3 — Средний",
            "P4": "P4 — Низкий",
        },
        "acceptance_criteria_options": [
            "Акт выполненных работ подписан",
            "Фотофиксация до/после выполнена",
            "Проверка на месте комиссией проведена",
            "Отчёт подрядчика принят",
            "Обращения жителей прекратились",
            "Контрольный замер показателей в норме",
            "Объект введён в эксплуатацию",
        ],
    }


# ── Статистика ───────────────────────────────────────────────────────────────

@router.get(
    "/api/tasks/stats",
    tags=["Пространство задач"],
    summary="Статистика задач",
)
def api_tasks_stats():
    return get_task_stats()


# ── Контрагенты ──────────────────────────────────────────────────────────────

@router.get(
    "/api/contractors",
    tags=["Пространство задач"],
    summary="Справочник контрагентов (аварийные службы, МУПы)",
)
def api_contractors(category: str | None = Query(None)):
    return get_contractors(with_task_count=True, category=category or None)


@router.get(
    "/api/contractors/categories",
    tags=["Пространство задач"],
    summary="Уникальные категории контрагентов",
)
def api_contractor_categories():
    return get_contractor_categories()


@router.post(
    "/api/contractors",
    tags=["Пространство задач"],
    summary="Создать контрагента",
)
async def api_contractors_create(request: Request):
    data = await request.json()
    if not data.get("org_name"):
        raise HTTPException(400, "Поле org_name обязательно")
    return create_contractor(data)


@router.get(
    "/api/contractors/{contractor_id}",
    tags=["Пространство задач"],
    summary="Получить контрагента по ID",
)
def api_contractor_get(contractor_id: str):
    c = get_contractor(contractor_id)
    if not c:
        raise HTTPException(404, "Контрагент не найден")
    return c


@router.put(
    "/api/contractors/{contractor_id}",
    tags=["Пространство задач"],
    summary="Обновить контрагента",
)
async def api_contractor_update(contractor_id: str, request: Request):
    data = await request.json()
    result = update_contractor(contractor_id, data)
    if not result:
        raise HTTPException(400, "Нет полей для обновления")
    return result


# ── Инициативы ───────────────────────────────────────────────────────────────

@router.get(
    "/api/initiatives",
    tags=["Пространство задач"],
    summary="Список инициатив",
)
def api_initiatives_list(status: str | None = Query(None)):
    return get_initiatives(status)


@router.post(
    "/api/initiatives",
    tags=["Пространство задач"],
    summary="Создать инициативу",
)
async def api_initiatives_create(request: Request):
    data = await request.json()
    if not data.get("title"):
        raise HTTPException(400, "Поле title обязательно")
    return create_initiative(data)


@router.put(
    "/api/initiatives/{initiative_id}",
    tags=["Пространство задач"],
    summary="Обновить инициативу",
)
async def api_initiatives_update(initiative_id: str, request: Request):
    data = await request.json()
    result = update_initiative(initiative_id, data)
    if not result:
        raise HTTPException(400, "Нет полей для обновления")
    return result


@router.delete(
    "/api/initiatives/{initiative_id}",
    tags=["Пространство задач"],
    summary="Удалить инициативу",
)
def api_initiatives_delete(initiative_id: str):
    delete_initiative(initiative_id)
    return {"deleted": True}


# ── Задачи ───────────────────────────────────────────────────────────────────

@router.get(
    "/api/tasks",
    tags=["Пространство задач"],
    summary="Список задач с фильтрами",
)
def api_tasks_list(
    status: str | None = Query(None),
    priority: str | None = Query(None),
    initiative_id: str | None = Query(None),
    department: str | None = Query(None),
    contractor_id: str | None = Query(None),
):
    return get_tasks(status, priority, initiative_id, department, contractor_id)


@router.get(
    "/api/tasks/{task_id}",
    tags=["Пространство задач"],
    summary="Получить задачу по ID",
)
def api_tasks_get(task_id: str):
    task = get_task(task_id)
    if not task:
        raise HTTPException(404, "Задача не найдена")
    task["comments"] = get_comments(task_id)
    return task


@router.post(
    "/api/tasks",
    tags=["Пространство задач"],
    summary="Создать задачу",
)
async def api_tasks_create(request: Request):
    data = await request.json()
    if not data.get("title"):
        raise HTTPException(400, "Поле title обязательно")
    return create_task(data)


@router.put(
    "/api/tasks/{task_id}",
    tags=["Пространство задач"],
    summary="Обновить задачу",
)
async def api_tasks_update(task_id: str, request: Request):
    data = await request.json()
    result = update_task(task_id, data)
    if not result:
        raise HTTPException(400, "Нет полей для обновления")
    return result


@router.delete(
    "/api/tasks/{task_id}",
    tags=["Пространство задач"],
    summary="Удалить задачу",
)
def api_tasks_delete(task_id: str):
    delete_task(task_id)
    return {"deleted": True}


# ── Комментарии ──────────────────────────────────────────────────────────────

@router.post(
    "/api/tasks/{task_id}/comments",
    tags=["Пространство задач"],
    summary="Добавить комментарий к задаче",
)
async def api_task_comment(task_id: str, request: Request):
    data = await request.json()
    text = data.get("text", "").strip()
    if not text:
        raise HTTPException(400, "Комментарий не может быть пустым")
    return add_comment(task_id, text, data.get("author", ""))


# ── Быстрая смена статуса (drag-and-drop канбан) ─────────────────────────────

@router.patch(
    "/api/tasks/{task_id}/status",
    tags=["Пространство задач"],
    summary="Быстрая смена статуса задачи",
)
async def api_task_status(task_id: str, request: Request):
    data = await request.json()
    new_status = data.get("status")
    if new_status not in TASK_STATUSES:
        raise HTTPException(400, f"Недопустимый статус: {new_status}")
    result = update_task(task_id, {"status": new_status})
    if not result:
        raise HTTPException(404, "Задача не найдена")
    return result


# ── Импорт строительных компаний ─────────────────────────────────────────

@router.post(
    "/api/contractors/seed-construction",
    tags=["Пространство задач"],
    summary="Импорт строительных компаний из opendata",
)
def api_seed_construction():
    from ..contractors_loader import seed_construction_contractors
    count = seed_construction_contractors()
    return {"imported": count}


# ── Email ─────────────────────────────────────────────────────────────────

@router.get(
    "/api/email/config",
    tags=["Пространство задач"],
    summary="Получить SMTP-конфиг (без пароля)",
)
def api_email_config_get():
    from ..email_sender import get_smtp_config
    return get_smtp_config()


@router.post(
    "/api/email/config",
    tags=["Пространство задач"],
    summary="Сохранить SMTP-конфиг",
)
async def api_email_config_save(request: Request):
    data = await request.json()
    from ..email_sender import save_smtp_config
    return save_smtp_config(data)


@router.post(
    "/api/email/send",
    tags=["Пространство задач"],
    summary="Отправить email с вложениями",
)
async def api_email_send(request: Request):
    """Принимает multipart/form-data: to, subject, body, cc, files[]."""
    import asyncio
    from ..email_sender import send_email

    content_type = request.headers.get("content-type", "")

    if "multipart/form-data" in content_type:
        form = await request.form()
        to = form.get("to", "")
        subject = form.get("subject", "")
        body = form.get("body", "")
        cc = form.get("cc", "")

        attachments = []
        for key in form:
            item = form[key]
            if hasattr(item, "read"):  # UploadFile
                content = await item.read()
                attachments.append((item.filename, content))

        if not to:
            raise HTTPException(400, "Поле to обязательно")
        # smtplib блокирующий — запускаем в thread pool чтобы не блокировать event loop
        return await asyncio.to_thread(
            send_email, to, subject, body, attachments or None, cc
        )
    else:
        data = await request.json()
        if not data.get("to"):
            raise HTTPException(400, "Поле to обязательно")
        return await asyncio.to_thread(
            send_email, data["to"], data.get("subject", ""), data.get("body", ""),
            None, data.get("cc", ""),
        )
