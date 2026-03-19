"""ЦИИ НГУ (AI Center) endpoints + news editor."""

import logging
from pathlib import Path

from fastapi import APIRouter, Query, UploadFile, File, Form, Header, HTTPException
from fastapi.responses import HTMLResponse, FileResponse

log = logging.getLogger(__name__)

router = APIRouter()

_STATIC = Path(__file__).parent.parent / "static"


@router.get("/ciinsu", tags=["ЦИИ НГУ"])
def get_ciinsu(section: str = Query("center", description="center | projects | team | publications | news | contacts")) -> dict:
    """Данные о Центре искусственного интеллекта НГУ из data/ciinsu/knowledge_base.json."""
    from ..ciinsu import get_section
    return get_section(section)


@router.post("/ciinsu/login", tags=["ЦИИ НГУ"])
def ciinsu_login(password: str = Form(...)) -> dict:
    """Авторизация редактора новостей. Возвращает токен при верном пароле."""
    from ..ciinsu import login as _login
    token = _login(password)
    if not token:
        raise HTTPException(status_code=401, detail="Неверный пароль")
    return {"token": token}


@router.get("/ciinsu/news", tags=["ЦИИ НГУ"])
def list_news() -> list:
    """Публичный список новостей ЦИИ НГУ (сначала новые)."""
    from ..ciinsu import get_news
    return get_news()


@router.post("/ciinsu/news", tags=["ЦИИ НГУ"])
async def create_news_post(
    title: str = Form(...),
    body: str = Form(...),
    date: str = Form(""),
    photo: UploadFile = File(None),
    x_admin_token: str = Header(None, alias="x-admin-token"),
) -> dict:
    """Создать новый пост (требует заголовок X-Admin-Token)."""
    from ..ciinsu import verify_token, create_news as _create
    if not verify_token(x_admin_token or ""):
        raise HTTPException(status_code=401, detail="Требуется авторизация")
    photo_filename = ""
    if photo and photo.filename:
        photos_dir = Path(__file__).parent.parent.parent / "data" / "ciinsu" / "photos"
        photos_dir.mkdir(parents=True, exist_ok=True)
        import uuid as _uuid
        ext = Path(photo.filename).suffix.lower()
        photo_filename = _uuid.uuid4().hex[:12] + ext
        content = await photo.read()
        (photos_dir / photo_filename).write_bytes(content)
    return _create(title=title, body=body, photo=photo_filename, date=date)


@router.put("/ciinsu/news/{post_id}", tags=["ЦИИ НГУ"])
async def update_news_post(
    post_id: str,
    title: str = Form(None),
    body: str = Form(None),
    date: str = Form(None),
    photo: UploadFile = File(None),
    x_admin_token: str = Header(None, alias="x-admin-token"),
) -> dict:
    """Обновить пост (требует заголовок X-Admin-Token)."""
    from ..ciinsu import verify_token, update_news as _update
    if not verify_token(x_admin_token or ""):
        raise HTTPException(status_code=401, detail="Требуется авторизация")
    photo_filename: str | None = None
    if photo and photo.filename:
        photos_dir = Path(__file__).parent.parent.parent / "data" / "ciinsu" / "photos"
        photos_dir.mkdir(parents=True, exist_ok=True)
        import uuid as _uuid
        ext = Path(photo.filename).suffix.lower()
        photo_filename = _uuid.uuid4().hex[:12] + ext
        content = await photo.read()
        (photos_dir / photo_filename).write_bytes(content)
    result = _update(post_id, title=title, body=body, photo=photo_filename, date=date)
    if result is None:
        raise HTTPException(status_code=404, detail="Пост не найден")
    return result


@router.delete("/ciinsu/news/{post_id}", tags=["ЦИИ НГУ"])
def delete_news_post(
    post_id: str,
    x_admin_token: str = Header(None, alias="x-admin-token"),
) -> dict:
    """Удалить пост (требует заголовок X-Admin-Token)."""
    from ..ciinsu import verify_token, delete_news as _delete
    if not verify_token(x_admin_token or ""):
        raise HTTPException(status_code=401, detail="Требуется авторизация")
    if not _delete(post_id):
        raise HTTPException(status_code=404, detail="Пост не найден")
    return {"deleted": post_id}


@router.get("/ciinsu/photo/{filename}", tags=["ЦИИ НГУ"], include_in_schema=False)
def get_photo(filename: str):
    """Отдаёт загруженное фото."""
    import re as _re
    if not _re.match(r'^[\w\-\.]+$', filename):
        raise HTTPException(status_code=400, detail="Недопустимое имя файла")
    path = Path(__file__).parent.parent.parent / "data" / "ciinsu" / "photos" / filename
    if not path.exists():
        raise HTTPException(status_code=404, detail="Файл не найден")
    return FileResponse(str(path))


@router.get("/news-editor", include_in_schema=False)
def news_editor_page():
    """Страница редактора новостей ЦИИ НГУ."""
    html_file = _STATIC / "news-editor.html"
    if not html_file.exists():
        return HTMLResponse("<h1>news-editor.html not found</h1>")
    content = html_file.read_text(encoding="utf-8")
    return HTMLResponse(content, headers={"Cache-Control": "no-store"})
