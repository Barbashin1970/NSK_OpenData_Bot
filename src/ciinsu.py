"""Модуль данных о Центре ИИ НГУ.

Статические данные  — data/ciinsu/knowledge_base.json (центр, проекты, команда, публикации).
Динамические новости — data/ciinsu/news.json (CRUD через API).
Аутентификация       — data/ciinsu/config.json (пароль: sigma2024 при первом запуске).
"""

import hashlib
import json
import secrets
import uuid
from datetime import datetime
from pathlib import Path

_DATA_DIR   = Path(__file__).parent.parent / "data" / "ciinsu"
_KB_PATH    = Path(__file__).parent / "ciinsu_knowledge_base.json"
_NEWS_PATH  = _DATA_DIR / "news.json"
_CONFIG_PATH = _DATA_DIR / "config.json"

_kb: dict | None = None


# ── Статическая база знаний ──────────────────────────────────────────────────

def _load() -> dict:
    global _kb
    if _kb is None:
        _kb = json.loads(_KB_PATH.read_text(encoding="utf-8"))
    return _kb


def get_section(section: str) -> dict:
    """Возвращает данные по секции: center, projects, team, publications, news, contacts."""
    if section == "news":
        return {"section": "news", "data": get_news()}

    kb = _load()

    if section == "contacts":
        c = kb["center"]
        return {
            "section": "contacts",
            "data": {
                "address": c["address"],
                "email": c["email"],
                "phone": c["phone"],
                "website": c["website"],
                "nsu_page": c["nsu_page"],
                "parent_orgs": c["parent_orgs"],
            },
        }

    if section == "center":
        c = kb["center"]
        return {
            "section": "center",
            "data": {
                "name": c["full_name"],
                "founded": c["founded"],
                "stats": c["stats"],
                "mission": c["mission"],
                "directions": c["directions"],
                "advantages": c["advantages"],
                "geography": c["geography"],
            },
        }

    if section in ("projects", "team", "publications"):
        return {"section": section, "data": kb[section]}

    if section == "all":
        return {
            "section": "all",
            "center": kb["center"],
            "projects": kb["projects"],
            "team": kb["team"],
            "publications": kb["publications"],
            "news": get_news(),
        }

    return {"section": section, "data": [], "error": f"Неизвестная секция: {section}"}


# ── Аутентификация ───────────────────────────────────────────────────────────

def _load_config() -> dict:
    """Загружает (или создаёт) конфиг с токеном. Дефолтный пароль: sigma2024."""
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    if not _CONFIG_PATH.exists():
        secret = secrets.token_hex(16)
        default_pwd = "sigma2024"
        valid_token = hashlib.sha256(f"{default_pwd}{secret}".encode()).hexdigest()
        cfg: dict = {"token_secret": secret, "valid_token": valid_token}
        _CONFIG_PATH.write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
        return cfg
    return json.loads(_CONFIG_PATH.read_text(encoding="utf-8"))


def login(password: str) -> str | None:
    """Проверяет пароль. Возвращает токен при успехе, None при неверном пароле."""
    cfg = _load_config()
    candidate = hashlib.sha256(f"{password}{cfg['token_secret']}".encode()).hexdigest()
    return candidate if candidate == cfg.get("valid_token", "") else None


def verify_token(token: str) -> bool:
    """Проверяет токен администратора."""
    return bool(token) and token == _load_config().get("valid_token", "")


# ── Динамические новости ─────────────────────────────────────────────────────

def _load_news() -> list[dict]:
    if not _NEWS_PATH.exists():
        return []
    try:
        data = json.loads(_NEWS_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _save_news(news: list[dict]) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _NEWS_PATH.write_text(json.dumps(news, indent=2, ensure_ascii=False), encoding="utf-8")


def get_news() -> list[dict]:
    """Возвращает список новостей (сначала новые)."""
    return _load_news()


def create_news(title: str, body: str, photo: str = "", date: str = "",
                format: str = "txt") -> dict:
    """Создаёт новый пост. Возвращает созданный объект.

    Args:
        format: 'txt' (plain text) или 'md' (Markdown).
    """
    news = _load_news()
    post: dict = {
        "id": uuid.uuid4().hex[:8],
        "title": title.strip(),
        "date": date.strip() or datetime.now().strftime("%Y-%m-%d"),
        "body": body.strip(),
        "photo": photo,
        "format": format if format in ("txt", "md") else "txt",
        "created_at": datetime.now().isoformat(),
    }
    news.insert(0, post)
    _save_news(news)
    return post


def update_news(post_id: str, title: str | None = None, body: str | None = None,
                photo: str | None = None, date: str | None = None,
                format: str | None = None) -> dict | None:
    """Обновляет пост. Возвращает обновлённый объект или None если не найден."""
    news = _load_news()
    for post in news:
        if post["id"] == post_id:
            if title is not None:
                post["title"] = title.strip()
            if body is not None:
                post["body"] = body.strip()
            if photo is not None:
                post["photo"] = photo
            if date is not None:
                post["date"] = date.strip()
            if format is not None and format in ("txt", "md"):
                post["format"] = format
            post["updated_at"] = datetime.now().isoformat()
            _save_news(news)
            return post
    return None


def delete_news(post_id: str) -> bool:
    """Удаляет пост по id. Возвращает True при успехе."""
    news = _load_news()
    filtered = [p for p in news if p["id"] != post_id]
    if len(filtered) == len(news):
        return False
    _save_news(filtered)
    return True


def search(query: str) -> list[dict]:
    """Текстовый поиск по всей базе знаний ЦИИ."""
    kb = _load()
    q = query.lower()
    results: list[dict] = []

    for p in kb["projects"]:
        haystack = f"{p['name']} {p['short']} {p['description']}".lower()
        if any(word in haystack for word in q.split()):
            results.append({"section": "project", "title": p["name"], "text": p["short"], "url": p.get("url", "")})

    for pub in kb["publications"]:
        haystack = f"{pub['title']} {pub['authors']} {pub['journal']}".lower()
        if any(word in haystack for word in q.split()):
            results.append({"section": "publication", "title": pub["title"], "text": f"{pub['authors']} · {pub['journal']}", "url": ""})

    for n in get_news():
        haystack = f"{n['title']} {n['body']}".lower()
        if any(word in haystack for word in q.split()):
            results.append({"section": "news", "title": n["title"], "text": n["body"][:120], "url": ""})

    return results
