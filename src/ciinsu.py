"""Модуль данных о Центре ИИ НГУ.

Читает data/ciinsu/knowledge_base.json и предоставляет
структурированные ответы по секциям.
"""

import json
from pathlib import Path

_KB_PATH = Path(__file__).parent.parent / "data" / "ciinsu" / "knowledge_base.json"
_kb: dict | None = None


def _load() -> dict:
    global _kb
    if _kb is None:
        _kb = json.loads(_KB_PATH.read_text(encoding="utf-8"))
    return _kb


def get_section(section: str) -> dict:
    """Возвращает данные по секции: center, projects, team, publications, news, contacts."""
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

    if section in ("projects", "team", "publications", "news"):
        return {"section": section, "data": kb[section]}

    # Поиск по всем секциям
    if section == "all":
        return {
            "section": "all",
            "center": kb["center"],
            "projects": kb["projects"],
            "team": kb["team"],
            "publications": kb["publications"],
            "news": kb["news"],
        }

    return {"section": section, "data": [], "error": f"Неизвестная секция: {section}"}


def search(query: str) -> list[dict]:
    """Простой текстовый поиск по всей базе знаний ЦИИ.

    Возвращает список совпадений с полями section, title, text.
    """
    kb = _load()
    q = query.lower()
    results: list[dict] = []

    # Поиск в проектах
    for p in kb["projects"]:
        haystack = f"{p['name']} {p['short']} {p['description']}".lower()
        if any(word in haystack for word in q.split()):
            results.append({
                "section": "project",
                "title": p["name"],
                "text": p["short"],
                "url": p.get("url", ""),
            })

    # Поиск в публикациях
    for pub in kb["publications"]:
        haystack = f"{pub['title']} {pub['authors']} {pub['journal']}".lower()
        if any(word in haystack for word in q.split()):
            results.append({
                "section": "publication",
                "title": pub["title"],
                "text": f"{pub['authors']} · {pub['journal']}",
                "url": "",
            })

    # Поиск в новостях
    for n in kb["news"]:
        haystack = f"{n['title']} {n['summary']}".lower()
        if any(word in haystack for word in q.split()):
            results.append({
                "section": "news",
                "title": n["title"],
                "text": n["summary"][:120],
                "url": "",
            })

    return results
