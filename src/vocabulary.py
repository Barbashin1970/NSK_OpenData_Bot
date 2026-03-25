"""Пользовательский словарь синонимов.

Загружает config/vocabulary.yaml и добавляет термины
в keyword-списки router.py (overlay поверх базового словаря).
Поддерживает hot-reload через API Студии.
"""

import logging
import re
from datetime import date
from pathlib import Path
from typing import Any

import yaml

log = logging.getLogger(__name__)

_VOCAB_PATH = Path(__file__).parent.parent / "config" / "vocabulary.yaml"

# Кэш загруженных терминов
_terms: list[dict[str, str]] = []

# Все темы, к которым можно привязать синоним
ALL_TOPICS: dict[str, str] = {
    # YAML-темы (datasets.yaml)
    "parking": "Парковки",
    "stops": "Остановки",
    "culture": "Организации культуры",
    "schools": "Школы",
    "kindergartens": "Детские сады",
    "libraries": "Библиотеки",
    "parks": "Парки",
    "sport_grounds": "Спортивные площадки",
    "pharmacies": "Аптеки",
    "sport_orgs": "Спортивные организации",
    # Спецтемы (hardcoded в router.py)
    "power_outages": "Отключения ЖКХ",
    "ecology": "Экология и метеорология",
    "construction": "Строительство",
    "cameras": "Камеры фиксации",
    "heat_sources": "Тепловые источники",
    "emissions": "Выбросы в атмосферу",
    "metro": "Метро",
    "airport": "Аэропорт",
    "medical": "Медицинские учреждения",
    "traffic_index": "Индекс пробок",
    "transit": "Маршруты транспорта",
    # ЦИИ НГУ (фронтенд-темы, обработка в _trySpecialQuery)
    "ciinsu_center": "ЦИИ НГУ — о центре",
    "ciinsu_projects": "ЦИИ НГУ — проекты",
    "ciinsu_news": "ЦИИ НГУ — новости",
    "ciinsu_team": "ЦИИ НГУ — команда",
    "ciinsu_publications": "ЦИИ НГУ — публикации",
    "ciinsu_contacts": "ЦИИ НГУ — контакты",
}


def load_vocabulary() -> list[dict[str, str]]:
    """Загрузить словарь из YAML. Возвращает список терминов."""
    global _terms
    try:
        if not _VOCAB_PATH.exists():
            _terms = []
            return _terms
        with open(_VOCAB_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        _terms = data.get("terms", []) or []
        log.info("Vocabulary loaded: %d terms", len(_terms))
    except Exception as e:
        log.warning("Failed to load vocabulary: %s", e)
        _terms = []
    return _terms


def save_vocabulary(terms: list[dict[str, str]]) -> None:
    """Сохранить словарь в YAML."""
    global _terms
    _terms = terms
    data = {"terms": terms}
    _VOCAB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_VOCAB_PATH, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)
    log.info("Vocabulary saved: %d terms", len(terms))


def get_terms() -> list[dict[str, str]]:
    """Текущий список терминов (из кэша)."""
    return _terms


def add_term(term: str, topic: str, added_by: str = "оператор") -> dict[str, str]:
    """Добавить термин в словарь и сохранить."""
    entry = {
        "term": term.strip().lower(),
        "topic": topic,
        "added_by": added_by,
        "added_at": str(date.today()),
    }
    # Не добавлять дубликат
    for t in _terms:
        if t["term"] == entry["term"] and t["topic"] == entry["topic"]:
            return t
    _terms.append(entry)
    save_vocabulary(_terms)
    return entry


def remove_term(term: str, topic: str | None = None) -> bool:
    """Удалить термин из словаря. Возвращает True если найден и удалён."""
    global _terms
    before = len(_terms)
    if topic:
        _terms = [t for t in _terms if not (t["term"] == term and t["topic"] == topic)]
    else:
        _terms = [t for t in _terms if t["term"] != term]
    if len(_terms) < before:
        save_vocabulary(_terms)
        return True
    return False


def get_extra_keywords(topic: str) -> list[str]:
    """Получить дополнительные ключевые слова для темы из словаря."""
    return [t["term"] for t in _terms if t.get("topic") == topic]


def patch_registry(registry: dict[str, Any]) -> int:
    """Добавить vocabulary-термины в keywords YAML-тем.

    Возвращает количество добавленных терминов.
    """
    count = 0
    for t in _terms:
        topic = t.get("topic", "")
        term = t.get("term", "")
        if not topic or not term:
            continue
        ds = registry.get(topic)
        if not ds:
            continue
        keywords = ds.get("keywords", [])
        if term not in keywords:
            keywords.append(term)
            ds["keywords"] = keywords
            count += 1
    return count
