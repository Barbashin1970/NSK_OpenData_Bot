"""Маршрутизация русских запросов к темам датасетов.

Использует взвешенное сопоставление ключевых слов.
Возвращает список (тема, уверенность) в порядке убывания уверенности.
"""

import re
from dataclasses import dataclass, field
from typing import Any

from .registry import load_registry

# Список районов Новосибирска с вариантами написания
DISTRICTS: dict[str, list[str]] = {
    "Дзержинский район": ["дзержинск"],
    "Железнодорожный район": ["железнодорожн"],
    "Заельцовский район": ["заельцовск", "заельцов"],
    "Калининский район": ["калининск"],
    "Кировский район": ["кировск"],
    "Ленинский район": ["ленинск"],
    "Октябрьский район": ["октябрьск"],
    "Первомайский район": ["первомайск"],
    "Советский район": ["советск"],
    "Центральный район": ["централь", "центр"],
}

# Паттерны для извлечения района из запроса
DISTRICT_PATTERNS = [
    r"в\s+([\w\-]+(?:\s+район(?:е|а)?)?)",
    r"([\w\-]+)\s+район(?:е|а)?",
    r"район[еа]?\s+([\w\-]+)",
]


@dataclass
class RouteResult:
    topic: str
    confidence: float
    name: str
    matched_keywords: list[str] = field(default_factory=list)


def _normalize(text: str) -> str:
    """Приводит текст к нижнему регистру, убирает лишнее."""
    return re.sub(r"\s+", " ", text.lower().strip())


def extract_district(query: str) -> str | None:
    """Извлекает название района из запроса."""
    q = _normalize(query)
    for district_name, patterns in DISTRICTS.items():
        for pat in patterns:
            if pat in q:
                return district_name
    return None



# Слова, которые нельзя принять за название улицы
_STREET_STOPWORDS = {
    "районам", "районе", "районах", "районах", "район",
    "типу", "типам", "видам", "видам",
    "количеству", "числу", "мест",
    "новосибирск", "новосибирске", "городе", "города",
    "всего", "каждому", "всем",
}


def extract_street(query: str) -> str | None:
    """Извлекает название улицы из запроса (простая эвристика).

    Использует только явные маркеры улицы (ул., улица, проспект и т.д.).
    """
    q = _normalize(query)
    patterns = [
        r"(?:ул\.?|улиц[еу]?|проспект[еу]?|пр-?т\.?|переулк[еу]?|бульвар[еу]?)\s+([\w\-]+(?:\s+[\w\-]+)?)",
        r"([\w\-]+)\s+(?:улица|проспект|переулок|бульвар|набережная|шоссе)",
    ]
    for pat in patterns:
        m = re.search(pat, q)
        if m:
            candidate = m.group(1).strip()
            if candidate.lower() not in _STREET_STOPWORDS and len(candidate) > 2:
                return candidate
    return None


def extract_limit(query: str) -> int | None:
    """Извлекает числовой лимит (топ-N) из запроса."""
    q = _normalize(query)
    patterns = [
        r"топ[- ]?(\d+)",
        r"(\d+)\s+(?:первых|лучших|объект|запис|результ)",
        r"покажи\s+(\d+)",
        r"первы[хе]?\s+(\d+)",
    ]
    for pat in patterns:
        m = re.search(pat, q)
        if m:
            return int(m.group(1))
    return None


# Ключевые слова для темы отключений электроснабжения
_POWER_KEYWORDS = [
    "электричеств",
    "электроснабжен",
    "свет отключ",
    "отключен свет",
    "нет света",
    "обесточен",
    "отключили свет",
    "отключение электр",
    "отключен электр",
]

# Маркеры для минимального определения запроса об электро-отключениях
_POWER_PRIMARY = ["электр", "свет", "обесточ", "отключен"]
_POWER_CONTEXT = ["отключ", "нет", "план", "история", "сейчас", "сегодня", "неделю"]


def _route_power(q: str) -> "RouteResult | None":
    """Проверяет, относится ли запрос к отключениям электроснабжения."""
    has_power = any(m in q for m in _POWER_PRIMARY)
    has_context = any(m in q for m in _POWER_CONTEXT)
    if not has_power:
        return None

    score = 0.0
    matched: list[str] = []
    for kw in _POWER_KEYWORDS:
        kw_norm = _normalize(kw)
        kw_parts = kw_norm.split()
        all_match = all(
            re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in kw_parts
        )
        if all_match:
            matched.append(kw)
            score += len(kw_parts) ** 1.5

    if score == 0 and has_power:
        score = 1.0
        matched = ["электроснабжение"]

    confidence = min(1.0, score / max(len(_POWER_KEYWORDS), 1) * 5)
    confidence = max(confidence, 0.45 if has_power else 0.0)
    return RouteResult(
        topic="power_outages",
        confidence=confidence,
        name="Отключения электроснабжения",
        matched_keywords=matched,
    )


# ── Дизамбигуация «парки» vs «парковки» ──────────────────────────────────────
# Слова, которые однозначно указывают на парковки (не на зелёные парки).
# Если хотя бы одно из них есть в запросе — parks-результат удаляется.
_PARKING_SIGNALS = re.compile(
    r"парковк|парковоч|стоянк|машиноместо|паркован|припаркова"
)
# Точное слово «парк» (не является префиксом «парковки»).
_PARK_EXACT = re.compile(r"(?<![а-яёa-z])парк(?![а-яё])")


def route(query: str) -> list[RouteResult]:
    """Возвращает список RouteResult, отсортированный по убыванию уверенности."""
    q = _normalize(query)
    registry = load_registry()
    results: list[RouteResult] = []

    # Тема отключений электроснабжения (не в YAML-реестре, обрабатывается отдельно)
    power_result = _route_power(q)
    if power_result:
        results.append(power_result)

    for topic_id, ds in registry.items():
        keywords: list[str] = ds.get("keywords", [])
        matched = []
        score = 0.0

        for kw in keywords:
            kw_norm = _normalize(kw)
            kw_parts = kw_norm.split()

            # Каждый стем проверяется как начало слова в запросе (lookbehind)
            # Это позволяет "спортивн" → матч "спортивные", "площадк" → "площадки"
            all_parts_match = True
            for part in kw_parts:
                pat = r"(?<![а-яёa-z])" + re.escape(part)
                if not re.search(pat, q):
                    all_parts_match = False
                    break

            if not all_parts_match:
                continue

            matched.append(kw)
            # Длинные совпадения ценнее; бонус за многословность
            score += len(kw_parts) ** 1.5

        if score > 0:
            # Нормируем на число ключевых слов, добавляем бонус за длину совпадений
            confidence = min(1.0, score / max(len(keywords), 1) * 3)
            results.append(RouteResult(
                topic=topic_id,
                confidence=confidence,
                name=ds.get("name", topic_id),
                matched_keywords=matched,
            ))

    # ── Дизамбигуация parks vs parking ───────────────────────────────────────
    # Если в запросе есть явные признаки парковки — убираем parks из результатов.
    if _PARKING_SIGNALS.search(q):
        results = [r for r in results if r.topic != "parks"]

    # Если встречается точное слово «парк» (без суффиксов парковки),
    # и parks ещё не найден — добавляем с базовой уверенностью.
    if _PARK_EXACT.search(q) and not _PARKING_SIGNALS.search(q):
        if not any(r.topic == "parks" for r in results):
            ds = registry.get("parks", {})
            results.append(RouteResult(
                topic="parks",
                confidence=0.4,
                name=ds.get("name", "Парки"),
                matched_keywords=["парк"],
            ))

    results.sort(key=lambda r: r.confidence, reverse=True)
    return results


def best_topic(query: str) -> RouteResult | None:
    """Возвращает лучшую тему или None, если ничего не найдено."""
    results = route(query)
    return results[0] if results else None
