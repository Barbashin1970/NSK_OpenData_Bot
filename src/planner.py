"""Планировщик операций: разбирает русский запрос на тип + параметры.

Типы операций:
  COUNT   — "сколько ...", "количество ..."
  TOP_N   — "топ-5 ...", "первые N ..."
  GROUP   — "по районам", "по типам", "где больше всего"
  FILTER  — "покажи ...", "найди ...", "список ..."
  INFO    — "какие есть ...", "что ты умеешь"
"""

import re
from dataclasses import dataclass, field
from typing import Any

from .router import (
    extract_district, extract_limit, extract_street, extract_sub_district,
    _detect_utility, UTILITY_FILTER_MAP,
)

# Паттерны для распознавания типа операции (pre-compiled)
COUNT_PATTERNS = re.compile(
    r"\bсколько\b|\bколичество\b|\bчисло\b|\bкол-?во\b|\bвсего\b.*\bесть\b|\bесть ли\b"
)
TOP_N_PATTERNS = re.compile(
    r"\bтоп[- ]\d+|\bтоп\b|\bпервы[хе]?\s+\d+|\bнаибольших\b|\bнаиболее\b"
    r"|\bсамых?\b|\bбольше всего\b|\bмаксимальн\b"
)
GROUP_PATTERNS = re.compile(
    r"\bпо районам\b|\bпо каждому\b|\bпо типу\b|\bпо видам?\b"
    r"|\bраспределен\b|\bгде больше всего\b|\bв каких районах\b|\bпо округам\b"
)
FILTER_PATTERNS = re.compile(
    r"\bпокажи\b|\bнайди\b|\bсписок\b|\bвсе\b|\bкакие\b.*\bесть\b"
    r"|\bперечисли\b|\bвыведи\b|\bвсех\b"
)
INFO_PATTERNS = re.compile(
    r"\bчто ты умеешь\b|\bкакие темы\b|\bчем можешь помочь\b|\bтемы\b|\bвозможности\b"
)
DISTRICTS_PATTERNS = re.compile(
    r"\bрайон[ыаов]*\b|\bсписок районов\b|\bкакие районы\b|\bгородск\w* район"
)

# --- Паттерны для запросов об отключениях (тема power_outages) ---
AUDIENCE_CHILD_PATTERNS = re.compile(
    r"\bдетск|\bдля детей\b|\bдетям\b|\bюношеск"
)
AUDIENCE_ADULT_PATTERNS = re.compile(
    r"\bвзросл|\bдля взрослых\b"
)

POWER_STATUS_PATTERNS = re.compile(
    r"\bсейчас\b|\bпрямо сейчас\b|\bв данный момент\b|\bтекущ\w*\b|\bактивн\w*\b"
)
POWER_TODAY_PATTERNS = re.compile(
    r"\bсегодня\b|\bсегодняшн\w*\b"
)
POWER_HISTORY_PATTERNS = re.compile(
    r"\bза неделю\b|\bпрошл\w*\b|\bбыл\w* отключ\w*\b|\bистори\w*\b"
    r"|\bархив\w*\b|\bза последн\w+\b|\bднях?\b"
)
POWER_PLANNED_PATTERNS = re.compile(
    r"\bплан\w*\b|\bпредстоящ\w*\b|\bбудущ\w*\b|\bближайш\w*\b"
    r"|\bна неделю\b|\bнеделю вперёд\b|\bзапланирован\w*\b|\bнеделю\b|\b7 дней\b"
)

# --- Паттерны для запросов об экологии и метеорологии ---
ECO_PDK_PATTERNS = re.compile(
    r"\bпдк\b|\bпревышен\w*|\bопас\w+|\bвредн\w*"
)
ECO_HISTORY_PATTERNS = re.compile(
    r"\bза\s+\d+\s+дн|\bнеделю\b|\bнедел\w+\b|\bдинамик\b|\bистори\w*\b"
    r"|\bтренд\b|\bпрошл\w+\b"
)
ECO_RISKS_PATTERNS = re.compile(
    r"\bриск\w*|\bиндекс\w*|\bгололед\w*|\bгололедиц\w*|\bчёрн\w*\s+лед\w*"
    r"|\bчерн\w*\s+лед\w*|\bнму\b|\bчёрн\w*\s+неб\w*|\bчерн\w*\s+неб\w*"
    r"|\bрекоменд\w*|\bпрескрипт\w*|\bавтомобил\w*\s+индекс\w*"
    r"|\bтемператур\w*\s+шок\w*|\bшок\w*\s+холод\w*|\bопасност\w*\s+воздух\w*"
)

# Паттерны для числовых значений в контексте «больше X», «от X», «за X год»
YEAR_PATTERN = re.compile(r"\bза\s+(20\d{2})\b|\b(20\d{2})\s*год[а-я]*\b")
MIN_VALUE_PATTERN = re.compile(r"больше\s+(\d+)|от\s+(\d+)|минимум\s+(\d+)")


@dataclass
class Plan:
    operation: str          # COUNT | TOP_N | GROUP | FILTER | INFO
                            # | POWER_STATUS | POWER_TODAY | POWER_HISTORY | POWER_PLANNED
                            # | ECO_STATUS | ECO_PDK | ECO_HISTORY | ECO_RISKS
    topic: str | None       # выбранная тема
    district: str | None    # фильтр по району (канонический, для SQL)
    street: str | None      # фильтр по улице
    limit: int              # лимит строк (для TOP_N и FILTER)
    year: str | None        # фильтр по году
    min_value: int | None   # минимальное значение
    sub_district: str | None = None  # отображаемый подрайон («Академгородок», «Шлюз», ...)
    extra_filters: dict[str, str] = field(default_factory=dict)


def make_plan(query: str, topic: str | None) -> Plan:
    """Разбирает запрос и возвращает Plan."""
    q = query.lower()

    # Для темы экологии — специальные операции
    if topic == "ecology":
        if ECO_PDK_PATTERNS.search(q):
            operation = "ECO_PDK"
        elif ECO_HISTORY_PATTERNS.search(q):
            operation = "ECO_HISTORY"
        elif ECO_RISKS_PATTERNS.search(q):
            operation = "ECO_RISKS"
        else:
            operation = "ECO_STATUS"
    # Для темы отключений — специальные операции
    elif topic == "power_outages":
        if POWER_HISTORY_PATTERNS.search(q):
            operation = "POWER_HISTORY"
        elif POWER_PLANNED_PATTERNS.search(q):
            operation = "POWER_PLANNED"
        elif POWER_TODAY_PATTERNS.search(q):
            operation = "POWER_TODAY"
        else:
            operation = "POWER_STATUS"
    # Тип операции
    elif INFO_PATTERNS.search(q):
        operation = "INFO"
    elif GROUP_PATTERNS.search(q):
        operation = "GROUP"
    elif COUNT_PATTERNS.search(q):
        operation = "COUNT"
    elif TOP_N_PATTERNS.search(q):
        operation = "TOP_N"
    else:
        operation = "FILTER"

    # Параметры
    district = extract_district(query)
    sub = extract_sub_district(query)
    sub_district = sub[1] if sub else None
    street = extract_street(query)
    limit = extract_limit(query) or (10 if operation == "TOP_N" else 20)

    year_match = YEAR_PATTERN.search(q)
    year = (year_match.group(1) or year_match.group(2)) if year_match else None

    min_match = MIN_VALUE_PATTERN.search(q)
    min_value = None
    if min_match:
        for g in min_match.groups():
            if g:
                min_value = int(g)
                break

    extra_filters: dict[str, str] = {}
    if AUDIENCE_CHILD_PATTERNS.search(q):
        extra_filters["audience"] = "children"
    elif AUDIENCE_ADULT_PATTERNS.search(q):
        extra_filters["audience"] = "adults"

    # Для темы отключений ЖКХ — определяем тип ресурса
    if topic == "power_outages":
        utility_key = _detect_utility(q)
        extra_filters["utility"] = UTILITY_FILTER_MAP.get(utility_key, "")

    return Plan(
        operation=operation,
        topic=topic,
        district=district,
        street=street,
        limit=limit,
        year=year,
        min_value=min_value,
        sub_district=sub_district,
        extra_filters=extra_filters,
    )
