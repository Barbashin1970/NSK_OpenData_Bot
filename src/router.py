"""Маршрутизация русских запросов к темам датасетов.

Использует взвешенное сопоставление ключевых слов.
Возвращает список (тема, уверенность) в порядке убывания уверенности.
"""

import re
from dataclasses import dataclass, field
from typing import Any

from .registry import load_registry

# Список районов Новосибирска с вариантами написания.
# Кольцово — наукоград (городской округ в Новосибирском р-не НСО),
# включён как отдельная локация: есть точка мониторинга погоды/воздуха.
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
    "Кольцово": ["кольцов"],
}

# Подрайоны/микрорайоны → (родительский район, отображаемое название)
# Формат: (compiled_pattern, canonical_district, display_name)
# Порядок важен: более специфичные паттерны — первыми
_SUB_DISTRICTS: list[tuple[re.Pattern, str, str]] = [
    # ── Советский район ─────────────────────────────────────────────────────
    # Академгородок: полное название (стем «академгород» покрывает все падежи:
    # -ок, -ка, -ке, -ком — именит., родит., предл., творит.)
    (re.compile(r"академгород"),                                         "Советский район", "Академгородок"),
    # Academ (латиница — на вывесках, сайтах): «в Academ», «Academ парк»
    (re.compile(r"(?<![а-яёa-z])academ(?![a-z])", re.IGNORECASE),      "Советский район", "Академгородок"),
    # Разговорное «Академe» — все падежные формы + омоглифы (кирилл./лат.):
    # именит./предл.: «Академe», «в Академe» (Cyrillic е или Latin e)
    # родительный:   «Академa», «школы Академa» (Cyrillic а или Latin a)
    # дательный:     «к Академy» (Cyrillic у или Latin y)
    # Lookahead/lookbehind блокирует «академгородок», «академем» и т.п.
    (re.compile(r"(?<![а-яёa-z])акад[еe]м[еeаaуy]?(?![а-яёa-z])"),    "Советский район", "Академгородок"),
    # Шлюз: «в Шлюзе», «на Шлюзе», «шлюзовой»
    (re.compile(r"(?<![а-яё])шлюз"),                     "Советский район", "Шлюз"),
    # Верхняя зона Академгородка
    (re.compile(r"верхн\w*\s+зон"),                      "Советский район", "Верхняя зона"),
    # Микрорайон «Щ»: «мкр. Щ», «микрорайон Щ», «в Щ», просто «Щ»
    (re.compile(r"мкр[\s.]*щ\b|микрорайон\s*[«\"']?щ"), "Советский район", 'мкр. "Щ"'),
    (re.compile(r"(?<![а-яёa-z])щ(?![а-яёa-z])"),       "Советский район", 'мкр. "Щ"'),
]

# Публичный словарь подрайонов для документации и рендера
# {display_name: (parent_district, [примеры написания])}
SUB_DISTRICTS_INFO: dict[str, tuple[str, list[str]]] = {
    "Академгородок": ("Советский район", ["академгородок", "Academ", "Академe", "в Академе", "Академa", "к Академy"]),
    "Шлюз":          ("Советский район", ["шлюз", "на Шлюзе"]),
    "Верхняя зона":  ("Советский район", ["верхняя зона", "в Верхней зоне"]),
    'мкр. "Щ"':      ("Советский район", ["мкр. Щ", "микрорайон Щ", "в Щ"]),
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
    utility_type: str = ""   # для power_outages: electricity|heat|hot_water|cold_water|gas|all


def _normalize(text: str) -> str:
    """Приводит текст к нижнему регистру, убирает лишнее."""
    return re.sub(r"\s+", " ", text.lower().strip())


def extract_sub_district(query: str) -> tuple[str, str] | None:
    """Распознаёт подрайон/микрорайон в запросе.

    Возвращает (canonical_district, display_name) или None.
    Например: «в Академгородке» → («Советский район», «Академгородок»)
    """
    q = _normalize(query)
    for pattern, parent_district, display_name in _SUB_DISTRICTS:
        if pattern.search(q):
            return parent_district, display_name
    return None


def extract_district(query: str) -> str | None:
    """Извлекает название района из запроса (включая подрайоны → родительский район)."""
    # Сначала проверяем подрайоны/микрорайоны
    sub = extract_sub_district(query)
    if sub:
        return sub[0]  # возвращаем родительский район для фильтрации
    # Затем стандартные районы
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
    "кольцово", "кольцов",
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


# ── Экология и метеорология ──────────────────────────────────────────────────
_ECOLOGY_KEYWORDS = [
    "качество воздуха",
    "загрязнен",
    "pm2",
    "pm10",
    "aqi",
    "экологи",
    "пыль",
    "смог",
    "дышать",
    "дышится",
    "гарь",
    "no2",
    "диоксид азота",
    "частиц",
    "атмосфер",
]

_WEATHER_KEYWORDS = [
    "погод",
    "температур",
    "ветер",
    "ветра",
    "давлен",
    "влажност",
    "метеор",
    "прогноз",
]

_ECOLOGY_PRIMARY = ["воздух", "загрязн", "pm2", "pm10", "aqi", "смог", "экологи", "пыль", "дыш", "гарь", "no2", "частиц"]
_WEATHER_PRIMARY = ["погод", "температур", "ветер", "ветра", "ветру", "ветром", "давлен", "влажност", "метеор"]
# Риски / прескриптивная аналитика — тоже относятся к теме экологии
_RISKS_PRIMARY   = ["риск", "гололед", "нму", "чёрн", "черн", "ловушк", "шок", "индекс водит", "индекс прогул"]


def _route_ecology(q: str) -> "RouteResult | None":
    """Проверяет, относится ли запрос к экологии или метеорологии."""
    has_eco     = any(m in q for m in _ECOLOGY_PRIMARY)
    has_weather = any(m in q for m in _WEATHER_PRIMARY)
    has_risks   = any(m in q for m in _RISKS_PRIMARY)
    if not has_eco and not has_weather and not has_risks:
        return None

    score = 0.0
    matched: list[str] = []
    all_kw = _ECOLOGY_KEYWORDS + _WEATHER_KEYWORDS
    for kw in all_kw:
        kw_norm = _normalize(kw)
        kw_parts = kw_norm.split()
        all_match = all(
            re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in kw_parts
        )
        if all_match:
            matched.append(kw)
            score += len(kw_parts) ** 1.5

    if score == 0:
        score = 1.0
        matched = ["экология" if has_eco else ("погода" if has_weather else "риски")]

    all_count = len(all_kw)
    confidence = min(1.0, score / max(all_count, 1) * 8)
    confidence = max(confidence, 0.45 if (has_eco or has_risks) else 0.40)
    return RouteResult(
        topic="ecology",
        confidence=confidence,
        name="Экология и метеорология",
        matched_keywords=matched,
    )


# ── Ключевые слова отключений по типу ресурса ────────────────────────────────

# Электроснабжение
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
_POWER_PRIMARY = ["электр", "свет", "обесточ"]

# Теплоснабжение
_HEAT_KEYWORDS = [
    "теплоснабжен",
    "отопление",
    "нет тепла",
    "тепло отключ",
    "отключили тепло",
    "батарей",
    "батарея холодн",
    "котельн",
    "отключение тепл",
]
_HEAT_PRIMARY = ["отоплен", "теплоснабж", "котельн", "батаре"]

# Горячее водоснабжение
_HOT_WATER_KEYWORDS = [
    "горячая вода",
    "горяч вод",
    "нет горяч",
    "горячее водоснабжен",
    "отключили горяч",
    "горяч отключ",
]
_HOT_WATER_PRIMARY = ["горяч"]

# Холодное водоснабжение / водоснабжение в целом
_COLD_WATER_KEYWORDS = [
    "холодная вода",
    "холодн вод",
    "нет холодн",
    "нет воды",
    "воды нет",
    "водоснабжен",
    "отключили воду",
    "отключение воды",
]
_COLD_WATER_PRIMARY = ["холодн вод", "нет вод", "вод отключ", "водоснабж"]

# Газоснабжение
_GAS_KEYWORDS = [
    "газоснабжен",
    "газ отключ",
    "нет газа",
    "отключили газ",
    "отключение газ",
]
_GAS_PRIMARY = ["газоснабж", "газ отключ", "нет газ"]

# Общий контекст «отключение ЖКХ» без явного типа ресурса
_UTILITY_OUTAGE_PRIMARY = ["отключ", "коммунальн", "жкх", "аварий", "авари"]

# Соответствие ключевых слов → utility_filter для DuckDB ILIKE
UTILITY_FILTER_MAP = {
    "electricity": "электроснабж",
    "heat":        "теплоснабж",
    "hot_water":   "горяч",
    "cold_water":  "холодн",
    "gas":         "газоснабж",
    "all":         "",   # пустой = все типы
}

_POWER_CONTEXT = ["отключ", "нет", "план", "история", "сейчас", "сегодня", "неделю"]


def _detect_utility(q: str) -> str:
    """Определяет тип ресурса в запросе об отключениях.

    Возвращает один из ключей UTILITY_FILTER_MAP.
    """
    # Важен порядок: горячая вода ПЕРЕД холодной и общим «вода»
    if any(m in q for m in _HEAT_PRIMARY):
        return "heat"
    if any(m in q for m in _HOT_WATER_PRIMARY):
        return "hot_water"
    if any(
        re.search(r"(?<![а-яёa-z])" + re.escape(m), q)
        for m in _COLD_WATER_PRIMARY
    ):
        return "cold_water"
    if any(re.search(r"(?<![а-яёa-z])" + re.escape(m), q) for m in _GAS_PRIMARY):
        return "gas"
    if any(m in q for m in _POWER_PRIMARY) or "обесточ" in q:
        return "electricity"
    return "all"


def _route_power(q: str) -> "RouteResult | None":
    """Проверяет, относится ли запрос к отключениям ЖКХ любого типа."""
    # Проверяем любой из типов ресурсов
    has_electricity = any(m in q for m in _POWER_PRIMARY) or "обесточ" in q
    has_heat        = any(m in q for m in _HEAT_PRIMARY)
    has_hot_water   = any(m in q for m in _HOT_WATER_PRIMARY)
    has_cold_water  = any(
        re.search(r"(?<![а-яёa-z])" + re.escape(m), q) for m in _COLD_WATER_PRIMARY
    )
    has_gas         = any(
        re.search(r"(?<![а-яёa-z])" + re.escape(m), q) for m in _GAS_PRIMARY
    )
    # «отключения ЖКХ» без уточнения типа
    has_utility     = any(m in q for m in _UTILITY_OUTAGE_PRIMARY)

    has_any = has_electricity or has_heat or has_hot_water or has_cold_water or has_gas
    has_context = any(m in q for m in _POWER_CONTEXT)

    if not has_any and not (has_utility and has_context):
        return None

    # Выбираем ключевые слова для подбора уверенности
    if has_heat:
        kw_list, label = _HEAT_KEYWORDS, "теплоснабжение"
    elif has_hot_water:
        kw_list, label = _HOT_WATER_KEYWORDS, "горячее водоснабжение"
    elif has_cold_water:
        kw_list, label = _COLD_WATER_KEYWORDS, "холодное водоснабжение"
    elif has_gas:
        kw_list, label = _GAS_KEYWORDS, "газоснабжение"
    else:
        kw_list, label = _POWER_KEYWORDS, "электроснабжение"

    score = 0.0
    matched: list[str] = []
    for kw in kw_list:
        kw_norm = _normalize(kw)
        kw_parts = kw_norm.split()
        all_match = all(
            re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in kw_parts
        )
        if all_match:
            matched.append(kw)
            score += len(kw_parts) ** 1.5

    if score == 0:
        score = 1.0
        matched = [label]

    confidence = min(1.0, score / max(len(kw_list), 1) * 5)
    confidence = max(confidence, 0.45)

    # Определяем тип утилиты для передачи в planner
    utility_type = _detect_utility(q)
    return RouteResult(
        topic="power_outages",
        confidence=confidence,
        name=f"Отключения ЖКХ ({label})",
        matched_keywords=matched,
        utility_type=utility_type,
    )


# ── Камеры фиксации нарушений ПДД ────────────────────────────────────────────
_CAMERAS_KEYWORDS = [
    "камер",
    "видеофиксац",
    "видеокамер",
    "радар",
    "стационарн камер",
    "гибдд камер",
    "фиксац скорост",
    "превышен скорост",
    "камер наблюден",
    "дорожн камер",
]
_CAMERAS_PRIMARY = ["камер", "видеофиксац", "радар", "видеокамер", "дорожн камер"]


def _route_cameras(q: str) -> "RouteResult | None":
    """Проверяет, относится ли запрос к камерам фиксации нарушений."""
    if not any(m in q for m in _CAMERAS_PRIMARY):
        return None

    score = 0.0
    matched: list[str] = []
    for kw in _CAMERAS_KEYWORDS:
        kw_norm = _normalize(kw)
        kw_parts = kw_norm.split()
        all_match = all(
            re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in kw_parts
        )
        if all_match:
            matched.append(kw)
            score += len(kw_parts) ** 1.5

    if score == 0:
        score = 1.0
        matched = ["камеры"]

    confidence = min(1.0, score / max(len(_CAMERAS_KEYWORDS), 1) * 6)
    confidence = max(confidence, 0.55)
    return RouteResult(
        topic="cameras",
        confidence=confidence,
        name="Камеры фиксации нарушений ПДД",
        matched_keywords=matched,
    )


# ── Тепловые источники (ТЭЦ, котельные СГК и УЭВ) ────────────────────────────
_HEAT_SOURCES_PRIMARY = ["тэц", "тепловые источник", "тепловые станц", "тепловая станц", "сгк", "уэв"]
_HEAT_SOURCES_KEYWORDS = [
    "тэц",
    "тепловые источник",
    "тепловые станц",
    "тепловая станц",
    "тепловая электроцентраль",
    "сгк",
    "уэв",
    "теплоэнерг",
    "источник тепла",
    "тепловая мощност",
    "котельная сгк",
    "газовая котельная",
    "тепловые объект",
    "источник теплоснабжен",
]


def _route_heat_sources(q: str) -> "RouteResult | None":
    """Маршрутизация запросов к справочнику тепловых источников."""
    if not any(m in q for m in _HEAT_SOURCES_PRIMARY):
        return None

    score = sum(
        len(kw.split()) ** 1.5
        for kw in _HEAT_SOURCES_KEYWORDS
        if all(re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in _normalize(kw).split())
    )
    if score == 0:
        score = 1.0

    confidence = min(1.0, max(score / 8, 0.6))
    return RouteResult(
        topic="heat_sources",
        confidence=confidence,
        name="Тепловые источники НСО",
        matched_keywords=[kw for kw in _HEAT_SOURCES_KEYWORDS if _normalize(kw) in q],
    )


# ── Выбросы в атмосферу (2-ТП Воздух) ───────────────────────────────────────
_EMISSIONS_PRIMARY = ["выброс", "загрязнен воздух", "атмосфер загрязн", "2-тп", "двутп"]
_EMISSIONS_KEYWORDS = [
    "выброс",
    "загрязнение воздух",
    "атмосферный выброс",
    "качество воздух",
    "загрязнен атмосфер",
    "вредные выброс",
    "2-тп воздух",
    "экология воздух",
    "so2",
    "диоксид серы",
    "твердые частиц",
    "пыл выброс",
]


def _route_emissions(q: str) -> "RouteResult | None":
    """Маршрутизация запросов к данным выбросов 2-ТП Воздух."""
    if not any(m in q for m in _EMISSIONS_PRIMARY):
        return None

    score = sum(
        len(kw.split()) ** 1.5
        for kw in _EMISSIONS_KEYWORDS
        if all(re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in _normalize(kw).split())
    )
    if score == 0:
        score = 1.0

    confidence = min(1.0, max(score / 8, 0.65))
    return RouteResult(
        topic="emissions",
        confidence=confidence,
        name="Выбросы в атмосферу НСО (2-ТП Воздух 2024)",
        matched_keywords=[kw for kw in _EMISSIONS_KEYWORDS if _normalize(kw) in q],
    )


# ── Индекс пробок / дорожная нагрузка ────────────────────────────────────────
_TRAFFIC_PRIMARY = ["пробк", "трафик", "загруженност", "час пик", "дорог сейчас", "индекс пробок"]


def _route_traffic(q: str) -> "RouteResult | None":
    """Проверяет, является ли запрос запросом об индексе дорожной нагрузки."""
    if not any(m in q for m in _TRAFFIC_PRIMARY):
        return None
    return RouteResult(
        topic="traffic_index",
        confidence=0.90,
        name="Индекс дорожной нагрузки",
        matched_keywords=["пробки"],
    )


# ── Маршруты общественного транспорта ────────────────────────────────────────
_TRANSIT_PRIMARY = ["проехат", "маршрут", "добраться", "доехат", "попасть"]


def extract_transit_districts(query: str) -> tuple[str | None, str | None]:
    """Извлекает районы «откуда» и «куда» из транзитного запроса.

    Returns: (from_district, to_district) — канонические названия или None.
    """
    q = _normalize(query)

    def match_district(text: str) -> str | None:
        for pattern, parent_district, _ in _SUB_DISTRICTS:
            if pattern.search(text):
                return parent_district
        for district_name, patterns in DISTRICTS.items():
            for pat in patterns:
                if pat in text:
                    return district_name
        return None

    m_from = re.search(r"из\s+([\w\s\-]+?)(?=\s+(?:в|до)\s|\s*$)", q)
    m_to   = re.search(r"(?:^|\s)в\s+([\w\s\-]+?)(?=\s+из\s|\s*$)", q)
    m_do   = re.search(r"\bдо\s+([\w\s\-]+?)(?=\s+из\s|\s*$)", q)

    from_d = match_district(m_from.group(1).strip()) if m_from else None
    to_text = (m_to.group(1).strip() if m_to else None) or (m_do.group(1).strip() if m_do else None)
    to_d = match_district(to_text) if to_text else None

    return from_d, to_d


def _route_transit(q: str) -> "RouteResult | None":
    """Проверяет, является ли запрос маршрутным (транспорт между районами)."""
    if not any(m in q for m in _TRANSIT_PRIMARY):
        return None
    from_d, to_d = extract_transit_districts(q)
    if not from_d and not to_d:
        return None
    return RouteResult(
        topic="transit",
        confidence=0.85,
        name="Маршрут общественного транспорта",
        matched_keywords=["маршрут", "проехать"],
    )


# ── Строительство ─────────────────────────────────────────────────────────────
_CONSTRUCTION_KEYWORDS = [
    "стройк",
    "строек",
    "строительств",
    "застройщик",
    "разрешени на строительств",
    "активн стройк",
    "строящ",
    "жилой дом строит",
    "новостройк",
    "незавершен строительств",
    "ввод в эксплуатацию",
    "введен в эксплуатацию",
    "сдан в эксплуатацию",
    "разрешени на ввод",
    "стройплощадк",
    "возводит",
    "возводимый",
]
_CONSTRUCTION_PRIMARY = ["стройк", "строек", "строительств", "застройщик", "новостройк", "стройплощадк", "ввод в эксплуатацию"]


# ── Метро ─────────────────────────────────────────────────────────────────────
_METRO_KEYWORDS = [
    "метро",
    "метрополитен",
    "станция метро",
    "станции метро",
    "подземк",
    "линия метро",
    "ветка метро",
    "метромост",
    "электричка метро",
]
_METRO_PRIMARY = ["метро", "метрополитен", "подземк"]


def _route_metro(q: str) -> "RouteResult | None":
    """Проверяет, относится ли запрос к метрополитену."""
    if not any(m in q for m in _METRO_PRIMARY):
        return None

    score = 0.0
    matched: list[str] = []
    for kw in _METRO_KEYWORDS:
        kw_norm = _normalize(kw)
        kw_parts = kw_norm.split()
        if all(re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in kw_parts):
            matched.append(kw)
            score += len(kw_parts) ** 1.5

    if score == 0:
        score = 1.0
        matched = ["метро"]

    confidence = min(1.0, score / max(len(_METRO_KEYWORDS), 1) * 6)
    confidence = max(confidence, 0.70)
    return RouteResult(
        topic="metro",
        confidence=confidence,
        name="Новосибирский метрополитен",
        matched_keywords=matched,
    )


# ── Аэропорт ──────────────────────────────────────────────────────────────────
_AIRPORT_KEYWORDS = [
    "аэропорт",
    "толмачёв",
    "толмачев",
    "авиарейс",
    "вылет",
    "прилёт",
    "прилет",
    "авиабилет",
    "самолёт",
    "самолет",
    "рейс из новосибирск",
    "рейс в новосибирск",
    "ovb",
    "новапорт",
    "аэровокзал",
]
_AIRPORT_PRIMARY = ["аэропорт", "толмачёв", "толмачев", "авиарейс", "вылет", "прилёт", "прилет", "самолёт", "самолет", "авиабилет", "ovb", "аэровокзал"]


def _route_airport(q: str) -> "RouteResult | None":
    """Проверяет, относится ли запрос к аэропорту."""
    if not any(m in q for m in _AIRPORT_PRIMARY):
        return None

    score = 0.0
    matched: list[str] = []
    for kw in _AIRPORT_KEYWORDS:
        kw_norm = _normalize(kw)
        kw_parts = kw_norm.split()
        if all(re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in kw_parts):
            matched.append(kw)
            score += len(kw_parts) ** 1.5

    if score == 0:
        score = 1.0
        matched = ["аэропорт"]

    confidence = min(1.0, score / max(len(_AIRPORT_KEYWORDS), 1) * 6)
    confidence = max(confidence, 0.70)
    return RouteResult(
        topic="airport",
        confidence=confidence,
        name="Аэропорт Толмачёво",
        matched_keywords=matched,
    )


# ── Медицинские учреждения ────────────────────────────────────────────────────
_MEDICAL_PRIMARY = [
    "больниц", "поликлиник", "клиник", "медицин", "стационар",
    "хирург", "инфекцион", "онколог", "амбулатор",
]
_MEDICAL_KEYWORDS = [
    "больниц",
    "поликлиник",
    "клиник",
    "медицинск учреждени",
    "медицинск организаци",
    "медицинск помощ",
    "стационар",
    "хирургическ",
    "инфекционн больниц",
    "онкологическ",
    "амбулатор",
    "скорая помощ",
    "приёмный покой",
    "приемный покой",
    "лечебн учреждени",
    "гкб",
]


def _route_medical(q: str) -> "RouteResult | None":
    """Маршрутизация запросов к медицинским учреждениям (OSM Overpass)."""
    if not any(m in q for m in _MEDICAL_PRIMARY):
        return None

    score = sum(
        len(kw.split()) ** 1.5
        for kw in _MEDICAL_KEYWORDS
        if all(re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in _normalize(kw).split())
    )
    if score == 0:
        score = 1.0

    confidence = min(1.0, max(score / 8, 0.60))
    return RouteResult(
        topic="medical",
        confidence=confidence,
        name="Медицинские учреждения Новосибирска",
        matched_keywords=[kw for kw in _MEDICAL_KEYWORDS if _normalize(kw) in q],
    )


def _route_construction(q: str) -> "RouteResult | None":
    """Проверяет, относится ли запрос к теме строительства."""
    if not any(m in q for m in _CONSTRUCTION_PRIMARY):
        return None

    score = 0.0
    matched: list[str] = []
    for kw in _CONSTRUCTION_KEYWORDS:
        kw_norm = _normalize(kw)
        kw_parts = kw_norm.split()
        all_match = all(
            re.search(r"(?<![а-яёa-z])" + re.escape(p), q) for p in kw_parts
        )
        if all_match:
            matched.append(kw)
            score += len(kw_parts) ** 1.5

    if score == 0:
        score = 1.0
        matched = ["строительство"]

    confidence = min(1.0, score / max(len(_CONSTRUCTION_KEYWORDS), 1) * 6)
    confidence = max(confidence, 0.55)
    return RouteResult(
        topic="construction",
        confidence=confidence,
        name="Строительство",
        matched_keywords=matched,
    )


def route(query: str) -> list[RouteResult]:
    """Возвращает список RouteResult, отсортированный по убыванию уверенности."""
    q = _normalize(query)
    registry = load_registry()
    results: list[RouteResult] = []

    # Тема индекса пробок (высокий приоритет)
    traffic_result = _route_traffic(q)
    if traffic_result:
        results.append(traffic_result)

    # Тема маршрутов (не в YAML-реестре, проверяем первой — высокий приоритет)
    transit_result = _route_transit(q)
    if transit_result:
        results.append(transit_result)

    # Тема отключений ЖКХ (не в YAML-реестре, обрабатывается отдельно)
    power_result = _route_power(q)
    if power_result:
        results.append(power_result)

    # Тема экологии и метеорологии (не в YAML-реестре, обрабатывается отдельно)
    ecology_result = _route_ecology(q)
    if ecology_result:
        results.append(ecology_result)

    # Тема камер фиксации нарушений (не в YAML-реестре, обрабатывается отдельно)
    cameras_result = _route_cameras(q)
    if cameras_result:
        results.append(cameras_result)

    # Тема тепловых источников (не в YAML-реестре, обрабатывается отдельно)
    heat_sources_result = _route_heat_sources(q)
    if heat_sources_result:
        results.append(heat_sources_result)

    # Тема выбросов в атмосферу (не в YAML-реестре, обрабатывается отдельно)
    emissions_result = _route_emissions(q)
    if emissions_result:
        results.append(emissions_result)

    # Тема медицинских учреждений (не в YAML-реестре, обрабатывается отдельно)
    medical_result = _route_medical(q)
    if medical_result:
        results.append(medical_result)

    # Тема строительства (не в YAML-реестре, обрабатывается отдельно)
    construction_result = _route_construction(q)
    if construction_result:
        results.append(construction_result)

    # Тема метро (не в YAML-реестре, обрабатывается отдельно)
    metro_result = _route_metro(q)
    if metro_result:
        results.append(metro_result)

    # Тема аэропорта (не в YAML-реестре, обрабатывается отдельно)
    airport_result = _route_airport(q)
    if airport_result:
        results.append(airport_result)

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

    results.sort(key=lambda r: r.confidence, reverse=True)
    return results


def best_topic(query: str) -> RouteResult | None:
    """Возвращает лучшую тему или None, если ничего не найдено."""
    results = route(query)
    return results[0] if results else None
