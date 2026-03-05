"""Тесты маршрутизатора запросов (src/router.py)."""

import pytest
from src.router import best_topic, extract_district, extract_limit, extract_street, extract_sub_district


# ── extract_district ────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected", [
    ("парковки в Центральном районе", "Центральный район"),
    ("аптеки в советском районе", "Советский район"),
    ("школы в Кировском районе", "Кировский район"),
    ("остановки в Дзержинском", "Дзержинский район"),
    ("библиотеки в Ленинском районе", "Ленинский район"),
    ("детские сады в Октябрьском районе", "Октябрьский район"),
    ("спортплощадки в Калининском районе", "Калининский район"),
    ("организации в Первомайском районе", "Первомайский район"),
    ("всё в Заельцовском", "Заельцовский район"),
    ("всё в центре", "Центральный район"),
    ("школы в Новосибирске", None),           # нет района → None
    ("сколько парковок", None),
])
def test_extract_district(query, expected):
    assert extract_district(query) == expected


# ── extract_limit ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected", [
    ("топ-10 парковок по числу мест", 10),
    ("топ 5 школ", 5),
    ("топ3 остановок", 3),
    ("первые 7 аптек", 7),
    ("сколько парковок", None),
    ("покажи все библиотеки", None),
])
def test_extract_limit(query, expected):
    assert extract_limit(query) == expected


# ── extract_street ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_prefix", [
    ("аптеки на ул. Ленина", "ленина"),
    ("аптеки на улице Ленина", "ленина"),
    ("остановки на проспекте Маркса", "маркса"),
    ("парковки в Центральном районе", None),  # нет улицы
])
def test_extract_street(query, expected_prefix):
    result = extract_street(query)
    if expected_prefix is None:
        assert result is None
    else:
        assert result is not None and expected_prefix in result.lower()


# ── best_topic routing ──────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_topic", [
    ("сколько парковок в Новосибирске", "parking"),
    ("парковки по районам", "parking"),
    ("топ-10 парковок", "parking"),
    ("покажи парковки в Ленинском районе", "parking"),
    ("остановки в Советском районе", "stops"),
    ("сколько автобусных остановок", "stops"),  # "автобусн" ключевое слово для stops
    ("список организаций культуры", "culture"),
    ("театры Новосибирска", "culture"),
    ("сколько школ в Новосибирске", "schools"),
    ("школы в Кировском районе", "schools"),
    ("детские сады в Октябрьском районе", "kindergartens"),
    ("сколько дошкольных организаций", "kindergartens"),
    ("библиотеки в Ленинском районе", "libraries"),
    ("сколько библиотек", "libraries"),
    ("парки культуры и отдыха", "parks"),
    ("спортивные площадки в Калининском районе", "sport_grounds"),
    ("аптеки в Железнодорожном районе", "pharmacies"),
    ("спортивные организации в Первомайском районе", "sport_orgs"),
    # power_outages
    ("отключения электричества сегодня", "power_outages"),
    ("есть ли свет в советском районе", "power_outages"),
    ("плановые отключения электроснабжения", "power_outages"),
    ("история отключений электроэнергии за неделю", "power_outages"),
])
def test_best_topic(query, expected_topic):
    result = best_topic(query)
    assert result is not None, f"Нет темы для запроса: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic} "
        f"(confidence={result.confidence:.2f}, matched={result.matched_keywords})"
    )


def test_best_topic_no_match():
    """Запрос вне области знаний → None или очень низкий confidence."""
    # «погода» теперь поддерживается (ecology), используем явно OOD-запрос
    result = best_topic("рецепты борща с говядиной")
    assert result is None or result.confidence < 0.3


def test_route_returns_sorted():
    """route() возвращает результаты отсортированные по убыванию confidence."""
    from src.router import route
    results = route("сколько школ и библиотек по районам")
    confidences = [r.confidence for r in results]
    assert confidences == sorted(confidences, reverse=True)


# ── extract_sub_district ─────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_sub,expected_parent", [
    # Академгородок — полное название, все падежи
    ("в Академгородке",              "Академгородок", "Советский район"),
    ("Академгородок",                "Академгородок", "Советский район"),
    ("из Академгородка",             "Академгородок", "Советский район"),
    ("рядом с Академгородком",       "Академгородок", "Советский район"),
    # Академгородок — латиница Academ
    ("Academ",                        "Академгородок", "Советский район"),
    ("аптеки в Academ",               "Академгородок", "Советский район"),
    ("academ",                        "Академгородок", "Советский район"),
    # Академгородок — разговорное «Академe» / «Академa», кириллица
    ("остановки Академa",             "Академгородок", "Советский район"),
    ("школы Академa",                 "Академгородок", "Советский район"),
    ("в Академе",                     "Академгородок", "Советский район"),
    # Академгородок — омоглифы (Latin e вместо кирилл. е, Latin a вместо кирилл. а)
    ("Академe",                       "Академгородок", "Советский район"),   # лат. e
    ("школы Академa",                 "Академгородок", "Советский район"),   # лат. a
    ("к Академy",                     "Академгородок", "Советский район"),   # лат. y
    # Шлюз — все формы
    ("на Шлюзе",                      "Шлюз",          "Советский район"),
    ("школы на Шлюзе",                "Шлюз",          "Советский район"),
    ("шлюз",                          "Шлюз",          "Советский район"),
    # Верхняя зона
    ("в Верхней зоне",                "Верхняя зона",  "Советский район"),
    ("парки Верхней зоны",            "Верхняя зона",  "Советский район"),
    # Микрорайон «Щ» — разные записи
    ("мкр. Щ",                        'мкр. "Щ"',      "Советский район"),
    ("мкр Щ",                         'мкр. "Щ"',      "Советский район"),
    ("микрорайон Щ",                  'мкр. "Щ"',      "Советский район"),
    ("детские сады в мкр. Щ",         'мкр. "Щ"',      "Советский район"),
    ("остановки в Щ",                 'мкр. "Щ"',      "Советский район"),
])
def test_extract_sub_district_match(query, expected_sub, expected_parent):
    """Подрайон распознаётся, возвращает (parent_district, display_name)."""
    result = extract_sub_district(query)
    assert result is not None, f"Подрайон не распознан в запросе: {query!r}"
    parent, sub = result
    assert sub == expected_sub, f"Запрос {query!r}: sub_district={sub!r}, ожидалось {expected_sub!r}"
    assert parent == expected_parent, f"Запрос {query!r}: district={parent!r}, ожидалось {expected_parent!r}"


@pytest.mark.parametrize("query", [
    "школы в Ленинском районе",
    "парковки по районам",
    "аптеки в Центральном",
    "сколько парковок",
    "советский район",           # Советский без подрайона — не sub_district
    "обычный текст без смысла",
    "электричество в Дзержинском",
])
def test_extract_sub_district_no_match(query):
    """Запросы без подрайонов возвращают None."""
    assert extract_sub_district(query) is None, f"Ложное срабатывание на: {query!r}"


@pytest.mark.parametrize("query,expected_district", [
    # Подрайоны → родительский район
    ("аптеки в Академгородке",   "Советский район"),
    ("школы Академa",            "Советский район"),
    ("остановки на Шлюзе",       "Советский район"),
    ("детские сады в мкр. Щ",    "Советский район"),
    ("парки Верхней зоны",       "Советский район"),
    ("Academ",                   "Советский район"),
    # Обычные районы — не ломаются
    ("парковки в Центральном",   "Центральный район"),
    ("школы в Ленинском районе", "Ленинский район"),
])
def test_extract_district_via_sub_district(query, expected_district):
    """extract_district возвращает родительский район для запросов с подрайоном."""
    assert extract_district(query) == expected_district, (
        f"Запрос: {query!r} → ожидалось {expected_district!r}"
    )


@pytest.mark.parametrize("query,expected_topic", [
    # Подрайоны не ломают маршрутизацию к теме
    ("школы в Академгородке",        "schools"),
    ("аптеки Академa",               "pharmacies"),
    ("остановки на Шлюзе",           "stops"),
    ("детские сады в мкр. Щ",        "kindergartens"),
    ("спортплощадки Верхней зоны",   "sport_grounds"),
    ("отключения света в Академгородке", "power_outages"),
    ("библиотеки Academ",            "libraries"),
])
def test_sub_district_routing(query, expected_topic):
    """Запросы с подрайонами корректно маршрутизируются к теме."""
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic} "
        f"(confidence={result.confidence:.2f})"
    )
