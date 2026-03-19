"""Тесты маршрутизации запросов из чипов-подсказок (ROLE_CHIPS_TPL в index.html).

Каждый чип при клике отправляет запрос в router.py.
Тесты проверяют, что все чипы корректно маршрутизируются после изменений шаблонов.

Плейсхолдеры {D1}/{D2}/{D3} заменены NSK-дефолтами:
  D1 = "Центральном районе",  D1_SHORT = "Центральный"
  D2 = "Ленинском районе",    D2_SHORT = "Ленинский"
  D3 = "Советском районе",    D3_SHORT = "Советский"
  SUBURB = "Академгородке"
"""

import pytest
from src.router import best_topic


# ── Citizen chips ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_topic", [
    # Отключения ЖКХ
    ("нет горячей воды",                              "power_outages"),
    ("отключения электричества сейчас",               "power_outages"),
    ("нет горячей воды в Ленинском районе",           "power_outages"),
    ("отключение отопления сегодня",                  "power_outages"),
    # Экология и погода (включая "риски")
    ("качество воздуха в городе",                     "ecology"),
    ("погода в Академгородке",                        "ecology"),
    ("риски для жизни в городе",                      "ecology"),
    # Инфраструктура (opendata CSV)
    ("топ-5 аптек",                                   "pharmacies"),
    ("аптеки в Центральном районе",                   "pharmacies"),
    ("поликлиники в Центральном районе",              "medical"),
    ("библиотеки для детей",                          "libraries"),
    ("детские спортивные организации",                "sport_orgs"),
    ("школы в Ленинском районе",                      "schools"),
    ("активные стройки",                              "construction"),
    # Медицина
    ("больницы Новосибирска",                         "medical"),
    # Камеры
    ("камеры видеофиксации",                          "cameras"),
    # Пробки
    ("пробки сейчас",                                 "traffic_index"),
])
def test_citizen_chips_routing(query, expected_topic):
    """Все чипы роли «Горожанин» маршрутизируются к правильной теме."""
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic} "
        f"(confidence={result.confidence:.2f}, matched={result.matched_keywords})"
    )


@pytest.mark.parametrize("query,expected_topic", [
    ("метро Новосибирск",  "metro"),
    ("аэропорт Толмачёво", "airport"),
])
def test_citizen_feature_chips(query, expected_topic):
    """Чипы metro/airport маршрутизируются к соответствующим темам."""
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic}"
    )


def test_transit_chip_routing():
    """Транзитный чип «как проехать из X в Y» маршрутизируется к transit."""
    from src.router import route
    # Вариант с short-label районов (шаблон {D3_SHORT} в {D1_SHORT})
    results = route("как проехать из Советский в Центральный")
    topics = [r.topic for r in results]
    assert "transit" in topics, (
        f"Транзитный запрос не распознан: topics={topics}"
    )


# ── Official chips ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_topic", [
    ("аварийные отключения ЖКХ сейчас",   "power_outages"),
    ("плановые отключения на неделю",      "power_outages"),
    ("отключения тепла сейчас",            "power_outages"),
    ("отключения воды сейчас",             "power_outages"),
    ("история отключений за неделю",       "power_outages"),
    ("превышение ПДК по PM2.5",           "ecology"),
    ("качество воздуха по всем районам",   "ecology"),
    ("динамика PM2.5 за неделю",           "ecology"),
    ("сколько парковок по районам",        "parking"),
    ("школы по районам",                   "schools"),
    ("аптеки в Ленинском районе",          "pharmacies"),
    ("библиотеки города",                  "libraries"),
    ("культурные организации",             "culture"),
    ("камеры фиксации нарушений",          "cameras"),
    ("больницы по районам",                "medical"),
    ("поликлиники города",                 "medical"),
    ("активные стройки",                   "construction"),
    ("стройки по районам",                 "construction"),
])
def test_official_chips_routing(query, expected_topic):
    """Все чипы роли «Служащий» маршрутизируются к правильной теме."""
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic} "
        f"(confidence={result.confidence:.2f})"
    )


# ── Mayor chips ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_topic", [
    ("отключения ЖКХ сейчас",             "power_outages"),
    ("риски для жизни в городе",           "ecology"),
    ("качество воздуха в городе",          "ecology"),
    ("динамика качества воздуха",          "ecology"),
    ("история отключений за неделю",       "power_outages"),
    ("школы по районам",                   "schools"),
    ("сколько парковок по районам",        "parking"),
    ("топ-5 аптек",                        "pharmacies"),
    ("культурные организации",             "culture"),
    ("аптеки в Центральном районе",        "pharmacies"),
    ("камеры видеофиксации на дорогах",    "cameras"),
    ("больницы по районам",                "medical"),
    ("больницы с приёмным покоем",         "medical"),
    ("активные стройки",                   "construction"),
    ("стройки по районам",                 "construction"),
])
def test_mayor_chips_routing(query, expected_topic):
    """Все чипы роли «Мэр» маршрутизируются к правильной теме."""
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic} "
        f"(confidence={result.confidence:.2f})"
    )


# ── Omsk district variants ────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_topic", [
    # Омские округа вместо NSK районов — чипы должны работать для любого города
    ("нет горячей воды в Ленинском округе",   "power_outages"),
    ("аптеки в Советском округе",             "pharmacies"),
    ("больницы в Центральном округе",         "medical"),
    ("школы в Кировском округе",              "schools"),
])
def test_omsk_district_chips_routing(query, expected_topic):
    """Чипы с омскими округами также маршрутизируются корректно."""
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic}"
    )


# ── Kemerovo district variants ───────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_topic", [
    # Кемеровские районы — чипы должны работать для любого города
    ("больницы в Центральном районе",   "medical"),
    ("школы в Рудничном районе",        "schools"),
    ("аптеки в Заводском районе",       "pharmacies"),
    ("камеры в Кировском районе",       "cameras"),
])
def test_kemerovo_district_chips_routing(query, expected_topic):
    """Чипы с кемеровскими районами маршрутизируются корректно."""
    result = best_topic(query)
    assert result is not None, f"Тема не определена для: {query!r}"
    assert result.topic == expected_topic, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_topic}, получено: {result.topic}"
    )
