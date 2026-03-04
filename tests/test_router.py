"""Тесты маршрутизатора запросов (src/router.py)."""

import pytest
from src.router import best_topic, extract_district, extract_limit, extract_street


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
    """Запрос вне области знаний → None."""
    result = best_topic("погода в Новосибирске завтра")
    # либо None, либо confidence очень низкий — мы проверяем только "не крашится"
    assert result is None or result.confidence < 0.3


def test_route_returns_sorted():
    """route() возвращает результаты отсортированные по убыванию confidence."""
    from src.router import route
    results = route("сколько школ и библиотек по районам")
    confidences = [r.confidence for r in results]
    assert confidences == sorted(confidences, reverse=True)
