"""Тесты планировщика операций (src/planner.py)."""

import pytest
from src.planner import make_plan


# ── Стандартные операции ────────────────────────────────────────────────────

@pytest.mark.parametrize("query,topic,expected_op", [
    ("сколько парковок в Новосибирске",      "parking",     "COUNT"),
    ("количество школ",                      "schools",     "COUNT"),
    ("есть ли парковки в центре",            "parking",     "COUNT"),
    ("топ-5 парковок по числу мест",         "parking",     "TOP_N"),
    ("первые 3 остановки",                   "stops",       "TOP_N"),
    ("парковки по районам",                  "parking",     "GROUP"),
    ("где больше всего школ по районам",     "schools",     "GROUP"),
    ("распределение школ по районам",        "schools",     "GROUP"),
    ("покажи библиотеки в Ленинском",       "libraries",   "FILTER"),
    ("список аптек",                         "pharmacies",  "FILTER"),
    ("все парковки",                         "parking",     "FILTER"),
    ("что ты умеешь",                        None,          "INFO"),
    ("какие темы поддерживаешь",             None,          "INFO"),
])
def test_standard_operations(query, topic, expected_op):
    plan = make_plan(query, topic)
    assert plan.operation == expected_op, (
        f"Запрос: {query!r}, тема: {topic}\n"
        f"Ожидалось: {expected_op}, получено: {plan.operation}"
    )


# ── Power операции ───────────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_op", [
    ("отключения электричества сейчас",                   "POWER_STATUS"),
    ("текущее состояние электроснабжения",                 "POWER_STATUS"),
    ("активные отключения в советском районе",             "POWER_STATUS"),
    ("отключения сегодня",                                 "POWER_TODAY"),
    ("есть ли свет сегодня",                               "POWER_TODAY"),
    ("плановые отключения",                                "POWER_PLANNED"),
    ("запланированные отключения на неделю",               "POWER_PLANNED"),
    ("история отключений за неделю",                       "POWER_HISTORY"),
    ("что отключали в прошлую неделю",                     "POWER_HISTORY"),
    ("архив отключений электроэнергии",                    "POWER_HISTORY"),
])
def test_power_operations(query, expected_op):
    plan = make_plan(query, "power_outages")
    assert plan.operation == expected_op, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_op}, получено: {plan.operation}"
    )


# ── Извлечение параметров ────────────────────────────────────────────────────

def test_plan_district():
    plan = make_plan("парковки в Советском районе", "parking")
    assert plan.district == "Советский район"


def test_plan_no_district():
    plan = make_plan("сколько парковок", "parking")
    assert plan.district is None


def test_plan_limit_topn():
    plan = make_plan("топ-10 парковок", "parking")
    assert plan.operation == "TOP_N"
    assert plan.limit == 10


def test_plan_limit_default_topn():
    """Без числа после топ — лимит по умолчанию 10."""
    plan = make_plan("топ парковок по числу мест", "parking")
    assert plan.operation == "TOP_N"
    assert plan.limit == 10


def test_plan_limit_filter_default():
    """Для FILTER — лимит по умолчанию 20."""
    plan = make_plan("покажи все библиотеки", "libraries")
    assert plan.operation == "FILTER"
    assert plan.limit == 20


def test_plan_street():
    plan = make_plan("аптеки на ул. Ленина", "pharmacies")
    assert plan.street is not None
    assert "ленина" in plan.street.lower()


def test_plan_year():
    plan = make_plan("парковки за 2024 год", "parking")
    assert plan.year == "2024"


def test_plan_min_value():
    plan = make_plan("парковки с числом мест больше 100", "parking")
    assert plan.min_value == 100


def test_plan_power_history_priority_over_planned():
    """История имеет приоритет над плановыми при совпадении обоих паттернов."""
    plan = make_plan("история плановых отключений за неделю", "power_outages")
    assert plan.operation == "POWER_HISTORY"


def test_plan_power_default_status():
    """Запрос без специфических паттернов → POWER_STATUS."""
    plan = make_plan("отключения электричества", "power_outages")
    assert plan.operation == "POWER_STATUS"


# ── sub_district в Plan ──────────────────────────────────────────────────────

@pytest.mark.parametrize("query,topic,expected_sub,expected_district", [
    # Академгородок — разные формы
    ("школы в Академгородке",        "schools",       "Академгородок", "Советский район"),
    ("аптеки Академa",               "pharmacies",    "Академгородок", "Советский район"),
    ("остановки в Академe",          "stops",         "Академгородок", "Советский район"),
    ("библиотеки Academ",            "libraries",     "Академгородок", "Советский район"),
    # Шлюз
    ("детские сады на Шлюзе",        "kindergartens", "Шлюз",          "Советский район"),
    ("школы на Шлюзе",               "schools",       "Шлюз",          "Советский район"),
    # Верхняя зона
    ("аптеки в Верхней зоне",        "pharmacies",    "Верхняя зона",  "Советский район"),
    # Микрорайон «Щ»
    ("остановки в мкр. Щ",           "stops",         'мкр. "Щ"',      "Советский район"),
    ("школы микрорайон Щ",           "schools",       'мкр. "Щ"',      "Советский район"),
])
def test_plan_sub_district_populated(query, topic, expected_sub, expected_district):
    """Plan.sub_district и Plan.district заполняются корректно."""
    plan = make_plan(query, topic)
    assert plan.sub_district == expected_sub, (
        f"Запрос: {query!r}\n"
        f"sub_district={plan.sub_district!r}, ожидалось {expected_sub!r}"
    )
    assert plan.district == expected_district, (
        f"Запрос: {query!r}\n"
        f"district={plan.district!r}, ожидалось {expected_district!r}"
    )


@pytest.mark.parametrize("query,topic", [
    ("парковки в Советском районе",  "parking"),
    ("школы в Ленинском районе",     "schools"),
    ("аптеки в Центральном",         "pharmacies"),
    ("сколько библиотек",            "libraries"),
])
def test_plan_sub_district_none_for_regular_district(query, topic):
    """Обычные районы → Plan.sub_district = None."""
    plan = make_plan(query, topic)
    assert plan.sub_district is None, (
        f"Запрос: {query!r} → sub_district должен быть None, получено {plan.sub_district!r}"
    )


def test_plan_sub_district_power_outages():
    """Power outages + подрайон: district=Советский, sub_district=Академгородок."""
    plan = make_plan("отключения электричества в Академгородке", "power_outages")
    assert plan.operation == "POWER_STATUS"
    assert plan.district == "Советский район"
    assert plan.sub_district == "Академгородок"


# ── Construction операции ────────────────────────────────────────────────────

@pytest.mark.parametrize("query,expected_op,expected_permit_type", [
    ("активные стройки",                            "CONSTRUCTION_ACTIVE",       None),
    ("строящиеся объекты в Кировском районе",       "CONSTRUCTION_ACTIVE",       None),
    ("незавершённое строительство",                 "CONSTRUCTION_ACTIVE",       None),
    ("разрешения на строительство",                 "CONSTRUCTION_PERMITS",      None),
    ("сколько активных строек",                     "CONSTRUCTION_COUNT",        "active"),
    ("количество строек в Калининском",             "CONSTRUCTION_COUNT",        "active"),
    ("стройки по районам",                          "CONSTRUCTION_GROUP",        "active"),
    ("где больше всего строек",                     "CONSTRUCTION_GROUP",        "active"),
    ("ввод в эксплуатацию",                         "CONSTRUCTION_COMMISSIONED", None),
    ("введены в эксплуатацию объекты",              "CONSTRUCTION_COMMISSIONED", None),
    ("сколько введено в эксплуатацию",              "CONSTRUCTION_COUNT",        "commissioned"),
    ("ввод в эксплуатацию по районам",              "CONSTRUCTION_GROUP",        "commissioned"),
])
def test_construction_operations(query, expected_op, expected_permit_type):
    plan = make_plan(query, "construction")
    assert plan.operation == expected_op, (
        f"Запрос: {query!r}\n"
        f"Ожидалось: {expected_op}, получено: {plan.operation}"
    )
    if expected_permit_type is not None:
        assert plan.extra_filters.get("permit_type") == expected_permit_type, (
            f"Запрос: {query!r}\n"
            f"permit_type={plan.extra_filters.get('permit_type')!r}, ожидалось {expected_permit_type!r}"
        )


def test_construction_district_filter():
    """Фильтр по району корректно извлекается для темы construction."""
    plan = make_plan("активные стройки в Калининском районе", "construction")
    assert plan.topic == "construction"
    assert plan.district == "Калининский район"


def test_construction_default_limit():
    """По умолчанию лимит 20 для construction."""
    plan = make_plan("активные стройки", "construction")
    assert plan.limit == 20


def test_construction_group_before_count():
    """GROUP имеет приоритет над COUNT при совпадении обоих паттернов."""
    plan = make_plan("сколько строек по районам", "construction")
    assert plan.operation == "CONSTRUCTION_GROUP"


def test_plan_sub_district_does_not_override_street():
    """Подрайон и улица могут присутствовать одновременно."""
    plan = make_plan("аптеки на ул. Ленина в Академгородке", "pharmacies")
    assert plan.sub_district == "Академгородок"
    assert plan.district == "Советский район"
    assert plan.street is not None
