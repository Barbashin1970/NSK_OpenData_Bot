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
