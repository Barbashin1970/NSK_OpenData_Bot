"""Тесты power_cache.py: get_power_meta и query_power.

Проверяют, что цифры в шапке (active_houses / planned_houses)
совпадают с суммой строк таблицы для того же типа ресурса.
"""

import pytest
from datetime import datetime, timezone

from src.cache import _get_conn
from src.power_cache import (
    init_power_table,
    upsert_outages,
    get_power_meta,
    query_power,
)


_SCRAPED_AT = "2099-12-31T23:59:59+00:00"  # всегда новее реальных данных → MAX(scraped_at) = тестовые

_SAMPLE_RECORDS = [
    # Электроснабжение — активные
    {
        "id": "e-active-1",
        "utility": "Электроснабжение",
        "utility_id": "electric",
        "group_type": "active",
        "district": "Кировский район",
        "houses": 40,
        "scraped_at": _SCRAPED_AT,
        "source_url": "http://test",
    },
    # Электроснабжение — плановые
    {
        "id": "e-planned-1",
        "utility": "Электроснабжение",
        "utility_id": "electric",
        "group_type": "planned",
        "district": "Дзержинский район",
        "houses": 48,
        "scraped_at": _SCRAPED_AT,
        "source_url": "http://test",
    },
    {
        "id": "e-planned-2",
        "utility": "Электроснабжение",
        "utility_id": "electric",
        "group_type": "planned",
        "district": "Калининский район",
        "houses": 220,
        "scraped_at": _SCRAPED_AT,
        "source_url": "http://test",
    },
    # Теплоснабжение — активные (другой тип ресурса)
    {
        "id": "h-active-1",
        "utility": "Теплоснабжение",
        "utility_id": "heat",
        "group_type": "active",
        "district": "Ленинский район",
        "houses": 14,
        "scraped_at": _SCRAPED_AT,
        "source_url": "http://test",
    },
    # Водоснабжение — плановые (другой тип ресурса)
    {
        "id": "w-planned-1",
        "utility": "Холодное водоснабжение",
        "utility_id": "cold_water",
        "group_type": "planned",
        "district": "Советский район",
        "houses": 30,
        "scraped_at": _SCRAPED_AT,
        "source_url": "http://test",
    },
]


@pytest.fixture(autouse=True)
def clean_power_table():
    """Очищает тестовые записи до и после каждого теста."""
    _cleanup()
    yield
    _cleanup()


def _cleanup():
    init_power_table()
    conn = _get_conn()
    try:
        conn.execute("DELETE FROM power_outages WHERE source_url = 'http://test'")
    finally:
        conn.close()


@pytest.fixture
def loaded_records():
    upsert_outages(_SAMPLE_RECORDS)


# ── get_power_meta без фильтра — сумма по всем типам ресурсов ─────────────────

def test_meta_no_filter_active_houses(loaded_records):
    """Без фильтра active_houses = сумма по всем utility."""
    meta = get_power_meta()
    # 40 (электро) + 14 (тепло) = 54
    assert meta["active_houses"] == 54


def test_meta_no_filter_planned_houses(loaded_records):
    """Без фильтра planned_houses = сумма по всем utility."""
    meta = get_power_meta()
    # 48 + 220 (электро) + 30 (вода) = 298
    assert meta["planned_houses"] == 298


# ── get_power_meta с фильтром — совпадает с суммой строк таблицы ──────────────

def test_meta_electricity_active_matches_rows(loaded_records):
    """active_houses для электроснабжения == сумма houses в строках таблицы."""
    rows = query_power(utility_filter="электроснабж", group_filter="active", latest_only=True)
    row_sum = sum(r["houses"] for r in rows)

    meta = get_power_meta(utility_filter="электроснабж")
    assert meta["active_houses"] == row_sum, (
        f"Шапка: {meta['active_houses']}, строки: {row_sum} — числа расходятся"
    )


def test_meta_electricity_planned_matches_rows(loaded_records):
    """planned_houses для электроснабжения == сумма houses в строках таблицы."""
    rows = query_power(utility_filter="электроснабж", group_filter="planned", latest_only=True)
    row_sum = sum(r["houses"] for r in rows)

    meta = get_power_meta(utility_filter="электроснабж")
    assert meta["planned_houses"] == row_sum


def test_meta_electricity_active_excludes_other_utilities(loaded_records):
    """Фильтр по электроснабжению не включает Теплоснабжение."""
    meta_all = get_power_meta()
    meta_elec = get_power_meta(utility_filter="электроснабж")
    # Электро: 40 активных, Тепло: 14 активных → без фильтра сумма больше
    assert meta_elec["active_houses"] < meta_all["active_houses"]
    assert meta_elec["active_houses"] == 40


def test_meta_heat_filter(loaded_records):
    """Фильтр по теплоснабжению считает только тепловые дома."""
    meta = get_power_meta(utility_filter="теплоснабж")
    assert meta["active_houses"] == 14
    assert meta["planned_houses"] == 0


def test_meta_all_utilities_filter_none(loaded_records):
    """utility_filter=None (все типы) эквивалентен вызову без аргумента."""
    assert get_power_meta(utility_filter=None) == get_power_meta()


# ── district_filter: шапка совпадает со строками при фильтре по району ────────

def test_meta_district_filter_planned_matches_rows(loaded_records):
    """planned_houses с district_filter == сумма houses в строках таблицы для того же района."""
    rows = query_power(utility_filter="электроснабж", group_filter="planned",
                       district_filter="Дзержинский", latest_only=True)
    row_sum = sum(r["houses"] for r in rows)

    meta = get_power_meta(utility_filter="электроснабж", district_filter="Дзержинский")
    assert meta["planned_houses"] == row_sum
    assert meta["planned_houses"] == 48  # только Дзержинский, не 268


def test_meta_district_filter_excludes_other_districts(loaded_records):
    """Фильтр по одному району не включает дома из других районов."""
    meta_all = get_power_meta(utility_filter="электроснабж")
    meta_one = get_power_meta(utility_filter="электроснабж", district_filter="Дзержинский")
    # Без фильтра planned: 48+220=268; с фильтром по Дзержинскому: 48
    assert meta_one["planned_houses"] < meta_all["planned_houses"]
    assert meta_one["planned_houses"] == 48


def test_meta_hot_water_district_filter_matches_rows(loaded_records):
    """Сценарий скриншота: горячая вода + район → шапка совпадает с таблицей."""
    # В тестовых данных нет горячей воды, проверяем generic-логику через холодную воду
    rows = query_power(utility_filter="холодн", district_filter="Советский", latest_only=True)
    row_sum = sum(r["houses"] for r in rows)

    meta = get_power_meta(utility_filter="холодн", district_filter="Советский")
    assert meta["planned_houses"] == row_sum
    assert meta["planned_houses"] == 30
