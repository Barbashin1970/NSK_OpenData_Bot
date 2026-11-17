"""
Формула «домов в моменте» в «Отчёте за всё время» (power_cache, сверка 2026-11-17).

Среднее по паре «район × услуга» делится на число снимков с записью ЭТОЙ группы
(active_records / planned_records), а не на все снимки пары: иначе плановые работы,
висевшие часть дня рядом с аварией, занижались, а короткая авария округлялась в ноль.
Чтение из БД подменяется синтетикой с известным ответом.
"""

import pytest

import src.power_cache as pc

DAY, DIST, EL = "2026-11-10", "Советский район", "Электроснабжение"


@pytest.fixture
def fake_history(monkeypatch):
    def install(rows):
        monkeypatch.setattr(pc, "query_power_history", lambda *a, **k: rows)
        monkeypatch.setattr(pc, "_query_now_by_district", lambda *a, **k: {})
    return install


def row(**kw):
    base = {"day": DAY, "district": DIST, "utility": EL, "active_houses": 0, "planned_houses": 0,
            "active_records": 0, "planned_records": 0, "snapshots": 0}
    base.update(kw)
    return base


def test_плановые_делятся_на_свои_снимки(fake_history):
    # 2 снимка по 100 домов плановых; авария висела все 10 снимков по 3 дома
    fake_history([row(planned_houses=200, planned_records=2, active_houses=30, active_records=10, snapshots=10)])
    d = pc.query_power_history_by_day(days=30)[0]
    assert d["planned_houses"] == 100          # старая формула давала 20
    assert d["active_houses"] == 3


def test_короткая_авария_не_округляется_в_ноль(fake_history):
    # плановые висели весь день (29 снимков), авария — один снимок на один дом
    fake_history([row(planned_houses=290, planned_records=29, active_houses=1, active_records=1, snapshots=29)])
    assert pc.query_power_history_by_day(days=30)[0]["active_houses"] == 1   # было 0 (1/29)
    dist = pc.query_power_history_by_district(days=30)[0]
    assert dist["days_with_outages"] == 1


def test_avg_while_listed_без_записей_ноль():
    assert pc._avg_while_listed(row(planned_houses=50), "planned") == 0.0
