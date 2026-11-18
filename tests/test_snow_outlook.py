"""
══════════════════════════════════════════════════════════════════════════════
  СНЕГОПАД ПО РАЙОНАМ НА 1–3 СУТОК И АРХИВ ВЫПУСКОВ ПОГОДЫ
  src/ecology_cache.py: snow_level, query_snow_outlook, upsert_forecast
  Разбор: docs/SNEGOPAD-RAZBOR.md (интервью 2026-11-18)
══════════════════════════════════════════════════════════════════════════════

Пороги за сутки — 2, 5 и 15 см (уровни 1–3). Мэру — районы × сутки, району — действия ДЭУ,
тексты УК и жителям, нормы со сроками. Кольцово — отдельное МО, в прогноз не входит.
Проверка прогноза — ретропроверка на прошлых прогнозах против ERA5 (фаза С.1).
"""

import csv
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import src.ecology_cache as ec

ROOT = Path(__file__).parent.parent
LEVELS = [{"level": 1, "min_cm": 2.0}, {"level": 2, "min_cm": 5.0}, {"level": 3, "min_cm": 15.0}]


@pytest.mark.parametrize("cm, ozhid", [(None, 0), (0.0, 0), (1.9, 0), (2.0, 1), (4.9, 1), (5.0, 2),
                                       (14.9, 2), (15.0, 3), (40.0, 3)])
def test_уровень_снегопада_по_порогам(cm, ozhid):
    assert ec.snow_level(cm, LEVELS) == ozhid


@pytest.fixture
def baza(tmp_path, monkeypatch):
    import duckdb
    import src.city_config as cc
    db = str(tmp_path / "eco.duckdb")
    monkeypatch.setattr(ec, "_get_conn", lambda *a, **k: duckdb.connect(db))
    monkeypatch.setattr(ec, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cc, "get_utc_offset", lambda: 0)
    # регламент — из config/rules (сеялка), а не из рабочей копии data/rules разработчика
    yaml = pytest.importorskip("yaml")
    seed = yaml.safe_load((ROOT / "config/rules/ecology_rules.yaml").read_text(encoding="utf-8"))
    orig = ec._rules.get
    monkeypatch.setattr(ec._rules, "get", lambda name: seed if name == "ecology_rules" else orig(name))
    return db


def stroka(st, den, sneg, fetched, tmin=-5.0, tmax=-1.0):
    return {"id": f"{st}_{den}", "station_id": st, "district": st, "forecast_date": den,
            "temp_max": tmax, "temp_min": tmin, "wind_max": 4.0, "precipitation": round(sneg / 1.4, 1),
            "snowfall_cm": sneg, "rain_mm": 0.0, "weathercode": 75 if sneg >= 15 else 73 if sneg >= 5 else 71 if sneg else 3,
            "fetched_at": fetched}


def zapolnit():
    segodnya = datetime.now(timezone.utc).date()
    d = [str(segodnya + timedelta(days=k)) for k in range(0, 4)]
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for st, sneg in (("Советский район", (9.0, 1.0, 6.0, 16.0)), ("Ленинский район", (0.0, 0.0, 0.0, 3.0)),
                     ("Центральный район", (0.0, 0.0, 0.0, 0.0)), ("Кольцово", (0.0, 25.0, 25.0, 25.0))):
        rows += [stroka(st, den, s, now) for den, s in zip(d, sneg)]
    ec.upsert_forecast(rows)
    return d


def test_мэру_районы_по_суткам_кольцово_не_входит(baza):
    d = zapolnit()
    so = ec.query_snow_outlook(None)
    assert [x["day"] for x in so["days"]] == d[1:4]                  # завтра и ещё двое суток, не сегодня
    names = [x["district"] for x in so["districts"]]
    assert "Кольцово" not in names and set(names) == {"Советский район", "Ленинский район", "Центральный район"}
    sov = next(x for x in so["districts"] if x["district"] == "Советский район")
    assert [x["snow_cm"] for x in sov["days"]] == [1.0, 6.0, 16.0] and [x["level"] for x in sov["days"]] == [0, 2, 3]
    assert sov["total_cm"] == 23.0 and sov["max_level"] == 3
    assert so["worst"] == {"district": "Советский район", "day": d[3], "snow_cm": 16.0, "level": 3}
    assert so["max_level"] == 3 and so["city"][2]["districts"] == ["Советский район", "Ленинский район"]
    # действия — по наибольшему уровню горизонта, с нормой и сроком
    deu = " ".join(a["deu"] for a in so["actions"])
    assert "12 ч" in deu and "УГМС" in deu and "ГОСТ Р 50597-2017" in deu
    assert so["retro"]["lead1"]["named"] == 0.78 and "Open-Meteo" in so["source_note"]


def test_району_тексты_ДЭУ_УК_и_жителям(baza):
    zapolnit()
    so = ec.query_snow_outlook("Ленинский")
    assert [x["district"] for x in so["districts"]] == ["Ленинский район"] and so["max_level"] == 1
    a = so["actions"]
    assert [x["level"] for x in a] == [1]
    assert "2 см" in a[0]["deu"] and a[0]["uk"] and a[0]["citizens"]


def test_снега_нет_уровень_ноль_и_действий_нет(baza):
    zapolnit()
    so = ec.query_snow_outlook("Центральный")
    assert so["max_level"] == 0 and so["actions"] == [] and so["worst"] is None


def test_архив_выпусков_погоды(baza):
    """Каждый выданный прогноз остаётся в архиве; eco_forecast хранит последний."""
    import duckdb
    den = str(datetime.now(timezone.utc).date() + timedelta(days=2))
    ec.upsert_forecast([stroka("Советский район", den, 3.0, "2026-11-18T03:00:00+00:00")])
    ec.upsert_forecast([stroka("Советский район", den, 8.0, "2026-11-18T09:00:00+00:00")])
    con = duckdb.connect(baza)
    try:
        arhiv = con.execute("SELECT issued_at, snowfall_cm FROM eco_forecast_issues ORDER BY issued_at").fetchall()
        posl = con.execute("SELECT snowfall_cm FROM eco_forecast").fetchall()
    finally:
        con.close()
    assert arhiv == [("2026-11-18T03:00:00+00:00", 3.0), ("2026-11-18T09:00:00+00:00", 8.0)]
    assert posl == [(8.0,)]


def test_итог_ретропроверки_сходится_с_таблицей():
    """Сторож: числа на табло (SNOW_RETRO) пересчитываются из сохранённой суточной таблицы."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("snow_retro", ROOT / "scripts/snow_retro/snow_retro.py")
    sr = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(sr)
    rows = [r for r in sr.load(str(ROOT / "docs/snegopad-2026/snow_retro_daily.csv")) if int(r["day"][5:7]) in sr.COLD_MONTHS]
    for lead in (1, 3):
        t = sr.table(rows, "era5", lead, 2.0)
        assert round(t["pod"], 2) == ec.SNOW_RETRO[f"lead{lead}"]["named"]
        assert round(t["sbylos"], 2) == ec.SNOW_RETRO[f"lead{lead}"]["came_true"]
    assert sr.table(rows, "era5", 1, 2.0)["obs"] == ec.SNOW_RETRO["events"]
    osen = sr.table([r for r in rows if sr.season(r["day"]) == "2026/27"], "era5", 1, 2.0)
    assert (osen["fc"], osen["tp"]) == (ec.SNOW_RETRO["autumn_2026"]["forecasts"], ec.SNOW_RETRO["autumn_2026"]["came_true"])


def test_регламент_снегопада():
    yaml = pytest.importorskip("yaml")
    d = yaml.safe_load((ROOT / "config/rules/ecology_rules.yaml").read_text(encoding="utf-8"))
    so = d["snow_outlook"]
    assert d["version"] == "1.3" and so["horizon_days"] == 3 and "Кольцово" in so["exclude_districts"]
    assert [(x["level"], x["min_cm"]) for x in so["levels"]] == [(1, 2.0), (2, 5.0), (3, 15.0)]
    assert all(x["deu"] and x["uk"] and x["citizens"] for x in so["levels"])
