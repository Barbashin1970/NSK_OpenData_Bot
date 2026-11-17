"""
══════════════════════════════════════════════════════════════════════════════
  ГОЛОЛЁД ПО ВЛАГЕ (ecology_rules 1.1) — src/ecology_cache.py: ice_level
  Решение заказчика 2026-11-17 · docs/EKOLOGIYA-POGODA-ISSLEDOVANIE.md
══════════════════════════════════════════════════════════════════════════════

Было: «средняя за сутки от −3 до +2 °C» — без осадков; в ноябре 2026 горело 14 суток из 16.
Стало: «гололёд» — ледяной дождь или морось в прогнозе; «возможна гололедица» — днём дождь
от 1 мм, а следующей ночью мороз до −1 °C; в сухую погоду около нуля — молчать.
"""

from datetime import date, datetime, timedelta, timezone

import pytest

import src.ecology_cache as ec

CFG = {"freezing_codes": [56, 57, 66, 67], "refreeze_rain_mm": 1.0, "refreeze_night_temp_c": -1.0}

# Ноябрь 2026 по городу — Open-Meteo, модельный анализ (forecast?past_days=31, 2026-11-17):
# (сутки, t мин, t макс, t сред., дождь мм, код погоды WMO)
NOYABR = [
    ("11-01", 0.3, 4.3, 1.9, 1.6, 73), ("11-02", -1.7, 3.3, 1.4, 3.3, 51),
    ("11-03", -4.9, 0.6, -2.1, 0.0, 73), ("11-04", -0.4, 1.8, 0.5, 0.5, 85),
    ("11-05", -2.5, 0.9, -0.7, 0.2, 85), ("11-06", -2.2, 3.4, 0.5, 0.0, 3),
    ("11-07", 0.8, 5.8, 2.8, 0.7, 85), ("11-08", -1.8, 4.2, 1.7, 0.1, 51),
    ("11-09", 3.9, 6.5, 4.8, 17.2, 61), ("11-10", -3.1, 4.2, 1.1, 0.4, 51),
    ("11-11", -4.5, 0.9, -1.8, 0.0, 3), ("11-12", -1.1, 0.3, -0.4, 0.0, 3),
    ("11-13", -1.4, 0.7, -0.6, 0.0, 3), ("11-14", -1.4, 0.9, -0.3, 0.0, 3),
    ("11-15", -2.7, -0.1, -1.3, 0.1, 51), ("11-16", -3.2, 0.1, -2.0, 0.0, 3),
    ("11-17", -5.0, -0.3, -3.1, 0.0, 3),
]


def den(tmin, tmax, rain, code):
    return {"temp_min": tmin, "temp_max": tmax, "rain_mm": rain, "weathercode": code}


class TestУровень:
    def test_сухо_около_нуля_молчим(self):
        """Ровно жалоба заказчика: около нуля без осадков льда нет."""
        assert ec.ice_level(den(-2, 1, 0.0, 3), den(-3, 0, 0.0, 3), CFG) is None

    @pytest.mark.parametrize("kod", [56, 57, 66, 67])
    def test_ледяной_дождь_или_морось_это_гололёд(self, kod):
        assert ec.ice_level(den(-1, 1, 0.0, kod), None, CFG) == "glaze"

    def test_дождь_и_ночной_мороз_возможна_гололедица(self):
        assert ec.ice_level(den(1, 5, 3.3, 61), den(-2, 2, 0, 3), CFG) == "refreeze"

    def test_дождь_без_ночного_мороза_молчим(self):
        assert ec.ice_level(den(1, 5, 3.3, 61), den(0, 4, 0, 3), CFG) is None

    def test_слабый_дождь_молчим(self):
        assert ec.ice_level(den(1, 5, 0.5, 51), den(-5, 0, 0, 3), CFG) is None

    def test_снег_без_дождя_молчим(self):
        assert ec.ice_level(den(-4, 0, 0.0, 73), den(-8, -2, 0, 73), CFG) is None

    def test_дождь_неизвестен_молчим(self):
        """Строки прогноза до миграции (rain_mm нет) не поднимают тревогу."""
        assert ec.ice_level(den(1, 5, None, 61), den(-5, 0, 0, 3), CFG) is None

    def test_усреднённый_код_не_делает_ледяной_дождь(self):
        """Сводка по городу: морось (51) и дождь (61) в среднем дают 56; решает freezing_any."""
        d = dict(den(0, 3, 0.4, 56), freezing_any=0)
        assert ec.ice_level(d, den(0, 3, 0, 3), CFG) is None
        assert ec.ice_level(dict(d, freezing_any=1), None, CFG) == "glaze"


def test_ноябрь_2026_было_14_стало_3():
    """Замер «до / после» на погоде ноября: сухие дни около нуля больше не горят."""
    dni = NOYABR[:16]                                       # 1–16 ноября
    staroe = sum(-3 <= t_sr <= 2 for _, _, _, t_sr, _, _ in dni)
    novoe = [d for i, (d, tmin, tmax, _, rain, code) in enumerate(dni)
             if ec.ice_level(den(tmin, tmax, rain, code), den(NOYABR[i + 1][1], NOYABR[i + 1][2], 0, 3), CFG)]
    assert staroe == 14
    assert novoe == ["11-01", "11-02", "11-09"]


def test_регламент_читается():
    yaml = pytest.importorskip("yaml")
    from pathlib import Path
    d = yaml.safe_load((Path(__file__).parent.parent / "config/rules/ecology_rules.yaml").read_text(encoding="utf-8"))
    assert d["version"] == "1.1" and d["risks"]["ice"] == CFG
    assert "black_ice" not in d["risks"] and "ice_risk" not in d["forecast"]


def test_сводка_прогноза_и_риск_сегодня_на_временной_базе(tmp_path, monkeypatch):
    """Сквозная проверка: таблица прогноза → сводка по городу → флаги и риск на табло."""
    import duckdb
    import src.city_config as cc
    db = str(tmp_path / "eco.duckdb")
    monkeypatch.setattr(ec, "_get_conn", lambda *a, **k: duckdb.connect(db))
    monkeypatch.setattr(ec, "DATA_DIR", tmp_path)
    monkeypatch.setattr(cc, "get_utc_offset", lambda: 0)
    monkeypatch.setattr(ec, "_ice_rules", lambda: CFG)
    segodnya = datetime.now(timezone.utc).date()
    d = [str(segodnya + timedelta(days=k)) for k in range(4)]
    now = datetime.now(timezone.utc).isoformat()

    def stroka(st, den_, tmin, tmax, rain, code):
        return {"id": f"{st}_{den_}", "station_id": st, "district": f"{st} район", "forecast_date": den_,
                "temp_max": tmax, "temp_min": tmin, "wind_max": 3.0, "precipitation": rain,
                "snowfall_cm": 0.0, "rain_mm": rain, "weathercode": code, "fetched_at": now}

    ec.upsert_forecast([
        # сегодня: морось и дождь в двух точках (среднее кодов было бы 56!), ночью мороз → гололедица
        stroka("А", d[0], -1, 3, 2.0, 51), stroka("Б", d[0], -1, 3, 3.0, 61),
        stroka("А", d[1], -4, 1, 0.0, 3), stroka("Б", d[1], -4, 1, 0.0, 3),
        # послезавтра в одной точке ледяной дождь → гололёд по городу
        stroka("А", d[2], -2, 1, 0.5, 66), stroka("Б", d[2], -2, 1, 0.5, 3),
        stroka("А", d[3], -6, -1, 0.0, 3), stroka("Б", d[3], -6, -1, 0.0, 3),
    ])
    lvl, seg, zavtra = ec._ice_today(None)
    # сводка по городу: самое сильное явление (61), а не среднее кодов (было 56 — «ледяная морось»)
    assert lvl == "refreeze" and seg["freezing_any"] == 0 and seg["weathercode"] == 61

    prognoz = {r["forecast_date"]: r for r in ec.query_forecast(None, days=7)}
    assert prognoz[d[1]]["ice_risk"] is False
    assert prognoz[d[2]]["ice_glaze"] is True and prognoz[d[2]]["ice_level"] == "glaze"

    monkeypatch.setattr(ec, "query_current", lambda *a, **k: [{"temperature_c": 1.0, "wind_speed_ms": 3.0,
                                                               "pm25": 5.0, "humidity_pct": 80}])
    monkeypatch.setattr(ec, "query_history", lambda *a, **k: [])
    led = [r for r in ec.query_risks(None) if r["id"] == "ice"]
    assert len(led) == 1 and led[0]["scenario"] == "Возможна гололедица"
    assert "мост" in led[0]["citizen"] and "эстакад" in led[0]["official"]
    assert "ГОСТ Р 50597-2017, п. 3.4" in led[0]["official"]      # норма — аргумент для мэрии
    assert "чёрн" not in (led[0]["title"] + led[0]["citizen"] + led[0]["official"]).lower()
