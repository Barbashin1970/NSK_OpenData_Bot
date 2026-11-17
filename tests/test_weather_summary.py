"""
══════════════════════════════════════════════════════════════════════════════
  СВОДКА ПРОГНОЗА ПО ГОРОДУ: КОД ПОГОДЫ И ФЛАГ СНЕГОПАДА (ecology_rules 1.2)
  src/ecology_cache.py: strongest_weathercode, _forecast_days, query_forecast
══════════════════════════════════════════════════════════════════════════════

Замечание РАГРАФ 17.11.2026 (DOPOLNENIE-SIMA-RAZBOR, п. 3.4): коды WMO растут с силой
явления только внутри группы (61 → 63 → 65), а не между группами.
- Было MAX(weathercode): MAX(75 «сильный снег», 80 «слабый ливень») = 80;
  MAX(73, 95) = 95 — снег пропадал из сводки по городу.
- Флаг снегопада по диапазону 70–86 горел на ливневом дожде (80–82), в том числе летом.
Стало: явный ранг явлений и список кодов снега 71, 73, 75, 77, 85, 86.
"""

from datetime import datetime, timedelta, timezone

import pytest

import src.ecology_cache as ec


@pytest.mark.parametrize("kody, ozhid", [
    ([75, 80], 75),        # сильный снег против слабого ливня — пример РАГРАФ
    ([73, 95], 73),        # снег против грозы — снег не пропадает
    ([80, 75], 75),        # порядок точек не важен
    ([51, 61], 61),        # внутри группы — по силе: морось слабее дождя
    ([80, 65], 65),        # сильный дождь сильнее слабого ливня
    ([3, 61], 61),
    ([66, 75], 66),        # ледяной дождь — гололёд — важнее снега
    ([0, 45], 45),         # туман важнее ясного неба
    ([95, 65], 95),        # гроза важнее дождя
    ([None, 3], 3),
    ([], None),
])
def test_самое_сильное_явление(kody, ozhid):
    assert ec.strongest_weathercode(kody) == ozhid


def test_ранг_знает_все_коды_табло():
    """У каждого кода, который табло умеет подписать, есть ранг — иначе он «самый слабый»."""
    assert set(ec.WMO_WEATHER) <= set(ec.WMO_RANK)


def test_сводка_по_городу_снег_не_теряется_и_ливень_не_снегопад(tmp_path, monkeypatch):
    """Сквозная проверка на временной базе: две точки прогноза → сводка по городу → флаги."""
    import duckdb
    db = str(tmp_path / "eco.duckdb")
    monkeypatch.setattr(ec, "_get_conn", lambda *a, **k: duckdb.connect(db))
    monkeypatch.setattr(ec, "DATA_DIR", tmp_path)
    segodnya = datetime.now(timezone.utc).date()
    d = [str(segodnya + timedelta(days=k)) for k in range(1, 4)]
    now = datetime.now(timezone.utc).isoformat()

    def stroka(st, den_, kod, osadki, sneg, tmin, tmax):
        return {"id": f"{st}_{den_}", "station_id": st, "district": f"{st} район", "forecast_date": den_,
                "temp_max": tmax, "temp_min": tmin, "wind_max": 3.0, "precipitation": osadki,
                "snowfall_cm": sneg, "rain_mm": osadki - sneg / 1.4 if osadki > sneg / 1.4 else 0.0,
                "weathercode": kod, "fetched_at": now}

    ec.upsert_forecast([
        # сильный снег в одной точке, слабый ливень в другой (межсезонье)
        stroka("А", d[0], 75, 6.0, 8.0, -3, 0), stroka("Б", d[0], 80, 2.0, 0.0, 1, 4),
        # умеренный снег и гроза
        stroka("А", d[1], 73, 3.0, 3.0, -2, 1), stroka("Б", d[1], 95, 5.0, 0.0, 8, 15),
        # летний ливень в одной точке, ясно в другой
        stroka("А", d[2], 80, 4.0, 0.0, 15, 26), stroka("Б", d[2], 1, 0.0, 0.0, 14, 27),
    ])
    prognoz = {r["forecast_date"]: r for r in ec.query_forecast(None, days=7)}
    assert prognoz[d[0]]["weathercode"] == 75 and prognoz[d[0]]["snow_risk"] is True
    assert prognoz[d[1]]["weathercode"] == 73 and prognoz[d[1]]["snow_risk"] is True
    assert prognoz[d[2]]["weathercode"] == 80 and prognoz[d[2]]["snow_risk"] is False


def test_регламент_снег_по_списку_кодов():
    yaml = pytest.importorskip("yaml")
    from pathlib import Path
    d = yaml.safe_load((Path(__file__).parent.parent / "config/rules/ecology_rules.yaml").read_text(encoding="utf-8"))
    assert d["forecast"]["snow_risk"] == {"snow_codes": [71, 73, 75, 77, 85, 86]}
    assert not set(d["forecast"]["snow_risk"]["snow_codes"]) & {80, 81, 82}
