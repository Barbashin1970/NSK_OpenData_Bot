"""
══════════════════════════════════════════════════════════════════════════════
  ИНДЕКС «ВОДИТЕЛЬ»: мороз и ветер, гололёд — отдельной ветвью по влаге
  src/routes/ecology.py: _compute_life_indices · src/static/index.html: computeLifeIndices
══════════════════════════════════════════════════════════════════════════════

Находка сессии РАГРАФ 18.11: индекс давал +5 при средней температуре от −3 до +2 °C без
всякого условия на осадки — прежний «сухой ноль — гололёд», от которого оповещения отказались
17.11 (правило не прошло проверку на ДТП, отчёт РАГРАФ-ПРОГНОЗ.НИР.001-2026, п. 9.2). Теперь —
как в РАГРАФ: индекс считает мороз и ветер, гололёд — по уровню ice_level на сегодня
(«гололёд» +5, «возможна гололедица» +3).
"""

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

KOREN = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def reglament_iz_config(monkeypatch):
    """Регламент индексов — из config/rules (сеялка), а не из рабочей копии data/rules разработчика."""
    yaml = pytest.importorskip("yaml")
    from src.rule_engine import rules
    seed = yaml.safe_load((KOREN / "config/rules/life_indices_rules.yaml").read_text(encoding="utf-8"))
    orig = rules.get
    monkeypatch.setattr(rules, "get", lambda name: seed if name == "life_indices_rules" else orig(name))


def stroki(t, w=3.0):
    return [{"temperature_c": t, "wind_speed_ms": w, "pm25": 5.0, "aqi": 20}]


class TestСервер:
    def test_сухо_около_нуля_без_штрафа(self):
        from src.routes.ecology import _compute_life_indices
        assert _compute_life_indices(stroki(0.5))["driver"]["score"] == 0

    @pytest.mark.parametrize("uroven, ballov", [("glaze", 5), ("refreeze", 3), (None, 0)])
    def test_гололёд_по_влаге(self, uroven, ballov):
        from src.routes.ecology import _compute_life_indices
        assert _compute_life_indices(stroki(0.5), ice_level=uroven)["driver"]["score"] == ballov

    def test_мороз_и_ветер_как_прежде(self):
        from src.routes.ecology import _compute_life_indices
        assert _compute_life_indices(stroki(-22.0))["driver"]["score"] == 2          # сильный мороз
        assert _compute_life_indices(stroki(-5.0, w=13.0))["driver"]["score"] == 2   # только ветер: лёд по температуре больше не начисляется


def funkciya_tablo() -> str:
    """Текст computeLifeIndices из страницы — по балансу фигурных скобок."""
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    i = html.index("function computeLifeIndices(")
    j, glubina = html.index("{", i), 0
    for k in range(j, len(html)):
        glubina += {"{": 1, "}": -1}.get(html[k], 0)
        if glubina == 0:
            return html[i:k + 1]
    raise AssertionError("конец функции не найден")


@pytest.mark.parametrize("t, uroven, ballov", [(0.5, None, 0), (0.5, "glaze", 5), (0.5, "refreeze", 3),
                                               (-5.0, None, 0), (-22.0, None, 2)])
def test_табло_та_же_логика(tmp_path, t, uroven, ballov):
    node = shutil.which("node")
    if not node:
        pytest.skip("node не установлен")
    stend = (funkciya_tablo() + "\nconsole.log(JSON.stringify(computeLifeIndices("
             + json.dumps(stroki(t)) + ", 0, " + json.dumps(uroven) + ").driver.score));")
    f = tmp_path / "ix.js"
    f.write_text(stend, encoding="utf-8")
    assert json.loads(subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout) == ballov


def test_табло_передаёт_уровень_гололёда():
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    assert "computeLifeIndices(ecoRows, eco?.snow_impact)" not in html          # все вызовы с ice_level
    # и с ночным минимумом (регламент 1.3): гололёд и ночь — из одного прогноза
    assert len(re.findall(r"computeLifeIndices\([^)]*ice_level, [^)]*night_min\)", html)) == 3


def test_регламент_индексов():
    yaml = pytest.importorskip("yaml")
    d = yaml.safe_load((KOREN / "config/rules/life_indices_rules.yaml").read_text(encoding="utf-8"))
    dr = d["driver"]
    assert d["version"] in ("1.1", "1.2", "1.3") and dr["ice_glaze_score"] == 5 and dr["ice_refreeze_score"] == 3
    assert not any(k.startswith(("temp_black_ice", "temp_regular_ice")) for k in dr)


# ── «Погодная нагрузка на ЖКХ» (было «Коммунальное напряжение»), решение руководителя работы 18.11 ──
# Мороз по ночному минимуму и снегопад по прогнозу; «штиль + загрязнение» убран: смог — это экология,
# он учтён в индексе «Прогулка», а данные о воздухе — модель CAMS. Название — чтобы не путать с
# загрузкой бригад ЖКХ (прогноз на 2 недели): этот индекс считается только по погоде.

def stroki_vozduh(t, w, pm):
    return [{"temperature_c": t, "wind_speed_ms": w, "pm25": pm, "aqi": 50}]


class TestПогоднаяНагрузка:
    def test_смог_не_ЖКХ(self):
        from src.routes.ecology import _compute_life_indices
        ix = _compute_life_indices(stroki_vozduh(2.0, 0.8, 45.0), snow_impact=0, night_min=1.0)   # штиль, грязный воздух, тепло
        assert ix["utility"]["score"] == 0 and ix["utility"]["name"] == "Погодная нагрузка на ЖКХ"

    def test_мороз_и_снегопад(self):
        from src.routes.ecology import _compute_life_indices
        assert _compute_life_indices(stroki_vozduh(-12.0, 3.0, 5.0), snow_impact=0, night_min=-12.0)["utility"]["score"] == 2
        ix = _compute_life_indices(stroki_vozduh(-12.0, 3.0, 5.0), snow_impact=8, night_min=-12.0)
        assert ix["utility"]["score"] == 5 and ix["utility"]["label"] == "Повышенная"
        assert ix["driver"]["score"] == 1 + 2                                 # водитель: мороз −12 (+1) и снегопад (+2), как на табло


# t — и текущая температура, и ночной минимум прогноза: здесь проверяется сумма мороза и снега
@pytest.mark.parametrize("t, w, pm, sneg, ballov", [(2.0, 0.8, 45.0, 0, 0), (-12.0, 3.0, 5.0, 0, 2), (-12.0, 3.0, 5.0, 8, 5)])
def test_табло_погодная_нагрузка(tmp_path, t, w, pm, sneg, ballov):
    node = shutil.which("node")
    if not node:
        pytest.skip("node не установлен")
    stend = (funkciya_tablo() + "\nconst ix = computeLifeIndices(" + json.dumps(stroki_vozduh(t, w, pm)) + ", " + str(sneg)
             + ", null, " + str(t) + ");\nconsole.log(JSON.stringify([ix.utility.score, ix.utility.name, ix.utility.short]));")
    f = tmp_path / "u.js"
    f.write_text(stend, encoding="utf-8")
    score, name, short = json.loads(subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout)
    assert (score, name, short) == (ballov, "Погодная нагрузка на ЖКХ", "ЖКХ: погода")


def test_регламент_погодной_нагрузки():
    yaml = pytest.importorskip("yaml")
    d = yaml.safe_load((KOREN / "config/rules/life_indices_rules.yaml").read_text(encoding="utf-8"))
    ut = d["utility"]
    assert d["version"] == "1.3" and not any(k.startswith("smog") for k in ut)
    assert [lv["label"] for lv in ut["levels"]] == ["Норма", "Повышенная", "Высокая"]
    assert d["driver"]["snow_heavy_score"] == 2 and d["driver"]["snow_moderate_score"] == 1
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    assert "Коммунальное напряжение" not in html and "Высокое напряжение" not in html


# ── Сверка с РАГРАФ 18.11 (docs/INDEKSY-ZHIZNI-RAZBOR.md): С1 — мороз по ночному минимуму,
# С2 — каждый уровень достижим, С3 — нет прогноза не значит «снега нет» ──────────────────────

class TestСверкаРАГРАФ:
    def test_С1_мороз_по_ночному_минимуму_а_не_по_текущей(self):
        from src.routes.ecology import _compute_life_indices
        # днём тепло, ночью −12: нагрузка на теплосети есть
        assert _compute_life_indices(stroki_vozduh(2.0, 3.0, 5.0), snow_impact=0, night_min=-12.0)["utility"]["score"] == 2
        # сейчас −12, ночь мягкая: балла за мороз нет
        assert _compute_life_indices(stroki_vozduh(-12.0, 3.0, 5.0), snow_impact=0, night_min=-3.0)["utility"]["score"] == 0

    def test_С3_нет_прогноза_не_ноль(self):
        from src.routes.ecology import _compute_life_indices
        ix = _compute_life_indices(stroki_vozduh(-12.0, 3.0, 5.0))           # прогноза нет: ⊥
        ut, dr = ix["utility"], ix["driver"]
        assert ut["score"] is None and ut["label"] == "Нет прогноза" and ut["partial"]
        assert dr["partial"] and "не оценены" in dr["missing"] and dr["score"] == 1   # мороз −12 по текущей — известен
        polu = _compute_life_indices(stroki_vozduh(2.0, 3.0, 5.0), snow_impact=8)   # снег есть, ночи нет
        assert polu["utility"]["score"] == 3 and polu["utility"]["partial"] and "ночной минимум" in polu["utility"]["missing"]
        polnye = _compute_life_indices(stroki_vozduh(2.0, 3.0, 5.0), snow_impact=0, night_min=-3.0)
        assert not polnye["utility"]["partial"] and not polnye["driver"]["partial"]

    def test_входы_прогноза_без_прогноза_None(self, monkeypatch):
        import src.ecology_cache as ec
        monkeypatch.setattr(ec, "_ice_today", lambda d: (None, None, None))
        monkeypatch.setattr(ec, "query_forecast", lambda **k: [])
        assert ec.life_index_inputs(None) == {"ice_level": None, "night_min": None, "snow_impact": None}
        monkeypatch.setattr(ec, "_ice_today", lambda d: ("refreeze", {"rain_mm": 2.0}, {"temp_min": -14.0}))
        monkeypatch.setattr(ec, "query_forecast", lambda **k: [{"snow_impact": 0}, {"snow_impact": 3}, {}])
        assert ec.life_index_inputs(None) == {"ice_level": "refreeze", "night_min": -14.0, "snow_impact": 3.0}


def _tablo(rows, sneg, led, noch):
    node = shutil.which("node")
    if not node:
        pytest.skip("node не установлен")
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        f = Path(d) / "s.js"
        f.write_text(funkciya_tablo() + "\nconst ix = computeLifeIndices(" + ", ".join(json.dumps(v) for v in (rows, sneg, led, noch))
                     + ");\nconsole.log(JSON.stringify(ix));", encoding="utf-8")
        return json.loads(subprocess.run([node, str(f)], capture_output=True, text=True, check=True).stdout)


def test_табло_С1_и_С3_как_сервер():
    from src.routes.ecology import _compute_life_indices
    sluchai = [(2.0, 0, None, -12.0), (-12.0, 0, None, -3.0), (-12.0, None, None, None), (2.0, 8, None, None),
               (2.0, 0, "glaze", -3.0), (-25.0, 9, "refreeze", -33.0)]
    for t, sneg, led, noch in sluchai:
        s = _compute_life_indices(stroki_vozduh(t, 3.0, 5.0), ice_level=led, snow_impact=sneg, night_min=noch)
        b = _tablo(stroki_vozduh(t, 3.0, 5.0), sneg, led, noch)
        for key in ("driver", "utility"):
            assert (b[key]["score"], b[key]["label"], b[key]["partial"]) == \
                   (s[key]["score"], s[key]["label"], s[key]["partial"]), (t, sneg, led, noch, key)


def test_уровни_погодной_нагрузки_достижимы():
    """С2: «Критическая» от 8 была недостижима — сумма мороза и снега не больше 7. Каждый уровень
    регламента должен выпадать хоть при одной погоде, и табло не знает уровней сверх регламента."""
    yaml = pytest.importorskip("yaml")
    ut = yaml.safe_load((KOREN / "config/rules/life_indices_rules.yaml").read_text(encoding="utf-8"))["utility"]
    naibolshaya = max(ut[k] for k in ut if k.startswith("temp_") and k.endswith("_score")) \
        + max(ut[k] for k in ut if k.startswith("snow_") and k.endswith("_score"))
    porogi = [lv["max"] for lv in ut["levels"]]
    assert porogi[-1] >= naibolshaya                                # выше последнего уровня балл не уходит
    nizhe = [-1] + porogi
    assert all(nizhe[i] < min(porogi[i], naibolshaya) for i in range(len(porogi))), (porogi, naibolshaya)
    from src.routes.ecology import _compute_life_indices
    metki = {_compute_life_indices(stroki_vozduh(0.0, 3.0, 5.0), snow_impact=s, night_min=n)["utility"]["label"]
             for n in (0.0, -6.0, -12.0, -25.0, -35.0) for s in (0, 2, 5, 8)}
    assert metki == {lv["label"] for lv in ut["levels"]}
    html = (KOREN / "src" / "static" / "index.html").read_text(encoding="utf-8")
    assert "'Критическая'" not in funkciya_tablo() and "Критическая" not in json.dumps(ut, ensure_ascii=False)
    assert "Нет прогноза" in html


class TestОкноПрогноза:
    """С4 (РАГРАФ 18.11): окно «сутки 1–3 после сегодняшних» — по местным суткам, как прогноз
    Open-Meteo (timezone=Asia/Novosibirsk), а не по UTC: с 00:00 до 07:00 по Новосибирску дата
    по UTC ещё вчерашняя, и окно захватывало сегодняшние местные сутки."""

    def test_местные_сутки_ночью(self, monkeypatch):
        from datetime import date, datetime, timezone
        import src.city_config as cc
        from src.ecology_cache import _local_today
        monkeypatch.setattr(cc, "get_utc_offset", lambda: 7)
        assert _local_today(datetime(2026, 11, 18, 20, 0, tzinfo=timezone.utc)) == date(2026, 11, 19)   # 03:00 местного
        assert _local_today(datetime(2026, 11, 19, 1, 0, tzinfo=timezone.utc)) == date(2026, 11, 19)    # 08:00 местного

    def test_окно_query_forecast_по_местным_суткам(self, monkeypatch):
        from datetime import date
        import src.ecology_cache as ec

        class Conn:
            def close(self):
                pass

        okna = []
        monkeypatch.setattr(ec, "init_ecology_tables", lambda: None)
        monkeypatch.setattr(ec, "_get_conn", lambda *a, **k: Conn())
        monkeypatch.setattr(ec, "_local_today", lambda now=None: date(2031, 1, 15))
        monkeypatch.setattr(ec, "_forecast_days",
                            lambda conn, d, f, t, include_from=True: okna.append((f, t, include_from)) or [])
        ec.query_forecast(days=3)                          # snow_impact индексов: сутки 1–3 после сегодняшних
        ec.query_forecast(days=0, include_today=True)      # индекс пробок: только сегодня
        assert okna == [("2031-01-15", "2031-01-18", False), ("2031-01-15", "2031-01-15", True)]

    def test_пробки_берут_прогноз_на_сегодня(self):
        kod = (KOREN / "src" / "traffic_index.py").read_text(encoding="utf-8")
        assert "query_forecast(days=0, include_today=True)" in kod and "query_forecast(days=1)" not in kod
