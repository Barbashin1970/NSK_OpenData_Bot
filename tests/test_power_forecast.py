"""
══════════════════════════════════════════════════════════════════════════════
  ТЕСТЫ ПРОГНОЗА ЗАГРУЗКИ ЖКХ (src/power_forecast.py, /power/forecast)
  Разбор: docs/ZHKH-PROGNOZ-RAZBOR.md · сценарии СЦ-ЖКХ-1, СЦ-ЖКХ-2
══════════════════════════════════════════════════════════════════════════════

Расчёт проверяется на синтетике с известным ответом: неделя с работами по
понедельникам, вторникам и пятницам и без работ по средам и четвергам.
"""

from datetime import date, timedelta
from pathlib import Path

import pytest

from src.power_forecast import _quantile, build_forecast, heating_regime, merge_params

TODAY = date(2026, 11, 16)          # понедельник
EL, GAS = "Электроснабжение", "Газоснабжение"
D1, D2 = "Советский район", "Ленинский район"
SNAPS = 10                           # снимков в сутки, в которых отключение висело
V1 = {"estimator": "weekday", "free_max_share": 0.25}   # прежнее правило (регламент 1.0)


def make_rows(pattern, district=D1, utility=EL, days_back=120):
    """pattern(d) -> (объём «домов в списке» или None, была ли авария)."""
    rows = []
    for i in range(1, days_back + 1):
        d = TODAY - timedelta(days=i)
        load, emerg = pattern(d)
        if load is None and not emerg:
            continue
        rows.append({
            "day": d.isoformat(), "district": district, "utility": utility,
            "planned_houses": int(round((load or 0) * SNAPS)),
            "planned_records": SNAPS if load else 0,
            "active_houses": 5 * SNAPS if emerg else 0,
            "active_records": SNAPS if emerg else 0,
            "snapshots": SNAPS,
        })
    return rows


def all_days(days_back=120, skip=()):
    return [(TODAY - timedelta(days=i)).isoformat() for i in range(1, days_back + 1)
            if (TODAY - timedelta(days=i)) not in skip]


def cell(fc, day, district=D1, utility=EL):
    return next(c for c in fc["cells"]
                if c["day"] == day.isoformat() and c["district"] == district and c["utility"] == utility)


def weekly(d):
    """Работы по пн, вт, пт (10 домов); по ср и чт работ нет."""
    return (10.0 if d.weekday() in (0, 1, 4) else None), False


NEXT_TUE, NEXT_WED, NEXT_SAT, NEXT_MON = (TODAY + timedelta(days=k) for k in (1, 2, 5, 7))


class TestСтатусыДней:
    """СЦ-ЖКХ-2: в ячейке — свободно / обычно / перегруз / выходной."""

    def test_пустой_день_недели_свободен(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY)
        assert cell(fc, NEXT_WED)["status"] == "free"

    def test_день_с_работами_обычный(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY, V1)
        c = cell(fc, NEXT_MON)
        assert c["status"] == "normal"
        assert c["p_planned"] == 1.0 and c["load"] == 10.0

    def test_выходной_не_предлагается_свободным(self):
        """Т-3: суббота — «выходной», а не «свободно»: цель — без сверхурочных."""
        fc = build_forecast(make_rows(weekly), all_days(), TODAY)
        assert cell(fc, NEXT_SAT)["status"] == "off"

    def test_праздник_из_календаря_выходной(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY, non_working={NEXT_WED})
        assert cell(fc, NEXT_WED)["status"] == "off"
        assert next(d for d in fc["days"] if d["day"] == NEXT_WED.isoformat())["workday"] is False

    def test_провал_сбора_не_делает_день_свободным(self):
        """Т-6, разрыв 3 (несущий): ненаблюдённые среды — не «среды без работ»."""
        gaps = {TODAY - timedelta(days=k) for k in (5, 12, 19, 26)}   # 4 последние среды
        def wed_works(d):
            return (None if d in gaps else 10.0 if d.weekday() == 2 else None), False
        fc = build_forecast(make_rows(wed_works), all_days(skip=gaps), TODAY)
        assert cell(fc, NEXT_WED)["status"] != "free"        # v2: опорные дни — раньше провала
        fc1 = build_forecast(make_rows(wed_works), all_days(skip=gaps), TODAY, V1)
        assert cell(fc1, NEXT_WED)["status"] == "nodata"     # v1: в окне 6 недель опор не хватило


class TestГипотезаЁмкости:
    """Т-4: ёмкость — гипотеза из регламента, а не измерение."""

    @staticmethod
    def heavy_tuesdays(d):
        if d.weekday() == 1 and d >= TODAY - timedelta(days=28):
            return 100.0, False                       # последние 4 вторника тяжёлые
        return weekly(d)

    def test_перегруз_когда_объём_выше_ёмкости_в_половине_опорных_дней(self):
        fc = build_forecast(make_rows(self.heavy_tuesdays), all_days(), TODAY)
        c = cell(fc, NEXT_TUE)
        assert c["status"] == "overload"
        assert c["capacity"] == 10.0 and c["p_overload"] == 1.0
        assert any(o["day"] == NEXT_TUE.isoformat() for o in fc["overloads"])

    def test_коэффициент_гипотезы_снимает_перегруз(self):
        params = {"capacity": {"factor": 10.0}}       # «бригады могут вдесятеро больше»
        fc = build_forecast(make_rows(self.heavy_tuesdays), all_days(), TODAY, params)
        assert cell(fc, NEXT_TUE)["status"] == "normal"

    def test_где_работы_редки_ёмкость_не_оценивается(self):
        rare_days = {TODAY - timedelta(days=14), TODAY - timedelta(days=42)}   # два раза за 12 недель
        rare = lambda d: ((5.0 if d in rare_days else None), False)
        rows = make_rows(weekly) + make_rows(rare, utility=GAS)
        fc = build_forecast(rows, all_days(), TODAY)
        gas = cell(fc, NEXT_WED, utility=GAS)
        assert gas["capacity"] is None and gas["regular"] is False
        assert not any(w["utility"] == GAS for w in fc["free_windows"])

    def test_объём_делится_на_снимки_с_плановой_записью(self):
        """Баг 2026-11-17: делили на снимки с ЛЮБОЙ записью пары (включая аварийные)."""
        rows = []
        for k in range(1, 13):
            d = TODAY - timedelta(weeks=k)                    # понедельники
            rows.append({"day": d.isoformat(), "district": D1, "utility": EL,
                         "planned_houses": 200, "planned_records": 2,   # 2 снимка по 100 домов
                         "active_houses": 400, "active_records": 10,    # авария висела весь день
                         "snapshots": 10})
        fc = build_forecast(rows, all_days(), TODAY)
        assert cell(fc, NEXT_MON)["load"] == 100.0          # не 20.0

    def test_гипотеза_проговорена_в_ответе(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY)
        assert "гипотеза" in fc["hypothesis"].lower()


class TestРайонИГород:
    """СЦ-ЖКХ-1 (город) и СЦ-ЖКХ-2 (район)."""

    def test_сужение_на_район(self):
        rows = make_rows(weekly, district=D1) + make_rows(weekly, district=D2)
        fc = build_forecast(rows, all_days(), TODAY, district="Советский")
        assert fc["districts"] == [D1]
        assert {c["district"] for c in fc["cells"]} == {D1}

    def test_неизвестный_район_это_ошибка_а_не_пустой_прогноз(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY, district="Марсианский")
        assert fc["error"] and fc["cells"] == []

    def test_свободные_окна_по_городу(self):
        rows = make_rows(weekly, district=D1) + make_rows(weekly, district=D2)
        fc = build_forecast(rows, all_days(), TODAY)
        windows = {(w["district"], w["utility"]): w["days"] for w in fc["free_windows"]}
        # среды и четверги двух недель горизонта
        assert len(windows[(D1, EL)]) == 4 and NEXT_WED.isoformat() in windows[(D2, EL)]


    def test_сначала_окна_где_бригады_обычно_заняты(self):
        """Свободная пятница у занятой бригады важнее ячейки, где работ почти нет."""
        busy_but_friday = lambda d: ((10.0 if d.weekday() < 4 else None), False)
        quiet_lately = lambda d: ((10.0 if d.weekday() < 2 and 35 <= (TODAY - d).days <= 63 else None), False)
        rows = make_rows(busy_but_friday, district=D1) + make_rows(quiet_lately, district=D2)
        fc = build_forecast(rows, all_days(), TODAY, V1)      # порядок окон — логика, не оценщик
        first, second = fc["free_windows"][0], fc["free_windows"][1]
        assert (first["district"], first["busy_share"]) == (D1, 0.8)
        assert (second["district"], len(second["days"])) == (D2, fc["workdays"])


class TestСамопроверкаИРиск:
    """Т-5: рядом с прогнозом — его проверка на прошлых неделях."""

    def test_на_строго_периодичных_данных_правило_не_ошибается(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY)
        reg = fc["backtest"]["free"]["regular"]
        assert reg["n"] > 0 and reg["precision"] == 1.0 and reg["recall"] == 1.0

    def test_риск_аварий_по_частоте(self):
        emerg_every_4th = lambda d: (None, (TODAY - d).days % 4 == 0)
        fc = build_forecast(make_rows(emerg_every_4th), all_days(), TODAY)
        r = next(x for x in fc["risk"] if x["district"] == D1 and x["utility"] == EL)
        assert r["days_with_emergency"] == 7 and r["p_emergency"] == 0.25 and r["expected_days"] == 3.5


class TestРегламент:
    def test_квантиль(self):
        assert _quantile([1, 2, 3, 4], 0.5) == 2.5
        assert _quantile([10] * 36 + [100] * 4, 0.75) == 10

    def test_регламент_читается_и_совпадает_с_умолчаниями(self):
        yaml = pytest.importorskip("yaml")
        path = Path(__file__).parent.parent / "config" / "rules" / "power_forecast_rules.yaml"
        p = merge_params(yaml.safe_load(path.read_text(encoding="utf-8")))
        assert p["version"] == "1.1" and p["estimator"] == "eb_regime"
        assert p["capacity"]["quantile"] == 0.75 and p["capacity"]["factor"] == 1.0
        assert p["capacity"]["regime_utilities"] == ["heating"]
        assert p["free_max_share"] == 0.35 and p["horizon_days"] == 14
        assert p["shrinkage"]["strength"] == 4 and p["regime"] == {"threshold_c": 8.0, "run_days": 5}


class TestКалибровкаV2:
    """docs/ZHKH-KALIBROVKA-RAZBOR.md: режим отопления, сглаживание, «обещано / сбылось»."""

    def test_режим_по_правилу_пяти_суток(self):
        temps = {}
        for i in range(10):
            temps[(date(2026, 10, 1) + timedelta(days=i)).isoformat()] = 15.0   # тепло
        for i in range(10, 17):
            temps[(date(2026, 10, 1) + timedelta(days=i)).isoformat()] = 2.0    # холод с 11.10
        reg = heating_regime(temps, 8.0, 5)
        assert reg[date(2026, 10, 1)] is False                 # первая серия — тёплая
        assert reg[date(2026, 10, 15)] is False                # 5-й холодный день ещё «выключено»
        assert reg[date(2026, 10, 16)] is True                 # включено со следующего дня

    def test_пропуск_в_температуре_прерывает_серию(self):
        temps = {(date(2026, 10, 1) + timedelta(days=i)).isoformat(): 2.0 for i in (0, 1, 2, 4, 5)}
        assert heating_regime(temps, 8.0, 5) == {}             # 3 + 2 холодных суток — серии из 5 нет

    def test_сглаживание_к_обычной_частоте(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY)        # без температуры
        # пн: k=4 из n=4, обычная частота b=3/5; p = (4 + 4·0,6) / (4 + 4) = 0,8
        assert cell(fc, NEXT_MON)["p_planned"] == 0.8
        assert cell(fc, NEXT_WED)["p_planned"] == 0.3 and cell(fc, NEXT_WED)["p_free"] == 0.7

    def test_после_включения_отопления_день_не_свободен(self):
        """Ровно жалоба заказчика: летние дни без работ по теплу не должны делать
        осенний день «свободным» — v2 берёт опоры из прошлого отопительного сезона."""
        temps = {}
        for k in range(1, 201):
            d = TODAY - timedelta(days=k)
            cold = k > 170 or k <= 10                          # весна и последние 10 суток
            temps[d.isoformat()] = 2.0 if cold else 15.0
        reg = heating_regime(temps, 8.0, 5)
        heat_works = lambda d: ((20.0 if reg.get(d) and d.weekday() < 5 else None), False)
        rows = make_rows(heat_works, utility="Теплоснабжение", days_back=200)
        v2 = build_forecast(rows, all_days(200), TODAY, None, (), None, temps)
        v1 = build_forecast(rows, all_days(200), TODAY, V1)
        assert v2["regime"]["heating_on"] is True
        assert cell(v2, NEXT_WED, utility="Теплоснабжение")["status"] != "free"
        assert cell(v1, NEXT_WED, utility="Теплоснабжение")["status"] == "free"   # старый изъян

    def test_самопроверка_обещано_сбылось(self):
        fc = build_forecast(make_rows(weekly), all_days(), TODAY)
        cal = fc["backtest"]["calibration"]
        assert len(cal) == 5 and sum(c["n"] for c in cal) == fc["backtest"]["free"]["regular"]["n"]
        assert fc["backtest"]["brier"] is not None


class TestЭндпоинт:
    def test_power_forecast_отвечает_структурой(self):
        from fastapi.testclient import TestClient
        from src.api import app
        r = TestClient(app).get("/power/forecast")
        assert r.status_code == 200
        body = r.json()
        assert body["operation"] == "POWER_FORECAST"
        for key in ("days", "free_windows", "overloads", "risk", "backtest", "hypothesis", "regime", "estimator"):
            assert key in body
        assert body["cells"] == []            # по городу ячейки не отдаются
