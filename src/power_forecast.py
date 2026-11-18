"""Прогноз загрузки ЖКХ на 1–2 недели: свободные и перегруженные дни, риск аварий.

Разборы: docs/ZHKH-PROGNOZ-RAZBOR.md (v1), docs/ZHKH-KALIBROVKA-RAZBOR.md (v2).

Оценщик вероятности плановых работ в ячейке «район × услуга × день» задаётся
регламентом (`estimator`):

* ``eb_regime`` (v2, по умолчанию) — режим «отопление включено / выключено» по
  температуре (n суток подряд ниже / выше порога). Опорные дни — тот же день недели
  в том же режиме. Оценка сглаживается к частоте работ ячейки в этом режиме
  (эмпирический Байес): ``p = (k + a·b) / (n + a)``. Насколько вероятности честны
  («свободно 80%» должно сбываться примерно в 80% случаев), показывает самопроверка
  «обещано / сбылось» по корзинам CONFIDENCE_BINS — на табло и в модалке «?».
* ``weekday`` (v1) — тот же день недели за последние недели, ``p = k / n``.

Ёмкость бригад — ГИПОТЕЗА: численности бригад в данных нет, за ёмкость принят
процентиль объёма загруженных дней × коэффициент. Для услуг из
``capacity.regime_utilities`` (тепло) ёмкость считается в пределах режима.

Расчёт — чистая функция build_forecast() без обращения к БД; get_forecast() только
собирает данные, температуру и регламенты. Сутки — как в суточном архиве (UTC,
07:00–07:00 по Новосибирску); рабочие часы бригад целиком внутри одних суток.
"""

from __future__ import annotations

import hashlib
import json
import logging
import math
from bisect import bisect_right
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable

log = logging.getLogger(__name__)

UTILITY_CODES: dict[str, str] = {
    "Электроснабжение": "electricity",
    "Теплоснабжение": "heating",
    "Горячее водоснабжение": "hot_water",
    "Холодное водоснабжение и канализация": "cold_water_sewer",
    "Газоснабжение": "gas",
}

DEFAULT_PARAMS: dict[str, Any] = {
    "estimator": "eb_regime",
    "horizon_days": 14,
    "history_weeks": 4,
    "min_ref_days": 3,               # только для estimator = weekday
    "free_max_share": 0.35,
    "shrinkage": {"strength": 4, "base_weeks": 8, "min_base_days": 5},
    "regime": {"threshold_c": 8.0, "run_days": 5},
    "capacity": {"window_weeks": 12, "min_days": 5, "quantile": 0.75, "factor": 1.0,
                 "overload_share": 0.5, "regime_utilities": ["heating"]},
    "risk": {"window_days": 28},
    "backtest": {"window_days": 28, "min_lag_weeks": 2},
}

HYPOTHESIS_NOTE = (
    "Ёмкость бригад — гипотеза: численности бригад в данных нет. За ёмкость района "
    "по услуге принят объём, с которым бригады справлялись в трёх из четырёх "
    "загруженных дней за последние недели (процентиль и коэффициент — в регламенте)."
)
METHOD_NOTE = {
    "eb_regime": (
        "Правило, а не модель: тот же день недели в том же режиме отопления; оценка "
        "сглажена к обычной частоте работ района по услуге. «Свободно» — вероятность "
        "работ не выше порога; «перегруз» — объём превышал ёмкость хотя бы в половине "
        "таких дней. Выходные и праздники как свободные не предлагаются."
    ),
    "weekday": (
        "Правило, а не модель: тот же день недели за последние недели. «Свободно» — "
        "плановые работы были не чаще порога; «перегруз» — объём превышал ёмкость "
        "хотя бы в половине таких дней. Выходные и праздники как свободные не предлагаются."
    ),
}

STATUSES = ("free", "normal", "overload", "off", "nodata")
# Корзины уверенности «свободно» (1 − p) для самопроверки «обещано / сбылось»
CONFIDENCE_BINS = ((0.9, 1.0001), (0.7, 0.9), (0.5, 0.7), (0.3, 0.5), (0.0, 0.3))


def merge_params(raw: dict | None) -> dict[str, Any]:
    """Параметры регламента поверх значений по умолчанию (вложенные — по ключам)."""
    p: dict[str, Any] = {k: (dict(v) if isinstance(v, dict) else v) for k, v in DEFAULT_PARAMS.items()}
    for k, v in (raw or {}).items():
        if isinstance(v, dict) and isinstance(p.get(k), dict):
            p[k].update(v)
        else:
            p[k] = v
    return p


def non_working_days() -> set[date]:
    """Нерабочие дни из holiday_calendar.yaml: праздники и перенесённые выходные."""
    from .rule_engine import rules as _rules
    out: set[date] = set()
    for entry in (_rules.get("holiday_calendar") or {}).get("holidays", []):
        try:
            if entry.get("type") in ("holiday", "bridge"):
                out.add(date.fromisoformat(str(entry["date"])))
        except Exception as exc:  # битая строка календаря не должна ронять прогноз
            log.warning("power_forecast: некорректная запись календаря %s: %s", entry, exc)
    return out


def heating_regime(temps: dict, threshold_c: float = 8.0, run_days: int = 5) -> dict[date, bool]:
    """Режим «отопление включено» по суточным температурам.

    Отопление включается со дня, следующего за последним днём серии из run_days суток
    подряд со среднесуточной температурой ниже порога, и выключается так же после
    серии выше порога. Пропуск в ряду температуры и ровно пороговое значение прерывают
    серию. Дни до первой полной серии получают режим, на который эта серия указывает.
    """
    series = sorted((date.fromisoformat(str(d)[:10]), float(v)) for d, v in temps.items() if v is not None)
    out: dict[date, bool] = {}
    state: bool | None = None
    pending: list[date] = []
    cold = warm = 0
    prev: date | None = None
    for d, v in series:
        if prev is not None and (d - prev).days != 1:
            cold = warm = 0
        prev = d
        cold, warm = (cold + 1, 0) if v < threshold_c else (0, warm + 1) if v > threshold_c else (0, 0)
        if state is None:
            pending.append(d)
        else:
            out[d] = state                     # переключение действует со следующего дня
        new = True if (state is not True and cold >= run_days) else \
              False if (state is not False and warm >= run_days) else None
        if new is not None:
            if state is None:
                out.update({x: new for x in pending})
                pending = []
            state = new
    return out


def _district_matches(name: str, wanted: str | None) -> bool:
    """Тот же принцип, что у фильтра панели служащего: по первому слову названия."""
    if not wanted or not wanted.strip():
        return True
    key = wanted.strip().split()[0].lower()
    return key in (name or "").lower()


def _quantile(values: list[float], q: float) -> float:
    """Процентиль с линейной интерполяцией (q = 0.5 — медиана)."""
    v = sorted(values)
    pos = min(max(q, 0.0), 1.0) * (len(v) - 1)
    lo, hi = math.floor(pos), math.ceil(pos)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def _summary(tp: int, fp: int, fn: int, tn: int) -> dict[str, Any]:
    n = tp + fp + fn + tn
    return {
        "n": n,
        "base_rate": round((tp + fn) / n, 3) if n else None,
        "precision": round(tp / (tp + fp), 3) if tp + fp else None,
        "recall": round(tp / (tp + fn), 3) if tp + fn else None,
        "flagged": tp + fp,
    }


def build_forecast(
    rows: Iterable[dict],
    observed_days: Iterable[str],
    today: date,
    params: dict | None = None,
    non_working: Iterable[date] = (),
    district: str | None = None,
    temps: dict | None = None,
) -> dict[str, Any]:
    """Прогноз на horizon_days суток вперёд от завтрашнего дня.

    rows — строки «день × район × услуга» (формат query_power_history):
      day, district, utility, planned_houses, planned_records, active_records, snapshots.
    observed_days — дни, когда сборщик работал (query_observed_days). День без
      строк, но наблюдённый, — день без отключений; ненаблюдённый — провал сбора.
    today — сегодняшние сутки (по часовому поясу города). В историю не входят.
    temps — {день: среднесуточная температура по городу}; нужна оценщику eb_regime.
      Без неё режим неизвестен, и оценка сглаживается без учёта режима.
    """
    p = merge_params(params)
    est = str(p.get("estimator") or "eb_regime")
    use_eb = est != "weekday"
    hist_w = int(p["history_weeks"])
    min_ref = max(1, int(p["min_ref_days"]))   # 0 в регламенте дал бы деление на ноль
    free_max = float(p["free_max_share"])
    horizon = int(p["horizon_days"])
    cap_p, risk_p, bt_p, shr, reg_p = p["capacity"], p["risk"], p["backtest"], p["shrinkage"], p["regime"]
    over_share = float(cap_p["overload_share"])
    reg_utils = set(cap_p.get("regime_utilities") or [])
    holidays = set(non_working)

    # ── История по ячейкам ────────────────────────────────────────────────
    cells: dict[tuple[str, str, str], dict] = {}
    districts: set[str] = set()
    for r in rows:
        dist = r.get("district") or ""
        util = r.get("utility") or ""
        if not dist or dist == "all" or util not in UTILITY_CODES:
            continue
        districts.add(dist)
        cells[(str(r.get("day"))[:10], dist, util)] = r

    scope = sorted(d for d in districts if _district_matches(d, district))
    utils = list(UTILITY_CODES)
    observed = sorted({date.fromisoformat(str(d)[:10]) for d in observed_days})
    observed = [d for d in observed if d < today]
    observed_set = set(observed)

    def workday(d: date) -> bool:
        return d.weekday() < 5 and d not in holidays

    def load(d: date, dist: str, util: str) -> float | None:
        """Объём плановых работ («домов в списке»); None — работ в этот день не было.

        Делим на planned_records — число снимков, где висела ПЛАНОВАЯ запись
        (одна строка на снимок), а не на все снимки пары (сверка 2026-11-17).
        """
        r = cells.get((d.isoformat(), dist, util))
        n_planned = int((r or {}).get("planned_records") or 0)
        if n_planned <= 0:
            return None
        return int(r.get("planned_houses") or 0) / n_planned

    def emergency(d: date, dist: str, util: str) -> bool:
        r = cells.get((d.isoformat(), dist, util))
        return bool(r and int(r.get("active_records") or 0) > 0)

    workdays_observed = [d for d in observed if workday(d)]

    # ── Режим отопления по температуре ───────────────────────────────────────
    regime_map = heating_regime(temps or {}, float(reg_p["threshold_c"]), int(reg_p["run_days"])) if use_eb else {}
    regime_days = sorted(regime_map)

    def regime(d: date) -> bool | None:
        """Режим на дату; за пределами ряда — последний известный до неё."""
        if not regime_days:
            return None
        i = bisect_right(regime_days, d) - 1
        return regime_map[regime_days[max(i, 0)]]

    # ── Опорные дни, базовая частота, оценка вероятности ──────────────────────
    def refs_for(cutoff: date, wd: int, rg: bool | None, recent: bool = False) -> list[date]:
        """Опорные дни: тот же день недели до cutoff.

        Для вероятности работ (v2) — в том же режиме, сколь угодно далеко назад: сразу
        после включения отопления это дни прошлого отопительного сезона. Для перегруза
        и объёма (recent=True) и для v1 — последние недели независимо от режима:
        «тяжёлый день» — свойство нынешней нагрузки, а не прошлой весны (замер
        2026-11-17: ёмкость в режиме + последние недели дали подъём ×2,2 против ×1,8).
        """
        cand = [d for d in reversed(workdays_observed) if d < cutoff and d.weekday() == wd]
        if use_eb and not recent:
            return [d for d in cand if rg is None or regime(d) == rg][:hist_w]
        lo = cutoff - timedelta(weeks=hist_w + 2)
        return [d for d in cand if d >= lo][:hist_w]

    base_cache: dict[tuple, list[date]] = {}

    def base_days(cutoff: date, rg: bool | None) -> list[date]:
        key = (cutoff, rg)
        if key not in base_cache:
            lo = cutoff - timedelta(weeks=int(shr["base_weeks"]))
            days = [d for d in workdays_observed if lo <= d < cutoff]
            same = [d for d in days if rg is None or regime(d) == rg]
            base_cache[key] = same if len(same) >= int(shr["min_base_days"]) else days
        return base_cache[key]

    def estimate(refs: list[date], base: list[date], dist: str, util: str) -> float | None:
        """Вероятность плановых работ; None — оценить не на чем."""
        k = sum(load(d, dist, util) is not None for d in refs)
        n = len(refs)
        if not use_eb:
            return k / n if n >= min_ref else None
        if base:
            b = sum(load(d, dist, util) is not None for d in base) / len(base)
        elif n:
            b = k / n
        else:
            return None
        a = float(shr["strength"])
        return (k + a * b) / (n + a)

    cap_cache: dict[tuple, float | None] = {}

    def capacity_at(end: date, dist: str, util: str, rg: bool | None) -> float | None:
        """Гипотеза ёмкости по данным строго до даты end (без подглядывания вперёд)."""
        in_regime = use_eb and rg is not None and UTILITY_CODES[util] in reg_utils
        key = (end, dist, util, rg if in_regime else None)
        if key not in cap_cache:
            start = end - timedelta(weeks=int(cap_p["window_weeks"]))
            loads = [v for d in workdays_observed
                     if start <= d < end and (not in_regime or regime(d) == rg)
                     for v in (load(d, dist, util),) if v]
            cap_cache[key] = (_quantile(loads, float(cap_p["quantile"])) * float(cap_p["factor"])
                              if len(loads) >= int(cap_p["min_days"]) else None)
        return cap_cache[key]

    def judge(p_pl: float, refs: list[date], dist: str, util: str, cap: float | None) -> tuple[str, float, float]:
        """Статус рабочего дня: (статус, p_перегруза, средний объём по опорным дням)."""
        loads = [load(d, dist, util) for d in refs]
        n = len(loads)
        p_heavy = (sum(v is not None and v > cap for v in loads) / n) if (cap and n) else 0.0
        mean_load = (sum(v or 0.0 for v in loads) / n) if n else 0.0
        if p_pl <= free_max:
            st = "free"
        elif cap and p_heavy >= over_share:
            st = "overload"
        else:
            st = "normal"
        return st, p_heavy, mean_load

    # ── Горизонт: режим сегодняшнего дня продлевается на две недели ─────────────
    rg_now = regime(today - timedelta(days=1)) if use_eb else None
    refs_by_weekday = {wd: refs_for(today, wd, rg_now) for wd in range(7)}
    recent_by_weekday = {wd: refs_for(today, wd, rg_now, recent=True) for wd in range(7)}
    base_now = base_days(today, rg_now) if use_eb else []
    capacity = {(dist, util): capacity_at(today, dist, util, rg_now) for dist in scope for util in utils}

    out_cells: list[dict] = []
    days_out: list[dict] = []
    for i in range(1, horizon + 1):
        t = today + timedelta(days=i)
        refs = refs_by_weekday[t.weekday()]
        counts = dict.fromkeys(STATUSES, 0)
        free_regular = 0
        for dist in scope:
            for util in utils:
                cap = capacity[(dist, util)]
                p_pl = p_heavy = mean_load = None
                if not workday(t):
                    st = "off"
                else:
                    p_pl = estimate(refs, base_now, dist, util)
                    if p_pl is None:
                        st = "nodata"
                    else:
                        st, p_heavy, mean_load = judge(p_pl, recent_by_weekday[t.weekday()], dist, util, cap)
                counts[st] += 1
                # «Регулярная» ячейка — где плановые работы идут постоянно и ёмкость
                # оценена. Свободный день там и есть незанятая бригада; где работ
                # почти не бывает (газ), «свободно» тривиально и на табло не идёт.
                regular = cap is not None
                if st == "free" and regular:
                    free_regular += 1
                c = {
                    "day": t.isoformat(),
                    "district": dist,
                    "utility": util,
                    "utility_code": UTILITY_CODES[util],
                    "status": st,
                    "regular": regular,
                }
                # Нет оценки — нет и величины: поля нет совсем, а не 0 и не строка
                # «нет данных» (договор оракула РАГРАФ, 2026-11-17)
                if p_pl is not None:
                    c["p_planned"] = round(p_pl, 2)
                    c["p_free"] = round(1 - p_pl, 2)
                if p_heavy is not None:
                    c["p_overload"] = round(p_heavy, 2)
                if mean_load is not None:
                    c["load"] = round(mean_load, 1)
                if cap is not None:
                    c["capacity"] = round(cap, 1)
                out_cells.append(c)
        days_out.append({
            "day": t.isoformat(),
            "dow": t.isoweekday(),
            "workday": workday(t),
            "refs": len(refs) if workday(t) else 0,
            **counts,
            "free_regular": free_regular,
        })

    free_windows: dict[tuple[str, str], list[dict]] = defaultdict(list)
    busy: dict[tuple[str, str], list[float]] = defaultdict(list)
    overloads: list[dict] = []
    for c in out_cells:
        if "p_planned" in c:
            busy[(c["district"], c["utility"])].append(c["p_planned"])
        if c["status"] == "free" and c["regular"]:
            free_windows[(c["district"], c["utility"])].append({"day": c["day"], "p_free": c["p_free"]})
        elif c["status"] == "overload":
            overloads.append(c)
    # Сначала окна там, где бригады обычно заняты: их свободный день — действительно
    # простаивающая бригада. Ячейка, свободная все дни подряд, говорит скорее о том,
    # что работ там вообще мало.
    free_list = [
        {"district": d, "utility": u, "utility_code": UTILITY_CODES[u],
         "days": [w["day"] for w in ws], "p_free": [w["p_free"] for w in ws],
         "busy_share": round(sum(busy[(d, u)]) / len(busy[(d, u)]), 2) if busy[(d, u)] else 0.0}
        for (d, u), ws in free_windows.items()
    ]
    free_list.sort(key=lambda x: (-x["busy_share"], -len(x["days"]), x["district"], x["utility_code"]))
    overloads.sort(key=lambda c: (-c["p_overload"], c["day"], c["district"]))

    # ── Риск аварий: частота за последние недели (персистентность) ─────────
    risk_from = today - timedelta(days=int(risk_p["window_days"]))
    risk_days = [d for d in observed if d >= risk_from]
    risk: list[dict] = []
    if risk_days:
        for dist in scope:
            for util in utils:
                e = sum(emergency(d, dist, util) for d in risk_days)
                pe = e / len(risk_days)
                risk.append({
                    "district": dist, "utility": util, "utility_code": UTILITY_CODES[util],
                    "p_emergency": round(pe, 2),
                    "expected_days": round(pe * horizon, 1),
                    "days_with_emergency": e,
                    "days_observed": len(risk_days),
                })
    risk.sort(key=lambda x: (-x["p_emergency"], x["district"], x["utility_code"]))

    # ── Самопроверка правила на прошлых неделях ──────────────────────────────
    # Для каждого прошедшего рабочего дня t — то же правило, «как если бы» прогноз
    # был выпущен за min_lag_weeks недель до t: опорные дни, базовая частота, режим
    # и ёмкость — только по данным до дня выпуска включительно.
    bt_from = today - timedelta(days=int(bt_p["window_days"]))
    lag = int(bt_p["min_lag_weeks"])
    free_by_util: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])   # tp, fp, fn, tn
    free_regular_conf = [0, 0, 0, 0]
    over_conf = [0, 0, 0, 0]
    calib = [[0, 0.0, 0] for _ in CONFIDENCE_BINS]        # n, Σ обещанного, сбылось
    sq_err, n_err = 0.0, 0
    bt_days = [d for d in workdays_observed if d >= bt_from]
    for t in bt_days:
        cutoff = t - timedelta(weeks=lag) + timedelta(days=1)
        rg = regime(cutoff - timedelta(days=1)) if use_eb else None
        refs = refs_for(cutoff, t.weekday(), rg)
        recent = refs_for(cutoff, t.weekday(), rg, recent=True)
        base = base_days(cutoff, rg) if use_eb else []
        for dist in scope:
            for util in utils:
                p_pl = estimate(refs, base, dist, util)
                if p_pl is None:
                    continue
                cap = capacity_at(cutoff, dist, util, rg)
                st, _, _ = judge(p_pl, recent, dist, util, cap)
                actual = load(t, dist, util)
                pred_free, act_free = st == "free", actual is None
                idx = 0 if pred_free and act_free else 1 if pred_free else 2 if act_free else 3
                free_by_util[UTILITY_CODES[util]][idx] += 1
                if cap is not None:
                    free_regular_conf[idx] += 1
                    pred_over, act_over = st == "overload", actual is not None and actual > cap
                    over_conf[0 if pred_over and act_over else 1 if pred_over else 2 if act_over else 3] += 1
                    p_free = 1 - p_pl
                    for j, (lo, hi) in enumerate(CONFIDENCE_BINS):
                        if lo <= p_free < hi:
                            calib[j][0] += 1
                            calib[j][1] += p_free
                            calib[j][2] += int(act_free)
                            break
                    sq_err += (p_pl - (0.0 if act_free else 1.0)) ** 2
                    n_err += 1

    backtest = {
        "window_from": bt_days[0].isoformat() if bt_days else None,
        "window_to": bt_days[-1].isoformat() if bt_days else None,
        "horizon_weeks": lag,
        "free": {
            "regular": _summary(*free_regular_conf),
            "by_utility": {code: _summary(*free_by_util[code])
                           for code in UTILITY_CODES.values() if code in free_by_util},
        },
        "overload": _summary(*over_conf),
        # «Обещано / сбылось»: калибровка уверенности «свободно» по корзинам
        "calibration": [
            {"from": lo, "to": min(hi, 1.0), "n": n,
             "promised": round(s / n, 3) if n else None,
             "observed": round(k / n, 3) if n else None}
            for (lo, hi), (n, s, k) in zip(CONFIDENCE_BINS, calib)
        ],
        "brier": round(sq_err / n_err, 4) if n_err else None,
    }

    regime_info = None
    if use_eb:
        since = None
        if regime_days and rg_now is not None:
            past = [d for d in regime_days if d < today]
            for d in reversed(past):
                if regime_map[d] != rg_now:
                    since = d + timedelta(days=1)
                    break
            since = since or (past[0] if past else None)
        regime_info = {
            "heating_on": rg_now,
            "since": since.isoformat() if since else None,
            "threshold_c": float(reg_p["threshold_c"]),
            "run_days": int(reg_p["run_days"]),
            "temperature_days": len(regime_map),
        }

    error = None
    if district and not scope:
        error = f"район «{district}» не найден в истории отключений"

    return {
        "operation": "POWER_FORECAST",
        "error": error,
        "rules_version": str(p.get("version") or ""),
        "estimator": est,
        "regime": regime_info,
        "today": today.isoformat(),
        "horizon_from": (today + timedelta(days=1)).isoformat(),
        "horizon_to": (today + timedelta(days=horizon)).isoformat(),
        "district": district or None,
        "districts": scope,
        "history_from": observed[0].isoformat() if observed else None,
        "history_to": observed[-1].isoformat() if observed else None,
        "observed_days": len(observed),
        "workdays": sum(1 for d in days_out if d["workday"]),
        "params": p,
        "hypothesis": HYPOTHESIS_NOTE,
        "method": METHOD_NOTE.get(est, METHOD_NOTE["eb_regime"]),
        "days": days_out,
        "cells": out_cells,
        "free_windows": free_list,
        "overloads": overloads,
        "capacity": [
            {"district": d, "utility": u, "utility_code": UTILITY_CODES[u],
             "capacity": round(v, 1) if v is not None else None}
            for (d, u), v in sorted(capacity.items())
        ],
        "risk": risk,
        "backtest": backtest,
    }


def history_days_needed(p: dict) -> int:
    """Сколько суток истории нужно правилу при этих параметрах."""
    hist_w = int(p["history_weeks"])
    lag = int(p["backtest"]["min_lag_weeks"])
    need = max(
        int(p["capacity"]["window_weeks"]) * 7 + int(p["backtest"]["window_days"]),
        (hist_w + 2) * 7,
        int(p["backtest"]["window_days"]) + (lag + hist_w) * 7,
        int(p["risk"]["window_days"]),
    ) + 2
    # v2 ищет тот же день недели в том же режиме: сразу после смены режима опорные
    # дни лежат в прошлом таком же сезоне — нужна история за год
    return max(need, 400) if str(p.get("estimator") or "eb_regime") != "weekday" else need


def _city_temperatures(days: int) -> dict[str, float]:
    """Среднесуточная температура по городу: медиана районов из экологического архива.

    Строки-заглушки (snapshots = 0) не берутся — урок 2026-08-19 о синтетике.
    """
    from .cache import _get_conn
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%d")
    conn = _get_conn()
    try:
        rows = conn.execute(
            """
            SELECT day, median(temp_avg) FROM ecology_daily_archive
            WHERE temp_avg IS NOT NULL AND COALESCE(snapshots, 0) > 0 AND day >= ?
            GROUP BY day
            """,
            [cutoff],
        ).fetchall()
        return {str(d)[:10]: float(t) for d, t in rows}
    except Exception as e:
        log.warning("power_forecast: температура недоступна (%s) — режим отопления не учитывается", e)
        return {}
    finally:
        conn.close()


# ── Выпуски: что табло показало на самом деле ─────────────────────────────────
# Самопроверка (шаг 10) пересчитывает прошлое нынешним кодом по нынешнему архиву — это
# не то, что видел мэр. Проверка «обещано / сбылось» по выданному требует сохранять
# каждый выпуск (замечание РАГРАФ 7, 2026-11-17). Выпуск — прогноз по городу: день
# выпуска, версия регламента, ячейки с p и статусом. Тот же прогноз при повторном
# расчёте — тот же выпуск; изменилось содержимое (досчитались вчерашние сутки, правка
# регламента в Студии) — новый выпуск со своим моментом.
_ISSUES_DDL = (
    """
    CREATE TABLE IF NOT EXISTS power_forecast_issues (
        issue_id      VARCHAR PRIMARY KEY,
        issued_at     VARCHAR,      -- момент выпуска, UTC: когда этот прогноз посчитан впервые
        issue_day     VARCHAR,      -- «сегодня» по местному времени; горизонт — со следующих суток
        rules_version VARCHAR,
        estimator     VARCHAR,
        params_json   VARCHAR,
        history_to    VARCHAR,      -- последние наблюдённые сутки, на которых стоит прогноз
        heating_on    BOOLEAN,
        cells         INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS power_forecast_issue_cells (
        issue_id   VARCHAR,
        day        VARCHAR,
        district   VARCHAR,
        utility    VARCHAR,
        status     VARCHAR,
        regular    BOOLEAN,
        p_planned  DOUBLE,          -- NULL: оценки нет (выходной, нет опорных дней)
        p_overload DOUBLE,
        capacity   DOUBLE,
        PRIMARY KEY (issue_id, day, district, utility)
    )
    """,
)
ISSUE_CELL_FIELDS = ("day", "district", "utility_code", "status", "regular", "p_planned", "p_overload", "capacity")
# Договор с РАГРАФ (2026-11-18): регламент sima-brigady-peregruz раз в час читает
# GET /power/forecast?district=Советский. Эти имена полей не менять без предупреждения сессии
# РАГРАФ — сторож tests/test_power_forecast.py::TestВыпуски::test_договор_полей_с_РАГРАФ.
RAGRAF_POLYA = ("days", "cells", "status", "p_free", "p_overload", "issue_id", "issued_at", "rules_version")
_ISSUED: dict[str, str] = {}     # issue_id → issued_at: уже сохранённые этим процессом


def issue_id_of(fc: dict) -> str:
    """Идентификатор выпуска: день выпуска, версия регламента и отпечаток содержимого."""
    body = [[c.get(k) for k in ISSUE_CELL_FIELDS] for c in fc["cells"]]
    raw = json.dumps([fc["today"], fc["rules_version"], fc["estimator"], fc["params"], body],
                     ensure_ascii=False, sort_keys=True, default=str)
    return f"{fc['today']}.{fc['rules_version'] or '0'}.{hashlib.sha1(raw.encode('utf-8')).hexdigest()[:10]}"


def record_issue(fc: dict) -> dict[str, Any]:
    """Сохраняет выпуск прогноза по городу (однажды) и возвращает его паспорт.

    fc — ответ build_forecast(district=None) со всеми ячейками. Сбой записи не ломает
    прогноз: паспорт отдаётся с issue_stored = False.
    """
    if not fc.get("cells"):
        # пустой прогноз (нет истории) — не выпуск: величин в нём нет
        return {"issue_id": None, "issued_at": None, "issue_stored": False}
    iid = issue_id_of(fc)
    if iid in _ISSUED:
        return {"issue_id": iid, "issued_at": _ISSUED[iid], "issue_stored": True}
    issued_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    from .cache import _get_conn
    try:
        conn = _get_conn()
    except Exception as e:
        log.warning("power_forecast: выпуск %s не сохранён — нет базы (%s)", iid, e)
        return {"issue_id": iid, "issued_at": issued_at, "issue_stored": False}
    try:
        for ddl in _ISSUES_DDL:
            conn.execute(ddl)
        row = conn.execute("SELECT issued_at FROM power_forecast_issues WHERE issue_id = ?", [iid]).fetchone()
        if row is None:
            names = ("utility",) + ISSUE_CELL_FIELDS[3:]
            cells = [[iid, c["day"], c["district"]] + [c.get(k) for k in names] for c in fc["cells"]]
            regime = fc.get("regime") or {}
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                "INSERT INTO power_forecast_issues VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING",
                [iid, issued_at, fc["today"], fc["rules_version"], fc["estimator"],
                 json.dumps(fc["params"], ensure_ascii=False, sort_keys=True, default=str),
                 fc.get("history_to"), regime.get("heating_on"), len(cells)],
            )
            for i in range(0, len(cells), 200):        # пачками: executemany в DuckDB ~1 с на 700 строк
                part = cells[i:i + 200]
                conn.execute(
                    "INSERT INTO power_forecast_issue_cells VALUES "
                    + ", ".join(["(?, ?, ?, ?, ?, ?, ?, ?, ?)"] * len(part)) + " ON CONFLICT DO NOTHING",
                    [v for row in part for v in row],
                )
            conn.execute("COMMIT")
            row = conn.execute("SELECT issued_at FROM power_forecast_issues WHERE issue_id = ?", [iid]).fetchone()
        _ISSUED[iid] = str(row[0])
        return {"issue_id": iid, "issued_at": _ISSUED[iid], "issue_stored": True}
    except Exception as e:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        log.warning("power_forecast: выпуск %s не сохранён (%s)", iid, e)
        return {"issue_id": iid, "issued_at": issued_at, "issue_stored": False}
    finally:
        conn.close()


def get_forecast(district: str | None = None, today: date | None = None) -> dict[str, Any]:
    """Прогноз на живых данных СИМА: суточный архив, снимки за 30 дней, температура.

    Каждый ответ несёт паспорт выпуска (issue_id, issued_at, rules_version). Выпуск —
    прогноз по городу; прогноз района — его часть с тем же паспортом.
    """
    from .power_cache import query_observed_days, query_power_history
    from .rule_engine import rules as _rules

    p = merge_params(_rules.get("power_forecast_rules"))
    if today is None:
        # «Сегодня» — по местному времени города: в 00:00–07:00 по Новосибирску дата
        # по UTC ещё вчерашняя, и прогноз начинался бы с сегодняшнего дня как с «завтра».
        from .city_config import get_utc_offset
        try:
            offset = int(get_utc_offset())
        except Exception:
            offset = 7
        today = (datetime.now(timezone.utc) + timedelta(hours=offset)).date()
    need = history_days_needed(p)
    rows = query_power_history(days=need)
    observed = query_observed_days(days=need)
    temps = _city_temperatures(need) if str(p.get("estimator") or "eb_regime") != "weekday" else {}
    holidays = non_working_days()
    fc = build_forecast(rows, observed, today, p, holidays, district, temps)
    city = fc if not district else build_forecast(rows, observed, today, p, holidays, None, temps)
    fc.update(record_issue(city))
    return fc
