"""Прогноз загрузки ЖКХ на 1–2 недели: свободные и перегруженные дни, риск аварий.

Версия 1 — правило, а не модель (docs/ZHKH-PROGNOZ-RAZBOR.md):

* профиль дня недели по последним неделям: в какой доле таких дней в ячейке
  «район × услуга» были плановые работы и в какой доле объём превышал ёмкость;
* ёмкость бригад — ГИПОТЕЗА из регламента power_forecast_rules.yaml: численности
  бригад в данных нет, за ёмкость принят процентиль объёма загруженных дней;
* самопроверка: то же правило на прошлых неделях против факта — табло показывает,
  насколько прогнозу можно верить.

Расчёт — чистая функция build_forecast() без обращения к БД (её проверяют тесты
на синтетике); get_forecast() только собирает данные и регламенты.

Сутки — как в суточном архиве: по UTC, то есть 07:00–07:00 по Новосибирску.
Рабочие часы бригад (09–17 местного) целиком внутри одних суток.
"""

from __future__ import annotations

import logging
import math
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
    "horizon_days": 14,
    "history_weeks": 4,
    "min_ref_days": 3,
    "free_max_share": 0.25,
    "capacity": {"window_weeks": 12, "min_days": 5, "quantile": 0.75, "factor": 1.0,
                 "overload_share": 0.5},
    "risk": {"window_days": 28},
    "backtest": {"window_days": 28, "min_lag_weeks": 2},
}

HYPOTHESIS_NOTE = (
    "Ёмкость бригад — гипотеза: численности бригад в данных нет. За ёмкость района "
    "по услуге принят объём, с которым бригады справлялись в трёх из четырёх "
    "загруженных дней за последние недели (процентиль и коэффициент — в регламенте)."
)
METHOD_NOTE = (
    "Правило, а не модель: тот же день недели за последние недели. «Свободно» — "
    "плановые работы были не чаще чем в четверти таких дней; «перегруз» — объём "
    "превышал ёмкость хотя бы в половине таких дней. Выходные и праздники как "
    "свободные не предлагаются."
)

STATUSES = ("free", "normal", "overload", "off", "nodata")


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
) -> dict[str, Any]:
    """Прогноз на horizon_days суток вперёд от завтрашнего дня.

    rows — строки «день × район × услуга» (формат query_power_history):
      day, district, utility, planned_houses, planned_records, active_records, snapshots.
    observed_days — дни, когда сборщик работал (query_observed_days). День без
      строк, но наблюдённый, — день без отключений; ненаблюдённый — провал сбора,
      он в расчёт не идёт.
    today — сегодняшние сутки (UTC). Они неполные и в историю не входят.
    """
    p = merge_params(params)
    hist_w = int(p["history_weeks"])
    min_ref = max(1, int(p["min_ref_days"]))   # 0 в регламенте дал бы деление на ноль
    free_max = float(p["free_max_share"])
    horizon = int(p["horizon_days"])
    cap_p, risk_p, bt_p = p["capacity"], p["risk"], p["backtest"]
    over_share = float(cap_p["overload_share"])
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
        """Объём плановых работ («домов в списке»); None — работ в этот день не было."""
        r = cells.get((d.isoformat(), dist, util))
        if not r or int(r.get("planned_records") or 0) <= 0:
            return None
        snaps = int(r.get("snapshots") or 0)
        return int(r.get("planned_houses") or 0) / snaps if snaps else 0.0

    def emergency(d: date, dist: str, util: str) -> bool:
        r = cells.get((d.isoformat(), dist, util))
        return bool(r and int(r.get("active_records") or 0) > 0)

    workdays_observed = [d for d in observed if workday(d)]
    cap_cache: dict[tuple, float | None] = {}

    def capacity_at(end: date, dist: str, util: str) -> float | None:
        """Гипотеза ёмкости по данным строго до даты end (без подглядывания вперёд)."""
        key = (end, dist, util)
        if key not in cap_cache:
            start = end - timedelta(weeks=int(cap_p["window_weeks"]))
            loads = [v for d in workdays_observed if start <= d < end
                     for v in (load(d, dist, util),) if v]
            cap_cache[key] = (_quantile(loads, float(cap_p["quantile"])) * float(cap_p["factor"])
                              if len(loads) >= int(cap_p["min_days"]) else None)
        return cap_cache[key]

    def judge(refs: list[date], dist: str, util: str, cap: float | None) -> tuple[str, float, float, float]:
        """Статус рабочего дня по опорным дням: (статус, p_работ, p_перегруза, средний объём)."""
        loads = [load(d, dist, util) for d in refs]
        p_pl = sum(v is not None for v in loads) / len(loads)
        p_heavy = (sum(v is not None and v > cap for v in loads) / len(loads)) if cap else 0.0
        mean_load = sum(v or 0.0 for v in loads) / len(loads)
        if p_pl <= free_max:
            st = "free"
        elif cap and p_heavy >= over_share:
            st = "overload"
        else:
            st = "normal"
        return st, p_pl, p_heavy, mean_load

    # ── Опорные дни прогноза: тот же день недели, только рабочие и наблюдённые ─
    ref_from = today - timedelta(weeks=hist_w + 2)
    refs_by_weekday = {
        wd: [d for d in reversed(workdays_observed) if d >= ref_from and d.weekday() == wd][:hist_w]
        for wd in range(7)
    }
    capacity = {(dist, util): capacity_at(today, dist, util) for dist in scope for util in utils}

    # ── Горизонт ──────────────────────────────────────────────────────────
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
                elif len(refs) < min_ref:
                    st = "nodata"
                else:
                    st, p_pl, p_heavy, mean_load = judge(refs, dist, util, cap)
                counts[st] += 1
                # «Регулярная» ячейка — где плановые работы идут постоянно и ёмкость
                # оценена. Свободный день там и есть незанятая бригада; где работ
                # почти не бывает (газ), «свободно» тривиально и на табло не идёт.
                regular = cap is not None
                if st == "free" and regular:
                    free_regular += 1
                out_cells.append({
                    "day": t.isoformat(),
                    "district": dist,
                    "utility": util,
                    "utility_code": UTILITY_CODES[util],
                    "status": st,
                    "regular": regular,
                    "p_planned": round(p_pl, 2) if p_pl is not None else None,
                    "p_overload": round(p_heavy, 2) if p_heavy is not None else None,
                    "load": round(mean_load, 1) if mean_load is not None else None,
                    "capacity": round(cap, 1) if cap is not None else None,
                })
        days_out.append({
            "day": t.isoformat(),
            "dow": t.isoweekday(),
            "workday": workday(t),
            "refs": len(refs) if workday(t) else 0,
            **counts,
            "free_regular": free_regular,
        })

    free_windows: dict[tuple[str, str], list[str]] = defaultdict(list)
    busy: dict[tuple[str, str], list[float]] = defaultdict(list)
    overloads: list[dict] = []
    for c in out_cells:
        if c["p_planned"] is not None:
            busy[(c["district"], c["utility"])].append(c["p_planned"])
        if c["status"] == "free" and c["regular"]:
            free_windows[(c["district"], c["utility"])].append(c["day"])
        elif c["status"] == "overload":
            overloads.append(c)
    # Сначала окна там, где бригады обычно заняты: их свободный день — это
    # действительно простаивающая бригада. Ячейка, свободная все дни подряд,
    # говорит скорее о том, что работ там вообще мало.
    free_list = [
        {"district": d, "utility": u, "utility_code": UTILITY_CODES[u], "days": ds,
         "busy_share": round(sum(busy[(d, u)]) / len(busy[(d, u)]), 2) if busy[(d, u)] else 0.0}
        for (d, u), ds in free_windows.items()
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
    # был сделан за min_lag_weeks недель до t: опорные дни и ёмкость берутся только
    # из данных до этой даты (худший горизонт табло, без подглядывания вперёд).
    bt_from = today - timedelta(days=int(bt_p["window_days"]))
    lag = int(bt_p["min_lag_weeks"])
    free_by_util: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0, 0])   # tp, fp, fn, tn
    free_regular_conf = [0, 0, 0, 0]
    over_conf = [0, 0, 0, 0]
    bt_days = [d for d in workdays_observed if d >= bt_from]
    for t in bt_days:
        issued = t - timedelta(weeks=lag)
        refs = [t - timedelta(weeks=k) for k in range(lag, lag + hist_w)]
        refs = [d for d in refs if d in observed_set and workday(d)]
        if len(refs) < min_ref:
            continue
        for dist in scope:
            for util in utils:
                cap = capacity_at(issued, dist, util)
                st, _, _, _ = judge(refs, dist, util, cap)
                actual = load(t, dist, util)
                pred_free, act_free = st == "free", actual is None
                idx = 0 if pred_free and act_free else 1 if pred_free else 2 if act_free else 3
                free_by_util[UTILITY_CODES[util]][idx] += 1
                if cap is not None:
                    free_regular_conf[idx] += 1
                    pred_over, act_over = st == "overload", actual is not None and actual > cap
                    over_conf[0 if pred_over and act_over else 1 if pred_over else 2 if act_over else 3] += 1

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
    }

    error = None
    if district and not scope:
        error = f"район «{district}» не найден в истории отключений"

    return {
        "operation": "POWER_FORECAST",
        "error": error,
        "rules_version": str(p.get("version") or ""),
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
        "method": METHOD_NOTE,
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
    return max(
        int(p["capacity"]["window_weeks"]) * 7 + int(p["backtest"]["window_days"]),
        (hist_w + 2) * 7,
        int(p["backtest"]["window_days"]) + (lag + hist_w) * 7,
        int(p["risk"]["window_days"]),
    ) + 2


def get_forecast(district: str | None = None, today: date | None = None) -> dict[str, Any]:
    """Прогноз на живых данных СИМА: суточный архив + сырые снимки последних 30 дней."""
    from .power_cache import query_observed_days, query_power_history
    from .rule_engine import rules as _rules

    p = merge_params(_rules.get("power_forecast_rules"))
    today = today or datetime.now(timezone.utc).date()
    need = history_days_needed(p)
    rows = query_power_history(days=need)
    observed = query_observed_days(days=need)
    return build_forecast(rows, observed, today, p, non_working_days(), district)
