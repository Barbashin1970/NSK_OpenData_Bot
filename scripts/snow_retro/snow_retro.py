"""Ретропроверка прогноза снегопада на 1–3 суток (разбор docs/SNEGOPAD-RAZBOR.md, фаза С.1).

Прогноз — тот же Open-Meteo, что на табло СИМА (модель по умолчанию, best_match): архив прошлых
прогнозов (Previous Runs API) даёт для каждого часа значение, выданное за 1, 2, 3 суток до него.
Факт — реанализ ERA5 (Historical Weather API, models=era5, сетка 0,25°). Модель архива по
умолчанию брать нельзя: для этих лет она отдаёт ECMWF IFS — те же данные, что «свежий прогноз»
(сутки 0), и проверка стала бы сравнением прогноза с самим собой (найдено 2026-11-18).
Оба источника — модельные продукты, а не замер на станции: это ограничение названо в разборе.

Только стандартная библиотека. Два режима:

    python3 scripts/snow_retro/snow_retro.py fetch <папка> [--from 2024-01-01] [--to ГГГГ-ММ-ДД]
        скачать часовые прогнозы и ERA5, сохранить суточную таблицу <папка>/snow_retro_daily.csv
    python3 scripts/snow_retro/snow_retro.py score <папка>/snow_retro_daily.csv
        посчитать попадания, пропуски и ложные тревоги по сохранённой таблице (без сети)

Точка — центр Новосибирска (55.03, 82.92); в запросах только координаты города.
"""

from __future__ import annotations

import csv
import json
import ssl
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import date, timedelta

LAT, LON, TZ = 55.03, 82.92, "Asia/Novosibirsk"
PREV_URL = "https://previous-runs-api.open-meteo.com/v1/forecast"
ERA5_URL = "https://archive-api.open-meteo.com/v1/archive"
LEADS = (0, 1, 2, 3)                 # 0 — свежий прогноз на тот же час, 1–3 — выданный за N суток
THRESHOLDS = (2.0, 5.0)              # см за сутки — уровни 1 и 2 регламента snow_outlook
COLD_MONTHS = (10, 11, 12, 1, 2, 3, 4)
COLS = ["day", "era5"] + [f"lead{n}" for n in LEADS]


def _ssl_context() -> ssl.SSLContext:
    """Корневые сертификаты: у Python с python.org на macOS своих нет — берём certifi, если есть."""
    try:
        import certifi
        return ssl.create_default_context(cafile=certifi.where())
    except ImportError:
        return ssl.create_default_context()


def get_json(url: str, params: dict, tries: int = 3) -> dict:
    q = url + "?" + urllib.parse.urlencode(params)
    ctx = _ssl_context()
    for k in range(tries):
        try:
            with urllib.request.urlopen(urllib.request.Request(q, headers={"User-Agent": "sima-snow-retro"}),
                                        timeout=120, context=ctx) as r:
                return json.load(r)
        except Exception as e:          # сеть — повторить с паузой
            if k == tries - 1:
                raise
            print(f"  повтор после ошибки: {e}", file=sys.stderr)
            time.sleep(5 * (k + 1))
    return {}


def chunks(a: date, b: date, days: int = 90):
    while a <= b:
        z = min(b, a + timedelta(days=days - 1))
        yield a, z
        a = z + timedelta(days=1)


def fetch(out_dir: str, d_from: date, d_to: date) -> None:
    daily: dict[str, dict[str, float | None]] = defaultdict(dict)
    hourly_vars = ",".join(["snowfall"] + [f"snowfall_previous_day{n}" for n in LEADS if n])
    for a, z in chunks(d_from, d_to):
        d = get_json(PREV_URL, {"latitude": LAT, "longitude": LON, "hourly": hourly_vars, "timezone": TZ,
                                "start_date": a.isoformat(), "end_date": z.isoformat()})
        h = d.get("hourly", {})
        for n in LEADS:
            key = "snowfall" if n == 0 else f"snowfall_previous_day{n}"
            per_day: dict[str, list] = defaultdict(list)
            for t, v in zip(h.get("time", []), h.get(key, [])):
                per_day[t[:10]].append(v)
            for day, vals in per_day.items():
                # сутки считаются, только если все 24 часа есть: иначе сумма занижена
                daily[day][f"lead{n}"] = round(sum(vals), 2) if len(vals) == 24 and None not in vals else None
        e = get_json(ERA5_URL, {"latitude": LAT, "longitude": LON, "daily": "snowfall_sum", "timezone": TZ,
                                "models": "era5", "start_date": a.isoformat(), "end_date": z.isoformat()})
        for day, v in zip(e.get("daily", {}).get("time", []), e.get("daily", {}).get("snowfall_sum", [])):
            daily[day]["era5"] = v
        print(f"  {a} … {z}: скачано", file=sys.stderr)
    path = f"{out_dir.rstrip('/')}/snow_retro_daily.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(COLS)
        for day in sorted(daily):
            w.writerow([day] + [daily[day].get(c) for c in COLS[1:]])
    print(f"сохранено: {path}, суток {len(daily)}", file=sys.stderr)


def load(path: str) -> list[dict]:
    rows = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            rows.append({k: (r[k] if k == "day" else (float(r[k]) if r[k] not in ("", "None") else None))
                         for k in COLS})
    return rows


def season(day: str) -> str:
    y, m = int(day[:4]), int(day[5:7])
    return f"{y - 1}/{str(y)[2:]}" if m <= 4 else f"{y}/{str(y + 1)[2:]}"


def table(rows: list[dict], fact: str, lead: int, thr: float) -> dict:
    tp = fp = fn = tn = 0
    for r in rows:
        f, o = r.get(f"lead{lead}"), r.get(fact)
        if f is None or o is None:
            continue
        pf, po = f >= thr, o >= thr
        tp += pf and po; fp += pf and not po; fn += (not pf) and po; tn += (not pf) and (not po)
    n = tp + fp + fn + tn
    return {"n": n, "obs": tp + fn, "fc": tp + fp, "tp": tp, "fp": fp, "fn": fn,
            "pod": tp / (tp + fn) if tp + fn else None,        # доля снегопадов, которые прогноз назвал
            "sbylos": tp / (tp + fp) if tp + fp else None,     # доля прогнозов снегопада, которые сбылись (1 − FAR)
            "csi": tp / (tp + fp + fn) if tp + fp + fn else None,
            "bias": (tp + fp) / (tp + fn) if tp + fn else None}


def pct(x) -> str:
    return "—" if x is None else f"{100 * x:.0f}%"


def score(path: str) -> None:
    rows = [r for r in load(path) if int(r["day"][5:7]) in COLD_MONTHS]
    have = [r for r in rows if r.get("lead1") is not None and r.get("era5") is not None]
    print(f"Холодные месяцы (окт–апр): суток {len(rows)}, с прогнозом на 1 сутки и ERA5 — {len(have)}; "
          f"{have[0]['day'] if have else '—'} … {have[-1]['day'] if have else '—'}")
    print("Точка: центр Новосибирска (55.03, 82.92); прогноз — Open-Meteo best_match; факт — ERA5 (models=era5).\n")
    for fact, name in (("era5", "факт — ERA5"),
                       ("lead0", "факт — свежий прогноз на тот же день, сутки 0 (согласованность, не независимый факт)")):
        print(f"═══ {name} ═══")
        for thr in THRESHOLDS:
            print(f"  снег ≥ {thr:g} см за сутки")
            print("    заблаговр.  суток  снегопадов  прогнозов  попал  ложн  пропуск   названо  сбылось   CSI  смещение")
            for lead in (1, 2, 3):
                if fact == "lead0" and lead == 0:
                    continue
                t = table(rows, fact, lead, thr)
                print(f"    {lead} сут      {t['n']:5d}  {t['obs']:10d}  {t['fc']:9d}  {t['tp']:5d}  {t['fp']:4d}  {t['fn']:7d}"
                      f"   {pct(t['pod']):>6}   {pct(t['sbylos']):>6}  {pct(t['csi']):>4}  "
                      f"{'—' if t['bias'] is None else f'{t['bias']:.2f}':>7}")
        print()
    print("═══ По зимам: снег ≥ 2 см, заблаговременность 1 сутки, факт ERA5 ═══")
    by = defaultdict(list)
    for r in rows:
        by[season(r["day"])].append(r)
    for s in sorted(by):
        t = table(by[s], "era5", 1, 2.0)
        print(f"  {s}: суток {t['n']:3d}, снегопадов {t['obs']:3d}, прогнозов {t['fc']:3d}, "
              f"названо {pct(t['pod'])}, сбылось {pct(t['sbylos'])}")
    print("\nНазвано — доля дней со снегом ≥ порога (по факту), на которые прогноз его дал (POD).")
    print("Сбылось — доля дней с прогнозом снега ≥ порога, когда снег действительно был (1 − FAR).")
    print("CSI — попадания на все дни, где снег был в прогнозе или в факте. Смещение — прогнозов на снегопад.")


def main(argv: list[str]) -> None:
    if len(argv) < 3 or argv[1] not in ("fetch", "score"):
        sys.exit(__doc__)
    if argv[1] == "score":
        score(argv[2])
        return
    opts = dict(zip(argv[3::2], argv[4::2]))
    d_from = date.fromisoformat(opts.get("--from", "2024-01-01"))
    d_to = date.fromisoformat(opts.get("--to", (date.today() - timedelta(days=6)).isoformat()))  # ERA5 — с задержкой ~5 сут
    fetch(argv[2], d_from, d_to)


if __name__ == "__main__":
    main(sys.argv)
