"""Надёжность v1 и v2 на отложенных выпусках (замечание РАГРАФ № 8, 17.11.2026).

Постановка — ровно calibration_selection.py: выпуск по понедельникам с 27.04, прогнозные
рабочие дни F … F+13 (горизонт 1–14 суток от последнего дня данных F−1), регулярные ячейки —
не менее 5 рабочих дней с работами за 12 недель до F. v1 — R4 (доля 4 последних таких же
дней недели, при 3 и более); v2 — EB4a4 в пределах режима (a = 4, режим по +8 °C: 28.04 / 11.10).

Что печатает:
- какие выпуски отборочные и какие отложенные, какие дни они покрывают;
- «обещано / сбылось» по корзинам уверенности «свободно», как на табло (CONFIDENCE_BINS
  src/power_forecast.py), с числом прогнозов, и шкалу Брайера — для v1 и v2, на отборе и на
  отложенных выпусках; на отложенных — ещё по горизонтам 1–7 и 8–14 суток;
- те же корзины по p, что в reliability.py (табл. 8 отчёта), на отложенных выпусках;
- сверку с табл. 7 отчёта: точность / полнота «свободно» при пороге 0,25 (v1) и 0,35 (v2).
"""
import sys
from collections import defaultdict
from datetime import date, timedelta
import numpy as np, pandas as pd

D = sys.argv[1]
df = pd.read_csv(f"{D}/zhkh_daily_district.csv", encoding="utf-8-sig"); df["day"] = pd.to_datetime(df["day"]).dt.date
obs = df[(df.collected == 1) & (df.low_sampling == 0) & (df.partial_day == 0)]
work = obs[(obs.is_weekend == 0) & (obs.is_holiday == 0)]
Y = work.pivot_table(index="day", columns=["district", "utility_code"], values="planned_any", aggfunc="first").sort_index()
wd = list(Y.index)
SPRING_END, AUTUMN_START = date(2026, 4, 28), date(2026, 10, 11)        # правило +8 °C по температуре СИМА
heat = lambda d: d < SPRING_END or d >= AUTUMN_START
SELECT_TO, HOLD_FROM = date(2026, 10, 12), date(2026, 10, 19)
mondays = [date(2026, 4, 27) + timedelta(weeks=k) for k in range(30)]
mondays = [F for F in mondays if F + timedelta(days=13) <= max(wd)]
THETA = {"v1": 0.25, "v2": 0.35}
BOARD_BINS = ((0.9, 1.0001), (0.7, 0.9), (0.5, 0.7), (0.3, 0.5), (0.0, 0.3))   # как CONFIDENCE_BINS на табло
P_BINS = [0, 0.1, 0.25, 0.4, 0.6, 0.75, 0.9, 1.0001]                            # как в reliability.py

P = defaultdict(list); Yv = defaultdict(list); Hv = defaultdict(list)          # (part, variant) → массивы
issues = defaultdict(list); skipped = []
for F in mondays:
    part = "отбор" if F <= SELECT_TO else ("отложено" if F >= HOLD_FROM else None)
    if part is None: continue
    hist = [d for d in wd if d < F]
    if len(hist) < 25:
        skipped.append(F); continue
    last8 = [d for d in hist if d >= F - timedelta(weeks=8)]
    b = Y.loc[last8].mean().values
    reg8 = [d for d in last8 if heat(d) == heat(F)]
    b_reg = Y.loc[reg8].mean().values if len(reg8) >= 5 else b
    regular = Y.loc[[d for d in hist if d >= F - timedelta(weeks=12)]].sum().values >= 5
    targets = []
    for t in [d for d in wd if F <= d <= F + timedelta(days=13)]:
        refs = [d for d in reversed(hist) if d.weekday() == t.weekday()]
        refs_reg = [d for d in refs if heat(d) == heat(t)]
        if len(refs[:4]) < 3: continue
        r4, rr = refs[:4], refs_reg[:4]
        y = Y.loc[t].values[regular]
        h = (t - F).days + 1
        pv = {"v1": Y.loc[r4].values.mean(axis=0),
              "v2": ((Y.loc[rr].values.sum(axis=0) if rr else 0) + 4 * b_reg) / (len(rr) + 4)}
        for v, p in pv.items():
            P[(part, v)].append(p[regular]); Yv[(part, v)].append(y); Hv[(part, v)].append(np.full(int(regular.sum()), h))
        targets.append(t)
    if not targets:                      # ни одного дня с 3+ опорными днями — выпуска нет
        skipped.append(F); continue
    issues[part].append((F, targets[0], targets[-1], len(targets), int(regular.sum())))

cat = lambda key: (np.concatenate(P[key]), np.concatenate(Yv[key]), np.concatenate(Hv[key]))

print("Выпуски (понедельник F; прогнозные рабочие дни F … F+13; рабочих дней; регулярных ячеек):")
print(f"  пропущены — меньше 25 рабочих дней истории: {', '.join(F.strftime('%d.%m') for F in skipped) or 'нет'}")
for part in ("отбор", "отложено"):
    lst = issues[part]
    print(f"  [{part}] выпусков {len(lst)}: {lst[0][0].strftime('%d.%m')} … {lst[-1][0].strftime('%d.%m')}; "
          f"прогнозные дни {lst[0][1].strftime('%d.%m')} … {lst[-1][2].strftime('%d.%m')}")
    for F, a, z, n, r in lst[-3:] if part == "отбор" else lst:
        print(f"      F={F.strftime('%d.%m')}  дни {a.strftime('%d.%m')}–{z.strftime('%d.%m')}  рабочих {n}  регулярных ячеек {r}")
sel_last = issues["отбор"][-1]; hold_first = issues["отложено"][0]
if sel_last[2] >= hold_first[1]:
    print(f"  ! последний отборочный выпуск {sel_last[0].strftime('%d.%m')} прогнозирует дни до {sel_last[2].strftime('%d.%m')}: "
          f"исходы {hold_first[1].strftime('%d.%m')}–{sel_last[2].strftime('%d.%m')} участвовали и в отборе")


def board_table(p, y, title, bins=BOARD_BINS):
    conf, free = 1 - p, (y == 0)
    print(f"   {title}")
    print("     уверенность «свободно»   n      обещано   сбылось")
    for lo, hi in bins:
        m = (conf >= lo) & (conf < hi)
        if m.sum():
            print(f"     {lo:.0%}–{min(hi, 1):.0%}{'':14s} {m.sum():5d}    {conf[m].mean():6.1%}   {free[m].mean():6.1%}")
        else:
            print(f"     {lo:.0%}–{min(hi, 1):.0%}{'':14s}     0        —         —")


def p_table(p, y, title):
    print(f"   {title}")
    for lo, hi in zip(P_BINS[:-1], P_BINS[1:]):
        m = (p >= lo) & (p < hi)
        if m.sum():
            print(f"     p∈[{lo:.2f}; {min(hi, 1):.2f})  n={m.sum():5d}  «свободно» обещано {1 - p[m].mean():.0%}, сбылось {1 - y[m].mean():.0%}")
        else:
            print(f"     p∈[{lo:.2f}; {min(hi, 1):.2f})  n=    0")


def prec_rec(p, y, th):
    pred, act = p <= th, y == 0
    tp, fp, fn = np.sum(pred & act), np.sum(pred & ~act), np.sum(~pred & act)
    return tp / (tp + fp), tp / (tp + fn), int(tp + fp)


print("\nСверка с табл. 7 отчёта (точность / полнота «свободно», регулярные ячейки):")
for part in ("отбор", "отложено"):
    s = []
    for v in ("v1", "v2"):
        p, y, _ = cat((part, v)); pr, rc, fl = prec_rec(p, y, THETA[v])
        s.append(f"{v} порог {THETA[v]}: {pr:.1%} / {rc:.1%} (помечено {fl})")
    print(f"   [{part}] " + "   ".join(s))

for part in ("отложено", "отбор"):
    print(f"\n═══ {part.upper()} — «обещано / сбылось» по корзинам табло ═══")
    for v in ("v1", "v2"):
        p, y, _ = cat((part, v))
        print(f"  {v}: прогнозов {len(p)}, Брайер {np.mean((p - y) ** 2):.4f}, доля свободных дней {np.mean(y == 0):.1%}")
        board_table(p, y, "корзины уверенности «свободно» (как на табло):")

print("\n═══ ОТЛОЖЕНО — по горизонтам ═══")
for lo_h, hi_h in ((1, 7), (8, 14)):
    for v in ("v1", "v2"):
        p, y, h = cat(("отложено", v)); m = (h >= lo_h) & (h <= hi_h)
        print(f"  горизонт {lo_h}–{hi_h} сут, {v}: прогнозов {m.sum()}, Брайер {np.mean((p[m] - y[m]) ** 2):.4f}")
        board_table(p[m], y[m], "корзины 90–100 и 70–90:", bins=BOARD_BINS[:2])

print("\n═══ ОТЛОЖЕНО — корзины по p, как в reliability.py (табл. 8) ═══")
for v in ("v1", "v2"):
    p, y, _ = cat(("отложено", v))
    p_table(p, y, f"{v}: прогнозов {len(p)}")

n_all = {v: sum(len(cat((part, v))[0]) for part in ("отбор", "отложено")) for v in ("v1", "v2")}
b_all = {v: np.mean(np.concatenate([(cat((part, v))[0] - cat((part, v))[1]) ** 2 for part in ("отбор", "отложено")])) for v in ("v1", "v2")}
print(f"\nСверка с reliability.py (все выпуски): v1 — {n_all['v1']} прогнозов, Брайер {b_all['v1']:.4f}; "
      f"v2 — {n_all['v2']} прогнозов, Брайер {b_all['v2']:.4f}")
