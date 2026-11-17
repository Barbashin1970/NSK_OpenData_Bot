"""Калибровка вероятностей: прогноз «p работ» против факта, по корзинам."""
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
SPRING_END, AUTUMN_START = date(2026, 4, 28), date(2026, 10, 11)
heat = lambda d: d < SPRING_END or d >= AUTUMN_START
mondays = [F for F in (date(2026, 4, 27) + timedelta(weeks=k) for k in range(30)) if F + timedelta(days=13) <= max(wd)]
P = defaultdict(list); Yv = defaultdict(list); B = []; issued = set()
for F in mondays:
    hist = [d for d in wd if d < F]
    if len(hist) < 25: continue
    last8 = [d for d in hist if d >= F - timedelta(weeks=8)]
    b = Y.loc[last8].mean().values
    reg8 = [d for d in last8 if heat(d) == heat(F)]
    b_reg = Y.loc[reg8].mean().values if len(reg8) >= 5 else b
    regular = Y.loc[[d for d in hist if d >= F - timedelta(weeks=12)]].sum().values >= 5
    for t in [d for d in wd if F <= d <= F + timedelta(days=13)]:
        refs = [d for d in reversed(hist) if d.weekday() == t.weekday()]
        if len(refs[:4]) < 3: continue
        rr = [d for d in refs if heat(d) == heat(t)][:4]
        y = Y.loc[t].values[regular]
        P["R4"].extend(Y.loc[refs[:4]].values.mean(axis=0)[regular]); Yv["R4"].extend(y)
        issued.add(F)                   # выпуск с прогнозами (27.04, 04.05 — мало истории; 25.05 — простой сбора)
        P["v2"].extend((((Y.loc[rr].values.sum(axis=0) if rr else 0) + 4 * b_reg) / (len(rr) + 4))[regular]); Yv["v2"].extend(y)
        B.extend(b[regular])
bins = [0, 0.1, 0.25, 0.4, 0.6, 0.75, 0.9, 1.0001]
BOARD_BINS = ((0.9, 1.0001), (0.7, 0.9), (0.5, 0.7), (0.3, 0.5), (0.0, 0.3))   # как CONFIDENCE_BINS на табло
for v in ("R4", "v2"):
    p, y = np.array(P[v]), np.array(Yv[v])
    print(f"{v}: прогнозов {len(p)}, Брайер {np.mean((p - y) ** 2):.4f}")
    print("   p работ (прогноз)  →  факт: доля дней с работами   | уверенность «свободно» = 1 − p")
    for lo, hi in zip(bins[:-1], bins[1:]):
        m = (p >= lo) & (p < hi)
        if m.sum():
            print(f"   [{lo:.2f}; {min(hi, 1):.2f})  n={m.sum():5d}  прогноз в среднем {p[m].mean():.2f}  факт {y[m].mean():.2f}"
                  f"   → «свободно» обещано {1 - p[m].mean():.0%}, сбылось {1 - y[m].mean():.0%}")
        else:                                       # пустую корзину тоже показываем: n = 0
            print(f"   [{lo:.2f}; {min(hi, 1):.2f})  n=    0")
    print("   корзины уверенности «свободно», как на табло: n, обещано, сбылось")
    conf = 1 - p
    for lo, hi in BOARD_BINS:
        m = (conf >= lo) & (conf < hi)
        print(f"     {lo:.0%}–{min(hi, 1):.0%}: n={m.sum():5d}" +
              (f"  обещано {conf[m].mean():.1%}  сбылось {np.mean(y[m] == 0):.1%}" if m.sum() else ""))
p, y, b = np.array(P["R4"]), np.array(Yv["R4"]), np.array(B)
for name, m in (("бригада обычно занята (b ≥ 0,7)", b >= 0.7), ("остальные", b < 0.7)):
    f = (p <= 0.25) & m
    print(f"R4 «свободно» там, где {name}: помечено {f.sum()}, сбылось {1 - y[f].mean():.0%}")
print(f"\nВыпусков с прогнозами {len(issued)} из {len(mondays)} понедельников; прогнозов на один выпуск (две недели): "
      f"≈{len(P['R4']) / len(issued):.0f} (регулярные ячейки × рабочие дни)")
