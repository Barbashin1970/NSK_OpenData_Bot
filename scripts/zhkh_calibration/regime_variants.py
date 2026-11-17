"""Какие режимы лучше: по температуре, по официальным датам или 4 режима (с гидроиспытаниями)."""
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
d_ = lambda m, dd: date(2026, m, dd)
REGIMES = {
    "температура (28.04 / 11.10)": lambda d: "heat" if d < d_(4, 28) or d >= d_(10, 11) else "off",
    "официальные даты (29.04 / 23.09)": lambda d: "heat" if d < d_(4, 29) or d >= d_(9, 23) else "off",
    "4 режима (переход, гидроиспытания)": lambda d: ("heat" if d < d_(4, 29) or d >= d_(10, 17) else
                                                     "transition" if d >= d_(9, 23) else
                                                     "hydro" if d_(5, 16) <= d <= d_(8, 26) else "off"),
}
SELECT_TO, HOLD_FROM = d_(10, 12), d_(10, 19)
mondays = [F for F in (d_(4, 27) + timedelta(weeks=k) for k in range(30)) if F + timedelta(days=13) <= max(wd)]
for name, rg in REGIMES.items():
    conf = defaultdict(lambda: np.zeros(3)); se = defaultdict(list)
    for F in mondays:
        part = "отбор" if F <= SELECT_TO else ("отложено" if F >= HOLD_FROM else None)
        if part is None: continue
        hist = [d for d in wd if d < F]
        if len(hist) < 25: continue
        last8 = [d for d in hist if d >= F - timedelta(weeks=8)]
        b_all = Y.loc[last8].mean().values
        regular = Y.loc[[d for d in hist if d >= F - timedelta(weeks=12)]].sum().values >= 5
        rF = rg(F - timedelta(days=1))                      # режим известен на день выпуска
        same8 = [d for d in last8 if rg(d) == rF]
        b = Y.loc[same8].mean().values if len(same8) >= 5 else b_all
        for t in [d for d in wd if F <= d <= F + timedelta(days=13)]:
            y = Y.loc[t].values
            rr = [d for d in reversed(hist) if d.weekday() == t.weekday() and rg(d) == rF][:4]
            p = ((Y.loc[rr].values.sum(axis=0) if rr else 0) + 4 * b) / (len(rr) + 4)
            se[part].extend(((p - y) ** 2)[regular])
            for th in (0.3, 0.35, 0.4):
                pred = (p <= th) & regular; act = (y == 0) & regular
                conf[(part, th)] += [np.sum(pred & act), np.sum(pred & ~act), np.sum(~pred & act)]
    def pr(s): tp, fp, fn = s; return f"{tp/(tp+fp):.0%}/{tp/(tp+fn):.0%}"
    print(f"{name:36s} | Брайер отбор {np.mean(se['отбор']):.4f} отложено {np.mean(se['отложено']):.4f} | "
          + "  ".join(f"θ{th}: отбор {pr(conf[('отбор', th)])} отложено {pr(conf[('отложено', th)])}" for th in (0.3, 0.35, 0.4)))
