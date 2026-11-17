import sys
from collections import defaultdict
from datetime import date, timedelta
import numpy as np, pandas as pd
D = sys.argv[1]
df = pd.read_csv(f"{D}/zhkh_daily_district.csv", encoding="utf-8-sig"); df["day"] = pd.to_datetime(df["day"]).dt.date
obs = df[(df.collected == 1) & (df.low_sampling == 0) & (df.partial_day == 0)]
work = obs[(obs.is_weekend == 0) & (obs.is_holiday == 0)]
Y = work.pivot_table(index="day", columns=["district", "utility_code"], values="planned_any", aggfunc="first").sort_index()
L = work.pivot_table(index="day", columns=["district", "utility_code"], values="planned_houses_listed", aggfunc="first").sort_index()
util = np.array([c[1] for c in Y.columns]); wd = list(Y.index)
SPRING_END, AUTUMN_START = date(2026, 4, 28), date(2026, 10, 11)
heat_on = lambda d: d < SPRING_END or d >= AUTUMN_START
mondays = [date(2026, 4, 27) + timedelta(weeks=k) for k in range(30)]
mondays = [F for F in mondays if F + timedelta(days=13) <= max(wd)]
se = defaultdict(list)                     # (variant, segment) -> sq errors for heating & hot_water
base = defaultdict(lambda: np.zeros(4))    # (q, season) -> tp, fp, fn, N  for overload
SEASON = {4: "весна", 5: "весна", 6: "лето", 7: "лето", 8: "лето", 9: "осень", 10: "осень", 11: "осень"}
for F in mondays:
    hist = [d for d in wd if d < F]
    if len(hist) < 25: continue
    last8 = [d for d in hist if d >= F - timedelta(weeks=8)]
    b_all = Y.loc[last8].mean().values
    same_reg8 = [d for d in last8 if heat_on(d) == heat_on(F)]
    b_reg = Y.loc[same_reg8].mean().values if len(same_reg8) >= 5 else b_all
    for t in [d for d in wd if F <= d <= F + timedelta(days=13)]:
        y = Y.loc[t].values
        refs = [d for d in reversed(hist) if d.weekday() == t.weekday()]
        refs_reg = [d for d in refs if heat_on(d) == heat_on(t)]
        cand = {}
        if len(refs[:4]) >= 3:
            r = refs[:4]; cand["R4"] = Y.loc[r].values.mean(axis=0)
            cand["EB4a4"] = (Y.loc[r].values.sum(axis=0) + 4 * b_all) / (len(r) + 4)
        rr = refs_reg[:4]
        cand["EB4a4_режим"] = ((Y.loc[rr].values.sum(axis=0) if rr else 0) + 4 * b_reg) / (len(rr) + 4)
        if "R4" not in cand: continue
        # сегмент: первые 4 недели после смены режима — там и должна помогать «память в режиме»
        near_switch = (AUTUMN_START <= t < AUTUMN_START + timedelta(weeks=4)) or (SPRING_END <= t < SPRING_END + timedelta(weeks=4))
        for v, p in cand.items():
            for u in ("heating", "hot_water"):
                m = util == u
                se[(v, u, "4 нед. после смены режима" if near_switch else "остальное")].extend(((p - y) ** 2)[m])
        # перегруз: базовая частота тяжёлых дней и подъём (lift)
        r4 = refs[:4]
        win = [d for d in hist if d >= F - timedelta(weeks=12)]
        Lw = np.where(L.loc[win].values > 0, L.loc[win].values, np.nan)
        ok = np.sum(~np.isnan(Lw), axis=0) >= 5
        for q in (0.5, 0.75, 0.9):
            with np.errstate(all="ignore"):
                C = np.nanquantile(Lw, q, axis=0)
            pred = ((L.loc[r4].values > C).mean(axis=0) >= 0.5) & ok
            act = (L.loc[t].values > C) & ok
            base[(q, SEASON[t.month])] += [np.sum(pred & act), np.sum(pred & ~act), np.sum(~pred & act), np.sum(ok)]
print("Тепло и ГВС: Брайер (меньше — лучше)")
for u in ("heating", "hot_water"):
    for seg in ("4 нед. после смены режима", "остальное"):
        row = "  ".join(f"{v}: {np.mean(se[(v, u, seg)]):.4f}" for v in ("R4", "EB4a4", "EB4a4_режим") if se[(v, u, seg)])
        print(f"  {u:9s} {seg:26s} n={len(se[('R4', u, seg)]):4d} | {row}")
print("\nПерегруз: точность / база тяжёлых дней / подъём (во сколько раз лучше случайного) / помечено")
for season in ("весна", "лето", "осень"):
    out = []
    for q in (0.5, 0.75, 0.9):
        tp, fp, fn, N = base[(q, season)]
        prec = tp / (tp + fp) if tp + fp else float("nan"); br = (tp + fn) / N if N else float("nan")
        out.append(f"q{q}: {prec:.0%} / {br:.0%} / ×{prec / br:.1f} / {int(tp + fp)}")
    print(f"  {season:6s} " + "   ".join(out))
