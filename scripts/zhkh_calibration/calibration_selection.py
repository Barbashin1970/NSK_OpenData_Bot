"""Калибровка v2: выбор оценщика и порога на отборочных неделях, проверка на отложенных."""
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
SPRING_END, AUTUMN_START = date(2026, 4, 28), date(2026, 10, 11)        # правило +8 °C по температуре СИМА
heat = lambda d: d < SPRING_END or d >= AUTUMN_START
SELECT_TO, HOLD_FROM = date(2026, 10, 12), date(2026, 10, 19)
mondays = [date(2026, 4, 27) + timedelta(weeks=k) for k in range(30)]
mondays = [F for F in mondays if F + timedelta(days=13) <= max(wd)]
THETAS = (0.2, 0.25, 0.3, 0.35, 0.4, 0.5)
conf = defaultdict(lambda: np.zeros(3))          # (set, variant, θ) → tp, fp, fn  (свободно, регулярные)
over = defaultdict(lambda: np.zeros(4))          # (set, capvariant) → tp, fp, fn, N
for F in mondays:
    part = "отбор" if F <= SELECT_TO else ("отложено" if F >= HOLD_FROM else None)
    if part is None: continue
    hist = [d for d in wd if d < F]
    if len(hist) < 25: continue
    last8 = [d for d in hist if d >= F - timedelta(weeks=8)]
    b = Y.loc[last8].mean().values
    reg8 = [d for d in last8 if heat(d) == heat(F)]
    b_reg = Y.loc[reg8].mean().values if len(reg8) >= 5 else b
    regular = Y.loc[[d for d in hist if d >= F - timedelta(weeks=12)]].sum().values >= 5
    for t in [d for d in wd if F <= d <= F + timedelta(days=13)]:
        y = Y.loc[t].values
        refs = [d for d in reversed(hist) if d.weekday() == t.weekday()]
        refs_reg = [d for d in refs if heat(d) == heat(t)]
        if len(refs[:4]) < 3: continue
        r4, rr = refs[:4], refs_reg[:4]
        P = {"R4": Y.loc[r4].values.mean(axis=0),
             "EB4a4": (Y.loc[r4].values.sum(axis=0) + 4 * b) / (len(r4) + 4),
             "EB4a2": (Y.loc[r4].values.sum(axis=0) + 2 * b) / (len(r4) + 2),
             "EB4a4_режим": ((Y.loc[rr].values.sum(axis=0) if rr else 0) + 4 * b_reg) / (len(rr) + 4),
             "EB4a2_режим": ((Y.loc[rr].values.sum(axis=0) if rr else 0) + 2 * b_reg) / (len(rr) + 2),
             "EB4a1_режим": ((Y.loc[rr].values.sum(axis=0) if rr else 0) + 1 * b_reg) / (len(rr) + 1),
             "CLIM": b}
        # тепло — в пределах режима, остальные услуги — EB4a4 (гибрид по выводам разведки)
        P["гибрид"] = np.where(util == "heating", P["EB4a4_режим"], P["EB4a4"])
        for v, p in P.items():
            for th in THETAS:
                pred = (p <= th) & regular; act = (y == 0) & regular
                conf[(part, v, th)] += [np.sum(pred & act), np.sum(pred & ~act), np.sum(~pred & act)]
        # перегруз: ёмкость P75 за 12 недель — по всем дням / в пределах режима
        for cv in ("P75_12нед", "P75_12нед_режим", "P75_режим_тепло"):
            win_all = [d for d in hist if d >= F - timedelta(weeks=12)]
            win = [d for d in win_all if cv == "P75_12нед" or heat(d) == heat(t)]
            Lw = np.where(L.loc[win].values > 0, L.loc[win].values, np.nan) if win else np.full((1, len(util)), np.nan)
            ok = np.sum(~np.isnan(Lw), axis=0) >= 5
            with np.errstate(all="ignore"):
                C = np.nanquantile(Lw, 0.75, axis=0)
            if cv == "P75_режим_тепло":
                Lall = np.where(L.loc[win_all].values > 0, L.loc[win_all].values, np.nan)
                with np.errstate(all="ignore"):
                    Call = np.nanquantile(Lall, 0.75, axis=0)
                okall = np.sum(~np.isnan(Lall), axis=0) >= 5
                C = np.where(util == "heating", C, Call); ok = np.where(util == "heating", ok, okall)
            pred = ((L.loc[r4].values > C).mean(axis=0) >= 0.5) & ok
            act = (L.loc[t].values > C) & ok
            over[(part, cv)] += [np.sum(pred & act), np.sum(pred & ~act), np.sum(~pred & act), np.sum(ok)]
def pr(s):
    tp, fp, fn = s[:3]
    return (tp / (tp + fp) if tp + fp else float("nan")), (tp / (tp + fn) if tp + fn else float("nan")), int(tp + fp)
print("«Свободно», регулярные ячейки: точность / полнота / помечено")
for part in ("отбор", "отложено"):
    print(f" [{part}]")
    for v in ("R4", "EB4a4_режим", "EB4a2_режим", "EB4a1_режим"):
        print(f"   {v:12s} " + "  ".join(f"θ{th}: {pr(conf[(part, v, th)])[0]:.0%}/{pr(conf[(part, v, th)])[1]:.0%}" for th in THETAS))
# выбор на отборе: максимум точности при полноте не ниже R4@0.25
base_p, base_r, _ = pr(conf[("отбор", "R4", 0.25)])
cands = [(pr(conf[("отбор", v, th)]), v, th) for v in ("R4", "EB4a2", "EB4a4", "EB4a4_режим", "EB4a2_режим", "EB4a1_режим", "гибрид", "CLIM") for th in THETAS]
ok_c = [c for c in cands if c[0][1] >= base_r - 1e-9]
best = max(ok_c, key=lambda c: c[0][0])
print(f"\nВыбор на отборе (точность max при полноте ≥ R4@0,25 = {base_r:.0%}): {best[1]} θ={best[2]} → {best[0][0]:.0%}/{best[0][1]:.0%} против R4 {base_p:.0%}/{base_r:.0%}")
hp, hr, hf = pr(conf[("отложено", best[1], best[2])]); rp, rr_, rf = pr(conf[("отложено", "R4", 0.25)])
print(f"Отложенные недели: выбранный {hp:.0%}/{hr:.0%} (помечено {hf})  против R4@0,25 {rp:.0%}/{rr_:.0%} (помечено {rf})  → Δточности {100*(hp-rp):+.1f} п.п., Δполноты {100*(hr-rr_):+.1f} п.п.")
print("\nПерегруз: точность / база тяжёлых дней / подъём / помечено")
for part in ("отбор", "отложено"):
    for cv in ("P75_12нед", "P75_12нед_режим", "P75_режим_тепло"):
        tp, fp, fn, N = over[(part, cv)]
        p = tp / (tp + fp) if tp + fp else float("nan"); br = (tp + fn) / N if N else float("nan")
        print(f"   [{part:8s}] {cv:16s} {p:.0%} / {br:.0%} / ×{p / br:.1f} / {int(tp + fp)}")
