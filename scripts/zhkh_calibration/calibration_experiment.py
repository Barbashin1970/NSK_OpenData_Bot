"""Калибровка правила «свободных дней» по сезонам: скользящее начало, без подглядывания."""
import sys
from collections import defaultdict
from datetime import date, timedelta
import numpy as np, pandas as pd

D = sys.argv[1]
df = pd.read_csv(f"{D}/zhkh_daily_district.csv", encoding="utf-8-sig")
df["day"] = pd.to_datetime(df["day"]).dt.date
obs = df[(df.collected == 1) & (df.low_sampling == 0) & (df.partial_day == 0)]
work = obs[(obs.is_weekend == 0) & (obs.is_holiday == 0)]
Y = work.pivot_table(index="day", columns=["district", "utility_code"], values="planned_any", aggfunc="first").sort_index()
L = work.pivot_table(index="day", columns=["district", "utility_code"], values="planned_houses_listed", aggfunc="first").sort_index()
cols = Y.columns; util_of = np.array([c[1] for c in cols])
wd_days = list(Y.index)

# ── 1. Отопительный период по температуре: 5 суток подряд ниже/выше +8 °C ──
t = obs.groupby("day")["temp_avg_city"].first().dropna()
def run_end(cond, after):
    run, prev = 0, None
    for d, v in t.items():
        if d < after: continue
        run = run + 1 if (cond(v) and prev is not None and (d - prev).days == 1) or (cond(v) and run == 0) else (1 if cond(v) else 0)
        prev = d
        if run >= 5: return d + timedelta(days=1)
    return None
spring_end = run_end(lambda v: v > 8, date(2026, 3, 1))
autumn_start = run_end(lambda v: v < 8, date(2026, 8, 1))
print(f"Отопительный период по правилу +8 °C (по температуре СИМА): закончился {spring_end}, начался {autumn_start}")

SEASON = {4: "весна", 5: "весна", 6: "лето", 7: "лето", 8: "лето", 9: "осень", 10: "осень", 11: "осень"}
def regime(d):
    return "отопление" if (spring_end and d < spring_end) or (autumn_start and d >= autumn_start) else "без отопления"

# ── 2. Варианты оценки вероятности работ ────────────────────────────────────
VARIANTS = ["R2", "R3", "R4", "R6", "R8", "EB4a2", "EB4a4", "EB8a4", "TSB.3", "TSB.5", "CLIM"]
mondays = [d for d in (date(2026, 4, 27) + timedelta(weeks=k) for k in range(30)) if d + timedelta(days=13) <= max(wd_days)]
err = defaultdict(lambda: defaultdict(list))      # err[key][variant] -> list of sq errors
free = defaultdict(lambda: np.zeros(3))            # (variant, season) -> tp, fp, fn  (θ = 0.25, регулярные ячейки)
over = defaultdict(lambda: np.zeros(3))            # (q, W, season) -> tp, fp, fn
weekly = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))  # (season, F) -> variant -> [sum sq, n]
for F in mondays:
    hist = [d for d in wd_days if d < F]
    if len(hist) < 25: continue
    Yh = Y.loc[hist]
    last8 = [d for d in hist if d >= F - timedelta(weeks=8)]
    b = Yh.loc[last8].mean().values                      # частота работ ячейки за 8 недель (любой день)
    last12 = [d for d in hist if d >= F - timedelta(weeks=12)]
    regular = (Yh.loc[last12].sum().values >= 5)
    # v2 (как calibration_selection.py, EB4a4_режим): базовая частота — 8 недель в режиме дня
    # выпуска, если таких дней не меньше 5, иначе все 8 недель
    reg8 = [d for d in last8 if regime(d) == regime(F)]
    b_reg = Yh.loc[reg8].mean().values if len(reg8) >= 5 else b
    for t_ in [d for d in wd_days if F <= d <= F + timedelta(days=13)]:
        y = Y.loc[t_].values; season = SEASON[t_.month]; reg = regime(t_)
        refs_all = [d for d in reversed(hist) if d.weekday() == t_.weekday()]
        P = {}
        for H in (2, 3, 4, 6, 8):
            r = refs_all[:H]
            if len(r) >= min(3, H): P[f"R{H}"] = Y.loc[r].values.mean(axis=0)
        for H, a in ((4, 2), (4, 4), (8, 4)):
            r = refs_all[:H]
            if len(r) >= 3: P[f"EB{H}a{a}"] = (Y.loc[r].values.sum(axis=0) + a * b) / (len(r) + a)
        for beta in (0.3, 0.5):
            z = b.copy()
            for d in reversed(refs_all[:12]): z = beta * Y.loc[d].values + (1 - beta) * z
            P[f"TSB{str(beta)[1:]}"] = z
        P["CLIM"] = b
        rr = [d for d in refs_all if regime(d) == reg][:4]       # v2: тот же день недели в режиме прогнозного дня
        P["v2"] = ((Y.loc[rr].values.sum(axis=0) if rr else 0) + 4 * b_reg) / (len(rr) + 4)
        if "R4" not in P: continue
        for v, p in P.items():
            se = (p - y) ** 2
            err[("season", season)][v].extend(se); err[("regime", reg)][v].extend(se)
            err[("month", t_.month)][v].extend(se); err[("util", util_of[0])]  # no-op keep structure
            for u in np.unique(util_of):
                err[("season_util", season, u)][v].extend(se[util_of == u])
            w = weekly[(season, F)][v]; w[0] += se.sum(); w[1] += se.size
            pred = (p <= 0.25) & regular; act = (y == 0) & regular
            free[(v, season)] += [np.sum(pred & act), np.sum(pred & ~act), np.sum(~pred & act)]
        # перегруз: ёмкость = квантиль ненулевых объёмов за W недель, опорные — 4 тех же дня недели
        r4 = refs_all[:4]
        for W in (6, 12):
            win = [d for d in hist if d >= F - timedelta(weeks=W)]
            Lw = L.loc[win].values; Lw = np.where(Lw > 0, Lw, np.nan)
            for q in (0.5, 0.75, 0.9):
                with np.errstate(all="ignore"):
                    C = np.nanquantile(Lw, q, axis=0)
                ok = (np.sum(~np.isnan(Lw), axis=0) >= 5)
                heavy = (L.loc[r4].values > C).mean(axis=0) >= 0.5
                pred = heavy & ok; act = (L.loc[t_].values > C) & ok
                over[(q, W, season)] += [np.sum(pred & act), np.sum(pred & ~act), np.sum(~pred & act)]

def brier(lst): return float(np.mean(lst)) if lst else float("nan")
print("\nШкала Брайера (меньше — лучше) и выигрыш против текущего R4, по сезонам и режимам:")
for key in [("season", "весна"), ("season", "лето"), ("season", "осень"), ("regime", "отопление"), ("regime", "без отопления")]:
    e = err[key]; base = brier(e["R4"])
    row = "  ".join(f"{v}:{brier(e[v]):.4f}" for v in VARIANTS + ["v2"])
    best = min(VARIANTS, key=lambda v: brier(e[v]))        # «лучший» — среди вариантов разведки, без v2
    print(f"  {key[1]:14s} n={len(e['R4']):5d} | лучший {best} ({brier(e[best]):.4f}) против R4 {base:.4f} | {row}")
print("  (v2 — EB4a4 в пределах режима, как в calibration_selection.py; в выбор «лучшего» не входит)")

print("\nБутстреп по неделям выпуска (2000 повторов): разница Брайера «лучший − R4», 90% интервал")
rng = np.random.default_rng(7)
for season in ("весна", "лето", "осень"):
    Fs = [F for (s, F) in weekly if s == season]
    e = err[("season", season)]; best = min(VARIANTS, key=lambda v: brier(e[v]))
    diffs = []
    for _ in range(2000):
        pick = rng.choice(len(Fs), len(Fs), replace=True)
        sb = sum(weekly[(season, Fs[i])][best][0] for i in pick); nb = sum(weekly[(season, Fs[i])][best][1] for i in pick)
        s4 = sum(weekly[(season, Fs[i])]["R4"][0] for i in pick); n4 = sum(weekly[(season, Fs[i])]["R4"][1] for i in pick)
        diffs.append(sb / nb - s4 / n4)
    lo, hi = np.quantile(diffs, [0.05, 0.95])
    print(f"  {season:6s} недель {len(Fs):2d} | {best}: Δ = {np.mean(diffs):+.4f}  [{lo:+.4f}; {hi:+.4f}]  {'значимо' if hi < 0 else 'не отличимо от R4'}")

# ── Замечание РАГРАФ № 9 (17.11.2026): горизонты соседних выпусков пересекаются ──────
# Выпуск F прогнозирует F…F+13, выпуск F+7 — F+7…F+20: соседние недели выпуска делят
# неделю прогнозных дней и не независимы, бутстреп по отдельным неделям сужает интервал.
# Блочный бутстреп: скользящие блоки по 2 недели выпуска (старты 0…n−2, ceil(n/2) блоков
# с возвращением, обрезка до n недель). Δ — по всей выборке; интервалы — 90% и 95%.
BLOCK, N_REP = 2, 2000
by_F = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))       # F → вариант → [Σ кв. ошибок, n] по всем сезонам
for (s, F), vv in weekly.items():
    for v, (sq, n) in vv.items():
        by_F[F][v][0] += sq; by_F[F][v][1] += n


def delta(groups, Fs, va, vb):
    sa = sum(groups[F][va][0] for F in Fs); na = sum(groups[F][va][1] for F in Fs)
    sb = sum(groups[F][vb][0] for F in Fs); nb = sum(groups[F][vb][1] for F in Fs)
    return sa / na - sb / nb


def week_boot(groups, Fs, va, vb, rng):
    out = [delta(groups, [Fs[i] for i in rng.choice(len(Fs), len(Fs), replace=True)], va, vb) for _ in range(N_REP)]
    return np.quantile(out, [0.05, 0.95, 0.025, 0.975])


def block_boot(groups, Fs, va, vb, rng):
    n = len(Fs); starts = n - BLOCK + 1; k = -(-n // BLOCK)
    if starts < 2:
        return None                                     # блок один: интервал не определён
    out = []
    for _ in range(N_REP):
        idx = [i for s0 in rng.integers(0, starts, k) for i in range(s0, s0 + BLOCK)][:n]
        out.append(delta(groups, [Fs[i] for i in idx], va, vb))
    return np.quantile(out, [0.05, 0.95, 0.025, 0.975])


def fmt(q):
    if q is None:
        return "интервал не определён (один блок)"
    tag = "значимо" if q[1] < 0 else ("хуже R4" if q[0] > 0 else "не отличимо от R4")
    return f"90% [{q[0]:+.4f}; {q[1]:+.4f}]  95% [{q[2]:+.4f}; {q[3]:+.4f}]  {tag}"


print(f"\nБутстреп, замечание РАГРАФ № 9: по неделям и блоками по {BLOCK} недели выпуска ({N_REP} повторов)")
rng_w, rng_b = np.random.default_rng(8), np.random.default_rng(9)
for season in ("весна", "лето", "осень", "все сезоны"):
    if season == "все сезоны":
        groups, Fs = by_F, sorted(by_F)
        e = {v: [] for v in VARIANTS + ["v2"]}
        for s in ("весна", "лето", "осень"):
            for v in e: e[v].extend(err[("season", s)][v])
    else:
        groups = {F: weekly[(s, F)] for (s, F) in weekly if s == season}
        Fs = sorted(groups); e = err[("season", season)]
    best = min(VARIANTS, key=lambda v: brier(e[v]))
    blocks = max(len(Fs) - BLOCK + 1, 0)
    print(f"  {season:10s} недель выпуска {len(Fs):2d}, блоков-кандидатов {blocks}, в повторе {-(-len(Fs) // BLOCK)}")
    for va in (best, "v2"):
        d0 = delta(groups, Fs, va, "R4")
        print(f"     {va + ' − R4':12s} Δ = {d0:+.4f} | по неделям: {fmt(week_boot(groups, Fs, va, 'R4', rng_w))}")
        print(f"     {'':12s} {'':10s} | блоками:    {fmt(block_boot(groups, Fs, va, 'R4', rng_b))}")

print("\nЛучший вариант по месяцам (так выглядела бы помесячная калибровка):")
for m in sorted({k[1] for k in err if k[0] == "month"}):
    e = err[("month", m)]; best = min(VARIANTS, key=lambda v: brier(e[v]))
    print(f"  месяц {m:2d}: n={len(e['R4']):4d} лучший {best:6s} {brier(e[best]):.4f} | R4 {brier(e['R4']):.4f} | CLIM {brier(e['CLIM']):.4f}")

print("\nЛучший вариант по сезону × услуге:")
for season in ("весна", "лето", "осень"):
    parts = []
    for u in ("electricity", "heating", "hot_water", "cold_water_sewer", "gas"):
        e = err[("season_util", season, u)]
        if not e["R4"]: continue
        best = min(VARIANTS, key=lambda v: brier(e[v]))
        parts.append(f"{u}: {best} ({brier(e[best]):.3f} vs R4 {brier(e['R4']):.3f})")
    print(f"  {season}: " + " | ".join(parts))

print("\n«Свободно» (порог 0,25, регулярные ячейки): точность / полнота / доля свободных")
for season in ("весна", "лето", "осень"):
    out = []
    for v in ("R2", "R4", "R8", "EB4a4", "TSB.3"):
        tp, fp, fn = free[(v, season)]
        out.append(f"{v}: {tp/(tp+fp) if tp+fp else float('nan'):.0%}/{tp/(tp+fn) if tp+fn else float('nan'):.0%}")
    tp, fp, fn = free[("R4", season)]
    print(f"  {season:6s} " + "  ".join(out))

print("\nПерегруз (≥ половины опорных дней выше ёмкости): точность / полнота / помечено")
for season in ("весна", "лето", "осень"):
    out = []
    for W in (6, 12):
        for q in (0.5, 0.75, 0.9):
            tp, fp, fn = over[(q, W, season)]
            out.append(f"W{W}q{q}: {tp/(tp+fp) if tp+fp else float('nan'):.0%}/{tp/(tp+fn) if tp+fn else float('nan'):.0%}/{int(tp+fp)}")
    print(f"  {season:6s} " + "  ".join(out))
