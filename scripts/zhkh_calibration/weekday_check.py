import sys
import numpy as np, pandas as pd
D = sys.argv[1]
DOW = {1: "пн", 2: "вт", 3: "ср", 4: "чт", 5: "пт", 6: "сб", 7: "вс"}
# ── A. История (сутки по UTC, как в архиве) ───────────────────────────────
df = pd.read_csv(f"{D}/zhkh_daily_district.csv", encoding="utf-8-sig")
ok = df[(df.collected == 1) & (df.low_sampling == 0) & (df.partial_day == 0) & (df.is_holiday == 0)]
print("A. Доля «район × день» с плановыми работами по дням недели (сутки UTC, вся история)")
print("   услуга            " + "  ".join(f"{DOW[d]:>5s}" for d in range(1, 8)))
for u in ("electricity", "heating", "hot_water", "cold_water_sewer", "gas"):
    s = ok[ok.utility_code == u].groupby("dow")["planned_any"].mean()
    print(f"   {u:17s} " + "  ".join(f"{s.get(d, np.nan):5.0%}" for d in range(1, 8)) + f"   max: {DOW[int(s.idxmax())]}")
anyu = ok.groupby(["day", "district", "dow"])["planned_any"].max().reset_index()
print("   все услуги        " + "  ".join(f"{anyu[anyu.dow == d].planned_any.mean():5.0%}" for d in range(1, 8)))
print("\n   По районам (любая услуга; доля дней с плановыми работами пн..пт, самый загруженный день):")
for dist, g in anyu.groupby("district"):
    s = g.groupby("dow")["planned_any"].mean()
    print(f"   {dist:24s} " + " ".join(f"{DOW[d]} {s.get(d, np.nan):4.0%}" for d in range(1, 6)) + f"  → {DOW[int(s.loc[1:5].idxmax())]}")

# ── B. Сырые снимки: сутки UTC против суток по Новосибирску ───────────────
sn = pd.read_csv(f"{D}/zhkh_snapshots.csv", encoding="utf-8-sig")
sn = sn[sn.source_backup != "2026-03-31T11:53"]                   # мартовское окно — сбор по запросу
sn["ts"] = pd.to_datetime(sn.scraped_at_utc, utc=True)
sn["day_utc"] = sn.ts.dt.date
sn["day_nsk"] = pd.to_datetime(sn.day_nsk).dt.date
sn["hour_nsk"] = sn.hour_nsk.astype(int)
pl = sn[(sn.kind == "planned") & (sn.district != "all") & (sn.houses > 0)]
snaps = sn.drop_duplicates("scraped_at_utc")[["scraped_at_utc", "day_utc", "day_nsk", "hour_nsk", "source_backup"]]
# полные сутки внутри окон
def full_days(col):
    c = snaps.groupby([col, "source_backup"]).size().reset_index(name="n")
    return set(c[c.n >= 25][col])
print("\nB. Сырые снимки 20.07–19.08 и 17.10–16.11")
print("   Час (НСК), когда плановые отключения висят в списке: доля снимков с ≥1 плановым, по часам")
by_h = snaps.merge(pl.groupby("scraped_at_utc").size().rename("npl").reset_index(), how="left").fillna({"npl": 0})
h = by_h.groupby("hour_nsk")["npl"].agg(lambda x: (x > 0).mean())
mean_pairs = by_h.groupby("hour_nsk")["npl"].mean()
print("   " + " ".join(f"{hh:02d}:{h.get(hh, np.nan):4.0%}" for hh in range(24)))
print("   среднее число «район × услуга» с плановыми в снимке по часам:")
print("   " + " ".join(f"{hh:02d}:{mean_pairs.get(hh, np.nan):4.1f}" for hh in range(24)))
for col, label in (("day_utc", "сутки UTC (как архив)"), ("day_nsk", "сутки НСК")):
    days = full_days(col)
    pairs = pl[pl[col].isin(days)].groupby([col, "district", "utility_code"]).size().reset_index()
    cells = pd.MultiIndex.from_product([sorted(days), sorted(pl.district.unique()), sorted(pl.utility_code.unique())],
                                       names=[col, "district", "utility_code"]).to_frame(index=False)
    cells["any"] = cells.set_index([col, "district", "utility_code"]).index.isin(pairs.set_index([col, "district", "utility_code"]).index)
    cells["dow"] = pd.to_datetime(cells[col]).dt.weekday + 1
    s = cells.groupby("dow")["any"].mean()
    print(f"   {label:22s} доля «район × услуга × сутки» с плановыми: " + "  ".join(f"{DOW[d]} {s.get(d, np.nan):4.0%}" for d in range(1, 8)))

# ── C. «Домов в списке»: делим на снимки с ЛЮБОЙ записью пары, а не только с плановой ──
rows = sn[(sn.district != "all")]
g = rows.groupby(["day_utc", "district", "utility_code"])
any_snaps = g.scraped_at_utc.nunique()
pl_snaps = rows[(rows.kind == "planned") & (rows.houses > 0)].groupby(["day_utc", "district", "utility_code"]).scraped_at_utc.nunique()
both = pd.concat([any_snaps.rename("any"), pl_snaps.rename("pl")], axis=1).dropna()
diluted = (both["any"] > both["pl"]).mean()
print(f"\nC. Пары с плановыми работами, у которых снимков «с любой записью» больше, чем с плановой: {diluted:.0%} "
      f"(медиана отношения {np.median(both['pl'] / both['any']):.2f})")
